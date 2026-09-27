"""Zero-trust access control (NIST SP 800-207 style): verify every request, never trust location.

- Local demo identity provider: argon2-hashed passwords (OIDC-shaped; SSO is a config swap).
- Access tokens: HS256 JWT, 15 min, claims sub/role/regions/mfa/jti/fam. RS256 + an IdP in production.
- Refresh tokens: random, stored only as SHA-256 hashes, single-use and rotating; presenting a used
  refresh token revokes the whole session family.
- One policy function decides allow/deny for (user, action, resource region).
- Hash-chained, append-only audit log in SQLite; an admin endpoint verifies the chain.
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

from ..config import REPORTS_DIR

ACCESS_TTL_S = 15 * 60
REFRESH_TTL_S = 8 * 3600
ALGO = "HS256"

ROLE_HOME = {"ops_analyst": "/ops", "admin": "/ops", "trader": "/trader"}
# action -> roles allowed
POLICY = {
    "view_ops": {"ops_analyst", "admin"},
    "view_trader": {"trader"},
    "read": {"ops_analyst", "admin", "trader"},
    "chat": {"ops_analyst", "admin", "trader"},
    "chaos": {"ops_analyst", "admin"},
    "control": {"ops_analyst", "admin"},
    "ack": {"ops_analyst", "admin"},
    "incident_detail": {"ops_analyst", "admin"},
    "quarantine": {"ops_analyst", "admin"},
    "audit": {"admin"},
}

DEMO_PASSWORD = "demo123"
DEMO_USERS = {
    "ops_us": ("Priya Nair", "ops_analyst", ["US"]),
    "trader_us": ("Daniel Brooks", "trader", ["US"]),
    "trader_eu": ("Sofia Lindqvist", "trader", ["EU"]),
    "admin": ("Platform Admin", "admin", ["US", "EU", "GLOBAL"]),
}


@dataclass(frozen=True)
class User:
    username: str
    display_name: str
    role: str
    regions: tuple

    def public(self) -> dict:
        return {"username": self.username, "display_name": self.display_name, "role": self.role,
                "regions": list(self.regions), "home": ROLE_HOME.get(self.role, "/login")}


def allow(user: User | None, action: str, region: str | None = None) -> bool:
    """The single policy decision point: role (RBAC) and region attribute (ABAC)."""
    if user is None:
        return False
    if user.role not in POLICY.get(action, set()):
        return False
    if region is not None and region not in user.regions:
        return False
    return True


class AuthError(Exception):
    pass


class AuthService:
    def __init__(self, secret: str | None = None):
        self.secret = secret or os.environ.get("FEEDSENTINEL_JWT_SECRET") or secrets.token_urlsafe(48)
        self.ph = PasswordHasher()
        self.users: dict[str, tuple[User, str]] = {}
        for name, (display, role, regions) in DEMO_USERS.items():
            self.users[name] = (User(name, display, role, tuple(regions)), self.ph.hash(DEMO_PASSWORD))
        self._lock = threading.Lock()
        self.refresh: dict[str, dict] = {}       # sha256(token) -> {user, fam, used, exp}
        self.revoked_fams: set[str] = set()
        self.revoked_jti: set[str] = set()

    # ------------------------------------------------------------------ passwords
    def authenticate(self, username: str, password: str) -> User:
        rec = self.users.get(username)
        if rec is None:
            # hash anyway so timing does not reveal which usernames exist
            try:
                self.ph.verify(self.users["admin"][1], password + "x")
            except VerifyMismatchError:
                pass
            raise AuthError("invalid username or password")
        try:
            self.ph.verify(rec[1], password)
        except VerifyMismatchError:
            raise AuthError("invalid username or password") from None
        return rec[0]

    # ------------------------------------------------------------------ tokens
    def issue(self, user: User, fam: str | None = None) -> tuple[str, str, str]:
        fam = fam or secrets.token_urlsafe(12)
        now = int(time.time())
        claims = {"sub": user.username, "role": user.role, "regions": list(user.regions),
                  "mfa": False, "jti": secrets.token_urlsafe(12), "fam": fam,
                  "iat": now, "exp": now + ACCESS_TTL_S}
        access = jwt.encode(claims, self.secret, algorithm=ALGO)
        refresh = secrets.token_urlsafe(32)
        with self._lock:
            self.refresh[hashlib.sha256(refresh.encode()).hexdigest()] = {
                "user": user.username, "fam": fam, "used": False, "exp": now + REFRESH_TTL_S}
        return access, refresh, fam

    def verify(self, token: str) -> tuple[User, dict]:
        try:
            claims = jwt.decode(token, self.secret, algorithms=[ALGO], options={"require": ["exp", "sub", "jti"]})
        except jwt.ExpiredSignatureError:
            raise AuthError("token expired") from None
        except jwt.InvalidTokenError:
            raise AuthError("invalid token") from None
        if claims["jti"] in self.revoked_jti or claims.get("fam") in self.revoked_fams:
            raise AuthError("token revoked")
        rec = self.users.get(claims["sub"])
        if rec is None:
            raise AuthError("unknown user")
        return rec[0], claims

    def rotate(self, refresh: str) -> tuple[User, str, str]:
        h = hashlib.sha256(refresh.encode()).hexdigest()
        with self._lock:
            rec = self.refresh.get(h)
            if rec is None or rec["exp"] < time.time() or rec["fam"] in self.revoked_fams:
                raise AuthError("invalid refresh token")
            if rec["used"]:
                # reuse of a rotated token: assume theft, kill the whole session family
                self.revoked_fams.add(rec["fam"])
                raise AuthError("refresh token reuse detected; session revoked")
            rec["used"] = True
        user = self.users[rec["user"]][0]
        access, new_refresh, _ = self.issue(user, fam=rec["fam"])
        return user, access, new_refresh

    def logout(self, claims: dict) -> None:
        self.revoked_jti.add(claims["jti"])
        if claims.get("fam"):
            self.revoked_fams.add(claims["fam"])


class RateLimiter:
    def __init__(self, limit: int, per_s: float):
        self.limit, self.per_s = limit, per_s
        self.hits: dict[str, deque] = {}

    def check(self, key: str) -> bool:
        now = time.monotonic()
        dq = self.hits.setdefault(key, deque())
        while dq and now - dq[0] > self.per_s:
            dq.popleft()
        if len(dq) >= self.limit:
            return False
        dq.append(now)
        return True


class AuditLog:
    """Append-only, hash-chained: each entry stores the hash of the previous one."""

    def __init__(self, path=None):
        self.path = str(path or (REPORTS_DIR / "audit.db"))
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._conn() as c:
            c.execute("CREATE TABLE IF NOT EXISTS audit (seq INTEGER PRIMARY KEY AUTOINCREMENT, t_ms INTEGER, "
                      "actor TEXT, action TEXT, target TEXT, outcome TEXT, detail TEXT, prev_hash TEXT, hash TEXT)")

    def _conn(self):
        return sqlite3.connect(self.path, timeout=5)

    @staticmethod
    def _digest(prev: str, t_ms, actor, action, target, outcome, detail) -> str:
        body = json.dumps([prev, t_ms, actor, action, target, outcome, detail], separators=(",", ":"))
        return hashlib.sha256(body.encode()).hexdigest()

    def record(self, actor: str, action: str, target: str = "", outcome: str = "ok", detail: str = "") -> None:
        detail = (detail or "")[:500]
        with self._lock, self._conn() as c:
            row = c.execute("SELECT hash FROM audit ORDER BY seq DESC LIMIT 1").fetchone()
            prev = row[0] if row else "GENESIS"
            t_ms = int(time.time() * 1000)
            h = self._digest(prev, t_ms, actor, action, target, outcome, detail)
            c.execute("INSERT INTO audit (t_ms, actor, action, target, outcome, detail, prev_hash, hash) "
                      "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (t_ms, actor, action, target, outcome, detail, prev, h))

    def entries(self, limit: int = 100) -> list[dict]:
        with self._conn() as c:
            rows = c.execute("SELECT seq, t_ms, actor, action, target, outcome, detail, hash FROM audit "
                             "ORDER BY seq DESC LIMIT ?", (int(limit),)).fetchall()
        keys = ("seq", "t_ms", "actor", "action", "target", "outcome", "detail", "hash")
        return [dict(zip(keys, r)) for r in rows]

    def verify(self) -> dict:
        with self._conn() as c:
            rows = c.execute("SELECT seq, t_ms, actor, action, target, outcome, detail, prev_hash, hash "
                             "FROM audit ORDER BY seq").fetchall()
        prev = "GENESIS"
        for r in rows:
            seq, t_ms, actor, action, target, outcome, detail, p, h = r
            if p != prev or self._digest(prev, t_ms, actor, action, target, outcome, detail) != h:
                return {"ok": False, "entries": len(rows), "broken_at": seq}
            prev = h
        return {"ok": True, "entries": len(rows)}
