"""FeedSentinel server: FastAPI + WebSocket in front of the real detection engine.

Every REST route and every WebSocket connection is authenticated; one policy function decides
role (RBAC) and region (ABAC) access; data is filtered server-side per user. The replay runs in the
same event loop, paced against the wall clock, so handlers never race the engine.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .. import PRODUCT
from .. import schema as S
from ..config import DASHBOARD_DIR, MS, NS, REPLAY, REPORTS_DIR
from ..data.lobster import load_tape
from ..detect import rca as RCA
from ..detect.ml import Models
from ..refdata import SymbolDirectory
from ..runtime.replay import ReplaySession
from ..sim.chaos import SCENARIOS, scenario_list
from ..timeutil import NEW_YORK, clock, day_str
from . import chat as CHAT
from .security import AuditLog, AuthError, AuthService, RateLimiter, User, allow
from .timeline import Timeline

log = logging.getLogger("feedsentinel.server")

REGION = "US"                       # the LOBSTER replay feeds and symbols are US (Nasdaq)
SPEEDS = [1, 2, 5, 10, 20]
TICK_S = 0.25                       # <= 4 Hz per connection


class Runtime:
    def __init__(self, start: str = "10:30", speed: int = 5):
        self.start = start
        self.speed = speed
        self.paused = False
        self.tape = load_tape()
        self.refdata = SymbolDirectory.load()
        self.models = Models.load()
        try:
            from ..explain.explainer import Explainer
            self.explainer = Explainer()
        except Exception as exc:  # the explainer is optional; templates live in the engine fallback
            log.warning("explainer unavailable: %s", exc)
            self.explainer = None
        self.auth = AuthService()
        self.audit_log = AuditLog()
        self.login_limit = RateLimiter(10, 60)
        self.chat_limit = RateLimiter(30, 60)
        self.version = 0
        self.t_wall0 = time.time()
        self.events_per_s = 0.0
        self.lag_ms = 0.0
        self.demo_task: asyncio.Task | None = None
        self.start_session()

    # ------------------------------------------------------------------ session
    def start_session(self) -> None:
        self.session = ReplaySession(self.tape, self.refdata, REPLAY, models=self.models,
                                     explainer=self.explainer, start=self.start, warmup_s=120)
        self.engine = self.session.engine
        self.timeline = Timeline(NEW_YORK)
        self.engine.on_window = self.timeline.on_window
        self.session.warmup()
        self.chart_symbol = self.session.symbols[0]
        self.acks: dict[str, dict] = {}
        self.stopwatch_fault = None
        self.version += 1

    async def loop(self) -> None:
        last = time.perf_counter()
        last_count, last_rate_t = self.engine.processed, last
        while True:
            await asyncio.sleep(0.02)
            now = time.perf_counter()
            dt, last = now - last, now
            if not self.paused:
                c0 = time.perf_counter()
                try:
                    self.session.run_until(self.session.now + int(min(dt, 0.5) * self.speed * NS), step_ms=50)
                    if self.session.finished:
                        self.start_session()
                        last_count = self.engine.processed
                except Exception:  # fail open: a bug must never stop monitoring
                    log.exception("replay step failed")
                self.lag_ms = (time.perf_counter() - c0) * 1000
            if now - last_rate_t >= 1.0:
                self.events_per_s = (self.engine.processed - last_count) / (now - last_rate_t)
                last_count, last_rate_t = self.engine.processed, now

    # ------------------------------------------------------------------ scope helpers
    @staticmethod
    def in_scope(user: User) -> bool:
        return REGION in user.regions

    def audit(self, actor, action, target="", outcome="ok", detail="") -> None:
        try:
            self.audit_log.record(actor, action, target, outcome, detail)
        except Exception:
            log.exception("audit write failed")

    def clock(self) -> str:
        return clock(self.session.now, NEW_YORK)

    def symbol_regions(self) -> dict:
        return {s: REGION for s in self.session.symbols}

    def all_feed_ids(self) -> list:
        return list(self.engine.feed_ids)

    def best_feed_name(self, exclude=None) -> str:
        return self.engine._best_feed_name(exclude=exclude)

    # ------------------------------------------------------------------ payloads
    def hello(self, user: User) -> dict:
        scoped = self.in_scope(user)
        ops = allow(user, "view_ops")
        has_models = self.models is not None
        scen = [s for s in scenario_list() if has_models or s["group"] != "novel"]
        return {
            "type": "hello", "view": "ops" if ops else "trader", "user": user.public(), "mode": "replay",
            "product": PRODUCT, "source": self.session.source, "tz": "America/New_York",
            "speed": self.speed, "speeds": SPEEDS, "live_available": False,
            "feeds": self.session.feeds if scoped else [],
            "symbols": self.session.symbols if scoped else [],
            "scenarios": scen if (ops and scoped) else [],
            "models": {"anomaly": has_models and self.models.has_anomaly,
                       "classifier": has_models and self.models.has_classifier},
            "llm": self.explainer.status if self.explainer is not None else {"enabled": False, "provider": "template"},
        }

    def incident_summaries(self, limit=30) -> list:
        out = self.engine.incidents.summaries(limit)
        for s in out:
            a = self.acks.get(s["id"])
            s["acked_by"] = a["by"] if a else None
            s["acked_ms"] = a["ms"] if a else None
        return out

    def stopwatch(self):
        f = self.stopwatch_fault
        if f is None:
            return None
        sp = f.spec
        now = self.session.now
        out = {"scenario": f.scenario, "label": sp.label, "feed": f.feed, "group": sp.group,
               "started_ms": f.start_ns // MS, "detected": False, "ttd_s": None, "incident_id": None,
               "false_alerts": 0}
        incs = self.engine.incidents.incidents
        if sp.group == "market":
            end = (f.ended_ns + 5 * NS) if f.ended_ns is not None else now
            out["false_alerts"] = sum(1 for i in incs if f.start_ns <= i.opened_ns <= end)
            out["elapsed_s"] = round(((f.ended_ns or now) - f.start_ns) / NS, 1)
            return out
        hit = next((i for i in incs if i.feed == f.feed and i.code in sp.expected and i.opened_ns >= f.start_ns),
                   None)
        if hit is None:   # the same fault was already open on that feed when injected
            hit = next((i for i in incs if i.feed == f.feed and i.code in sp.expected and i.status == "OPEN"), None)
            if hit is not None:
                out.update(detected=True, ttd_s=0.0, incident_id=hit.id, elapsed_s=0.0)
                return out
        if hit is not None:
            out.update(detected=True, ttd_s=round((hit.opened_ns - f.start_ns) / NS, 2), incident_id=hit.id,
                       elapsed_s=round((hit.opened_ns - f.start_ns) / NS, 1))
        else:
            out["elapsed_s"] = round(((f.ended_ns or now) - f.start_ns) / NS, 1)
        return out

    def ops_tick(self, user: User) -> dict:
        e, now = self.engine, self.session.now
        base = {"type": "tick", "t_ms": now // MS, "clock": clock(now, NEW_YORK), "date": day_str(now, NEW_YORK),
                "session": e.ctx.session, "mode": "replay", "speed": self.speed, "paused": self.paused,
                "self": {"events_per_s": round(self.events_per_s), "lag_ms": round(self.lag_ms, 1),
                         "processed": e.processed, "uptime_s": round(time.time() - self.t_wall0, 1)}}
        if not self.in_scope(user):
            base.update(feeds=[], recommended={}, chart=None, incidents=[], timeline=[],
                        chaos={"active": [], "stopwatch": None})
            return base
        base.update(feeds=e.feed_views(), recommended=dict(e.recommended), chart=e.chart_view(self.chart_symbol),
                    incidents=self.incident_summaries(), timeline=self.timeline.view(60),
                    chaos={"active": self.session.chaos.active_view(now), "stopwatch": self.stopwatch()})
        return base

    def tool_feed_health(self, user):
        return self.engine.feed_views() if self.in_scope(user) else []

    def tool_incidents(self, user):
        return self.engine.incidents.all() if self.in_scope(user) else []

    def tool_rca(self, inc):
        return RCA.analyse(inc, self.engine)

    def tool_timeline(self, user, minutes: int):
        if not self.in_scope(user):
            return []
        cutoff = (self.session.now - minutes * 60 * NS) // MS
        return [x for x in self.timeline.view(2000) if x["t_ms"] >= cutoff]

    def tool_metrics(self):
        p = REPORTS_DIR / "metrics.json"
        try:
            return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        except Exception:
            return {}

    def tool_symbol_status(self, user) -> list:
        if not self.in_scope(user):
            return []
        e = self.engine
        out = []
        for s in e.symbols:
            rec = e.recommended.get(s, e.feed_ids[0])
            fr = e.feeds[rec]
            healthy = [f for f in e.feed_ids if e.feeds[f].health.state == S.HEALTHY
                       and s not in e.feeds[f].bad_symbols]
            bad = [f for f in e.feed_ids if f not in healthy]
            if fr.health.state == S.HEALTHY and len(healthy) >= 2:
                trust, reason = "VERIFIED", f"{len(healthy)} independent feeds agree"
                if bad:
                    reason += f"; {', '.join(e.feeds[b].name for b in bad)} excluded"
            elif healthy:
                trust, reason = "USE CAUTION", f"only {e.feeds[healthy[0]].name} is healthy; no second feed to confirm"
            else:
                trust, reason = "DO NOT USE", "no healthy feed for this symbol"
            q = fr.quotes.get(s)
            cons = e.cons.cons.get(s)
            price = cons if cons is not None else ((q[0] + q[1]) / 2 if q else None)
            sec = self.refdata.get(s)
            out.append({"symbol": s, "name": (sec.name.split(" - ")[0] if sec else s),
                        "price": None if price is None else round(price, 2), "trust": trust, "trust_reason": reason,
                        "source": rec, "source_name": fr.name,
                        "updated_s": None if q is None else round(max(0, e.now - q[4]) / NS, 1)})
        return out

    def trader_alerts(self) -> list:
        e = self.engine
        out = []
        for inc in list(e.incidents.incidents)[-40:]:
            syms = ", ".join(inc.symbols) if inc.symbols else "All"
            best = self.best_feed_name(exclude=inc.feed)
            best_name = best.split("(")[-1].rstrip(")") if "(" in best else best
            out.append({"t_ms": inc.opened_ns // MS, "clock": clock(inc.opened_ns, NEW_YORK),
                        "level": "critical" if inc.severity == S.CRITICAL else "warn",
                        "text": f"{syms} prices from {inc.feed_name} feed unreliable since "
                                f"{clock(inc.opened_ns, NEW_YORK)}; using {best_name} feed"})
            if inc.status != "OPEN" and inc.closed_ns and not inc.merged_into:
                out.append({"t_ms": inc.closed_ns // MS, "clock": clock(inc.closed_ns, NEW_YORK), "level": "ok",
                            "text": f"{inc.feed_name} feed back to normal at {clock(inc.closed_ns, NEW_YORK)}"})
        out.sort(key=lambda a: (a["t_ms"], a["level"] == "critical"), reverse=True)
        seen, uniq = set(), []
        for a in out:
            if a["text"] not in seen:
                seen.add(a["text"])
                uniq.append(a)
        return uniq[:20]

    def trader_tick(self, user: User) -> dict:
        now = self.session.now
        out = {"type": "trader_tick", "t_ms": now // MS, "clock": clock(now, NEW_YORK),
               "date": day_str(now, NEW_YORK), "session": self.engine.ctx.session, "tz": "America/New_York",
               "watchlist": [], "alerts": []}
        if not self.in_scope(user):
            out["notice"] = f"No feeds in your region ({', '.join(user.regions)}) are streaming right now."
            return out
        out["watchlist"] = self.tool_symbol_status(user)
        out["alerts"] = self.trader_alerts()
        return out

    def state_for(self, user: User) -> dict:
        return self.ops_tick(user) if allow(user, "view_ops") else self.trader_tick(user)

    # ------------------------------------------------------------------ demo autopilot
    async def demo_script(self) -> None:
        steps = [(2, "packet_loss", "C"), (12, None, None), (3, "frozen", "C"), (12, None, None),
                 (3, "test_leak_2017", "C"), (12, None, None), (3, "market_move", None), (12, None, None)]
        try:
            for wait, scen, feed in steps:
                await asyncio.sleep(wait)
                if scen is None:
                    self.session.clear_all()
                else:
                    self.stopwatch_fault = self.session.inject(scen, feed)
        except asyncio.CancelledError:
            pass


# ====================================================================== app
class LoginBody(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=128)


class ChaosBody(BaseModel):
    scenario: str = Field(max_length=64)
    feed: str | None = Field(default=None, max_length=8)
    symbols: list[str] | None = None
    params: dict = Field(default_factory=dict)
    duration_s: float | None = Field(default=None, ge=0, le=3600)


class ControlBody(BaseModel):
    speed: int | None = None
    paused: bool | None = None
    symbol: str | None = Field(default=None, max_length=16)
    restart: bool | None = None
    mode: str | None = Field(default=None, max_length=16)


class NoteBody(BaseModel):
    label: str | None = Field(default=None, max_length=32)
    note: str = Field(default="", max_length=500)


class DemoBody(BaseModel):
    action: str = Field(max_length=8)


class ChatBody(BaseModel):
    message: str = Field(min_length=1, max_length=500)


def create_app(start: str = "10:30", speed: int = 5) -> FastAPI:
    app = FastAPI(title="FeedSentinel", docs_url=None, redoc_url=None, openapi_url=None)
    rt = Runtime(start=start, speed=speed)
    app.state.rt = rt

    @app.on_event("startup")
    async def _start():
        app.state.loop_task = asyncio.create_task(rt.loop())

    @app.middleware("http")
    async def headers(request: Request, call_next):
        resp = await call_next(request)
        resp.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
            "connect-src 'self' ws: wss:; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["Referrer-Policy"] = "no-referrer"
        resp.headers["Cache-Control"] = "no-store" if request.url.path.startswith("/api") else "no-cache"
        return resp

    # ---------------------------------------------------------------- auth deps
    def current(request: Request) -> tuple[User, dict]:
        h = request.headers.get("authorization", "")
        if not h.lower().startswith("bearer "):
            raise HTTPException(401, "authentication required")
        try:
            return rt.auth.verify(h[7:].strip())
        except AuthError as exc:
            raise HTTPException(401, str(exc)) from None

    def need(action: str):
        def dep(request: Request, uc=Depends(current)) -> User:
            user = uc[0]
            if not allow(user, action):
                rt.audit(user.username, "access_denied", f"{request.method} {request.url.path}", "denied", action)
                raise HTTPException(403, f"not permitted for role {user.role}")
            return user
        return dep

    def ip(request: Request) -> str:
        return request.client.host if request.client else "?"

    def token_response(user: User, access: str, refresh: str) -> JSONResponse:
        resp = JSONResponse({"access_token": access, "expires_in": 900, "user": user.public()})
        resp.set_cookie("fs_refresh", refresh, httponly=True, secure=True, samesite="strict",
                        path="/api/auth", max_age=8 * 3600)
        return resp

    # ---------------------------------------------------------------- pages
    @app.get("/")
    async def root():
        return RedirectResponse("/login")

    for page in ("/login", "/ops", "/trader"):
        app.add_api_route(page, lambda: FileResponse(DASHBOARD_DIR / "index.html"), methods=["GET"],
                          include_in_schema=False)
    app.mount("/static", StaticFiles(directory=DASHBOARD_DIR), name="static")

    # ---------------------------------------------------------------- auth routes
    @app.post("/api/auth/login")
    async def login(body: LoginBody, request: Request):
        if not rt.login_limit.check(ip(request)):
            rt.audit(body.username, "login", "", "rate_limited")
            raise HTTPException(429, "too many attempts; wait a minute")
        try:
            user = rt.auth.authenticate(body.username, body.password)
        except AuthError:
            rt.audit(body.username, "login", "", "failed", ip(request))
            raise HTTPException(401, "invalid username or password") from None
        access, refresh, _ = rt.auth.issue(user)
        rt.audit(user.username, "login", "", "ok", ip(request))
        return token_response(user, access, refresh)

    @app.post("/api/auth/refresh")
    async def refresh(request: Request):
        tok = request.cookies.get("fs_refresh")
        if not tok:
            raise HTTPException(401, "no refresh token")
        try:
            user, access, new_refresh = rt.auth.rotate(tok)
        except AuthError as exc:
            rt.audit("?", "token_refresh", "", "denied", str(exc))
            raise HTTPException(401, str(exc)) from None
        rt.audit(user.username, "token_refresh")
        return token_response(user, access, new_refresh)

    @app.post("/api/auth/logout")
    async def logout(uc=Depends(current)):
        rt.auth.logout(uc[1])
        rt.audit(uc[0].username, "logout")
        resp = JSONResponse({"ok": True})
        resp.delete_cookie("fs_refresh", path="/api/auth")
        return resp

    @app.get("/api/auth/me")
    async def me(uc=Depends(current)):
        return uc[0].public()

    # ---------------------------------------------------------------- state
    @app.get("/api/hello")
    async def hello(user=Depends(need("read"))):
        return rt.hello(user)

    @app.get("/api/state")
    async def state(user=Depends(need("read"))):
        return rt.state_for(user)

    @app.get("/api/incidents")
    async def incidents(limit: int = 100, user=Depends(need("incident_detail"))):
        return rt.incident_summaries(max(1, min(limit, 500))) if rt.in_scope(user) else []

    def get_incident(iid: str, user: User):
        inc = rt.engine.incidents.get(iid)
        if inc is None or not rt.in_scope(user):
            raise HTTPException(404, "unknown incident")
        return inc

    @app.get("/api/incidents/{iid}")
    async def incident(iid: str, user=Depends(need("incident_detail"))):
        inc = get_incident(iid, user)
        d = inc.detail(NEW_YORK)
        a = rt.acks.get(iid)
        d["acked_by"], d["acked_ms"] = (a["by"], a["ms"]) if a else (None, None)
        d["rca"] = RCA.analyse(inc, rt.engine)
        return d

    @app.get("/api/rca/{iid}")
    async def rca(iid: str, user=Depends(need("incident_detail"))):
        return RCA.analyse(get_incident(iid, user), rt.engine)

    @app.post("/api/incidents/{iid}/ack")
    async def ack(iid: str, body: NoteBody, user=Depends(need("ack"))):
        inc = get_incident(iid, user)
        rt.acks[iid] = {"by": user.username, "ms": rt.session.now // MS, "note": body.note}
        rt.timeline.on_ack(rt.session.now, inc, user.username)
        rt.audit(user.username, "incident_ack", iid, "ok", body.note)
        return {"ok": True}

    @app.post("/api/incidents/{iid}/feedback")
    async def feedback(iid: str, body: NoteBody, user=Depends(need("ack"))):
        inc = get_incident(iid, user)
        if body.label not in ("confirmed", "false_positive"):
            raise HTTPException(400, "label must be confirmed or false_positive")
        inc.feedback, inc.feedback_note = body.label, body.note
        rt.audit(user.username, "incident_feedback", iid, "ok", body.label)
        return {"ok": True}

    @app.get("/api/timeline")
    async def timeline(limit: int = 200, feed: str | None = None, user=Depends(need("incident_detail"))):
        return rt.timeline.view(max(1, min(limit, 2000)), feed) if rt.in_scope(user) else []

    @app.get("/api/quarantine")
    async def quarantine(feed: str | None = None, limit: int = 50, user=Depends(need("quarantine"))):
        return rt.engine.quarantine_view(feed, max(1, min(limit, 200))) if rt.in_scope(user) else []

    @app.get("/api/metrics")
    async def metrics(user=Depends(need("read"))):
        return rt.tool_metrics() if rt.in_scope(user) else {}

    # ---------------------------------------------------------------- chaos & control
    @app.post("/api/chaos")
    async def chaos(body: ChaosBody, user=Depends(need("chaos"))):
        sp = SCENARIOS.get(body.scenario)
        if sp is None:
            raise HTTPException(400, f"unknown scenario {body.scenario}")
        if sp.group != "market" and body.feed not in rt.engine.feed_ids:
            raise HTTPException(400, "feed must be one of " + ", ".join(rt.engine.feed_ids))
        if body.symbols and not set(body.symbols) <= set(rt.session.symbols):
            raise HTTPException(400, "unknown symbol")
        f = rt.session.inject(body.scenario, body.feed, symbols=body.symbols or None, params=body.params,
                              duration_s=body.duration_s)
        rt.stopwatch_fault = f
        rt.audit(user.username, "chaos_inject", f"{body.scenario}@{f.feed or 'all'}", "ok")
        return next(a for a in rt.session.chaos.active_view(rt.session.now) if a["id"] == f.id) \
            if f.active else {"id": f.id, "scenario": f.scenario}

    @app.delete("/api/chaos/{fid}")
    async def chaos_clear_one(fid: str, user=Depends(need("chaos"))):
        ok = rt.session.clear(fid)
        rt.audit(user.username, "chaos_clear", fid, "ok" if ok else "not_found")
        return {"ok": ok}

    @app.post("/api/chaos/clear")
    async def chaos_clear(user=Depends(need("chaos"))):
        n = rt.session.clear_all()
        rt.audit(user.username, "chaos_clear_all", "", "ok", str(n))
        return {"ok": True, "cleared": n}

    @app.post("/api/control")
    async def control(body: ControlBody, user=Depends(need("control"))):
        if body.mode is not None and body.mode != "replay":
            raise HTTPException(400, "live mode is not enabled on this server")
        if body.speed is not None:
            if body.speed not in SPEEDS:
                raise HTTPException(400, f"speed must be one of {SPEEDS}")
            rt.speed = body.speed
        if body.paused is not None:
            rt.paused = body.paused
        if body.symbol is not None:
            if body.symbol not in rt.session.symbols:
                raise HTTPException(400, "unknown symbol")
            rt.chart_symbol = body.symbol
        if body.restart:
            rt.start_session()
        rt.audit(user.username, "control", "", "ok", body.model_dump_json(exclude_none=True))
        return {"ok": True}

    @app.post("/api/demo")
    async def demo(body: DemoBody, user=Depends(need("control"))):
        if rt.demo_task is not None and not rt.demo_task.done():
            rt.demo_task.cancel()
        if body.action == "start":
            rt.demo_task = asyncio.create_task(rt.demo_script())
        elif body.action != "stop":
            raise HTTPException(400, "action must be start or stop")
        rt.audit(user.username, "demo", body.action)
        return {"ok": True}

    # ---------------------------------------------------------------- chat & audit
    @app.post("/api/chat")
    async def chat_route(body: ChatBody, request: Request, user=Depends(need("chat"))):
        if not rt.chat_limit.check(user.username):
            raise HTTPException(429, "slow down: too many questions")
        rt.audit(user.username, "chat_question", "", "ok", body.message)
        return CHAT.answer(body.message, user, rt)

    @app.get("/api/audit")
    async def audit(limit: int = 100, user=Depends(need("audit"))):
        return rt.audit_log.entries(max(1, min(limit, 1000)))

    @app.get("/api/audit/verify")
    async def audit_verify(user=Depends(need("audit"))):
        return rt.audit_log.verify()

    # ---------------------------------------------------------------- websocket
    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        try:
            user, claims = rt.auth.verify(websocket.query_params.get("token", ""))
        except AuthError as exc:
            rt.audit("?", "ws_connect", "", "denied", str(exc))
            await websocket.close(code=4401)
            return
        seq, version = 0, None
        try:
            while True:
                if claims["exp"] <= time.time() or claims["jti"] in rt.auth.revoked_jti \
                        or claims.get("fam") in rt.auth.revoked_fams:
                    await websocket.close(code=4401)
                    return
                if version != rt.version:
                    version = rt.version
                    seq += 1
                    await websocket.send_text(json.dumps({**rt.hello(user), "seq": seq}))
                seq += 1
                await websocket.send_text(json.dumps({**rt.state_for(user), "seq": seq}))
                await asyncio.sleep(TICK_S)
        except (WebSocketDisconnect, RuntimeError):
            return
        except Exception:
            log.exception("websocket error")

    return app
