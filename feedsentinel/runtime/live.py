"""Live venue feed handlers: independent crypto venues play feeds A/B/C.

In live mode three genuinely independent public venues (Coinbase, Kraken, Gemini,
Bitstamp; whichever are reachable) act as the three feeds, so cross-feed consensus
has real independent sources. Each adapter turns one venue's public WebSocket into
the canonical message stream defined in :mod:`feedsentinel.schema`:

* ``Q`` quotes: top of book per symbol, emitted only when (bid, bid_sz, ask, ask_sz)
  changed, both sides are known and bid < ask.
* ``T`` trades: ``px, sz`` and ``side`` ("B" buyer-aggressor / "S" seller-aggressor)
  when the venue says who the aggressor was.
* ``H`` heartbeats: about once per wall-clock second while the connection is open and
  the venue has sent anything within the last 5 s; ``seq`` is the next sequence number
  (MoldUDP64 semantics). When a venue goes silent or disconnects, heartbeats stop.

Sequencing: every (re)connection is a new ``session`` (``"kraken-3"``) whose ``seq``
starts at 1 and increments by one per emitted Q/T. Where the venue itself reveals loss
(Gemini ``socket_sequence``), a venue gap of k messages advances ``seq`` by k too, so
the downstream sequencer sees the loss; the venue number is kept in ``vseq``.

Times: ``recv_ts = time.time_ns()`` stamped on receipt of each venue message;
``exch_ts`` is the venue's own event time in int ns. A venue message without a
timestamp uses ``recv_ts`` and carries ``"ts_src": "recv"``.

The parsing logic lives in small pure functions (``parse_kraken``, ``parse_gemini``,
``parse_bitstamp``, ``parse_coinbase``) operating on a :class:`FeedState`, so it can
be unit tested without the network. Faults are injected downstream (Chaos Lab); these
adapters are meant to be faithful and well behaved.

CLI::

    python -m feedsentinel.runtime.live --seconds 20
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import itertools
import json
import logging
import re
import statistics
import time
from typing import Any, Callable, Iterable, Iterator

from websockets.asyncio.client import connect as ws_connect

from feedsentinel.schema import HEARTBEAT, QUOTE, TRADE

log = logging.getLogger("feedsentinel.live")

Emit = Callable[[dict], None]

SYMBOLS: tuple[str, ...] = ("BTC-USD", "ETH-USD")

# canonical symbol -> venue symbol
VENUE_SYMBOLS: dict[str, dict[str, str]] = {
    "coinbase": {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"},
    "kraken": {"BTC-USD": "XBT/USD", "ETH-USD": "ETH/USD"},
    "gemini": {"BTC-USD": "BTCUSD", "ETH-USD": "ETHUSD"},
    "bitstamp": {"BTC-USD": "btcusd", "ETH-USD": "ethusd"},
}
# venue symbol -> canonical symbol
CANONICAL: dict[str, dict[str, str]] = {
    v: {vs: cs for cs, vs in m.items()} for v, m in VENUE_SYMBOLS.items()
}

HEARTBEAT_INTERVAL_S = 1.0   # emit an "H" about this often ...
ALIVE_WINDOW_S = 5.0         # ... while the venue sent anything within this window
IDLE_TIMEOUT_S = 15.0        # a socket silent this long is considered dead -> reconnect
BACKOFF_START_S = 1.0
BACKOFF_MAX_S = 15.0
HEALTHY_SESSION_S = 10.0     # a session that lasted this long resets the backoff


class VenueReconnect(Exception):
    """Raised by a parser when the venue asks the client to reconnect."""


# =========================================================================== timestamps
def secs_to_ns(value: str | float | int) -> int:
    """Decimal seconds ("1790401259.939649") -> int ns, exact for decimal strings."""
    s = value if isinstance(value, str) else (str(value) if isinstance(value, int) else f"{value:.9f}")
    s = s.strip()
    neg = s.startswith("-")
    if neg:
        s = s[1:]
    whole, _, frac = s.partition(".")
    ns = int(whole or 0) * 1_000_000_000 + int((frac + "000000000")[:9])
    return -ns if neg else ns


def ms_to_ns(value: str | int) -> int:
    """Integer milliseconds (Gemini ``timestampms``) -> int ns."""
    return int(value) * 1_000_000


def us_to_ns(value: str | int) -> int:
    """Integer microseconds (Bitstamp ``microtimestamp``) -> int ns."""
    return int(value) * 1_000


_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?"
    r"(Z|[+-]\d{2}:?\d{2})?$")


def iso_to_ns(value: str) -> int:
    """ISO-8601 UTC time ("2022-10-19T23:28:22.061769Z") -> int ns, exact (no floats)."""
    m = _ISO_RE.match(value.strip())
    if not m:
        raise ValueError(f"bad ISO timestamp: {value!r}")
    y, mo, d, h, mi, s, frac, tz = m.groups()
    secs = calendar.timegm((int(y), int(mo), int(d), int(h), int(mi), int(s), 0, 0, 0))
    if tz and tz != "Z":
        sign = 1 if tz[0] == "+" else -1
        tz = tz[1:].replace(":", "")
        secs -= sign * (int(tz[:2]) * 3600 + int(tz[2:]) * 60)
    return secs * 1_000_000_000 + int(((frac or "") + "000000000")[:9])


# =========================================================================== state
class FeedState:
    """Per-session sequencing and de-duplication state of one feed (no I/O).

    Builds complete canonical messages: parsers call :meth:`quote`, :meth:`trade`,
    :meth:`skip` and :meth:`venue_seq`; the runner calls :meth:`heartbeat`.
    """

    def __init__(self, feed: str, session: str):
        self.feed = feed
        self.session = session
        self.next_seq = 1                     # seq of the next Q/T message
        self.last_quote: dict[str, tuple] = {}  # sym -> last emitted (bid, bid_sz, ask, ask_sz)
        self.book: dict[str, list] = {}       # sym -> [bid, bid_sz, ask, ask_sz] being assembled
        self.vseq: dict[Any, int] = {}        # stream key -> last venue sequence number seen
        self.venue_lost = 0                   # venue messages reported lost (sum of gaps)

    def _msg(self, typ: str, seq: int, exch_ts: int | None, recv_ts: int, **payload) -> dict:
        msg = {"feed": self.feed, "session": self.session, "seq": seq, "type": typ,
               "exch_ts": int(exch_ts if exch_ts is not None else recv_ts),
               "recv_ts": int(recv_ts)}
        msg.update(payload)
        if exch_ts is None:
            msg["ts_src"] = "recv"
        return msg

    def _take_seq(self) -> int:
        seq = self.next_seq
        self.next_seq += 1
        return seq

    def quote(self, sym: str, bid, bid_sz, ask, ask_sz, exch_ts: int | None, recv_ts: int,
              **extra) -> dict | None:
        """A "Q" message, or None if a side is unknown, the book is crossed/locked or
        nothing changed since the last emitted quote for ``sym``."""
        if bid is None or ask is None or bid_sz is None or ask_sz is None:
            return None
        bid, bid_sz, ask, ask_sz = float(bid), float(bid_sz), float(ask), float(ask_sz)
        if not (0.0 < bid < ask) or not (bid_sz >= 0.0 and ask_sz >= 0.0):
            return None
        tup = (bid, bid_sz, ask, ask_sz)
        if self.last_quote.get(sym) == tup:
            return None
        self.last_quote[sym] = tup
        return self._msg(QUOTE, self._take_seq(), exch_ts, recv_ts, sym=sym, bid=bid,
                         bid_sz=bid_sz, ask=ask, ask_sz=ask_sz, **extra)

    def trade(self, sym: str, px, sz, side: str | None, exch_ts: int | None, recv_ts: int,
              **extra) -> dict | None:
        """A "T" message (``side`` "B"/"S" is omitted when unknown)."""
        px, sz = float(px), float(sz)
        if not (px > 0.0 and sz > 0.0):
            return None
        payload: dict[str, Any] = {"sym": sym, "px": px, "sz": sz}
        if side in ("B", "S"):
            payload["side"] = side
        return self._msg(TRADE, self._take_seq(), exch_ts, recv_ts, **payload, **extra)

    def heartbeat(self, now_ns: int | None = None) -> dict:
        """An "H" message: seq is the next sequence number, exch_ts == recv_ts == now."""
        now = time.time_ns() if now_ns is None else int(now_ns)
        return self._msg(HEARTBEAT, self.next_seq, now, now)

    def skip(self, k: int) -> None:
        """Advance seq by k: the venue told us k messages were lost."""
        if k > 0:
            self.next_seq += k
            self.venue_lost += k

    def venue_seq(self, key: Any, vseq: int) -> int:
        """Track a per-connection venue sequence (first expected value 0). Returns the
        number of venue messages lost before ``vseq`` and advances our seq by as much."""
        last = self.vseq.get(key, -1)
        if vseq <= last:            # duplicate / replay: nothing lost, keep the high mark
            return 0
        gap = vseq - last - 1
        self.vseq[key] = vseq
        self.skip(gap)
        return gap


def _loads(raw: str | bytes | Any) -> Any:
    return json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw


def _is_error(msg: Any) -> bool:
    return isinstance(msg, dict) and (
        msg.get("type") == "error" or msg.get("event") in ("error", "bts:error")
        or msg.get("status") == "error" or msg.get("result") == "error")


# =========================================================================== parsers
_KRAKEN_SIDE = {"b": "B", "s": "S"}


def parse_kraken(raw, state: FeedState, recv_ts: int) -> list[dict]:
    """Kraken v1: ``spread`` -> Q, ``trade`` -> T. Events (systemStatus,
    subscriptionStatus, heartbeat) produce nothing.

    spread: ``[chanID, [bid, ask, ts, bid_vol, ask_vol], "spread", "XBT/USD"]``
    trade:  ``[chanID, [[px, vol, ts, "b"|"s", "l"|"m", misc], ...], "trade", "XBT/USD"]``
    """
    m = _loads(raw)
    if isinstance(m, dict):
        if m.get("event") == "subscriptionStatus" and m.get("status") == "error":
            log.warning("kraken: subscription error: %s", m.get("errorMessage"))
        elif m.get("event") == "systemStatus" and m.get("status") != "online":
            log.warning("kraken: system status %s", m.get("status"))
        return []
    if not isinstance(m, list) or len(m) < 4:
        return []
    channel, sym = m[-2], CANONICAL["kraken"].get(m[-1])
    if sym is None:
        return []
    out: list[dict] = []
    if channel == "spread":
        bid, ask, ts, bid_sz, ask_sz = m[1][:5]
        q = state.quote(sym, bid, bid_sz, ask, ask_sz, secs_to_ns(ts), recv_ts)
        if q:
            out.append(q)
    elif channel == "trade":
        for t in m[1]:
            tr = state.trade(sym, t[0], t[1], _KRAKEN_SIDE.get(t[3]), secs_to_ns(t[2]), recv_ts)
            if tr:
                out.append(tr)
    return out


_GEMINI_AGGRESSOR = {"bid": "S", "ask": "B"}   # makerSide -> aggressor side


def parse_gemini(raw, state: FeedState, recv_ts: int, sym: str) -> list[dict]:
    """Gemini v1 market data (one socket per symbol, ``top_of_book=true``).

    ``change`` events carry the new best price and its total size for one side; the
    latest bid and ask are kept per symbol and a quote is emitted when either changes.
    ``trade`` events -> T (``makerSide`` "bid" means a seller hit the bid -> "S").
    ``socket_sequence`` (0, 1, 2, ... on every message, heartbeats included) gaps
    advance seq by the number of lost venue messages; it is kept as ``vseq``.
    """
    m = _loads(raw)
    if not isinstance(m, dict):
        return []
    vseq = m.get("socket_sequence")
    if isinstance(vseq, int):
        state.venue_seq(sym, vseq)
    if m.get("type") != "update":
        return []
    ts = m.get("timestampms")
    exch_ts = ms_to_ns(ts) if ts is not None else None   # the "initial" snapshot has none
    extra = {"vseq": vseq} if isinstance(vseq, int) else {}
    book = state.book.setdefault(sym, [None, None, None, None])
    out: list[dict] = []
    changed = False
    for ev in m.get("events") or ():
        et = ev.get("type")
        if et == "change":
            side = ev.get("side")
            px, rem = float(ev["price"]), float(ev["remaining"])
            level = (px, rem) if rem > 0 else (None, None)   # side emptied -> unknown
            if side == "bid":
                book[0], book[1] = level
                changed = True
            elif side == "ask":
                book[2], book[3] = level
                changed = True
        elif et == "trade":
            tr = state.trade(sym, ev["price"], ev["amount"],
                             _GEMINI_AGGRESSOR.get(ev.get("makerSide")), exch_ts, recv_ts, **extra)
            if tr:
                out.append(tr)
    if changed:
        q = state.quote(sym, *book, exch_ts, recv_ts, **extra)
        if q:
            out.append(q)
    return out


_BITSTAMP_SIDE = {0: "B", 1: "S"}   # live_trades "type": 0 buy, 1 sell (aggressor)


def _bitstamp_ts(d: dict) -> int | None:
    if d.get("microtimestamp") is not None:
        return us_to_ns(d["microtimestamp"])
    if d.get("timestamp") is not None:
        return secs_to_ns(str(d["timestamp"]))
    return None


def parse_bitstamp(raw, state: FeedState, recv_ts: int) -> list[dict]:
    """Bitstamp: ``order_book_{pair}`` snapshot (level 0 of bids/asks) -> Q,
    ``live_trades_{pair}`` -> T. Raises :class:`VenueReconnect` on
    ``bts:request_reconnect``."""
    m = _loads(raw)
    if not isinstance(m, dict):
        return []
    event, channel = m.get("event"), m.get("channel") or ""
    if event == "bts:request_reconnect":
        raise VenueReconnect("bitstamp requested reconnect")
    if event == "bts:error":
        log.warning("bitstamp: error: %s", m.get("data"))
        return []
    d = m.get("data")
    if not isinstance(d, dict):
        return []
    if event == "data" and channel.startswith("order_book_"):
        sym = CANONICAL["bitstamp"].get(channel[len("order_book_"):])
        bids, asks = d.get("bids"), d.get("asks")
        if sym is None or not bids or not asks:
            return []
        q = state.quote(sym, bids[0][0], bids[0][1], asks[0][0], asks[0][1], _bitstamp_ts(d), recv_ts)
        return [q] if q else []
    if event == "trade" and channel.startswith("live_trades_"):
        sym = CANONICAL["bitstamp"].get(channel[len("live_trades_"):])
        if sym is None:
            return []
        tr = state.trade(sym, d.get("price_str", d.get("price")), d.get("amount_str", d.get("amount")),
                         _BITSTAMP_SIDE.get(d.get("type")), _bitstamp_ts(d), recv_ts)
        return [tr] if tr else []
    return []


_COINBASE_MAKER_TO_AGGRESSOR = {"buy": "S", "sell": "B"}   # match "side" is the maker side


def parse_coinbase(raw, state: FeedState, recv_ts: int) -> list[dict]:
    """Coinbase Exchange: ``ticker`` -> Q (best_bid/best_ask and sizes), ``match`` ->
    T (``side`` is the maker side, so the aggressor is the opposite). ``last_match``
    (historical snapshot sent on subscribe), ``heartbeat`` and ``subscriptions``
    produce nothing. Ticker ``sequence`` gaps are normal (ticker messages are
    droppable/batched) and are deliberately not mapped to seq gaps."""
    m = _loads(raw)
    if not isinstance(m, dict):
        return []
    typ = m.get("type")
    if typ == "error":
        log.warning("coinbase: error: %s %s", m.get("message"), m.get("reason", ""))
        return []
    sym = CANONICAL["coinbase"].get(m.get("product_id"))
    if sym is None:
        return []
    exch_ts = iso_to_ns(m["time"]) if m.get("time") else None
    if typ == "ticker":
        if m.get("best_bid_size") is None or m.get("best_ask_size") is None:
            return []
        q = state.quote(sym, m.get("best_bid"), m.get("best_bid_size"), m.get("best_ask"),
                        m.get("best_ask_size"), exch_ts, recv_ts)
        return [q] if q else []
    if typ == "match":
        tr = state.trade(sym, m["price"], m["size"],
                         _COINBASE_MAKER_TO_AGGRESSOR.get(m.get("side")), exch_ts, recv_ts)
        return [tr] if tr else []
    return []


# =========================================================================== adapters
_SESSION_COUNTERS: dict[str, Iterator[int]] = {}


def _new_session(venue: str) -> str:
    """Process-unique session id per venue: "kraken-1", "kraken-2", ..."""
    counter = _SESSION_COUNTERS.setdefault(venue, itertools.count(1))
    return f"{venue}-{next(counter)}"


def _short(e: BaseException) -> str:
    text = str(e)
    return f"{type(e).__name__}: {text}" if text else type(e).__name__


class VenueFeed:
    """Base class: connection lifecycle, heartbeats, reconnect/backoff.

    Subclasses define ``venue``, ``display_name``, :meth:`streams`,
    :meth:`subscriptions` and :meth:`parse` (and optionally :meth:`keepalive`).
    """

    venue: str = ""
    display_name: str = ""
    heartbeat_interval: float = HEARTBEAT_INTERVAL_S
    alive_window: float = ALIVE_WINDOW_S
    idle_timeout: float = IDLE_TIMEOUT_S
    backoff_start: float = BACKOFF_START_S
    backoff_max: float = BACKOFF_MAX_S
    keepalive_interval: float | None = None   # app-level ping to the venue, if any

    def __init__(self, feed_id: str, symbols: Iterable[str] = SYMBOLS):
        self.feed_id = feed_id
        self.symbols = tuple(symbols)
        unknown = [s for s in self.symbols if s not in VENUE_SYMBOLS[self.venue]]
        if unknown:
            raise ValueError(f"{self.venue}: unsupported symbols {unknown}")
        self.connected = False
        self.session: str | None = None
        self.sessions = 0
        self.parse_errors = 0
        self.last_error: str | None = None

    # ---------------------------------------------------------------- venue specifics
    def streams(self) -> list[tuple[str, Any]]:
        """(url, key) per socket; ``key`` is passed back to :meth:`parse`."""
        raise NotImplementedError

    def subscriptions(self, key: Any) -> list[dict]:
        """JSON messages to send right after a socket opens."""
        return []

    def parse(self, raw, state: FeedState, recv_ts: int, key: Any) -> list[dict]:
        raise NotImplementedError

    def keepalive(self) -> dict | None:
        return None

    def vsym(self, sym: str) -> str:
        return VENUE_SYMBOLS[self.venue][sym]

    # ---------------------------------------------------------------- metadata
    def info(self) -> dict:
        return {"id": self.feed_id, "name": self.display_name,
                "role": f"Live venue feed ({self.display_name} public WebSocket)"}

    def status(self) -> dict:
        return {"id": self.feed_id, "venue": self.venue, "connected": self.connected,
                "session": self.session, "sessions": self.sessions,
                "parse_errors": self.parse_errors, "last_error": self.last_error}

    def __repr__(self) -> str:
        return f"{type(self).__name__}(feed_id={self.feed_id!r}, symbols={self.symbols!r})"

    # ---------------------------------------------------------------- lifecycle
    @staticmethod
    async def _open(url: str):
        return await ws_connect(url, open_timeout=10, close_timeout=2, ping_interval=20,
                                ping_timeout=20, max_size=2 ** 22, max_queue=256)

    async def run(self, emit: Emit, stop: asyncio.Event) -> None:
        """Stream canonical messages to ``emit`` until ``stop`` is set, reconnecting with
        exponential backoff. Never raises on network errors."""
        loop = asyncio.get_running_loop()
        delay = self.backoff_start
        while not stop.is_set():
            t0 = loop.time()
            try:
                reason = await self._session(emit, stop)
            except asyncio.CancelledError:
                raise
            except Exception as e:   # network errors and anything unexpected: reconnect
                reason = _short(e)
            if stop.is_set():
                break
            self.last_error = reason
            if loop.time() - t0 >= HEALTHY_SESSION_S:
                delay = self.backoff_start
            log.warning("%s (feed %s): %s; reconnecting in %.0f s",
                        self.display_name, self.feed_id, reason, delay)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass
            delay = min(delay * 2, self.backoff_max)
        log.info("%s (feed %s): stopped", self.display_name, self.feed_id)

    async def _session(self, emit: Emit, stop: asyncio.Event) -> str:
        """One connection (group of sockets). Returns why it ended."""
        streams = self.streams()
        opened = await asyncio.gather(*(self._open(url) for url, _ in streams),
                                      return_exceptions=True)
        sockets = [ws for ws in opened if not isinstance(ws, BaseException)]
        tasks: list[asyncio.Task] = []
        try:
            for ws in opened:
                if isinstance(ws, BaseException):
                    raise ws
            for ws, (_, key) in zip(sockets, streams):
                for sub in self.subscriptions(key):
                    await ws.send(json.dumps(sub))
            state = FeedState(self.feed_id, _new_session(self.venue))
            self.session, self.sessions, self.connected = state.session, self.sessions + 1, True
            log.info("%s (feed %s): connected, session %s", self.display_name, self.feed_id,
                     state.session)
            loop = asyncio.get_running_loop()
            now = loop.time()
            rx = {"any": None, **{key: now for _, key in streams}}
            tasks = [asyncio.create_task(self._reader(ws, key, state, emit, rx))
                     for ws, (_, key) in zip(sockets, streams)]
            tasks.append(asyncio.create_task(self._ticker(state, emit, stop, rx, sockets)))
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            first = done.pop()
            if first.exception() is not None:
                return _short(first.exception())
            return first.result() or "connection closed"
        finally:
            self.connected = False
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(*(ws.close() for ws in sockets), return_exceptions=True)

    async def _reader(self, ws, key, state: FeedState, emit: Emit, rx: dict) -> str:
        loop = asyncio.get_running_loop()
        warned = 0
        async for raw in ws:
            recv_ts = time.time_ns()
            rx["any"] = rx[key] = loop.time()
            lost_before = state.venue_lost
            try:
                msgs = self.parse(raw, state, recv_ts, key)
            except VenueReconnect as e:
                return str(e)
            except Exception as e:   # garbled venue message: skip it, keep the feed alive
                self.parse_errors += 1
                if warned < 3:
                    warned += 1
                    log.warning("%s (feed %s): unparseable message (%s): %.200s",
                                self.display_name, self.feed_id, _short(e), raw)
                continue
            if state.venue_lost != lost_before:
                log.warning("%s (feed %s): venue sequence gap of %d on %s",
                            self.display_name, self.feed_id, state.venue_lost - lost_before, key)
            for msg in msgs:
                self._emit(emit, msg)
        return f"closed by venue (code {ws.close_code})"

    async def _ticker(self, state: FeedState, emit: Emit, stop: asyncio.Event, rx: dict,
                      sockets: list) -> str | None:
        """Heartbeats, idle watchdog and venue keepalives; returns on stop."""
        loop = asyncio.get_running_loop()
        next_hb = loop.time() + self.heartbeat_interval
        next_ka = loop.time() + (self.keepalive_interval or 0.0)
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0.0, next_hb - loop.time()))
                return "stopped"
            except TimeoutError:
                pass
            now = loop.time()
            for key, t in rx.items():
                if key != "any" and now - t > self.idle_timeout:
                    return f"no data from {key} for {now - t:.0f} s"
            if rx["any"] is not None and now - rx["any"] <= self.alive_window:
                self._emit(emit, state.heartbeat(time.time_ns()))
            ka = self.keepalive() if self.keepalive_interval else None
            if ka is not None and now >= next_ka:
                next_ka = now + self.keepalive_interval
                for ws in sockets:
                    await ws.send(json.dumps(ka))
            next_hb += self.heartbeat_interval
            if next_hb <= now:   # fell behind (e.g. event loop stall): don't burst
                next_hb = now + self.heartbeat_interval

    def _emit(self, emit: Emit, msg: dict) -> None:
        try:
            emit(msg)
        except Exception:
            log.exception("%s (feed %s): emit callback failed", self.display_name, self.feed_id)

    # ---------------------------------------------------------------- probing
    async def probe(self, timeout: float = 5.0) -> tuple[bool, str]:
        """Connect the first stream, subscribe, and wait for a first message."""
        url, key = self.streams()[0]
        t0 = time.perf_counter()
        ws = None
        try:
            async with asyncio.timeout(timeout):
                ws = await self._open(url)
                for sub in self.subscriptions(key):
                    await ws.send(json.dumps(sub))
                raw = await ws.recv()
            try:
                first = json.loads(raw)
            except ValueError:
                first = None
            if _is_error(first):
                return False, f"venue error: {str(raw)[:120]}"
            return True, f"first message after {(time.perf_counter() - t0) * 1e3:.0f} ms"
        except TimeoutError:
            return False, f"no message within {timeout:.0f} s"
        except Exception as e:
            return False, _short(e)
        finally:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass


class KrakenFeed(VenueFeed):
    """Kraken v1 public WebSocket: ``spread`` (top of book) and ``trade`` channels.
    Kraken sends ``{"event": "heartbeat"}`` after 1 s without other traffic."""

    venue = "kraken"
    display_name = "Kraken"
    url = "wss://ws.kraken.com"

    def streams(self):
        return [(self.url, "kraken")]

    def subscriptions(self, key):
        pairs = [self.vsym(s) for s in self.symbols]
        return [{"event": "subscribe", "pair": pairs, "subscription": {"name": "spread"}},
                {"event": "subscribe", "pair": pairs, "subscription": {"name": "trade"}}]

    def parse(self, raw, state, recv_ts, key):
        return parse_kraken(raw, state, recv_ts)


class GeminiFeed(VenueFeed):
    """Gemini v1 market data: one socket per symbol with ``top_of_book=true``,
    trades and heartbeats (every 5 s). All sockets form one session; if any drops,
    the whole group reconnects as a new session."""

    venue = "gemini"
    display_name = "Gemini"
    url_template = ("wss://api.gemini.com/v1/marketdata/{sym}?top_of_book=true&heartbeat=true"
                    "&trades=true&bids=true&offers=true&auctions=false")

    def streams(self):
        return [(self.url_template.format(sym=self.vsym(s)), s) for s in self.symbols]

    def parse(self, raw, state, recv_ts, key):
        return parse_gemini(raw, state, recv_ts, key)


class BitstampFeed(VenueFeed):
    """Bitstamp v2 WebSocket: ``order_book_{pair}`` (top 100 snapshots, ~10/s) and
    ``live_trades_{pair}``. Bitstamp has no server heartbeat, so a ``bts:heartbeat``
    request is sent every 2.5 s; its reply shows the connection is alive."""

    venue = "bitstamp"
    display_name = "Bitstamp"
    url = "wss://ws.bitstamp.net"
    keepalive_interval = 2.5

    def streams(self):
        return [(self.url, "bitstamp")]

    def subscriptions(self, key):
        subs = []
        for s in self.symbols:
            p = self.vsym(s)
            subs.append({"event": "bts:subscribe", "data": {"channel": f"order_book_{p}"}})
            subs.append({"event": "bts:subscribe", "data": {"channel": f"live_trades_{p}"}})
        return subs

    def keepalive(self):
        return {"event": "bts:heartbeat"}

    def parse(self, raw, state, recv_ts, key):
        return parse_bitstamp(raw, state, recv_ts)


class CoinbaseFeed(VenueFeed):
    """Coinbase Exchange public feed: ``ticker`` (quotes), ``matches`` (trades) and
    ``heartbeat`` (1/s per product) channels."""

    venue = "coinbase"
    display_name = "Coinbase"
    url = "wss://ws-feed.exchange.coinbase.com"

    def streams(self):
        return [(self.url, "coinbase")]

    def subscriptions(self, key):
        return [{"type": "subscribe", "product_ids": [self.vsym(s) for s in self.symbols],
                 "channels": ["heartbeat", "ticker", "matches"]}]

    def parse(self, raw, state, recv_ts, key):
        return parse_coinbase(raw, state, recv_ts)


VENUES: dict[str, type[VenueFeed]] = {
    "coinbase": CoinbaseFeed,
    "kraken": KrakenFeed,
    "gemini": GeminiFeed,
    "bitstamp": BitstampFeed,
}
PREFERRED_VENUES = ("coinbase", "kraken", "gemini", "bitstamp")


# =========================================================================== selection
async def _probe(venue: str, timeout: float) -> tuple[bool, str]:
    cls = VENUES.get(venue)
    if cls is None:
        return False, "unknown venue"
    return await cls("?", SYMBOLS).probe(timeout)


async def probe(venue: str, timeout: float = 5.0) -> bool:
    """Can we connect to ``venue`` and receive a first message within ``timeout``?"""
    ok, _ = await _probe(venue, timeout)
    return ok


async def select_venues(preferred: Iterable[str] = PREFERRED_VENUES, n: int = 3,
                        timeout: float = 5.0) -> list[str]:
    """Probe ``preferred`` venues concurrently; return the first ``n`` reachable ones in
    preference order (fewer if not enough are reachable)."""
    preferred = list(preferred)
    results = await asyncio.gather(*(_probe(v, timeout) for v in preferred))
    chosen: list[str] = []
    for venue, (ok, why) in zip(preferred, results):
        if ok and len(chosen) < n:
            chosen.append(venue)
            log.info("venue %s: reachable (%s)", venue, why)
        elif ok:
            log.info("venue %s: reachable, not needed", venue)
        else:
            log.warning("venue %s: skipped (%s)", venue, why)
    if len(chosen) < n:
        log.warning("only %d of %d live venues reachable: %s", len(chosen), n, chosen)
    return chosen


def build_feeds(venues: Iterable[str], symbols: Iterable[str] = SYMBOLS) -> list[VenueFeed]:
    """One adapter per venue, feed ids "A", "B", "C", ... in order."""
    symbols = tuple(symbols)
    return [VENUES[v](chr(ord("A") + i), symbols) for i, v in enumerate(venues)]


def feed_info(feeds: Iterable[VenueFeed]) -> list[dict]:
    """``FEED_INFO``-style metadata for the UI."""
    return [f.info() for f in feeds]


# =========================================================================== CLI
class _Stats:
    """Per-feed counters for the CLI, including a MoldUDP64-style gap check."""

    def __init__(self, feed: VenueFeed):
        self.feed = feed
        self.counts = {QUOTE: 0, TRADE: 0, HEARTBEAT: 0}
        self.sessions: list[str] = []
        self.expected: int | None = None    # next seq expected in the current session
        self.gap_events = 0
        self.gap_msgs = 0
        self.recv_ts_src = 0
        self.lat_ms: list[float] = []
        self.last_quote: dict[str, tuple[float, float]] = {}

    def add(self, m: dict) -> None:
        typ, seq = m["type"], m["seq"]
        self.counts[typ] = self.counts.get(typ, 0) + 1
        if not self.sessions or self.sessions[-1] != m["session"]:
            self.sessions.append(m["session"])
            self.expected = 1
        if seq > self.expected:
            self.gap_events += 1
            self.gap_msgs += seq - self.expected
        if typ == HEARTBEAT:
            self.expected = max(self.expected, seq)
        else:
            self.expected = max(self.expected, seq + 1)
            if m.get("ts_src") == "recv":
                self.recv_ts_src += 1
            else:
                self.lat_ms.append((m["recv_ts"] - m["exch_ts"]) / 1e6)
        if typ == QUOTE:
            self.last_quote[m["sym"]] = (m["bid"], m["ask"])


async def _cli(args) -> int:
    symbols = tuple(s.strip() for s in args.symbols.split(",") if s.strip())
    if args.venues:
        venues = [v.strip() for v in args.venues.split(",") if v.strip()]
    else:
        venues = await select_venues(n=args.n, timeout=args.probe_timeout)
    if not venues:
        print("no live venue reachable")
        return 1
    feeds = build_feeds(venues, symbols)
    stats = {f.feed_id: _Stats(f) for f in feeds}
    sink = open(args.jsonl, "w", encoding="utf-8") if args.jsonl else None

    def emit(msg: dict) -> None:
        stats[msg["feed"]].add(msg)
        if sink:
            sink.write(json.dumps(msg) + "\n")

    for f in feeds:
        print(f"feed {f.feed_id}: {f.display_name:<9} {f.info()['role']}", flush=True)
    stop = asyncio.Event()
    tasks = [asyncio.create_task(f.run(emit, stop)) for f in feeds]
    try:
        await asyncio.sleep(args.seconds)
    finally:
        stop.set()
        await asyncio.wait(tasks, timeout=5)
        if sink:
            sink.close()

    print(f"\n{args.seconds:.0f} s of live data, symbols {', '.join(symbols)}")
    head = (f"{'feed':<4} {'venue':<9} {'sess':>4} {'quotes':>6} {'trades':>6} {'hb':>4} "
            f"{'gaps(msgs)':>10} {'ts=recv':>7} {'lat_med_ms':>10}  last bid / ask")
    print(head)
    print("-" * len(head) + "-" * 30)
    medians = []
    for fid, st in stats.items():
        med = statistics.median(st.lat_ms) if st.lat_ms else None
        if med is not None:
            medians.append(med)
        books = "  ".join(f"{s} {b:.2f}/{a:.2f}" for s, (b, a) in sorted(st.last_quote.items()))
        print(f"{fid:<4} {st.feed.display_name:<9} {len(st.sessions):>4} {st.counts[QUOTE]:>6} "
              f"{st.counts[TRADE]:>6} {st.counts[HEARTBEAT]:>4} "
              f"{f'{st.gap_events}({st.gap_msgs})':>10} {st.recv_ts_src:>7} "
              f"{(f'{med:.1f}' if med is not None else '-'):>10}  {books}")
    if medians and max(medians) < -250:
        print("\nnote: every venue's timestamps are ahead of the local clock, so the local "
              "clock is probably behind. Sync it (NTP) for meaningful latency numbers.")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m feedsentinel.runtime.live",
                                 description="Run the live venue feeds and print per-feed stats.")
    ap.add_argument("--seconds", type=float, default=20.0, help="how long to run (default 20)")
    ap.add_argument("--venues", default="", help="comma list (skips probing), e.g. kraken,gemini")
    ap.add_argument("--symbols", default=",".join(SYMBOLS))
    ap.add_argument("-n", type=int, default=3, help="number of venues to select (default 3)")
    ap.add_argument("--probe-timeout", type=float, default=5.0)
    ap.add_argument("--jsonl", default="", help="also write every emitted message to this file")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    if not args.verbose:
        logging.getLogger("websockets").setLevel(logging.WARNING)
    try:
        return asyncio.run(_cli(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
