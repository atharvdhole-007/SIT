"""LOBSTER sample data -> one merged, time-ordered tape of top-of-book quotes and trades.

LOBSTER reconstructs the Nasdaq order book from TotalView-ITCH. Each symbol has two files with one
row per event:

message file   Time (s after midnight, ns precision), Event Type, Order ID, Size, Price, Direction
orderbook file Ask Price 1, Ask Size 1, Bid Price 1, Bid Size 1  (book state *after* the event)

Event types: 1 submit, 2 partial cancel, 3 delete, 4 execution (visible), 5 execution (hidden),
6 cross trade, 7 trading halt (price -1 = halt, 0 = quote resume, 1 = trading resume).
Prices are integers x 10,000 (ITCH Price(4)). Empty book sides use dummy prices (+/-9999999999);
those are "no quote", not garbage.

The tape emits a quote whenever the top of book changes and a trade for every execution, so each
downstream feed carries the level-1 view a consolidated feed would.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from ..config import PROCESSED_DIR, RAW_DIR
from ..timeutil import NS, midnight_ns

log = logging.getLogger("feedsentinel.lobster")

SYMBOLS = ("AAPL", "AMZN", "GOOG", "INTC", "MSFT")
TRADING_DAY = date(2012, 6, 21)
FILE_FMT = "{sym}_2012-06-21_34200000_57600000_{kind}_1.csv"
HF_URL = ("https://huggingface.co/datasets/totalorganfailure/lobster-data/resolve/main/"
          "LOBSTER_SampleFile_{sym}_2012-06-21_1/{file}")
SOURCE_LABEL = "Nasdaq TotalView-ITCH via LOBSTER · 21 Jun 2012"

KIND_QUOTE, KIND_TRADE, KIND_STATUS = 0, 1, 2
DUMMY_ASK = 9_999_999_999
PRICE_SCALE = 10_000  # Price(4)
CACHE_VERSION = 2


@dataclass
class Tape:
    """Columnar event tape, sorted by exchange time. Prices stay as Price(4) integers."""
    symbols: tuple[str, ...]
    ts: np.ndarray        # int64 epoch ns
    sym: np.ndarray       # int8 index into symbols
    kind: np.ndarray      # int8 KIND_*
    bid: np.ndarray       # int64 Price(4) (quotes)
    bid_sz: np.ndarray    # int64
    ask: np.ndarray       # int64 Price(4)
    ask_sz: np.ndarray    # int64
    px: np.ndarray        # int64 Price(4) (trades); halt code for status rows
    sz: np.ndarray        # int64
    side: np.ndarray      # int8: +1 buyer-initiated, -1 seller-initiated, 0 n/a
    day: date = TRADING_DAY

    def __len__(self) -> int:
        return len(self.ts)

    @property
    def start_ns(self) -> int:
        return int(self.ts[0])

    @property
    def end_ns(self) -> int:
        return int(self.ts[-1])

    def index_at(self, t_ns: int) -> int:
        """First event index with ts >= t_ns."""
        return int(np.searchsorted(self.ts, t_ns, side="left"))

    def stats(self) -> dict:
        out = {"events": len(self), "quotes": int((self.kind == KIND_QUOTE).sum()),
               "trades": int((self.kind == KIND_TRADE).sum()),
               "halts": int((self.kind == KIND_STATUS).sum())}
        for i, s in enumerate(self.symbols):
            out[s] = int((self.sym == i).sum())
        return out


def raw_paths(sym: str, raw_dir: Path = RAW_DIR) -> tuple[Path, Path]:
    return (raw_dir / FILE_FMT.format(sym=sym, kind="message"),
            raw_dir / FILE_FMT.format(sym=sym, kind="orderbook"))


def _load_symbol(sym: str, idx: int, day0_ns: int, raw_dir: Path) -> dict[str, np.ndarray]:
    mpath, opath = raw_paths(sym, raw_dir)
    msg = pd.read_csv(mpath, header=None, usecols=range(6),
                      names=["t", "type", "oid", "size", "price", "dir"],
                      dtype={"t": "float64", "type": "int64", "oid": "int64", "size": "int64",
                             "price": "int64", "dir": "int64"})
    ob = pd.read_csv(opath, header=None, usecols=range(4),
                     names=["ask", "ask_sz", "bid", "bid_sz"], dtype="int64")
    if len(msg) != len(ob):
        raise ValueError(f"{sym}: message ({len(msg)}) and orderbook ({len(ob)}) row counts differ")

    t = day0_ns + np.round(msg["t"].to_numpy() * NS).astype(np.int64)
    etype = msg["type"].to_numpy()
    ask = ob["ask"].to_numpy()
    bid = ob["bid"].to_numpy()
    ask_sz = ob["ask_sz"].to_numpy()
    bid_sz = ob["bid_sz"].to_numpy()

    # A quote row wherever the top of book differs from the previous row. Rows where a side is
    # empty (dummy price) carry no valid two-sided quote, so they are not emitted as quotes.
    book = np.stack([ask, ask_sz, bid, bid_sz], axis=1)
    changed = np.ones(len(book), dtype=bool)
    changed[1:] = (book[1:] != book[:-1]).any(axis=1)
    two_sided = (ask < DUMMY_ASK) & (bid > -DUMMY_ASK) & (ask > 0) & (bid > 0)
    is_quote = changed & two_sided
    is_trade = np.isin(etype, (4, 5))
    is_status = etype == 7

    n = len(msg)
    # Order within one LOBSTER row: trade (if any) first, then the resulting quote.
    rows = []
    order_key = np.arange(n, dtype=np.int64) * 3
    for mask, kind, sub in ((is_status, KIND_STATUS, 0), (is_trade, KIND_TRADE, 1),
                            (is_quote, KIND_QUOTE, 2)):
        k = np.flatnonzero(mask)
        if len(k) == 0:
            continue
        rows.append(dict(
            ts=t[k], order=order_key[k] + sub, kind=np.full(len(k), kind, np.int8),
            bid=np.where(kind == KIND_QUOTE, bid[k], 0), ask=np.where(kind == KIND_QUOTE, ask[k], 0),
            bid_sz=np.where(kind == KIND_QUOTE, bid_sz[k], 0),
            ask_sz=np.where(kind == KIND_QUOTE, ask_sz[k], 0),
            px=msg["price"].to_numpy()[k] if kind != KIND_QUOTE else np.zeros(len(k), np.int64),
            sz=msg["size"].to_numpy()[k] if kind == KIND_TRADE else np.zeros(len(k), np.int64),
            # Direction -1: a sell limit order was executed, i.e. the buyer initiated the trade.
            side=np.where(kind == KIND_TRADE, -msg["dir"].to_numpy()[k], 0).astype(np.int8),
        ))
    cols = {c: np.concatenate([r[c] for r in rows]) for c in rows[0]}
    o = np.lexsort((cols["order"], cols["ts"]))
    out = {c: v[o] for c, v in cols.items() if c != "order"}
    out["sym"] = np.full(len(o), idx, np.int8)
    log.info("%s: %d LOBSTER rows -> %d quotes, %d trades, %d status", sym, n,
             int(is_quote.sum()), int(is_trade.sum()), int(is_status.sum()))
    return out


def build_tape(symbols=SYMBOLS, raw_dir: Path = RAW_DIR) -> Tape:
    day0 = midnight_ns(TRADING_DAY)
    parts = [_load_symbol(s, i, day0, raw_dir) for i, s in enumerate(symbols)]
    cols = {c: np.concatenate([p[c] for p in parts]) for c in parts[0]}
    # Stable merge by time; ties keep symbol order, and within a symbol the per-row order.
    o = np.argsort(cols["ts"], kind="stable")
    cols = {c: v[o] for c, v in cols.items()}
    return Tape(symbols=tuple(symbols), ts=cols["ts"].astype(np.int64), sym=cols["sym"],
                kind=cols["kind"], bid=cols["bid"].astype(np.int64),
                bid_sz=cols["bid_sz"].astype(np.int64), ask=cols["ask"].astype(np.int64),
                ask_sz=cols["ask_sz"].astype(np.int64), px=cols["px"].astype(np.int64),
                sz=cols["sz"].astype(np.int64), side=cols["side"])


def cache_path(symbols=SYMBOLS) -> Path:
    return PROCESSED_DIR / f"tape_{TRADING_DAY.isoformat()}_{'-'.join(symbols)}_v{CACHE_VERSION}.npz"


def load_tape(symbols=SYMBOLS, raw_dir: Path = RAW_DIR, use_cache: bool = True) -> Tape:
    """Load the merged tape, building and caching it on first use."""
    path = cache_path(symbols)
    if use_cache and path.exists():
        z = np.load(path)
        return Tape(symbols=tuple(symbols), **{k: z[k] for k in
                    ("ts", "sym", "kind", "bid", "bid_sz", "ask", "ask_sz", "px", "sz", "side")})
    missing = [p for s in symbols for p in raw_paths(s, raw_dir) if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "LOBSTER files missing: " + ", ".join(p.name for p in missing)
            + ". Run: python -m feedsentinel download")
    tape = build_tape(symbols, raw_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(path, ts=tape.ts, sym=tape.sym, kind=tape.kind, bid=tape.bid, bid_sz=tape.bid_sz,
             ask=tape.ask, ask_sz=tape.ask_sz, px=tape.px, sz=tape.sz, side=tape.side)
    log.info("cached tape (%d events) at %s", len(tape), path)
    return tape
