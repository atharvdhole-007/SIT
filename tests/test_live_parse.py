"""Unit tests for the live venue parsers (no network).

Kraken, Gemini and Bitstamp samples were captured from the real public streams on
2026-09-26 (Bitstamp order books trimmed to 3 levels). Coinbase is blocked on the
capture machine, so its samples are hand-written from the Coinbase Exchange docs.
"""
from __future__ import annotations

import asyncio
import calendar
import json

import pytest

from feedsentinel.runtime import live
from feedsentinel.runtime.live import (
    BitstampFeed, CoinbaseFeed, FeedState, GeminiFeed, KrakenFeed, VenueReconnect,
    build_feeds, iso_to_ns, ms_to_ns, parse_bitstamp, parse_coinbase, parse_gemini,
    parse_kraken, secs_to_ns, us_to_ns,
)
from feedsentinel.schema import HEARTBEAT, QUOTE, TRADE, required_fields

FLOAT_FIELDS = {"bid", "bid_sz", "ask", "ask_sz", "px", "sz"}
INT_FIELDS = {"seq", "exch_ts", "recv_ts"}
ALLOWED_EXTRA = {"recv_ts", "side", "vseq", "ts_src"}


def check_schema(msg: dict, feed: str = "A") -> None:
    """Every schema field present with the right type, and nothing unexpected."""
    for f in required_fields(msg["type"]):
        assert f in msg, f"{f} missing from {msg}"
    extra = set(msg) - set(required_fields(msg["type"])) - ALLOWED_EXTRA
    assert not extra, f"unexpected fields {extra}"
    assert msg["feed"] == feed and isinstance(msg["session"], str)
    for f in INT_FIELDS & set(msg):
        assert type(msg[f]) is int, f"{f} must be int: {msg}"
    for f in FLOAT_FIELDS & set(msg):
        assert type(msg[f]) is float, f"{f} must be float: {msg}"
    if "side" in msg:
        assert msg["side"] in ("B", "S")
    if "ts_src" in msg:
        assert msg["ts_src"] == "recv" and msg["exch_ts"] == msg["recv_ts"]
    if msg["type"] == QUOTE:
        assert msg["bid"] < msg["ask"]
        assert msg["sym"] in ("BTC-USD", "ETH-USD")
    # exchange timestamps are ns since the epoch (sanity: 2020..2100)
    assert 1_577_836_800 * 10**9 < msg["exch_ts"] < 4_102_444_800 * 10**9
    json.dumps(msg)   # JSON serialisable


def run_all(parse, samples, state, *args):
    out = []
    for recv_ts, raw in samples:
        out.extend(parse(raw, state, recv_ts, *args))
    return out


def assert_contiguous(msgs, start=1):
    data = [m["seq"] for m in msgs if m["type"] in (QUOTE, TRADE)]
    assert data == list(range(start, start + len(data)))


# =========================================================================== timestamps
def test_secs_to_ns_exact():
    assert secs_to_ns("1790401259.939649") == 1_790_401_259_939_649_000
    assert secs_to_ns("1790401259") == 1_790_401_259_000_000_000
    assert secs_to_ns("1790401259.123456789123") == 1_790_401_259_123_456_789
    assert secs_to_ns(1790401259) == 1_790_401_259_000_000_000
    assert isinstance(secs_to_ns("1.5"), int)


def test_ms_and_us_to_ns():
    assert ms_to_ns(1790401258816) == 1_790_401_258_816_000_000
    assert us_to_ns("1790401258521974") == 1_790_401_258_521_974_000
    assert type(ms_to_ns("1790401258816")) is int and type(us_to_ns(1)) is int


def test_iso_to_ns_exact():
    base = calendar.timegm((2022, 10, 19, 23, 28, 22, 0, 0, 0)) * 10**9
    assert iso_to_ns("2022-10-19T23:28:22.061769Z") == base + 61_769_000
    assert iso_to_ns("2022-10-19T23:28:22Z") == base
    assert iso_to_ns("2022-10-19T23:28:22.123456789Z") == base + 123_456_789
    assert iso_to_ns("2022-10-19T23:28:22.5+00:00") == base + 500_000_000
    assert iso_to_ns("2022-10-20T01:28:22.061769+02:00") == base + 61_769_000
    with pytest.raises(ValueError):
        iso_to_ns("not a time")


# =========================================================================== FeedState
def test_quote_dedup_and_validation():
    st = FeedState("A", "x-1")
    q = st.quote("BTC-USD", 100.0, 1.0, 101.0, 2.0, 5, 6)
    assert q and q["seq"] == 1
    assert st.quote("BTC-USD", 100.0, 1.0, 101.0, 2.0, 7, 8) is None      # unchanged tuple
    assert st.quote("BTC-USD", 101.0, 1.0, 101.0, 2.0, 7, 8) is None      # locked
    assert st.quote("BTC-USD", 102.0, 1.0, 101.0, 2.0, 7, 8) is None      # crossed
    assert st.quote("BTC-USD", None, 1.0, 101.0, 2.0, 7, 8) is None       # side unknown
    q2 = st.quote("BTC-USD", 100.0, 1.5, 101.0, 2.0, 9, 10)               # size change
    assert q2 and q2["seq"] == 2
    assert st.quote("ETH-USD", 100.0, 1.0, 101.0, 2.0, 9, 10)["seq"] == 3  # per-symbol state


def test_heartbeat_semantics():
    st = FeedState("B", "kraken-9")
    st.quote("BTC-USD", 100.0, 1.0, 101.0, 2.0, 5, 6)
    hb = st.heartbeat(1_790_401_259_000_000_000)
    check_schema(hb, feed="B")
    assert hb["type"] == HEARTBEAT and hb["seq"] == 2          # next seq (MoldUDP64)
    assert hb["exch_ts"] == hb["recv_ts"] == 1_790_401_259_000_000_000
    assert set(hb) == set(required_fields(HEARTBEAT)) | {"recv_ts"}
    assert st.next_seq == 2                                    # heartbeats consume nothing


def test_missing_venue_timestamp_uses_recv():
    st = FeedState("A", "x-1")
    q = st.quote("BTC-USD", 100.0, 1.0, 101.0, 2.0, None, 1_790_401_259_000_000_001)
    assert q["ts_src"] == "recv" and q["exch_ts"] == q["recv_ts"] == 1_790_401_259_000_000_001


# =========================================================================== Kraken
KRAKEN = [
    (1790401256575510300, '{"event":"systemStatus","version":"1.9.6","status":"online","connectionID":14463420050456470503,"upcoming_maintenance":[],"emergency":[]}'),
    (1790401256746131000, '{"channelID":119930890,"channelName":"spread","event":"subscriptionStatus","pair":"XBT/USD","status":"subscribed","subscription":{"name":"spread"}}'),
    (1790401256748781100, '[119930890,["83942.70000","83942.80000","1790401259.939649","0.09390166","0.18763801"],"spread","XBT/USD"]'),
    (1790401256748805600, '[13959178,["2687.94000","2687.95000","1790401259.878200","1.95021283","0.55812016"],"spread","ETH/USD"]'),
    (1790401256898904600, '[119930890,["83942.70000","83942.80000","1790401259.939649","0.09390166","0.18525599"],"spread","XBT/USD"]'),
    # same tuple as the previous XBT quote, new timestamp -> de-duplicated
    (1790401256900000000, '[119930890,["83942.70000","83942.80000","1790401260.000001","0.09390166","0.18525599"],"spread","XBT/USD"]'),
    (1790401257145490700, '[13959169,[["2688.04000","0.17453200","1790401260.639729","b","l",""]],"trade","ETH/USD"]'),
    (1790401257200000000, '{"event":"heartbeat"}'),
    (1790401258000000000, '[119930881,[["83942.80000","0.00044674","1790401261.643178","b","l",""],["83942.80000","0.00010000","1790401261.643178","s","m",""]],"trade","XBT/USD"]'),
]


def test_kraken_parse():
    st = FeedState("A", "kraken-1")
    out = run_all(parse_kraken, KRAKEN, st)
    for m in out:
        check_schema(m)
    assert [m["type"] for m in out] == ["Q", "Q", "Q", "T", "T", "T"]
    assert_contiguous(out)
    q = out[0]
    assert q["sym"] == "BTC-USD" and q["bid"] == 83942.7 and q["ask"] == 83942.8
    assert q["bid_sz"] == 0.09390166 and q["ask_sz"] == 0.18763801
    assert q["exch_ts"] == 1_790_401_259_939_649_000 and q["recv_ts"] == 1790401256748781100
    assert out[1]["sym"] == "ETH-USD"
    t = out[3]
    assert (t["sym"], t["px"], t["sz"], t["side"]) == ("ETH-USD", 2688.04, 0.174532, "B")
    assert [m["side"] for m in out[4:]] == ["B", "S"]
    assert all(m["recv_ts"] == 1790401258000000000 for m in out[4:])


def test_kraken_ignores_unknown_pairs_and_junk():
    st = FeedState("A", "kraken-1")
    assert parse_kraken('[1,["1","2","3","4","5"],"spread","DOGE/USD"]', st, 1) == []
    assert parse_kraken('{"event":"pong"}', st, 1) == []
    assert st.next_seq == 1


# =========================================================================== Gemini
GEMINI_BTC = [
    # real captured messages (a subset); socket_sequence renumbered 0..5 to stay contiguous
    (1790401255224431800, '{"eventId":1764559146464275,"events":[{"delta":"0.06198727","price":"83948.71","reason":"initial","remaining":"0.06198727","side":"ask","type":"change"},{"delta":"0.10350843","price":"83948.7","reason":"initial","remaining":"0.10350843","side":"bid","type":"change"}],"socket_sequence":0,"type":"update"}'),
    (1790401255363312900, '{"eventId":1764559146470614,"events":[{"price":"83948.71","reason":"top-of-book","remaining":"0.06738727","side":"ask","type":"change"}],"socket_sequence":1,"timestamp":1790401258,"timestampms":1790401258816,"type":"update"}'),
    (1790401255849016000, '{"socket_sequence":2,"type":"heartbeat"}'),
    (1790401257805678500, '{"eventId":1764559146474487,"events":[{"price":"83948.73","reason":"top-of-book","remaining":"0.01217013","side":"ask","type":"change"}],"socket_sequence":3,"timestamp":1790401261,"timestampms":1790401261217,"type":"update"}'),
    (1790401257805722300, '{"eventId":1764559146474497,"events":[{"price":"83948.72","reason":"top-of-book","remaining":"0.0054","side":"bid","type":"change"}],"socket_sequence":4,"timestamp":1790401261,"timestampms":1790401261222,"type":"update"}'),
    (1790401258203108000, '{"eventId":1764559146475721,"events":[{"amount":"0.0011912","makerSide":"ask","price":"83955.14","tid":2840141029940425,"type":"trade"}],"socket_sequence":5,"timestamp":1790401261,"timestampms":1790401261620,"type":"update"}'),
]


def test_gemini_parse_top_of_book():
    st = FeedState("C", "gemini-1")
    out = run_all(parse_gemini, GEMINI_BTC, st, "BTC-USD")
    for m in out:
        check_schema(m, feed="C")
    assert [m["type"] for m in out] == ["Q", "Q", "Q", "Q", "T"]
    assert_contiguous(out)
    first = out[0]   # "initial" snapshot has no timestamp -> recv time
    assert first["ts_src"] == "recv" and first["exch_ts"] == 1790401255224431800
    assert (first["bid"], first["bid_sz"], first["ask"], first["ask_sz"]) == (
        83948.7, 0.10350843, 83948.71, 0.06198727)
    assert out[1]["ask_sz"] == 0.06738727 and out[1]["bid"] == 83948.7   # bid side kept
    assert out[1]["exch_ts"] == 1_790_401_258_816_000_000 and "ts_src" not in out[1]
    assert (out[2]["ask"], out[2]["ask_sz"], out[2]["bid"]) == (83948.73, 0.01217013, 83948.7)
    assert (out[3]["bid"], out[3]["bid_sz"], out[3]["ask"]) == (83948.72, 0.0054, 83948.73)
    t = out[4]
    assert (t["px"], t["sz"], t["side"]) == (83955.14, 0.0011912, "B")    # makerSide ask
    assert [m["vseq"] for m in out] == [0, 1, 3, 4, 5]
    assert st.venue_lost == 0


def test_gemini_crossed_top_suppressed_until_fixed():
    # a new best bid above the current best ask (other side not yet updated) is crossed
    st = FeedState("C", "gemini-1")
    parse_gemini(GEMINI_BTC[0][1], st, 1, "BTC-USD")
    raw = ('{"events":[{"price":"83950","reason":"top-of-book","remaining":"1","side":"bid",'
           '"type":"change"}],"socket_sequence":1,"timestampms":1790401258816,"type":"update"}')
    assert parse_gemini(raw, st, 2, "BTC-USD") == []
    raw = ('{"events":[{"price":"83951","reason":"top-of-book","remaining":"2","side":"ask",'
           '"type":"change"}],"socket_sequence":2,"timestampms":1790401258817,"type":"update"}')
    (q,) = parse_gemini(raw, st, 3, "BTC-USD")
    assert (q["bid"], q["ask"], q["seq"]) == (83950.0, 83951.0, 2)


def test_gemini_seller_aggressor():
    st = FeedState("C", "gemini-1")
    raw = ('{"events":[{"amount":"0.5","makerSide":"bid","price":"2688.1","tid":1,"type":"trade"}],'
           '"socket_sequence":0,"timestampms":1790401261620,"type":"update"}')
    (t,) = parse_gemini(raw, st, 1790401261000000000, "ETH-USD")
    assert t["side"] == "S" and t["sym"] == "ETH-USD"
    raw = raw.replace('"bid"', '"auction"').replace('"socket_sequence":0', '"socket_sequence":1')
    (t,) = parse_gemini(raw, st, 1790401261000000000, "ETH-USD")
    assert "side" not in t


def test_gemini_socket_sequence_gap_maps_to_seq_gap():
    st = FeedState("C", "gemini-1")
    msgs = [GEMINI_BTC[0], GEMINI_BTC[1]]                     # vseq 0, 1 -> seq 1, 2
    # venue messages 2, 3 and 4 were lost; next one is socket_sequence 5
    lost = GEMINI_BTC[3][1].replace('"socket_sequence":3', '"socket_sequence":5')
    msgs.append((GEMINI_BTC[3][0], lost))
    out = run_all(parse_gemini, msgs, st, "BTC-USD")
    assert [m["seq"] for m in out] == [1, 2, 6]               # advanced by k = 3
    assert out[-1]["vseq"] == 5 and st.venue_lost == 3
    assert st.heartbeat(1_790_401_262_000_000_000)["seq"] == 7
    # a gap revealed by a venue heartbeat also advances seq (seen by our next "H")
    parse_gemini('{"socket_sequence":8,"type":"heartbeat"}', st, 1, "BTC-USD")
    assert st.venue_lost == 5 and st.heartbeat(1_790_401_262_000_000_000)["seq"] == 9


def test_gemini_sockets_tracked_per_symbol():
    st = FeedState("C", "gemini-1")
    parse_gemini(GEMINI_BTC[0][1], st, 1790401255224431800, "BTC-USD")
    eth = ('{"eventId":1764559146460076,"events":[{"delta":"0.205329","price":"2687.85","reason":"initial",'
           '"remaining":"0.205329","side":"ask","type":"change"},{"delta":"1.011981","price":"2687.84",'
           '"reason":"initial","remaining":"1.011981","side":"bid","type":"change"}],"socket_sequence":0,'
           '"type":"update"}')
    (q,) = parse_gemini(eth, st, 1790401255111292700, "ETH-USD")   # own socket_sequence 0: no gap
    assert q["sym"] == "ETH-USD" and q["seq"] == 2 and st.venue_lost == 0
    assert st.book["BTC-USD"][0] == 83948.7 and st.book["ETH-USD"][0] == 2687.84


# =========================================================================== Bitstamp
BITSTAMP = [
    (1790401254900000000, '{"event":"bts:subscription_succeeded","channel":"order_book_btcusd","data":{}}'),
    (1790401255013095500, '{"data": {"timestamp": "1790401258", "microtimestamp": "1790401258521974", "bids": [["83938.99", "0.85114353"], ["83938.46", "0.25014847"], ["83938.45", "0.05956745"]], "asks": [["83939.00", "0.72730996"], ["83939.19", "0.12500000"], ["83939.26", "0.06250000"]]}, "channel": "order_book_btcusd", "event": "data"}'),
    # deeper levels changed, level 0 identical -> de-duplicated
    (1790401255195762900, '{"data": {"timestamp": "1790401258", "microtimestamp": "1790401258709923", "bids": [["83938.99", "0.85114353"], ["83938.46", "0.25014847"], ["83938.45", "0.05956745"]], "asks": [["83939.00", "0.72730996"], ["83939.26", "0.06250000"], ["83941.78", "0.06250000"]]}, "channel": "order_book_btcusd", "event": "data"}'),
    (1790401255208901600, '{"data": {"timestamp": "1790401258", "microtimestamp": "1790401258710411", "bids": [["2687.69", "4.074128"], ["2687.66", "3.720695"], ["2687.62", "0.318055"]], "asks": [["2687.70", "1.860382"], ["2687.81", "0.186051"], ["2687.88", "1.250000"]]}, "channel": "order_book_ethusd", "event": "data"}'),
    (1790401256000000000, '{"event":"bts:heartbeat","channel":"","data":{"status":"success"}}'),
    (1790401273666093400, '{"data":{"id":643660479,"timestamp":"1790401277","amount":0.125,"amount_str":"0.12500000","price":83946.07,"price_str":"83946.07","type":1,"microtimestamp":"1790401277185000","buy_order_id":2054415511941130,"sell_order_id":2054415575154691},"channel":"live_trades_btcusd","event":"trade"}'),
    (1790401273666156400, '{"data":{"id":643660480,"timestamp":"1790401277","amount":0.04929643,"amount_str":"0.04929643","price":83946.07,"price_str":"83946.07","type":0,"microtimestamp":"1790401277185000","buy_order_id":2054415512244224,"sell_order_id":2054415575154691},"channel":"live_trades_btcusd","event":"trade"}'),
]


def test_bitstamp_parse():
    st = FeedState("B", "bitstamp-1")
    out = run_all(parse_bitstamp, BITSTAMP, st)
    for m in out:
        check_schema(m, feed="B")
    assert [m["type"] for m in out] == ["Q", "Q", "T", "T"]
    assert_contiguous(out)
    q = out[0]
    assert (q["sym"], q["bid"], q["bid_sz"], q["ask"], q["ask_sz"]) == (
        "BTC-USD", 83938.99, 0.85114353, 83939.0, 0.72730996)
    assert q["exch_ts"] == 1_790_401_258_521_974_000
    assert out[1]["sym"] == "ETH-USD"
    t1, t2 = out[2:]
    assert (t1["px"], t1["sz"], t1["side"]) == (83946.07, 0.125, "S")
    assert t2["side"] == "B" and t2["exch_ts"] == 1_790_401_277_185_000_000


def test_bitstamp_request_reconnect():
    with pytest.raises(VenueReconnect):
        parse_bitstamp('{"event":"bts:request_reconnect","channel":"","data":""}',
                       FeedState("B", "bitstamp-1"), 1)


# =========================================================================== Coinbase
# Hand-written from the Coinbase Exchange WebSocket docs (ticker/matches/heartbeat).
COINBASE = [
    (1666222102100000000, '{"type":"subscriptions","channels":[{"name":"heartbeat","product_ids":["BTC-USD","ETH-USD"]},{"name":"ticker","product_ids":["BTC-USD","ETH-USD"]},{"name":"matches","product_ids":["BTC-USD","ETH-USD"]}]}'),
    (1666222102200000000, '{"type":"last_match","trade_id":370843400,"maker_order_id":"a","taker_order_id":"b","side":"sell","size":"0.1","price":"1285.00","product_id":"ETH-USD","sequence":37475248700,"time":"2022-10-19T23:20:00.000000Z"}'),
    (1666222102300000000, '{"type":"heartbeat","sequence":37475248780,"last_trade_id":370843400,"product_id":"ETH-USD","time":"2022-10-19T23:28:22.000000Z"}'),
    (1666222102400000000, '{"type":"match","trade_id":370843401,"maker_order_id":"c","taker_order_id":"d","side":"sell","size":"11.4396987","price":"1285.22","product_id":"ETH-USD","sequence":37475248783,"time":"2022-10-19T23:28:22.061769Z"}'),
    (1666222102410000000, '{"type":"ticker","sequence":37475248783,"product_id":"ETH-USD","price":"1285.22","open_24h":"1310.79","volume_24h":"245532.79269678","low_24h":"1280.52","high_24h":"1313.8","volume_30d":"9788783.60117027","best_bid":"1285.04","best_bid_size":"0.46688654","best_ask":"1285.27","best_ask_size":"1.56637040","side":"buy","time":"2022-10-19T23:28:22.061769Z","trade_id":370843401,"last_size":"11.4396987"}'),
    # venue sequence jumps by 1000 (ticker is droppable): must NOT become a seq gap
    (1666222102500000000, '{"type":"ticker","sequence":37475249783,"product_id":"BTC-USD","price":"19100.00","best_bid":"19099.99","best_bid_size":"0.5","best_ask":"19100.00","best_ask_size":"0.25","side":"sell","time":"2022-10-19T23:28:22.5Z","trade_id":1,"last_size":"0.01"}'),
    (1666222102600000000, '{"type":"match","trade_id":2,"side":"buy","size":"0.01","price":"19099.99","product_id":"BTC-USD","sequence":37475249790,"time":"2022-10-19T23:28:22.6Z"}'),
]


def test_coinbase_parse():
    st = FeedState("A", "coinbase-1")
    out = run_all(parse_coinbase, COINBASE, st)
    for m in out:
        check_schema(m)
    assert [m["type"] for m in out] == ["T", "Q", "Q", "T"]   # last_match/heartbeat skipped
    assert_contiguous(out)                                     # no mapping of venue gaps
    t = out[0]
    assert (t["sym"], t["px"], t["sz"], t["side"]) == ("ETH-USD", 1285.22, 11.4396987, "B")
    assert t["exch_ts"] == iso_to_ns("2022-10-19T23:28:22.061769Z")
    q = out[1]
    assert (q["bid"], q["bid_sz"], q["ask"], q["ask_sz"]) == (1285.04, 0.46688654, 1285.27, 1.5663704)
    assert out[3]["side"] == "S" and st.venue_lost == 0


def test_coinbase_ticker_dedup():
    st = FeedState("A", "coinbase-1")
    raw = COINBASE[4][1]
    assert len(parse_coinbase(raw, st, 1)) == 1
    assert parse_coinbase(raw.replace("370843401", "370843402"), st, 2) == []


# =========================================================================== adapters
def test_registry_build_feeds_and_info():
    assert set(live.VENUES) == {"coinbase", "kraken", "gemini", "bitstamp"}
    feeds = build_feeds(["kraken", "gemini", "bitstamp"])
    assert [f.feed_id for f in feeds] == ["A", "B", "C"]
    assert [type(f) for f in feeds] == [KrakenFeed, GeminiFeed, BitstampFeed]
    assert feeds[0].info() == {"id": "A", "name": "Kraken",
                               "role": "Live venue feed (Kraken public WebSocket)"}
    assert live.feed_info(feeds)[2]["name"] == "Bitstamp"
    with pytest.raises(ValueError):
        KrakenFeed("A", ["DOGE-USD"])


def test_subscriptions_use_venue_symbols():
    k = KrakenFeed("A").subscriptions("kraken")
    assert {s["subscription"]["name"] for s in k} == {"spread", "trade"}
    assert all(s["pair"] == ["XBT/USD", "ETH/USD"] for s in k)
    g = GeminiFeed("B").streams()
    assert [key for _, key in g] == ["BTC-USD", "ETH-USD"]
    assert "/BTCUSD?" in g[0][0] and "top_of_book=true" in g[0][0] and "heartbeat=true" in g[0][0]
    b = {s["data"]["channel"] for s in BitstampFeed("C").subscriptions("bitstamp")}
    assert b == {"order_book_btcusd", "order_book_ethusd", "live_trades_btcusd", "live_trades_ethusd"}
    (c,) = CoinbaseFeed("A").subscriptions("coinbase")
    assert c["product_ids"] == ["BTC-USD", "ETH-USD"] and "heartbeat" in c["channels"]


# =========================================================================== run loop
def test_run_loop_heartbeats_and_reconnect_against_local_server():
    """KrakenFeed against a local fake server: data flows, heartbeats carry the next
    seq, the server drop creates a new session starting at seq 1, stop ends run()."""
    from websockets.asyncio.server import serve

    async def scenario():
        conns = 0

        async def handler(ws):
            nonlocal conns
            conns += 1
            await ws.send(KRAKEN[0][1])
            await ws.recv()   # the two subscribe requests
            await ws.recv()
            for _, raw in KRAKEN[2:5]:
                await ws.send(raw)
            if conns == 1:
                await asyncio.sleep(0.35)
                return        # drop the first connection
            while True:
                await asyncio.sleep(0.05)
                await ws.send('{"event":"heartbeat"}')

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            feed = KrakenFeed("A")
            feed.url = f"ws://127.0.0.1:{port}"
            feed.heartbeat_interval, feed.backoff_start = 0.1, 0.1
            got: list[dict] = []
            stop = asyncio.Event()
            task = asyncio.create_task(feed.run(got.append, stop))
            await asyncio.sleep(1.0)
            stop.set()
            await asyncio.wait_for(task, 5)
            return got, conns

    got, conns = asyncio.run(scenario())
    assert conns >= 2
    sessions = list(dict.fromkeys(m["session"] for m in got))
    assert len(sessions) >= 2 and all(s.startswith("kraken-") for s in sessions)
    for s in sessions[:2]:
        msgs = [m for m in got if m["session"] == s]
        data = [m for m in msgs if m["type"] != HEARTBEAT]
        assert [m["seq"] for m in data] == [1, 2, 3]
        hbs = [m for m in msgs if m["type"] == HEARTBEAT]
        assert hbs and all(h["seq"] == 4 for h in hbs)
        for m in msgs:
            check_schema(m)
