# FeedSentinel API contract (server ⇄ dashboard)

The server is FastAPI on `http://127.0.0.1:8000`. The dashboard is static files served from `/`
(`dashboard/index.html`, `/static/...`). All timestamps sent to the browser are **integer
milliseconds since the UNIX epoch** (`*_ms`), because nanosecond epochs do not fit in a JS number.
Pre-formatted clock strings are in the market's time zone (America/New_York for replay, UTC for live).

## WebSocket `/ws`

Server → client only. The client may reconnect at any time; the first message after connecting is
always `hello`, then `tick` messages arrive about 4 times per wall-clock second.

### `hello`
```json
{
  "type": "hello",
  "mode": "replay",                       // "replay" | "live"
  "product": "FeedSentinel",
  "source": "Nasdaq TotalView-ITCH via LOBSTER · 21 Jun 2012",
  "tz": "America/New_York",
  "speed": 5,                             // replay speed multiplier (market seconds per wall second)
  "speeds": [1, 2, 5, 10, 20],
  "feeds": [
    {"id": "A", "name": "Direct", "role": "Nasdaq direct feed (fastest)"},
    {"id": "B", "name": "SIP",    "role": "Consolidated tape (slower, authoritative)"},
    {"id": "C", "name": "Vendor", "role": "Third-party vendor feed (Chaos Lab target)"}
  ],
  "symbols": ["AAPL", "AMZN", "GOOG", "INTC", "MSFT"],
  "scenarios": [
    {"id": "packet_loss", "label": "Packet loss 5%", "group": "fault",
     "description": "Drops 5% of messages (UDP loss)", "accepts_symbols": false},
    {"id": "frozen", "label": "Frozen feed", "group": "fault",
     "description": "Handler stuck: heartbeats and sequence numbers stay perfect, prices stop moving",
     "accepts_symbols": true}
    // ... groups: "fault" | "incident" (historical replays) | "novel" (not in the classifier's
    //     training set) | "market" (real market events that must NOT raise an alert)
  ],
  "models": {"anomaly": true, "classifier": true},   // which trained models are loaded
  "llm": {"enabled": false, "provider": "template"}  // explainer status
}
```

### `tick`
```json
{
  "type": "tick",
  "t_ms": 1340290927250,          // market time
  "clock": "10:42:07",            // market time, formatted
  "date": "2012-06-21",
  "session": "OPEN",              // "PRE" | "OPEN" | "CLOSED"
  "mode": "replay",
  "speed": 5,
  "paused": false,
  "feeds": [
    {
      "id": "C", "name": "Vendor",
      "health": 31.4,                 // 0..100
      "state": "CRITICAL",            // "HEALTHY" | "DEGRADED" | "CRITICAL"
      "trust": 0.42,                  // 0..1 consensus trust weight
      "msg_rate": 412.0,              // data messages per market second (last 1 s window)
      "lat_p50_ms": 6.1, "lat_p99_ms": 14.8,
      "gaps": 0, "dups": 0, "quarantined": 0,        // cumulative counters since start
      "headline": "FROZEN: 5 symbols flat for 3.2 s", // "" when healthy
      "codes": ["FROZEN"],            // active finding codes, most severe first
      "ml": {"score": 0.71, "threshold": 0.58, "flag": true},   // null if no model loaded
      "diagnosis": {"label": "FROZEN", "p": 0.93},              // classifier; null if no model
      "history": [98.1, 97.9, 31.4]   // health for the last <=120 market seconds, oldest first
    }
  ],
  "recommended": {"AAPL": "A", "AMZN": "A", "GOOG": "A", "INTC": "A", "MSFT": "A"},
  "chart": {
    "symbol": "AAPL",
    "t_ms": [1340290807000, 1340290807250],          // sample times, oldest first (~250 ms apart)
    "series": {"consensus": [585.31, 585.33], "A": [585.31, 585.33], "B": [585.31, null], "C": [585.1, 585.1]},
    "markers": [{"t_ms": 1340290925000, "feed": "C", "code": "FROZEN", "severity": "CRITICAL"}]
  },
  "incidents": [ /* up to 30 incident summaries, open ones first, then newest resolved */ ],
  "chaos": {
    "active": [
      {"id": "F3", "scenario": "frozen", "label": "Frozen feed", "group": "fault", "feed": "C",
       "symbols": null, "started_ms": 1340290924000, "age_s": 3.2}
    ],
    "stopwatch": {                 // the last injected scenario
      "scenario": "frozen", "label": "Frozen feed", "feed": "C", "group": "fault",
      "started_ms": 1340290924000,
      "elapsed_s": 3.2,             // market seconds since injection (frozen once detected)
      "detected": true,
      "ttd_s": 1.4,                 // time to detect, market seconds; null until detected
      "incident_id": "INC-0007",
      "false_alerts": 0             // for group "market": alerts raised since injection (should stay 0)
    }                                // null if nothing injected yet
  },
  "self": {"events_per_s": 38200, "lag_ms": 4.0, "processed": 1234567, "uptime_s": 120.0}
}
```

### Incident summary (inside `tick.incidents`, and `GET /api/incidents`)
```json
{
  "id": "INC-0007", "feed": "C", "feed_name": "Vendor",
  "code": "FROZEN", "title": "Frozen values (feed alive, prices not moving)",
  "headline": "5 symbols flat for 3.2 s while A and B moved 14 times",
  "severity": "CRITICAL",          // "DEGRADED" | "CRITICAL"
  "status": "OPEN",                // "OPEN" | "RESOLVED"
  "opened_ms": 1340290925400, "opened_clock": "10:42:05",
  "last_ms": 1340290927000, "closed_ms": null, "duration_s": 1.6,
  "ttd_s": 1.4,                    // null when no injected fault matches (e.g. live data)
  "action": "Restart the feed handler; fail consumers over to A (Direct)",
  "symbols": ["AAPL", "AMZN", "GOOG", "INTC", "MSFT"],
  "explanation_source": "template", // "template" | "llm"
  "feedback": null                 // null | "confirmed" | "false_positive"
}
```

### Incident detail (`GET /api/incidents/{id}`) = summary plus:
```json
{
  "evidence": {"frozen_symbols": 5, "frozen_for_s": 3.2, "peer_changes": 14, "heartbeat_age_s": 0.4},
  "samples": [ {"feed": "C", "seq": 1234, "type": "Q", "sym": "AAPL", "bid": 585.1, "...": "..."} ],
  "top_features": [ {"name": "frozen_frac", "value": 1.0, "baseline": 0.0, "z": 12.5} ],
  "diagnosis": {"label": "FROZEN", "p": 0.93, "alternatives": [{"label": "STALE", "p": 0.04}]},
  "explanation": "Line 1\nLine 2\nLine 3",
  "timeline": [ {"t_ms": 1340290925400, "clock": "10:42:05", "event": "opened CRITICAL"} ],
  "fault": {"scenario": "frozen", "label": "Frozen feed", "started_ms": 1340290924000}  // or null
}
```

## REST

| Method & path | Body | Returns |
|---|---|---|
| `GET /api/state` | – | the latest `tick` payload |
| `GET /api/hello` | – | the `hello` payload |
| `GET /api/incidents?limit=100` | – | `[summary, ...]` newest first |
| `GET /api/incidents/{id}` | – | incident detail, 404 if unknown |
| `POST /api/incidents/{id}/feedback` | `{"label": "confirmed" \| "false_positive", "note": ""}` | `{"ok": true}` |
| `POST /api/chaos` | `{"scenario": "frozen", "feed": "C", "symbols": null \| ["AAPL"], "params": {}, "duration_s": null}` | the active fault object |
| `DELETE /api/chaos/{id}` | – | `{"ok": true}` |
| `POST /api/chaos/clear` | – | `{"ok": true, "cleared": 3}` |
| `POST /api/control` | any of `{"speed": 5}`, `{"paused": true}`, `{"symbol": "AAPL"}`, `{"restart": true}`, `{"mode": "live" \| "replay"}` | `{"ok": true}` |
| `POST /api/demo` | `{"action": "start" \| "stop"}` | `{"ok": true}` – scripted autopilot of the demo |
| `GET /api/quarantine?feed=C&limit=50` | – | `[{"reason": "OFF_TICK", "t_ms": ..., "msg": {...}}]` |
| `GET /api/metrics` | – | `reports/metrics.json` (evaluation results) or `{}` |

Errors use HTTP status codes with `{"detail": "..."}` (FastAPI default).

Notes for clients:
- A new `hello` can arrive at any time on an open socket (for example after `{"mode": "live"}` or
  `{"restart": true}`). Treat it as a full re-initialisation: feeds, symbols and scenarios may change.
- In live mode, `feeds` are real venues (for example Kraken, Gemini, Bitstamp) and `symbols` are
  crypto pairs such as `BTC-USD`. `ttd_s` is null for incidents that no injected fault explains.

## `GET /api/metrics` (evaluation report, written by `python -m feedsentinel evaluate`)
```json
{
  "generated_at": "2026-09-26T15:00:00Z",
  "data": {"source": "LOBSTER AAPL/AMZN/GOOG/INTC/MSFT 2012-06-21", "train": "09:30-12:30",
           "validation": "12:30-13:30", "test": "13:30-16:00"},
  "clean": {"market_hours": 2.5, "false_alarms": 0, "false_alarms_per_hour": 0.0,
            "by_feed": {"A": 0, "B": 0, "C": 0}, "market_events": 12, "market_event_alerts": 0},
  "faults": [
    {"fault": "packet_loss", "label": "Packet loss", "group": "fault", "episodes": 12, "detected": 12,
     "detection_rate": 1.0, "correct_code_rate": 1.0, "ttd_p50_s": 0.6, "ttd_p95_s": 1.1}
  ],
  "incidents": {"total": 150, "matched": 148, "precision": 0.987, "collateral_on_healthy_feeds": 0},
  "classifier": {"labels": ["NORMAL", "GAP"], "confusion": [[10, 0], [1, 9]],
                 "per_class": [{"label": "GAP", "precision": 0.9, "recall": 0.9, "f1": 0.9, "support": 10}],
                 "macro_f1": 0.9, "accuracy": 0.95},
  "anomaly": {"threshold": 0.58, "percentile": 99.5, "clean_windows": 50000, "flag_rate_clean": 0.005},
  "ablation": {"configs": ["rules", "rules+consensus", "rules+consensus+ml"],
               "rows": [{"fault": "frozen", "label": "Frozen feed", "rates": [0.0, 1.0, 1.0]}],
               "overall": [0.6, 0.9, 0.95], "false_alarms_per_hour": [0.0, 0.0, 0.4]},
  "throughput": {"events_per_s": 40000}
}
```
Any section may be missing if that part of the evaluation has not been run; clients must cope.

# Section B: add-ons (auth, roles, regions, timeline, RCA, trader view, chat)

## Auth (server-enforced; the UI hiding things is never the boundary)
- `POST /api/auth/login` `{"username","password"}` → `200 {"access_token","expires_in","user":{"username","display_name","role","regions":[...],"home":"/ops"|"/trader"}}`; also sets an httpOnly `fs_refresh` cookie. `401 {"detail"}` on bad credentials. Rate limited (429).
- `POST /api/auth/refresh` (cookie) → same shape as login; the refresh token rotates and is single-use (reuse revokes the session family → 401).
- `POST /api/auth/logout` → `{"ok": true}` (revokes the access token jti and the refresh family).
- `GET /api/auth/me` → `user`.
- Every other `/api/*` route needs `Authorization: Bearer <access_token>`: `401` = missing/expired (client calls `/api/auth/refresh` once, else goes to `/login`), `403` = role/region denied.
- WebSocket: `/ws?token=<access_token>`; the server closes with code `4401` for a bad/expired token (client refreshes, reconnects) and when the token expires.
- Demo users (password `demo123`): `ops_us` (ops_analyst, US), `trader_us` (trader, US), `trader_eu` (trader, EU), `admin` (admin, US/EU/GLOBAL).
- Roles: `ops_analyst` and `admin` → ops view; Chaos Lab, control, demo autopilot and incident ack allowed. `trader` → trader view, read-only.
- Pages: `/login`, `/ops`, `/trader` all serve `dashboard/index.html` (the SPA routes on `location.pathname`); `/` redirects to `/login`.

## Real-time sync
Every WebSocket message carries `"seq"` (per-connection counter starting at 1). A gap → `GET /api/state` to resync. If the socket fails, poll `GET /api/state` every 2 s. The UI shows `live` / `polling` / `stale` (no update for > 5 s).

## Ops (`ops_analyst`, `admin`)
`hello` gains `"user"` and `"view": "ops"`. `tick` gains:
```json
"timeline": [ {"id": 12, "t_ms": 1340290927250, "clock": "10:32:14", "feed": "C", "feed_name": "Vendor",
               "level": "minor", "icon": "🟠", "text": "Sequence gap detected", "count": 1,
               "incident_id": "INC-0003"} ]      // newest last, up to 60
```
levels/icons: `ok` 🟢, `warn` 🟡, `minor` 🟠, `major` 🔴, `incident` 🚨, `action` 🔄, `info` 🔵. `count` > 1 means collapsed repeats (text then reads e.g. "Sequence gap x14 in 8 s").
- `GET /api/timeline?limit=200&feed=C` → entries.
- `GET /api/rca/{incident_id}` → RCA object (also included as `"rca"` in `GET /api/incidents/{id}`):
```json
{"incident_id": "INC-0003", "primary": {"cause": "Packet loss", "confidence": 78},
 "secondary": {"cause": "Network latency / path", "confidence": 41},
 "evidence": ["Sequence gap: 342 messages", "Latency 13 ms -> 812 ms", "Other feeds unaffected"],
 "scope": "Other feeds unaffected: feed-path issue on C (Vendor)", "affected": ["AAPL", "MSFT"],
 "action": "Failover to Feed A (Direct)", "text": "ROOT CAUSE ANALYSIS\nPrimary: ...\n..."}
```
- `POST /api/incidents/{id}/ack` `{"note": ""}` → `{"ok": true}`; incident summaries gain `"acked_by"` and `"acked_ms"` (null until acknowledged).
- Admin only: `GET /api/audit?limit=100` → `[{"seq","t_ms","actor","action","target","outcome","detail","hash"}]`, `GET /api/audit/verify` → `{"ok": true, "entries": 57}`.

## Trader (`trader`)
`hello` gains `"user"` and `"view": "trader"` (no scenarios for traders). Instead of `tick`, traders receive:
```json
{"type": "trader_tick", "seq": 41, "t_ms": 1340290927250, "clock": "10:32:14", "date": "2012-06-21", "session": "OPEN",
 "tz": "America/New_York",
 "watchlist": [{"symbol": "MSFT", "name": "Microsoft Corporation", "price": 30.85,
                "trust": "VERIFIED",            // "VERIFIED" | "USE CAUTION" | "DO NOT USE"
                "trust_reason": "3 feeds agree", "source": "A", "source_name": "Direct", "updated_s": 0.4}],
 "alerts": [{"t_ms": ..., "clock": "10:32:14", "level": "critical"|"warn"|"ok",
             "text": "MSFT prices from Vendor feed unreliable since 10:32:14; using Direct feed"}]}
```
`GET /api/state` returns the same payload for a trader. A trader whose regions contain no streaming data gets an empty watchlist and `"notice": "No feeds in your region (EU) are streaming right now."`.

## Chat (all roles, answers only from the caller's scope)
`POST /api/chat` `{"message": "why is Feed C red?"}` → `{"answer": "text, may contain newlines", "sources": ["INC-0003", "feed C health @ 10:32:14"], "tools": ["get_feed_health", "get_rca"], "refused": false}`. Rate limited. Out-of-scope requests return `"refused": true` with an explanation.
