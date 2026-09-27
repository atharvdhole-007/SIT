# FeedSentinel: the whole project, explained

> **One line:** FeedSentinel watches every market-data feed, tells a real market move from a broken feed,
> names what broke and why, and tells each user which feed to trust, usually within a few seconds.

This document explains everything we built, in plain words: the problem, the data, how the data is
combined, how detection works layer by layer, the AI, the dashboards, security, how we measured it,
the tech stack, and what is still missing.

---

## Contents
1. [The problem](#1-the-problem)
2. [The big idea](#2-the-big-idea)
3. [What is real and what is simulated](#3-what-is-real-and-what-is-simulated)
4. [The data we use](#4-the-data-we-use)
5. [How all data becomes one format](#5-how-all-data-becomes-one-format)
6. [The journey of one message](#6-the-journey-of-one-message)
7. [Three feeds from one source](#7-three-feeds-from-one-source)
8. [Chaos Lab: breaking feeds on purpose](#8-chaos-lab-breaking-feeds-on-purpose)
9. [The detection engine, layer by layer (L0-L7)](#9-the-detection-engine-layer-by-layer-l0-l7)
10. [Where the AI is](#10-where-the-ai-is)
11. [Root-cause analysis](#11-root-cause-analysis)
12. [Incident timeline](#12-incident-timeline)
13. [Incident explanations (template + LLM)](#13-incident-explanations-template--llm)
14. [The chat assistant](#14-the-chat-assistant)
15. [Security: login, roles, regions, audit](#15-security-login-roles-regions-audit)
16. [The screens](#16-the-screens)
17. [How we proved it works](#17-how-we-proved-it-works)
18. [Tech stack](#18-tech-stack)
19. [Folder structure](#19-folder-structure)
20. [How to run it and the demo script](#20-how-to-run-it-and-the-demo-script)
21. [Bugs we found and fixed while building](#21-bugs-we-found-and-fixed-while-building)
22. [What is not built yet](#22-what-is-not-built-yet)
23. [Questions judges will ask](#23-questions-judges-will-ask)

---

## 1. The problem

Nasdaq's problem statement: *"Exchanges rely on many data feeds sending prices, volumes, and company
information constantly. Build an AI system that monitors incoming data feeds and flags ones that look
garbled, delayed, duplicated, or otherwise broken or unreliable in real time."*

Why this is hard: **silence and jumps are ambiguous.**
- A price chart going flat at 4:05 pm is fine (the market closed). The same flat line at 11:00 am means
  a broken feed.
- A price jumping 3% can be real news or a broken decoder.
- Real incidents: on **3 July 2017** test data leaked into production and Apple, Amazon, Microsoft and
  Google all showed **$123.47** on Google Finance, Yahoo and Bloomberg. On **22 August 2013** a
  reconnect storm flooded Nasdaq's SIP and trading in Nasdaq stocks halted for about 3 hours.

## 2. The big idea

Don't ask *"is this number weird?"*. Ask two questions:

1. **Is this feed behaving like a healthy feed?** (sequence numbers, heartbeats, latency, message rate)
2. **Do independent feeds agree?** (if three copies of the same data disagree, the odd one out is broken;
   if all three move together, it is the market)

Plain rules catch the certain cases. Cross-feed **consensus** catches faults no single-feed rule can
see (like a frozen feed that still sends perfect heartbeats). **AI** catches unusual behaviour nobody
wrote a rule for, and names the fault type.

## 3. What is real and what is simulated

| Real | Simulated | Not built (roadmap) |
|---|---|---|
| Nasdaq order-book data (LOBSTER, derived from TotalView-ITCH), the Nasdaq Symbol Directory, every detector, the trained ML models, the evaluation numbers, the dashboards, login and security, the live crypto adapters | The **three feed paths** (split from one real source, each with its own latency) and the **faults** (injected by the Chaos Lab) | A binary ITCH decoder, a real Nasdaq Cloud Data Service (Kafka) connection, SQL/JSON source adapters, MFA, an EU data feed |

We say this openly because Nasdaq engineers will ask, and it is the honest way to test a detector:
nobody publishes labelled "this feed broke at 10:42:03" data, so we inject faults into real data and
know exactly when each one started.

## 4. The data we use

| Data | Where from | Format | What it gives us |
|---|---|---|---|
| **LOBSTER sample, 21 June 2012** for AAPL, AMZN, GOOG, INTC, MSFT (level 1) | Hugging Face mirror of the LOBSTER samples (the LOBSTER site did not resolve from our network) | CSV, two files per stock | Every order-book event from 09:30 to 16:00, with nanosecond timestamps |
| **Nasdaq Symbol Directory** (`nasdaqlisted.txt`) | nasdaqtrader.com | Pipe-separated text | 5,636 listed securities: name, test-issue flag, financial status, round lot |
| **Live crypto prices** (optional) | Kraken, Gemini, Bitstamp public WebSockets (Coinbase adapter exists, but Coinbase is blocked on our network) | JSON over WebSocket | Three genuinely independent live venues |

Facts about the Nasdaq data that we handle correctly:
- Prices are stored as integers × 10,000 (Nasdaq's "Price(4)" format), so we divide by 10,000.
- Time is "seconds after midnight" with 9 decimals, so we convert to nanoseconds since 1970 (UTC).
- AAPL trades around $580 because this is before its 2014 stock split. That is correct, not an error.
- Trades can be sub-penny (hidden mid-point executions); quotes cannot. The checks know the difference.
- Single stocks go quiet for up to **38 seconds** during normal trading. So "no update for 5 s" can
  never mean "broken" by itself. That is why we compare feeds against each other.

After loading, the day is one merged tape of **1,141,010 events** (1,017,026 quotes and 123,984 trades),
cached as a NumPy file so it reloads in under a second.

## 5. How all data becomes one format

Every source has its own small **translator (adapter)**. Each adapter reads its own format and produces
the same **standard message**, defined in `feedsentinel/schema.py`. After this point the detector does
not know or care where a message came from.

**The standard message (a simple key-value record):**

| Field | Meaning |
|---|---|
| `feed` | which feed it came from (A, B, C, ...) |
| `session`, `seq` | session id and sequence number (like Nasdaq's MoldUDP64 protocol), used to spot lost or repeated messages |
| `type` | `Q` quote, `T` trade, `H` heartbeat, `S` system event (market open/close), `R` trading halt/resume |
| `exch_ts` | when the source sent it, nanoseconds since 1970 (UTC) |
| `recv_ts` | when we received it, stamped by us |
| `sym` | symbol, e.g. `AAPL` or `BTC-USD` |
| `bid`, `bid_sz`, `ask`, `ask_sz` | for quotes: best bid/ask price and size |
| `px`, `sz`, `side` | for trades: price, size, buyer or seller initiated |

**What each translator does:**
- **Nasdaq CSV (LOBSTER):** divide prices by 10,000; turn "seconds after midnight" plus the date into
  nanoseconds; emit a quote whenever the best bid/ask changes and a trade for every execution; merge
  the 5 stocks into one time-ordered stream.
- **Crypto JSON (Kraken, Gemini, Bitstamp, Coinbase):** rename each venue's fields to ours, map symbols
  (Kraken's `XBT/USD` becomes `BTC-USD`), convert each venue's timestamp to nanoseconds, add sequence
  numbers and one heartbeat per second, and only send a quote when it actually changed.
- **Symbol Directory:** loaded into a lookup table, used to reject unknown symbols and to spot test
  stocks such as `ZVZZT`.

Then the **gatekeeper** checks every standard message. A broken message is **quarantined with a reason,
never silently dropped**, because the broken message is the evidence.

## 6. The journey of one message

```
 LOBSTER CSV ──► tape (one merged stream) ──► split into 3 feeds ──► Chaos Lab (optional fault)
                                                  A Direct   ~0.4 ms
                                                  B SIP      ~35 ms
                                                  C Vendor   ~10 ms
                                                          │
                                                          ▼
 ┌────────────────────────── DETECTION ENGINE (per message) ──────────────────────────┐
 │ L0 market context → L1 header check → L2 sequence & timing → L1 payload check      │
 │                     → accepted into the feed's state and the cross-feed consensus   │
 └──────────────────────────────────────────────────────────────────────────────────────┘
                                                          │ every 1 second (market time)
                                                          ▼
 ┌────────────────────────── DETECTION ENGINE (per window) ───────────────────────────┐
 │ L5 consensus summary + L3 22 behaviour features → L4 AI (anomaly score, fault type) │
 │ → findings → L6 health score, state, trust → L7 incidents (+ RCA, timeline, notes)  │
 └──────────────────────────────────────────────────────────────────────────────────────┘
                                                          │
                                                          ▼
 FastAPI server ──(WebSocket, 4 updates/s, filtered per user)──► Ops dashboard / Trader dashboard / Chat
```

## 7. Three feeds from one source

Real exchanges publish the same data over several paths. We copy that: one real Nasdaq source becomes
three feeds (`sim/feeds.py`):

| Feed | Plays the role of | Typical latency |
|---|---|---|
| **A: Direct** | Nasdaq's direct feed, the fastest | ~0.4 ms |
| **B: SIP** | The consolidated tape: slower, but correct | ~35 ms |
| **C: Vendor** | A third-party vendor feed, the default Chaos Lab target | ~10 ms |

Each feed has its own sequence numbers, sends a **heartbeat every second** (carrying the next sequence
number, like MoldUDP64, so loss is visible even when the market is quiet), and has realistic latency with
rare queueing micro-bursts. An important design point: **slow is not broken.** The SIP is always slower,
and it must never be flagged just for that.

## 8. Chaos Lab: breaking feeds on purpose

The Chaos Lab (`sim/chaos.py`) is both the **training data** and the **demo**. It has 20 scenarios in
four groups, and records exactly when each one starts and ends (the "ground truth").

| Group | Scenarios | Expected alert |
|---|---|---|
| **Faults** | Packet loss, Delay +800 ms, Duplicate storm, Conflicting duplicates, Out-of-order, Garbled payloads (8 corruption types), **Frozen feed**, Silent feed, Disconnect, Price spikes, Decimal shift, Slow drift, Clock skew, Sequence reset | The matching fingerprint (GAP, DELAY, FROZEN, ...) |
| **Replayed real incidents** | **2017 test-data leak** ($123.47 on every stock), **2013 reconnect storm** (floods of replayed messages and unknown symbols) | TEST_DATA_LEAK, RATE_STORM |
| **Novel faults** (never shown to the AI's classifier, no rule written for them) | Silent throttling (vendor quietly drops most updates), Volume corruption (sizes × 100) | ML_ANOMALY |
| **Real market events** | Trading halt, Real market move +3% (applied to all feeds together) | **No alert** |

Faults can hit different stages, just like real life: what the sender publishes (frozen values, spikes,
drift, wrong timestamps), what the network delivers (loss, delay, reordering, duplicates, disconnects),
or corruption of the message itself.

**The killer demo: the frozen feed.** Heartbeats keep coming, sequence numbers are perfect, every format
check passes. A rules-only monitor stays green. FeedSentinel turns C red because C's prices stopped
moving while A and B kept moving.

## 9. The detection engine, layer by layer (L0-L7)

All in `feedsentinel/detect/`. Cheap checks run on every message; heavier analysis runs once per second.

### L0: Market context (`context.py`), "quiet is not broken"
- Tracks the trading session (open/closed) and per-symbol halts. It changes state only when a
  **majority of feeds agree**, so one bad feed cannot switch the alarms off.
- Keeps a reference price per symbol for the price-band check (like the real LULD rule: ±5%, doubled
  near the open and close).
- If most feeds break the band together, it is a **real market move**, so the reference re-anchors and
  nothing is flagged.

### L1: Gatekeeper (`gatekeeper.py`), "quarantine, never drop"
Checks every message: known message type, sane timestamp, required fields present, numbers really are
numbers, price above zero, bid below ask, quotes on the $0.01 tick grid, symbol exists in the Nasdaq
Symbol Directory, not a test stock, price inside the band. A price exactly 10× or 100× off the reference
is recognised as a **decimal-shift** (price-scale) bug.

### L2: Sequence & timing (`sequencer.py`), the same logic as a real MoldUDP64 receiver
- A missing sequence number opens a **gap**. If it arrives within 100 ms it was just **reordered**;
  otherwise it is **lost**.
- A repeated number with the same content is a harmless **duplicate**; with different content it is a
  **conflicting duplicate**, which means corruption.
- Numbers jumping back to 1 mid-session is a **sequence reset** (the handler restarted).
- Heartbeats reveal loss even when no data follows. No heartbeat for 1.6 s means **disconnected**.
- Latency is compared with **each feed's own learned normal**, so the slow SIP is never flagged.
- Timestamps from the future mean **clock skew**.

### L3: Feature windows (`features.py`)
Every second, each feed becomes **22 numbers describing its behaviour**: message rate compared with the
other feeds and with its own history, latency compared with its normal, lost/duplicate/reordered counts,
quarantine rate, deviation from consensus, share of frozen or silent symbols, heartbeat age, future
timestamps, identical prices across stocks, and more. The AI never sees raw prices, only behaviour.

### L4: AI (`ml.py`) — see [section 10](#10-where-the-ai-is).

### L5: Cross-feed consensus (`consensus.py`), "do independent feeds agree?"
- For every price update, each feed's value is compared with the **trust-weighted median** of all feeds,
  **as of the same exchange timestamp**. A feed is only compared once it has delivered everything up to
  that moment, so a slow-but-correct feed is never punished for being slow.
- A feed lagging too far behind sits out of the comparison (its lag is reported as DELAY instead).
- From this one mechanism come the faults no single-feed rule can see:
  - **FROZEN:** the feed keeps publishing fresh messages but its price never changes while the
    consensus keeps moving (and we never convict a feed whose value currently agrees with consensus).
  - **STALE:** the feed stopped publishing a symbol while the others keep updating it.
  - **Price spike:** a price the feed itself published that sits far from consensus.
  - **DRIFT:** a small, persistent bias, caught by a statistical CUSUM test.
- **Trust** per feed drops fast when a feed breaks and recovers slowly, so a broken feed stops
  influencing the consensus automatically.

### L6: Health, state and trust (`health.py`)
- Health score = 100 × Π(1 − weight × strength) over the findings in the last second.
- States: **HEALTHY → DEGRADED → CRITICAL**. They escalate immediately and recover only after 5 calm
  seconds (hysteresis, so they don't flicker).
- **Corroboration rule (false-alarm control):** the AI alone can only make a feed DEGRADED. CRITICAL
  needs hard evidence from a deterministic rule.
- **Absorption:** one root problem gives one card. A disconnect explains the gap that follows it, and a
  test-data leak explains the price spikes, so those child alerts are folded into the parent.

### L7: Incidents (`incidents.py`), "one card, not 500 alerts"
One incident per feed and fault type. It opens on first evidence, collects evidence while the fault
continues, escalates if it gets worse, and resolves after 10 s without new evidence. Each incident has
the evidence numbers, sample bad messages, the AI's top signals and diagnosis, a runbook action (for
example "Request retransmission of seq 15911-15911 (MoldUDP64 re-request); fail over to A (Direct)"),
a timeline, a written explanation and a root-cause analysis.

**Recommended source per symbol:** the fastest feed that is healthy and trusted for that symbol. This is
the "which feed should I use?" answer shown to everyone.

## 10. Where the AI is

| AI piece | What it does | Why it matters |
|---|---|---|
| **Isolation Forest** (anomaly model) | Trained **only on healthy data**; scores how unusual each feed's 1-second behaviour is | Catches new kinds of failure nobody wrote a rule for |
| **Calibrated threshold** | Set at the 99.5th percentile of scores on separate clean data | A deliberate false-alarm budget, not "flag 5% of everything" |
| **Random Forest** (fault classifier) | Trained on labelled Chaos Lab windows; names the fault type (GAP, FROZEN, DELAY, ...) with a probability | The operator's action depends on the type: retransmit, fail over or block |
| **Explanations** | Shows the top 3 features that look most unusual (robust z-scores) | Operators see *why* the AI is worried |
| **Online model for live mode** | Learns the live venues' normal behaviour from their first minutes, in the background | Replay-trained models don't describe crypto venues |
| **LLM explainer** (optional, Claude) | Rewrites incident evidence as a 3-line note for operators | Cuts triage time; strictly grounded (see section 13) |

The ablation study in [section 17](#17-how-we-proved-it-works) shows each layer adds something: consensus
adds frozen and drift detection, and ML adds the novel faults.

## 11. Root-cause analysis

File: `detect/rca.py`. It is **deterministic first**: it uses a scoring table, not a black box.

1. **Score 10 candidate causes** from the incident's signals, plus other open incidents on the same
   feed: network latency/path, packet loss, feed-handler freeze, decoder/schema change, source
   authentication failure, upstream/common-mode source, clock skew, test-data leak, reconnect storm,
   genuine market event.
2. **Scope test:** are the other feeds fine? Then it is a problem on this feed's path. All feeds hit?
   Then it is upstream, a shared problem, or the market.
3. **Blast radius:** which symbols are likely affected.
4. **Action:** the runbook step for the top cause, with a failover target (the most trusted healthy feed).

Example output after injecting packet loss and delay on feed C:
```
ROOT CAUSE ANALYSIS
Primary: Packet loss (46%)
Secondary: Network latency / path (41%)
Evidence: • Sequence gaps: 71 messages lost (latest seq 15911-15911) • Latency (p99) 14.3 ms -> 846.5 ms • Other feeds unaffected
Likely affected: AAPL, AMZN, GOOG, INTC, MSFT
Recommended action: Request retransmission of the missing range; failover to Feed A (Direct) if loss persists
```

## 12. Incident timeline

File: `server/timeline.py`. An ordered, per-feed story of what happened, built from the engine's output:
```
10:31:58  🟢 Feed healthy
10:32:04  🟡 Latency increasing
10:32:14  🟠 Sequence gap detected x14 in 8 s
10:32:17  🚨 Incident created: INC-0003 GAP (CRITICAL)
10:32:20  🔄 Alternate feed recommended: A (Direct) for AAPL, MSFT
10:32:45  🟢 Feed recovered
```
Entries are added on state changes, the first time each fault type appears, incident open/close,
recommendation changes and operator acknowledgements. Repeats are collapsed ("x14 in 8 s"), and each
entry links to its incident.

## 13. Incident explanations (template + LLM)

File: `explain/explainer.py`. Every incident gets a 3-line note: **what broke (with numbers), how we
know, what to do.**
- A **template** version is always available instantly and works offline.
- If `ANTHROPIC_API_KEY` is set, **Claude (Haiku 4.5)** writes a friendlier version in the background.
- **Grounding check:** the LLM text is accepted only if **every number in it appears in the incident's
  evidence**, it has exactly 3 lines, and it names only feeds that exist in the incident. Otherwise we
  keep the template. The LLM can never invent a number.

## 14. The chat assistant

File: `server/chat.py`. An "Ask FeedSentinel" panel on both dashboards.
- It answers from **read-only tools**: feed health, incidents, root cause, timeline, symbol status, your
  profile, evaluation metrics.
- **Every tool runs with the caller's own permissions** (role + regions). The assistant can never see
  data the user can't see.
- It **cites sources** (incident id or "feed C health @ 10:46:38") and only uses numbers the tools returned.
- It **refuses** out-of-region questions (e.g. `trader_eu` asking about AAPL), role-escalation attempts
  and write actions, and each refusal is written to the audit log.
- The tone depends on the role: technical for ops ("Root cause: Packet loss (46%)...") and plain for
  traders ("MSFT at 30.88: VERIFIED - you can rely on it").
- It works **fully offline**: it is an intent matcher over the tools, so it needs no Wi-Fi.

## 15. Security: login, roles, regions, audit

File: `server/security.py`. The design follows **Zero Trust (NIST SP 800-207)**: never trust by network
location, verify every request.

| Piece | What we did |
|---|---|
| **Passwords** | Hashed with **argon2** (the modern standard); never stored or logged in plain text |
| **Access token** | **JWT**, signed (HS256), valid **15 minutes**, carries user, role and regions |
| **Refresh token** | Random, stored only as a hash, sent as an **httpOnly Secure SameSite=Strict cookie**, **single-use and rotating**. Reusing an old one **revokes the whole session** (theft detection) |
| **Logout** | Revokes the token id and the session |
| **Roles (RBAC)** | `ops_analyst` and `admin` get the ops view, Chaos Lab and acknowledge; `trader` is read-only; `admin` also gets the audit log |
| **Regions (ABAC)** | Every feed and symbol has a region (the replay data is `US`). Data is filtered **on the server** for REST, WebSocket, incidents, timeline, metrics and chat |
| **One policy function** | `allow(user, action, region)` makes every access decision |
| **WebSocket** | Checks the token at connect time and closes with code 4401 when it is bad or expired |
| **Audit log** | Append-only, **hash-chained** SQLite table (each entry stores the hash of the previous one). It records logins, failures, refreshes, chaos actions, acknowledgements, chat questions and access denials. Admins can verify the chain |
| **Hardening** | Input validation (pydantic), security headers (CSP, X-Frame-Options, nosniff), rate limits on login and chat, same-origin only |
| **Fail open** | If a monitoring step errors, it is logged and monitoring continues. Security problems never stop detection |

Demo users (password `demo123`): `ops_us` (ops, US), `trader_us` (trader, US), `trader_eu` (trader, EU),
`admin` (all regions).

## 16. The screens

The design follows the team's "Premium Minimal Fintech" system (`docs/DESIGN.md`): lavender backgrounds,
dark navy stat bands, soft rounded cards, pill buttons. States always use **colour + icon + text**
(● HEALTHY, ⚠ DEGRADED, ✖ CRITICAL), never colour alone.

**Login (`/login`):** a headline ("Know which feed to trust."), a sign-in card and demo-account chips.
After login, each role goes to its own dashboard.

**Operations dashboard (`/ops`), for ops_analyst and admin:**
- Navy band: market clock, session, feeds healthy, open incidents, detector events per second.
- Controls: replay speed (1×-20×), pause, **Demo autopilot** (runs the demo story by itself).
- **Feed cards:** health 0-100, state, trust, messages/s, latency, gaps/duplicates/quarantined, AI
  anomaly score against its threshold, AI diagnosis, and a health sparkline.
- **Recommended source** for each symbol.
- **Price chart:** consensus against every feed, with incident markers.
- **Incidents** list. Click one for the "Why?" drawer: root-cause analysis, runbook, **Acknowledge**,
  explanation, evidence, AI signals and raw bad messages.
- **Incident timeline** panel.
- **Chaos Lab:** pick a target feed and symbols, click a scenario, and a **stopwatch** shows the time to
  detect (in market seconds). For market events it shows "false alerts: 0".
- Tabs: **Evaluation** (measured results), **About** (how it works, what is real or simulated), **Audit**
  (admin only).
- Connection pill: `live` / `polling` / `stale`. Every WebSocket message carries a sequence number; if the
  browser misses one it re-syncs, and if the socket drops it polls every 2 s.

**Trader dashboard (`/trader`), for traders:**
- A watchlist card per stock: price, trust badge (**VERIFIED / USE CAUTION / DO NOT USE**) with the reason
  ("2 independent feeds agree; Vendor excluded"), which feed it comes from and how fresh it is.
- Plain-language alerts: "MSFT prices from Vendor feed unreliable since 10:32:14; using Direct feed".
- No raw messages, no Chaos Lab, no root-cause internals.

## 17. How we proved it works

Script: `feedsentinel/ml/evaluate.py`. It trains the AI and evaluates the whole system, **split by time
of day so the test data is never seen in training**:

| Block | Market time | Used for |
|---|---|---|
| Train | 09:45-12:30 | Clean windows → anomaly model; Chaos Lab windows → fault classifier |
| Calibrate | 12:30-13:15 | Clean windows → anomaly threshold (99.5th percentile) |
| Test | 13:30-16:00 | Everything below |

**Headline results (test block):**
- **1.0 false alarm per hour** on a clean hour that included **6 real market events** (halts and 3% moves),
  with **0 alerts on those market events**.
- **Incident precision 100%:** all 50 incidents raised matched a real injected fault.
- **0 alerts on healthy feeds** while another feed was broken.
- Fault classifier: **87% accuracy**, macro F1 0.74.

**Per fault (2 test episodes each; TTD = time to detect, market seconds):**

| Fault | Detected | Right fault type | TTD median | TTD p95 |
|---|---|---|---|---|
| Packet loss | 100% | 100% | 8.5 s | 11.7 s |
| Delay | 100% | 100% | 2.0 s | 2.0 s |
| Duplicates | 100% | 100% | 2.0 s | 2.9 s |
| Conflicting duplicates | 100% | 100% | 5.0 s | 7.7 s |
| Out-of-order | 100% | 100% | 1.5 s | 2.0 s |
| Garbled | 100% | 100% | 1.0 s | 1.0 s |
| Frozen feed | 100% | 100% | 3.5 s | 4.9 s |
| Silent feed | 100% | 100% | 3.0 s | 3.9 s |
| Disconnect | 100% | 100% | 2.0 s | 2.0 s |
| Price spikes | 100% | 100% | 11.5 s | 16.5 s |
| Decimal shift | 100% | 100% | 1.0 s | 1.0 s |
| Slow drift | 100% | 100% | 7.0 s | 8.8 s |
| Clock skew | 100% | 100% | 1.0 s | 1.0 s |
| Sequence reset | 100% | 100% | 1.0 s | 1.0 s |
| 2017 test-data leak | 100% | 100% | 3.0 s | 3.9 s |
| 2013 reconnect storm | 100% | 100% | 1.0 s | 1.0 s |
| Silent throttling (novel) | 100% | 100% | 2.0 s | 2.0 s |
| Volume corruption (novel) | 50% | 50% | 10.0 s | 10.0 s |

Slow detection for packet loss and price spikes is mostly because those faults are random and sparse:
at low loss rates or low spike probability the first bad message can take several seconds to appear.

**Ablation: does each layer earn its place?** (share of faults caught with the right fault type)

| Configuration | Overall | What it misses |
|---|---|---|
| Rules only (L1-L3) | **78%** | Frozen feed, slow drift, both novel faults |
| Rules + consensus (L5) | **89%** | Both novel faults |
| Rules + consensus + AI (L4) | **97%** | Volume corruption 1 of 2 |

**Honest caveats:** only 2 episodes per fault type in the test run, so the percentages are indicative, not
precise. All numbers come from `reports/metrics.json` and can be regenerated in about 2 minutes.

**Speed:** the engine processes about 40,000 messages per second on one CPU core without ML, and about
9,000 per second with the ML models scoring every window. Both are far above what the demo needs.

## 18. Tech stack

| Part | What we used | Why |
|---|---|---|
| Language | **Python 3.14** | One language for the whole team |
| Data | **pandas, NumPy** | Loading CSVs, fast columnar tape |
| AI / ML | **scikit-learn** (Isolation Forest, Random Forest), **joblib** | Trains in seconds on a CPU, explainable, no GPU needed |
| LLM (optional) | **Claude Haiku 4.5** via the **Anthropic SDK** | Fast incident notes; template fallback offline |
| Backend | **FastAPI**, **uvicorn**, **WebSockets**, **pydantic** | Live push to the browser, validated inputs |
| Security | **PyJWT**, **argon2-cffi**, **SQLite** (standard library) | Tokens, password hashing, tamper-evident audit log |
| Live data | **websockets**, **httpx** | Crypto exchange streams |
| Frontend | Plain **HTML/CSS/JavaScript** + **Chart.js** (saved locally) | No build step, works offline on a projector |
| Testing | **pytest** (88 tests) | Parsers and explainer covered |
| Deliberately not used | Docker, a Kafka broker, Spark, deep learning | Setup risk with no demo value |

## 19. Folder structure

```
feedsentinel/
  schema.py            the standard message format, fault codes, runbook actions
  config.py            every threshold in one place (replay and live presets)
  refdata.py           Nasdaq Symbol Directory loader
  timeutil.py          time conversions (nanoseconds, New York time)
  data/lobster.py      LOBSTER CSV → merged, cached tape
  sim/feeds.py         one source → 3 feeds (latency, sequence numbers, heartbeats)
  sim/chaos.py         Chaos Lab: 20 scenarios + ground truth
  detect/context.py    L0 market context
  detect/gatekeeper.py L1 validation and quarantine
  detect/sequencer.py  L2 gaps, duplicates, reorder, resets
  detect/features.py   L3 22 behaviour features
  detect/ml.py         L4 AI models and explanations
  detect/consensus.py  L5 trust-weighted consensus, frozen / stale / spike / drift
  detect/health.py     L6 health score, states, trust, absorption
  detect/incidents.py  L7 incidents
  detect/engine.py     wires L0-L7 together
  detect/rca.py        root-cause analysis
  explain/explainer.py 3-line notes: template + grounded Claude
  runtime/replay.py    replay session (tape → feeds → chaos → engine)
  runtime/live.py      live crypto venue adapters
  ml/evaluate.py       train + evaluate + ablation → models/ and reports/metrics.json
  server/app.py        FastAPI server, WebSocket, all routes
  server/security.py   login, tokens, roles/regions policy, audit log, rate limits
  server/timeline.py   incident timeline
  server/chat.py       chat assistant
  __main__.py          `python -m feedsentinel serve`
dashboard/             index.html, app.js, style.css, vendor/chart.umd.min.js
docs/API.md            server ⇄ browser contract
docs/DESIGN.md         design system
data/reference/        nasdaqlisted.txt (data/raw and data/processed are git-ignored)
models/                trained models
reports/               metrics.json, audit.db
tests/                 pytest tests
tools/mock_server.py   fake server used to build the UI
```

## 20. How to run it and the demo script

```bash
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt pyjwt argon2-cffi
.venv/Scripts/python.exe -m feedsentinel.ml.evaluate     # optional: retrain + re-measure (~2 min)
.venv/Scripts/python.exe -m feedsentinel serve --port 8000
```
Open **http://127.0.0.1:8000**. Every demo account uses the password `demo123`.

**2-minute demo:**
1. **Hook:** "Is a flat price line a broken feed or a closed market? In 2017, Apple, Amazon, Microsoft
   and Google all showed $123.47. Telling a real market from a broken feed, in real time, is the problem."
2. Log in as **ops_us**. Three healthy feeds of real Nasdaq data. "B, the SIP, is slower but healthy:
   slow is not broken."
3. Chaos Lab → **Packet loss** on C: C turns red, the stopwatch shows time to detect, and the timeline
   fills in. Open the incident to show the **root-cause analysis** and failover to A.
4. **Frozen feed**: "Heartbeats fine, sequence perfect, every rule passes, and it is still broken."
   Consensus catches it.
5. **2017 test-data leak**: blocked within seconds.
6. **Real market move**: all feeds agree, no alert.
7. **Silent throttling**: no rule exists for it, and the AI catches it.
8. **Ask FeedSentinel**: "Why is Feed C red?"
9. Sign out, log in as **trader_us**: plain-language watchlist and trust badges.
10. Evaluation tab: false alarms per hour, detection, ablation.
11. **Close:** "In December Nasdaq moves to 23-hour trading. At 3 a.m. the market is quiet by design.
    FeedSentinel knows the difference between quiet and broken."

Or click **Demo autopilot** and it runs packet loss → frozen → 2017 leak → market move by itself.

## 21. Bugs we found and fixed while building

These show the system was actually tested, not just drawn:

| Symptom | Cause | Fix |
|---|---|---|
| A **delayed** feed was also called FROZEN and RATE_STORM | It was compared at moments it had not reported yet, and latency shifted bursts between seconds | Each feed has a watermark; a lagging feed sits out of the consensus; storm check uses 3-second sums |
| **Duplicates** also looked like reordering | The simulator sometimes delivered the copy before the original | Copies now always arrive after the original |
| Decimal shift / test leak also opened a STALE card | Quarantined data looked like "no data" | Those fingerprints absorb STALE; a parent incident merges open child incidents |
| Rare false STALE on the SIP | A latency burst still in flight | Conservative watermark and a re-check of the newest data before convicting |
| False PRICE_SPIKE during a **real market move** | The slower feed had not yet delivered that update | A spike counts only if the feed itself published the bad price at that moment |
| False RATE_STORM during a market event | High rate alone isn't the 2013 signature | Requires corroboration: replays, unknown symbols or malformed messages |
| Throttled feed sometimes called FROZEN | Its sampled value happened to equal old values | Never convict a feed whose value currently agrees with consensus |
| Test stocks (ZVZZT) would page people | They legitimately appear on production feeds | One test-issue message is filtered, not an incident; the leak needs identical prices across stocks |
| Price chart only 150 px tall | The chart container had no height | CSS fix plus an automatic resize check |

## 22. What is not built yet

- **Multi-format source adapters** (JSON/CSV/SQL/Kafka with a mapping file per source, a demo SQLite
  vendor database, SCHEMA_DRIFT alerts, lineage fields). Today: CSV replay plus the JSON live adapters.
- **TOTP MFA** and **HMAC-signed source records** (UNAUTHENTICATED_SOURCE). Tokens honestly say `mfa=false`.
- An **EU data feed**. Region scoping is enforced, but only US data exists, so `trader_eu` sees an empty view.
- **Live mode inside the server.** The live adapters work on their own:
  `python -m feedsentinel.runtime.live --seconds 20`.
- **LLM rephrasing in chat.** The chat is offline intent matching over the same scoped tools.
- Roadmap: native ITCH 5.0 binary decoder, Nasdaq Cloud Data Service (Kafka) consumer, order-book
  rebuild checks, alerting integrations (Slack/PagerDuty), night-session baselines for 23-hour trading.
- Note: this PC's clock is about 3.5 s behind real time. Run `w32tm /resync` before any live-data demo.
  In live mode the engine also estimates a clock offset shared by all venues ("if every venue looks
  skewed the same way, it is our clock").

## 23. Questions judges will ask

| Question | Answer |
|---|---|
| Why AI and not just rules? | Rules catch the certain cases. Consensus catches frozen and drift, which rules can't see. The AI caught silent throttling, which had no rule and was never in training. The ablation table proves each layer adds detection. |
| How do you avoid false alarms? | Each feed's own learned baselines, cross-feed comparison as of exchange time, hysteresis, the corroboration rule (AI alone can't go CRITICAL), market-context suppression, incident grouping. We **measure** it: 1.0 per hour, 0 on market events. |
| Real market move or broken feed? | Independent feeds agree on a real move; a broken feed disagrees. Halts and sessions come from the feeds themselves, by majority vote. |
| What if every feed is wrong the same way? | Consensus can't see a common-mode failure. Per-feed rules, reference data and cross-stock checks (like identical prices) still run, and the root-cause analysis reports "all feeds affected: upstream or common-mode". In production we would add one truly independent source. |
| How fast? | Per-message checks take microseconds; window checks run every second. Typical detection is 1-4 s, and median time to detect is ≤ 3.5 s for 13 of the 18 fault types. |
| Does it scale? | State is per feed and symbol, so it partitions by symbol (e.g. Kafka partitions). The AI runs per feed per second, not per message. The hot path can move to C++/Rust. |
| Can timestamps be trusted? | We compare exchange and receive times, detect future timestamps as clock skew, and in live mode estimate our own clock offset from all venues. |
| What if the LLM makes things up? | Any number not in the evidence means the LLM text is rejected and the template is used. Chat answers only from tool results and cites sources. |
| What is simulated? | The three feed paths and the faults. The data, detectors, models, evaluation, dashboards and security are real. |
| What if the detector fails? | Self-health (events/s, lag) is shown on screen. It fails open: errors are logged and it never blocks the feed it watches. |
