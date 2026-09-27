"""AI assistant: answers from read-only tools that run with the caller's scope (role + regions).

Offline-first: an intent matcher over the same tools with templated answers, so it works with no
network. Every number in an answer comes from a tool result, with its source cited. Feed and tool
data are treated as data, never as instructions (the matcher never executes anything it reads).
"""
from __future__ import annotations

import re

from .. import schema as S

ESCALATION = re.compile(r"\b(make me|grant|give me)\b.*\b(admin|ops|access|role)\b|\b(delete|drop|disable|inject|"
                        r"chaos|shut ?down|turn off|ignore (all|previous|your) instructions)\b", re.I)
FEED_WORDS = {"direct": "A", "sip": "B", "vendor": "C"}


def _feed_in(text: str, feed_ids) -> str | None:
    m = re.search(r"\bfeed\s*([a-z])\b", text, re.I)
    if m and m.group(1).upper() in feed_ids:
        return m.group(1).upper()
    for w, f in FEED_WORDS.items():
        if re.search(rf"\b{w}\b", text, re.I) and f in feed_ids:
            return f
    return None


def _symbols_in(text: str, universe) -> list[str]:
    toks = {t.upper() for t in re.findall(r"[A-Za-z][A-Za-z.\-]{1,6}", text)}
    return [s for s in universe if s in toks]


def answer(message: str, user, rt) -> dict:
    """rt: the server runtime exposing scoped read-only tools."""
    text = (message or "").strip()[:500]
    tools, sources = [], []
    ops = user.role in ("ops_analyst", "admin")
    feeds = rt.tool_feed_health(user)
    tools.append("get_feed_health")
    feed_ids = [f["id"] for f in feeds]
    sym_region = rt.symbol_regions()

    def refuse(msg: str, why: str) -> dict:
        rt.audit(user.username, "chat_refused", why, "denied", text)
        return {"answer": msg, "sources": [], "tools": tools, "refused": True}

    if ESCALATION.search(text):
        return refuse("I can only read monitoring data for your role and regions. I can't change roles, "
                      "run Chaos Lab actions or take write actions from chat.", "escalation")
    mentioned = _symbols_in(text, sym_region)
    for s in mentioned:
        if sym_region[s] not in user.regions:
            return refuse(f"{s} is a {sym_region[s]} instrument and your access covers "
                          f"{', '.join(user.regions)} only, so I can't share anything about it.", f"region:{s}")
    feed = _feed_in(text, rt.all_feed_ids())
    if feed and feed not in feed_ids:
        return refuse(f"Feed {feed} is outside your regions ({', '.join(user.regions)}).", f"feed:{feed}")
    low = text.lower()

    # --- profile
    if re.search(r"\b(my (role|regions?|profile|access)|who am i|what can i see)\b", low):
        tools.append("get_my_profile")
        return {"answer": f"You are {user.display_name} ({user.username}), role {user.role}, regions "
                          f"{', '.join(user.regions)}. You land on the {'operations' if ops else 'trader'} dashboard.",
                "sources": ["profile"], "tools": tools, "refused": False}
    if not feed_ids:
        return {"answer": f"No feeds in your regions ({', '.join(user.regions)}) are streaming right now, so "
                          "there is nothing to report.", "sources": [], "tools": tools, "refused": False}
    snap = rt.clock()

    # --- symbol trust
    if mentioned or re.search(r"\b(trust|which feed|watchlist|price)\b", low):
        tools.append("get_symbol_status")
        rows = rt.tool_symbol_status(user)
        if mentioned:
            rows = [r for r in rows if r["symbol"] in mentioned]
        lines = []
        for r in rows:
            if ops:
                lines.append(f"{r['symbol']}: {r['trust']} ({r['trust_reason']}); recommended source "
                             f"{r['source']} ({r['source_name']}), price {r['price']}, updated {r['updated_s']} s ago.")
            else:
                verdict = {"VERIFIED": "you can rely on it", "USE CAUTION": "use with caution",
                           "DO NOT USE": "do not use it for decisions right now"}[r["trust"]]
                lines.append(f"{r['symbol']} at {r['price']}: {r['trust']} - {verdict}. Showing the "
                             f"{r['source_name']} feed ({r['trust_reason']}).")
            sources.append(f"{r['symbol']} status @ {snap}")
        return {"answer": "\n".join(lines) or "No symbols in your watchlist.", "sources": sources,
                "tools": tools, "refused": False}

    # --- why is feed X red / what's wrong
    if feed or re.search(r"\b(red|critical|degraded|wrong|problem|broken|why)\b", low):
        if feed is None:
            bad = [f for f in feeds if f["state"] != S.HEALTHY]
            feed = bad[0]["id"] if bad else None
        if feed is None:
            return {"answer": f"All your feeds are healthy as of {snap}: " + ", ".join(
                f"{f['id']} {f['name']} {f['health']:.0f}/100" for f in feeds) + ".",
                "sources": [f"feed health @ {snap}"], "tools": tools, "refused": False}
        fv = next(f for f in feeds if f["id"] == feed)
        tools.append("get_incidents")
        incs = [i for i in rt.tool_incidents(user) if i.feed == feed and i.status == "OPEN"]
        if fv["state"] == S.HEALTHY and not incs:
            return {"answer": f"Feed {feed} ({fv['name']}) is healthy ({fv['health']:.0f}/100) as of {snap}.",
                    "sources": [f"feed {feed} health @ {snap}"], "tools": tools, "refused": False}
        top = sorted(incs, key=lambda i: -S.STATE_RANK[i.severity])[0] if incs else None
        lines = [f"Feed {feed} ({fv['name']}) is {fv['state']} (health {fv['health']:.0f}/100) as of {snap}."]
        sources.append(f"feed {feed} health @ {snap}")
        if top is not None:
            tools.append("get_rca")
            rca = rt.tool_rca(top)
            sources.append(top.id)
            if ops:
                lines.append(f"{top.id} {top.code}: {top.headline}.")
                lines.append(f"Root cause: {rca['primary']['cause']} ({rca['primary']['confidence']}%). "
                             f"{rca['scope']}.")
                lines.append("Evidence: " + "; ".join(rca["evidence"][:3]) + ".")
                lines.append(f"Action: {rca['action']}.")
            else:
                lines.append(f"Its prices are unreliable ({S.CODE_INFO[top.code]['title'].lower()}). "
                             f"FeedSentinel is using {rt.best_feed_name(exclude=feed)} instead, so your watchlist "
                             "stays on trusted data.")
        return {"answer": "\n".join(lines), "sources": sources, "tools": tools, "refused": False}

    # --- what happened / timeline
    if re.search(r"\b(happen|timeline|last|recent|incidents?|assigned|today|minutes?)\b", low):
        tools.append("get_timeline")
        m = re.search(r"last\s+(\d+)\s*min", low)
        minutes = int(m.group(1)) if m else 10
        entries = rt.tool_timeline(user, minutes)
        interesting = [e for e in entries if e["level"] not in ("ok",)][-8:]
        if not interesting:
            return {"answer": f"Nothing notable in the last {minutes} minutes (market time); all feeds stayed healthy.",
                    "sources": [f"timeline @ {snap}"], "tools": tools, "refused": False}
        lines = [f"Last {minutes} minutes (market time):"]
        for e in interesting:
            lines.append(f"{e['clock']} {e['icon']} {('Feed ' + e['feed'] + ': ') if e['feed'] else ''}{e['text']}"
                         + (f" x{e['count']}" if e["count"] > 1 and "x" not in e["text"] else ""))
            if e.get("incident_id"):
                sources.append(e["incident_id"])
        return {"answer": "\n".join(lines), "sources": sorted(set(sources)) or [f"timeline @ {snap}"],
                "tools": tools, "refused": False}

    if re.search(r"\b(metric|accuracy|false alarm|precision|recall|evaluation)\b", low):
        tools.append("get_metrics")
        m = rt.tool_metrics()
        clean = m.get("clean") or {}
        if not clean:
            return {"answer": "The evaluation report has not been generated yet.", "sources": [], "tools": tools,
                    "refused": False}
        det = m.get("faults") or []
        rate = sum(f["detected"] for f in det) / max(1, sum(f["episodes"] for f in det))
        return {"answer": f"On {clean.get('market_hours')} market hours of clean data there were "
                          f"{clean.get('false_alarms_per_hour')} false alarms per hour; across "
                          f"{sum(f['episodes'] for f in det)} injected faults the detection rate was {rate:.0%}.",
                "sources": ["reports/metrics.json"], "tools": tools, "refused": False}

    return {"answer": "I can answer from live monitoring data: why a feed is red, which feed to trust for a symbol, "
                      "what happened in the last N minutes, your role and regions, and evaluation metrics.",
            "sources": [], "tools": tools, "refused": False}
