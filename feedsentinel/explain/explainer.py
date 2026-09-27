"""Incident explainer: a three-line note for the on-call feed operator.

    Line 1  what broke, with the numbers
    Line 2  how we know (which detection layer, which evidence)
    Line 3  what to do

A deterministic template is always available instantly and never touches the network. When Claude
is enabled, a background worker asks it for a better-worded note, and that note replaces the
template only if it passes a grounding check: exactly three lines, a last line that starts with
"Action:", every number present in the incident, and no feed id the incident does not mention.
Anything else (timeout, API error, refusal, invented number) falls back to the template.

Environment
    ANTHROPIC_API_KEY        turns the Claude path on when ``Explainer(enabled=None)`` (auto)
    FEEDSENTINEL_LLM         "off" keeps auto mode on the template even with a key set
    FEEDSENTINEL_LLM_MODEL   Claude model id (default ``claude-haiku-4-5``, the fastest current model)
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import string
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, NamedTuple

from feedsentinel.schema import (
    CLOCK_SKEW, CODE_INFO, CONFLICTING_DUPLICATE, DECIMAL_SHIFT, DELAY, DISCONNECT, DRIFT, DUPLICATE,
    FROZEN, GAP, GARBLED, ML_ANOMALY, OUT_OF_ORDER, PRICE_SPIKE, RATE_STORM, SEQ_RESET, STALE,
    TEST_DATA_LEAK,
)

__all__ = [
    "Explainer", "DEFAULT_MODEL", "SYSTEM_PROMPT", "extract_numbers", "is_grounded",
    "normalise_output", "resolve_action", "build_llm_payload",
]

log = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-haiku-4-5"
MAX_TOKENS = 1024           # the note is ~80 tokens; headroom for models that think first
MAX_WORKERS = 2             # concurrent Claude calls
MAX_WORDS = 90              # the prompt asks for <= 60; this only rejects runaway output
ERROR_BACKOFF_S = 30.0      # after a failed call, don't retry the same incident version for this long
CACHE_SIZE = 512
ALWAYS_ALLOWED = frozenset({1.0, 2.0, 3.0})
_OFF_VALUES = {"off", "0", "false", "no", "none", "template"}

# --------------------------------------------------------------------------- prompt
SYSTEM_PROMPT = """\
You are the market-data operations assistant inside FeedSentinel, an exchange's feed-integrity \
monitor. You write the incident note that the on-call feed operator reads in the first seconds \
of an incident to decide what to do.

You receive one incident as JSON: the finding code and title, a headline, the evidence the \
detectors measured (key suffixes: _ms milliseconds, _s seconds, _pct percent, _bps basis points), \
optional model outputs, and the runbook action.

Write exactly 3 lines of plain text, at most 60 words in total, with no markdown, bullets or line numbers:
Line 1: what broke. Name the feed (id and name) and the fault, with the key numbers.
Line 2: begin with "How we know:". The evidence that shows it, and why it points to a feed fault \
rather than a real market move where the evidence supports that.
Line 3: begin with "Action:". The given action, tightened if needed but with the same meaning. \
Keep its feed ids and sequence numbers and add no new steps.

Every number you write must appear in the incident JSON. Do not calculate, convert units or \
change precision; if unsure, leave the number out. Do not speculate about causes the evidence \
does not show. The note is checked automatically and discarded if it contains a number that is \
not in the incident or is not exactly 3 lines."""

_PAYLOAD_KEYS = ("id", "feed", "feed_name", "code", "title", "headline", "severity", "status",
                 "symbols", "evidence", "top_features", "diagnosis", "ttd_s")


def build_llm_payload(incident: dict) -> dict:
    """The subset of the incident sent to Claude (samples trimmed to 2, lists capped)."""
    incident = incident if isinstance(incident, dict) else {}
    payload: dict[str, Any] = {}
    for key in _PAYLOAD_KEYS:
        value = incident.get(key)
        if value is None or (isinstance(value, (str, list, dict)) and not value):
            continue
        payload[key] = value
    layer = CODE_INFO.get(_code(incident), {}).get("layer")
    if layer:
        payload["detection_layer"] = layer
    payload["action"] = resolve_action(incident)
    if isinstance(payload.get("top_features"), list):
        payload["top_features"] = payload["top_features"][:5]
    if isinstance(payload.get("symbols"), list):
        payload["symbols"] = payload["symbols"][:10]
    samples = incident.get("samples")
    if isinstance(samples, list) and samples:
        payload["samples"] = samples[:2]
    return payload


def build_user_prompt(incident: dict) -> str:
    blob = json.dumps(build_llm_payload(incident), sort_keys=True, default=str, ensure_ascii=False)
    return f"Incident:\n{blob}\n\nWrite the 3-line note."


# --------------------------------------------------------------------------- template text
LAYER_NAMES = {"L1": "validation", "L2": "transport", "L3": "rate", "L4": "anomaly model",
               "L5": "consensus"}

# evidence key -> (human label, unit suffix). Unknown keys render as "key: value".
EVIDENCE_LABELS: dict[str, tuple[str, str]] = {
    "missing": ("missing messages", ""), "gap_first": ("first missing seq", ""),
    "gap_last": ("last missing seq", ""), "loss_rate_pct": ("loss rate", "%"),
    "lat_p99_ms": ("p99 latency", " ms"), "baseline_p99_ms": ("baseline p99 latency", " ms"),
    "lat_ratio": ("latency vs baseline", "x"),
    "duplicates": ("duplicate messages", ""), "dup_rate_pct": ("duplicate rate", "%"),
    "conflicts": ("conflicting sequence numbers", ""), "example_seq": ("example seq", ""),
    "reordered": ("out-of-order messages", ""),
    "from_seq": ("seq before reset", ""), "to_seq": ("seq after reset", ""),
    "quarantined": ("quarantined messages", ""), "quarantine_rate_pct": ("quarantine rate", "%"),
    "top_reason": ("top reason", ""), "reasons": ("reasons", ""),
    "frozen_symbols": ("frozen symbols", ""), "symbols": ("symbols", ""),
    "frozen_for_s": ("frozen for", " s"), "peer_changes": ("peer price changes", ""),
    "silent_for_s": ("silent for", " s"), "peer_msgs": ("peer messages", ""),
    "heartbeat_age_s": ("last heartbeat", " s ago"), "last_seq": ("last seq", ""),
    "resume_seq": ("resume from seq", ""),
    "max_dev_bps": ("max deviation", " bps"), "symbol": ("symbol", ""), "price": ("price", ""),
    "consensus": ("consensus", ""), "ratio": ("price ratio", "x"),
    "power": ("decimal places shifted", ""),
    "bias_bps": ("bias", " bps"), "cusum": ("CUSUM", ""),
    "identical_price": ("identical price", ""), "symbols_at_price": ("symbols at that price", ""),
    "test_symbols": ("test symbols", ""),
    "rate_ratio": ("rate vs normal", "x"), "msg_rate": ("message rate", " msgs/s"),
    "peer_rate": ("peer rate", " msgs/s"), "unknown_symbols": ("unknown symbols", ""),
    "skew_ms": ("clock skew", " ms"), "future_msgs": ("future-stamped messages", ""),
    "score": ("anomaly score", ""), "threshold": ("threshold", ""),
    "top_feature": ("top feature", ""), "diagnosis": ("classifier diagnosis", ""),
    "best_feed": ("best feed", ""),
}


class _CodeText(NamedTuple):
    what: tuple[str, ...]            # line-1 alternatives (first fully renderable wins)
    lead: str                        # line-2 reasoning, number-free, always true for the code
    how: tuple[str | tuple[str, ...], ...]   # line-2 evidence clauses; a tuple = alternatives


CODE_TEXT: dict[str, _CodeText] = {
    GAP: _CodeText(
        ("{missing} messages lost (seq {gap_first}-{gap_last})",
         "seq {gap_first}-{gap_last} never arrived", "{missing} messages lost",
         "{loss_rate_pct}% of messages lost"),
        "sequence numbers skipped ahead, so messages were lost between the source and us",
        ("missing seq {gap_first}-{gap_last}", "{missing} messages missing",
         "loss rate {loss_rate_pct}%")),
    DELAY: _CodeText(
        ("p99 latency {lat_p99_ms} ms vs {baseline_p99_ms} ms normally",
         "p99 latency {lat_p99_ms} ms", "latency {lat_ratio}x normal"),
        "messages still arrive, but far behind this feed's normal latency",
        (("p99 latency {lat_p99_ms} ms vs baseline {baseline_p99_ms} ms",
          "p99 latency {lat_p99_ms} ms"), "{lat_ratio}x the baseline")),
    DUPLICATE: _CodeText(
        ("{duplicates} duplicate messages ({dup_rate_pct}% of traffic)",
         "{duplicates} duplicate messages", "{dup_rate_pct}% of messages duplicated"),
        "already-seen sequence numbers arrived again with identical content",
        ("{duplicates} duplicates", "duplicate rate {dup_rate_pct}%")),
    CONFLICTING_DUPLICATE: _CodeText(
        ("{conflicts} sequence numbers arrived twice with different content",
         "seq {example_seq} arrived twice with different content"),
        "the same sequence number carried different content, and a genuine retransmission is "
        "always identical",
        ("{conflicts} conflicting sequence numbers", "e.g. seq {example_seq}")),
    OUT_OF_ORDER: _CodeText(
        ("{reordered} messages delivered out of order",),
        "messages arrived with lower sequence numbers after higher ones had already been received",
        ("{reordered} messages out of order",)),
    SEQ_RESET: _CodeText(
        ("sequence jumped back from {from_seq} to {to_seq} mid-session",
         "sequence restarted at {to_seq} mid-session"),
        "sequence numbers went backwards within the same session instead of continuing",
        (("seq went from {from_seq} to {to_seq}", "restarted at {to_seq}"),)),
    GARBLED: _CodeText(
        ("{quarantined} malformed messages quarantined ({quarantine_rate_pct}% of traffic)",
         "{quarantined} malformed messages quarantined", "{quarantine_rate_pct}% of messages malformed"),
        "messages failed field and schema validation, so they were quarantined instead of published",
        ("{quarantined} messages quarantined", "quarantine rate {quarantine_rate_pct}%",
         ("top reason {top_reason}", "reasons {reasons}"))),
    FROZEN: _CodeText(
        ("{frozen_symbols} symbols flat for {frozen_for_s} s while peers moved {peer_changes} times",
         "{frozen_symbols} symbols flat for {frozen_for_s} s",
         "prices flat for {frozen_for_s} s while peers moved {peer_changes} times",
         "prices flat for {frozen_for_s} s"),
        "heartbeats and sequence numbers look normal, but prices stopped moving while the other "
        "feeds kept updating",
        (("{frozen_symbols} symbols unchanged for {frozen_for_s} s", "unchanged for {frozen_for_s} s",
          "{frozen_symbols} symbols unchanged"),
         "peer feeds changed {peer_changes} times", "last heartbeat {heartbeat_age_s} s ago")),
    STALE: _CodeText(
        ("no data for {silent_for_s} s while peers sent {peer_msgs} messages",
         "no data for {silent_for_s} s"),
        "heartbeats keep arriving but market data does not, while the peer feeds are still publishing",
        ("silent for {silent_for_s} s", "peers sent {peer_msgs} messages",
         "last heartbeat {heartbeat_age_s} s ago")),
    DISCONNECT: _CodeText(
        ("no heartbeat for {heartbeat_age_s} s (last seq {last_seq})",
         "no heartbeat for {heartbeat_age_s} s", "session lost after seq {last_seq}"),
        "heartbeats have stopped as well as data, so the session itself is down",
        ("last heartbeat {heartbeat_age_s} s ago", "last seq {last_seq}",
         "resume from seq {resume_seq}")),
    PRICE_SPIKE: _CodeText(
        ("{symbol} at {price} vs consensus {consensus} ({max_dev_bps} bps off)",
         "{symbol} {max_dev_bps} bps away from consensus", "prices {max_dev_bps} bps away from consensus"),
        "this feed's price broke away from the cross-feed consensus that the other feeds agree on",
        ("{symbol} {price} vs consensus {consensus}", "deviation {max_dev_bps} bps")),
    DECIMAL_SHIFT: _CodeText(
        ("{symbol} at {price} vs consensus {consensus}, {ratio}x off",
         "{symbol} prices {ratio}x off consensus", "prices {ratio}x off consensus"),
        "the price is off from consensus by an exact power of ten, a scaling bug rather than a "
        "market move",
        ("{symbol} {price} vs consensus {consensus}", "ratio {ratio}x",
         "decimal point moved {power} place(s)")),
    DRIFT: _CodeText(
        ("{symbol} biased {bias_bps} bps vs consensus", "prices biased {bias_bps} bps vs consensus"),
        "a small, persistent offset from consensus keeps accumulating instead of reverting",
        ("bias {bias_bps} bps", "CUSUM {cusum}", "symbol {symbol}")),
    TEST_DATA_LEAK: _CodeText(
        ("{symbols_at_price} symbols printing the identical price {identical_price}",
         "test symbols in production ({test_symbols})",
         "identical price {identical_price} across symbols"),
        "unrelated symbols print one identical price, a test-data signature that real markets "
        "never produce",
        (("{symbols_at_price} symbols at {identical_price}", "identical price {identical_price}"),
         "test symbols {test_symbols}")),
    RATE_STORM: _CodeText(
        ("{msg_rate} msgs/s, {rate_ratio}x normal (peers {peer_rate} msgs/s)",
         "message rate {rate_ratio}x normal", "{msg_rate} msgs/s vs peers {peer_rate} msgs/s"),
        "the feed is sending far more than its peers, consistent with a reconnect loop replaying data",
        ("{rate_ratio}x normal rate", "{msg_rate} msgs/s vs peers {peer_rate} msgs/s",
         "{unknown_symbols} unknown symbols", "duplicate rate {dup_rate_pct}%")),
    CLOCK_SKEW: _CodeText(
        ("timestamps {skew_ms} ms in the future ({future_msgs} messages)",
         "timestamps {skew_ms} ms in the future", "{future_msgs} messages stamped in the future"),
        "source timestamps are later than the moment we received the messages, which is "
        "physically impossible",
        ("skew {skew_ms} ms", "{future_msgs} messages stamped in the future")),
    ML_ANOMALY: _CodeText(
        ("anomaly score {score} above threshold {threshold}",
         "behaviour outside the learned normal range (top driver {top_feature})",
         "behaviour outside the learned normal range (top driver {tf_name})"),
        "no single rule fired, but the model trained on clean data flags this behaviour as unusual",
        ("anomaly score {score} vs threshold {threshold}",
         ("top driver {tf_name} {tf_value} vs baseline {tf_baseline}", "top driver {top_feature}",
          "top driver {tf_name}"),
         ("classifier: {dx_label} (p {dx_p})", "classifier: {diagnosis}"))),
}
_GENERIC_LEAD = "the monitor flagged behaviour outside this feed's normal range"
_GENERIC_ACTION = "Investigate the feed and compare it against the peer feeds before acting"

_FORMATTER = string.Formatter()


def _one_line(value: Any) -> str:
    return " ".join(str(value).split())


def _code(incident: dict) -> str | None:
    code = incident.get("code")
    if not isinstance(code, str) or not code:
        return None
    return code if code in CODE_INFO else (code.upper() if code.upper() in CODE_INFO else code)


def _fmt(value: Any) -> str | None:
    """Human string for an evidence value, or None if there is nothing to show."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        if value.is_integer() and abs(value) < 1e15:
            return str(int(value))
        text = f"{value:.2f}" if abs(value) >= 1 else f"{value:.4f}"
        text = text.rstrip("0").rstrip(".")
        return text if text not in ("0", "-0") else f"{value:.3g}"
    if isinstance(value, (list, tuple, set)):
        items = [s for s in (_fmt(v) for v in list(value)) if s]
        if not items:
            return None
        return ", ".join(items[:5]) + (", ..." if len(items) > 5 else "")
    if isinstance(value, dict):
        items = [f"{k} {s}" for k, s in ((k, _fmt(v)) for k, v in list(value.items())[:5]) if s]
        return ", ".join(items) or None
    text = _one_line(value)
    if not text:
        return None
    return text if len(text) <= 60 else text[:57] + "..."


def _evidence(incident: dict) -> dict:
    ev = incident.get("evidence")
    return ev if isinstance(ev, dict) else {}


def _values(incident: dict) -> dict[str, str]:
    """Formatted evidence plus a few facts from the model outputs (tf_* and dx_*)."""
    vals: dict[str, str] = {}
    for key, value in _evidence(incident).items():
        text = _fmt(value)
        if text is not None:
            vals[str(key)] = text
    features = incident.get("top_features")
    if isinstance(features, list) and features and isinstance(features[0], dict):
        for src in ("name", "value", "baseline", "z"):
            text = _fmt(features[0].get(src))
            if text is not None:
                vals.setdefault(f"tf_{src}", text)
    diagnosis = incident.get("diagnosis")
    if isinstance(diagnosis, dict):
        for src in ("label", "p"):
            text = _fmt(diagnosis.get(src))
            if text is not None:
                vals.setdefault(f"dx_{src}", text)
    return vals


def _fields(template: str) -> list[str]:
    return [name for _, name, _, _ in _FORMATTER.parse(template) if name]


def _render(options: str | tuple[str, ...], vals: dict[str, str]) -> tuple[str, list[str]] | None:
    """First alternative whose placeholders are all present -> (text, fields used)."""
    for template in (options,) if isinstance(options, str) else options:
        names = _fields(template)
        if all(n in vals for n in names):
            return template.format_map(vals), names
    return None


def _labelled(key: str, text: str) -> str:
    if key in EVIDENCE_LABELS:
        label, unit = EVIDENCE_LABELS[key]
        return f"{label}: {text}{unit}"
    return f"{key}: {text}"


def _generic_items(incident: dict, exclude: set[str], limit: int) -> list[str]:
    items = []
    for key, value in _evidence(incident).items():
        if len(items) >= limit:
            break
        key = str(key)
        text = _fmt(value)
        if text is None or key in exclude or key == "best_feed":
            continue
        items.append(_labelled(key, text))
    return items


def _sentence(text: str) -> str:
    text = _one_line(text)
    return text if not text or text[-1] in ".!?" else text + "."


def _short_title(title: str) -> str:
    short = re.sub(r"\s*\([^)]*\)\s*$", "", title).strip()
    return short or title


def _layer_label(layer: Any) -> str:
    if not layer:
        return "detection"
    parts = [p.strip() for p in str(layer).split("/") if p.strip()]
    names = [LAYER_NAMES[p] for p in parts if p in LAYER_NAMES]
    return f"{layer} {' + '.join(names)}" if names else str(layer)


def resolve_action(incident: dict) -> str:
    """The incident's action, or the CODE_INFO runbook action filled from evidence."""
    incident = incident if isinstance(incident, dict) else {}
    action = _one_line(incident.get("action") or "")
    if action:
        return action
    template = CODE_INFO.get(_code(incident), {}).get("action")
    if not template:
        return _GENERIC_ACTION
    ev = _evidence(incident)

    def val(key: str) -> str | None:
        return _fmt(ev.get(key))

    if val("gap_first") is None or val("gap_last") is None:
        template = template.replace("seq {gap_first}-{gap_last}", "the missing sequence range")
    if val("resume_seq") is None:
        template = template.replace("from seq {resume_seq}", "from the last sequence number received")
    fallback = {"best_feed": "the healthiest peer feed"}
    return re.sub(r"\{(\w+)\}",
                  lambda m: val(m.group(1)) or fallback.get(m.group(1), m.group(1).replace("_", " ")),
                  template)


def _line1(incident: dict, code: str | None, vals: dict[str, str]) -> str:
    feed = _one_line(incident.get("feed") or "")
    name = _one_line(incident.get("feed_name") or "")
    if feed:
        who = f"Feed {feed}" + (f" ({name})" if name and name != feed else "")
    else:
        who = f"Feed {name}" if name else "Unknown feed"
    title = incident.get("title") or CODE_INFO.get(code, {}).get("title") or code or "Feed incident"
    title = _short_title(_one_line(title))

    headline = _one_line(incident.get("headline") or "")
    prefix = re.match(r"^([A-Z_]+):\s*", headline)           # drop a "FROZEN: " code prefix
    if prefix and prefix.group(1) in CODE_INFO:
        headline = headline[prefix.end():]
    headline = headline.rstrip(" .")
    if not headline and code in CODE_TEXT:
        rendered = _render(CODE_TEXT[code].what, vals)
        headline = rendered[0] if rendered else ""
    if not headline:
        headline = "; ".join(_generic_items(incident, set(), 2))
    if not headline:
        severity = _one_line(incident.get("severity") or "")
        headline = f"detected ({severity})" if severity else "detected"
    return _sentence(f"{who} — {title}: {headline}")


def _line2(incident: dict, code: str | None, vals: dict[str, str], line1: str) -> str:
    spec = CODE_TEXT.get(code)
    lead = spec.lead if spec else _GENERIC_LEAD
    clauses: list[str] = []
    used: set[str] = set()
    for options in spec.how if spec else ():
        rendered = _render(options, vals)
        if rendered is None:
            continue
        text, names = rendered
        used.update(names)
        if text not in line1 and text not in clauses:
            clauses.append(text)
        if len(clauses) == 3:
            break
    if len(clauses) < 2:
        clauses += [c for c in _generic_items(incident, used, 3 - len(clauses)) if c not in line1]
    layer = _layer_label(CODE_INFO.get(code, {}).get("layer"))
    body = f"{lead} — {'; '.join(clauses)}" if clauses else lead
    return _sentence(f"How we know ({layer}): {body}")


def _line3(incident: dict) -> str:
    action = re.sub(r"^action\s*:\s*", "", resolve_action(incident), flags=re.IGNORECASE)
    return _sentence(f"Action: {action or _GENERIC_ACTION}")


def render_template(incident: dict) -> str:
    """Exactly three lines, deterministic, built only from values present in the incident."""
    incident = incident if isinstance(incident, dict) else {}
    code = _code(incident)
    vals = _values(incident)
    line1 = _line1(incident, code, vals)
    return "\n".join((line1, _line2(incident, code, vals, line1), _line3(incident)))


# --------------------------------------------------------------------------- grounding check
class _NumToken(NamedTuple):
    raw: str
    value: float
    decimals: int
    exp: int
    pct: bool


# A number not glued to a word or another number ("L5", "p99", "MoldUDP64" are identifiers, not
# numbers). Handles 1,234  1.4  -3  $123.47  5%  1.2e-05, and both ends of ranges like 1200-1260
# (the "-" after a digit is a range dash, not a sign).
_NUM_RE = re.compile(
    r"(?<![A-Za-z0-9_.])"
    r"(?P<sign>[-−])?\$?"
    r"(?P<int>\d{1,3}(?:,\d{3})+(?!\d)|\d+)"
    r"(?P<frac>\.\d+)?"
    r"(?:[eE](?P<exp>[-+]?\d+))?"
    r"(?P<pct>\s?%)?"
)
_FEED_REF_RE = re.compile(
    r"\b(?:[Ff]eeds?|to|from)\s+(?P<a>[A-Z])(?![A-Za-z0-9])"   # "feed C", "over to A"
    r"|(?<![A-Za-z0-9])(?P<b>[A-Z])\s*\((?=[A-Za-z])"           # "A (Direct)"
)
_STANDALONE_LETTER_RE = re.compile(r"(?<![A-Za-z0-9])[A-Z](?![A-Za-z0-9])")
_BULLET_RE = re.compile(r"^\s*(?:[-*•·]\s+|#{1,6}\s+)")


def _number_tokens(text: str) -> list[_NumToken]:
    tokens = []
    for m in _NUM_RE.finditer(str(text or "")):
        frac = m.group("frac") or ""
        exp = int(m.group("exp") or 0)
        try:
            value = float(m.group("int").replace(",", "") + frac) * (10.0 ** exp)
        except (OverflowError, ValueError):
            continue
        if m.group("sign"):
            value = -value
        tokens.append(_NumToken(m.group(0).strip(), value, max(len(frac) - 1, 0), exp,
                                bool(m.group("pct"))))
    return tokens


def extract_numbers(text: str) -> list[float]:
    """Every number in ``text`` as a float, in order ("1,234" -> 1234.0, "-3 bps" -> -3.0)."""
    return [t.value for t in _number_tokens(text)]


def _walk(value: Any, numbers: list[float], strings: list[str], depth: int = 0) -> None:
    if depth > 8 or value is None or isinstance(value, bool):
        return
    if isinstance(value, (int, float)):
        if math.isfinite(value):
            numbers.append(abs(float(value)))
    elif isinstance(value, str):
        strings.append(value)
        numbers.extend(abs(t.value) for t in _number_tokens(value))
    elif isinstance(value, dict):
        for v in value.values():
            _walk(v, numbers, strings, depth + 1)
    elif isinstance(value, (list, tuple, set)):
        for v in value:
            _walk(v, numbers, strings, depth + 1)


def _incident_facts(incident: dict) -> tuple[list[float], set[str]]:
    """(absolute values of every number in the incident, feed ids it mentions)."""
    numbers: list[float] = []
    strings: list[str] = []
    _walk(incident, numbers, strings)
    action = resolve_action(incident)          # what the model was shown as the action
    strings.append(action)
    numbers.extend(abs(t.value) for t in _number_tokens(action))
    feeds = {_one_line(incident.get("feed"))} if incident.get("feed") else set()
    for s in strings:
        feeds.update(_STANDALONE_LETTER_RE.findall(s))
    return numbers, feeds


def _number_ok(tok: _NumToken, allowed: list[float]) -> bool:
    a = abs(tok.value)
    if tok.decimals == 0 and tok.exp == 0 and a in ALWAYS_ALLOWED:
        return True
    # Tolerance = half a unit in the last printed digit: "3.2" matches 3.21 or 3.249, not 3.26.
    tol = 0.5 * 10.0 ** (tok.exp - tok.decimals) + 1e-9 * max(1.0, a)
    if any(abs(a - b) <= tol for b in allowed):
        return True
    return tok.pct and any(abs(a - 100.0 * b) <= tol for b in allowed)   # 0.05 written as 5%


def normalise_output(text: str) -> str:
    """Trim, drop bullets/markdown emphasis and empty lines, collapse whitespace per line."""
    lines = []
    for raw in str(text or "").replace("\r", "\n").split("\n"):
        line = _BULLET_RE.sub("", raw).replace("**", "").replace("__", "").replace("`", "")
        line = " ".join(line.split())
        if line:
            lines.append(line)
    return "\n".join(lines)


def is_grounded(text: str, incident: dict) -> tuple[bool, list[str]]:
    """Accept an LLM note only if it is 3 lines and every number / feed id is in the incident.

    Returns (ok, problems). Numbers are compared by absolute value, with a tolerance of half a
    unit in the last digit written (rounding), and a "%" number may match a fraction x 100. The
    small integers 1, 2 and 3 are always allowed; numbers inside any text field of the incident
    (ids, headline, action, clock strings) count as present.
    """
    incident = incident if isinstance(incident, dict) else {}
    clean = normalise_output(text)
    lines = clean.split("\n") if clean else []
    problems: list[str] = []
    if len(lines) != 3:
        problems.append(f"expected 3 lines, got {len(lines)}")
    words = sum(len(line.split()) for line in lines)
    if words > MAX_WORDS:
        problems.append(f"too long: {words} words")
    if lines and not lines[-1].lower().startswith("action"):
        problems.append("last line must start with 'Action:'")
    allowed, feeds = _incident_facts(incident)
    body = "\n".join(lines)
    for tok in _number_tokens(body):
        if not _number_ok(tok, allowed):
            problems.append(f"number not in incident: {tok.raw}")
    for m in _FEED_REF_RE.finditer(body):
        feed = m.group("a") or m.group("b")
        if feed not in feeds:
            problems.append(f"feed not in incident: {feed}")
    return not problems, problems


# --------------------------------------------------------------------------- Claude client
_EFFORT_MODELS = re.compile(r"^claude-(?:opus-5|opus-4-[5-9]|sonnet-5|sonnet-4-6|fable|mythos)")


def _build_client(timeout_s: float):
    """Create the Anthropic client (no network). Tests monkeypatch this."""
    import anthropic
    # max_retries=0: timeout_s is the whole latency budget and the template covers any failure.
    return anthropic.Anthropic(timeout=timeout_s, max_retries=0)


class _LLMError(Exception):
    pass


class _Busy(Exception):
    pass


def _describe_error(exc: BaseException, model: str) -> tuple[str, bool]:
    """(message for status, fatal?) -- fatal errors switch the explainer to template mode."""
    if isinstance(exc, _LLMError):
        return str(exc), False
    try:
        return _classify_sdk_error(exc, model)
    except Exception:  # noqa: BLE001 - SDK missing or a different SDK version
        return f"{type(exc).__name__}: {exc}", False


def _classify_sdk_error(exc: BaseException, model: str) -> tuple[str, bool]:
    import anthropic
    credentials = getattr(anthropic, "CredentialsError", anthropic.AuthenticationError)
    if isinstance(exc, (anthropic.AuthenticationError, anthropic.PermissionDeniedError, credentials)):
        return f"Claude credentials rejected ({type(exc).__name__}); using templates", True
    if isinstance(exc, anthropic.NotFoundError):
        return f"model not found: {model}; using templates", True
    if isinstance(exc, anthropic.RateLimitError):
        return "rate limited by the Claude API", False
    if isinstance(exc, anthropic.BadRequestError):
        return f"bad request: {getattr(exc, 'message', exc)}", False
    if isinstance(exc, anthropic.APIStatusError):
        return f"Claude API error {exc.status_code}", False
    if isinstance(exc, anthropic.APITimeoutError):
        return "Claude API timed out", False
    if isinstance(exc, anthropic.APIConnectionError):
        return "cannot reach the Claude API", False
    return f"{type(exc).__name__}: {exc}", False


def _cache_key(incident: dict) -> tuple[str | None, str]:
    basis = {"code": incident.get("code"), "feed": incident.get("feed"),
             "evidence": incident.get("evidence")}
    try:
        blob = json.dumps(basis, sort_keys=True, default=str)
    except (TypeError, ValueError):
        blob = repr(basis)
    iid = incident.get("id")
    return (None if iid is None else str(iid)), hashlib.sha1(blob.encode("utf-8")).hexdigest()


def _safe_callback(callback: Callable, incident_id: Any, text: str, source: str) -> None:
    try:
        callback(incident_id, text, source)
    except Exception:  # noqa: BLE001 - a bad callback must not kill the worker
        log.exception("explainer callback failed for %s", incident_id)


# --------------------------------------------------------------------------- public API
class Explainer:
    """Three-line incident notes: instant template, optional grounded Claude rewrite.

    ``submit`` never blocks and never runs the callback inline: the callback is always invoked
    on a worker thread, so from asyncio hand results over with ``loop.call_soon_threadsafe``.
    """

    def __init__(self, enabled: bool | None = None, model: str | None = None,
                 timeout_s: float = 8.0, *, client: Any = None, max_workers: int = MAX_WORKERS):
        self.model = (model or os.environ.get("FEEDSENTINEL_LLM_MODEL") or DEFAULT_MODEL).strip()
        self.timeout_s = float(timeout_s)
        self._max_workers = max(1, int(max_workers))
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(self._max_workers)   # caps concurrent API calls
        self._executor: ThreadPoolExecutor | None = None
        self._closed = False
        self._cache: OrderedDict[tuple, str] = OrderedDict()   # key -> accepted text, "" = rejected
        self._backoff: dict[tuple, float] = {}
        self._inflight: set = set()
        self._queued: dict = {}
        self._calls = self._accepted = self._rejected = self._errors = 0
        self._last_error: str | None = None
        if enabled is None:
            enabled = (bool(os.environ.get("ANTHROPIC_API_KEY"))
                       and os.environ.get("FEEDSENTINEL_LLM", "").strip().lower() not in _OFF_VALUES)
        self._enabled = bool(enabled)
        self._client = client
        if self._enabled and self._client is None:
            try:
                self._client = _build_client(self.timeout_s)
            except Exception as exc:  # noqa: BLE001 - SDK missing, bad config, ...
                self._enabled = False
                self._last_error = f"Claude unavailable ({type(exc).__name__}: {exc}); using templates"

    # ----------------------------------------------------------------- status
    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def status(self) -> dict:
        with self._lock:
            return {
                "enabled": self._enabled,
                "provider": "claude" if self._enabled else "template",
                "model": self.model if self._enabled else None,
                "last_error": self._last_error,
                "calls": self._calls,
                "accepted": self._accepted,
                "rejected_ungrounded": self._rejected,
                "errors": self._errors,
            }

    # ----------------------------------------------------------------- text
    def template(self, incident: dict) -> str:
        return render_template(incident)

    def explain_sync(self, incident: dict) -> tuple[str, str]:
        """(text, "llm" | "template"). Blocks for at most about ``timeout_s``."""
        incident = incident if isinstance(incident, dict) else {}
        template = self.template(incident)
        if not self._enabled or self._closed:
            return template, "template"
        key = _cache_key(incident)
        with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                self._cache.move_to_end(key)
                return (cached, "llm") if cached else (template, "template")
            if self._backoff.get(key, 0.0) > time.monotonic():
                return template, "template"
        try:
            raw = self._call_llm(incident)
        except _Busy:
            return template, "template"
        except Exception as exc:  # noqa: BLE001 - every failure falls back to the template
            message, fatal = _describe_error(exc, self.model)
            log.warning("explainer: %s (incident %s)", message, incident.get("id"))
            with self._lock:
                self._errors += 1
                self._last_error = message
                self._backoff[key] = time.monotonic() + ERROR_BACKOFF_S
                if len(self._backoff) > 4 * CACHE_SIZE:
                    now = time.monotonic()
                    self._backoff = {k: t for k, t in self._backoff.items() if t > now}
                if fatal:
                    self._enabled = False
            return template, "template"

        text = normalise_output(raw)
        ok, problems = is_grounded(text, incident)
        with self._lock:
            if ok:
                self._accepted += 1
            else:
                self._rejected += 1
                self._last_error = "rejected ungrounded LLM note: " + "; ".join(problems[:3])
            self._cache[key] = text if ok else ""
            self._cache.move_to_end(key)
            while len(self._cache) > CACHE_SIZE:
                self._cache.popitem(last=False)
        if not ok:
            log.info("explainer: rejected LLM note for %s: %s", incident.get("id"), problems)
            return template, "template"
        return text, "llm"

    def _call_llm(self, incident: dict) -> str:
        if not self._slots.acquire(timeout=self.timeout_s):
            raise _Busy()
        try:
            with self._lock:
                if self._client is None:
                    self._client = _build_client(self.timeout_s)
                client = self._client
                self._calls += 1
            params: dict[str, Any] = dict(
                model=self.model,
                max_tokens=MAX_TOKENS,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": build_user_prompt(incident)}],
            )
            if _EFFORT_MODELS.match(self.model):
                params["output_config"] = {"effort": "low"}   # short note: keep latency down
            response = client.messages.create(**params)
        finally:
            self._slots.release()
        stop = getattr(response, "stop_reason", None)
        if stop == "refusal":
            raise _LLMError("model declined to answer (refusal)")
        if stop == "max_tokens":
            raise _LLMError("LLM output truncated (max_tokens)")
        text = "".join(getattr(block, "text", "") or ""
                       for block in (getattr(response, "content", None) or [])
                       if getattr(block, "type", None) == "text")
        if not text.strip():
            raise _LLMError("empty LLM response")
        return text

    # ----------------------------------------------------------------- background
    def submit(self, incident: dict, callback: Callable[[Any, str, str], None]) -> None:
        """Explain in the background, then ``callback(incident_id, text, source)``.

        Does nothing when disabled. Unchanged incidents are served from the cache; while a call
        for an incident is running, newer versions of it are coalesced (latest wins).
        """
        if not self._enabled or self._closed or not isinstance(incident, dict) or callback is None:
            return
        iid = incident.get("id")
        key = _cache_key(incident)
        slot = key[0] if key[0] is not None else key
        with self._lock:
            if self._closed:
                return
            cached = self._cache.get(key)
            if cached is None:
                if slot in self._inflight:
                    self._queued[slot] = (incident, callback)
                    return
                self._inflight.add(slot)
            if self._executor is None:
                self._executor = ThreadPoolExecutor(max_workers=self._max_workers,
                                                    thread_name_prefix="feedsentinel-explain")
            executor = self._executor
        try:
            if cached is not None:   # never call the API inline: this may be the event-loop thread
                text, source = (cached, "llm") if cached else (self.template(incident), "template")
                executor.submit(_safe_callback, callback, iid, text, source)
            else:
                executor.submit(self._work, slot, incident, callback)
        except RuntimeError:  # executor shut down by close() meanwhile
            with self._lock:
                self._inflight.discard(slot)

    def _work(self, slot: Any, incident: dict, callback: Callable) -> None:
        try:
            text, source = self.explain_sync(incident)
            if not self._closed:
                _safe_callback(callback, incident.get("id"), text, source)
        except Exception:  # noqa: BLE001
            log.exception("explainer worker failed")
        finally:
            with self._lock:
                self._inflight.discard(slot)
                queued = self._queued.pop(slot, None)
            if queued is not None:
                self.submit(*queued)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            executor, self._executor = self._executor, None
            self._queued.clear()
            client = self._client
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        closer = getattr(client, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:  # noqa: BLE001
                pass
