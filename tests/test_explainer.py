"""Tests for the incident explainer. No network: every Claude client here is a fake."""
from __future__ import annotations

import asyncio
import json
import threading
import time
import types

import pytest

from feedsentinel.explain import explainer as ex
from feedsentinel.explain.explainer import (
    Explainer, build_llm_payload, extract_numbers, is_grounded, normalise_output,
)
from feedsentinel.schema import CODE_INFO

A = "A (Direct)"
SAMPLE_EVIDENCE = {
    "GAP": {"missing": 60, "gap_first": 1200, "gap_last": 1259, "loss_rate_pct": 5.0, "best_feed": A},
    "DELAY": {"lat_p99_ms": 148.2, "baseline_p99_ms": 14.8, "lat_ratio": 10.01, "best_feed": A},
    "DUPLICATE": {"duplicates": 312, "dup_rate_pct": 7.5},
    "CONFLICTING_DUPLICATE": {"conflicts": 4, "example_seq": 88213, "best_feed": A},
    "OUT_OF_ORDER": {"reordered": 41},
    "SEQ_RESET": {"from_seq": 120455, "to_seq": 1},
    "GARBLED": {"quarantined": 37, "quarantine_rate_pct": 2.4, "top_reason": "OFF_TICK",
                "reasons": "OFF_TICK:30, BAD_SIDE:7"},
    "FROZEN": {"frozen_symbols": 5, "symbols": "AAPL,AMZN,GOOG,INTC,MSFT", "frozen_for_s": 3.21,
               "peer_changes": 14, "heartbeat_age_s": 0.4, "best_feed": A},
    "STALE": {"silent_for_s": 4.5, "peer_msgs": 1830, "heartbeat_age_s": 0.6, "best_feed": A},
    "DISCONNECT": {"heartbeat_age_s": 6.2, "last_seq": 88213, "resume_seq": 88214, "best_feed": A},
    "PRICE_SPIKE": {"max_dev_bps": 412.5, "symbol": "AAPL", "price": 609.44, "consensus": 585.31,
                    "best_feed": A},
    "DECIMAL_SHIFT": {"ratio": 100.0, "power": 2, "symbol": "AAPL", "price": 58531.0,
                      "consensus": 585.31},
    "DRIFT": {"bias_bps": 38.4, "symbol": "MSFT", "cusum": 12.7, "best_feed": A},
    "TEST_DATA_LEAK": {"identical_price": 199.99, "symbols_at_price": 5, "test_symbols": "ZVZZT"},
    "RATE_STORM": {"rate_ratio": 8.3, "msg_rate": 3420.0, "peer_rate": 412.0, "unknown_symbols": 3,
                   "dup_rate_pct": 22.1, "best_feed": A},
    "CLOCK_SKEW": {"skew_ms": 2500.0, "future_msgs": 188},
    "ML_ANOMALY": {"score": 0.71, "threshold": 0.58, "top_feature": "frozen_frac", "diagnosis": "FROZEN"},
}

FAITHFUL = (
    "Feed C (Vendor) is frozen: 5 symbols flat for 3.2 s while A and B moved 14 times.\n"
    "How we know: heartbeats (0.4 s old) and sequence numbers are normal, but prices stopped "
    "while the peers kept moving.\n"
    "Action: Restart the feed handler and fail consumers over to A (Direct)."
)


def frozen_incident(**overrides) -> dict:
    incident = {
        "id": "INC-0007", "feed": "C", "feed_name": "Vendor", "code": "FROZEN",
        "title": CODE_INFO["FROZEN"]["title"],
        "headline": "5 symbols flat for 3.2 s while A and B moved 14 times",
        "severity": "CRITICAL", "status": "OPEN",
        "action": "Restart the feed handler; fail consumers over to A (Direct)",
        "symbols": ["AAPL", "AMZN", "GOOG", "INTC", "MSFT"],
        "evidence": dict(SAMPLE_EVIDENCE["FROZEN"]),
        "top_features": [{"name": "frozen_frac", "value": 1.0, "baseline": 0.0, "z": 12.5}],
        "diagnosis": {"label": "FROZEN", "p": 0.93, "alternatives": [{"label": "STALE", "p": 0.04}]},
        "samples": [{"feed": "C", "seq": 1234 + i, "type": "Q", "sym": "AAPL", "bid": 585.1}
                    for i in range(5)],
        "ttd_s": 1.4,
    }
    incident.update(overrides)
    return incident


class FakeClient:
    """Stands in for anthropic.Anthropic: records requests, returns a canned reply."""

    def __init__(self, reply=FAITHFUL, stop_reason="end_turn", exc=None, delay=0.0):
        self.reply, self.stop_reason, self.exc, self.delay = reply, stop_reason, exc, delay
        self.requests: list[dict] = []
        self._lock = threading.Lock()
        self.messages = self            # client.messages.create(...)

    def create(self, **params):
        with self._lock:
            self.requests.append(params)
        if self.delay:
            time.sleep(self.delay)
        if self.exc is not None:
            raise self.exc
        block = types.SimpleNamespace(type="text", text=self.reply)
        return types.SimpleNamespace(stop_reason=self.stop_reason, content=[block])


@pytest.fixture(autouse=True)
def no_real_claude(monkeypatch):
    for var in ("ANTHROPIC_API_KEY", "FEEDSENTINEL_LLM", "FEEDSENTINEL_LLM_MODEL"):
        monkeypatch.delenv(var, raising=False)

    def refuse(timeout_s):
        raise RuntimeError("tests must not build a real Claude client")
    monkeypatch.setattr(ex, "_build_client", refuse)


def use_fake(monkeypatch, fake: FakeClient) -> FakeClient:
    monkeypatch.setattr(ex, "_build_client", lambda timeout_s: fake)
    return fake


def three_lines(text: str) -> list[str]:
    lines = text.split("\n")
    assert len(lines) == 3, text
    assert all(line.strip() for line in lines), text
    return lines


# --------------------------------------------------------------------------- template
def test_sample_evidence_covers_every_code():
    assert set(SAMPLE_EVIDENCE) == set(CODE_INFO)


@pytest.mark.parametrize("code", sorted(CODE_INFO))
def test_template_three_lines_for_every_code(code):
    incident = {"id": "INC-0001", "feed": "C", "feed_name": "Vendor", "code": code,
                "evidence": dict(SAMPLE_EVIDENCE[code])}
    exp = Explainer(enabled=False)
    text = exp.template(incident)
    line1, line2, line3 = three_lines(text)
    assert line1.startswith("Feed C (Vendor)")
    assert line2.startswith(f"How we know ({CODE_INFO[code]['layer']}")
    assert line3.startswith("Action: ")
    assert "{" not in text and "}" not in text          # every action placeholder was filled
    assert exp.template(incident) == text               # deterministic
    ok, problems = is_grounded(text, incident)          # the template never invents numbers
    assert ok, problems


@pytest.mark.parametrize("code", sorted(CODE_INFO))
def test_template_three_lines_with_empty_evidence(code):
    exp = Explainer(enabled=False)
    for incident in ({"code": code, "evidence": {}}, {"code": code},
                     {"code": code, "feed": "B", "evidence": {}, "severity": "DEGRADED"}):
        text = exp.template(incident)
        three_lines(text)
        assert "{" not in text


@pytest.mark.parametrize("incident", [
    {}, None, {"code": None, "evidence": None, "feed": None, "headline": None, "action": None},
    {"code": "NOT_A_CODE", "evidence": {"weird_key": 3, "other": "x"}},
    {"code": "ML_ANOMALY", "evidence": ["not", "a", "dict"], "top_features": [None],
     "diagnosis": "FROZEN", "samples": None, "symbols": None},
    {"code": "GAP", "headline": "line one\nline two\n\nline three", "evidence": {"missing": None}},
])
def test_template_never_crashes(incident):
    three_lines(Explainer(enabled=False).template(incident))


def test_template_uses_headline_evidence_and_action():
    text = Explainer(enabled=False).template(frozen_incident(headline="FROZEN: 5 symbols flat for 3.2 s"))
    line1, line2, line3 = three_lines(text)
    assert line1 == "Feed C (Vendor) — Frozen values: 5 symbols flat for 3.2 s."
    assert "L5 consensus" in line2 and "14 times" in line2 and "heartbeats" in line2
    assert line3 == "Action: Restart the feed handler; fail consumers over to A (Direct)."


def test_template_fills_runbook_action_from_evidence():
    exp = Explainer(enabled=False)
    text = exp.template({"feed": "C", "code": "GAP", "evidence": SAMPLE_EVIDENCE["GAP"]})
    assert "seq 1200-1259" in text.split("\n")[2] and "A (Direct)" in text
    bare = exp.template({"feed": "C", "code": "GAP", "evidence": {}})
    assert "the missing sequence range" in bare and "{" not in bare


# --------------------------------------------------------------------------- grounding
def test_extract_numbers():
    text = ("1,234 msgs at 1.4x, -3 bps, 5% loss, 12 ms, $123.47, seq 1200-1260, "
            "L5 p99 MoldUDP64 INC-0007")
    assert extract_numbers(text) == [1234.0, 1.4, -3.0, 5.0, 12.0, 123.47, 1200.0, 1260.0, 7.0]
    assert extract_numbers("no numbers here, only L2 and p99") == []


def test_grounding_accepts_faithful_note():
    ok, problems = is_grounded(FAITHFUL, frozen_incident())
    assert ok, problems


def test_grounding_rejects_invented_number():
    text = FAITHFUL.replace("3.2 s", "7.5 s")
    ok, problems = is_grounded(text, frozen_incident())
    assert not ok
    assert any("7.5" in p for p in problems)


def test_grounding_rejects_two_lines():
    text = "\n".join(FAITHFUL.split("\n")[::2])
    ok, problems = is_grounded(text, frozen_incident())
    assert not ok and any("3 lines" in p for p in problems)


def test_grounding_normalises_whitespace_and_bullets():
    messy = "\n\n  - " + FAITHFUL.replace("\n", "\n\n   * ") + "   \n"
    assert normalise_output(messy) == FAITHFUL
    assert is_grounded(messy, frozen_incident())[0]


def test_grounding_tolerates_rounding_only():
    incident = frozen_incident()          # frozen_for_s = 3.21
    ok, _ = is_grounded(FAITHFUL.replace("3.2 s", "3.21 s"), incident)
    assert ok
    ok, problems = is_grounded(FAITHFUL.replace("3.2 s", "3.3 s"), incident)
    assert not ok and any("3.3" in p for p in problems)


def test_grounding_rejects_unknown_feed_in_action():
    text = FAITHFUL.replace("to A (Direct)", "to D (Backup)")
    ok, problems = is_grounded(text, frozen_incident())
    assert not ok and any("feed not in incident: D" in p for p in problems)


def test_grounding_allows_ids_and_small_integers():
    text = FAITHFUL.replace("How we know:", "How we know (INC-0007, 2 of 3 feeds agree):")
    assert is_grounded(text, frozen_incident())[0]


def test_grounding_requires_action_line():
    text = FAITHFUL.replace("Action: ", "Next: ")
    assert not is_grounded(text, frozen_incident())[0]


# --------------------------------------------------------------------------- explain_sync
def test_explain_sync_disabled_returns_template():
    exp = Explainer(enabled=False)
    incident = frozen_incident()
    assert exp.explain_sync(incident) == (exp.template(incident), "template")
    status = exp.status
    assert status["enabled"] is False and status["provider"] == "template"
    assert status["model"] is None and status["calls"] == 0


def test_auto_enable_follows_environment(monkeypatch):
    use_fake(monkeypatch, FakeClient())
    assert Explainer().enabled is False                       # no key -> template
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("FEEDSENTINEL_LLM_MODEL", "claude-sonnet-5")
    exp = Explainer()
    assert exp.status["enabled"] is True and exp.status["provider"] == "claude"
    assert exp.status["model"] == "claude-sonnet-5"
    monkeypatch.setenv("FEEDSENTINEL_LLM", "off")
    assert Explainer().enabled is False


def test_enabled_without_sdk_or_client_falls_back_to_template():
    exp = Explainer(enabled=True)     # the fixture makes client construction fail
    assert exp.enabled is False and exp.status["last_error"]
    assert exp.explain_sync(frozen_incident())[1] == "template"


def test_llm_note_accepted_when_grounded(monkeypatch):
    fake = use_fake(monkeypatch, FakeClient())
    exp = Explainer(enabled=True, model="claude-haiku-4-5")
    incident = frozen_incident()
    assert exp.explain_sync(incident) == (FAITHFUL, "llm")
    status = exp.status
    assert (status["calls"], status["accepted"], status["rejected_ungrounded"]) == (1, 1, 0)

    request = fake.requests[0]
    assert request["model"] == "claude-haiku-4-5"
    assert "exactly 3 lines" in request["system"]
    assert "output_config" not in request               # Haiku 4.5 does not take effort
    sent = json.loads(request["messages"][0]["content"].split("\n", 1)[1].rsplit("\n\n", 1)[0])
    assert len(sent["samples"]) == 2 and sent["evidence"] == incident["evidence"]
    assert sent["action"] == incident["action"]

    assert exp.explain_sync(incident) == (FAITHFUL, "llm")   # cached: no second call
    assert len(fake.requests) == 1


def test_llm_note_rejected_when_ungrounded(monkeypatch):
    use_fake(monkeypatch, FakeClient(reply=FAITHFUL.replace("14 times", "40 times")))
    exp = Explainer(enabled=True)
    incident = frozen_incident()
    assert exp.explain_sync(incident) == (exp.template(incident), "template")
    status = exp.status
    assert status["rejected_ungrounded"] == 1 and status["accepted"] == 0
    assert "40" in status["last_error"]


@pytest.mark.parametrize("fake", [
    FakeClient(exc=RuntimeError("boom")),
    FakeClient(stop_reason="refusal"),
    FakeClient(reply=""),
])
def test_llm_failures_fall_back_to_template(monkeypatch, fake):
    use_fake(monkeypatch, fake)
    exp = Explainer(enabled=True)
    incident = frozen_incident()
    assert exp.explain_sync(incident) == (exp.template(incident), "template")
    assert exp.status["last_error"]
    exp.explain_sync(incident)                        # backoff: no immediate retry
    assert len(fake.requests) == 1


def test_effort_sent_only_to_models_that_support_it(monkeypatch):
    fake = use_fake(monkeypatch, FakeClient())
    Explainer(enabled=True, model="claude-opus-5").explain_sync(frozen_incident())
    assert fake.requests[0]["output_config"] == {"effort": "low"}


def test_payload_drops_empty_fields_and_extra_samples():
    payload = build_llm_payload(frozen_incident(headline=None, symbols=[]))
    assert "headline" not in payload and "symbols" not in payload
    assert len(payload["samples"]) == 2 and payload["detection_layer"] == "L5"


# --------------------------------------------------------------------------- submit
def test_submit_calls_back(monkeypatch):
    use_fake(monkeypatch, FakeClient())
    exp = Explainer(enabled=True)
    done, got = threading.Event(), []

    def callback(incident_id, text, source):
        got.append((incident_id, text, source))
        done.set()

    exp.submit(frozen_incident(), callback)
    assert done.wait(5)
    assert got == [("INC-0007", FAITHFUL, "llm")]
    exp.close()


def test_submit_disabled_does_nothing():
    exp = Explainer(enabled=False)
    called = threading.Event()
    exp.submit(frozen_incident(), lambda *args: called.set())
    assert not called.wait(0.3)


def test_submit_cached_incident_does_not_call_again(monkeypatch):
    fake = use_fake(monkeypatch, FakeClient())
    exp = Explainer(enabled=True)
    for _ in range(2):
        done = threading.Event()
        exp.submit(frozen_incident(), lambda *args: done.set())
        assert done.wait(5)
    assert len(fake.requests) == 1
    exp.close()


def test_submit_coalesces_updates_while_a_call_is_running(monkeypatch):
    fake = use_fake(monkeypatch, FakeClient(delay=0.3))
    exp = Explainer(enabled=True)
    results, both = [], threading.Event()

    def callback(incident_id, text, source):
        results.append(source)
        if len(results) == 2:
            both.set()

    for frozen_for in (3.21, 3.5, 3.8):          # evidence changes every tick
        evidence = dict(SAMPLE_EVIDENCE["FROZEN"], frozen_for_s=frozen_for)
        exp.submit(frozen_incident(evidence=evidence), callback)
    assert both.wait(5)
    assert len(fake.requests) == 2                # first version, then only the latest
    last = json.loads(fake.requests[1]["messages"][0]["content"].split("\n", 1)[1].rsplit("\n\n", 1)[0])
    assert last["evidence"]["frozen_for_s"] == 3.8
    exp.close()


def test_submit_from_asyncio_event_loop(monkeypatch):
    use_fake(monkeypatch, FakeClient())
    exp = Explainer(enabled=True)

    async def main():
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        exp.submit(frozen_incident(),
                   lambda *args: loop.call_soon_threadsafe(future.set_result, args))
        return await asyncio.wait_for(future, 5)

    assert asyncio.run(main()) == ("INC-0007", FAITHFUL, "llm")
    exp.close()


def test_submit_after_close_is_a_no_op(monkeypatch):
    fake = use_fake(monkeypatch, FakeClient())
    exp = Explainer(enabled=True)
    exp.close()
    called = threading.Event()
    exp.submit(frozen_incident(), lambda *args: called.set())
    assert not called.wait(0.2) and fake.requests == []
