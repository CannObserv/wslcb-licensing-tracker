"""Tests for jev_client.py — the TypeSafe Jev transport (#207).

Every request goes through ``httpx.MockTransport``; ``sleep`` is recorded, not
slept. A malformed answer must fail the whole batch (Review Focus 1), and the
wording must stay the research's verbatim, or the gold-set score no longer
applies.
"""

import importlib.util
import json
from pathlib import Path

import httpx
import pytest

from wslcb_licensing_tracker import jev_client
from wslcb_licensing_tracker.config import ENV_PREFIX

RESEARCH = Path(__file__).resolve().parent.parent / "docs" / "research" / "person-vs-org"


def _load_evaluate():
    spec = importlib.util.spec_from_file_location("_jev_evaluate", RESEARCH / "evaluate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _answer(choice: str, confidence: float) -> dict:
    return {"choice": choice, "confidence": confidence}


def _body(answers: dict, *, tokens: int = 500, model: str | None = "jev-1.14.0") -> dict:
    body: dict = {"usage": {"input_tokens": tokens}, "answers": answers}
    if model is not None:
        body["model"] = model
    return body


def _two_names_ok() -> dict:
    return _body(
        {
            "kind_0": _answer("person", 0.97),
            "org_0": _answer("business", 0.4),
            "kind_1": _answer("trust", 0.88),
            "org_1": _answer("nonprofit", 0.3),
        }
    )


class _Recorder:
    """A MockTransport handler answering from a list of (status, body) in turn."""

    def __init__(self, *responses: tuple[int, dict | None]) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        status, body = self.responses[min(len(self.requests), len(self.responses)) - 1]
        return httpx.Response(status, json=body if body is not None else {})


class _Sleeps:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


async def _classify(handler, names=("JOHN SMITH", "SMITH FAMILY TRUST"), sleep=None):
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        return await jev_client.classify_batch(
            list(names), api_key="k", model="jev-1.14.0", client=client, sleep=sleep or _Sleeps()
        )


def test_wording_matches_research():
    ev = _load_evaluate()
    assert jev_client.KIND_CRITERIA == ev.FIVE
    assert jev_client.CONTEXT == ev.CTX
    assert jev_client.URL == ev.URL
    assert ev.MODEL == jev_client.DEFAULT_MODEL


def test_build_request_shape():
    body = jev_client.build_request(["JOHN SMITH", "ACME LLC"], "jev-9")
    assert body["model"] == "jev-9"
    assert body["state"] == {
        "context": jev_client.CONTEXT,
        "names": {"n0": "JOHN SMITH", "n1": "ACME LLC"},
    }
    assert set(body["questions"]) == {"kind_0", "org_0", "kind_1", "org_1"}
    kind_1 = body["questions"]["kind_1"]
    assert kind_1["type"] == "choice"
    assert "`names.n1`" in kind_1["instructions"]
    assert kind_1["criteria"] == jev_client.KIND_CRITERIA
    org_0 = body["questions"]["org_0"]
    assert "`names.n0`" in org_0["instructions"]
    assert org_0["criteria"] == jev_client.ORG_CRITERIA


def test_org_criteria_are_the_resolvers_jev_subtypes():
    from wslcb_licensing_tracker.entity_classification import JEV_ORG_SUBTYPES

    assert tuple(jev_client.ORG_CRITERIA) == JEV_ORG_SUBTYPES


async def test_classify_batch_parses_in_order():
    rec = _Recorder((200, _two_names_ok()))
    batch = await _classify(rec)
    (request,) = rec.requests
    assert request.headers["Authorization"] == "Bearer k"
    assert str(request.url) == jev_client.URL
    assert json.loads(request.content)["state"]["names"]["n1"] == "SMITH FAMILY TRUST"
    assert batch.model == "jev-1.14.0"
    assert batch.input_tokens == 500
    assert [a.kind for a in batch.answers] == ["person", "trust"]
    assert batch.answers[1].kind_confidence == 0.88
    assert batch.answers[1].org == "nonprofit"
    assert batch.answers[1].org_confidence == 0.3
    assert batch.answers[1].raw == {
        "kind": _answer("trust", 0.88),
        "org": _answer("nonprofit", 0.3),
    }


async def test_missing_model_echo_falls_back_to_the_requested_model():
    rec = _Recorder((200, {**_two_names_ok(), "model": ""}))
    assert (await _classify(rec)).model == "jev-1.14.0"
    rec = _Recorder((200, _body(_two_names_ok()["answers"], model=None)))
    assert (await _classify(rec)).model == "jev-1.14.0"


async def test_retries_429_and_529():
    rec = _Recorder((429, None), (529, None), (200, _two_names_ok()))
    sleeps = _Sleeps()
    batch = await _classify(rec, sleep=sleeps)
    assert len(rec.requests) == 3
    assert sleeps.calls == [1, 2]
    assert batch.input_tokens == 500


async def test_gives_up_after_max_attempts():
    rec = _Recorder((429, None))
    sleeps = _Sleeps()
    with pytest.raises(jev_client.JevError) as exc:
        await _classify(rec, sleep=sleeps)
    assert len(rec.requests) == jev_client.MAX_ATTEMPTS == 6
    assert sleeps.calls == [1, 2, 4, 8, 16]  # never after the last attempt
    assert exc.value.status == 429
    assert not exc.value.auth


async def test_other_status_not_retried():
    rec = _Recorder((500, None))
    with pytest.raises(jev_client.JevError) as exc:
        await _classify(rec)
    assert exc.value.status == 500
    assert len(rec.requests) == 1


async def test_status_error_carries_a_bounded_body_excerpt():
    """The first live call debugs a schema rejection from the vendor's own words."""
    detail = "questions.kind_0.criteria:\n  must be a mapping " + "x" * 500

    def handler(request):
        return httpx.Response(422, json={"error": detail})

    with pytest.raises(jev_client.JevError) as exc:
        await _classify(handler)
    message = str(exc.value)
    assert message.startswith("Jev: HTTP 422: ")
    assert "must be a mapping" in message
    assert "\n" not in message
    assert len(message) <= len("Jev: HTTP 422: ") + jev_client.ERROR_EXCERPT_CHARS + 1


async def test_auth_rejection_is_never_quoted():
    def handler(request):
        return httpx.Response(401, json={"error": "invalid key k-live-1234"})

    with pytest.raises(jev_client.JevError) as exc:
        await _classify(handler)
    assert str(exc.value) == "Jev: HTTP 401"


async def test_retries_exhausted_message_has_no_excerpt_noise():
    with pytest.raises(jev_client.JevError) as exc:
        await _classify(_Recorder((429, None)))
    assert str(exc.value) == "Jev: HTTP 429 after 6 attempts"


@pytest.mark.parametrize("status", [401, 403])
async def test_401_403_are_auth_errors(status):
    with pytest.raises(jev_client.JevError) as exc:
        await _classify(_Recorder((status, None)))
    assert exc.value.auth
    assert exc.value.status == status


def _drop(key):
    answers = dict(_two_names_ok()["answers"])
    del answers[key]
    return _body(answers)


def _with(key, value):
    return _body({**_two_names_ok()["answers"], key: value})


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(_drop("org_0"), id="missing-org_0"),
        pytest.param(_drop("kind_1"), id="missing-kind_1"),
        pytest.param(_with("kind_0", _answer("robot", 0.9)), id="unknown-kind-choice"),
        pytest.param(_with("org_1", _answer("trust", 0.9)), id="kind-choice-in-org"),
        pytest.param(_with("kind_0", _answer("person", 1.7)), id="confidence-above-1"),
        pytest.param(_with("kind_0", _answer("person", -0.1)), id="confidence-below-0"),
        pytest.param(_with("kind_0", _answer("person", "high")), id="confidence-string"),
        pytest.param(_with("kind_0", {"choice": "person", "confidence": True}), id="conf-bool"),
        pytest.param(_with("kind_0", {"choice": "person"}), id="confidence-missing"),
        pytest.param(_with("kind_0", "person"), id="answer-not-an-object"),
        pytest.param({"answers": _two_names_ok()["answers"]}, id="usage-missing"),
        pytest.param({**_two_names_ok(), "usage": {"input_tokens": "500"}}, id="tokens-not-an-int"),
        pytest.param({"usage": {"input_tokens": 1}}, id="answers-missing"),
        pytest.param([], id="body-not-an-object"),
    ],
)
async def test_malformed_response_raises(body):
    with pytest.raises(jev_client.JevError) as exc:
        await _classify(_Recorder((200, body)))
    assert exc.value.status == 200
    assert not exc.value.auth


async def test_nan_confidence_is_malformed():
    def handler(request):
        text = json.dumps(_two_names_ok()).replace("0.97", "NaN")
        return httpx.Response(200, content=text, headers={"content-type": "application/json"})

    with pytest.raises(jev_client.JevError):
        await _classify(handler)


async def test_non_json_body_is_malformed():
    def handler(request):
        return httpx.Response(200, content=b"<html>gateway</html>")

    with pytest.raises(jev_client.JevError):
        await _classify(handler)


async def test_undecodable_body_is_a_failed_batch():
    """A corrupt content encoding raises httpx.DecodingError, not a TransportError."""

    def handler(request):
        return httpx.Response(200, content=b"not gzip", headers={"content-encoding": "gzip"})

    with pytest.raises(jev_client.JevError) as exc:
        await _classify(handler)
    assert "DecodingError" in str(exc.value)


async def test_transport_error_raises():
    def handler(request):
        raise httpx.ConnectError("boom", request=request)

    with pytest.raises(jev_client.JevError) as exc:
        await _classify(handler)
    assert exc.value.status is None
    assert not exc.value.auth


async def test_error_never_carries_the_key():
    def handler(request):
        msg = f"refused {request.headers['Authorization']}"
        raise httpx.ConnectError(msg, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(jev_client.JevError) as exc:
            await jev_client.classify_batch(
                ["X"], api_key="sekrit-key", model="m", client=client, sleep=_Sleeps()
            )
    assert "sekrit-key" not in str(exc.value)


@pytest.mark.parametrize("n", [0, 26])
async def test_batch_size_bounds(n):
    rec = _Recorder((200, _two_names_ok()))
    with pytest.raises(ValueError, match="25"):
        await _classify(rec, names=[f"N{i}" for i in range(n)])
    assert rec.requests == []


def test_env_getters(monkeypatch):
    for name in ("ENABLE_JEV_CLASSIFICATION", "TYPESAFE_API_KEY", "JEV_MODEL"):
        monkeypatch.delenv(f"{ENV_PREFIX}{name}", raising=False)
    monkeypatch.delenv(f"{ENV_PREFIX}JEV_MAX_NAMES_PER_RUN", raising=False)
    assert jev_client.is_classification_enabled() is False
    assert jev_client.get_api_key() == ""
    assert jev_client.get_model() == "jev-1.14.0"
    assert jev_client.max_names_per_run() == 5000

    for truthy in ("1", "true", "YES"):
        monkeypatch.setenv(f"{ENV_PREFIX}ENABLE_JEV_CLASSIFICATION", truthy)
        assert jev_client.is_classification_enabled() is True
    monkeypatch.setenv(f"{ENV_PREFIX}ENABLE_JEV_CLASSIFICATION", "0")
    assert jev_client.is_classification_enabled() is False

    monkeypatch.setenv(f"{ENV_PREFIX}TYPESAFE_API_KEY", " k ")
    assert jev_client.get_api_key() == "k"
    monkeypatch.setenv(f"{ENV_PREFIX}JEV_MODEL", "jev-2")
    assert jev_client.get_model() == "jev-2"
    monkeypatch.setenv(f"{ENV_PREFIX}JEV_MAX_NAMES_PER_RUN", "120")
    assert jev_client.max_names_per_run() == 120


@pytest.mark.parametrize("raw", ["x", "0", "-5", ""])
def test_max_names_invalid_warns_and_defaults(monkeypatch, caplog, raw):
    monkeypatch.setenv(f"{ENV_PREFIX}JEV_MAX_NAMES_PER_RUN", raw)
    assert jev_client.max_names_per_run() == 5000
    if raw:
        assert "JEV_MAX_NAMES_PER_RUN" in caplog.text
