# -*- coding: utf-8 -*-

"""
Tests for kiro.stream_validation (validating HTTP 200 Kiro streams).

Covers:
- classify_stream_exception: throttling / client / auth / server mapping
- probe_kiro_stream: OK (with byte-exact replay), EMPTY, ERROR, first-token timeout
- KiroHttpClient.request_with_retry(validate_stream=True): retries, failover hand-off,
  synthetic error responses, timeout exhaustion, opt-out switch
- classify_error for the synthesized reasons
- End to end through the routes: OpenAI and Anthropic, streaming and non-streaming,
  empty upstream answers become errors (never an empty 200), and a later
  successful attempt is replayed to the client
"""

import asyncio
import json
import time
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException

from kiro.account_errors import ErrorType, classify_error
from kiro.aws_eventstream import encode_frame
from kiro.http_client import KiroHttpClient
from kiro.parsers import REFUSAL_EXCEPTION_TYPE
from kiro.stream_validation import (
    REASON_CONTENT_FILTERED,
    REASON_EMPTY_RESPONSE,
    REASON_REASONING_REFUSED,
    REASON_STREAM_ERROR,
    REASON_THROTTLED,
    KiroStreamError,
    ProbeOutcome,
    ReplayResponse,
    StreamFailure,
    build_failure_response,
    classify_stream_exception,
    probe_kiro_stream,
)


MODEL = "claude-sonnet-4.5"


def content_frame(text: str) -> bytes:
    payload = json.dumps({"content": text}).encode("utf-8")
    return encode_frame({":message-type": "event", ":event-type": "assistantResponseEvent"}, payload)


def metering_frame() -> bytes:
    return encode_frame({":message-type": "event", ":event-type": "meteringEvent"}, b'{"usage":0.01}')


def context_frame() -> bytes:
    return encode_frame(
        {":message-type": "event", ":event-type": "contextUsageEvent"}, b'{"contextUsagePercentage":1.5}'
    )


def exception_frame(exception_type: str, message: str) -> bytes:
    return encode_frame(
        {":message-type": "exception", ":exception-type": exception_type},
        json.dumps({"message": message}).encode("utf-8"),
    )


def refusal_frame(category: str, explanation: str = "refused") -> bytes:
    payload = json.dumps({
        "stopDetails": {"refusal": {"category": category, "explanation": explanation}},
        "stopReason": "CONTENT_FILTERED",
    }).encode("utf-8")
    return encode_frame({":message-type": "event", ":event-type": "metadataEvent"}, payload)


def reasoning_frame(text: str) -> bytes:
    return encode_frame(
        {":message-type": "event", ":event-type": "reasoningContentEvent"},
        json.dumps({"text": text}).encode("utf-8"),
    )


def make_response(chunks: List[bytes], status_code: int = 200, delay: float = 0.0) -> MagicMock:
    """httpx-like streaming response whose body yields ``chunks``."""
    response = MagicMock()
    response.status_code = status_code
    response.headers = httpx.Headers({"content-type": "application/vnd.amazon.eventstream"})
    response.request = httpx.Request("POST", "https://runtime.example/generateAssistantResponse")
    response.aclose = AsyncMock()
    response.aread = AsyncMock(return_value=b"".join(chunks))

    async def aiter_bytes():
        if delay:
            await asyncio.sleep(delay)
        for chunk in chunks:
            yield chunk

    response.aiter_bytes = aiter_bytes
    return response


# ==================================================================================================
# classify_stream_exception / build_failure_response
# ==================================================================================================

class TestClassifyStreamException:
    @pytest.mark.parametrize("exception_type", [
        "ThrottlingException", "TooManyRequestsException", "ServiceQuotaExceededException",
        "RateLimitExceededException", "LimitExceededException",
    ])
    def test_throttling_types_map_to_429(self, exception_type):
        failure = classify_stream_exception(exception_type, "slow down")
        assert failure.status_code == 429
        assert failure.reason == REASON_THROTTLED
        assert failure.is_rate_limit and failure.is_retryable
        assert exception_type in failure.message

    def test_validation_keeps_kiro_reason(self):
        """
        What it does: Classifies a context-overflow validation error.
        Purpose: Existing 400 handling (FATAL, friendly message) keeps working.
        """
        failure = classify_stream_exception("ValidationException", "Input is too long.", "CONTENT_LENGTH_EXCEEDS_THRESHOLD")
        assert failure.status_code == 400
        assert failure.reason == "CONTENT_LENGTH_EXCEEDS_THRESHOLD"
        assert not failure.is_retryable

    def test_validation_mentioning_capacity_is_not_throttling(self):
        failure = classify_stream_exception("ValidationException", "Input exceeds model capacity")
        assert failure.status_code == 400
        assert failure.reason == "ValidationException"

    def test_auth_maps_to_403(self):
        failure = classify_stream_exception("AccessDeniedException", "denied")
        assert failure.status_code == 403
        assert not failure.is_retryable

    @pytest.mark.parametrize("message", ["Too many requests", "Rate exceeded", "insufficient model capacity"])
    def test_unknown_types_with_throttling_wording(self, message):
        assert classify_stream_exception("SomethingNew", message).status_code == 429

    @pytest.mark.parametrize("exception_type", ["InternalServerException", "ServiceUnavailableException", "invalidStateEvent", "error"])
    def test_server_side_maps_to_retryable_502(self, exception_type):
        failure = classify_stream_exception(exception_type, "oops")
        assert failure.status_code == 502
        assert failure.reason == REASON_STREAM_ERROR
        assert failure.is_retryable

    def test_build_failure_response_uses_kiro_error_shape(self):
        failure = StreamFailure(status_code=429, reason=REASON_THROTTLED, message="busy")
        request = httpx.Request("POST", "https://x")
        response = build_failure_response(failure, request)
        assert response.status_code == 429
        assert response.headers["retry-after"] == "5"
        assert json.loads(response.content) == {"message": "busy", "reason": REASON_THROTTLED}
        assert response.request is request

    def test_build_failure_response_without_retry_after(self):
        failure = StreamFailure(status_code=502, reason=REASON_EMPTY_RESPONSE, message="empty")
        assert "retry-after" not in build_failure_response(failure, None).headers

    def test_kiro_stream_error_message(self):
        error = KiroStreamError("ThrottlingException", "slow", "R")
        assert error.exception_type == "ThrottlingException"
        assert error.reason == "R"
        assert "ThrottlingException" in str(error) and "slow" in str(error)


class TestClassifyErrorForStreamReasons:
    @pytest.mark.parametrize("reason", [REASON_EMPTY_RESPONSE, REASON_STREAM_ERROR])
    def test_retryable_stream_reasons_fail_over(self, reason):
        assert classify_error(502, reason) == ErrorType.RECOVERABLE

    def test_other_502_stays_fatal(self):
        assert classify_error(502, None) == ErrorType.FATAL
        assert classify_error(500, REASON_EMPTY_RESPONSE) == ErrorType.FATAL

    def test_throttled_429_fails_over(self):
        assert classify_error(429, REASON_THROTTLED) == ErrorType.RECOVERABLE


# ==================================================================================================
# probe_kiro_stream
# ==================================================================================================

class TestProbeKiroStream:
    @pytest.mark.asyncio
    async def test_content_stream_is_ok_and_replays_every_byte(self):
        """
        What it does: Probes a stream with metadata first, content second, more after.
        Purpose: The replayed body is byte-identical to what Kiro sent.
        """
        chunks = [context_frame(), content_frame("Hello"), content_frame(" world"), metering_frame()]
        response = make_response(chunks)

        result = await probe_kiro_stream(response, first_token_timeout=1)

        assert result.outcome == ProbeOutcome.OK
        assert isinstance(result.response, ReplayResponse)
        assert result.response.status_code == 200
        assert await result.response.aread() == b"".join(chunks)
        response.aclose.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_probe_stops_reading_at_first_content(self):
        consumed = []

        response = make_response([])

        async def aiter_bytes():
            for chunk in [content_frame("a"), content_frame("b"), content_frame("c")]:
                consumed.append(chunk)
                yield chunk

        response.aiter_bytes = aiter_bytes
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert len(consumed) == 1
        replay = [chunk async for chunk in result.response.aiter_bytes()]
        assert len(replay) == 3
        assert len(consumed) == 3

    @pytest.mark.asyncio
    async def test_text_mode_stream_is_ok(self):
        response = make_response([b'{"content":"plain"}'])
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert result.outcome == ProbeOutcome.OK

    @pytest.mark.asyncio
    async def test_tool_call_only_stream_is_ok(self):
        tool = encode_frame(
            {":message-type": "event", ":event-type": "toolUseEvent"},
            b'{"name":"f","toolUseId":"t","input":"{}","stop":true}',
        )
        result = await probe_kiro_stream(make_response([tool]), first_token_timeout=1)
        assert result.outcome == ProbeOutcome.OK

    @pytest.mark.asyncio
    async def test_tool_call_started_counts_as_output(self):
        tool = encode_frame({":message-type": "event", ":event-type": "toolUseEvent"}, b'{"name":"f","toolUseId":"t"}')
        result = await probe_kiro_stream(make_response([tool, metering_frame()]), first_token_timeout=1)
        assert result.outcome == ProbeOutcome.OK

    @pytest.mark.asyncio
    @pytest.mark.parametrize("chunks", [
        [],
        [metering_frame(), context_frame()],
        [content_frame("")],
        [b"garbage without events"],
    ], ids=["no-bytes", "metadata-only", "empty-content", "garbage"])
    async def test_streams_without_output_are_empty(self, chunks):
        """
        What it does: Probes 200 bodies that carry nothing usable.
        Purpose: These are the "chars=0, finish_reason=stop" answers seen in production.
        """
        response = make_response(chunks)
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert result.outcome == ProbeOutcome.EMPTY
        assert result.failure.reason == REASON_EMPTY_RESPONSE
        assert result.failure.status_code == 502
        response.aclose.assert_awaited()

    @pytest.mark.asyncio
    async def test_exception_before_content_is_error(self):
        response = make_response([context_frame(), exception_frame("ThrottlingException", "Too many requests")])
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert result.outcome == ProbeOutcome.ERROR
        assert result.failure.status_code == 429
        response.aclose.assert_awaited()

    @pytest.mark.asyncio
    async def test_exception_after_content_is_left_to_the_stream(self):
        """
        What it does: Content arrives before the exception.
        Purpose: The probe commits on content; the exception surfaces mid-stream instead.
        """
        result = await probe_kiro_stream(
            make_response([content_frame("partial"), exception_frame("InternalServerException", "x")]),
            first_token_timeout=1,
        )
        assert result.outcome == ProbeOutcome.OK

    @pytest.mark.asyncio
    async def test_first_chunk_timeout_raises_and_closes(self):
        response = make_response([content_frame("late")], delay=0.5)
        with pytest.raises(asyncio.TimeoutError):
            await probe_kiro_stream(response, first_token_timeout=0.05)
        response.aclose.assert_awaited()

    @pytest.mark.asyncio
    async def test_close_errors_are_swallowed(self):
        response = make_response([])
        response.aclose = AsyncMock(side_effect=httpx.ReadError("closed"))
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert result.outcome == ProbeOutcome.EMPTY

    @pytest.mark.asyncio
    async def test_replay_response_delegates_and_closes(self):
        response = make_response([content_frame("x")])
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert result.response.headers["content-type"] == "application/vnd.amazon.eventstream"
        assert result.response.request is response.request
        await result.response.aclose()
        response.aclose.assert_awaited()


# ==================================================================================================
# KiroHttpClient.request_with_retry(validate_stream=True)
# ==================================================================================================

def _http_client(responses):
    auth_manager = MagicMock()
    auth_manager.get_access_token = AsyncMock(return_value="token")
    auth_manager.fingerprint = "fp"
    shared = MagicMock()
    shared.build_request = MagicMock(return_value="request")
    shared.send = AsyncMock(side_effect=responses)
    return KiroHttpClient(auth_manager, shared_client=shared), shared


class TestRequestWithRetryValidation:
    @pytest.mark.asyncio
    async def test_empty_then_content_is_retried_transparently(self):
        client, shared = _http_client([make_response([metering_frame()]), make_response([content_frame("ok")])])
        with patch("kiro.http_client.asyncio.sleep", AsyncMock()) as sleep:
            response = await client.request_with_retry("POST", "u", {}, stream=True, validate_stream=True)
        assert response.status_code == 200
        assert b"ok" in await response.aread()
        assert shared.send.await_count == 2
        assert [c.args[0] for c in sleep.await_args_list] == [1.0]

    @pytest.mark.asyncio
    async def test_persistently_empty_returns_synthetic_502(self):
        client, shared = _http_client([make_response([]) for _ in range(3)])
        with patch("kiro.http_client.asyncio.sleep", AsyncMock()) as sleep:
            response = await client.request_with_retry("POST", "u", {}, stream=True, validate_stream=True)
        assert response.status_code == 502
        assert json.loads(await response.aread())["reason"] == REASON_EMPTY_RESPONSE
        assert shared.send.await_count == 3
        # No pointless sleep after the final attempt
        assert [c.args[0] for c in sleep.await_args_list] == [1.0, 2.0]

    @pytest.mark.asyncio
    async def test_failover_mode_hands_back_immediately(self):
        """
        What it does: retry_on_429=False (round_robin with 2+ accounts).
        Purpose: Another account is tried instead of retrying this one.
        """
        client, shared = _http_client([make_response([exception_frame("ThrottlingException", "busy")])])
        response = await client.request_with_retry(
            "POST", "u", {}, stream=True, retry_on_429=False, validate_stream=True
        )
        assert response.status_code == 429
        assert shared.send.await_count == 1

    @pytest.mark.asyncio
    async def test_non_retryable_failure_returns_immediately(self):
        client, shared = _http_client([make_response([exception_frame("ValidationException", "bad")])])
        response = await client.request_with_retry("POST", "u", {}, stream=True, validate_stream=True)
        assert response.status_code == 400
        assert shared.send.await_count == 1

    @pytest.mark.asyncio
    async def test_first_token_timeouts_exhausted_raise_504(self):
        client, shared = _http_client([make_response([content_frame("x")], delay=1) for _ in range(3)])
        with patch("kiro.http_client.FIRST_TOKEN_TIMEOUT", 0.01):
            with pytest.raises(HTTPException) as exc_info:
                await client.request_with_retry("POST", "u", {}, stream=True, validate_stream=True)
        assert exc_info.value.status_code == 504
        assert "FIRST_TOKEN_TIMEOUT" in exc_info.value.detail
        assert shared.send.await_count == 3

    @pytest.mark.asyncio
    async def test_timeout_then_success(self):
        client, shared = _http_client([make_response([content_frame("x")], delay=1), make_response([content_frame("y")])])
        with patch("kiro.http_client.FIRST_TOKEN_TIMEOUT", 0.01):
            response = await client.request_with_retry("POST", "u", {}, stream=True, validate_stream=True)
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_validation_disabled_by_config(self):
        empty = make_response([])
        client, _ = _http_client([empty])
        with patch("kiro.http_client.STREAM_VALIDATION_ENABLED", False):
            response = await client.request_with_retry("POST", "u", {}, stream=True, validate_stream=True)
        assert response is empty

    @pytest.mark.asyncio
    async def test_not_requested_means_pass_through(self):
        empty = make_response([])
        client, _ = _http_client([empty])
        assert await client.request_with_retry("POST", "u", {}, stream=True) is empty

    @pytest.mark.asyncio
    async def test_non_stream_requests_are_not_probed(self):
        plain = make_response([])
        client, _ = _http_client([plain])
        shared_request = client._shared_client.request = AsyncMock(return_value=plain)
        assert await client.request_with_retry("GET", "u", None, stream=False, validate_stream=True) is plain
        shared_request.assert_awaited_once()


# ==================================================================================================
# End to end through the routes
# ==================================================================================================

def _make_manager(tmp_path, count: int):
    from kiro.account_manager import Account, AccountManager
    from kiro.cache import ModelInfoCache

    manager = AccountManager(str(tmp_path / "credentials.json"), str(tmp_path / "state.json"))
    manager._selection = "round_robin"
    for i in range(count):
        auth_manager = MagicMock()
        auth_manager.api_host = f"https://runtime.example/{i}"
        auth_manager.profile_arn = "arn:aws:codewhisperer:us-east-1:123:profile/test"
        auth_manager.get_access_token = AsyncMock(return_value="token")
        auth_manager.fingerprint = "fp"
        manager._accounts[f"/accounts/acct-{i}.sqlite3"] = Account(
            id=f"/accounts/acct-{i}.sqlite3",
            auth_manager=auth_manager,
            model_cache=ModelInfoCache(),
            model_resolver=MagicMock(),
            models_cached_at=time.time(),
        )
    return manager


@pytest.fixture
def routed_app(clean_app):
    """App in account mode whose Kiro HTTP traffic goes to a scripted upstream."""
    from fastapi.testclient import TestClient

    with TestClient(clean_app) as client:
        saved = (clean_app.state.account_system, clean_app.state.account_manager, clean_app.state.http_client)

        def install(tmp_path, upstream, accounts=1, account_system=True):
            manager = _make_manager(tmp_path, accounts)
            clean_app.state.account_system = account_system
            clean_app.state.account_manager = manager
            shared = MagicMock()
            shared.build_request = MagicMock(return_value="request")
            shared.send = AsyncMock(side_effect=upstream)
            clean_app.state.http_client = shared
            return manager, shared

        async def get_client(self, stream=False):
            return clean_app.state.http_client

        try:
            with patch.object(KiroHttpClient, "_get_client", get_client), \
                    patch("kiro.http_client.asyncio.sleep", AsyncMock()), \
                    patch("kiro.routes_openai.WEB_SEARCH_ENABLED", False), \
                    patch("kiro.routes_anthropic.WEB_SEARCH_ENABLED", False):
                yield client, install
        finally:
            (clean_app.state.account_system, clean_app.state.account_manager,
             clean_app.state.http_client) = saved


def _post_openai(client, api_key, stream):
    return client.post(
        "/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"model": MODEL, "messages": [{"role": "user", "content": "Hello"}], "stream": stream},
    )


def _post_anthropic(client, api_key, stream):
    return client.post(
        "/v1/messages",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01"},
        json={"model": MODEL, "max_tokens": 16, "stream": stream,
              "messages": [{"role": "user", "content": "Hello"}]},
    )


POSTS = {"openai": _post_openai, "anthropic": _post_anthropic}


class TestRoutesEndToEnd:
    """All four API × mode combinations go through the same validation."""

    @pytest.mark.parametrize("api", ["openai", "anthropic"])
    @pytest.mark.parametrize("stream", [False, True])
    def test_empty_answers_become_an_error_not_an_empty_200(self, routed_app, tmp_path, valid_proxy_api_key, api, stream):
        """
        What it does: Kiro answers 200 with only metadata on every attempt.
        Purpose: The client gets an actionable 502 instead of "200, chars=0, finish_reason=stop".
        """
        client, install = routed_app
        manager, shared = install(tmp_path, [make_response([context_frame(), metering_frame()]) for _ in range(3)])

        response = POSTS[api](client, valid_proxy_api_key, stream)

        assert response.status_code == 502, response.text
        assert "empty response" in response.text
        assert shared.send.await_count == 3
        account = next(iter(manager._accounts.values()))
        assert account.stats.successful_requests == 0
        assert account.in_flight == 0

    @pytest.mark.parametrize("api", ["openai", "anthropic"])
    @pytest.mark.parametrize("stream", [False, True])
    def test_empty_then_content_is_served(self, routed_app, tmp_path, valid_proxy_api_key, api, stream):
        client, install = routed_app
        manager, shared = install(tmp_path, [
            make_response([metering_frame()]),
            make_response([content_frame("pong"), metering_frame(), context_frame()]),
        ])

        response = POSTS[api](client, valid_proxy_api_key, stream)

        assert response.status_code == 200, response.text
        assert "pong" in response.text
        assert shared.send.await_count == 2
        assert next(iter(manager._accounts.values())).in_flight == 0

    @pytest.mark.parametrize("api", ["openai", "anthropic"])
    @pytest.mark.parametrize("stream", [False, True])
    def test_in_stream_throttling_fails_over_to_next_account(self, routed_app, tmp_path, valid_proxy_api_key, api, stream):
        """
        What it does: Account A throttles inside a 200 stream, account B answers.
        Purpose: Throttling is treated like HTTP 429: cooldown + failover, invisible to the client.
        """
        client, install = routed_app
        manager, shared = install(tmp_path, [
            make_response([exception_frame("ThrottlingException", "Too many requests")]),
            make_response([content_frame("from B"), metering_frame()]),
        ], accounts=2)

        response = POSTS[api](client, valid_proxy_api_key, stream)

        assert response.status_code == 200, response.text
        assert "from B" in response.text
        accounts = list(manager._accounts.values())
        assert accounts[0].cooldown_until > time.time()
        assert accounts[1].stats.successful_requests == 1
        assert all(a.in_flight == 0 for a in accounts)

    @pytest.mark.parametrize("stream", [False, True])
    def test_validation_exception_is_returned_as_400(self, routed_app, tmp_path, valid_proxy_api_key, stream):
        client, install = routed_app
        install(tmp_path, [make_response([exception_frame("ValidationException", "Improperly formed request.")])])

        response = _post_openai(client, valid_proxy_api_key, stream)

        assert response.status_code == 400
        assert "Improperly formed request" in response.text or "rejected" in response.text

    def test_openai_mid_stream_exception_sends_error_chunk(self, routed_app, tmp_path, valid_proxy_api_key):
        """
        What it does: Content, then an exception frame.
        Purpose: The OpenAI stream ends with an error chunk instead of a silent [DONE].
        """
        client, install = routed_app
        install(tmp_path, [make_response([content_frame("partial"), exception_frame("InternalServerException", "boom")])])

        response = _post_openai(client, valid_proxy_api_key, True)

        assert response.status_code == 200
        lines = [line for line in response.text.splitlines() if line.startswith("data: ")]
        assert any("partial" in line for line in lines)
        error_lines = [json.loads(line[6:]) for line in lines if line.startswith('data: {"error"')]
        assert error_lines and "boom" in error_lines[0]["error"]["message"]
        assert lines[-1] == "data: [DONE]"

    def test_anthropic_mid_stream_exception_sends_error_event(self, routed_app, tmp_path, valid_proxy_api_key):
        client, install = routed_app
        install(tmp_path, [make_response([content_frame("partial"), exception_frame("InternalServerException", "boom")])])

        response = _post_anthropic(client, valid_proxy_api_key, True)

        assert "event: error" in response.text
        assert "boom" in response.text


class TestLegacyModeEndToEnd:
    """ACCOUNT_SYSTEM=false (single account, no failover loop) is validated too."""

    @pytest.mark.parametrize("api", ["openai", "anthropic"])
    @pytest.mark.parametrize("stream", [False, True])
    def test_empty_answers_become_an_error(self, routed_app, tmp_path, valid_proxy_api_key, api, stream):
        client, install = routed_app
        _, shared = install(tmp_path, [make_response([metering_frame()]) for _ in range(3)], account_system=False)

        response = POSTS[api](client, valid_proxy_api_key, stream)

        assert response.status_code == 502, response.text
        assert shared.send.await_count == 3

    @pytest.mark.parametrize("api", ["openai", "anthropic"])
    @pytest.mark.parametrize("stream", [False, True])
    def test_content_is_served(self, routed_app, tmp_path, valid_proxy_api_key, api, stream):
        client, install = routed_app
        install(tmp_path, [make_response([content_frame("legacy ok"), metering_frame()])], account_system=False)

        response = POSTS[api](client, valid_proxy_api_key, stream)

        assert response.status_code == 200, response.text
        assert "legacy ok" in response.text


class TestRefusalClassification:
    """Refusals reported in metadataEvent (real Kiro behaviour, not throttling)."""

    def test_reasoning_extraction_is_retryable_without_injection(self):
        failure = classify_stream_exception(REFUSAL_EXCEPTION_TYPE, "refused", "REASONING_EXTRACTION")
        assert failure.status_code == 400
        assert failure.reason == REASON_REASONING_REFUSED
        # Not a rate limit, not blindly retried on another account
        assert not failure.is_rate_limit

    def test_other_refusal_is_content_filtered(self):
        failure = classify_stream_exception(REFUSAL_EXCEPTION_TYPE, "blocked", "CONTENT_FILTERED")
        assert failure.status_code == 400
        assert failure.reason == REASON_CONTENT_FILTERED
        assert not failure.is_retryable


class TestProbeRefusalAndReasoning:
    @pytest.mark.asyncio
    async def test_reasoning_extraction_refusal_reports_error(self):
        """
        What it does: a 200 stream whose only content is the REASONING_EXTRACTION
        refusal metadataEvent (the captured production symptom).
        Purpose: probe returns ERROR with the retry-without-injection reason.
        """
        response = make_response([context_frame(), refusal_frame("REASONING_EXTRACTION")])
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert result.outcome == ProbeOutcome.ERROR
        assert result.failure.reason == REASON_REASONING_REFUSED
        response.aclose.assert_awaited()

    @pytest.mark.asyncio
    async def test_native_reasoning_only_answer_is_ok(self):
        """
        What it does: a stream with reasoning frames and no assistantResponse content
        (seen on some GPT answers), ending normally.
        Purpose: a reasoning-only answer must not be treated as empty.
        """
        response = make_response([reasoning_frame("thinking..."), reasoning_frame(" more"), metering_frame()])
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert result.outcome == ProbeOutcome.OK
        assert await result.response.aread()  # replays the bytes

    @pytest.mark.asyncio
    async def test_reasoning_then_refusal_still_reports_error(self):
        """Native reasoning does not commit the stream; a later refusal still wins."""
        response = make_response([reasoning_frame("thinking"), refusal_frame("REASONING_EXTRACTION")])
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert result.outcome == ProbeOutcome.ERROR
        assert result.failure.reason == REASON_REASONING_REFUSED

    @pytest.mark.asyncio
    async def test_reasoning_then_content_is_ok(self):
        response = make_response([reasoning_frame("r"), content_frame("answer"), metering_frame()])
        result = await probe_kiro_stream(response, first_token_timeout=1)
        assert result.outcome == ProbeOutcome.OK


class TestReasoningFallbackInHttpClient:
    @pytest.mark.asyncio
    async def test_refused_request_is_retried_without_injection(self):
        """
        What it does: first attempt refuses (REASONING_EXTRACTION), the client strips the
        injected prompt from the payload and the retry returns content.
        Purpose: opus-style refusals recover transparently in one request.
        """
        from kiro.converters_core import get_thinking_system_prompt_addition

        client, shared = _http_client([
            make_response([refusal_frame("REASONING_EXTRACTION")]),
            make_response([content_frame("pong"), metering_frame()]),
        ])
        addition = get_thinking_system_prompt_addition()
        prefix = (
            "<thinking_mode>enabled</thinking_mode>\n"
            "<max_thinking_length>8000</max_thinking_length>\n"
            "<thinking_instruction>x</thinking_instruction>\n\n"
        )
        payload = {
            "conversationState": {
                "currentMessage": {
                    "userInputMessage": {"content": prefix + "hi" + addition, "modelId": "claude-opus-5.5"}
                }
            }
        }
        with patch("kiro.http_client.asyncio.sleep", AsyncMock()):
            response = await client.request_with_retry("POST", "u", payload, stream=True, validate_stream=True)

        assert response.status_code == 200
        assert b"pong" in await response.aread()
        assert shared.send.await_count == 2
        # Payload was stripped in place so a caller retry reuses the clean version
        content = payload["conversationState"]["currentMessage"]["userInputMessage"]["content"]
        assert "<thinking_mode>" not in content

    @pytest.mark.asyncio
    async def test_model_marked_refused_skips_injection_next_time(self):
        from kiro import fake_reasoning_policy

        fake_reasoning_policy.reset_for_testing()
        try:
            client, _ = _http_client([make_response([refusal_frame("REASONING_EXTRACTION")]),
                                      make_response([content_frame("ok"), metering_frame()])])
            payload = {
                "conversationState": {
                    "currentMessage": {
                        "userInputMessage": {"content": "<thinking_mode>enabled</thinking_mode>\n"
                                             "<max_thinking_length>1</max_thinking_length>\n"
                                             "<thinking_instruction>x</thinking_instruction>\n\nhi",
                                             "modelId": "claude-opus-5.5"}
                    }
                }
            }
            with patch("kiro.http_client.asyncio.sleep", AsyncMock()):
                await client.request_with_retry("POST", "u", payload, stream=True, validate_stream=True)
            assert fake_reasoning_policy.should_inject("claude-opus-5.5") is False
        finally:
            fake_reasoning_policy.reset_for_testing()
