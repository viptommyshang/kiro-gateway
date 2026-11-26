# -*- coding: utf-8 -*-

# Kiro Gateway
# https://github.com/jwadow/kiro-gateway
# Copyright (C) 2025 Jwadow
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

"""
Validation of Kiro streams before they are committed to the client.

Kiro answers ``generateAssistantResponse`` with HTTP 200 even when the
generation fails: the failure arrives *inside* the body as an exception
frame (throttling, capacity, validation, ...) or the body simply carries no
content at all. Status-code based retry and account failover never see
these, and clients receive a "successful" empty completion.

This module reads a 200 stream until it proves useful (first content or
tool call), fails (exception frame), or ends empty. Useful streams are
handed back as a :class:`ReplayResponse` that replays the bytes already read;
failures are converted into a synthetic error response so that the existing
HTTP-status machinery (retry, failover, error classification, user-facing
messages) handles them like any other upstream error.

API-agnostic: used by both OpenAI and Anthropic routes, streaming and
non-streaming alike, because all of them read Kiro through
``KiroHttpClient.request_with_retry``.
"""

import asyncio
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncIterator, List, Optional

import httpx
from loguru import logger

from kiro.parsers import REFUSAL_EXCEPTION_TYPE, AwsEventStreamParser


# ==================================================================================================
# Failure taxonomy
# ==================================================================================================

REASON_EMPTY_RESPONSE = "EMPTY_RESPONSE"
"""Kiro returned HTTP 200 but no content, tool call or error."""

REASON_THROTTLED = "THROTTLED"
"""Kiro reported throttling / capacity exhaustion inside the stream."""

REASON_STREAM_ERROR = "UPSTREAM_STREAM_ERROR"
"""Kiro reported a server-side failure inside the stream."""

REASON_REASONING_REFUSED = "REASONING_EXTRACTION_REFUSED"
"""The model refused the request because of the injected fake-reasoning prompt."""

REASON_CONTENT_FILTERED = "CONTENT_FILTERED"
"""The model refused the request for content reasons (not retryable)."""

RETRYABLE_STREAM_REASONS = frozenset({REASON_EMPTY_RESPONSE, REASON_STREAM_ERROR})
"""Reasons where another attempt (or another account) is likely to succeed."""

_THROTTLING_EXCEPTIONS = frozenset({
    "ThrottlingException",
    "TooManyRequestsException",
    "ServiceQuotaExceededException",
    "RateLimitExceededException",
    "LimitExceededException",
})
_THROTTLING_MARKERS = ("throttl", "too many requests", "rate limit", "rate exceeded", "capacity")

_CLIENT_ERROR_EXCEPTIONS = frozenset({
    "ValidationException",
    "BadRequestException",
    "SerializationException",
    "ContentLengthExceededException",
})

_AUTH_EXCEPTIONS = frozenset({
    "AccessDeniedException",
    "UnauthorizedException",
    "ExpiredTokenException",
    "UnrecognizedClientException",
})

_PREVIEW_BYTES = 300
"""How much of an empty body to include in diagnostics."""


class ProbeOutcome(str, Enum):
    """Result category of :func:`probe_kiro_stream`."""

    OK = "ok"
    EMPTY = "empty"
    ERROR = "error"


@dataclass
class StreamFailure:
    """
    A Kiro stream failure mapped onto HTTP semantics.

    Attributes:
        status_code: HTTP status the failure is reported as
        reason: Machine-readable reason (used by error classification)
        message: Human-readable explanation
        exception_type: Original Kiro exception type, if any
    """
    status_code: int
    reason: str
    message: str
    exception_type: Optional[str] = None

    @property
    def is_rate_limit(self) -> bool:
        """True for throttling failures (treated like HTTP 429)."""
        return self.status_code == 429

    @property
    def is_retryable(self) -> bool:
        """True if retrying (same or other account) may succeed."""
        return self.is_rate_limit or self.reason in RETRYABLE_STREAM_REASONS


@dataclass
class ProbeResult:
    """
    Outcome of probing a Kiro stream.

    Attributes:
        outcome: OK, EMPTY or ERROR
        response: Replayable response (only for OK)
        failure: Mapped failure (only for EMPTY and ERROR)
    """
    outcome: ProbeOutcome
    response: Optional["ReplayResponse"] = None
    failure: Optional[StreamFailure] = None


class KiroStreamError(Exception):
    """
    Raised when Kiro reports an exception inside an already-started stream.

    Attributes:
        exception_type: Kiro exception type (e.g. ThrottlingException)
        reason: Optional reason from the payload
    """

    def __init__(self, exception_type: str, message: str, reason: Optional[str] = None) -> None:
        """
        Args:
            exception_type: Kiro exception type
            message: Exception message from Kiro
            reason: Optional reason code
        """
        self.exception_type = exception_type
        self.reason = reason
        super().__init__(f"Kiro API error during streaming ({exception_type}): {message}")


def classify_stream_exception(
    exception_type: str,
    message: str,
    reason: Optional[str] = None,
) -> StreamFailure:
    """
    Maps a Kiro in-stream exception onto an HTTP-style failure.

    Args:
        exception_type: Exception or event type from the frame headers
        message: Exception message from the payload
        reason: Optional ``reason`` field from the payload

    Returns:
        StreamFailure describing how the gateway should treat it

    Example:
        >>> classify_stream_exception("ThrottlingException", "Too many requests").status_code
        429
    """
    description = f"{exception_type}: {message}"
    throttled = StreamFailure(
        status_code=429,
        reason=REASON_THROTTLED,
        message=f"Kiro API is throttling this account ({description})",
        exception_type=exception_type,
    )

    if exception_type in _THROTTLING_EXCEPTIONS:
        return throttled
    if exception_type == REFUSAL_EXCEPTION_TYPE:
        if reason == "REASONING_EXTRACTION":
            return StreamFailure(
                status_code=400,
                reason=REASON_REASONING_REFUSED,
                message=(
                    f"The model refused the request because of the gateway's fake reasoning prompt ({message}). "
                    "Retry without it, or set FAKE_REASONING=false."
                ),
                exception_type=exception_type,
            )
        return StreamFailure(
            status_code=400,
            reason=REASON_CONTENT_FILTERED,
            message=f"The model refused to answer (category: {reason or 'unknown'}): {message}",
            exception_type=exception_type,
        )
    if exception_type in _CLIENT_ERROR_EXCEPTIONS:
        # Keep Kiro's own reason (e.g. CONTENT_LENGTH_EXCEEDS_THRESHOLD) so the
        # existing 400 classification and friendly messages still apply.
        return StreamFailure(
            status_code=400,
            reason=reason or exception_type,
            message=message,
            exception_type=exception_type,
        )
    if exception_type in _AUTH_EXCEPTIONS:
        return StreamFailure(
            status_code=403,
            reason=reason or exception_type,
            message=message,
            exception_type=exception_type,
        )
    # Unmodeled exceptions: fall back to the wording Kiro used.
    lowered = f"{exception_type} {message} {reason or ''}".lower()
    if any(marker in lowered for marker in _THROTTLING_MARKERS):
        return throttled
    return StreamFailure(
        status_code=502,
        reason=REASON_STREAM_ERROR,
        message=f"Kiro API failed while generating the response ({description})",
        exception_type=exception_type,
    )


def build_failure_response(failure: StreamFailure, request: Optional[httpx.Request]) -> httpx.Response:
    """
    Builds a synthetic HTTP response carrying a stream failure.

    The body uses Kiro's own error shape (``{"message", "reason"}``), so
    ``enhance_kiro_error`` and ``classify_error`` work unchanged.

    Args:
        failure: Failure to report
        request: Original request (attached for httpx compatibility)

    Returns:
        httpx.Response with the failure status and JSON body
    """
    headers = {"content-type": "application/json"}
    if failure.is_rate_limit:
        headers["retry-after"] = "5"
    return httpx.Response(
        status_code=failure.status_code,
        headers=headers,
        content=json.dumps({"message": failure.message, "reason": failure.reason}).encode("utf-8"),
        request=request,
    )


# ==================================================================================================
# Replay wrapper
# ==================================================================================================

class ReplayResponse:
    """
    Response wrapper that replays already-consumed bytes before the rest.

    Behaves like the wrapped ``httpx.Response`` for everything the streaming
    code uses (``status_code``, ``headers``, ``aiter_bytes``, ``aread``,
    ``aclose``); any other attribute is delegated.
    """

    def __init__(
        self,
        response: httpx.Response,
        consumed: List[bytes],
        remaining: Optional[AsyncIterator[bytes]],
    ) -> None:
        """
        Args:
            response: Original streaming response
            consumed: Chunks read during probing, in order
            remaining: Iterator positioned after ``consumed`` (None if exhausted)
        """
        self._response = response
        self._consumed = consumed
        self._remaining = remaining

    @property
    def status_code(self) -> int:
        """HTTP status of the wrapped response."""
        return self._response.status_code

    @property
    def headers(self) -> httpx.Headers:
        """Headers of the wrapped response."""
        return self._response.headers

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        """
        Yields the probed chunks, then the rest of the body.

        Yields:
            Body chunks
        """
        consumed, self._consumed = self._consumed, []
        for chunk in consumed:
            yield chunk
        if self._remaining is not None:
            remaining, self._remaining = self._remaining, None
            async for chunk in remaining:
                yield chunk

    async def aread(self) -> bytes:
        """
        Reads the whole (remaining) body.

        Returns:
            Concatenated body bytes
        """
        return b"".join([chunk async for chunk in self.aiter_bytes()])

    async def aclose(self) -> None:
        """Closes the wrapped response."""
        await self._response.aclose()

    def __getattr__(self, name: str) -> Any:
        """Delegates unknown attributes to the wrapped response."""
        return getattr(self._response, name)


# ==================================================================================================
# Probe
# ==================================================================================================

def _has_meaningful_output(events: List[dict], parser: AwsEventStreamParser) -> bool:
    """
    Checks whether parsed events contain something the client can use.

    Args:
        events: Events returned by the last ``parser.feed`` call
        parser: Parser (tool calls are tracked on it, not emitted as events)

    Returns:
        True on non-empty content or any tool call activity
    """
    if parser.current_tool_call is not None or parser.tool_calls:
        return True
    # Native reasoning alone does not commit the stream: Kiro reports refusals
    # only at the end, and committing early would make them unretryable.
    return any(event["type"] == "content" and event.get("data") for event in events)


async def probe_kiro_stream(response: httpx.Response, first_token_timeout: float) -> ProbeResult:
    """
    Reads a 200 Kiro stream until it is proven useful, failed, or empty.

    Only the wait for the *first* chunk is bounded by ``first_token_timeout``
    (same semantics as before probing existed); later reads rely on the
    client's read timeout.

    Args:
        response: Streaming HTTP 200 response from Kiro
        first_token_timeout: Seconds to wait for the first chunk

    Returns:
        ProbeResult. For OK, use ``result.response`` instead of ``response``.
        For EMPTY/ERROR the response has been closed.

    Raises:
        asyncio.TimeoutError: If no chunk arrives within ``first_token_timeout``
            (the response is closed before raising)
    """
    parser = AwsEventStreamParser()
    iterator = response.aiter_bytes().__aiter__()
    consumed: List[bytes] = []
    total_bytes = 0
    saw_reasoning = False

    async def close() -> None:
        try:
            await response.aclose()
        except (httpx.HTTPError, RuntimeError) as e:
            logger.debug(f"Error closing probed response: {e}")

    try:
        first = True
        while True:
            try:
                if first:
                    chunk = await asyncio.wait_for(iterator.__anext__(), timeout=first_token_timeout)
                    first = False
                else:
                    chunk = await iterator.__anext__()
            except StopAsyncIteration:
                break

            consumed.append(chunk)
            total_bytes += len(chunk)
            events = parser.feed(chunk)
            saw_reasoning = saw_reasoning or any(event["type"] == "reasoning" for event in events)

            for event in events:
                if event["type"] == "error":
                    data = event["data"]
                    failure = classify_stream_exception(
                        data["exception_type"], data["message"], data.get("reason")
                    )
                    await close()
                    return ProbeResult(outcome=ProbeOutcome.ERROR, failure=failure)

            if _has_meaningful_output(events, parser):
                return ProbeResult(
                    outcome=ProbeOutcome.OK,
                    response=ReplayResponse(response, consumed, iterator),
                )
    except asyncio.TimeoutError:
        await close()
        raise

    # Stream ended. A tool call may complete only at the very end; a
    # reasoning-only answer is still an answer.
    if _has_meaningful_output([], parser) or saw_reasoning:
        return ProbeResult(
            outcome=ProbeOutcome.OK,
            response=ReplayResponse(response, consumed, None),
        )

    preview = b"".join(consumed)[:_PREVIEW_BYTES]
    logger.warning(
        f"Kiro API returned HTTP 200 without any content ({total_bytes} bytes, "
        f"{len(consumed)} chunks). Body preview: {preview!r}"
    )
    await close()
    return ProbeResult(
        outcome=ProbeOutcome.EMPTY,
        failure=StreamFailure(
            status_code=502,
            reason=REASON_EMPTY_RESPONSE,
            message="Kiro API returned an empty response (no content or tool calls)",
        ),
    )
