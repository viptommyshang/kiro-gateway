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
HTTP client for Kiro API with retry logic support.

Handles:
- 403: automatic token refresh and retry
- 429: exponential backoff
- 5xx: exponential backoff
- Timeouts: exponential backoff
- HTTP 200 with empty body or in-stream exception (opt-in stream validation)

Supports both per-request clients and shared application-level client
with connection pooling for better resource management.
"""

import asyncio
import json
from typing import Optional

import httpx
from fastapi import HTTPException
from loguru import logger

from kiro.config import (
    MAX_RETRIES,
    BASE_RETRY_DELAY,
    FIRST_TOKEN_MAX_RETRIES,
    FIRST_TOKEN_TIMEOUT,
    STREAMING_READ_TIMEOUT,
    STREAM_VALIDATION_ENABLED,
)
from kiro.auth import KiroAuthManager
from kiro.utils import get_kiro_headers
from kiro.network_errors import classify_network_error, get_short_error_message, NetworkErrorInfo
from kiro import fake_reasoning_policy
from kiro.converters_core import get_thinking_system_prompt_addition
from kiro.stream_validation import (
    REASON_REASONING_REFUSED,
    ProbeOutcome,
    build_failure_response,
    probe_kiro_stream,
)


class KiroHttpClient:
    """
    HTTP client for Kiro API with retry logic support.
    
    Automatically handles errors and retries requests:
    - 403: refreshes token and retries
    - 429: waits with exponential backoff
    - 5xx: waits with exponential backoff
    - Timeouts: waits with exponential backoff
    
    Supports two modes of operation:
    1. Per-request client: Creates and owns its own httpx.AsyncClient
    2. Shared client: Uses an application-level shared client (recommended)
    
    Using a shared client reduces memory usage and enables connection pooling,
    which is especially important for handling concurrent requests.
    
    Attributes:
        auth_manager: Authentication manager for obtaining tokens
        client: httpx HTTP client (owned or shared)
    
    Example:
        >>> # Per-request client (legacy mode)
        >>> client = KiroHttpClient(auth_manager)
        >>> response = await client.request_with_retry(...)
        
        >>> # Shared client (recommended)
        >>> shared = httpx.AsyncClient(limits=httpx.Limits(...))
        >>> client = KiroHttpClient(auth_manager, shared_client=shared)
        >>> response = await client.request_with_retry(...)
    """
    
    def __init__(
        self,
        auth_manager: KiroAuthManager,
        shared_client: Optional[httpx.AsyncClient] = None
    ):
        """
        Initializes the HTTP client.
        
        Args:
            auth_manager: Authentication manager
            shared_client: Optional shared httpx.AsyncClient for connection pooling.
                          If provided, this client will be used instead of creating
                          a new one. The shared client will NOT be closed by close().
        """
        self.auth_manager = auth_manager
        self._shared_client = shared_client
        self._owns_client = shared_client is None
        self.client: Optional[httpx.AsyncClient] = shared_client
    
    async def _get_client(self, stream: bool = False) -> httpx.AsyncClient:
        """
        Returns or creates an HTTP client with proper timeouts.
        
        If a shared client was provided at initialization, it is returned as-is.
        Otherwise, creates a new client with appropriate timeout configuration.
        
        httpx timeouts:
        - connect: TCP handshake (DNS + TCP SYN/ACK)
        - read: waiting for data from server between chunks
        - write: sending data to server
        - pool: waiting for free connection from pool
        
        IMPORTANT: FIRST_TOKEN_TIMEOUT is NOT used here!
        It is applied in streaming_openai.py via asyncio.wait_for() to control
        the wait time for the first token from the model (retry business logic).
        
        Args:
            stream: If True, uses STREAMING_READ_TIMEOUT for read (only for new clients)
        
        Returns:
            Active HTTP client
        """
        # If using shared client, return it directly
        # Shared client should be pre-configured with appropriate timeouts
        if self._shared_client is not None:
            return self._shared_client
        
        # Create new client if needed (per-request mode)
        if self.client is None or self.client.is_closed:
            if stream:
                # For streaming:
                # - connect: 30 sec (TCP connection, usually < 1 sec)
                # - read: STREAMING_READ_TIMEOUT (300 sec) - model may "think" between chunks
                # - write/pool: standard values
                timeout_config = httpx.Timeout(
                    connect=30.0,
                    read=STREAMING_READ_TIMEOUT,
                    write=30.0,
                    pool=30.0
                )
                logger.debug(f"Creating streaming HTTP client (read_timeout={STREAMING_READ_TIMEOUT}s)")
            else:
                # For regular requests: single timeout of 300 sec
                timeout_config = httpx.Timeout(timeout=300.0)
                logger.debug("Creating non-streaming HTTP client (timeout=300s)")
            
            self.client = httpx.AsyncClient(timeout=timeout_config, follow_redirects=True)
        return self.client
    
    async def close(self) -> None:
        """
        Closes the HTTP client if this instance owns it.
        
        If using a shared client, this method does nothing - the shared client
        should be closed by the application lifecycle manager.
        
        Uses graceful exception handling to prevent errors during cleanup
        from masking the original exception in finally blocks.
        """
        # Don't close shared clients - they're managed by the application
        if not self._owns_client:
            return
        
        if self.client and not self.client.is_closed:
            try:
                await self.client.aclose()
            except Exception as e:
                # Log but don't propagate - we're in cleanup code
                # Propagating here could mask the original exception
                logger.warning(f"Error closing HTTP client: {e}")
    
    @staticmethod
    async def _read_and_release(response: httpx.Response) -> None:
        """
        Reads a response body so its pool connection is released.
        
        The body stays cached on the response, so the caller can still
        aread() it if this response is returned after retries are exhausted.
        On read failure the response is closed and the error is re-raised.
        """
        try:
            await response.aread()
        except BaseException:
            await response.aclose()
            raise
    
    async def request_with_retry(
        self,
        method: str,
        url: str,
        json_data: Optional[dict] = None,
        params: Optional[dict] = None,
        stream: bool = False,
        retry_on_429: bool = True,
        validate_stream: bool = False
    ) -> httpx.Response:
        """
        Executes an HTTP request with retry logic.
        
        Automatically handles various error types:
        - 403: refreshes token via auth_manager.force_refresh() and retries
        - 429: waits with exponential backoff (1s, 2s, 4s)
        - 5xx: waits with exponential backoff
        - Timeouts: waits with exponential backoff
        
        For streaming, STREAMING_READ_TIMEOUT is used for waiting between chunks.
        First token timeout is controlled separately in streaming_openai.py via asyncio.wait_for().
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            json_data: Optional JSON body (for POST/PUT/PATCH)
            params: Optional query parameters (for GET)
            stream: Use streaming (default False)
            retry_on_429: Retry 429 on this account (default True); when False,
                the 429 response is returned at once so the caller can fail over
            validate_stream: For streaming generateAssistantResponse calls, read
                the 200 body until it yields content (see kiro.stream_validation).
                Empty bodies and in-stream exceptions are retried like 5xx/429 and
                finally returned as synthetic error responses, so they reach the
                normal failover/error handling instead of the client as an empty
                "success". The returned 200 response replays the probed bytes.
        
        Returns:
            httpx.Response with successful response
        
        Raises:
            HTTPException: On failure after all attempts (502/504),
                or 503 immediately when the connection pool is exhausted
        """
        # Determine the number of retry attempts
        # FIRST_TOKEN_TIMEOUT is used in streaming_openai.py, not here
        max_retries = FIRST_TOKEN_MAX_RETRIES if stream else MAX_RETRIES
        
        client = await self._get_client(stream=stream)
        last_error = None
        last_error_info: Optional[NetworkErrorInfo] = None
        last_response: Optional[httpx.Response] = None  # Для сохранения последнего 429/5xx
        first_token_timeouts = 0
        reasoning_fallback_used = False
        validate_stream = validate_stream and STREAM_VALIDATION_ENABLED
        
        for attempt in range(max_retries):
            try:
                # Get current token
                token = await self.auth_manager.get_access_token()
                headers = get_kiro_headers(self.auth_manager, token)
                
                # Build request kwargs based on parameters
                request_kwargs = {"headers": headers}
                
                if json_data is not None:
                    request_kwargs["content"] = json.dumps(json_data).encode()
                
                if params is not None:
                    request_kwargs["params"] = params
                
                if stream:
                    # Prevent CLOSE_WAIT connection leak (issue #38)
                    headers["Connection"] = "close"
                    req = client.build_request(method, url, **request_kwargs)
                    logger.debug("Sending request to Kiro API...")
                    response = await client.send(req, stream=True)
                else:
                    logger.debug("Sending request to Kiro API...")
                    response = await client.request(method, url, **request_kwargs)
                
                # Check status
                if response.status_code == 200:
                    if not (validate_stream and stream):
                        return response
                    
                    try:
                        probe = await probe_kiro_stream(response, FIRST_TOKEN_TIMEOUT)
                    except asyncio.TimeoutError:
                        first_token_timeouts += 1
                        logger.warning(
                            f"[FirstTokenTimeout] Model did not respond within {FIRST_TOKEN_TIMEOUT}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        continue
                    
                    if probe.outcome == ProbeOutcome.OK and probe.response is not None:
                        return probe.response  # type: ignore[return-value]
                    
                    failure = probe.failure
                    assert failure is not None
                    synthetic = build_failure_response(failure, response.request)
                    
                    if (
                        failure.reason == REASON_REASONING_REFUSED
                        and not reasoning_fallback_used
                        and isinstance(json_data, dict)
                    ):
                        reasoning_fallback_used = True
                        fake_reasoning_policy.mark_refused(fake_reasoning_policy.get_payload_model_id(json_data))
                        # In place: callers that retry with the same payload benefit too
                        if fake_reasoning_policy.strip_fake_reasoning(json_data, get_thinking_system_prompt_addition()):
                            logger.warning("Retrying immediately without the fake reasoning prompt")
                            last_response = synthetic  # Reported if no attempt is left
                            continue
                    
                    if not failure.is_retryable:
                        logger.warning(
                            f"Kiro stream failed ({failure.reason}): {failure.message[:200]} - not retrying"
                        )
                        return synthetic
                    
                    # With failover enabled another account is the better retry target
                    if not retry_on_429:
                        logger.warning(
                            f"Kiro stream failed ({failure.reason}), handing over to account failover"
                        )
                        return synthetic
                    
                    last_response = synthetic
                    if attempt < max_retries - 1:
                        delay = BASE_RETRY_DELAY * (2 ** attempt)
                        logger.warning(
                            f"Kiro stream failed ({failure.reason}), retrying in {delay}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(delay)
                    continue
                
                # 403 - token expired, refresh and retry
                if response.status_code == 403:
                    logger.warning(f"Received 403, refreshing token (attempt {attempt + 1}/{MAX_RETRIES})")
                    # Release the pool connection before retrying
                    await response.aclose()
                    await self.auth_manager.force_refresh()
                    continue
                
                # 429 - rate limit, wait and retry
                if response.status_code == 429:
                    if not retry_on_429:
                        logger.warning("Received 429, not retrying on this account (failover enabled)")
                        return response
                    await self._read_and_release(response)
                    last_response = response  # Сохраняем для возврата после exhaustion
                    delay = BASE_RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Received 429, waiting {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                
                # 5xx - server error, wait and retry
                if 500 <= response.status_code < 600:
                    await self._read_and_release(response)
                    last_response = response  # Сохраняем для возврата после exhaustion
                    delay = BASE_RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Received {response.status_code}, waiting {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                
                # Other errors - return as is
                return response
                
            except httpx.TimeoutException as e:
                # Pool exhaustion is local overload: retrying only queues the request again
                if isinstance(e, httpx.PoolTimeout):
                    logger.error(f"Connection pool exhausted, not retrying (attempt {attempt + 1}/{max_retries})")
                    raise HTTPException(
                        status_code=503,
                        detail="Gateway overloaded: no free upstream connection, retry later",
                        headers={"Retry-After": "5"}
                    )
                
                last_error = e
                
                # Classify timeout error for user-friendly messaging
                error_info = classify_network_error(e)
                last_error_info = error_info
                
                # Log with user-friendly message
                short_msg = get_short_error_message(error_info)
                
                if error_info.is_retryable and attempt < max_retries - 1:
                    delay = BASE_RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"{short_msg} - waiting {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"{short_msg} - no more retries (attempt {attempt + 1}/{max_retries})")
                    if not error_info.is_retryable:
                        break  # Don't retry non-retryable errors
                
            except httpx.RequestError as e:
                last_error = e
                
                # Classify the error for user-friendly messaging
                error_info = classify_network_error(e)
                last_error_info = error_info
                
                # Log with user-friendly message
                short_msg = get_short_error_message(error_info)
                
                if error_info.is_retryable and attempt < max_retries - 1:
                    delay = BASE_RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"{short_msg} - waiting {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"{short_msg} - no more retries (attempt {attempt + 1}/{max_retries})")
                    if not error_info.is_retryable:
                        break  # Don't retry non-retryable errors
        
        # If we have a last_response (429/5xx retry exhausted), return it
        # This allows the caller to see the real status code and error body
        if last_response is not None:
            logger.warning(
                f"Retries exhausted for HTTP {last_response.status_code}, "
                f"returning response to caller for classification"
            )
            return last_response
        
        if first_token_timeouts and last_error_info is None:
            raise HTTPException(
                status_code=504,
                detail=(
                    f"Model did not respond within {FIRST_TOKEN_TIMEOUT}s after {max_retries} attempts. "
                    "Please try again, or raise FIRST_TOKEN_TIMEOUT for very large requests."
                )
            )
        
        # All attempts exhausted - provide detailed, user-friendly error message
        if last_error_info:
            # Use classified error information
            error_message = last_error_info.user_message
            
            # Add troubleshooting steps
            if last_error_info.troubleshooting_steps:
                error_message += "\n\nTroubleshooting:\n"
                for i, step in enumerate(last_error_info.troubleshooting_steps, 1):
                    error_message += f"{i}. {step}\n"
            
            # Add technical details for debugging
            error_message += f"\nTechnical details: {last_error_info.technical_details}"
            
            raise HTTPException(
                status_code=last_error_info.suggested_http_code,
                detail=error_message.strip()
            )
        else:
            # Fallback if no error was captured (shouldn't happen)
            if stream:
                raise HTTPException(
                    status_code=504,
                    detail=f"Streaming failed after {max_retries} attempts. Unknown error."
                )
            else:
                raise HTTPException(
                    status_code=502,
                    detail=f"Request failed after {max_retries} attempts. Unknown error."
                )
    
    async def __aenter__(self) -> "KiroHttpClient":
        """Async context manager support."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Closes the client when exiting context."""
        await self.close()