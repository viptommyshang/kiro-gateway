# -*- coding: utf-8 -*-

"""
Tests for ACCOUNT_SELECTION=round_robin and the 429 rate-limit cooldown.

Covers:
- AccountManager: round_robin rotation (default), sticky, 429 cooldown
- Routes (OpenAI + Anthropic): 429 failover within one request,
  no same-account 429 retries with 2+ accounts, 429 + Retry-After when
  every account is rate-limited, single-account behaviour unchanged
  (429 retried in place with backoff, no cooldown)
- Config: ACCOUNT_SELECTION parsing and round_robin default
"""

import importlib
import json
import time
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from kiro.account_errors import ErrorType
from kiro.account_manager import AccountManager
from kiro.config import ACCOUNT_RATE_LIMIT_COOLDOWN, _parse_account_selection


MODEL = "claude-sonnet-4.5"


def _make_manager(tmp_path, count: int, selection: str) -> AccountManager:
    """Build an AccountManager with `count` pre-initialized mock accounts."""
    manager = AccountManager(str(tmp_path / "credentials.json"), str(tmp_path / "state.json"))
    manager._selection = selection
    from kiro.account_manager import Account
    for i in range(count):
        account_id = f"/accounts/acct-{i}.sqlite3"
        auth_manager = MagicMock()
        auth_manager.api_host = f"https://runtime.example/{i}"
        auth_manager.profile_arn = "arn:aws:codewhisperer:us-east-1:123:profile/test"
        manager._accounts[account_id] = Account(
            id=account_id,
            auth_manager=auth_manager,
            model_cache=MagicMock(),
            model_resolver=MagicMock(),
            models_cached_at=time.time(),
        )
    return manager


def _rate_limited_response():
    response = Mock()
    response.status_code = 429
    response.aread = AsyncMock(return_value=json.dumps({
        "message": "Too many requests",
        "reason": "USER_REQUEST_RATE_EXCEEDED"
    }).encode())
    return response


def _ok_response():
    response = Mock()
    response.status_code = 200
    return response


class TestParseAccountSelection:
    def test_known_values(self):
        assert _parse_account_selection("sticky") == "sticky"
        assert _parse_account_selection(" ROUND_ROBIN ") == "round_robin"

    def test_unknown_value_falls_back_to_round_robin(self):
        assert _parse_account_selection("random") == "round_robin"

    def test_default_is_round_robin(self, monkeypatch, tmp_path):
        """
        What it does: Reloads kiro.config without ACCOUNT_SELECTION set.
        Purpose: round_robin is the default selection mode.
        """
        import kiro.config as config

        monkeypatch.delenv("ACCOUNT_SELECTION", raising=False)
        monkeypatch.setattr("dotenv.load_dotenv", lambda *args, **kwargs: False)
        try:
            importlib.reload(config)
            assert config.ACCOUNT_SELECTION == "round_robin"
        finally:
            monkeypatch.undo()
            importlib.reload(config)

    def test_account_manager_uses_configured_default(self, tmp_path):
        manager = AccountManager(str(tmp_path / "c.json"), str(tmp_path / "s.json"))
        assert manager._selection == "round_robin"


class TestRoundRobinSelection:
    @pytest.mark.asyncio
    async def test_round_robin_distributes_consecutive_requests(self, tmp_path):
        """
        What it does: Requests 6 accounts in a row from 3 accounts in round_robin.
        Purpose: Each request starts on the next account, even after successes.
        """
        manager = _make_manager(tmp_path, 3, "round_robin")
        ids = list(manager._accounts.keys())

        picked = []
        for _ in range(6):
            account = await manager.get_next_account(MODEL)
            picked.append(account.id)
            await manager.report_success(account.id, MODEL)

        assert picked == ids + ids
        assert manager._current_account_index == 0

    @pytest.mark.asyncio
    async def test_round_robin_failover_call_does_not_advance_cursor(self, tmp_path):
        manager = _make_manager(tmp_path, 3, "round_robin")
        ids = list(manager._accounts.keys())

        first = await manager.get_next_account(MODEL)
        second = await manager.get_next_account(MODEL, exclude_accounts={first.id})
        third = await manager.get_next_account(MODEL)

        assert [first.id, second.id, third.id] == [ids[0], ids[1], ids[1]]

    @pytest.mark.asyncio
    async def test_sticky_selection_unchanged(self, monkeypatch, tmp_path):
        """
        What it does: ACCOUNT_SELECTION=sticky keeps returning the last successful account.
        Purpose: sticky behaviour must not change now that it is opt-in.
        """
        monkeypatch.setattr("kiro.account_manager.ACCOUNT_SELECTION", "sticky")
        manager = AccountManager(str(tmp_path / "c.json"), str(tmp_path / "s.json"))
        assert manager._selection == "sticky"

        manager = _make_manager(tmp_path, 3, "sticky")
        ids = list(manager._accounts.keys())

        for _ in range(3):
            account = await manager.get_next_account(MODEL)
            assert account.id == ids[0]
            await manager.report_success(account.id, MODEL)

        await manager.report_success(ids[2], MODEL)
        assert manager._current_account_index == 2
        assert (await manager.get_next_account(MODEL)).id == ids[2]

    @pytest.mark.asyncio
    async def test_sticky_429_uses_circuit_breaker(self, tmp_path):
        manager = _make_manager(tmp_path, 2, "sticky")
        account_id = list(manager._accounts.keys())[0]

        await manager.report_failure(account_id, MODEL, ErrorType.RECOVERABLE, 429, "USER_REQUEST_RATE_EXCEEDED")

        account = manager._accounts[account_id]
        assert account.failures == 1
        assert account.cooldown_until == 0.0
        assert manager.retry_rate_limit_in_place() is True
        assert manager.rate_limit_retry_after() is None

    @pytest.mark.asyncio
    async def test_round_robin_429_sets_fixed_cooldown(self, tmp_path):
        """
        What it does: 429 in round_robin puts the account in a short fixed cooldown.
        Purpose: Rate limits skip the exponential breaker and the account is skipped.
        """
        manager = _make_manager(tmp_path, 2, "round_robin")
        ids = list(manager._accounts.keys())

        before = time.time()
        await manager.report_failure(ids[0], MODEL, ErrorType.RECOVERABLE, 429, "USER_REQUEST_RATE_EXCEEDED")

        account = manager._accounts[ids[0]]
        assert account.failures == 0
        assert account.cooldown_until >= before + ACCOUNT_RATE_LIMIT_COOLDOWN
        assert account.stats.failed_requests == 1
        assert manager.retry_rate_limit_in_place() is False

        for _ in range(3):
            assert (await manager.get_next_account(MODEL)).id == ids[1]

        assert 1 <= manager.rate_limit_retry_after() <= ACCOUNT_RATE_LIMIT_COOLDOWN

    @pytest.mark.asyncio
    async def test_round_robin_all_in_cooldown_returns_none(self, tmp_path):
        manager = _make_manager(tmp_path, 2, "round_robin")
        for account_id in manager._accounts:
            await manager.report_failure(account_id, MODEL, ErrorType.RECOVERABLE, 429, None)

        assert await manager.get_next_account(MODEL) is None
        assert manager.rate_limit_retry_after() is not None

    @pytest.mark.asyncio
    async def test_round_robin_single_account_unchanged(self, tmp_path):
        manager = _make_manager(tmp_path, 1, "round_robin")
        account_id = list(manager._accounts.keys())[0]

        await manager.report_failure(account_id, MODEL, ErrorType.RECOVERABLE, 429, None)

        account = manager._accounts[account_id]
        assert account.failures == 1
        assert account.cooldown_until == 0.0
        assert manager.retry_rate_limit_in_place() is True
        assert manager.rate_limit_retry_after() is None
        assert (await manager.get_next_account(MODEL)).id == account_id

    @pytest.mark.asyncio
    async def test_round_robin_single_account_gets_every_request(self, tmp_path):
        """
        What it does: Consecutive requests (successes and failures) on 1 account.
        Purpose: The only account is always selected; cursor and breaker are ignored.
        """
        manager = _make_manager(tmp_path, 1, "round_robin")
        account_id = list(manager._accounts.keys())[0]
        manager._rr_cursor = 7

        for status in (429, 402, 403, 429):
            await manager.report_failure(account_id, MODEL, ErrorType.RECOVERABLE, status, None)
            assert (await manager.get_next_account(MODEL)).id == account_id

        await manager.report_success(account_id, MODEL)
        for _ in range(3):
            assert (await manager.get_next_account(MODEL)).id == account_id

        account = manager._accounts[account_id]
        assert account.failures == 0
        assert account.cooldown_until == 0.0
        assert account.stats.successful_requests == 1
        assert account.stats.failed_requests == 4

    @pytest.mark.asyncio
    async def test_round_robin_cursor_wraps_after_accounts_removed(self, tmp_path):
        """
        What it does: Cursor past the end after accounts disappear.
        Purpose: Selection uses cursor modulo the current account count.
        """
        manager = _make_manager(tmp_path, 2, "round_robin")
        ids = list(manager._accounts.keys())
        manager._rr_cursor = 5
        manager._current_account_index = 5

        picked = [(await manager.get_next_account(MODEL)).id for _ in range(3)]

        assert picked == [ids[1], ids[0], ids[1]]


@pytest.fixture
def account_app(clean_app):
    """Run the app in account-system mode with a test AccountManager, restoring state afterwards."""
    from fastapi.testclient import TestClient

    with TestClient(clean_app) as client:
        saved = (clean_app.state.account_system, clean_app.state.account_manager)

        def install(manager):
            clean_app.state.account_system = True
            clean_app.state.account_manager = manager

        try:
            yield client, install
        finally:
            clean_app.state.account_system, clean_app.state.account_manager = saved


def _mock_http_client_class(responses):
    """KiroHttpClient replacement that records which auth manager each request used."""
    calls = []

    def factory(auth_manager, shared_client=None):
        client = MagicMock()
        client.client = MagicMock()
        client.close = AsyncMock()

        async def request_with_retry(*args, **kwargs):
            calls.append((auth_manager, kwargs.get("retry_on_429", True)))
            return responses.pop(0)

        client.request_with_retry = request_with_retry
        return client

    return factory, calls


def _openai_post(client, api_key):
    return client.post(
        "/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"model": MODEL, "messages": [{"role": "user", "content": "Hello"}], "stream": False},
    )


def _anthropic_post(client, api_key):
    return client.post(
        "/v1/messages",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01"},
        json={"model": MODEL, "max_tokens": 16, "messages": [{"role": "user", "content": "Hello"}]},
    )


class TestRoundRobinRoutes:
    def test_openai_429_fails_over_to_next_account(self, account_app, tmp_path, valid_proxy_api_key):
        """
        What it does: First account returns 429, second returns 200, in one request.
        Purpose: 429 fails over within the request without same-account retries.
        """
        client, install = account_app
        manager = _make_manager(tmp_path, 2, "round_robin")
        install(manager)
        accounts = list(manager._accounts.values())
        factory, calls = _mock_http_client_class([_rate_limited_response(), _ok_response()])

        with patch("kiro.routes_openai.KiroHttpClient", side_effect=factory), \
                patch("kiro.routes_openai.collect_stream_response", AsyncMock(return_value={"id": "ok"})):
            response = _openai_post(client, valid_proxy_api_key)

        assert response.status_code == 200
        assert response.json() == {"id": "ok"}
        assert [c[0] for c in calls] == [accounts[0].auth_manager, accounts[1].auth_manager]
        assert all(retry is False for _, retry in calls)
        assert accounts[0].cooldown_until > time.time()
        assert accounts[1].stats.successful_requests == 1

    def test_openai_all_rate_limited_returns_429(self, account_app, tmp_path, valid_proxy_api_key):
        client, install = account_app
        manager = _make_manager(tmp_path, 2, "round_robin")
        install(manager)
        factory, calls = _mock_http_client_class([_rate_limited_response(), _rate_limited_response()])

        with patch("kiro.routes_openai.KiroHttpClient", side_effect=factory):
            response = _openai_post(client, valid_proxy_api_key)

            assert response.status_code == 429
            assert 1 <= int(response.headers["Retry-After"]) <= ACCOUNT_RATE_LIMIT_COOLDOWN
            assert len(calls) == 2

            again = _openai_post(client, valid_proxy_api_key)

        assert again.status_code == 429
        assert len(calls) == 2

    def test_anthropic_all_rate_limited_returns_429(self, account_app, tmp_path, valid_proxy_api_key):
        client, install = account_app
        manager = _make_manager(tmp_path, 2, "round_robin")
        install(manager)
        factory, calls = _mock_http_client_class([_rate_limited_response(), _rate_limited_response()])

        with patch("kiro.routes_anthropic.KiroHttpClient", side_effect=factory):
            response = _anthropic_post(client, valid_proxy_api_key)

        assert response.status_code == 429
        assert response.headers["Retry-After"]
        assert response.json()["error"]["type"] == "rate_limit_error"
        assert len(calls) == 2
        assert all(retry is False for _, retry in calls)

    def test_sticky_multi_account_keeps_429_retries(self, account_app, tmp_path, valid_proxy_api_key):
        client, install = account_app
        manager = _make_manager(tmp_path, 2, "sticky")
        install(manager)
        factory, calls = _mock_http_client_class([_rate_limited_response(), _rate_limited_response()])

        with patch("kiro.routes_openai.KiroHttpClient", side_effect=factory), \
                patch("random.random", return_value=0.5):
            response = _openai_post(client, valid_proxy_api_key)

        assert response.status_code == 503
        assert all(retry is True for _, retry in calls)

    def test_single_account_round_robin_unchanged(self, account_app, tmp_path, valid_proxy_api_key):
        client, install = account_app
        manager = _make_manager(tmp_path, 1, "round_robin")
        install(manager)
        factory, calls = _mock_http_client_class([_rate_limited_response()])

        with patch("kiro.routes_openai.KiroHttpClient", side_effect=factory):
            response = _openai_post(client, valid_proxy_api_key)

        assert response.status_code == 429
        assert "Retry-After" not in response.headers
        assert calls[0][1] is True
        assert len(calls) == 1


def _upstream_response(status_code: int, body: bytes = b""):
    """httpx-like streamed response for KiroHttpClient."""
    if status_code == 200 and not body:
        # A 200 must carry content, otherwise stream validation treats it as empty
        body = b'{"content":"ok"}'
    response = MagicMock()
    response.status_code = status_code
    response.aread = AsyncMock(return_value=body)
    response.aclose = AsyncMock()

    async def aiter_bytes():
        yield body

    response.aiter_bytes = aiter_bytes
    return response


class TestSingleAccountRoundRobinEndToEnd:
    """Real KiroHttpClient + routes with one account in round_robin (the default)."""

    def _install(self, account_app, tmp_path, upstream):
        client, install = account_app
        manager = _make_manager(tmp_path, 1, "round_robin")
        account = list(manager._accounts.values())[0]
        account.auth_manager.get_access_token = AsyncMock(return_value="token")
        account.auth_manager.fingerprint = "fp"
        install(manager)
        shared = MagicMock()
        shared.build_request = MagicMock(return_value="request")
        shared.send = AsyncMock(side_effect=upstream)
        return client, manager, account, shared

    def test_429_retried_on_same_account_then_returned(self, account_app, tmp_path, valid_proxy_api_key):
        """
        What it does: Upstream answers 429 three times, then 200 on the next request.
        Purpose: 429 is retried in place with backoff, the client gets the real 429,
        and no cooldown blocks the next request.
        """
        rate_limited = json.dumps({"message": "Too many requests", "reason": "USER_REQUEST_RATE_EXCEEDED"}).encode()
        upstream = [_upstream_response(429, rate_limited) for _ in range(3)] + [_upstream_response(200)]
        client, manager, account, shared = self._install(account_app, tmp_path, upstream)
        sleep = AsyncMock()

        with patch.object(client.app.state, "http_client", shared), \
                patch("kiro.http_client.asyncio.sleep", sleep), \
                patch("kiro.routes_openai.collect_stream_response", AsyncMock(return_value={"id": "ok"})):
            response = _openai_post(client, valid_proxy_api_key)

            assert response.status_code == 429
            assert "Retry-After" not in response.headers
            assert "All available accounts" not in response.text
            assert shared.send.await_count == 3
            assert [c.args[0] for c in sleep.await_args_list] == [1.0, 2.0, 4.0]
            assert account.cooldown_until == 0.0

            again = _openai_post(client, valid_proxy_api_key)

        assert again.status_code == 200
        assert again.json() == {"id": "ok"}
        assert shared.send.await_count == 4
        assert account.failures == 0
        assert account.stats.successful_requests == 1

    def test_402_returned_with_real_status(self, account_app, tmp_path, valid_proxy_api_key):
        quota = json.dumps({"message": "Monthly limit reached", "reason": "MONTHLY_REQUEST_COUNT"}).encode()
        client, manager, account, shared = self._install(account_app, tmp_path, [_upstream_response(402, quota)])

        with patch.object(client.app.state, "http_client", shared):
            response = _anthropic_post(client, valid_proxy_api_key)

        assert response.status_code == 402
        assert shared.send.await_count == 1

    def test_consecutive_successes_stay_on_the_account(self, account_app, tmp_path, valid_proxy_api_key):
        client, manager, account, shared = self._install(
            account_app, tmp_path, [_upstream_response(200) for _ in range(3)]
        )

        with patch.object(client.app.state, "http_client", shared), \
                patch("kiro.routes_openai.collect_stream_response", AsyncMock(return_value={"id": "ok"})):
            statuses = [_openai_post(client, valid_proxy_api_key).status_code for _ in range(3)]

        assert statuses == [200, 200, 200]
        assert account.stats.successful_requests == 3


class TestAllAccountsBusyRoutes:
    """ACCOUNT_MAX_CONCURRENCY reached on every account: 503 + Retry-After, both APIs."""

    def _busy_manager(self, tmp_path):
        manager = _make_manager(tmp_path, 2, "round_robin")
        manager._max_concurrency = 1
        manager._queue_timeout = 0.01
        for account in manager._accounts.values():
            account.in_flight = 1
        return manager

    def test_openai_returns_503_with_retry_after(self, account_app, tmp_path, valid_proxy_api_key):
        client, install = account_app
        install(self._busy_manager(tmp_path))

        response = _openai_post(client, valid_proxy_api_key)

        assert response.status_code == 503
        assert response.headers["Retry-After"] == "5"
        assert "ACCOUNT_MAX_CONCURRENCY" in response.text

    def test_anthropic_returns_503_with_retry_after(self, account_app, tmp_path, valid_proxy_api_key):
        client, install = account_app
        install(self._busy_manager(tmp_path))

        response = _anthropic_post(client, valid_proxy_api_key)

        assert response.status_code == 503
        assert response.headers["Retry-After"] == "5"
        assert response.json()["error"]["type"] == "overloaded_error"

    def test_slots_are_returned_after_failed_requests(self, account_app, tmp_path, valid_proxy_api_key):
        """
        What it does: Both accounts answer 429.
        Purpose: Failover paths release every slot they reserved.
        """
        client, install = account_app
        manager = _make_manager(tmp_path, 2, "round_robin")
        install(manager)
        factory, _ = _mock_http_client_class([_rate_limited_response(), _rate_limited_response()])

        with patch("kiro.routes_openai.KiroHttpClient", side_effect=factory):
            response = _openai_post(client, valid_proxy_api_key)

        assert response.status_code == 429
        assert all(a.in_flight == 0 for a in manager._accounts.values())
