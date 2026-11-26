# -*- coding: utf-8 -*-

"""
Tests for the account pool: removing accounts without quota / with broken
credentials, automatic return after a TTL, one-click reset, status, state
persistence, and the /admin/accounts endpoints.
"""

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kiro.account_errors import ErrorType
from kiro.account_manager import (
    POOL_ACTIVE,
    POOL_EXHAUSTED,
    POOL_INVALID,
    Account,
    AccountManager,
)

MODEL = "claude-sonnet-5.5"


def _manager(tmp_path, count: int = 3) -> AccountManager:
    manager = AccountManager(str(tmp_path / "credentials.json"), str(tmp_path / "state.json"))
    manager._selection = "round_robin"
    for i in range(count):
        auth = MagicMock()
        auth.api_host = "https://runtime.us-east-1.kiro.dev"
        manager._accounts[f"acct-{i}"] = Account(
            id=f"acct-{i}", auth_manager=auth, model_cache=MagicMock(),
            model_resolver=MagicMock(), models_cached_at=time.time(),
        )
    return manager


async def _pick_ids(manager: AccountManager, n: int) -> list:
    ids = []
    for _ in range(n):
        account = await manager.get_next_account(MODEL)
        ids.append(account.id if account else None)
        if account:
            manager.lease(account.id).release()
    return ids


class TestPoolRemoval:
    @pytest.mark.asyncio
    async def test_quota_exhausted_account_leaves_rotation(self, tmp_path):
        """
        What it does: Account 0 answers 402 MONTHLY_REQUEST_COUNT.
        Purpose: It is skipped completely (no probabilistic retries), others share the load.
        """
        manager = _manager(tmp_path)
        await manager.report_failure("acct-0", MODEL, ErrorType.RECOVERABLE, 402, "MONTHLY_REQUEST_COUNT")

        account = manager._accounts["acct-0"]
        assert account.pool_state == POOL_EXHAUSTED
        assert account.pool_reason == "MONTHLY_REQUEST_COUNT"
        with patch("kiro.account_manager.random.random", return_value=0.0):  # would force a probabilistic retry
            picked = await _pick_ids(manager, 30)
        assert "acct-0" not in picked
        assert set(picked) == {"acct-1", "acct-2"}

    @pytest.mark.asyncio
    async def test_quota_reason_without_402_also_removes(self, tmp_path):
        manager = _manager(tmp_path)
        await manager.report_failure("acct-1", MODEL, ErrorType.RECOVERABLE, 400, "MONTHLY_REQUEST_COUNT")
        assert manager._accounts["acct-1"].pool_state == POOL_EXHAUSTED

    @pytest.mark.asyncio
    async def test_403_marks_credentials_invalid(self, tmp_path):
        manager = _manager(tmp_path)
        await manager.report_failure("acct-2", MODEL, ErrorType.RECOVERABLE, 403, None)
        account = manager._accounts["acct-2"]
        assert account.pool_state == POOL_INVALID
        assert account.pool_reason == "HTTP 403"

    @pytest.mark.asyncio
    async def test_failed_initialization_marks_invalid(self, tmp_path):
        manager = _manager(tmp_path, 2)
        manager._accounts["acct-0"].auth_manager = None
        manager._initialize_account = AsyncMock(return_value=False)
        account = await manager.get_next_account(MODEL)
        assert account.id == "acct-1"
        assert manager._accounts["acct-0"].pool_state == POOL_INVALID

    @pytest.mark.asyncio
    async def test_429_and_5xx_do_not_remove(self, tmp_path):
        manager = _manager(tmp_path)
        await manager.report_failure("acct-0", MODEL, ErrorType.RECOVERABLE, 429, "USER_REQUEST_RATE_EXCEEDED")
        await manager.report_failure("acct-1", MODEL, ErrorType.FATAL, 500, None)
        assert manager._accounts["acct-0"].pool_state == POOL_ACTIVE
        assert manager._accounts["acct-1"].pool_state == POOL_ACTIVE

    @pytest.mark.asyncio
    async def test_invalid_model_does_not_remove(self, tmp_path):
        manager = _manager(tmp_path)
        await manager.report_failure("acct-0", MODEL, ErrorType.RECOVERABLE, 400, "INVALID_MODEL_ID")
        assert manager._accounts["acct-0"].pool_state == POOL_ACTIVE

    @pytest.mark.asyncio
    async def test_all_removed_returns_none_and_explains(self, tmp_path):
        manager = _manager(tmp_path, 2)
        for account_id in ("acct-0", "acct-1"):
            await manager.report_failure(account_id, MODEL, ErrorType.RECOVERABLE, 402, "MONTHLY_REQUEST_COUNT")
        assert await manager.get_next_account(MODEL) is None
        assert manager.all_accounts_out_of_pool()
        message = manager.pool_empty_message()
        assert "MONTHLY_REQUEST_COUNT" in message
        assert "reset" in message


class TestPoolReturn:
    @pytest.mark.asyncio
    async def test_removed_account_returns_after_ttl(self, tmp_path):
        manager = _manager(tmp_path, 2)
        await manager.report_failure("acct-0", MODEL, ErrorType.RECOVERABLE, 402, "MONTHLY_REQUEST_COUNT")
        manager._accounts["acct-0"].pool_until = time.time() - 1  # TTL elapsed

        picked = await _pick_ids(manager, 4)

        assert "acct-0" in picked
        assert manager._accounts["acct-0"].pool_state == POOL_ACTIVE

    @pytest.mark.asyncio
    async def test_success_restores_account(self, tmp_path):
        manager = _manager(tmp_path)
        await manager.report_failure("acct-0", MODEL, ErrorType.RECOVERABLE, 403, None)
        await manager.report_success("acct-0", MODEL)
        assert manager._accounts["acct-0"].pool_state == POOL_ACTIVE
        assert manager._accounts["acct-0"].pool_reason == ""


class TestPoolReset:
    @pytest.mark.asyncio
    async def test_reset_clears_every_kind_of_failure(self, tmp_path):
        manager = _manager(tmp_path, 4)
        await manager.report_failure("acct-0", MODEL, ErrorType.RECOVERABLE, 402, "MONTHLY_REQUEST_COUNT")
        await manager.report_failure("acct-1", MODEL, ErrorType.RECOVERABLE, 403, None)
        manager._accounts["acct-2"].failures = 5
        manager._accounts["acct-2"].last_failure_time = time.time()
        manager._accounts["acct-3"].cooldown_until = time.time() + 300
        manager._accounts["acct-3"].stats.total_requests = 42

        summary = await manager.reset_pool()

        assert summary == {"accounts": 4, "exhausted": 1, "invalid": 1, "broken": 1, "cooldown": 1}
        for account in manager._accounts.values():
            assert account.pool_state == POOL_ACTIVE
            assert account.failures == 0
            assert account.cooldown_until == 0.0
        assert manager._accounts["acct-3"].stats.total_requests == 42  # stats are kept
        assert sorted(set(await _pick_ids(manager, 8))) == ["acct-0", "acct-1", "acct-2", "acct-3"]

    @pytest.mark.asyncio
    async def test_reset_persists_state(self, tmp_path):
        manager = _manager(tmp_path, 1)
        await manager.report_failure("acct-0", MODEL, ErrorType.RECOVERABLE, 402, "MONTHLY_REQUEST_COUNT")
        await manager.reset_pool()
        saved = json.loads((tmp_path / "state.json").read_text())
        assert saved["accounts"]["acct-0"]["pool_state"] == POOL_ACTIVE


class TestPoolPersistenceAndStatus:
    @pytest.mark.asyncio
    async def test_removal_survives_restart(self, tmp_path):
        manager = _manager(tmp_path, 2)
        await manager.report_failure("acct-0", MODEL, ErrorType.RECOVERABLE, 402, "MONTHLY_REQUEST_COUNT")
        await manager._save_state()

        restarted = _manager(tmp_path, 2)
        await restarted.load_state()

        account = restarted._accounts["acct-0"]
        assert account.pool_state == POOL_EXHAUSTED
        assert account.pool_reason == "MONTHLY_REQUEST_COUNT"
        assert account.pool_until > time.time()

    @pytest.mark.asyncio
    async def test_old_state_file_without_pool_fields_loads(self, tmp_path):
        (tmp_path / "state.json").write_text(json.dumps(
            {"current_account_index": 0, "accounts": {"acct-0": {"failures": 2}}, "model_to_accounts": {}}
        ))
        manager = _manager(tmp_path, 1)
        await manager.load_state()
        assert manager._accounts["acct-0"].pool_state == POOL_ACTIVE
        assert manager._accounts["acct-0"].failures == 2

    @pytest.mark.asyncio
    async def test_status_reports_states_without_secrets(self, tmp_path):
        manager = _manager(tmp_path, 4)
        await manager.report_failure("acct-0", MODEL, ErrorType.RECOVERABLE, 402, "MONTHLY_REQUEST_COUNT")
        manager._accounts["acct-1"].cooldown_until = time.time() + 30
        manager._accounts["acct-2"].failures = 1

        rows = {row["id"]: row for row in manager.pool_status()}

        assert rows["acct-0"]["state"] == POOL_EXHAUSTED
        assert rows["acct-0"]["returns_in_seconds"] > 0
        assert rows["acct-1"]["state"] == "cooldown"
        assert rows["acct-2"]["state"] == "degraded"
        assert rows["acct-3"]["state"] == POOL_ACTIVE
        assert "token" not in json.dumps(manager.pool_status()).lower()


class TestAdminEndpoints:
    @pytest.fixture
    def admin_client(self, clean_app, tmp_path):
        from fastapi.testclient import TestClient

        with TestClient(clean_app) as client:
            saved = clean_app.state.account_manager
            manager = _manager(tmp_path, 2)
            clean_app.state.account_manager = manager
            try:
                yield client, manager
            finally:
                clean_app.state.account_manager = saved

    def test_requires_api_key(self, admin_client):
        client, _ = admin_client
        assert client.get("/admin/accounts").status_code == 401
        assert client.post("/admin/accounts/reset", headers={"Authorization": "Bearer wrong"}).status_code == 401

    def test_list_and_reset(self, admin_client, valid_proxy_api_key):
        client, manager = admin_client
        headers = {"Authorization": f"Bearer {valid_proxy_api_key}"}
        manager._accounts["acct-0"].pool_state = POOL_EXHAUSTED
        manager._accounts["acct-0"].pool_reason = "MONTHLY_REQUEST_COUNT"
        manager._accounts["acct-0"].pool_until = time.time() + 3600

        listed = client.get("/admin/accounts", headers=headers).json()
        assert listed["total"] == 2 and listed["active"] == 1

        reset = client.post("/admin/accounts/reset", headers=headers).json()
        assert reset["reset"]["exhausted"] == 1
        assert all(row["state"] == POOL_ACTIVE for row in reset["accounts"])

    def test_route_explains_empty_pool(self, admin_client, valid_proxy_api_key):
        """
        What it does: every account is out of quota and a chat request arrives.
        Purpose: The client gets a 503 that says why and how to fix it.
        """
        client, manager = admin_client
        clean_state = client.app.state.account_system
        client.app.state.account_system = True
        try:
            for account in manager._accounts.values():
                account.pool_state = POOL_EXHAUSTED
                account.pool_reason = "MONTHLY_REQUEST_COUNT"
                account.pool_until = time.time() + 3600
            response = client.post(
                "/v1/chat/completions",
                headers={"Authorization": f"Bearer {valid_proxy_api_key}"},
                json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
            )
        finally:
            client.app.state.account_system = clean_state
        assert response.status_code == 503
        assert "removed from the pool" in response.text
        assert "MONTHLY_REQUEST_COUNT" in response.text
