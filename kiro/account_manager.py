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
Unified Account System for Kiro Gateway.

Manages multiple Kiro accounts with intelligent failover, sticky behavior,
and circuit breaker pattern for reliability.

Key features:
- Lazy initialization (only first working account at startup)
- Sticky behavior (prefer successful account)
- Circuit breaker with exponential backoff
- Probabilistic retry for "dead" accounts
- TTL-based model cache refresh (only when using account)
- Atomic state persistence
"""

import asyncio
import hashlib
import json
import math
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx
from loguru import logger

from kiro.auth import KiroAuthManager, AuthType
from kiro.cache import ModelInfoCache
from kiro.model_resolver import ModelResolver, normalize_model_name
from kiro.config import (
    HIDDEN_MODELS,
    MODEL_ALIASES,
    HIDDEN_FROM_LIST,
    ACCOUNT_RECOVERY_TIMEOUT,
    ACCOUNT_MAX_BACKOFF_MULTIPLIER,
    ACCOUNT_PROBABILISTIC_RETRY_CHANCE,
    ACCOUNT_CACHE_TTL,
    ACCOUNT_SELECTION,
    ACCOUNT_RATE_LIMIT_COOLDOWN,
    ACCOUNT_MAX_CONCURRENCY,
    ACCOUNT_QUEUE_TIMEOUT,
    ACCOUNT_QUOTA_EXHAUSTED_TTL,
    ACCOUNT_INVALID_TTL,
    STATE_SAVE_INTERVAL_SECONDS,
    FALLBACK_MODELS,
)
from kiro.utils import get_kiro_headers
from kiro.account_errors import ErrorType
from kiro.http_client import KiroHttpClient


def _is_runtime_endpoint(auth_manager: KiroAuthManager) -> bool:
    """
    Check if auth manager uses runtime endpoint that doesn't provide /ListAvailableModels.
    
    Runtime endpoint pattern: https://runtime.{region}.kiro.dev
    Old endpoint pattern: https://q.{region}.amazonaws.com
    
    Runtime endpoint does not provide /ListAvailableModels API (AWS limitation).
    
    Args:
        auth_manager: KiroAuthManager instance
    
    Returns:
        True if using runtime endpoint, False otherwise
    
    Examples:
        >>> auth_manager.api_host = "https://runtime.us-east-1.kiro.dev"
        >>> _is_runtime_endpoint(auth_manager)
        True
        >>> auth_manager.api_host = "https://runtime.eu-central-1.kiro.dev"
        >>> _is_runtime_endpoint(auth_manager)
        True
        >>> auth_manager.api_host = "https://q.us-east-1.amazonaws.com"
        >>> _is_runtime_endpoint(auth_manager)
        False
    """
    return "://runtime." in auth_manager.api_host


def _format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
    
    Returns:
        Formatted string (e.g., "30s", "5m", "2h", "1d")
    
    Examples:
        >>> _format_duration(30)
        '30s'
        >>> _format_duration(300)
        '5m'
        >>> _format_duration(7200)
        '2h'
        >>> _format_duration(86400)
        '1d'
    """
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds / 60)}m"
    elif seconds < 86400:
        return f"{int(seconds / 3600)}h"
    else:
        return f"{int(seconds / 86400)}d"


POOL_ACTIVE = "active"
"""Account is in rotation."""

POOL_EXHAUSTED = "exhausted"
"""Account ran out of quota (HTTP 402); out of rotation until pool_until."""

POOL_INVALID = "invalid"
"""Account credentials do not work; out of rotation until pool_until."""

QUOTA_REASONS = frozenset({"MONTHLY_REQUEST_COUNT", "QUOTA_EXCEEDED", "INSUFFICIENT_QUOTA", "OVERAGE_REQUEST_LIMIT"})
"""Kiro error reasons that mean the account has no quota left."""


@dataclass
class AccountStats:
    """
    Statistics for account usage.
    
    Tracks request counts for monitoring and future web UI.
    """
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0


@dataclass
class Account:
    """
    Complete account entity with all dependencies.
    
    Represents a single Kiro account with its authentication,
    model cache, resolver, and runtime state.
    
    Attributes:
        id: Unique identifier (path to credentials file)
        auth_manager: Authentication manager (lazy initialized)
        model_cache: Model metadata cache (lazy initialized)
        model_resolver: Model resolver (lazy initialized)
        failures: Consecutive failure count (for Circuit Breaker)
        last_failure_time: Timestamp of last failure
        models_cached_at: Timestamp of last model cache update
        cooldown_until: Rate-limit (429) cooldown end timestamp (round_robin, not persisted)
        in_flight: Requests currently holding a slot on this account (not persisted)
        pool_state: POOL_ACTIVE, POOL_EXHAUSTED or POOL_INVALID (persisted)
        pool_reason: Why the account left the pool (persisted)
        pool_until: Epoch seconds when a removed account returns to the pool (persisted)
        stats: Usage statistics
    """
    id: str
    auth_manager: Optional[KiroAuthManager] = None
    model_cache: Optional[ModelInfoCache] = None
    model_resolver: Optional[ModelResolver] = None
    failures: int = 0
    last_failure_time: float = 0.0
    models_cached_at: float = 0.0
    cooldown_until: float = 0.0
    in_flight: int = 0
    pool_state: str = POOL_ACTIVE
    pool_reason: str = ""
    pool_until: float = 0.0
    stats: AccountStats = field(default_factory=AccountStats)


@dataclass
class ModelAccountList:
    """
    List of accounts for a specific model.
    
    Attributes:
        accounts: List of account IDs that have this model
    
    Note: next_index removed - now using global _current_account_index
    """
    accounts: List[str] = field(default_factory=list)


class AccountLease:
    """
    A request's claim on one concurrency slot of an account.
    
    ``AccountManager.get_next_account`` reserves the slot; the caller owns
    this lease and must release it when the upstream request is finished
    (for streams: when the stream ends). ``release`` is idempotent, and a
    lease that is garbage-collected unreleased (e.g. a StreamingResponse
    whose body was never iterated because the client went away) releases
    itself so slots cannot leak.
    
    Example:
        >>> account = await manager.get_next_account(model)
        >>> lease = manager.lease(account.id)
        >>> try:
        ...     ...  # call Kiro
        ... finally:
        ...     lease.release()
    """
    
    def __init__(self, manager: "AccountManager", account_id: str) -> None:
        """
        Args:
            manager: Manager that reserved the slot
            account_id: Account whose slot this lease holds
        """
        self._manager = manager
        self.account_id = account_id
        self._released = False
    
    @property
    def released(self) -> bool:
        """True once the slot has been returned (or handed over)."""
        return self._released
    
    def release(self) -> None:
        """Returns the slot to the account (no-op if already released)."""
        if self._released:
            return
        self._released = True
        self._manager._release_slot(self.account_id)
    
    def transfer(self) -> "AccountLease":
        """
        Hands the slot over to a new lease (e.g. to a streaming generator).
        
        After this call releasing the old lease does nothing; the returned
        lease owns the slot.
        
        Returns:
            New lease owning the slot
        """
        if self._released:
            # Nothing to hand over; return an already-released lease
            successor = AccountLease(self._manager, self.account_id)
            successor._released = True
            return successor
        self._released = True
        return AccountLease(self._manager, self.account_id)
    
    def __del__(self) -> None:
        """Safety net: never leak a slot."""
        if not self._released:
            try:
                self.release()
            except Exception:  # noqa: BLE001 - never raise from a finalizer
                pass


class AccountManager:
    """
    Manages multiple Kiro accounts with intelligent failover.
    
    Responsibilities:
    - Load credentials from credentials.json
    - Lazy initialization of accounts
    - Select next available account (Circuit Breaker + Sticky)
    - Track statistics and failures
    - Persist state to state.json
    
    Example:
        >>> manager = AccountManager("credentials.json", "state.json")
        >>> await manager.load_credentials()
        >>> await manager.load_state()
        >>> account = await manager.get_next_account("claude-opus-4.5")
        >>> await manager.report_success(account.id, "claude-opus-4.5")
    """
    
    def __init__(self, credentials_file: str, state_file: str):
        """
        Initialize AccountManager.
        
        Args:
            credentials_file: Path to credentials.json
            state_file: Path to state.json
        """
        self._credentials_file = credentials_file
        self._state_file = state_file
        self._accounts: Dict[str, Account] = {}
        self._model_to_accounts: Dict[str, ModelAccountList] = {}
        self._lock = asyncio.Lock()
        self._dirty = False
        self._credentials_config: List[Dict] = []
        self._current_account_index: int = 0  # GLOBAL sticky index for all models
        self._selection: str = ACCOUNT_SELECTION
        self._rr_cursor: int = 0  # Next start index for round_robin
        # Per-account cap on concurrent requests (0 = unlimited)
        self._max_concurrency: int = ACCOUNT_MAX_CONCURRENCY
        self._queue_timeout: float = ACCOUNT_QUEUE_TIMEOUT
        self._slot_released = asyncio.Event()
        # Serializes (re)initialization per account without blocking the global lock
        self._init_locks: Dict[str, asyncio.Lock] = {}
    
    async def load_credentials(self) -> None:
        """
        Load credentials from credentials.json.
        
        Validates each entry and creates Account objects.
        Invalid entries are skipped with warnings.
        Folders are scanned for credential files.
        """
        creds_path = Path(self._credentials_file).expanduser()
        
        if not creds_path.exists():
            logger.warning(f"Credentials file not found: {self._credentials_file}")
            return
        
        try:
            with open(creds_path, 'r', encoding='utf-8') as f:
                self._credentials_config = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            return
        
        # Process each credential entry
        for entry in self._credentials_config:
            cred_type = entry.get("type")
            path = entry.get("path")
            enabled = entry.get("enabled", True)
            
            if not enabled:
                continue
            
            # Validate required fields based on type
            if not cred_type:
                logger.warning(f"Invalid credential entry (missing type): {entry}")
                continue
            
            # For json/sqlite types, path is required
            if cred_type in ("json", "sqlite") and not path:
                logger.warning(f"Invalid credential entry (type={cred_type} requires path): {entry}")
                continue
            
            # For refresh_token type, refresh_token field is required
            if cred_type == "refresh_token" and not entry.get("refresh_token"):
                logger.warning(f"Invalid credential entry (type=refresh_token requires refresh_token field): {entry}")
                continue
            
            # Handle refresh_token type (no path processing needed)
            if cred_type == "refresh_token":
                # Use deterministic hash for refresh_token (hash() is not deterministic between process restarts)
                token = entry.get('refresh_token', '')
                token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
                account_id = f"refresh_token_{token_hash}"
                self._accounts[account_id] = Account(id=account_id)
                logger.debug(f"Added account: {account_id}")
                continue  # Skip path processing for refresh_token
            
            # Handle folder scanning for json/sqlite types
            expanded_path = Path(path).expanduser()
            if expanded_path.is_dir():
                logger.info(f"Scanning folder for credentials: {path}")
                for file_path in sorted(expanded_path.iterdir()):
                    if not file_path.is_file():
                        continue
                    
                    # Validate file before adding as account
                    account_id = str(file_path.resolve())
                    is_valid = False
                    
                    # Try JSON validation
                    if cred_type == "json":
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                                # Valid if has refreshToken or clientId
                                if 'refreshToken' in data or 'clientId' in data:
                                    is_valid = True
                        except Exception as e:
                            logger.warning(f"Invalid JSON credentials file {file_path.name}: {e}")
                    
                    # Try SQLite validation
                    elif cred_type == "sqlite":
                        try:
                            import sqlite3
                            conn = sqlite3.connect(str(file_path))
                            cursor = conn.cursor()
                            # Check if auth_kv table exists
                            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='auth_kv'")
                            if cursor.fetchone():
                                is_valid = True
                            conn.close()
                        except Exception as e:
                            logger.warning(f"Invalid SQLite database file {file_path.name}: {e}")
                    
                    if is_valid:
                        self._accounts[account_id] = Account(id=account_id)
                        logger.debug(f"Added account from folder: {account_id}")
                    else:
                        logger.warning(f"Skipping invalid credentials file: {file_path.name}")
            elif expanded_path.is_file() or cred_type == "refresh_token":
                # Single file or refresh_token type
                if cred_type == "refresh_token":
                    # Use deterministic hash for refresh_token (hash() is not deterministic between process restarts)
                    token = entry.get('refresh_token', '')
                    token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
                    account_id = f"refresh_token_{token_hash}"
                else:
                    account_id = str(expanded_path.resolve())
                self._accounts[account_id] = Account(id=account_id)
                logger.debug(f"Added account: {account_id}")
            else:
                logger.warning(f"Credential path not found: {path}")
        
        logger.info(f"Loaded {len(self._accounts)} account(s) from credentials")
    
    async def load_state(self) -> None:
        """
        Load runtime state from state.json.
        
        Restores model_to_accounts mapping and account runtime state.
        Creates empty state if file doesn't exist.
        """
        state_path = Path(self._state_file)
        
        if not state_path.exists():
            logger.debug("State file not found, starting with empty state")
            return
        
        try:
            with open(state_path, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
            # Restore global current_account_index
            self._current_account_index = state_data.get("current_account_index", 0)
            
            # Restore model_to_accounts mapping (without next_index)
            for model, data in state_data.get("model_to_accounts", {}).items():
                self._model_to_accounts[model] = ModelAccountList(
                    accounts=data.get("accounts", [])
                )
            
            # Restore account runtime state
            for account_id, data in state_data.get("accounts", {}).items():
                if account_id in self._accounts:
                    account = self._accounts[account_id]
                    account.failures = data.get("failures", 0)
                    account.last_failure_time = data.get("last_failure_time", 0.0)
                    account.models_cached_at = data.get("models_cached_at", 0.0)
                    account.pool_state = data.get("pool_state", POOL_ACTIVE)
                    account.pool_reason = data.get("pool_reason", "")
                    account.pool_until = data.get("pool_until", 0.0)
                    
                    stats_data = data.get("stats", {})
                    account.stats = AccountStats(
                        total_requests=stats_data.get("total_requests", 0),
                        successful_requests=stats_data.get("successful_requests", 0),
                        failed_requests=stats_data.get("failed_requests", 0)
                    )
            
            logger.info(f"Loaded state: {len(self._model_to_accounts)} model mappings, {len(self._accounts)} accounts")
        
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
    
    async def _save_state(self) -> None:
        """
        Save runtime state to state.json atomically.
        
        Uses tmp file + rename for atomic write.
        """
        state_data = {
            "current_account_index": self._current_account_index,
            "accounts": {
                account_id: {
                    "failures": account.failures,
                    "last_failure_time": account.last_failure_time,
                    "models_cached_at": account.models_cached_at,
                    "pool_state": account.pool_state,
                    "pool_reason": account.pool_reason,
                    "pool_until": account.pool_until,
                    "stats": {
                        "total_requests": account.stats.total_requests,
                        "successful_requests": account.stats.successful_requests,
                        "failed_requests": account.stats.failed_requests
                    }
                }
                for account_id, account in self._accounts.items()
            },
            "model_to_accounts": {
                model: {
                    "accounts": mal.accounts
                }
                for model, mal in self._model_to_accounts.items()
            }
        }
        
        state_path = Path(self._state_file)
        tmp_path = state_path.with_suffix('.json.tmp')
        
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=2, ensure_ascii=False)
            
            # Atomic rename
            tmp_path.replace(state_path)
            logger.debug("State saved successfully")
        
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            if tmp_path.exists():
                tmp_path.unlink()
    
    async def save_state_periodically(self) -> None:
        """
        Background task for periodic state saving.
        
        Saves state every STATE_SAVE_INTERVAL_SECONDS if dirty flag is set.
        """
        while True:
            await asyncio.sleep(STATE_SAVE_INTERVAL_SECONDS)
            
            if self._dirty:
                async with self._lock:
                    await self._save_state()
                    self._dirty = False
    
    async def _initialize_account(self, account_id: str) -> bool:
        """
        Initialize account (lazy initialization).
        
        Creates auth_manager, fetches models, creates cache and resolver.
        
        Args:
            account_id: Account ID to initialize
        
        Returns:
            True if successful, False otherwise
        """
        account = self._accounts.get(account_id)
        if not account:
            return False
        
        try:
            # Find credentials config for this account
            creds_config = None
            for entry in self._credentials_config:
                path = entry.get("path", "")
                expanded_path = Path(path).expanduser()
                
                if entry.get("type") == "refresh_token":
                    # Match by deterministic hash for refresh_token type
                    token = entry.get('refresh_token', '')
                    token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
                    if account_id == f"refresh_token_{token_hash}":
                        creds_config = entry
                        break
                elif str(expanded_path.resolve()) == account_id or (expanded_path.is_dir() and account_id.startswith(str(expanded_path.resolve()) + os.sep)):
                    creds_config = entry
                    break
            
            if not creds_config:
                logger.error(f"No credentials config found for account: {account_id}")
                return False
            
            # Create KiroAuthManager based on type
            cred_type = creds_config.get("type")
            if cred_type == "json":
                auth_manager = KiroAuthManager(
                    creds_file=account_id,
                    profile_arn=creds_config.get("profile_arn"),
                    region=creds_config.get("region", "us-east-1"),
                    api_region=creds_config.get("api_region")
                )
            elif cred_type == "sqlite":
                auth_manager = KiroAuthManager(
                    sqlite_db=account_id,
                    profile_arn=creds_config.get("profile_arn"),
                    region=creds_config.get("region", "us-east-1"),
                    api_region=creds_config.get("api_region")
                )
            elif cred_type == "refresh_token":
                auth_manager = KiroAuthManager(
                    refresh_token=creds_config.get("refresh_token"),
                    profile_arn=creds_config.get("profile_arn"),
                    region=creds_config.get("region", "us-east-1"),
                    api_region=creds_config.get("api_region")
                )
            else:
                logger.error(f"Unknown credential type: {cred_type}")
                return False
            
            # Get token to verify credentials
            token = await auth_manager.get_access_token()
            
            # Determine if we should fetch models or use static list
            if _is_runtime_endpoint(auth_manager):
                # New runtime endpoint does not provide /ListAvailableModels (AWS limitation)
                # Use static list without attempting request
                logger.debug(f"Account {account_id}: Using static model list for runtime.kiro.dev endpoint")
                models_list = FALLBACK_MODELS
            else:
                # Old endpoint - attempt to fetch dynamic model list
                # Fetch models list with retry + fallback
                params = {"origin": "AI_EDITOR"}
                if auth_manager.auth_type == AuthType.KIRO_DESKTOP and auth_manager.profile_arn:
                    params["profileArn"] = auth_manager.profile_arn
                
                list_models_url = f"{auth_manager.q_host}/ListAvailableModels"
                
                # Use KiroHttpClient for retry logic (3 attempts with exponential backoff)
                http_client = KiroHttpClient(auth_manager, shared_client=None)
                
                try:
                    response = await http_client.request_with_retry(
                        method="GET",
                        url=list_models_url,
                        json_data=None,
                        params=params,
                        stream=False
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        models_list = data.get("models", [])
                    else:
                        # Shouldn't happen (retry handles non-200), but keep for safety
                        raise Exception(f"HTTP {response.status_code}")
                
                except Exception as e:
                    # All retries exhausted - use fallback
                    logger.error(f"Failed to fetch models for {account_id} after retries: {e}")
                    logger.warning("Using pre-configured fallback models. Models will be refreshed on next TTL cycle when network recovers.")
                    models_list = FALLBACK_MODELS
                
                finally:
                    await http_client.close()
            
            # Create model cache and update
            model_cache = ModelInfoCache()
            await model_cache.update(models_list)
            
            # Add hidden models
            for display_name, internal_id in HIDDEN_MODELS.items():
                model_cache.add_hidden_model(display_name, internal_id)
            
            # Create model resolver
            model_resolver = ModelResolver(
                cache=model_cache,
                hidden_models=HIDDEN_MODELS,
                aliases=MODEL_ALIASES,
                hidden_from_list=HIDDEN_FROM_LIST
            )
            
            # Update account
            account.auth_manager = auth_manager
            account.model_cache = model_cache
            account.model_resolver = model_resolver
            account.models_cached_at = time.time()
            
            # Update model_to_accounts mapping
            available_models = model_resolver.get_available_models()
            for model in available_models:
                if model not in self._model_to_accounts:
                    self._model_to_accounts[model] = ModelAccountList()
                if account_id not in self._model_to_accounts[model].accounts:
                    self._model_to_accounts[model].accounts.append(account_id)
            
            logger.info(f"Initialized account: {account_id} ({len(available_models)} models)")
            self._dirty = True
            return True
        
        except Exception as e:
            logger.error(f"Failed to initialize account {account_id}: {e}")
            return False
    
    async def _refresh_account_models(self, account_id: str) -> None:
        """
        Refresh model cache for account (TTL refresh).
        
        Args:
            account_id: Account ID to refresh
        """
        account = self._accounts.get(account_id)
        if not account or not account.auth_manager:
            return
        
        # Check if using runtime endpoint (no dynamic model list available)
        if _is_runtime_endpoint(account.auth_manager):
            # Runtime endpoint does not provide /ListAvailableModels
            # Use static list and update cache timestamp
            logger.debug(f"Account {account_id}: Skipping model refresh for runtime.kiro.dev endpoint (using static list)")
            await account.model_cache.update(FALLBACK_MODELS)
            account.models_cached_at = time.time()
            self._dirty = True
            return
        
        # Old endpoint - attempt to fetch dynamic model list
        # Use KiroHttpClient for retry logic
        http_client = KiroHttpClient(account.auth_manager, shared_client=None)
        
        try:
            params = {"origin": "AI_EDITOR"}
            if account.auth_manager.auth_type == AuthType.KIRO_DESKTOP and account.auth_manager.profile_arn:
                params["profileArn"] = account.auth_manager.profile_arn
            
            list_models_url = f"{account.auth_manager.q_host}/ListAvailableModels"
            
            response = await http_client.request_with_retry(
                method="GET",
                url=list_models_url,
                json_data=None,
                params=params,
                stream=False
            )
            
            if response.status_code == 200:
                data = response.json()
                models_list = data.get("models", [])
                await account.model_cache.update(models_list)
                account.models_cached_at = time.time()
                
                # Update model_to_accounts mapping (new models may have appeared)
                available_models = account.model_resolver.get_available_models()
                for model in available_models:
                    if model not in self._model_to_accounts:
                        self._model_to_accounts[model] = ModelAccountList()
                    if account_id not in self._model_to_accounts[model].accounts:
                        self._model_to_accounts[model].accounts.append(account_id)
                
                logger.debug(f"Refreshed models for {account_id}")
                self._dirty = True
        
        except Exception as e:
            # All retries exhausted - keep using stale cache
            logger.warning(f"Failed to refresh models for {account_id} after retries: {e}")
        
        finally:
            await http_client.close()
    
    async def get_next_account(self, model: str, exclude_accounts: Optional[set] = None) -> Optional[Account]:
        """
        Get next available account for model (Circuit Breaker + Sticky/Round Robin).
        
        Implements:
        - Sticky or round_robin selection (ACCOUNT_SELECTION)
        - Circuit Breaker with exponential backoff
        - Probabilistic retry for "dead" accounts (10%)
        - Rate-limit (429) cooldown skipping
        - Per-account concurrency cap with queueing (ACCOUNT_MAX_CONCURRENCY)
        - TTL-based model cache refresh
        - Exclusion of already-tried accounts in current failover loop
        
        The returned account has one concurrency slot reserved for the caller.
        Wrap it with ``lease(account.id)`` and release the lease when the
        upstream request finishes.
        
        Selection runs under the global lock, but account initialization and
        model refresh (network calls) do not, so one slow or dead account can
        no longer stall every concurrent request.
        
        Args:
            model: Model name (will be normalized)
            exclude_accounts: Set of account IDs to exclude (already tried in current failover loop)
        
        Returns:
            Account object or None if no accounts available
        """
        excluded = set(exclude_accounts or ())
        # Round robin advances once per new request, not per failover/retry pick
        advance_cursor = not exclude_accounts
        deadline: Optional[float] = None
        
        while True:
            async with self._lock:
                account, saturated = self._select_account(excluded, advance_cursor)
                advance_cursor = False
                if account is not None:
                    account.in_flight += 1
                elif saturated:
                    # Armed under the lock: a release after this point wakes us up
                    self._slot_released.clear()
            
            if account is None:
                if not saturated:
                    return None
                if deadline is None:
                    deadline = time.monotonic() + self._queue_timeout
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning(
                        f"All accounts busy (ACCOUNT_MAX_CONCURRENCY={self._max_concurrency}) "
                        f"for {self._queue_timeout:.0f}s, giving up"
                    )
                    return None
                logger.debug(f"All accounts at concurrency cap, waiting up to {remaining:.1f}s for a free slot")
                try:
                    await asyncio.wait_for(self._slot_released.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass
                continue
            
            if account.auth_manager is None:
                initialized = await self._initialize_account_once(account.id)
                if not initialized:
                    self._release_slot(account.id)
                    if len(self._accounts) == 1:
                        return None
                    async with self._lock:
                        account.failures += 1
                        account.last_failure_time = time.time()
                        self._remove_from_pool(account, POOL_INVALID, "initialization failed", ACCOUNT_INVALID_TTL)
                        self._dirty = True
                    excluded.add(account.id)
                    continue
            
            # Check TTL and refresh if needed
            if account.models_cached_at > 0:
                age = time.time() - account.models_cached_at
                if age > ACCOUNT_CACHE_TTL:
                    try:
                        await self._refresh_account_models(account.id)
                    except Exception as e:
                        logger.warning(f"Failed to refresh models for {account.id}: {e}")
            
            # No model validation - let Kiro API decide (gateway, not gatekeeper)
            return account
    
    def _select_account(self, excluded: set, advance_cursor: bool) -> Tuple[Optional[Account], bool]:
        """
        Picks the next eligible account. Pure in-memory; caller holds the lock.
        
        Args:
            excluded: Account IDs not to pick
            advance_cursor: Whether to move the round_robin cursor
        
        Returns:
            Tuple of (account or None, whether some otherwise eligible account
            was skipped only because it is at the concurrency cap)
        """
        # Special case: single account - bypass Circuit Breaker
        # Circuit Breaker is meaningless for single account - user should see real Kiro API errors
        # instead of generic "Account unavailable" after cooldown kicks in
        if len(self._accounts) == 1:
            account_id = list(self._accounts.keys())[0]
            account = self._accounts[account_id]
            if account_id in excluded:
                return None, False
            if self._at_capacity(account):
                return None, True
            return account, False
        
        # Multi-account logic
        all_account_ids = list(self._accounts.keys())
        
        if self._selection == "round_robin":
            # Each new request starts from the next account; failover calls reuse the cursor
            start_index = self._rr_cursor % len(all_account_ids)
            if advance_cursor:
                self._rr_cursor = (start_index + 1) % len(all_account_ids)
        else:
            # Start from GLOBAL index (one current account for ALL models)
            start_index = self._current_account_index
        
        saturated = False
        for i in range(len(all_account_ids)):
            current_index = (start_index + i) % len(all_account_ids)
            account_id = all_account_ids[current_index]
            account = self._accounts[account_id]
            
            # Skip accounts already tried in current failover loop
            if account_id in excluded:
                continue
            
            # Skip accounts in rate-limit (429) cooldown
            if account.cooldown_until > time.time():
                continue
            
            # Skip accounts removed from the pool (no quota / broken credentials)
            if not self._is_in_pool(account):
                continue
            
            # Check Circuit Breaker (Half-Open state with exponential backoff)
            if account.failures > 0:
                time_since_failure = time.time() - account.last_failure_time
                
                # Exponential backoff: base * 2^(failures - 1), capped at MAX_MULTIPLIER
                # 1 failure: 60s, 2: 120s, 3: 240s, ..., 12+: 86400s (1 day cap)
                backoff_multiplier = min(2 ** (account.failures - 1), ACCOUNT_MAX_BACKOFF_MULTIPLIER)
                effective_timeout = ACCOUNT_RECOVERY_TIMEOUT * backoff_multiplier
                
                if time_since_failure < effective_timeout:
                    # Probabilistic retry (10% chance)
                    if random.random() > ACCOUNT_PROBABILISTIC_RETRY_CHANCE:
                        continue
                    else:
                        logger.info(f"Probabilistic retry for broken account {account_id}")
                else:
                    # Half-Open: recovery timeout passed
                    logger.info(f"Half-Open state for {account_id} (recovery timeout passed, effective={effective_timeout}s)")
            
            if self._at_capacity(account):
                saturated = True
                continue
            
            # Account is suitable!
            return account, False
        
        # All accounts unavailable
        return None, saturated
    
    def _at_capacity(self, account: Account) -> bool:
        """
        Checks the per-account concurrency cap.
        
        Args:
            account: Account to check
        
        Returns:
            True if the cap is enabled and the account has no free slot
        """
        return self._max_concurrency > 0 and account.in_flight >= self._max_concurrency
    
    def _release_slot(self, account_id: str) -> None:
        """
        Returns one concurrency slot and wakes up queued requests.
        
        Synchronous on purpose so it can run from finalizers.
        
        Args:
            account_id: Account whose slot is released
        """
        account = self._accounts.get(account_id)
        if account is not None and account.in_flight > 0:
            account.in_flight -= 1
        self._slot_released.set()
    
    def _is_in_pool(self, account: Account) -> bool:
        """
        Checks pool membership, returning expired removals to the pool.
        
        Caller holds the lock.
        
        Args:
            account: Account to check
        
        Returns:
            True if the account may receive requests
        """
        if account.pool_state == POOL_ACTIVE:
            return True
        if account.pool_until and account.pool_until <= time.time():
            logger.info(
                f"Account {account.id} returns to pool (was {account.pool_state}: {account.pool_reason}); "
                f"it will be removed again if it still fails"
            )
            self._restore_to_pool(account)
            return True
        return False
    
    def _remove_from_pool(self, account: Account, state: str, reason: str, ttl: int) -> None:
        """
        Takes an account out of rotation for ``ttl`` seconds. Caller holds the lock.
        
        Args:
            account: Account to remove
            state: POOL_EXHAUSTED or POOL_INVALID
            reason: Human-readable reason (Kiro reason code)
            ttl: Seconds until it is automatically returned
        """
        already = account.pool_state == state
        account.pool_state = state
        account.pool_reason = reason
        account.pool_until = time.time() + ttl
        active = sum(1 for a in self._accounts.values() if a.pool_state == POOL_ACTIVE)
        if not already:
            logger.warning(
                f"Account {account.id} removed from pool: {state} ({reason}); "
                f"returns in {_format_duration(ttl)}. Active accounts left: {active}/{len(self._accounts)}"
            )
    
    def _restore_to_pool(self, account: Account) -> None:
        """
        Puts an account back into rotation. Caller holds the lock.
        
        Args:
            account: Account to restore
        """
        account.pool_state = POOL_ACTIVE
        account.pool_reason = ""
        account.pool_until = 0.0
        self._dirty = True
    
    async def reset_pool(self) -> Dict[str, int]:
        """
        Returns every account to the pool and clears failure state.
        
        Clears quota/invalid removals, circuit-breaker failures and 429
        cooldowns. Usage statistics are kept. Persists immediately.
        
        Returns:
            Counts: total accounts and how many were reset from each state
        """
        async with self._lock:
            summary = {"accounts": len(self._accounts), "exhausted": 0, "invalid": 0, "broken": 0, "cooldown": 0}
            now = time.time()
            for account in self._accounts.values():
                if account.pool_state == POOL_EXHAUSTED:
                    summary["exhausted"] += 1
                elif account.pool_state == POOL_INVALID:
                    summary["invalid"] += 1
                if account.failures > 0:
                    summary["broken"] += 1
                if account.cooldown_until > now:
                    summary["cooldown"] += 1
                self._restore_to_pool(account)
                account.failures = 0
                account.last_failure_time = 0.0
                account.cooldown_until = 0.0
            self._dirty = True
        await self._save_state()
        logger.info(f"Account pool reset: {summary}")
        return summary
    
    def pool_status(self) -> List[Dict[str, object]]:
        """
        Describes every account for monitoring (no secrets).
        
        Returns:
            One dict per account: id, state, reason, seconds until return,
            in-flight requests, failures and request statistics
        """
        now = time.time()
        rows: List[Dict[str, object]] = []
        for account in self._accounts.values():
            if account.pool_state != POOL_ACTIVE and not (account.pool_until and account.pool_until <= now):
                state = account.pool_state
            elif account.cooldown_until > now:
                state = "cooldown"
            elif account.failures > 0:
                state = "degraded"
            elif account.auth_manager is None:
                state = "not_initialized"
            else:
                state = POOL_ACTIVE
            returns_in = 0
            if state in (POOL_EXHAUSTED, POOL_INVALID):
                returns_in = max(0, int(account.pool_until - now))
            elif state == "cooldown":
                returns_in = max(0, int(account.cooldown_until - now))
            rows.append({
                "id": Path(account.id).name if os.sep in account.id else account.id,
                "state": state,
                "reason": account.pool_reason,
                "returns_in_seconds": returns_in,
                "in_flight": account.in_flight,
                "failures": account.failures,
                "total_requests": account.stats.total_requests,
                "successful_requests": account.stats.successful_requests,
                "failed_requests": account.stats.failed_requests,
            })
        return rows
    
    def all_accounts_busy(self) -> bool:
        """
        Checks whether every account is at its concurrency cap.
        
        Returns:
            True if the cap is enabled and no account has a free slot
        """
        if self._max_concurrency <= 0 or not self._accounts:
            return False
        return all(self._at_capacity(account) for account in self._accounts.values())
    
    def all_accounts_out_of_pool(self) -> bool:
        """
        Checks whether every account is currently removed from the pool.
        
        Returns:
            True if there is at least one account and none is in the pool
        """
        now = time.time()
        return bool(self._accounts) and all(
            a.pool_state != POOL_ACTIVE and a.pool_until > now for a in self._accounts.values()
        )
    
    def pool_empty_message(self) -> str:
        """
        User-facing explanation when no account is left in the pool.
        
        Returns:
            Actionable error message
        """
        reasons = sorted({f"{a.pool_state}: {a.pool_reason}" for a in self._accounts.values()})
        soonest = min(a.pool_until for a in self._accounts.values()) - time.time()
        return (
            f"No Kiro account is available: all {len(self._accounts)} account(s) were removed from the pool "
            f"({'; '.join(reasons)}). The first returns in {_format_duration(max(0, soonest))}. "
            f"Add accounts or reset the pool (scripts/kiro-accounts.sh reset / POST /admin/accounts/reset)."
        )
    
    def busy_message(self) -> str:
        """
        User-facing explanation for "all accounts busy".
        
        Returns:
            Actionable error message
        """
        return (
            f"All {len(self._accounts)} Kiro account(s) are busy "
            f"(ACCOUNT_MAX_CONCURRENCY={self._max_concurrency} concurrent requests each) and no slot "
            f"freed up within {self._queue_timeout:.0f}s. Retry later, add accounts, or raise "
            f"ACCOUNT_MAX_CONCURRENCY / ACCOUNT_QUEUE_TIMEOUT."
        )
    
    def lease(self, account_id: str) -> AccountLease:
        """
        Wraps the slot reserved by ``get_next_account`` in a lease.
        
        Args:
            account_id: Account returned by ``get_next_account``
        
        Returns:
            Lease that must be released when the request is done
        """
        return AccountLease(self, account_id)
    
    async def _initialize_account_once(self, account_id: str) -> bool:
        """
        Initializes an account, coalescing concurrent attempts.
        
        Runs outside the global lock; a per-account lock makes sure only one
        request performs the (network-bound) initialization while others for
        the same account wait for its result.
        
        Args:
            account_id: Account to initialize
        
        Returns:
            True if the account is initialized
        """
        init_lock = self._init_locks.setdefault(account_id, asyncio.Lock())
        async with init_lock:
            account = self._accounts.get(account_id)
            if account is None:
                return False
            if account.auth_manager is not None:
                return True
            return await self._initialize_account(account_id)
    
    async def report_success(self, account_id: str, model: str) -> None:
        """
        Report successful request (reset failures, update stats, sticky, dynamic learning).
        
        Args:
            account_id: Account ID
            model: Model name
        """
        async with self._lock:
            account = self._accounts.get(account_id)
            if not account:
                return
            
            # Reset failures
            if account.failures > 0:
                account.failures = 0
                self._dirty = True
            if account.pool_state != POOL_ACTIVE:
                logger.info(f"Account {account_id} is serving requests again, back in pool")
                self._restore_to_pool(account)
            
            # Update stats
            account.stats.total_requests += 1
            account.stats.successful_requests += 1
            self._dirty = True
            
            # Dynamic learning: add model to mapping if successful
            # This allows system to learn about new models not in FALLBACK_MODELS
            normalized_model = normalize_model_name(model)
            if normalized_model not in self._model_to_accounts:
                self._model_to_accounts[normalized_model] = ModelAccountList()
                logger.debug(f"Dynamic learning: discovered new model '{normalized_model}'")
            if account_id not in self._model_to_accounts[normalized_model].accounts:
                self._model_to_accounts[normalized_model].accounts.append(account_id)
                logger.debug(f"Dynamic learning: model '{normalized_model}' works on account {account_id}")
                self._dirty = True
            
            if self._selection != "sticky":
                return
            
            # GLOBAL STICKY: Update global current_account_index
            all_account_ids = list(self._accounts.keys())
            try:
                successful_index = all_account_ids.index(account_id)
                if self._current_account_index != successful_index:
                    self._current_account_index = successful_index
                    self._dirty = True
            except ValueError:
                pass
    
    async def report_failure(
        self,
        account_id: str,
        model: str,
        error_type: ErrorType,
        status_code: int,
        reason: Optional[str]
    ) -> None:
        """
        Report failed request (update failures, stats, failover).
        
        Args:
            account_id: Account ID
            model: Model name
            error_type: Error classification (FATAL or RECOVERABLE)
            status_code: HTTP status code
            reason: Error reason from Kiro API
        """
        async with self._lock:
            account = self._accounts.get(account_id)
            if not account:
                return
            
            # Special case: INVALID_MODEL_ID is discovery process, not account failure
            # Account is healthy, model is just not available on this account
            # Log for user visibility but don't penalize account statistics
            if reason == "INVALID_MODEL_ID":
                account.stats.total_requests += 1
                self._dirty = True
                logger.warning(
                    f"Model '{model}' not available on account {account_id}: "
                    f"status={status_code}, reason={reason}"
                )
                return
            
            # No quota left: take the account out of the pool instead of probing it
            if status_code == 402 or (reason or "").upper() in QUOTA_REASONS:
                account.stats.total_requests += 1
                account.stats.failed_requests += 1
                self._remove_from_pool(account, POOL_EXHAUSTED, reason or f"HTTP {status_code}", ACCOUNT_QUOTA_EXHAUSTED_TTL)
                self._dirty = True
                return
            
            # Token still rejected after a forced refresh: credentials are broken
            if status_code == 403:
                account.stats.total_requests += 1
                account.stats.failed_requests += 1
                self._remove_from_pool(account, POOL_INVALID, reason or "HTTP 403", ACCOUNT_INVALID_TTL)
                self._dirty = True
                return
            
            # round_robin with several accounts: 429 gets a short fixed cooldown, not the breaker
            if status_code == 429 and self._uses_rate_limit_cooldown():
                account.cooldown_until = time.time() + ACCOUNT_RATE_LIMIT_COOLDOWN
                account.stats.total_requests += 1
                account.stats.failed_requests += 1
                self._dirty = True
                logger.warning(
                    f"Account {account_id} rate-limited: reason={reason}, "
                    f"cooldown={_format_duration(ACCOUNT_RATE_LIMIT_COOLDOWN)}"
                )
                return
            
            # Update failure count (only for RECOVERABLE)
            if error_type == ErrorType.RECOVERABLE:
                account.failures += 1
                account.last_failure_time = time.time()
                self._dirty = True
                
                # Calculate backoff for logging
                backoff_multiplier = min(2 ** (account.failures - 1), ACCOUNT_MAX_BACKOFF_MULTIPLIER)
                effective_timeout = ACCOUNT_RECOVERY_TIMEOUT * backoff_multiplier
                logger.warning(
                    f"Account {account_id} failure #{account.failures}: "
                    f"status={status_code}, reason={reason}, "
                    f"cooldown={_format_duration(effective_timeout)}"
                )
            
            # Update stats
            account.stats.total_requests += 1
            account.stats.failed_requests += 1
            self._dirty = True
            
            # GLOBAL STICKY: Do NOT change _current_account_index on failure
            # It only changes on success (GLOBAL sticky behavior)
            # Failover happens through exclude_accounts in get_next_account()
    
    def _uses_rate_limit_cooldown(self) -> bool:
        """True when 429 should trigger cooldown + failover instead of same-account retries."""
        return self._selection == "round_robin" and len(self._accounts) > 1
    
    def retry_rate_limit_in_place(self) -> bool:
        """
        Whether request_with_retry should retry 429 on the same account.
        
        Returns:
            False in round_robin with 2+ accounts (fail over instead), True otherwise
        """
        return not self._uses_rate_limit_cooldown()
    
    def rate_limit_retry_after(self) -> Optional[int]:
        """
        Seconds until the first rate-limited account leaves its 429 cooldown.
        
        Used when no account is available, to answer 429 + Retry-After instead of 503.
        
        Returns:
            Seconds (>= 1) or None if no account is in 429 cooldown
        """
        now = time.time()
        remaining = [a.cooldown_until - now for a in self._accounts.values() if a.cooldown_until > now]
        if not remaining:
            return None
        return max(1, math.ceil(min(remaining)))
    
    def get_first_account(self) -> Account:
        """
        Get first initialized account (for legacy mode).
        
        Returns:
            First initialized account
        
        Raises:
            RuntimeError: If no initialized accounts available
        """
        for account in self._accounts.values():
            if account.auth_manager is not None:
                return account
        raise RuntimeError("No initialized accounts available")
    
    def get_all_available_models(self) -> List[str]:
        """
        Collect unique models from all initialized accounts.
        
        Used by /v1/models endpoint in account system to show
        all available models across all accounts.
        
        Returns:
            Sorted list of unique model IDs
        """
        all_models = set()
        for account in self._accounts.values():
            if account.model_resolver:
                all_models.update(account.model_resolver.get_available_models())
        return sorted(all_models)
