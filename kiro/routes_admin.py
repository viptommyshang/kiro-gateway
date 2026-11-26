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
Account pool administration endpoints.

Protected by the same PROXY_API_KEY as the API (Bearer token). They never
return credentials.

- ``GET  /admin/accounts``        pool overview (state per account)
- ``POST /admin/accounts/reset``  return every account to the pool
"""

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger

from kiro.routes_openai import verify_api_key

router = APIRouter()


def _account_manager(request: Request) -> Any:
    """
    Returns the AccountManager, or fails when it is not available.

    Args:
        request: Incoming request

    Returns:
        The application's AccountManager

    Raises:
        HTTPException: 503 if the account manager is not initialized
    """
    manager = getattr(request.app.state, "account_manager", None)
    if manager is None:
        raise HTTPException(status_code=503, detail="Account manager is not initialized")
    return manager


@router.get("/admin/accounts", dependencies=[Depends(verify_api_key)])
async def list_accounts(request: Request) -> Dict[str, Any]:
    """
    Returns the account pool overview.

    Args:
        request: Incoming request

    Returns:
        ``{"active": n, "total": n, "accounts": [...]}``
    """
    rows = _account_manager(request).pool_status()
    active = sum(1 for row in rows if row["state"] in ("active", "degraded", "not_initialized"))
    return {"active": active, "total": len(rows), "accounts": rows}


@router.post("/admin/accounts/reset", dependencies=[Depends(verify_api_key)])
async def reset_accounts(request: Request) -> Dict[str, Any]:
    """
    Returns every account to the pool (clears quota/invalid removals, failures and cooldowns).

    Args:
        request: Incoming request

    Returns:
        Reset summary plus the resulting pool overview
    """
    manager = _account_manager(request)
    summary = await manager.reset_pool()
    logger.info(f"Account pool reset via admin API: {summary}")
    rows = manager.pool_status()
    return {"reset": summary, "accounts": rows}
