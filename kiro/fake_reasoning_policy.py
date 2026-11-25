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
Per-model fallback for fake reasoning (thinking tag injection).

Some models refuse requests that carry the fake-reasoning prompt. Observed on
real Kiro traffic: ``claude-opus-5.5`` answers HTTP 200 with a single
``metadataEvent`` ``{"stopReason": "CONTENT_FILTERED", "stopDetails":
{"refusal": {"category": "REASONING_EXTRACTION", ...}}}`` and no content
whenever the "Extended Thinking Mode" system addition is present, while the
same request without it succeeds (and the model still reasons natively via
``reasoningContentEvent``).

This module:
- remembers which models refused the injection (with a TTL, so a change on
  Kiro's side is picked up again), so later requests skip it up front;
- removes an already injected prompt from a built payload, so the refused
  request can be retried transparently.

Only text the gateway itself injected is removed; user content is untouched.
"""

import re
import threading
import time
from typing import Any, Dict, Optional

from loguru import logger

from kiro.config import FAKE_REASONING_REFUSAL_TTL


REASONING_EXTRACTION_REFUSAL = "REASONING_EXTRACTION"
"""Kiro refusal category triggered by the fake-reasoning prompt."""

# Mirrors the prefix built by converters_core.inject_thinking_tags.
_THINKING_PREFIX_RE = re.compile(
    r"<thinking_mode>enabled</thinking_mode>\n"
    r"<max_thinking_length>\d+</max_thinking_length>\n"
    r"<thinking_instruction>.*?</thinking_instruction>\n\n",
    re.DOTALL,
)

_lock = threading.Lock()
_refused_until: Dict[str, float] = {}


def _normalize(model_id: str) -> str:
    """
    Normalizes a Kiro model id for registry lookups.

    Args:
        model_id: Model id as sent to Kiro

    Returns:
        Lower-cased, stripped id
    """
    return model_id.strip().lower()


def should_inject(model_id: Optional[str]) -> bool:
    """
    Checks whether fake reasoning may be injected for a model.

    Args:
        model_id: Kiro model id (None means unknown: allowed)

    Returns:
        False while the model is in its refusal cooldown, True otherwise
    """
    if not model_id:
        return True
    key = _normalize(model_id)
    with _lock:
        until = _refused_until.get(key)
        if until is None:
            return True
        if until <= time.monotonic():
            del _refused_until[key]
            logger.info(f"Fake reasoning re-enabled for model '{model_id}' (refusal cooldown expired)")
            return True
        return False


def mark_refused(model_id: Optional[str]) -> None:
    """
    Records that a model refused the fake-reasoning prompt.

    Args:
        model_id: Kiro model id (ignored when empty)
    """
    if not model_id:
        return
    key = _normalize(model_id)
    with _lock:
        first_time = key not in _refused_until
        _refused_until[key] = time.monotonic() + FAKE_REASONING_REFUSAL_TTL
    if first_time:
        logger.warning(
            f"Model '{model_id}' refused the fake reasoning prompt (REASONING_EXTRACTION); "
            f"skipping thinking injection for it for {FAKE_REASONING_REFUSAL_TTL}s. "
            f"Set FAKE_REASONING=false to disable injection for all models."
        )


def reset_for_testing() -> None:
    """Clears the refusal registry."""
    with _lock:
        _refused_until.clear()


def get_payload_model_id(payload: Dict[str, Any]) -> Optional[str]:
    """
    Extracts the model id from a Kiro generateAssistantResponse payload.

    Args:
        payload: Kiro request payload

    Returns:
        Model id, or None if the payload has no current user message
    """
    try:
        model_id = payload["conversationState"]["currentMessage"]["userInputMessage"]["modelId"]
    except (KeyError, TypeError):
        return None
    return model_id if isinstance(model_id, str) else None


def _strip_text(text: str, system_addition: str) -> str:
    """
    Removes injected fake-reasoning text from one content string.

    Args:
        text: Content string
        system_addition: Exact system prompt addition that was injected

    Returns:
        Content without the injected parts
    """
    stripped = _THINKING_PREFIX_RE.sub("", text)
    if system_addition:
        stripped = stripped.replace(system_addition, "")
        # When the system prompt was empty the addition was inserted stripped
        stripped = stripped.replace(system_addition.strip(), "")
    return stripped


def strip_fake_reasoning(payload: Dict[str, Any], system_addition: str) -> bool:
    """
    Removes fake-reasoning injection from a built Kiro payload, in place.

    Walks every ``content`` string (current message and history) because
    the system prompt is merged into a user message.

    Args:
        payload: Kiro request payload (modified in place)
        system_addition: Text returned by get_thinking_system_prompt_addition()

    Returns:
        True if anything was removed
    """
    changed = False

    def walk(node: Any) -> None:
        nonlocal changed
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "content" and isinstance(value, str):
                    new_value = _strip_text(value, system_addition)
                    if new_value != value:
                        # Kiro rejects empty content; keep a placeholder like the converters do
                        node[key] = new_value if new_value.strip() else "(empty placeholder)"
                        changed = True
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return changed
