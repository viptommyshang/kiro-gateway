# -*- coding: utf-8 -*-

"""
Tests for kiro.fake_reasoning_policy.

Covers the per-model refusal registry (should_inject / mark_refused with TTL),
payload model-id extraction, and in-place stripping of an injected
fake-reasoning prompt so a refused request can be retried.
"""

import time
from unittest.mock import patch

import pytest

from kiro import fake_reasoning_policy as frp
from kiro.converters_core import get_thinking_system_prompt_addition


@pytest.fixture(autouse=True)
def _clear_registry():
    frp.reset_for_testing()
    yield
    frp.reset_for_testing()


def _payload_with(content: str, model: str = "claude-opus-5.5") -> dict:
    return {
        "conversationState": {
            "currentMessage": {
                "userInputMessage": {"content": content, "modelId": model}
            },
            "history": [
                {"userInputMessage": {"content": content, "modelId": model}},
            ],
        }
    }


def _injected_content() -> str:
    return (
        "<thinking_mode>enabled</thinking_mode>\n"
        "<max_thinking_length>8000</max_thinking_length>\n"
        "<thinking_instruction>Think carefully.\nBe systematic.</thinking_instruction>\n\n"
        "What is 2+2?"
    )


class TestRefusalRegistry:
    def test_injection_allowed_by_default(self):
        assert frp.should_inject("claude-opus-5.5") is True
        assert frp.should_inject(None) is True

    def test_mark_refused_blocks_same_model(self):
        frp.mark_refused("claude-opus-5.5")
        assert frp.should_inject("claude-opus-5.5") is False
        # Case-insensitive, other models unaffected
        assert frp.should_inject("CLAUDE-OPUS-5.5") is False
        assert frp.should_inject("claude-sonnet-5.5") is True

    def test_cooldown_expires(self):
        with patch("kiro.fake_reasoning_policy.FAKE_REASONING_REFUSAL_TTL", 0.05):
            frp.mark_refused("claude-opus-5.5")
            assert frp.should_inject("claude-opus-5.5") is False
            time.sleep(0.06)
            assert frp.should_inject("claude-opus-5.5") is True

    def test_mark_refused_ignores_empty(self):
        frp.mark_refused(None)
        frp.mark_refused("")
        assert frp.should_inject("claude-opus-5.5") is True


class TestPayloadModelId:
    def test_extract(self):
        assert frp.get_payload_model_id(_payload_with("x")) == "claude-opus-5.5"

    def test_missing_returns_none(self):
        assert frp.get_payload_model_id({}) is None
        assert frp.get_payload_model_id({"conversationState": {}}) is None


class TestStripFakeReasoning:
    def test_strips_prefix_and_system_addition(self):
        addition = get_thinking_system_prompt_addition()
        assert addition  # fake reasoning enabled by default in tests
        payload = _payload_with(_injected_content() + addition)

        changed = frp.strip_fake_reasoning(payload, addition)

        assert changed is True
        content = payload["conversationState"]["currentMessage"]["userInputMessage"]["content"]
        assert "<thinking_mode>" not in content
        assert "Extended Thinking Mode" not in content
        assert "What is 2+2?" in content
        # History content is stripped too
        hist = payload["conversationState"]["history"][0]["userInputMessage"]["content"]
        assert "<thinking_mode>" not in hist

    def test_no_injection_means_no_change(self):
        payload = _payload_with("just a normal question")
        assert frp.strip_fake_reasoning(payload, get_thinking_system_prompt_addition()) is False
        assert (
            payload["conversationState"]["currentMessage"]["userInputMessage"]["content"]
            == "just a normal question"
        )

    def test_content_that_becomes_empty_gets_placeholder(self):
        addition = get_thinking_system_prompt_addition()
        # Prefix only, no user text after it
        prefix = (
            "<thinking_mode>enabled</thinking_mode>\n"
            "<max_thinking_length>8000</max_thinking_length>\n"
            "<thinking_instruction>x</thinking_instruction>\n\n"
        )
        payload = _payload_with(prefix)
        frp.strip_fake_reasoning(payload, addition)
        content = payload["conversationState"]["currentMessage"]["userInputMessage"]["content"]
        assert content.strip() != ""  # Kiro rejects empty content
