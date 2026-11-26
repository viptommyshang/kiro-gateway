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
Parsers for AWS Event Stream format.

Contains classes and functions for:
- Parsing binary AWS SSE stream
- Extracting JSON events
- Processing tool calls
- Content deduplication
"""

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from kiro.aws_eventstream import (
    PRELUDE_LENGTH,
    EventStreamDecoder,
    EventStreamFormatError,
    EventStreamFrame,
    looks_like_frame_prelude,
)
from kiro.utils import generate_tool_call_id


REFUSAL_EXCEPTION_TYPE = "ContentRefusal"
"""Synthetic exception type for refusals reported in a ``metadataEvent``."""


def find_matching_brace(text: str, start_pos: int) -> int:
    """
    Finds the position of the closing brace considering nesting and strings.
    
    Uses bracket counting for correct parsing of nested JSON.
    Accounts for quoted strings and escape sequences.
    
    Args:
        text: Text to search
        start_pos: Position of opening brace '{'
    
    Returns:
        Position of closing brace or -1 if not found
    
    Example:
        >>> find_matching_brace('{"a": {"b": 1}}', 0)
        14
        >>> find_matching_brace('{"a": "{}"}', 0)
        10
    """
    if start_pos >= len(text) or text[start_pos] != '{':
        return -1
    
    brace_count = 0
    in_string = False
    escape_next = False
    
    for i in range(start_pos, len(text)):
        char = text[i]
        
        if escape_next:
            escape_next = False
            continue
        
        if char == '\\' and in_string:
            escape_next = True
            continue
        
        if char == '"' and not escape_next:
            in_string = not in_string
            continue
        
        if not in_string:
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    return i
    
    return -1


def parse_bracket_tool_calls(response_text: str) -> List[Dict[str, Any]]:
    """
    Parses tool calls in [Called func_name with args: {...}] format.
    
    Some models return tool calls in text format instead of
    structured JSON. This function extracts them.
    
    Args:
        response_text: Model response text
    
    Returns:
        List of tool calls in OpenAI format
    
    Example:
        >>> text = "[Called get_weather with args: {\"city\": \"London\"}]"
        >>> calls = parse_bracket_tool_calls(text)
        >>> calls[0]["function"]["name"]
        'get_weather'
    """
    if not response_text or "[Called" not in response_text:
        return []
    
    tool_calls = []
    pattern = r'\[Called\s+(\w+)\s+with\s+args:\s*'
    
    for match in re.finditer(pattern, response_text, re.IGNORECASE):
        func_name = match.group(1)
        args_start = match.end()
        
        # Find JSON start
        json_start = response_text.find('{', args_start)
        if json_start == -1:
            continue
        
        # Find JSON end considering nesting
        json_end = find_matching_brace(response_text, json_start)
        if json_end == -1:
            continue
        
        json_str = response_text[json_start:json_end + 1]
        
        try:
            args = json.loads(json_str)
            tool_call_id = generate_tool_call_id()
            # index will be added later when forming the final response
            tool_calls.append({
                "id": tool_call_id,
                "type": "function",
                "function": {
                    "name": func_name,
                    "arguments": json.dumps(args)
                }
            })
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse tool call arguments: {json_str[:100]}")
    
    return tool_calls


def deduplicate_tool_calls(tool_calls: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Removes duplicate tool calls.
    
    Deduplication occurs by two criteria:
    1. By id - if there are multiple tool calls with the same id, keep the one with
       more arguments (not empty "{}")
    2. By name+arguments - remove complete duplicates
    
    Args:
        tool_calls: List of tool calls
    
    Returns:
        List of unique tool calls
    """
    # First deduplicate by id - keep tool call with non-empty arguments
    by_id: Dict[str, Dict[str, Any]] = {}
    for tc in tool_calls:
        tc_id = tc.get("id", "")
        if not tc_id:
            # Without id - add as is (will be deduplicated by name+args)
            continue
        
        existing = by_id.get(tc_id)
        if existing is None:
            by_id[tc_id] = tc
        else:
            # Duplicate by id exists - keep the one with more arguments
            existing_args = existing.get("function", {}).get("arguments", "{}")
            current_args = tc.get("function", {}).get("arguments", "{}")
            
            # Prefer non-empty arguments
            if current_args != "{}" and (existing_args == "{}" or len(current_args) > len(existing_args)):
                logger.debug(f"Replacing tool call {tc_id} with better arguments: {len(existing_args)} -> {len(current_args)}")
                by_id[tc_id] = tc
    
    # Collect tool calls: first those with id, then without id
    result_with_id = list(by_id.values())
    result_without_id = [tc for tc in tool_calls if not tc.get("id")]
    
    # Now deduplicate by name+arguments for all
    seen = set()
    unique = []
    
    for tc in result_with_id + result_without_id:
        # Protection against None in function
        func = tc.get("function") or {}
        func_name = func.get("name") or ""
        func_args = func.get("arguments") or "{}"
        key = f"{func_name}-{func_args}"
        if key not in seen:
            seen.add(key)
            unique.append(tc)
    
    if len(tool_calls) != len(unique):
        logger.debug(f"Deduplicated tool calls: {len(tool_calls)} -> {len(unique)}")
    
    return unique


class AwsEventStreamParser:
    """
    Parser for AWS Event Stream format.
    
    AWS returns events in binary format with :message-type...event delimiters.
    This class extracts JSON events from the stream and converts them to a convenient format.
    
    Supported event types:
    - content: Text content of response
    - tool_start: Start of tool call (name, toolUseId)
    - tool_input: Continuation of input for tool call
    - tool_stop: End of tool call
    - usage: Credit consumption information
    - context_usage: Context usage percentage
    - error: In-stream exception or refusal (binary frames only), e.g. ThrottlingException
    - reasoning: Native model reasoning text (reasoningContentEvent, binary frames only)
    
    Attributes:
        buffer: Buffer for accumulating data
        last_content: Last processed content (for deduplication)
        current_tool_call: Current incomplete tool call
        tool_calls: List of completed tool calls
    
    Example:
        >>> parser = AwsEventStreamParser()
        >>> events = parser.feed(chunk)
        >>> for event in events:
        ...     if event["type"] == "content":
        ...         print(event["data"])
    """
    
    # Patterns for finding JSON events
    EVENT_PATTERNS = [
        ('{"content":', 'content'),
        ('{"name":', 'tool_start'),
        ('{"input":', 'tool_input'),
        ('{"stop":', 'tool_stop'),
        ('{"followupPrompt":', 'followup'),
        ('{"usage":', 'usage'),
        ('{"contextUsagePercentage":', 'context_usage'),
    ]
    
    # Event types (``:event-type`` header) that signal a failed generation even
    # though they arrive as regular events rather than exception frames.
    ERROR_EVENT_TYPES = frozenset({"invalidStateEvent"})
    
    def __init__(self):
        """Initializes the parser."""
        self.buffer = ""
        self.last_content: Optional[str] = None  # For deduplicating repeating content
        self.current_tool_call: Optional[Dict[str, Any]] = None
        self.tool_calls: List[Dict[str, Any]] = []
        # Framing mode is detected from the first bytes: "binary" decodes AWS
        # event stream frames (headers included), "text" keeps the legacy
        # pattern scanner for anything that is not framed.
        self._mode: Optional[str] = None
        self._pending = bytearray()
        self._decoder = EventStreamDecoder()
        # Set by the streaming layer once native reasoning has been emitted
        self.native_reasoning_started = False
    
    def feed(self, chunk: bytes) -> List[Dict[str, Any]]:
        """
        Adds chunk to buffer and returns parsed events.
        
        Besides payload events, binary frames with ``:message-type`` of
        ``exception``/``error`` (and error-like events such as
        ``invalidStateEvent``) are returned as ``{"type": "error", ...}``
        events, so callers can tell a failed generation from an empty one.
        
        Args:
            chunk: Bytes of data from stream
        
        Returns:
            List of events in {"type": str, "data": Any} format
        """
        if not chunk:
            return []
        
        if self._mode is None:
            self._pending.extend(chunk)
            # Event stream frames start with a big-endian length < 16 MiB, so the
            # first byte is always 0x00. Anything else is plain text.
            if self._pending[0] != 0:
                self._mode = "text"
            elif len(self._pending) < PRELUDE_LENGTH:
                return []
            else:
                self._mode = "binary" if looks_like_frame_prelude(self._pending) else "text"
            chunk = bytes(self._pending)
            self._pending.clear()
            logger.debug(f"AWS event stream parser mode: {self._mode}")
        
        if self._mode == "binary":
            return self._feed_binary(chunk)
        return self._feed_text(chunk)
    
    def _feed_text(self, chunk: bytes) -> List[Dict[str, Any]]:
        """
        Legacy path: scans decoded text for known JSON payload prefixes.
        
        Args:
            chunk: Bytes of data from stream
        
        Returns:
            Parsed events
        """
        self.buffer += chunk.decode('utf-8', errors='ignore')
        events, self.buffer = self._extract_payload_events(self.buffer)
        return events
    
    def _feed_binary(self, chunk: bytes) -> List[Dict[str, Any]]:
        """
        Decodes AWS event stream frames and dispatches them by message type.
        
        Falls back to the text scanner (for the rest of the stream) if the
        bytes stop looking like frames, so a framing surprise degrades to the
        legacy behavior instead of losing data.
        
        Args:
            chunk: Bytes of data from stream
        
        Returns:
            Parsed events
        """
        try:
            frames = self._decoder.feed(chunk)
        except EventStreamFormatError as e:
            logger.warning(f"AWS event stream framing lost ({e}), falling back to text scanning")
            self._mode = "text"
            return self._feed_text(self._decoder.take_pending())
        
        events: List[Dict[str, Any]] = []
        for frame in frames:
            events.extend(self._process_frame(frame))
        return events
    
    def _process_frame(self, frame: EventStreamFrame) -> List[Dict[str, Any]]:
        """
        Converts one decoded frame into parser events.
        
        Args:
            frame: Decoded event stream frame
        
        Returns:
            Parsed events (possibly empty)
        """
        message_type = frame.message_type
        payload_text = frame.payload_text
        
        if message_type in ("exception", "error"):
            error_event = self._build_error_event(
                frame.exception_type or message_type,
                payload_text,
                frame.headers.get(":error-message"),
            )
            logger.warning(
                f"Kiro API stream {message_type}: "
                f"type={error_event['data']['exception_type']}, "
                f"message={error_event['data']['message'][:200]}"
            )
            return [error_event]
        
        event_type = frame.event_type
        if event_type == "metadataEvent":
            refusal_event = self._process_metadata_frame(payload_text)
            return [refusal_event] if refusal_event else []
        if event_type == "reasoningContentEvent":
            reasoning_event = self._process_reasoning_frame(payload_text)
            return [reasoning_event] if reasoning_event else []
        if event_type in self.ERROR_EVENT_TYPES:
            error_event = self._build_error_event(event_type, payload_text, None)
            logger.warning(
                f"Kiro API stream reported {event_type}: "
                f"reason={error_event['data']['reason']}, message={error_event['data']['message'][:200]}"
            )
            return [error_event]
        
        events, leftover = self._extract_payload_events(payload_text)
        if not events and payload_text.strip() and not self._is_known_silent_payload(payload_text):
            logger.debug(f"Ignoring Kiro event '{event_type}': {payload_text[:200]}")
        elif leftover.strip():
            logger.debug(f"Unparsed remainder in Kiro event '{event_type}': {leftover[:200]}")
        return events
    
    @staticmethod
    def _load_json_object(payload_text: str) -> Optional[Dict[str, Any]]:
        """
        Parses a frame payload that is expected to be a JSON object.
        
        Args:
            payload_text: Frame payload as text
        
        Returns:
            Parsed dict, or None if the payload is empty, invalid or not an object
        """
        if not payload_text.strip():
            return None
        try:
            data = json.loads(payload_text)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse Kiro frame payload: {payload_text[:100]}")
            return None
        return data if isinstance(data, dict) else None
    
    def _process_metadata_frame(self, payload_text: str) -> Optional[Dict[str, Any]]:
        """
        Inspects a ``metadataEvent`` for a model refusal.
        
        Kiro reports refusals (e.g. ``REASONING_EXTRACTION``) only here, as
        ``stopReason: CONTENT_FILTERED`` plus ``stopDetails.refusal`` inside an
        HTTP 200 stream that carries no content. Without this check the
        client gets an empty "successful" answer.
        
        Args:
            payload_text: Frame payload as text
        
        Returns:
            ``error`` event for refusals, otherwise None
        """
        data = self._load_json_object(payload_text)
        if data is None:
            return None
        stop_details = data.get("stopDetails")
        refusal = stop_details.get("refusal") if isinstance(stop_details, dict) else None
        if not isinstance(refusal, dict) and data.get("stopReason") != "CONTENT_FILTERED":
            return None
        
        refusal = refusal if isinstance(refusal, dict) else {}
        category = refusal.get("category")
        explanation = refusal.get("explanation")
        event = {
            "type": "error",
            "data": {
                "exception_type": REFUSAL_EXCEPTION_TYPE,
                "message": explanation if isinstance(explanation, str) and explanation else "The model refused to answer",
                "reason": category if isinstance(category, str) and category else str(data.get("stopReason") or "CONTENT_FILTERED"),
            },
        }
        logger.warning(
            f"Kiro API stream refusal: category={event['data']['reason']}, "
            f"message={event['data']['message'][:200]}"
        )
        return event
    
    def _process_reasoning_frame(self, payload_text: str) -> Optional[Dict[str, Any]]:
        """
        Converts a native ``reasoningContentEvent`` into a ``reasoning`` event.
        
        Kiro streams the model's own reasoning as ``{"text": "..."}`` frames,
        followed by a ``{"signature": "..."}`` frame that carries no text.
        
        Args:
            payload_text: Frame payload as text
        
        Returns:
            ``reasoning`` event, or None for signature/empty frames
        """
        data = self._load_json_object(payload_text)
        if data is None:
            return None
        text = data.get("text")
        if not isinstance(text, str) or not text:
            return None
        return {"type": "reasoning", "data": text}
    
    def _is_known_silent_payload(self, payload_text: str) -> bool:
        """
        Checks whether a payload matched a known pattern that yields no event.
        
        followupPrompt events, for instance, are recognised but intentionally
        produce nothing; they should not be reported as "ignored".
        
        Args:
            payload_text: Frame payload as text
        
        Returns:
            True if the payload starts with a known event pattern
        """
        stripped = payload_text.lstrip()
        return any(stripped.startswith(pattern) for pattern, _ in self.EVENT_PATTERNS)
    
    @staticmethod
    def _build_error_event(
        exception_type: str,
        payload_text: str,
        header_message: Optional[Any],
    ) -> Dict[str, Any]:
        """
        Builds a normalized error event from an exception frame.
        
        Args:
            exception_type: Exception/event type name (e.g. ThrottlingException)
            payload_text: Frame payload, usually ``{"message": "...", "reason": ...}``
            header_message: Optional ``:error-message`` header value
        
        Returns:
            Event dict ``{"type": "error", "data": {...}}``
        """
        message = ""
        reason: Optional[str] = None
        try:
            payload = json.loads(payload_text) if payload_text.strip() else {}
        except json.JSONDecodeError:
            payload = {}
            message = payload_text.strip()
        if isinstance(payload, dict):
            raw_message = payload.get("message") or payload.get("Message")
            if isinstance(raw_message, str):
                message = raw_message
            raw_reason = payload.get("reason")
            if isinstance(raw_reason, str):
                reason = raw_reason
        if not message and isinstance(header_message, str):
            message = header_message
        return {
            "type": "error",
            "data": {
                "exception_type": exception_type,
                "message": message or "(no message)",
                "reason": reason,
            },
        }
    
    def _extract_payload_events(self, text: str) -> Tuple[List[Dict[str, Any]], str]:
        """
        Extracts every complete known JSON event from ``text``.
        
        Args:
            text: Text buffer to scan
        
        Returns:
            Tuple of (events, unconsumed remainder of ``text``)
        """
        events = []
        
        while True:
            # Find nearest pattern
            earliest_pos = -1
            earliest_type = None
            
            for pattern, event_type in self.EVENT_PATTERNS:
                pos = text.find(pattern)
                if pos != -1 and (earliest_pos == -1 or pos < earliest_pos):
                    earliest_pos = pos
                    earliest_type = event_type
            
            if earliest_pos == -1:
                break
            
            # Find JSON end
            json_end = find_matching_brace(text, earliest_pos)
            if json_end == -1:
                # JSON not complete, wait for more data
                break
            
            json_str = text[earliest_pos:json_end + 1]
            text = text[json_end + 1:]
            
            try:
                data = json.loads(json_str)
                event = self._process_event(data, earliest_type)
                if event:
                    events.append(event)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON: {json_str[:100]}")
        
        return events, text
    
    def _process_event(self, data: dict, event_type: str) -> Optional[Dict[str, Any]]:
        """
        Processes a parsed event.
        
        Args:
            data: Parsed JSON
            event_type: Event type
        
        Returns:
            Processed event or None
        """
        if event_type == 'content':
            return self._process_content_event(data)
        elif event_type == 'tool_start':
            return self._process_tool_start_event(data)
        elif event_type == 'tool_input':
            return self._process_tool_input_event(data)
        elif event_type == 'tool_stop':
            return self._process_tool_stop_event(data)
        elif event_type == 'usage':
            return {"type": "usage", "data": data.get('usage', 0)}
        elif event_type == 'context_usage':
            return {"type": "context_usage", "data": data.get('contextUsagePercentage', 0)}
        
        return None
    
    def _process_content_event(self, data: dict) -> Optional[Dict[str, Any]]:
        """Processes content event."""
        content = data.get('content', '')
        
        # Skip followupPrompt
        if data.get('followupPrompt'):
            return None
        
        # Deduplicate repeating content
        if content == self.last_content:
            return None
        
        self.last_content = content
        
        return {"type": "content", "data": content}
    
    def _process_tool_start_event(self, data: dict) -> Optional[Dict[str, Any]]:
        """Processes tool call start."""
        # Finalize previous tool call if exists
        if self.current_tool_call:
            self._finalize_tool_call()
        
        # input can be string or object
        input_data = data.get('input', '')
        if isinstance(input_data, dict):
            if input_data:
                # Non-empty dict: serialize it
                input_str = json.dumps(input_data)
            else:
                # Empty dict {}: fragments will follow, use empty string
                input_str = ''
        else:
            input_str = str(input_data) if input_data else ''
        
        self.current_tool_call = {
            "id": data.get('toolUseId', generate_tool_call_id()),
            "type": "function",
            "function": {
                "name": data.get('name', ''),
                "arguments": input_str
            }
        }
        
        if data.get('stop'):
            self._finalize_tool_call()
        
        return None
    
    def _process_tool_input_event(self, data: dict) -> Optional[Dict[str, Any]]:
        """Processes input continuation for tool call."""
        if self.current_tool_call:
            # input can be string or object
            input_data = data.get('input', '')
            if isinstance(input_data, dict):
                if input_data:
                    input_str = json.dumps(input_data)
                else:
                    input_str = ''
            else:
                input_str = str(input_data) if input_data else ''
            self.current_tool_call['function']['arguments'] += input_str
        return None
    
    def _process_tool_stop_event(self, data: dict) -> Optional[Dict[str, Any]]:
        """Processes tool call end."""
        if self.current_tool_call and data.get('stop'):
            self._finalize_tool_call()
        return None
    
    def _finalize_tool_call(self) -> None:
        """Finalizes current tool call and adds to list."""
        if not self.current_tool_call:
            return
        
        # Try to parse and normalize arguments as JSON
        args = self.current_tool_call['function']['arguments']
        tool_name = self.current_tool_call['function'].get('name', 'unknown')
        
        logger.debug(f"Finalizing tool call '{tool_name}' with raw arguments: {repr(args)[:200]}")
        
        if isinstance(args, str):
            if args.strip():
                try:
                    parsed = json.loads(args)
                    # Ensure result is a JSON string
                    self.current_tool_call['function']['arguments'] = json.dumps(parsed)
                    logger.debug(f"Tool '{tool_name}' arguments parsed successfully: {list(parsed.keys()) if isinstance(parsed, dict) else type(parsed)}")
                except json.JSONDecodeError as e:
                    # Analyze the failure to provide better diagnostics
                    truncation_info = self._diagnose_json_truncation(args)
                    
                    if truncation_info["is_truncated"]:
                        # Mark for recovery system
                        self.current_tool_call['_truncation_detected'] = True
                        self.current_tool_call['_truncation_info'] = truncation_info
                        
                        # Check if recovery is enabled
                        from kiro.config import TRUNCATION_RECOVERY
                        tool_id = self.current_tool_call.get('id', 'unknown')
                        
                        # Clear error message: this is Kiro API's fault, not ours
                        logger.error(
                            f"Tool call truncated by Kiro API: "
                            f"tool='{tool_name}', id={tool_id}, size={truncation_info['size_bytes']} bytes, "
                            f"reason={truncation_info['reason']}. "
                            f"This is a Kiro API limitation. "
                            f"{'Model will be notified automatically about truncation.' if TRUNCATION_RECOVERY else 'Set TRUNCATION_RECOVERY=true in .env to auto-notify model about truncation.'}"
                        )
                    else:
                        # Regular JSON parse error
                        logger.warning(f"Failed to parse tool '{tool_name}' arguments: {e}. Raw: {args[:200]}")
                    
                    self.current_tool_call['function']['arguments'] = "{}"
            else:
                # Empty string - use empty object
                # This is normal behavior for duplicate tool calls from Kiro
                logger.debug(f"Tool '{tool_name}' has empty arguments string (will be deduplicated)")
                self.current_tool_call['function']['arguments'] = "{}"
        elif isinstance(args, dict):
            # If already an object - serialize to string
            self.current_tool_call['function']['arguments'] = json.dumps(args)
            logger.debug(f"Tool '{tool_name}' arguments already dict with keys: {list(args.keys())}")
        else:
            # Unknown type - empty object
            logger.warning(f"Tool '{tool_name}' has unexpected arguments type: {type(args)}")
            self.current_tool_call['function']['arguments'] = "{}"
        
        self.tool_calls.append(self.current_tool_call)
        self.current_tool_call = None
    
    def _diagnose_json_truncation(self, json_str: str) -> Dict[str, Any]:
        """
        Analyzes a malformed JSON string to determine if it was truncated.
        
        This helps distinguish between upstream issues (Kiro API cutting off
        large tool call arguments) and actual malformed JSON from the model.
        
        Args:
            json_str: The raw JSON string that failed to parse
        
        Returns:
            Dictionary with diagnostic information:
            - is_truncated: True if the JSON appears to be cut off
            - reason: Human-readable explanation of why it's truncated
            - size_bytes: Size of the received data
        """
        size_bytes = len(json_str.encode('utf-8'))
        stripped = json_str.strip()
        
        # Check for obvious truncation signs
        if not stripped:
            return {"is_truncated": False, "reason": "empty string", "size_bytes": size_bytes}
        
        # Count braces and brackets (simplified, doesn't account for strings perfectly)
        open_braces = stripped.count('{')
        close_braces = stripped.count('}')
        open_brackets = stripped.count('[')
        close_brackets = stripped.count(']')
        
        # Check if JSON starts with { but doesn't end with }
        if stripped.startswith('{') and not stripped.endswith('}'):
            missing = open_braces - close_braces
            return {
                "is_truncated": True,
                "reason": f"missing {missing} closing brace(s)",
                "size_bytes": size_bytes
            }
        
        # Check if JSON starts with [ but doesn't end with ]
        if stripped.startswith('[') and not stripped.endswith(']'):
            missing = open_brackets - close_brackets
            return {
                "is_truncated": True,
                "reason": f"missing {missing} closing bracket(s)",
                "size_bytes": size_bytes
            }
        
        # Check for unbalanced braces/brackets
        if open_braces != close_braces:
            diff = open_braces - close_braces
            return {
                "is_truncated": True,
                "reason": f"unbalanced braces ({open_braces} open, {close_braces} close)",
                "size_bytes": size_bytes
            }
        
        if open_brackets != close_brackets:
            diff = open_brackets - close_brackets
            return {
                "is_truncated": True,
                "reason": f"unbalanced brackets ({open_brackets} open, {close_brackets} close)",
                "size_bytes": size_bytes
            }
        
        # Check for unclosed string (ends with backslash or inside quotes)
        # This is a heuristic - count unescaped quotes
        quote_count = 0
        i = 0
        while i < len(stripped):
            if stripped[i] == '\\' and i + 1 < len(stripped):
                i += 2  # Skip escaped character
                continue
            if stripped[i] == '"':
                quote_count += 1
            i += 1
        
        if quote_count % 2 != 0:
            return {
                "is_truncated": True,
                "reason": "unclosed string literal",
                "size_bytes": size_bytes
            }
        
        # Doesn't look truncated, probably just malformed
        return {"is_truncated": False, "reason": "malformed JSON", "size_bytes": size_bytes}
    
    def get_tool_calls(self) -> List[Dict[str, Any]]:
        """
        Returns all collected tool calls.
        
        Finalizes current tool call if not finished.
        Removes duplicates.
        
        Returns:
            List of unique tool calls
        """
        if self.current_tool_call:
            self._finalize_tool_call()
        return deduplicate_tool_calls(self.tool_calls)
    
    def reset(self) -> None:
        """Resets parser state."""
        self.buffer = ""
        self.last_content = None
        self.current_tool_call = None
        self.tool_calls = []