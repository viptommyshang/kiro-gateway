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
Binary framing decoder for the AWS event stream wire format.

Kiro's generateAssistantResponse answers with
``application/vnd.amazon.eventstream``. Every message is a self-delimiting
frame::

    [total_length:u32][headers_length:u32][prelude_crc:u32]
    [headers ...][payload ...][message_crc:u32]

The headers carry the routing information that plain-text scanning cannot
see, most importantly ``:message-type`` (``event`` / ``exception`` /
``error``), ``:event-type`` and ``:exception-type``. Without them an
in-stream exception (for example a throttling error delivered inside an
HTTP 200 body) is indistinguishable from a silent, empty answer.

This module only decodes frames. Interpreting payloads is left to
``kiro.parsers.AwsEventStreamParser``.
"""

import struct
import zlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

from loguru import logger


PRELUDE_LENGTH = 12
"""Bytes in the prelude: total length, headers length and prelude CRC."""

MESSAGE_CRC_LENGTH = 4
"""Bytes in the trailing message CRC."""

MIN_FRAME_LENGTH = PRELUDE_LENGTH + MESSAGE_CRC_LENGTH
"""Smallest valid frame (no headers, empty payload)."""

MAX_FRAME_LENGTH = 16 * 1024 * 1024
"""AWS caps event stream messages at 16 MiB; anything larger is not a frame."""

HeaderValue = Union[str, bytes, bool, int]

# Header value type ids from the AWS event stream specification.
_HEADER_BOOL_TRUE = 0
_HEADER_BOOL_FALSE = 1
_HEADER_BYTE = 2
_HEADER_SHORT = 3
_HEADER_INTEGER = 4
_HEADER_LONG = 5
_HEADER_BYTE_ARRAY = 6
_HEADER_STRING = 7
_HEADER_TIMESTAMP = 8
_HEADER_UUID = 9

_FIXED_WIDTH_HEADERS = {
    _HEADER_BYTE: ">b",
    _HEADER_SHORT: ">h",
    _HEADER_INTEGER: ">i",
    _HEADER_LONG: ">q",
    _HEADER_TIMESTAMP: ">q",
}


class EventStreamFormatError(ValueError):
    """Raised when bytes claim to be an event stream frame but are malformed."""


@dataclass
class EventStreamFrame:
    """
    One decoded event stream message.

    Attributes:
        headers: Decoded headers keyed by name (e.g. ``:message-type``)
        payload: Raw payload bytes (JSON for Kiro events)
    """
    headers: Dict[str, HeaderValue] = field(default_factory=dict)
    payload: bytes = b""

    @property
    def message_type(self) -> str:
        """Value of ``:message-type`` (defaults to ``event``)."""
        value = self.headers.get(":message-type", "event")
        return value if isinstance(value, str) else "event"

    @property
    def event_type(self) -> Optional[str]:
        """Value of ``:event-type`` for event messages, if present."""
        value = self.headers.get(":event-type")
        return value if isinstance(value, str) else None

    @property
    def exception_type(self) -> Optional[str]:
        """
        Exception identifier for exception/error messages.

        Uses ``:exception-type`` for modeled exceptions and ``:error-code``
        for unmodeled errors.
        """
        for name in (":exception-type", ":error-code"):
            value = self.headers.get(name)
            if isinstance(value, str) and value:
                return value
        return None

    @property
    def payload_text(self) -> str:
        """Payload decoded as UTF-8 (invalid bytes replaced)."""
        return self.payload.decode("utf-8", errors="replace")


def looks_like_frame_prelude(data: Union[bytes, bytearray]) -> bool:
    """
    Checks whether ``data`` starts with a valid event stream prelude.

    The prelude CRC makes accidental matches on plain text practically
    impossible, so this is a safe way to choose between binary decoding and
    the legacy text scanner.

    Args:
        data: At least ``PRELUDE_LENGTH`` bytes from the start of the stream

    Returns:
        True if the first 12 bytes form a valid prelude
    """
    if len(data) < PRELUDE_LENGTH:
        return False
    total_length, headers_length, prelude_crc = struct.unpack(">III", bytes(data[:PRELUDE_LENGTH]))
    if total_length < MIN_FRAME_LENGTH or total_length > MAX_FRAME_LENGTH:
        return False
    if headers_length > total_length - MIN_FRAME_LENGTH:
        return False
    return zlib.crc32(bytes(data[:8])) & 0xFFFFFFFF == prelude_crc


def _decode_headers(raw: bytes) -> Dict[str, HeaderValue]:
    """
    Decodes the header block of a frame.

    Args:
        raw: Header bytes (exactly ``headers_length`` long)

    Returns:
        Mapping of header name to decoded value

    Raises:
        EventStreamFormatError: If the header block is truncated or uses an
            unknown value type
    """
    headers: Dict[str, HeaderValue] = {}
    pos = 0
    end = len(raw)
    try:
        while pos < end:
            name_length = raw[pos]
            pos += 1
            name = raw[pos:pos + name_length].decode("utf-8", errors="replace")
            pos += name_length
            value_type = raw[pos]
            pos += 1

            value: HeaderValue
            if value_type == _HEADER_BOOL_TRUE:
                value = True
            elif value_type == _HEADER_BOOL_FALSE:
                value = False
            elif value_type in _FIXED_WIDTH_HEADERS:
                fmt = _FIXED_WIDTH_HEADERS[value_type]
                width = struct.calcsize(fmt)
                (value,) = struct.unpack(fmt, raw[pos:pos + width])
                pos += width
            elif value_type in (_HEADER_BYTE_ARRAY, _HEADER_STRING):
                (length,) = struct.unpack(">H", raw[pos:pos + 2])
                pos += 2
                chunk = raw[pos:pos + length]
                pos += length
                value = chunk.decode("utf-8", errors="replace") if value_type == _HEADER_STRING else bytes(chunk)
            elif value_type == _HEADER_UUID:
                value = raw[pos:pos + 16].hex()
                pos += 16
            else:
                raise EventStreamFormatError(f"Unknown header value type {value_type} for header '{name}'")

            if pos > end:
                raise EventStreamFormatError(f"Header '{name}' runs past the header block")
            headers[name] = value
    except (IndexError, struct.error) as e:
        raise EventStreamFormatError(f"Truncated header block: {e}") from e
    return headers


class EventStreamDecoder:
    """
    Incremental decoder that turns arbitrary byte chunks into frames.

    Example:
        >>> decoder = EventStreamDecoder()
        >>> for frame in decoder.feed(chunk):
        ...     print(frame.message_type, frame.event_type)
    """

    def __init__(self) -> None:
        """Initializes an empty decoder."""
        self._buffer = bytearray()

    @property
    def pending_bytes(self) -> int:
        """Number of buffered bytes that do not yet form a complete frame."""
        return len(self._buffer)

    def take_pending(self) -> bytes:
        """
        Removes and returns buffered bytes that were not decoded.

        Used when the stream turns out not to be framed after all, so the
        caller can hand the bytes to a fallback parser.

        Returns:
            The undecoded bytes
        """
        data = bytes(self._buffer)
        self._buffer.clear()
        return data

    def feed(self, chunk: bytes) -> List[EventStreamFrame]:
        """
        Adds bytes and returns every frame completed by them.

        Args:
            chunk: Raw bytes from the HTTP body

        Returns:
            Completed frames in stream order (possibly empty)

        Raises:
            EventStreamFormatError: If buffered bytes do not start with a valid
                prelude. Already completed frames from this call are lost in
                that case, so callers should treat the error as "not framed".
        """
        self._buffer.extend(chunk)
        frames: List[EventStreamFrame] = []

        while len(self._buffer) >= PRELUDE_LENGTH:
            if not looks_like_frame_prelude(self._buffer):
                raise EventStreamFormatError("Invalid event stream prelude")

            total_length, headers_length = struct.unpack(">II", bytes(self._buffer[:8]))
            if len(self._buffer) < total_length:
                break

            frame_bytes = bytes(self._buffer[:total_length])
            del self._buffer[:total_length]

            (message_crc,) = struct.unpack(">I", frame_bytes[-MESSAGE_CRC_LENGTH:])
            if zlib.crc32(frame_bytes[:-MESSAGE_CRC_LENGTH]) & 0xFFFFFFFF != message_crc:
                # Prelude was valid, so framing is intact; keep going but make it visible.
                logger.warning("AWS event stream message CRC mismatch - payload may be corrupted")

            headers_end = PRELUDE_LENGTH + headers_length
            headers = _decode_headers(frame_bytes[PRELUDE_LENGTH:headers_end])
            payload = frame_bytes[headers_end:-MESSAGE_CRC_LENGTH]
            frames.append(EventStreamFrame(headers=headers, payload=payload))

        return frames


def encode_frame(headers: Dict[str, str], payload: bytes) -> bytes:
    """
    Encodes a frame with string headers.

    Kiro never needs this at runtime; it exists so tests (and debugging
    tools) can build byte-exact frames without hand-computing CRCs.

    Args:
        headers: Header name to string value
        payload: Payload bytes

    Returns:
        Encoded frame bytes
    """
    header_bytes = bytearray()
    for name, value in headers.items():
        name_raw = name.encode("utf-8")
        value_raw = value.encode("utf-8")
        header_bytes.append(len(name_raw))
        header_bytes.extend(name_raw)
        header_bytes.append(_HEADER_STRING)
        header_bytes.extend(struct.pack(">H", len(value_raw)))
        header_bytes.extend(value_raw)

    total_length = MIN_FRAME_LENGTH + len(header_bytes) + len(payload)
    prelude = struct.pack(">II", total_length, len(header_bytes))
    prelude += struct.pack(">I", zlib.crc32(prelude) & 0xFFFFFFFF)
    body = prelude + bytes(header_bytes) + payload
    return body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)

