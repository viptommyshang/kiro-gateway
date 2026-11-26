# -*- coding: utf-8 -*-

"""
Tests for kiro.aws_eventstream (binary AWS event stream framing).

Covers:
- Prelude detection (valid frames vs. plain text / garbage)
- Round-trip encode/decode, including every possible chunk split
- Header value types defined by the AWS specification
- Malformed input: bad prelude, truncated headers, unknown header types, CRC mismatch
"""

import struct
import zlib

import pytest

from kiro.aws_eventstream import (
    MIN_FRAME_LENGTH,
    PRELUDE_LENGTH,
    EventStreamDecoder,
    EventStreamFormatError,
    EventStreamFrame,
    _decode_headers,
    encode_frame,
    looks_like_frame_prelude,
)


def _event(payload: bytes, event_type: str = "assistantResponseEvent") -> bytes:
    return encode_frame(
        {":message-type": "event", ":event-type": event_type, ":content-type": "application/json"},
        payload,
    )


def _raw_frame(header_bytes: bytes, payload: bytes) -> bytes:
    """Builds a frame from pre-encoded header bytes (for exotic header types)."""
    total = MIN_FRAME_LENGTH + len(header_bytes) + len(payload)
    prelude = struct.pack(">II", total, len(header_bytes))
    prelude += struct.pack(">I", zlib.crc32(prelude) & 0xFFFFFFFF)
    body = prelude + header_bytes + payload
    return body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)


class TestLooksLikeFramePrelude:
    def test_valid_frame_is_detected(self):
        """
        What it does: Checks an encoded frame.
        Purpose: Binary mode must be chosen for real event streams.
        """
        assert looks_like_frame_prelude(_event(b'{"content":"hi"}')) is True

    @pytest.mark.parametrize("data", [
        b'{"content":"hello world"}',
        b"",
        b"\x00\x00\x00",
        b"\x00" * 12,  # zero length is below the minimum frame size
        b"data: {\"x\": 1}\n\n",
    ])
    def test_non_frames_are_rejected(self, data):
        """
        What it does: Feeds text, too-short and zero data.
        Purpose: Plain-text streams keep using the legacy scanner.
        """
        assert looks_like_frame_prelude(data) is False

    def test_corrupted_prelude_crc_is_rejected(self):
        frame = bytearray(_event(b"{}"))
        frame[9] ^= 0xFF
        assert looks_like_frame_prelude(frame) is False

    def test_oversized_length_is_rejected(self):
        prelude = struct.pack(">II", 32 * 1024 * 1024, 0)
        prelude += struct.pack(">I", zlib.crc32(prelude) & 0xFFFFFFFF)
        assert looks_like_frame_prelude(prelude) is False

    def test_headers_longer_than_frame_are_rejected(self):
        prelude = struct.pack(">II", 20, 10)
        prelude += struct.pack(">I", zlib.crc32(prelude) & 0xFFFFFFFF)
        assert looks_like_frame_prelude(prelude) is False


class TestEventStreamDecoder:
    def test_single_frame_round_trip(self):
        decoder = EventStreamDecoder()
        frames = decoder.feed(_event(b'{"content":"Hello"}'))

        assert len(frames) == 1
        assert frames[0].message_type == "event"
        assert frames[0].event_type == "assistantResponseEvent"
        assert frames[0].payload == b'{"content":"Hello"}'
        assert decoder.pending_bytes == 0

    def test_multiple_frames_in_one_chunk(self):
        data = _event(b'{"content":"a"}') + _event(b'{"content":"b"}') + _event(b'{"usage":1}', "meteringEvent")
        frames = EventStreamDecoder().feed(data)
        assert [f.payload for f in frames] == [b'{"content":"a"}', b'{"content":"b"}', b'{"usage":1}']
        assert frames[2].event_type == "meteringEvent"

    def test_every_split_point_produces_same_frames(self):
        """
        What it does: Splits two frames at every byte offset.
        Purpose: Network chunk boundaries never lose or duplicate frames.
        """
        data = _event('{"content":"Привет"}'.encode()) + _event(b'{"content":"!"}')
        for split in range(len(data) + 1):
            decoder = EventStreamDecoder()
            frames = decoder.feed(data[:split]) + decoder.feed(data[split:])
            assert [f.payload_text for f in frames] == ['{"content":"Привет"}', '{"content":"!"}'], split
            assert decoder.pending_bytes == 0

    def test_byte_by_byte_feeding(self):
        data = _event(b'{"content":"x"}')
        decoder = EventStreamDecoder()
        frames = []
        for i in range(len(data)):
            frames.extend(decoder.feed(data[i:i + 1]))
        assert len(frames) == 1

    def test_partial_frame_is_buffered(self):
        data = _event(b'{"content":"x"}')
        decoder = EventStreamDecoder()
        assert decoder.feed(data[:-1]) == []
        assert decoder.pending_bytes == len(data) - 1

    def test_invalid_prelude_raises_and_keeps_bytes_for_fallback(self):
        """
        What it does: Feeds text after a valid frame.
        Purpose: Callers can detect lost framing and recover the bytes.
        """
        decoder = EventStreamDecoder()
        decoder.feed(_event(b'{"content":"ok"}'))
        with pytest.raises(EventStreamFormatError):
            decoder.feed(b'{"content":"plain text now"}')
        assert decoder.take_pending() == b'{"content":"plain text now"}'
        assert decoder.pending_bytes == 0

    def test_message_crc_mismatch_still_decodes(self):
        """
        What it does: Corrupts the trailing CRC only.
        Purpose: A bad message CRC is logged, not fatal (prelude framing is intact).
        """
        frame = bytearray(_event(b'{"content":"x"}'))
        frame[-1] ^= 0xFF
        frames = EventStreamDecoder().feed(bytes(frame))
        assert frames[0].payload == b'{"content":"x"}'

    def test_exception_frame_exposes_exception_type(self):
        frame = encode_frame(
            {":message-type": "exception", ":exception-type": "ThrottlingException"},
            b'{"message":"Too many requests"}',
        )
        decoded = EventStreamDecoder().feed(frame)[0]
        assert decoded.message_type == "exception"
        assert decoded.exception_type == "ThrottlingException"
        assert decoded.event_type is None

    def test_error_frame_uses_error_code(self):
        frame = encode_frame(
            {":message-type": "error", ":error-code": "InternalFailure", ":error-message": "boom"},
            b"",
        )
        decoded = EventStreamDecoder().feed(frame)[0]
        assert decoded.message_type == "error"
        assert decoded.exception_type == "InternalFailure"
        assert decoded.headers[":error-message"] == "boom"

    def test_empty_payload_and_no_headers(self):
        frames = EventStreamDecoder().feed(encode_frame({}, b""))
        assert frames[0].headers == {}
        assert frames[0].payload == b""
        assert frames[0].message_type == "event"


class TestHeaderDecoding:
    def test_all_header_types(self):
        """
        What it does: Decodes one header of every AWS value type.
        Purpose: Non-string headers must not break framing.
        """
        def name(n: str) -> bytes:
            return bytes([len(n)]) + n.encode()

        raw = b"".join([
            name("t") + bytes([0]),
            name("f") + bytes([1]),
            name("b") + bytes([2]) + struct.pack(">b", -5),
            name("s") + bytes([3]) + struct.pack(">h", 300),
            name("i") + bytes([4]) + struct.pack(">i", 70000),
            name("l") + bytes([5]) + struct.pack(">q", 2 ** 40),
            name("ba") + bytes([6]) + struct.pack(">H", 3) + b"\x01\x02\x03",
            name("str") + bytes([7]) + struct.pack(">H", 2) + b"ok",
            name("ts") + bytes([8]) + struct.pack(">q", 1700000000000),
            name("u") + bytes([9]) + bytes(range(16)),
        ])
        headers = _decode_headers(raw)
        assert headers == {
            "t": True, "f": False, "b": -5, "s": 300, "i": 70000, "l": 2 ** 40,
            "ba": b"\x01\x02\x03", "str": "ok", "ts": 1700000000000, "u": bytes(range(16)).hex(),
        }

        # And inside a full frame
        frames = EventStreamDecoder().feed(_raw_frame(raw, b"{}"))
        assert frames[0].headers["str"] == "ok"

    def test_unknown_header_type_raises(self):
        with pytest.raises(EventStreamFormatError):
            _decode_headers(bytes([1]) + b"x" + bytes([42]))

    def test_truncated_header_raises(self):
        with pytest.raises(EventStreamFormatError):
            _decode_headers(bytes([3]) + b"abc" + bytes([7]) + struct.pack(">H", 10) + b"short")

    def test_truncated_fixed_width_raises(self):
        with pytest.raises(EventStreamFormatError):
            _decode_headers(bytes([1]) + b"i" + bytes([4]) + b"\x00")

    def test_non_string_message_type_defaults_to_event(self):
        frame = EventStreamFrame(headers={":message-type": True})
        assert frame.message_type == "event"
        assert frame.exception_type is None


def test_prelude_length_constant():
    assert PRELUDE_LENGTH == 12
    assert MIN_FRAME_LENGTH == 16
