"""
test_protocol.py - เทสต์ของเฟส 1

เน้นเรื่อง framing เป็นหลัก เพราะเป็นจุดที่พังแล้วพังทั้งโปรเจกต์:
  - 1 message สมบูรณ์
  - 2 message มาใน chunk เดียว
  - 1 message ถูกแบ่งมา 3 chunk
  - message ยาวเกิน 8192 bytes
  - header ที่ไม่มี colon
  - version ที่ไม่ใช่ CMSP/1.0

รันด้วย:  pytest -v   (จากโฟลเดอร์ cmsp/)
"""

import os
import sys

import pytest

# ให้ import protocol ได้แม้รัน pytest จากโฟลเดอร์ไหนก็ตาม
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import protocol
from protocol import (
    CMSP_VERSION,
    DELIMITER,
    MAX_MESSAGE_SIZE,
    STATUS,
    FrameBuffer,
    MalformedMessage,
    MessageTooLarge,
    UnsupportedVersion,
    decode,
    encode_request,
    encode_response,
)


# ---------------------------------------------------------------------------
# ตาราง status code
# ---------------------------------------------------------------------------

REQUIRED_CODES = [
    110, 111, 112,
    200, 201, 202, 203, 204, 205, 206, 207, 208,
    400, 401, 404, 405, 409, 410, 411, 413, 426, 429,
    500, 503, 504,
]


def test_status_table_complete():
    """status code ทุกตัวในสเปกหัวข้อ 6.6 ต้องมีอยู่จริงในตาราง"""
    for code in REQUIRED_CODES:
        assert code in STATUS, "ขาด status code %d" % code
    assert len(STATUS) == len(REQUIRED_CODES)


def test_status_phrases_match_spec():
    assert STATUS[110] == "DATA"
    assert STATUS[201] == "SUBSCRIBED"
    assert STATUS[208] == "STREAM RESUMED"
    assert STATUS[426] == "VERSION NOT SUPPORTED"
    assert protocol.status_line(201) == "201 SUBSCRIBED"
    assert protocol.phrase(111) == "ALERT TRIGGERED"


# ---------------------------------------------------------------------------
# encode
# ---------------------------------------------------------------------------

def test_encode_request_bytes_exactly():
    raw = encode_request("SUB", Symbol="BTCUSDT", Interval="1s")
    assert raw == b"SUB CMSP/1.0\r\nSymbol: BTCUSDT\r\nInterval: 1s\r\n\r\n"


def test_encode_request_with_no_headers_still_terminates():
    raw = encode_request("PING")
    assert raw == b"PING CMSP/1.0\r\n\r\n"
    assert raw.endswith(DELIMITER)


def test_encode_response_bytes_exactly():
    raw = encode_response(201, Symbol="BTCUSDT", Interval="1s", Active_Subs=2)
    assert raw == (b"CMSP/1.0 201 SUBSCRIBED\r\n"
                   b"Symbol: BTCUSDT\r\nInterval: 1s\r\nActive-Subs: 2\r\n\r\n")


def test_encode_underscore_becomes_hyphen():
    """keyword argument ใช้ขีดกลางไม่ได้ จึงเขียน Client_Name แล้วให้แปลงเป็น Client-Name"""
    raw = encode_request("HELLO", Client_Name="demo")
    assert b"Client-Name: demo" in raw


def test_encode_dynamic_item_headers_keep_order():
    """response แบบหลายรายการใช้ Count + Item-N เพื่อคงหลัก 1 message = 1 frame"""
    items = {"Item-1": "BTCUSDT 1s", "Item-2": "ETHUSDT 5s"}
    raw = encode_response(200, Type="SUB", Count=2, **items)
    msg = decode(raw[: -len(DELIMITER)])
    assert list(msg.headers) == ["Type", "Count", "Item-1", "Item-2"]
    assert msg.get("Item-2") == "ETHUSDT 5s"


def test_encode_unknown_status_code_rejected():
    with pytest.raises(ValueError):
        encode_response(299)


def test_encode_rejects_crlf_in_header_value():
    """กันไม่ให้ค่าที่มี CRLF ไปปลอม header หรือปิด message กลางคัน"""
    with pytest.raises(ValueError):
        encode_request("HELLO", Client_Name="evil\r\nSymbol: BTCUSDT")


# ---------------------------------------------------------------------------
# decode
# ---------------------------------------------------------------------------

def test_decode_request():
    msg = decode(b"SUB CMSP/1.0\r\nSymbol: BTCUSDT\r\nInterval: 5s")
    assert msg.kind == "request"
    assert msg.command == "SUB"
    assert msg.version == CMSP_VERSION
    assert msg.code is None
    assert msg.headers == {"Symbol": "BTCUSDT", "Interval": "5s"}


def test_decode_response_keeps_code_and_phrase():
    msg = decode(b"CMSP/1.0 111 ALERT TRIGGERED\r\nAlert-Id: 3")
    assert msg.kind == "response"
    assert msg.code == 111
    assert msg.phrase == "ALERT TRIGGERED"   # phrase มีหลายคำต้องไม่ขาด
    assert msg.get("Alert-Id") == "3"


def test_decode_header_lookup_is_case_insensitive():
    msg = decode(b"SUB CMSP/1.0\r\nsymbol: BTCUSDT")
    assert msg.get("Symbol") == "BTCUSDT"
    assert msg.get("SYMBOL") == "BTCUSDT"
    assert msg.get("Missing") is None
    assert msg.get("Missing", "x") == "x"
    assert msg.has("symbol") is True


def test_decode_strips_whitespace_around_value():
    msg = decode(b"SUB CMSP/1.0\r\nSymbol:    BTCUSDT   ")
    assert msg.get("Symbol") == "BTCUSDT"


def test_decode_value_may_contain_colon():
    """แยกที่ colon ตัวแรกเท่านั้น ค่าที่มี colon ต้องไม่ถูกตัด"""
    msg = decode(b"HELLO CMSP/1.0\r\nClient-Name: host:9009")
    assert msg.get("Client-Name") == "host:9009"


def test_decode_command_is_uppercased():
    assert decode(b"ping CMSP/1.0").command == "PING"


def test_decode_unknown_command_is_not_a_parse_error():
    """FOOBAR ผ่าน decode ได้ปกติ เพราะการตัดสินว่า 405 เป็นหน้าที่ของ server"""
    msg = decode(b"FOOBAR CMSP/1.0")
    assert msg.command == "FOOBAR"


def test_decode_tolerates_lf_only_line_endings():
    """เผื่อทดสอบด้วย nc/telnet ที่ส่ง LF ล้วน"""
    msg = decode(b"SUB CMSP/1.0\nSymbol: BTCUSDT")
    assert msg.get("Symbol") == "BTCUSDT"


def test_round_trip_encode_decode():
    raw = encode_response(110, Symbol="BTCUSDT", Price="63120.50", Seq=1043)
    msg = decode(raw[: -len(DELIMITER)])
    assert msg.encode() == raw


# -- decode: กรณีผิดรูปแบบ ---------------------------------------------------

def test_decode_header_without_colon_raises_400():
    with pytest.raises(MalformedMessage) as exc:
        decode(b"SUB CMSP/1.0\r\nSymbol BTCUSDT")
    assert exc.value.code == 400


def test_decode_header_with_empty_name_raises_400():
    with pytest.raises(MalformedMessage) as exc:
        decode(b"SUB CMSP/1.0\r\n: BTCUSDT")
    assert exc.value.code == 400


def test_decode_empty_message_raises_400():
    with pytest.raises(MalformedMessage):
        decode(b"")


def test_decode_start_line_without_version_raises_400():
    with pytest.raises(MalformedMessage) as exc:
        decode(b"SUB\r\nSymbol: BTCUSDT")
    assert exc.value.code == 400


def test_decode_response_without_code_raises_400():
    with pytest.raises(MalformedMessage):
        decode(b"CMSP/1.0")


def test_decode_response_with_non_numeric_code_raises_400():
    with pytest.raises(MalformedMessage):
        decode(b"CMSP/1.0 TWOHUNDRED OK")


def test_decode_invalid_utf8_raises_400():
    with pytest.raises(MalformedMessage):
        decode(b"SUB CMSP/1.0\r\nSymbol: \xff\xfe")


# -- decode: version ผิด -> 426 ---------------------------------------------

def test_decode_wrong_version_raises_426():
    with pytest.raises(UnsupportedVersion) as exc:
        decode(b"HELLO CMSP/2.0\r\nClient-Name: demo")
    assert exc.value.code == 426


def test_decode_wrong_version_on_response_raises_426():
    with pytest.raises(UnsupportedVersion):
        decode(b"CMSP/2.0 200 OK")


def test_unsupported_version_is_a_malformed_message():
    """except MalformedMessage ต้องจับ 426 ได้ด้วย แล้วค่อยแยกด้วย .code"""
    assert issubclass(UnsupportedVersion, MalformedMessage)


def test_version_checked_before_unknown_command():
    """FOOBAR CMSP/2.0 ต้องเป็น 426 ไม่ใช่ 405"""
    with pytest.raises(UnsupportedVersion):
        decode(b"FOOBAR CMSP/2.0")


# ---------------------------------------------------------------------------
# FrameBuffer - framing (หัวใจของงาน)
# ---------------------------------------------------------------------------

def test_single_complete_message_in_one_chunk():
    fb = FrameBuffer()
    msgs = fb.feed(encode_request("HELLO", Client_Name="demo"))
    assert len(msgs) == 1
    assert msgs[0].command == "HELLO"
    assert msgs[0].get("Client-Name") == "demo"
    assert fb.pending() == 0


def test_two_messages_in_one_chunk():
    """recv() ครั้งเดียวได้ 2 message ติดกัน ต้องแยกออกครบทั้งคู่"""
    chunk = encode_request("PING") + encode_request("STATS")
    msgs = FrameBuffer().feed(chunk)
    assert [m.command for m in msgs] == ["PING", "STATS"]


def test_three_messages_in_one_chunk_like_burst_mode():
    """เคสเดียวกับ client --burst: ยิง PING/STATS/LIST ติดกันโดยไม่รอ response"""
    chunk = (encode_request("PING")
             + encode_request("STATS")
             + encode_request("LIST", Type="SUB"))
    fb = FrameBuffer()
    msgs = fb.feed(chunk)
    assert [m.command for m in msgs] == ["PING", "STATS", "LIST"]
    assert msgs[2].get("Type") == "SUB"
    assert fb.pending() == 0


def test_one_message_split_across_three_chunks():
    """message เดียวถูกแบ่งมา 3 recv() ต้องไม่คืนอะไรจนกว่าจะครบ"""
    raw = encode_request("SUB", Symbol="BTCUSDT", Interval="1s")
    a, b, c = raw[:10], raw[10:20], raw[20:]
    fb = FrameBuffer()
    assert fb.feed(a) == []
    assert fb.pending() == len(a)
    assert fb.feed(b) == []
    msgs = fb.feed(c)
    assert len(msgs) == 1
    assert msgs[0].command == "SUB"
    assert msgs[0].get("Symbol") == "BTCUSDT"
    assert fb.pending() == 0


def test_delimiter_split_across_chunk_boundary():
    """เคสร้ายที่สุด: CRLF CRLF ถูกผ่ากลาง ต้องยังประกอบกลับได้"""
    raw = encode_request("PING")
    fb = FrameBuffer()
    assert fb.feed(raw[:-2]) == []      # เหลือแค่ CRLF สุดท้าย
    msgs = fb.feed(raw[-2:])
    assert len(msgs) == 1 and msgs[0].command == "PING"


def test_byte_by_byte_delivery():
    """ป้อนทีละ byte (จำลอง network ที่แย่ที่สุด) ต้องได้ message เดียวตอนจบ"""
    raw = encode_request("SUB", Symbol="ETHUSDT")
    fb = FrameBuffer()
    got = []
    for i in range(len(raw)):
        got.extend(fb.feed(raw[i:i + 1]))
    assert len(got) == 1
    assert got[0].get("Symbol") == "ETHUSDT"


def test_leftover_bytes_stay_in_buffer_for_next_recv():
    """message ที่ 2 มาไม่ครบ ต้องค้างใน buffer ไว้รอ chunk ถัดไป"""
    full = encode_request("PING")
    partial = encode_request("STATS")[:6]
    fb = FrameBuffer()
    msgs = fb.feed(full + partial)
    assert len(msgs) == 1
    assert fb.pending() == len(partial)
    msgs = fb.feed(encode_request("STATS")[6:])
    assert [m.command for m in msgs] == ["STATS"]


def test_feed_raw_returns_undecoded_frames():
    """server ใช้ feed_raw เพื่อ decode ทีละ frame ใน try/except ของตัวเอง"""
    fb = FrameBuffer()
    frames = fb.feed_raw(encode_request("PING") + encode_request("QUIT"))
    assert frames == [b"PING CMSP/1.0", b"QUIT CMSP/1.0"]


def test_feed_raw_keeps_good_frames_when_a_later_frame_is_bad():
    """frame พัง 1 อัน ต้องไม่ทำให้ frame ดีในชุดเดียวกันหายไปด้วย"""
    fb = FrameBuffer()
    frames = fb.feed_raw(encode_request("PING") + b"SUB CMSP/1.0\r\nBROKEN\r\n\r\n")
    assert len(frames) == 2
    assert decode(frames[0]).command == "PING"
    with pytest.raises(MalformedMessage):
        decode(frames[1])


def test_push_message_decoded_by_client_side_buffer():
    """ฝั่ง client ใช้ FrameBuffer ตัวเดียวกันอ่าน push ที่มาโดยไม่มี request นำ"""
    chunk = (encode_response(110, Symbol="BTCUSDT", Price="63120.50", Seq=1)
             + encode_response(111, Alert_Id=3, Symbol="BTCUSDT"))
    msgs = FrameBuffer().feed(chunk)
    assert [m.code for m in msgs] == [110, 111]
    assert msgs[0].get("Price") == "63120.50"


# -- FrameBuffer: ขนาดเกิน -> 413 -------------------------------------------

def test_complete_message_over_limit_raises_413():
    """ส่ง message ~10,000 bytes ที่ปิดท้ายครบ ก็ยังต้องเป็น 413"""
    big = b"HELLO CMSP/1.0\r\nClient-Name: " + b"A" * 10000 + DELIMITER
    fb = FrameBuffer()
    with pytest.raises(MessageTooLarge) as exc:
        fb.feed(big)
    assert exc.value.code == 413


def test_endless_message_without_delimiter_raises_413():
    """client ที่ไม่ยอมส่งบรรทัดว่าง ต้องโดนตัดที่เพดาน ไม่ปล่อยให้กิน memory"""
    fb = FrameBuffer()
    fb.feed(b"A" * 4000)          # ยังไม่เกิน ยังรอต่อได้
    with pytest.raises(MessageTooLarge):
        fb.feed(b"A" * 5000)


def test_message_exactly_at_limit_is_accepted():
    """ขอบพอดี 8192 bytes (รวม delimiter) ต้องผ่าน ไม่ใช่ 413"""
    head = b"HELLO CMSP/1.0\r\nClient-Name: "
    pad = MAX_MESSAGE_SIZE - len(head) - len(DELIMITER)
    raw = head + b"A" * pad + DELIMITER
    assert len(raw) == MAX_MESSAGE_SIZE
    msgs = FrameBuffer().feed(raw)
    assert len(msgs) == 1
    assert len(msgs[0].get("Client-Name")) == pad


def test_message_one_byte_over_limit_raises_413():
    head = b"HELLO CMSP/1.0\r\nClient-Name: "
    pad = MAX_MESSAGE_SIZE - len(head) - len(DELIMITER) + 1
    raw = head + b"A" * pad + DELIMITER
    assert len(raw) == MAX_MESSAGE_SIZE + 1
    with pytest.raises(MessageTooLarge):
        FrameBuffer().feed(raw)


def test_buffer_is_cleared_after_too_large_error():
    """หลัง 413 เราจะปิด connection อยู่แล้ว แต่ buffer ต้องไม่ค้างขยะไว้"""
    fb = FrameBuffer()
    with pytest.raises(MessageTooLarge):
        fb.feed(b"A" * (MAX_MESSAGE_SIZE + 10))
    assert fb.pending() == 0


def test_decode_rejects_oversized_frame_directly():
    with pytest.raises(MessageTooLarge):
        decode(b"A" * (MAX_MESSAGE_SIZE + 1))


def test_reset_clears_buffer():
    fb = FrameBuffer()
    fb.feed(b"partial")
    assert fb.pending() > 0
    fb.reset()
    assert fb.pending() == 0


# ---------------------------------------------------------------------------
# helper สำหรับ log
# ---------------------------------------------------------------------------

def test_raw_repr_shows_crlf_escapes():
    """โหมด --verbose ต้องเห็น \\r\\n ชัดเจน"""
    text = protocol.raw_repr(encode_request("PING"))
    assert text == r"b'PING CMSP/1.0\r\n\r\n'"


def test_raw_repr_truncates_long_payload():
    text = protocol.raw_repr(b"A" * 1000, limit=20)
    assert text.endswith("(1000 bytes)")


def test_summarize_request_and_response():
    req = decode(b"SUB CMSP/1.0\r\nSymbol: BTCUSDT\r\nInterval: 1s")
    assert protocol.summarize(req) == "SUB Symbol=BTCUSDT Interval=1s"
    res = decode(b"CMSP/1.0 201 SUBSCRIBED\r\nSymbol: BTCUSDT")
    assert protocol.summarize(res) == "201 SUBSCRIBED Symbol=BTCUSDT"
