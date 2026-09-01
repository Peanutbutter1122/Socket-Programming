"""
protocol.py - CMSP/1.0 (Crypto Market Streaming Protocol)

ไฟล์นี้เป็น "ภาษากลาง" ของทั้ง server และ client
ทั้งสองฝั่ง import จากไฟล์นี้ไฟล์เดียว ห้ามเขียนโค้ด parse/encode ซ้ำที่อื่น

ไฟล์นี้ใช้ stdlib ล้วน ไม่มี library ภายนอก และไม่แตะ socket เลย
(แยกหน้าที่ให้ชัด: ที่นี่รู้แค่ "รูปแบบข้อความ" ไม่รู้จัก "การส่ง")

รูปแบบข้อความ (ยืมแนวคิดจาก HTTP/1.1 = start line + headers + บรรทัดว่าง):

    <start line>CRLF
    <Key>: <Value>CRLF
    <Key>: <Value>CRLF
    CRLF                <-- บรรทัดว่างคือจุดจบของ message

    Request  start line:  "SUB CMSP/1.0"
    Response start line:  "CMSP/1.0 201 SUBSCRIBED"
"""

CMSP_VERSION = "CMSP/1.0"

# ขนาดสูงสุดต่อ 1 message (นับรวม delimiter ปิดท้าย) เกินแล้วตอบ 413 และปิด connection
MAX_MESSAGE_SIZE = 8192

# TCP เป็น byte stream ไม่มีขอบเขตข้อความ เราจึงต้องนิยามขอบเขตเอง = บรรทัดว่าง
DELIMITER = b"\r\n\r\n"

LINE_END = "\r\n"
ENCODING = "utf-8"

# ตาราง status code ทั้งหมดของ CMSP/1.0 (ครบตามสเปกหัวข้อ 6.6)
#   1xx = push จาก server โดยไม่มี request นำ
#   2xx = สำเร็จ
#   4xx = ความผิดฝั่ง client
#   5xx = ความผิดฝั่ง server / upstream
STATUS = {
    110: "DATA",
    111: "ALERT TRIGGERED",
    112: "SERVER NOTICE",
    200: "OK",
    201: "SUBSCRIBED",
    202: "UNSUBSCRIBED",
    203: "ALERT CREATED",
    204: "ALERT DELETED",
    205: "PONG",
    206: "GOODBYE",
    207: "STREAM PAUSED",
    208: "STREAM RESUMED",
    400: "BAD REQUEST",
    401: "UNAUTHORIZED",
    404: "SYMBOL NOT FOUND",
    405: "UNKNOWN COMMAND",
    409: "ALREADY SUBSCRIBED",
    410: "NOT SUBSCRIBED",
    411: "ALERT NOT FOUND",
    413: "MESSAGE TOO LARGE",
    426: "VERSION NOT SUPPORTED",
    429: "LIMIT EXCEEDED",
    500: "INTERNAL ERROR",
    503: "UPSTREAM UNAVAILABLE",
    504: "UPSTREAM TIMEOUT",
}

# คำสั่งที่ client ส่งได้ (server ใช้ตรวจว่าเป็น 405 UNKNOWN COMMAND หรือไม่)
COMMANDS = (
    "HELLO", "AUTH", "SUB", "UNSUB", "ALERT",
    "LIST", "STATS", "PAUSE", "RESUME", "PING", "QUIT",
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class CMSPError(Exception):
    """แม่ของ error ทุกตัวในโปรโตคอล มี .code ไว้แปลงเป็น status code ได้ตรงๆ"""

    code = 400

    def __init__(self, detail=""):
        super().__init__(detail or STATUS.get(self.code, ""))
        self.detail = detail


class MalformedMessage(CMSPError):
    """รูปแบบ message ผิด เช่น start line เพี้ยน หรือ header ไม่มี colon -> 400"""

    code = 400


class UnsupportedVersion(MalformedMessage):
    """version ไม่ใช่ CMSP/1.0 -> 426

    ทำเป็นลูกของ MalformedMessage เพื่อให้โค้ดที่ except MalformedMessage
    ยังจับได้ครบ แล้วค่อยใช้ .code แยกว่าจะตอบ 400 หรือ 426
    """

    code = 426


class MessageTooLarge(CMSPError):
    """message ยาวเกิน MAX_MESSAGE_SIZE -> 413 แล้วปิด connection"""

    code = 413


# ---------------------------------------------------------------------------
# Message
# ---------------------------------------------------------------------------

class Message:
    """ผลลัพธ์ของการ decode 1 frame

    เก็บ start line ดิบไว้ด้วย เพื่อให้ log ย้อนดูได้ตรงกับที่รับมาจริง
    """

    def __init__(self, start_line, headers=None):
        self.start_line = start_line
        # dict ธรรมดา รักษาลำดับ header ตามที่รับมา/ตามที่ใส่ตอน encode
        self.headers = dict(headers or {})

        # ฟิลด์ที่แยกมาจาก start line (ตัวใดไม่เกี่ยวจะเป็น None)
        self.kind = None      # "request" หรือ "response"
        self.command = None   # เช่น "SUB"            (เฉพาะ request)
        self.code = None      # เช่น 201              (เฉพาะ response)
        self.phrase = None    # เช่น "SUBSCRIBED"     (เฉพาะ response)
        self.version = None   # เช่น "CMSP/1.0"
        self._parse_start_line()

    # -- start line -------------------------------------------------------
    def _parse_start_line(self):
        parts = self.start_line.split()
        if not parts:
            raise MalformedMessage("empty start line")

        if parts[0].startswith("CMSP/"):
            # Response:  CMSP/1.0 <code> <PHRASE ...>
            self.kind = "response"
            self.version = parts[0]
            _check_version(self.version)
            if len(parts) < 2:
                raise MalformedMessage("response start line has no status code")
            try:
                self.code = int(parts[1])
            except ValueError:
                raise MalformedMessage("status code is not a number: %r" % parts[1])
            self.phrase = " ".join(parts[2:])
        else:
            # Request:  <COMMAND> CMSP/1.0
            self.kind = "request"
            self.command = parts[0].upper()
            if len(parts) < 2:
                raise MalformedMessage("request start line has no version")
            self.version = parts[1]
            # ตรวจ version ก่อนเรื่องอื่น: "FOOBAR CMSP/2.0" ต้องได้ 426 ไม่ใช่ 405
            _check_version(self.version)

    # -- header access ----------------------------------------------------
    def get(self, name, default=None):
        """อ่าน header แบบไม่สนตัวพิมพ์ใหญ่เล็ก (เหมือน HTTP)"""
        target = name.lower()
        for key, value in self.headers.items():
            if key.lower() == target:
                return value
        return default

    def has(self, name):
        return self.get(name) is not None

    def encode(self):
        """แปลงกลับเป็น bytes พร้อมส่ง (ใช้ในเทสต์ round-trip เป็นหลัก)"""
        return _build(self.start_line, self.headers)

    def __repr__(self):
        head = self.start_line
        if self.headers:
            head += " " + " ".join("%s=%s" % kv for kv in self.headers.items())
        return "<Message %s>" % head

    def __eq__(self, other):
        if not isinstance(other, Message):
            return NotImplemented
        return (self.start_line == other.start_line
                and self.headers == other.headers)


def _check_version(version):
    if version != CMSP_VERSION:
        raise UnsupportedVersion("expected %s, got %s" % (CMSP_VERSION, version))


# ---------------------------------------------------------------------------
# encode
# ---------------------------------------------------------------------------

def _header_name(name):
    """ทำให้เขียน encode_response(Client_Name="x") แล้วได้ header ชื่อ "Client-Name"

    (Python ไม่ยอมให้ keyword argument มีขีดกลาง จึงรับ underscore แทน
    ส่วนชื่อ dynamic เช่น Item-1 ให้ส่งเป็น **{"Item-1": ...} ได้ตรงๆ)
    """
    return name.replace("_", "-")


def _header_value(value):
    text = value if isinstance(value, str) else str(value)
    # ห้ามให้ค่ามี CR/LF เด็ดขาด ไม่งั้นจะปลอม header หรือปิด message กลางคันได้
    if "\r" in text or "\n" in text:
        raise ValueError("header value must not contain CR or LF: %r" % text)
    return text


def _build(start_line, headers):
    lines = [start_line]
    for name, value in headers.items():
        lines.append("%s: %s" % (_header_name(name), _header_value(value)))
    # ปิดท้ายด้วย CRLF ของบรรทัดสุดท้าย + CRLF ของบรรทัดว่าง = DELIMITER
    text = LINE_END.join(lines) + LINE_END + LINE_END
    return text.encode(ENCODING)


def encode_request(command, **headers):
    """สร้าง request frame เช่น  SUB CMSP/1.0 + Symbol: BTCUSDT + บรรทัดว่าง"""
    start_line = "%s %s" % (command.upper(), CMSP_VERSION)
    return _build(start_line, headers)


def encode_response(code, **headers):
    """สร้าง response/push frame เช่น  CMSP/1.0 201 SUBSCRIBED + headers + บรรทัดว่าง"""
    if code not in STATUS:
        raise ValueError("unknown status code: %r" % code)
    start_line = "%s %d %s" % (CMSP_VERSION, code, STATUS[code])
    return _build(start_line, headers)


def phrase(code):
    """แปลง code -> phrase เช่น 201 -> SUBSCRIBED (log ต้องมีทั้งเลขและคำเสมอ)"""
    return STATUS.get(code, "UNKNOWN")


def status_line(code):
    """ข้อความสำหรับ log เช่น "201 SUBSCRIBED" """
    return "%d %s" % (code, phrase(code))


# ---------------------------------------------------------------------------
# decode
# ---------------------------------------------------------------------------

def decode(raw):
    """แปลง 1 frame (bytes ที่ตัด delimiter ปิดท้ายออกแล้ว) -> Message

    raise MalformedMessage (400) / UnsupportedVersion (426) / MessageTooLarge (413)
    """
    if isinstance(raw, str):
        raw = raw.encode(ENCODING)
    if len(raw) + len(DELIMITER) > MAX_MESSAGE_SIZE:
        raise MessageTooLarge("frame is %d bytes" % len(raw))

    try:
        text = raw.decode(ENCODING)
    except UnicodeDecodeError as exc:
        raise MalformedMessage("payload is not valid UTF-8: %s" % exc)

    # ยอมรับทั้ง CRLF และ LF ตอนแยกบรรทัด เพื่อให้ทดสอบด้วย nc/telnet ได้สะดวก
    # (แต่ตอนส่งออกเราใช้ CRLF เสมอตามสเปก)
    lines = text.replace("\r\n", "\n").split("\n")
    while lines and lines[-1] == "":
        lines.pop()
    if not lines:
        raise MalformedMessage("empty message")

    headers = {}
    for line in lines[1:]:
        if line.strip() == "":
            continue
        if ":" not in line:
            raise MalformedMessage("header line has no colon: %r" % line)
        name, value = line.split(":", 1)
        name = name.strip()
        if not name:
            raise MalformedMessage("header line has empty name: %r" % line)
        headers[name] = value.strip()

    return Message(lines[0].strip(), headers)


# ---------------------------------------------------------------------------
# FrameBuffer - หัวใจของงานนี้
# ---------------------------------------------------------------------------

class FrameBuffer:
    """สะสม byte จาก recv() แล้วตัดออกมาเป็น message ทีละอัน

    ทำไมต้องมี: TCP ไม่รับประกันขอบเขตข้อความ
      - recv() ครั้งเดียวอาจได้ 3 message ติดกัน (client ส่งรัวๆ / segment ถูกรวม)
      - message เดียวอาจถูกแบ่งมาหลาย recv() (ถูกตัดตาม MSS หรือเน็ตหน่วง)
    FrameBuffer จัดการทั้งสองกรณีด้วย buffer เดียว: ต่อ chunk เข้าไปเรื่อยๆ
    แล้ววน split ตาม delimiter จนกว่าจะไม่เหลือ message ที่สมบูรณ์
    """

    def __init__(self, max_size=MAX_MESSAGE_SIZE):
        self.buffer = b""
        self.max_size = max_size

    def feed_raw(self, chunk=b""):
        """ต่อ chunk แล้วคืน "frame ดิบ" ที่สมบูรณ์ทั้งหมด (ยังไม่ decode)

        server/client ใช้ตัวนี้ เพราะจะได้ decode ทีละ frame ใน try/except ของตัวเอง
        แล้วตอบ 400 เฉพาะ frame ที่พัง โดย frame อื่นในชุดเดียวกันไม่หายไปด้วย
        """
        if chunk:
            self.buffer += chunk

        frames = []
        while DELIMITER in self.buffer:
            raw, self.buffer = self.buffer.split(DELIMITER, 1)
            if len(raw) + len(DELIMITER) > self.max_size:
                # message สมบูรณ์แต่ยาวเกินเพดาน -> 413 แล้วปิด connection
                self.buffer = b""
                raise MessageTooLarge("frame is %d bytes" % len(raw))
            frames.append(raw)

        # ยังไม่เจอจุดจบเลย แต่ buffer โตเกินเพดานแล้ว
        # ถ้าไม่ตัดตรงนี้ client ที่ไม่ยอมส่งบรรทัดว่างจะกิน memory ของ server ไปเรื่อยๆ
        if len(self.buffer) + len(DELIMITER) > self.max_size:
            self.buffer = b""
            raise MessageTooLarge("no delimiter within %d bytes" % self.max_size)

        return frames

    def feed(self, chunk=b""):
        """เหมือน feed_raw แต่ decode ให้เลย -> list[Message]

        สะดวกสำหรับเทสต์และโค้ดที่ไม่ต้องกู้ต่อจาก frame ที่พัง
        (ถ้า frame ใด decode ไม่ผ่านจะ raise ทันที)
        """
        return [decode(raw) for raw in self.feed_raw(chunk)]

    def pending(self):
        """จำนวน byte ที่ค้างอยู่ (ยังประกอบเป็น message ไม่ครบ) - ไว้ debug"""
        return len(self.buffer)

    def reset(self):
        self.buffer = b""


# ---------------------------------------------------------------------------
# helper สำหรับ log
# ---------------------------------------------------------------------------

def raw_repr(data, limit=512):
    """แสดง bytes ให้เห็น CRLF ชัดๆ สำหรับโหมด --verbose"""
    if len(data) > limit:
        return repr(data[:limit]) + ("... (%d bytes)" % len(data))
    return repr(data)


def summarize(msg):
    """ย่อ message ให้เหลือบรรทัดเดียวสำหรับ log ปกติ (ไม่ใช่โหมด verbose)"""
    if msg.kind == "response":
        head = "%d %s" % (msg.code, msg.phrase)
    else:
        head = msg.command
    tail = " ".join("%s=%s" % kv for kv in msg.headers.items())
    return (head + " " + tail).strip()
