"""
acceptance.py - รัน test matrix หัวข้อ 12 ของสเปกแบบอัตโนมัติ

สเปกขอไว้ว่าให้รันข้อ 1-9 และ 13-19 อัตโนมัติได้ ที่นี่ทำเพิ่มให้ครบ 19/20 ข้อ
(เหลือข้อ 12 --verbose ที่ต้องดูด้วยตาผ่าน console จริง)

วิธีรัน (จากโฟลเดอร์ cmsp/):
    python tests/acceptance.py              # เต็มรูปแบบ (มีช่วง pause 15 วินาทีตามสเปก)
    python tests/acceptance.py --quick      # ย่อช่วง pause เหลือ 6 วินาที
    python tests/acceptance.py --server-log # โชว์ log ของ server ไปด้วย

server ถูกรันในโปรเซสเดียวกัน (คนละเธรด) แต่ client ทุกตัวต่อผ่าน TCP socket จริง
จึงยังเป็นการทดสอบ path เดียวกับตอนรันแยกโปรเซสทุกประการ
"""

import argparse
import os
import socket
import struct
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import protocol
import server as server_module
from protocol import FrameBuffer, encode_request

QUIET = True
PAUSE_SECONDS = 15.0

# message ที่ server ส่งเองโดยไม่มี request นำ - ตอนรอ "response ของคำสั่ง" ต้องข้ามพวกนี้
PUSH_CODES = (110, 111, 112, 503, 504)

results = []


# ---------------------------------------------------------------------------
# helper: client จำลองที่คุยผ่าน socket จริง
# ---------------------------------------------------------------------------

class Peer:
    """client ขนาดจิ๋วสำหรับเทสต์ - อ่านแบบ blocking ทีละ message"""

    def __init__(self, port, host="127.0.0.1"):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((host, port))
        self.frames = FrameBuffer()
        self.inbox = []          # message ที่รับมาแล้วแต่ยังไม่ได้ใช้
        self.closed = False

    # -- ส่ง --
    def send_raw(self, raw):
        self.sock.sendall(raw)

    def send(self, command, **headers):
        self.sock.sendall(encode_request(command, **headers))

    # -- รับ --
    def _pump(self, timeout):
        self.sock.settimeout(timeout)
        try:
            chunk = self.sock.recv(4096)
        except socket.timeout:
            return False
        except OSError:
            self.closed = True
            return False
        if not chunk:
            self.closed = True
            return False
        for raw in self.frames.feed_raw(chunk):
            self.inbox.append(protocol.decode(raw))
        return True

    def next_message(self, codes=None, timeout=5.0, skip_push=True):
        """คืน message ตัวถัดไปที่ตรงเงื่อนไข (ข้าม push 110/112 ระหว่างทางได้)"""
        deadline = time.monotonic() + timeout
        while True:
            for index, msg in enumerate(self.inbox):
                if codes is not None and msg.code not in codes:
                    continue
                if codes is None and skip_push and msg.code in PUSH_CODES:
                    continue
                return self.inbox.pop(index)
            if time.monotonic() > deadline:
                return None
            if not self._pump(max(0.05, deadline - time.monotonic())):
                if self.closed:
                    return None

    def drain(self, seconds):
        """อ่านทิ้งไว้ในกล่องเฉยๆ ตามเวลาที่กำหนด"""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self._pump(min(0.3, max(0.05, deadline - time.monotonic())))

    def hello(self, name="tester"):
        self.send("HELLO", Client_Name=name)
        return self.next_message()

    def auth(self, user="student", token="1234"):
        self.send("AUTH", User=user, Token=token)
        return self.next_message()

    def ready(self, name="tester"):
        self.hello(name)
        self.auth()
        return self

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def kill(self):
        """ปิดแบบกระชากให้เกิด TCP RST เหมือนตอนผู้ใช้กด Ctrl+C ที่ client"""
        try:
            # SO_LINGER แบบ timeout 0 = ส่ง RST แทน FIN
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                 struct.pack("ii", 1, 0))
            self.sock.close()
        except OSError:
            pass


def start_server(**kwargs):
    """เปิด server ในเธรดของตัวเองบน port ที่ OS เลือกให้ (กันชนกับของจริง)"""
    options = dict(host="127.0.0.1", port=0, mock=True, verbose=False)
    options.update(kwargs)
    srv = server_module.Server(**options)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    for _ in range(100):
        if srv.running and srv.port:
            break
        time.sleep(0.05)
    else:
        raise RuntimeError("server did not start")
    return srv


def force_price(srv, symbol, price):
    """ตั้งราคาตั้งต้นของ mock feed ใหม่ ให้ tick ถัดไปทะลุ threshold แน่นอน

    random walk เดินแบบสุ่ม (sigma 0.08% ต่อ tick) จึงมีโอกาสราว 10% ที่ราคาจะไม่
    วิ่งขึ้นไปแตะ threshold ภายในเวลาที่เทสต์รอ ทำให้เทสต์ alert ตกทั้งที่โค้ดถูก
    ที่นี่จึงตั้งราคาให้ตรงๆ แล้วปล่อยให้ tick ไหลผ่าน path เดิมทุกประการ
    (feed -> Server.on_tick -> alert engine) ไม่ได้ข้ามขั้นตอนไหนของ server
    """
    srv.feed.set_price(symbol, price)


# ---------------------------------------------------------------------------
# helper: บันทึกผล
# ---------------------------------------------------------------------------

def check(number, title, condition, detail=""):
    results.append((number, title, bool(condition), detail))
    mark = "PASS" if condition else "FAIL"
    line = "  [%s] %2d. %s" % (mark, number, title)
    if detail:
        line += "  -> %s" % detail
    print(line, flush=True)
    return bool(condition)


def code_of(msg):
    return None if msg is None else msg.code


# ---------------------------------------------------------------------------
# test cases
# ---------------------------------------------------------------------------

def case_01_happy_path(srv):
    peer = Peer(srv.port)
    hello = peer.hello("demo")
    auth = peer.auth()
    peer.send("SUB", Symbol="BTCUSDT", Interval="1s")
    sub = peer.next_message()
    data = peer.next_message(codes=(110,), timeout=5.0)
    data2 = peer.next_message(codes=(110,), timeout=5.0)
    ok = (code_of(hello) == 200 and code_of(auth) == 200 and code_of(sub) == 201
          and data is not None and data2 is not None)
    detail = "200/%s 200/%s 201/%s แล้วได้ 110 ต่อเนื่อง (Seq=%s,%s)" % (
        code_of(hello), code_of(auth), code_of(sub),
        data.get("Seq") if data else "-", data2.get("Seq") if data2 else "-")
    check(1, "HELLO -> AUTH -> SUB -> รับราคา", ok, detail)
    peer.close()


def case_02_three_clients(srv):
    a, b, c = Peer(srv.port).ready("a"), Peer(srv.port).ready("b"), Peer(srv.port).ready("c")
    a.send("SUB", Symbol="BTCUSDT", Interval="1s")
    b.send("SUB", Symbol="BTCUSDT", Interval="1s")
    c.send("SUB", Symbol="ETHUSDT", Interval="1s")
    codes = [code_of(a.next_message()), code_of(b.next_message()), code_of(c.next_message())]
    refcount = dict(srv.upstream_refcount)
    got = [p.next_message(codes=(110,), timeout=6.0) for p in (a, b, c)]
    symbols_ok = (got[0] is not None and got[0].get("Symbol") == "BTCUSDT"
                  and got[1] is not None and got[1].get("Symbol") == "BTCUSDT"
                  and got[2] is not None and got[2].get("Symbol") == "ETHUSDT")
    ok = codes == [201, 201, 201] and refcount.get("BTCUSDT") == 2 \
        and refcount.get("ETHUSDT") == 1 and symbols_ok
    check(2, "3 client sub ทั้งซ้ำและต่าง symbol", ok,
          "refcount BTCUSDT=%s ETHUSDT=%s ทุกตัวได้ราคาถูก symbol" % (
              refcount.get("BTCUSDT"), refcount.get("ETHUSDT")))

    # ปิดทีละตัวเพื่อดูว่า refcount ลดถูกและกลับเป็น 0 (ไม่ leak)
    a.close()
    b.close()
    c.close()
    time.sleep(0.6)
    check(21, "ปิด client ทุกตัวแล้ว refcount กลับเป็น 0 (ไม่ leak)",
          not srv.upstream_refcount, "refcount=%s" % (srv.upstream_refcount or {}))


def case_03_alert_one_shot(srv):
    peer = Peer(srv.port).ready("alert")
    peer.send("SUB", Symbol="BTCUSDT", Interval="1s")
    peer.next_message()
    tick = peer.next_message(codes=(110,), timeout=6.0)
    price = float(tick.get("Price"))
    # ตั้งไว้เหนือราคาปัจจุบันนิดเดียว ให้ random walk วิ่งชนได้ในไม่กี่วินาที
    threshold = price * 1.0002
    peer.send("ALERT", Action="SET", Symbol="BTCUSDT", Condition="ABOVE",
              Value="%.2f" % threshold)
    created = peer.next_message()
    first = peer.next_message(codes=(111,), timeout=8.0)
    if first is None:
        # random walk ยังไม่วิ่งชนเอง - ดันราคาให้ทะลุ ผลเทสต์จะได้ไม่ขึ้นกับดวง
        force_price(srv, "BTCUSDT", threshold * 1.01)
        first = peer.next_message(codes=(111,), timeout=8.0)
    second = peer.next_message(codes=(111,), timeout=4.0)
    ok = code_of(created) == 203 and first is not None and second is None
    check(3, "alert ใกล้ราคาปัจจุบัน ยิง 111 ครั้งเดียว ไม่ยิงซ้ำ", ok,
          "threshold=%.2f trigger ที่ %s, ไม่มีครั้งที่สอง=%s" % (
              threshold, first.get("Price") if first else "-", second is None))

    # ยังอยู่ในรายการหลัง trigger และสถานะเปลี่ยนเป็น TRIGGERED
    peer.send("LIST", Type="ALERT")
    listed = peer.next_message()
    check(22, "alert ที่ยิงแล้วยังอยู่ในรายการ สถานะ TRIGGERED",
          listed is not None and "TRIGGERED" in (listed.get("Item-1") or ""),
          listed.get("Item-1") if listed else "-")
    peer.close()


def case_04_already_subscribed(srv):
    peer = Peer(srv.port).ready("dup")
    peer.send("SUB", Symbol="BTCUSDT")
    peer.next_message()
    peer.send("SUB", Symbol="BTCUSDT")
    again = peer.next_message()
    check(4, "SUB symbol ที่ sub อยู่แล้ว", code_of(again) == 409,
          "ได้ %s" % protocol.status_line(code_of(again) or 0))
    peer.close()


def case_05_symbol_not_found(srv):
    peer = Peer(srv.port).ready("nf")
    peer.send("SUB", Symbol="DOGE123")
    msg = peer.next_message()
    check(5, "SUB DOGE123 (รูปแบบถูกแต่ไม่มีบน upstream)", code_of(msg) == 404,
          "ได้ %s" % protocol.status_line(code_of(msg) or 0))
    peer.close()


def case_06_sub_before_auth(srv):
    peer = Peer(srv.port)
    peer.hello("noauth")
    peer.send("SUB", Symbol="BTCUSDT")
    msg = peer.next_message()
    check(6, "SUB ก่อน AUTH", code_of(msg) == 401,
          "ได้ %s" % protocol.status_line(code_of(msg) or 0))
    peer.close()


def case_07_unknown_command(srv):
    peer = Peer(srv.port).ready("unk")
    peer.send_raw(b"FOOBAR CMSP/1.0\r\n\r\n")
    msg = peer.next_message()
    check(7, "ส่ง FOOBAR CMSP/1.0", code_of(msg) == 405,
          "ได้ %s" % protocol.status_line(code_of(msg) or 0))
    peer.close()


def case_08_sub_limit(srv):
    peer = Peer(srv.port).ready("limit")
    for symbol in ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"):
        peer.send("SUB", Symbol=symbol, Interval="10s")
        peer.next_message()
    peer.send("SUB", Symbol="AAAUSDT")
    msg = peer.next_message()
    ok = code_of(msg) == 429 and "subscription" in (msg.get("Detail") or "")
    check(8, "SUB ตัวที่ 6 เกิน limit", ok,
          "ได้ %s Detail=%r" % (protocol.status_line(code_of(msg) or 0),
                                msg.get("Detail") if msg else None))
    peer.close()
    time.sleep(0.4)


def case_09_message_too_large(srv):
    peer = Peer(srv.port).ready("big")
    peer.send_raw(b"HELLO CMSP/1.0\r\nClient-Name: " + b"A" * 10000 + b"\r\n\r\n")
    msg = peer.next_message(timeout=3.0)
    closed = peer.next_message(timeout=2.0) is None and peer.closed
    check(9, "ส่ง message 10,000 bytes", code_of(msg) == 413 and closed,
          "ได้ %s แล้ว server ปิด connection=%s" % (
              protocol.status_line(code_of(msg) or 0), closed))
    peer.close()


def case_10_abrupt_disconnect(srv):
    peer = Peer(srv.port).ready("crash")
    peer.send("SUB", Symbol="SOLUSDT")
    peer.next_message()
    before = srv.upstream_refcount.get("SOLUSDT")
    peer.kill()                       # เหมือนกด Ctrl+C ที่ client (ได้ RST ไม่ใช่ FIN)
    time.sleep(1.0)
    alive = Peer(srv.port).ready("after-crash")
    alive.send("PING")
    pong = alive.next_message()
    ok = before == 1 and "SOLUSDT" not in srv.upstream_refcount and code_of(pong) == 205
    check(10, "client ตายกะทันหัน server ไม่ตาย + cleanup refcount", ok,
          "refcount SOLUSDT %s -> %s, server ยังตอบ PING เป็น %s" % (
              before, srv.upstream_refcount.get("SOLUSDT"),
              protocol.status_line(code_of(pong) or 0)))
    alive.close()


def case_11_upstream_failure():
    """ใช้ server อีกตัวที่เปิดด้วย --mock --fail-after เพื่อไม่ให้กระทบเทสต์อื่น"""
    srv = start_server(fail_after=3.0)
    peer = Peer(srv.port).ready("fail")
    peer.send("SUB", Symbol="BTCUSDT")
    peer.next_message()

    notice = peer.next_message(codes=(112,), timeout=10.0)
    unavailable = peer.next_message(codes=(503,), timeout=5.0)
    timeout_notice = peer.next_message(codes=(504,), timeout=8.0)
    back = peer.next_message(codes=(112,), timeout=10.0)
    resumed = peer.next_message(codes=(110,), timeout=8.0)
    ok = (notice is not None and unavailable is not None and back is not None
          and resumed is not None)
    check(11, "--mock --fail-after: 112 -> 503 -> 504 -> reconnect", ok,
          "112=%s 503=%s 504=%s กลับมา=%s แล้วราคาไหลต่อ=%s" % (
              notice is not None, unavailable is not None,
              timeout_notice is not None, back is not None, resumed is not None))
    peer.close()
    srv.shutdown()


def case_13_pause_resume(srv):
    peer = Peer(srv.port).ready("pause")
    peer.send("SUB", Symbol="BTCUSDT", Interval="1s")
    peer.next_message()
    peer.next_message(codes=(110,), timeout=5.0)

    peer.send("PAUSE")
    paused = peer.next_message()
    peer.inbox.clear()
    peer.drain(PAUSE_SECONDS)
    silent = [m for m in peer.inbox if m.code == 110]

    peer.send("RESUME")
    resumed = peer.next_message()
    missed = int(resumed.get("Missed-Ticks", -1)) if resumed else -1
    lower, upper = PAUSE_SECONDS - 3, PAUSE_SECONDS + 3
    ok = (code_of(paused) == 207 and code_of(resumed) == 208
          and not silent and lower <= missed <= upper)
    check(13, "pause %ds แล้ว resume" % PAUSE_SECONDS, ok,
          "207/%s, ระหว่าง pause ได้ 110 จำนวน %d, 208 Missed-Ticks=%d (คาด ~%d)" % (
              code_of(paused), len(silent), missed, PAUSE_SECONDS))

    after = peer.next_message(codes=(110,), timeout=5.0)
    check(23, "หลัง resume ราคาไหลต่อ", after is not None,
          "ได้ 110 อีกครั้ง Seq=%s" % (after.get("Seq") if after else "-"))
    peer.close()


def case_13b_alert_during_pause(srv):
    peer = Peer(srv.port).ready("pause-alert")
    peer.send("SUB", Symbol="ETHUSDT", Interval="1s")
    peer.next_message()
    tick = peer.next_message(codes=(110,), timeout=6.0)
    price = float(tick.get("Price"))
    peer.send("PAUSE")
    peer.next_message()
    peer.send("ALERT", Action="SET", Symbol="ETHUSDT", Condition="ABOVE",
              Value="%.4f" % (price * 1.0002))
    peer.next_message()
    fired = peer.next_message(codes=(111,), timeout=8.0)
    if fired is None:
        force_price(srv, "ETHUSDT", price * 1.01)
        fired = peer.next_message(codes=(111,), timeout=8.0)
    pushed = [m for m in peer.inbox if m.code == 110]
    check(24, "ระหว่าง pause ยังได้รับ 111 แต่ไม่ได้รับ 110",
          fired is not None and not pushed,
          "111=%s, 110 ที่หลุดมา=%d" % (fired is not None, len(pushed)))
    peer.close()


def case_14_alert_not_found(srv):
    peer = Peer(srv.port).ready("delalert")
    peer.send("ALERT", Action="DEL", Alert_Id="99")
    msg = peer.next_message()
    check(14, "alert del 99 (ไม่มีอยู่จริง)", code_of(msg) == 411,
          "ได้ %s" % protocol.status_line(code_of(msg) or 0))
    peer.close()


def case_15_sub_before_hello(srv):
    peer = Peer(srv.port)
    peer.send("SUB", Symbol="BTCUSDT")
    msg = peer.next_message()
    check(15, "ส่ง SUB ก่อน HELLO", code_of(msg) == 400,
          "ได้ %s" % protocol.status_line(code_of(msg) or 0))
    peer.close()


def case_16_bad_interval(srv):
    peer = Peer(srv.port).ready("interval")
    peer.send("SUB", Symbol="BTCUSDT", Interval="3s")
    msg = peer.next_message()
    check(16, "Interval: 3s (ค่าที่ไม่รองรับ)", code_of(msg) == 400,
          "ได้ %s Detail=%r" % (protocol.status_line(code_of(msg) or 0),
                                msg.get("Detail") if msg else None))
    peer.close()


def case_17_resume_without_pause(srv):
    peer = Peer(srv.port).ready("resume")
    peer.send("SUB", Symbol="BTCUSDT")
    peer.next_message()
    peer.send("RESUME")
    msg = peer.next_message()
    check(17, "resume ตอนไม่ได้ pause", code_of(msg) == 400,
          "ได้ %s" % protocol.status_line(code_of(msg) or 0))
    peer.close()


def case_18_pause_without_sub(srv):
    peer = Peer(srv.port).ready("nosub")
    peer.send("PAUSE")
    msg = peer.next_message()
    check(18, "pause ตอนไม่มี subscription", code_of(msg) == 410,
          "ได้ %s" % protocol.status_line(code_of(msg) or 0))
    peer.close()


def case_19_wrong_version(srv):
    peer = Peer(srv.port)
    peer.send_raw(b"HELLO CMSP/2.0\r\nClient-Name: old\r\n\r\n")
    msg = peer.next_message()
    check(19, "ส่ง HELLO CMSP/2.0", code_of(msg) == 426,
          "ได้ %s" % protocol.status_line(code_of(msg) or 0))
    peer.close()


def case_20_burst(srv):
    """ยิง 3 message ใน sendall() ครั้งเดียว = 1 TCP segment ที่มีหลาย CMSP message"""
    peer = Peer(srv.port).ready("burst")
    raw = (encode_request("PING")
           + encode_request("STATS")
           + encode_request("LIST", Type="SUB"))
    peer.send_raw(raw)
    first = peer.next_message()
    second = peer.next_message()
    third = peer.next_message()
    codes = [code_of(first), code_of(second), code_of(third)]
    ok = codes == [205, 200, 200] and third.get("Type") == "SUB"
    check(20, "--burst: 1 segment มีหลาย message แต่ server แยกได้ครบ", ok,
          "ส่ง %d bytes ครั้งเดียว ได้ตอบกลับ %s" % (len(raw), codes))
    peer.close()


def case_extra_errors(srv):
    """เก็บ status code ที่เหลือให้ครบตามสเปกหัวข้อ 14 (ทุก code ต้องมีทางไปถึงจริง)"""
    peer = Peer(srv.port).ready("misc")

    peer.send_raw(b"SUB CMSP/1.0\r\nSymbol BTCUSDT\r\n\r\n")   # header ไม่มี colon
    bad = peer.next_message()
    check(25, "header ไม่มี colon -> 400", code_of(bad) == 400,
          "ได้ %s" % protocol.status_line(code_of(bad) or 0))

    peer.send("SUB", Symbol="btc")                              # รูปแบบ symbol ผิด
    badsym = peer.next_message()
    check(26, "symbol ผิดรูปแบบ -> 400", code_of(badsym) == 400,
          "ได้ %s" % protocol.status_line(code_of(badsym) or 0))

    peer.send("AUTH", User="somchai", Token="wrong")            # token ผิด
    unauth = peer.next_message()
    check(27, "AUTH ด้วย token ผิด -> 401", code_of(unauth) == 401,
          "ได้ %s" % protocol.status_line(code_of(unauth) or 0))

    peer.send("UNSUB", Symbol="XRPUSDT")                        # ยังไม่ได้ sub
    notsub = peer.next_message()
    check(28, "UNSUB สิ่งที่ไม่ได้ sub -> 410", code_of(notsub) == 410,
          "ได้ %s" % protocol.status_line(code_of(notsub) or 0))

    my_alert_id = None
    for index in range(10):                                     # ตั้ง alert จนเต็ม limit
        peer.send("ALERT", Action="SET", Symbol="BTCUSDT",
                  Condition="BELOW", Value=str(1 + index))
        created = peer.next_message()
        if my_alert_id is None and created is not None:
            # Alert-Id เดินต่อเนื่องทั้ง server ไม่ได้เริ่มที่ 1 ของแต่ละ client
            # จึงต้องจำ id ที่ server ออกให้จริง ไม่ใช่เดาเอง
            my_alert_id = created.get("Alert-Id")
    peer.send("ALERT", Action="SET", Symbol="BTCUSDT", Condition="BELOW", Value="1")
    over = peer.next_message()
    ok = code_of(over) == 429 and "alert" in (over.get("Detail") or "")
    check(29, "ตั้ง alert เกิน 10 -> 429", ok,
          "ได้ %s Detail=%r" % (protocol.status_line(code_of(over) or 0),
                                over.get("Detail") if over else None))

    peer.send("ALERT", Action="DEL", Alert_Id=my_alert_id)      # ลบของตัวเองได้
    deleted = peer.next_message()
    check(30, "ลบ alert ของตัวเอง -> 204", code_of(deleted) == 204,
          "ได้ %s" % protocol.status_line(code_of(deleted) or 0))

    peer.send("STATS")                                          # 200 + ตัวเลขสถิติ
    stats = peer.next_message()
    ok = code_of(stats) == 200 and stats.has("Uptime") and stats.has("Messages-Sent")
    check(31, "STATS -> 200 พร้อม Uptime/Clients/Upstream-Symbols/Messages-Sent", ok,
          "%s" % (" ".join("%s=%s" % kv for kv in stats.headers.items()) if stats else "-"))

    peer.send("QUIT")                                           # 206 แล้วปิดสาย
    bye = peer.next_message()
    time.sleep(0.3)
    check(32, "QUIT -> 206 GOODBYE แล้ว server ปิด connection", code_of(bye) == 206,
          "ได้ %s" % protocol.status_line(code_of(bye) or 0))
    peer.close()


def case_response_before_push(srv):
    """SUB ตอนราคาไหลอยู่แล้ว: 201 ต้องถึง client ก่อน 110 เสมอ

    เป็นเคส race ระหว่าง reader thread (ตอบ 201) กับ feed thread (push 110)
    server แก้ด้วยการถือ send_lock คร่อมช่วง "ลงทะเบียน -> ตอบ 201"
    """
    warm = Peer(srv.port).ready("warm")
    warm.send("SUB", Symbol="BTCUSDT", Interval="1s")
    warm.next_message()

    rounds, bad = 20, 0
    for index in range(rounds):
        peer = Peer(srv.port).ready("racer%d" % index)
        peer.send("SUB", Symbol="BTCUSDT", Interval="1s")
        first = peer.next_message(timeout=5.0, skip_push=False)
        if first is None or first.code != 201:
            bad += 1
        peer.close()
    warm.close()
    check(34, "SUB ตอนราคาไหลอยู่แล้ว: 201 มาก่อน 110 เสมอ", bad == 0,
          "message แรกเป็น 201 ครบ %d/%d รอบ" % (rounds - bad, rounds))


def case_throttle(srv):
    """เกณฑ์ผ่านเฟส 3: interval 5s ต้อง push ถี่น้อยกว่า interval 1s จริง"""
    fast = Peer(srv.port).ready("fast")
    slow = Peer(srv.port).ready("slow")
    fast.send("SUB", Symbol="XRPUSDT", Interval="1s")
    fast.next_message()
    slow.send("SUB", Symbol="XRPUSDT", Interval="5s")
    slow.next_message()
    fast.inbox.clear()
    slow.inbox.clear()

    window = 10.0
    deadline = time.monotonic() + window
    while time.monotonic() < deadline:
        fast._pump(0.2)
        slow._pump(0.2)
    fast_count = len([m for m in fast.inbox if m.code == 110])
    slow_count = len([m for m in slow.inbox if m.code == 110])
    ok = fast_count >= 8 and slow_count <= 3 and fast_count > slow_count
    check(33, "throttle: interval 1s vs 5s ใน %.0f วินาที" % window, ok,
          "1s ได้ %d ครั้ง, 5s ได้ %d ครั้ง" % (fast_count, slow_count))
    fast.close()
    slow.close()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main(argv=None):
    global PAUSE_SECONDS

    parser = argparse.ArgumentParser(description="CMSP acceptance test (สเปกหัวข้อ 12)")
    parser.add_argument("--quick", action="store_true",
                        help="ย่อช่วง pause จาก 15 วินาทีเหลือ 6 วินาที")
    parser.add_argument("--server-log", action="store_true",
                        help="โชว์ log ของ server ไปด้วย (ค่าตั้งต้นคือซ่อนไว้ให้อ่านผลง่าย)")
    args = parser.parse_args(argv)

    if args.quick:
        PAUSE_SECONDS = 6.0
    if not args.server_log:
        server_module.log = lambda text: None

    print("=" * 78)
    print("CMSP acceptance test - test matrix หัวข้อ 12 (mock mode, ไม่ต้องต่อเน็ต)")
    print("=" * 78)

    srv = start_server()
    started = time.monotonic()
    try:
        case_01_happy_path(srv)
        case_02_three_clients(srv)
        case_03_alert_one_shot(srv)
        case_04_already_subscribed(srv)
        case_05_symbol_not_found(srv)
        case_06_sub_before_auth(srv)
        case_07_unknown_command(srv)
        case_08_sub_limit(srv)
        case_09_message_too_large(srv)
        case_10_abrupt_disconnect(srv)
        case_11_upstream_failure()
        case_13_pause_resume(srv)
        case_13b_alert_during_pause(srv)
        case_14_alert_not_found(srv)
        case_15_sub_before_hello(srv)
        case_16_bad_interval(srv)
        case_17_resume_without_pause(srv)
        case_18_pause_without_sub(srv)
        case_19_wrong_version(srv)
        case_20_burst(srv)
        case_extra_errors(srv)
        case_response_before_push(srv)
        case_throttle(srv)
    finally:
        srv.shutdown()

    passed = sum(1 for _, _, ok, _ in results if ok)
    failed = [r for r in results if not r[2]]
    print("-" * 78)
    print("ผ่าน %d / %d ข้อ  (ใช้เวลา %.1f วินาที)"
          % (passed, len(results), time.monotonic() - started))
    if failed:
        print("ข้อที่ไม่ผ่าน: %s" % ", ".join(str(r[0]) for r in failed))
    print("หมายเหตุ: ข้อ 12 (--verbose โชว์ raw bytes) ต้องดูด้วยตาบน console จริง")
    print("=" * 78)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
