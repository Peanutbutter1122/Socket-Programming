"""
server.py - CMSP/1.0 server บน raw TCP socket

ใช้ stdlib ล้วน: socket, threading, argparse, json, re, sys, time
ไม่มี framework ไม่มี socketserver ไม่มี asyncio - เรียก socket API เองทุกขั้นตอน:
    socket() -> setsockopt() -> bind() -> listen() -> accept() -> recv()/sendall() -> close()

Thread model (สเปกหัวข้อ 7.1)
    main thread          : accept loop รับ connection ใหม่
    client reader thread : 1 เธรดต่อ 1 client  (recv + framing + ตอบ response)
    feed thread          : รับ tick จาก upstream/mock -> เพิ่ม Seq -> dispatch
    watchdog thread      : เฝ้าดูว่า upstream เงียบเกิน timeout หรือไม่ (504)

หมายเหตุ: สเปกอนุญาตให้รวม feed กับ dispatcher เป็นเธรดเดียวได้ถ้าเขียนอธิบายไว้
ที่นี่รวมไว้จริง - on_tick() ที่ feed thread เรียก ทำหน้าที่ dispatcher ต่อในเธรดเดียวกันเลย
เหตุผล: ถ้าแยกอีกเธรดต้องมี queue คั่น ซึ่งเพิ่มความซับซ้อนโดยไม่ได้แก้ปัญหาจริง
(client ที่ช้าก็ยังบล็อก dispatcher อยู่ดี) ทางแก้ที่ถูกต้องคือ queue ต่อ client
ซึ่งเกินขอบเขตงานนี้ จึงเลือกทางที่ตรงไปตรงมาและอธิบายง่ายที่สุด
"""

import argparse
import json
import os
import re
import socket
import sys
import threading
import time
import traceback

import protocol
from protocol import (
    DELIMITER,
    MAX_MESSAGE_SIZE,
    FrameBuffer,
    MalformedMessage,
    MessageTooLarge,
    encode_response,
)

# ให้ log ที่มีลูกศร -> <- พิมพ์ได้บน console ภาษาไทยของ Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:      # pragma: no cover - console บางตัวไม่รองรับ
    pass

# ค่าที่ยอมรับตามสเปกหัวข้อ 6.7
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{5,12}$")
INTERVALS = {"1s": 1.0, "5s": 5.0, "10s": 10.0}
DEFAULT_INTERVAL = "1s"
CONDITIONS = ("ABOVE", "BELOW")

RECV_SIZE = 4096
USERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.json")


# ---------------------------------------------------------------------------
# logging
# ---------------------------------------------------------------------------

_print_lock = threading.Lock()


def now_hms():
    """เวลาแบบ HH:MM:SS.mmm ตามสเปกหัวข้อ 10"""
    t = time.time()
    return time.strftime("%H:%M:%S", time.localtime(t)) + ".%03d" % int((t % 1) * 1000)


def log(text):
    # ล็อกตอน print เพราะหลายเธรดเขียน stdout พร้อมกัน ถ้าไม่ล็อกบรรทัดจะปนกัน
    with _print_lock:
        print("[SERVER] %s  %s" % (now_hms(), text), flush=True)


def format_price(price):
    """เหรียญราคาสูงใช้ทศนิยมน้อย เหรียญราคาต่ำ (เช่น XRP) ต้องใช้ทศนิยมเยอะ"""
    if price >= 100:
        return "%.2f" % price
    if price >= 1:
        return "%.4f" % price
    return "%.6f" % price


# ---------------------------------------------------------------------------
# state objects
# ---------------------------------------------------------------------------

class AlertRule:
    """กฎ alert แบบ one-shot: ยิงครั้งเดียวแล้วเปลี่ยนเป็น TRIGGERED ไม่ยิงซ้ำ
    แต่ยังอยู่ในรายการจนกว่าเจ้าของจะสั่ง ALERT Action=DEL
    """

    def __init__(self, alert_id, conn_id, symbol, condition, value):
        self.alert_id = alert_id
        self.conn_id = conn_id
        self.symbol = symbol
        self.condition = condition
        self.value = value
        self.status = "ARMED"

    def matches(self, price):
        if self.condition == "ABOVE":
            return price >= self.value
        return price <= self.value

    def describe(self):
        return "id=%d %s %s %s status=%s" % (
            self.alert_id, self.symbol, self.condition,
            format_price(self.value), self.status)


class ClientSession:
    """สถานะของ client หนึ่งตัว (สเปกหัวข้อ 7.3)"""

    def __init__(self, conn, addr, conn_id):
        self.conn = conn
        self.addr = addr
        self.conn_id = conn_id
        # log ใช้เลข port ของ client เพราะอ่านง่ายและตรงกับที่เห็นใน netstat/Wireshark
        # ส่วน key ใน dict ใช้เลขรันนิ่ง เพื่อกันชนกันถ้ามี client จากคนละเครื่องได้ port เดียวกัน
        self.label = str(addr[1])

        self.state = "NEW"        # NEW -> GREETED -> READY -> STREAMING <-> PAUSED -> CLOSED
        self.name = None
        self.username = None

        # ล็อกต่อ client 1 ตัว: reader thread (ตอบ response) กับ feed thread (push ราคา)
        # เขียนลง socket เดียวกัน ถ้าไม่ล็อก byte ของสอง message จะปนกันจนพัง frame
        # ใช้ RLock เพราะบางคำสั่ง (SUB / RESUME) ถือล็อกนี้คร่อมหลายขั้นตอน
        # เพื่อคุมลำดับ message แล้วข้างในยังเรียก _write() ซึ่งถือล็อกตัวเดิมอีกชั้น
        self.send_lock = threading.RLock()

        self.subs = {}            # symbol -> interval string
        self.last_push = {}       # symbol -> monotonic time ของ push ล่าสุด (ใช้ throttle)
        self.paused = False
        self.missed_ticks = 0
        self.alive = True
        self.frames = FrameBuffer()


# ---------------------------------------------------------------------------
# server
# ---------------------------------------------------------------------------

class Server:
    def __init__(self, host="0.0.0.0", port=9009, mock=False, fail_after=None,
                 verbose=False, max_subs=5, max_alerts=10):
        # ค่าตั้งต้นคือต่อ Binance จริง (mock=False) ให้ตรงกับ CLI ที่ต้องสั่ง --mock เอง
        # ผู้ที่ต้องการ feed ปลอม (tests/acceptance.py, bench_http.py) ส่ง mock=True มาเอง
        self.host = host
        self.port = port
        self.mock = mock
        self.fail_after = fail_after
        self.verbose = verbose
        self.max_subs = max_subs
        self.max_alerts = max_alerts

        # ---- shared state (สเปกหัวข้อ 7.3) ----
        self.clients = {}             # conn_id -> ClientSession
        self.subscriptions = {}       # symbol -> set(conn_id)
        self.alerts = {}              # alert_id -> AlertRule
        self.upstream_refcount = {}   # symbol -> จำนวน client ที่ sub อยู่
        self.seq_counter = {}         # symbol -> sequence number (เริ่มที่ 1)

        # RLock ตัวเดียวคุม state ข้างบนทั้งหมด
        # ใช้ RLock (ไม่ใช่ Lock) เพราะ helper บางตัวเรียกซ้อนกันเองในเธรดเดียว
        self.state_lock = threading.RLock()

        self.next_conn_id = 1
        self.next_alert_id = 1
        self.messages_sent = 0
        self.started_at = time.time()

        self.running = False
        self.listener = None
        self.users = self._load_users()

        self.last_tick_at = time.monotonic()
        self.silence_reported = False
        self.feed = None

    # -- users ------------------------------------------------------------
    def _load_users(self):
        try:
            with open(USERS_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, ValueError) as exc:
            log("WARN could not read users.json (%s) - AUTH will always fail" % exc)
            return {}

    # -- feed -------------------------------------------------------------
    def _build_feed(self):
        if self.mock:
            import mock_feed
            return mock_feed.MockFeed(self.on_tick, self.on_upstream_status,
                                      fail_after=self.fail_after)
        # import ตรงนี้ (ไม่ใช่หัวไฟล์) เพื่อให้โหมด --mock ทำงานได้
        # แม้เครื่องนั้นยังไม่ได้ pip install websocket-client
        import upstream
        return upstream.BinanceUpstream(self.on_tick, self.on_upstream_status)

    # -- socket lifecycle -------------------------------------------------
    def serve_forever(self):
        # 1) สร้าง socket: AF_INET = IPv4, SOCK_STREAM = TCP
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 2) SO_REUSEADDR เพื่อให้ผูก port เดิมซ้ำได้ทันทีหลังปิด server (เลี่ยง TIME_WAIT)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # 3) bind = จองคู่ (IP, port) ให้ตัวเอง
        self.listener.bind((self.host, self.port))
        # 4) listen = เปลี่ยนเป็น passive socket + ตั้งคิว connection ที่รอ accept
        self.listener.listen(16)
        self.port = self.listener.getsockname()[1]   # เผื่อสั่ง port 0 ให้ OS เลือกให้

        self.running = True
        self.feed = self._build_feed()
        self.feed.start()

        threading.Thread(target=self._watchdog, name="watchdog", daemon=True).start()

        log("LISTEN on %s:%d (mode=%s)" % (self.host, self.port,
                                           "mock" if self.mock else "binance"))

        try:
            while self.running:
                # 5) accept = ดึง connection ที่ handshake เสร็จแล้วออกจากคิว
                #    ได้ socket ตัวใหม่สำหรับคุยกับ client ตัวนั้นโดยเฉพาะ
                try:
                    conn, addr = self.listener.accept()
                except OSError:
                    break        # listener ถูกปิดตอน shutdown
                self._on_accept(conn, addr)
        finally:
            self.shutdown()

    def _on_accept(self, conn, addr):
        # ปิด Nagle: ข้อมูลราคาเป็น message สั้นๆ ที่ต้องถึงเร็ว ไม่ควรถูกหน่วงเพื่อรวม packet
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with self.state_lock:
            conn_id = self.next_conn_id
            self.next_conn_id += 1
            session = ClientSession(conn, addr, conn_id)
            self.clients[conn_id] = session
            count = len(self.clients)
        log("ACCEPT ← %s:%d  (clients=%d)" % (addr[0], addr[1], count))
        # 1 client = 1 thread ทำให้โค้ดอ่านง่าย (บล็อกที่ recv() ได้เลย ไม่ต้องทำ event loop)
        threading.Thread(target=self._client_thread, args=(session,),
                         name="client-%s" % session.label, daemon=True).start()

    def shutdown(self):
        if not self.running:
            return
        self.running = False
        if self.feed is not None:
            self.feed.stop()
        with self.state_lock:
            sessions = list(self.clients.values())
        for session in sessions:
            self._close_session(session, notify=False)
        if self.listener is not None:
            try:
                self.listener.close()
            except OSError:
                pass
        log("SHUTDOWN complete")

    # -- send helpers -----------------------------------------------------
    def _write(self, session, raw):
        """จุดเดียวที่เขียนลง socket - ถือ send_lock เสมอเพื่อกัน message ปนกัน"""
        if not session.alive:
            return False
        try:
            with session.send_lock:
                # sendall() วนส่งจนครบเอง (send() ธรรมดาอาจส่งไม่หมดในครั้งเดียว)
                session.conn.sendall(raw)
            with self.state_lock:
                self.messages_sent += 1
            if self.verbose:
                log("[RAW SEND] %s" % protocol.raw_repr(raw))
            return True
        except OSError:
            # client ตายกลางคัน (BrokenPipe/ConnectionReset) - ไม่ใช่เรื่องผิดปกติ
            session.alive = False
            return False

    def send(self, session, code, **headers):
        """ตอบกลับ request (log ว่า SEND)"""
        raw = encode_response(code, **headers)
        if self._write(session, raw):
            log("SEND → %s  %s" % (session.label, self._log_tail(code, headers)))

    def push(self, session, code, **headers):
        """server ส่งเอง ไม่มี request นำ (log ว่า PUSH)"""
        raw = encode_response(code, **headers)
        if self._write(session, raw):
            log("PUSH → %s  %s" % (session.label, self._log_tail(code, headers)))

    def _log_tail(self, code, headers):
        """log ต้องมีทั้งเลข code และ phrase เสมอ เช่น "201 SUBSCRIBED" """
        text = protocol.status_line(code)
        if code == 110:
            text += " %s %s" % (headers.get("Symbol"), headers.get("Price"))
        elif code == 111:
            text += " %s %s %s" % (headers.get("Symbol"), headers.get("Condition"),
                                   headers.get("Threshold"))
        elif "Detail" in headers:
            text += " (%s)" % headers["Detail"]
        elif "Symbol" in headers:
            text += " %s" % headers["Symbol"]
        return text

    # -- client thread ----------------------------------------------------
    def _client_thread(self, session):
        try:
            self._recv_loop(session)
        except Exception:                      # กันไม่ให้เธรดตายเงียบๆ
            log("ERROR unhandled in client thread %s\n%s"
                % (session.label, traceback.format_exc()))
        finally:
            self._close_session(session)

    def _recv_loop(self, session):
        """framing loop - หัวใจของงาน (สเปกหัวข้อ 6.2)

        TCP เป็น byte stream: recv() หนึ่งครั้งอาจได้หลาย message หรือได้ไม่ครบ message
        FrameBuffer จึงสะสม byte ไว้แล้วตัดตาม \\r\\n\\r\\n ให้เป็น message ทีละอัน
        """
        while self.running and session.alive:
            try:
                chunk = session.conn.recv(RECV_SIZE)
            except (ConnectionResetError, ConnectionAbortedError, OSError):
                # client ตายกะทันหัน เช่นโดน Ctrl+C
                break
            if not chunk:
                break            # recv คืน b"" = อีกฝั่งปิด connection อย่างสุภาพ

            if self.verbose:
                log("[RAW RECV] %s" % protocol.raw_repr(chunk))

            try:
                # ใช้ feed_raw เพื่อ decode ทีละ frame เอง frame ที่พังจะได้ไม่ลาก frame ดีไปด้วย
                frames = session.frames.feed_raw(chunk)
            except MessageTooLarge as exc:
                log("RECV ← %s  message too large (%s)" % (session.label, exc.detail))
                self.send(session, 413, Detail="max %d bytes" % MAX_MESSAGE_SIZE)
                self._drain_before_close(session)
                break            # สเปกกำหนดให้ปิด connection หลัง 413
            for raw in frames:
                if not self._handle_frame(session, raw):
                    return       # QUIT หรือ error ที่ต้องปิด connection

    def _drain_before_close(self, session, seconds=1.0):
        """ปิดสายแบบสุภาพหลังตอบ error ที่ต้องตัด connection (เช่น 413)

        ถ้าเรา close() ทันทีขณะที่ client ยังส่งข้อมูลค้างมาอยู่ (เช่นเพิ่งยิงมา 10,000 bytes)
        TCP ฝั่งเราจะตอบ RST แทน FIN แล้ว RST จะทำให้ข้อมูลที่ยังค้างใน buffer ของ client
        ถูกทิ้งทั้งหมด - รวมถึง 413 ที่เราเพิ่งส่งไป client จึงไม่เห็น error เลย
        ทางแก้มาตรฐานคือ shutdown ฝั่งเขียนเพื่อส่ง FIN ก่อน แล้วอ่านที่เหลือทิ้งจนหมด
        """
        try:
            session.conn.shutdown(socket.SHUT_WR)   # ส่ง FIN บอกว่าเราพูดจบแล้ว
        except OSError:
            return
        deadline = time.monotonic() + seconds
        try:
            while time.monotonic() < deadline:
                session.conn.settimeout(0.2)
                if not session.conn.recv(RECV_SIZE):
                    break                            # เจอ FIN ของอีกฝั่งแล้ว ปิดได้ปลอดภัย
        except OSError:
            pass

    def _handle_frame(self, session, raw):
        """คืน False เมื่อต้องปิด connection"""
        try:
            msg = protocol.decode(raw)
        except MalformedMessage as exc:
            # UnsupportedVersion เป็นลูกของ MalformedMessage จึงใช้ .code แยก 400 / 426
            log("RECV ← %s  malformed (%s)" % (session.label, exc.detail))
            self.send(session, exc.code, Detail=exc.detail)
            return True
        except MessageTooLarge as exc:
            self.send(session, 413, Detail=exc.detail)
            self._drain_before_close(session)
            return False

        log("RECV ← %s  %s" % (session.label, protocol.summarize(msg)))

        try:
            return self._dispatch(session, msg)
        except Exception:
            # server ต้องไม่ตายเด็ดขาด - exception ที่ไม่คาดคิดตอบ 500 แล้วไปต่อ
            log("ERROR while handling %s from %s\n%s"
                % (msg.command, session.label, traceback.format_exc()))
            self.send(session, 500, Detail="internal error")
            return True

    # -- command dispatch -------------------------------------------------
    def _dispatch(self, session, msg):
        if msg.kind != "request":
            self.send(session, 400, Detail="expected a request, got a response")
            return True

        command = msg.command

        # QUIT ปิดได้จากทุกสถานะ (ตาม state machine ในสเปกหัวข้อ 6.8)
        if command == "QUIT":
            self.send(session, 206, Detail="bye")
            return False

        if command == "HELLO":
            return self._cmd_hello(session, msg)

        # สั่งอย่างอื่นก่อน HELLO -> 400
        if session.state == "NEW":
            self.send(session, 400, Detail="say HELLO first")
            return True

        if command == "AUTH":
            return self._cmd_auth(session, msg)

        if command == "PING":
            # PING เป็นการเช็กว่าสายยังดีอยู่ไหม จึงให้ใช้ได้ตั้งแต่ HELLO แล้ว
            self.send(session, 205, Timestamp=int(time.time() * 1000))
            return True

        # คำสั่งที่เหลือต้อง AUTH ก่อน
        if session.state == "GREETED":
            if command in protocol.COMMANDS:
                self.send(session, 401, Detail="AUTH required")
            else:
                self.send(session, 405, Detail="unknown command: %s" % command)
            return True

        handlers = {
            "SUB": self._cmd_sub,
            "UNSUB": self._cmd_unsub,
            "ALERT": self._cmd_alert,
            "LIST": self._cmd_list,
            "STATS": self._cmd_stats,
            "PAUSE": self._cmd_pause,
            "RESUME": self._cmd_resume,
        }
        handler = handlers.get(command)
        if handler is None:
            self.send(session, 405, Detail="unknown command: %s" % command)
            return True
        return handler(session, msg)

    # -- HELLO / AUTH -----------------------------------------------------
    def _cmd_hello(self, session, msg):
        if session.state != "NEW":
            self.send(session, 400, Detail="already greeted")
            return True
        name = msg.get("Client-Name")
        if not name:
            self.send(session, 400, Detail="missing header: Client-Name")
            return True
        session.name = name
        session.state = "GREETED"
        self.send(session, 200, Client_Name=name, Detail="AUTH required")
        return True

    def _cmd_auth(self, session, msg):
        user = msg.get("User")
        token = msg.get("Token")
        if not user or not token:
            self.send(session, 400, Detail="missing header: User or Token")
            return True
        if self.users.get(user) != token:
            self.send(session, 401, Detail="invalid user or token")
            return True
        session.username = user
        # AUTH ซ้ำได้ (idempotent) แค่ไม่ให้ถอยสถานะกลับถ้ากำลัง stream อยู่
        if session.state == "GREETED":
            session.state = "READY"
        self.send(session, 200, User=user, Detail="authenticated")
        return True

    # -- SUB / UNSUB ------------------------------------------------------
    def _cmd_sub(self, session, msg):
        symbol = (msg.get("Symbol") or "").upper()
        interval = msg.get("Interval") or DEFAULT_INTERVAL

        if not symbol:
            self.send(session, 400, Detail="missing header: Symbol")
            return True
        if not SYMBOL_PATTERN.match(symbol):
            self.send(session, 400, Symbol=symbol, Detail="symbol must match [A-Z0-9]{5,12}")
            return True
        if interval not in INTERVALS:
            self.send(session, 400, Interval=interval,
                      Detail="interval must be one of %s" % ", ".join(INTERVALS))
            return True

        # ตัดสินใจใน lock แต่ตอบกลับนอก lock เสมอ
        # (สเปกหัวข้อ 7.2 ห้ามเรียก sendall() ขณะถือ state_lock)
        with self.state_lock:
            if symbol in session.subs:
                refused = (409, {"Symbol": symbol, "Detail": "already subscribed"})
            # เช็ก limit ก่อนถาม upstream: เป็นการตรวจในบ้านที่ถูกกว่า
            # และทำให้ทดสอบ 429 ได้แม้ mock จะมี symbol ให้ sub แค่ 5 ตัว
            elif len(session.subs) >= self.max_subs:
                refused = (429, {"Symbol": symbol,
                                 "Detail": "subscription limit is %d" % self.max_subs})
            else:
                refused = None
        if refused is not None:
            self.send(session, refused[0], **refused[1])
            return True

        if not self.feed.has_symbol(symbol):
            self.send(session, 404, Symbol=symbol, Detail="symbol not available on upstream")
            return True
        if not self.feed.is_up():
            self.send(session, 503, Symbol=symbol, Detail="upstream is down, try again later")
            return True

        # ถือ send_lock ของ client ตัวนี้คร่อมช่วง "ลงทะเบียน -> ตอบ 201"
        # ถ้าไม่ถือ feed thread อาจแทรก 110 DATA ออกไปก่อนที่ 201 SUBSCRIBED จะถึงมือ client
        # (เกิดได้จริงตอน symbol นั้นมีคนอื่น sub อยู่แล้ว = ราคากำลังไหลอยู่)
        # ลำดับการถือล็อกทั้งโปรแกรมเป็น send_lock -> state_lock เสมอ จึงไม่มีวงจร deadlock
        with session.send_lock:
            with self.state_lock:
                session.subs[symbol] = interval
                session.last_push.pop(symbol, None)
                self.subscriptions.setdefault(symbol, set()).add(session.conn_id)
                before = self.upstream_refcount.get(symbol, 0)
                self.upstream_refcount[symbol] = before + 1
                self.seq_counter.setdefault(symbol, 0)
                if before == 0:
                    # เพิ่งเปิด stream ตัวนี้ ให้เริ่มจับเวลา "เงียบ" ใหม่
                    # ไม่งั้นเวลาที่ server ว่าง (ไม่มีใคร sub เลยไม่มี tick) จะถูกนับสะสม
                    # แล้ว watchdog จะยิง 504 ทันทีที่มีคน SUB ทั้งที่ข้อมูลกำลังจะมา
                    self.last_tick_at = time.monotonic()
                    self.silence_reported = False
                if session.state == "READY":
                    session.state = "STREAMING"
                active = len(session.subs)

            # เปิด stream จริงนอก state_lock เพราะอาจต้องต่อ network
            # (ห้ามบล็อกคนอื่นทั้ง server - ตอนนี้บล็อกได้อย่างมากแค่ client ตัวนี้)
            if before == 0:
                self.feed.open_symbol(symbol)
                log("UPSTREAM open %s (refcount 0→1)" % symbol)
            else:
                log("UPSTREAM reuse %s (refcount %d→%d)" % (symbol, before, before + 1))

            self.send(session, 201, Symbol=symbol, Interval=interval, Active_Subs=active)
        return True

    def _cmd_unsub(self, session, msg):
        symbol = (msg.get("Symbol") or "").upper()
        if not symbol:
            self.send(session, 400, Detail="missing header: Symbol")
            return True

        with self.state_lock:
            subscribed = symbol in session.subs
            if subscribed:
                counts = self._remove_subscription(session, symbol)
                active = len(session.subs)
                if not session.subs and session.state in ("STREAMING", "PAUSED"):
                    # ไม่เหลือ subscription แล้ว ถอยกลับสถานะ READY
                    session.state = "READY"
                    session.paused = False
                    session.missed_ticks = 0

        if not subscribed:
            self.send(session, 410, Symbol=symbol, Detail="not subscribed")
            return True
        # ปิด stream จริงนอก lock เพราะอาจต้องปิด socket ของ WebSocket ซึ่งบล็อกได้
        self._apply_refcount_drop(symbol, *counts)
        self.send(session, 202, Symbol=symbol, Active_Subs=active)
        return True

    def _remove_subscription(self, session, symbol):
        """ต้องเรียกขณะถือ state_lock - ลด refcount แล้วคืน (before, after)

        ที่นี่แตะเฉพาะ state ในหน่วยความจำ ส่วนการปิด stream จริงกับการ log
        ให้ผู้เรียกไปทำต่อด้วย _apply_refcount_drop() นอก lock
        (feed.close_symbol ฝั่ง Binance ต้องปิด socket ของ WebSocket ซึ่งบล็อกได้)
        """
        session.subs.pop(symbol, None)
        session.last_push.pop(symbol, None)
        holders = self.subscriptions.get(symbol)
        if holders is not None:
            holders.discard(session.conn_id)
            if not holders:
                self.subscriptions.pop(symbol, None)

        before = self.upstream_refcount.get(symbol, 0)
        after = max(before - 1, 0)
        if after == 0:
            self.upstream_refcount.pop(symbol, None)
        else:
            self.upstream_refcount[symbol] = after
        return before, after

    def _apply_refcount_drop(self, symbol, before, after):
        """ปิด upstream stream ที่ไม่มีใครดูแล้ว + log - ต้องเรียกนอก state_lock"""
        if after == 0:
            self.feed.close_symbol(symbol)
            log("UPSTREAM close %s (refcount %d→0)" % (symbol, before))
        else:
            log("UPSTREAM keep %s (refcount %d→%d)" % (symbol, before, after))

    # -- ALERT ------------------------------------------------------------
    def _cmd_alert(self, session, msg):
        action = (msg.get("Action") or "").upper()
        if action == "SET":
            return self._alert_set(session, msg)
        if action == "DEL":
            return self._alert_del(session, msg)
        self.send(session, 400, Detail="Action must be SET or DEL")
        return True

    def _alert_set(self, session, msg):
        symbol = (msg.get("Symbol") or "").upper()
        condition = (msg.get("Condition") or "").upper()
        raw_value = msg.get("Value")

        if not symbol or not condition or raw_value is None:
            self.send(session, 400, Detail="ALERT SET needs Symbol, Condition and Value")
            return True
        if not SYMBOL_PATTERN.match(symbol):
            self.send(session, 400, Symbol=symbol, Detail="symbol must match [A-Z0-9]{5,12}")
            return True
        if condition not in CONDITIONS:
            self.send(session, 400, Condition=condition, Detail="condition must be ABOVE or BELOW")
            return True
        try:
            value = float(raw_value)
        except ValueError:
            self.send(session, 400, Value=raw_value, Detail="value must be a positive number")
            return True
        if value <= 0:
            self.send(session, 400, Value=raw_value, Detail="value must be a positive number")
            return True

        with self.state_lock:
            mine = sum(1 for a in self.alerts.values() if a.conn_id == session.conn_id)
        if mine >= self.max_alerts:
            self.send(session, 429, Detail="alert limit is %d" % self.max_alerts)
            return True

        if not self.feed.has_symbol(symbol):
            self.send(session, 404, Symbol=symbol, Detail="symbol not available on upstream")
            return True

        with self.state_lock:
            alert_id = self.next_alert_id
            self.next_alert_id += 1
            self.alerts[alert_id] = AlertRule(alert_id, session.conn_id, symbol, condition, value)

        self.send(session, 203, Alert_Id=alert_id, Symbol=symbol,
                  Condition=condition, Value=format_price(value))
        return True

    def _alert_del(self, session, msg):
        raw_id = msg.get("Alert-Id")
        if raw_id is None:
            self.send(session, 400, Detail="missing header: Alert-Id")
            return True
        try:
            alert_id = int(raw_id)
        except ValueError:
            self.send(session, 400, Detail="Alert-Id must be a number")
            return True

        with self.state_lock:
            rule = self.alerts.get(alert_id)
            # ลบได้เฉพาะ alert ของตัวเอง - ของคนอื่นให้ตอบ 411 เหมือนไม่มีอยู่จริง
            is_mine = rule is not None and rule.conn_id == session.conn_id
            if is_mine:
                del self.alerts[alert_id]

        if not is_mine:
            self.send(session, 411, Alert_Id=alert_id, Detail="alert not found")
            return True
        self.send(session, 204, Alert_Id=alert_id)
        return True

    # -- LIST / STATS -----------------------------------------------------
    def _cmd_list(self, session, msg):
        kind = (msg.get("Type") or "").upper()
        if kind not in ("SUB", "ALERT"):
            self.send(session, 400, Detail="Type must be SUB or ALERT")
            return True

        with self.state_lock:
            if kind == "SUB":
                items = ["%s %s" % (s, i) for s, i in sorted(session.subs.items())]
            else:
                items = [a.describe() for a in sorted(self.alerts.values(),
                                                      key=lambda r: r.alert_id)
                         if a.conn_id == session.conn_id]

        # หลายรายการยังต้องอยู่ใน 1 message: ใช้ Count + Item-N ตามสเปกหัวข้อ 6.4
        headers = {"Type": kind, "Count": len(items)}
        for index, item in enumerate(items, start=1):
            headers["Item-%d" % index] = item
        self.send(session, 200, **headers)
        return True

    def _cmd_stats(self, session, msg):
        with self.state_lock:
            clients = len(self.clients)
            symbols = len(self.upstream_refcount)
            sent = self.messages_sent
        self.send(session, 200,
                  Uptime="%ds" % int(time.time() - self.started_at),
                  Clients=clients,
                  Upstream_Symbols=symbols,
                  Messages_Sent=sent)
        return True

    # -- PAUSE / RESUME ---------------------------------------------------
    def _cmd_pause(self, session, msg):
        with self.state_lock:
            has_subs = bool(session.subs)
            if has_subs:
                # PAUSE ซ้ำถือว่าไม่ผิด ตอบ 207 เหมือนเดิม (idempotent) สเปกไม่ได้ห้ามไว้
                session.paused = True
                session.state = "PAUSED"

        if not has_subs:
            self.send(session, 410, Detail="no active subscription to pause")
            return True
        self.send(session, 207, Detail="data push paused, alerts still delivered")
        return True

    def _cmd_resume(self, session, msg):
        # เหตุผลเดียวกับ SUB: ปลดล็อก pause แล้วต้องให้ 208 ออกไปก่อน 110 ตัวถัดไป
        with session.send_lock:
            with self.state_lock:
                was_paused = session.paused
                missed = session.missed_ticks
                if was_paused:
                    session.missed_ticks = 0
                    session.paused = False
                    session.state = "STREAMING"

            if not was_paused:
                self.send(session, 400, Detail="stream is not paused")
                return True
            self.send(session, 208, Missed_Ticks=missed)
        return True

    # -- disconnect / cleanup ---------------------------------------------
    def _close_session(self, session, notify=True):
        with self.state_lock:
            if session.conn_id not in self.clients:
                return
            del self.clients[session.conn_id]
            symbols = list(session.subs)
            drops = [(symbol,) + self._remove_subscription(session, symbol)
                     for symbol in symbols]
            dropped = [aid for aid, rule in self.alerts.items()
                       if rule.conn_id == session.conn_id]
            for alert_id in dropped:
                del self.alerts[alert_id]
            session.alive = False
            session.state = "CLOSED"

        # ปิด upstream นอก lock ด้วยเหตุผลเดียวกับ UNSUB
        for symbol, before, after in drops:
            self._apply_refcount_drop(symbol, before, after)

        try:
            session.conn.close()
        except OSError:
            pass
        if notify:
            log("DISCONNECT %s  cleanup: subs=%d alerts=%d"
                % (session.label, len(symbols), len(dropped)))

    # -- feed callbacks ---------------------------------------------------
    def on_tick(self, symbol, price, change, event_ms):
        """เรียกจาก feed thread ทุก tick - ทำหน้าที่ dispatcher ต่อในเธรดเดียวกัน"""
        now = time.monotonic()
        with self.state_lock:
            self.last_tick_at = now
            self.silence_reported = False

            # Seq เดินทุก tick จาก upstream ไม่ว่าจะส่งออกหรือถูก throttle ทิ้ง
            # เพื่อให้ client เห็น gap ได้ว่ามีข้อมูลที่ไม่ได้รับกี่ tick
            seq = self.seq_counter.get(symbol, 0) + 1
            self.seq_counter[symbol] = seq

            targets = []
            for conn_id in list(self.subscriptions.get(symbol, ())):
                session = self.clients.get(conn_id)
                if session is None or not session.alive:
                    continue
                interval = INTERVALS.get(session.subs.get(symbol, DEFAULT_INTERVAL), 1.0)
                # throttle: ส่งเฉพาะเมื่อครบรอบ interval ที่ client ขอไว้ตอน SUB
                if now - session.last_push.get(symbol, 0.0) < interval:
                    continue
                session.last_push[symbol] = now
                if session.paused:
                    # นับเฉพาะจังหวะที่ "ควรจะได้ push" แต่ถูกกลั้นไว้
                    # (จึงเลื่อน last_push ด้วย ไม่งั้น pause 15 วิ จะนับได้ 60 แทนที่จะเป็น 15)
                    session.missed_ticks += 1
                else:
                    targets.append(session)

            # ประเมิน alert ทุก tick ไม่ใช่ทุก push เพราะ throttle อาจทำให้พลาดจังหวะที่ราคาทะลุ
            fired = []
            for rule in list(self.alerts.values()):
                if rule.symbol == symbol and rule.status == "ARMED" and rule.matches(price):
                    rule.status = "TRIGGERED"      # one-shot ยิงครั้งเดียว
                    owner = self.clients.get(rule.conn_id)
                    if owner is not None and owner.alive:
                        fired.append((owner, rule))

        # ---- ออกจาก lock แล้วค่อยส่ง ----
        # ห้ามเรียก sendall() ขณะถือ state_lock: client ที่รับช้าตัวเดียวจะบล็อกทั้ง server
        text_price = format_price(price)
        for session in targets:
            self.push(session, 110, Symbol=symbol, Price=text_price,
                      Change24h="%.2f" % change, Seq=seq, Timestamp=event_ms)
        for session, rule in fired:
            # 111 ยังส่งแม้ client สั่ง pause ไว้ เพราะ alert คือข้อมูลสำคัญที่ผู้ใช้ตั้งใจรอ
            # (สิ่งที่ pause กลั้นไว้คือ 110 DATA ที่ไหลตลอดเวลาเท่านั้น)
            self.push(session, 111, Alert_Id=rule.alert_id, Symbol=symbol,
                      Condition=rule.condition, Threshold=format_price(rule.value),
                      Price=text_price, Timestamp=event_ms)

    def on_upstream_status(self, state, detail):
        """feed แจ้งว่าหลุด/กลับมา -> กระจาย 112 ให้ทุกคน และ 503 ให้คนที่ sub อยู่"""
        log("UPSTREAM status=%s (%s)" % (state, detail))
        with self.state_lock:
            sessions = list(self.clients.values())
            self.silence_reported = False
            self.last_tick_at = time.monotonic()
        for session in sessions:
            if session.state == "NEW":
                continue     # ยังไม่ HELLO ก็ยังไม่ควรได้รับ push
            self.push(session, 112, Detail=detail)
            if state == "down" and session.subs:
                self.push(session, 503, Detail="no upstream data while disconnected")

    def _watchdog(self):
        """เฝ้าดูว่า upstream เงียบเกิน timeout หรือไม่ -> 504 UPSTREAM TIMEOUT

        แยกจากกรณี 503 ตรงที่ 503 คือ "รู้ตัวว่าหลุด" ส่วน 504 คือ "สายยังอยู่แต่เงียบ"
        ซึ่งเป็นอาการที่เจอจริงกับ WebSocket ที่ค้างโดยไม่แจ้ง error
        """
        timeout = getattr(self.feed, "silence_timeout", 15.0)
        while self.running:
            time.sleep(0.5)
            with self.state_lock:
                has_symbols = bool(self.upstream_refcount)
                silent_for = time.monotonic() - self.last_tick_at
                already = self.silence_reported
                if has_symbols and not already and silent_for > timeout:
                    self.silence_reported = True
                    victims = [s for s in self.clients.values() if s.subs]
                else:
                    victims = []
            for session in victims:
                self.push(session, 504, Detail="no upstream tick for %.1fs" % silent_for)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="CMSP/1.0 streaming server (raw TCP socket)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9009)
    parser.add_argument("--mock", action="store_true",
                        help="ใช้ mock feed แทน Binance (ค่าตั้งต้น: ต่อ Binance จริง)")
    parser.add_argument("--fail-after", type=float, default=None,
                        help="(ใช้กับ --mock) จำลอง upstream ตายหลัง SEC วินาที")
    parser.add_argument("--verbose", action="store_true", help="log raw bytes ของทุก message")
    parser.add_argument("--max-subs", type=int, default=5)
    parser.add_argument("--max-alerts", type=int, default=10)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    server = Server(host=args.host, port=args.port, mock=args.mock,
                    fail_after=args.fail_after, verbose=args.verbose,
                    max_subs=args.max_subs, max_alerts=args.max_alerts)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("SIGINT received, shutting down")
        server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
