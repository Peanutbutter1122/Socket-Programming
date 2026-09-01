"""
bench_http.py - HTTP polling baseline ไว้เทียบกับ CMSP push

============================================================================
ไฟล์นี้เป็น 1 ใน 2 ไฟล์ที่ได้รับ "ยกเว้น" ให้ใช้ library ภายนอกได้
    ใช้: http.server + http.client (stdlib แต่เป็น HTTP framework ที่ปกติห้ามใช้)

เหตุผลที่ยกเว้น: ไฟล์นี้ไม่ได้เป็นส่วนหนึ่งของระบบ CMSP เลย มันคือ "คู่เทียบ"
ที่สร้างขึ้นเพื่อวัดว่าการทำ push บน raw socket ได้เปรียบ polling แบบ HTTP แค่ไหน
ถ้าเขียน HTTP เองด้วย raw socket การเทียบจะไม่ยุติธรรม เพราะจะกลายเป็นการเทียบกับ
HTTP เวอร์ชันที่เราทำเองแบบง่ายๆ ไม่ใช่ของที่ใช้กันจริง

*** ระบบ CMSP จริง (server.py / client.py) ยังเป็น raw socket 100% ***
============================================================================

วัด 3 ขาเพื่อให้เทียบได้ครบมุม
  1. HTTP/1.1 + keep-alive   <- baseline ที่ยุติธรรมที่สุด (คนทำจริงทำแบบนี้)
  2. HTTP/1.1 ไม่มี keep-alive <- ต่อ TCP ใหม่ทุก poll ให้เห็นต้นทุน handshake
  3. CMSP push (raw socket)

ความยุติธรรมของการเทียบ (ตั้งใจให้ HTTP ได้เปรียบเท่าที่ทำได้)
  - ใช้ mock feed ตัวเดียวกัน อัตรา tick เท่ากัน
  - ขา keep-alive ต่อ TCP ครั้งเดียวแล้วยิงซ้ำ ไม่ต้อง handshake ใหม่ทุกครั้ง
  - body สั้นที่สุดเท่าที่ยังสื่อความหมายได้ (JSON 4 ฟิลด์ เท่ากับที่ CMSP ส่ง)
  - นับเฉพาะ byte ชั้น application ทั้งสองฝั่งเหมือนกัน (ไม่นับ header ของ TCP/IP)
    ส่วน byte รวมบนสายให้วัดจาก packet capture แยกตาม port (ดู report/measure_bench.sh)
  - poll ทุก 1 วินาที = interval เดียวกับที่ CMSP client ขอไว้ตอน SUB

วิธีรัน:
    python bench_http.py                    # 20 วินาที, poll ทุก 1 วินาที
    python bench_http.py --seconds 30 --interval 1
    python bench_http.py --http-port 8080 --http-close-port 8081 --cmsp-port 9009 \
                         --json out.json    # ตรึง port ไว้ให้ pcap แยกได้
"""

import argparse
import http.client
import json
import random
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import mock_feed
import protocol
import server as server_module
from protocol import FrameBuffer, encode_request

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:      # pragma: no cover
    pass


# ---------------------------------------------------------------------------
# ฝั่ง HTTP: server เก็บราคาล่าสุดไว้ให้ client มาถาม (pull)
# ---------------------------------------------------------------------------

class PriceState:
    """ราคาล่าสุดต่อ symbol - เลียนแบบ REST API ที่เก็บ snapshot ไว้ให้ poll"""

    def __init__(self):
        self.lock = threading.Lock()
        self.data = {}

    def update(self, symbol, price, change, event_ms):
        with self.lock:
            self.data[symbol] = {"s": symbol, "c": round(price, 2),
                                 "P": round(change, 2), "E": event_ms}

    def get(self, symbol):
        with self.lock:
            return self.data.get(symbol)


class Counters:
    def __init__(self):
        self.lock = threading.Lock()
        self.bytes_in = 0
        self.bytes_out = 0
        self.requests = 0

    def add(self, bytes_in=0, bytes_out=0, requests=0):
        with self.lock:
            self.bytes_in += bytes_in
            self.bytes_out += bytes_out
            self.requests += requests


class PriceHandler(BaseHTTPRequestHandler):
    # HTTP/1.1 เพื่อให้ keep-alive ทำงาน (ไม่ต้อง handshake ใหม่ทุก poll)
    protocol_version = "HTTP/1.1"
    state = None
    counters = None

    def do_GET(self):
        symbol = "BTCUSDT"
        if "?" in self.path:
            query = self.path.split("?", 1)[1]
            for pair in query.split("&"):
                if pair.startswith("symbol="):
                    symbol = pair.split("=", 1)[1].upper()

        row = self.state.get(symbol)
        body = json.dumps(row if row else {}, separators=(",", ":")).encode("utf-8")

        # ถ้า client ขอปิดสาย ต้องตอบ Connection: close แล้วปิดจริง
        # (ขา "ไม่มี keep-alive" ใช้ทางนี้ - ต้อง handshake ใหม่ทุก poll)
        wants_close = (self.headers.get("Connection", "").lower() == "close")
        if wants_close:
            self.close_connection = True
            head = ("HTTP/1.1 200 OK\r\n"
                    "Content-Type: application/json\r\n"
                    "Connection: close\r\n"
                    "Content-Length: %d\r\n\r\n" % len(body)).encode("utf-8")
        else:
            head = ("HTTP/1.1 200 OK\r\n"
                    "Content-Type: application/json\r\n"
                    "Content-Length: %d\r\n\r\n" % len(body)).encode("utf-8")

        # นับ byte ชั้น application ของทั้งสองทิศ: request ที่รับมา + response ที่ส่งกลับ
        request_bytes = len(self.raw_requestline) + len(str(self.headers)) + 2
        self.counters.add(bytes_in=request_bytes,
                          bytes_out=len(head) + len(body), requests=1)

        self.wfile.write(head + body)

    def log_message(self, *args):
        pass        # ไม่ต้องพ่น log ของ HTTP ออกมากวนผลวัด


def run_http_leg(state, seconds, interval, symbol, keep_alive=True, port=0):
    """วัดขา HTTP polling

    keep_alive=True  -> ต่อ TCP ครั้งเดียวแล้วยิงซ้ำทุก poll (baseline ที่ยุติธรรม)
    keep_alive=False -> ต่อใหม่ทุก poll แล้วปิด (ให้เห็นต้นทุน handshake ที่จ่ายซ้ำ)
    """
    counters = Counters()
    PriceHandler.state = state
    PriceHandler.counters = counters

    httpd = ThreadingHTTPServer(("127.0.0.1", port), PriceHandler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    port = httpd.server_address[1]

    conn = None
    connections = 0
    if keep_alive:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        conn.connect()
        connections = 1

    staleness = []
    seen_events = set()
    fresh = 0
    wasted = 0

    # สุ่มเฟสเริ่มต้นก่อนเข้าลูป: ถ้าไม่สุ่ม รอบ poll (ทุก 1.000 วิ) จะล็อกเฟสกับ
    # tick ของ feed (ทุก 0.250 วิ) พอดี ทำให้ staleness ที่วัดได้เป็นเฟสเดียวซ้ำๆ
    # ไม่สะท้อนค่าที่ผู้ใช้จริงเจอ (ผู้ใช้ไม่ได้เริ่ม poll พร้อม tick)
    time.sleep(random.random() * interval)

    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if keep_alive:
            conn.request("GET", "/price?symbol=%s" % symbol)
            payload = json.loads(conn.getresponse().read().decode("utf-8") or "{}")
        else:
            # TCP handshake ใหม่ทุกครั้ง = ต้นทุนที่ keep-alive ประหยัดไปได้
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/price?symbol=%s" % symbol,
                         headers={"Connection": "close"})
            payload = json.loads(conn.getresponse().read().decode("utf-8") or "{}")
            conn.close()
            connections += 1

        if payload:
            # staleness = ข้อมูลที่เพิ่งได้มา "เก่า" ไปแล้วกี่มิลลิวินาที
            staleness.append(time.time() * 1000.0 - payload["E"])
            if payload["E"] in seen_events:
                wasted += 1          # poll แล้วได้ของเดิม = เสียเที่ยวเปล่า
            else:
                seen_events.add(payload["E"])
                fresh += 1
        # ใส่ jitter +-10% ให้เฟสของ poll กวาดครบช่วง tick (250 ms) ระหว่างการวัด
        # ไม่งั้นทั้ง 60 ตัวอย่างจะเป็นเฟสเดียวกันหมด แล้ว p50 จะขึ้นกับดวงล้วนๆ
        # อัตรา poll เฉลี่ยยังเท่ากับ 1 ครั้งต่อ interval เหมือนเดิม
        time.sleep(interval * random.uniform(0.9, 1.1))

    if keep_alive and conn is not None:
        conn.close()
    httpd.shutdown()
    return {
        "name": "HTTP polling (keep-alive)" if keep_alive
                else "HTTP polling (ไม่มี keep-alive)",
        "port": port,
        "bytes": counters.bytes_in + counters.bytes_out,
        "messages": counters.requests,
        "updates": fresh,
        "wasted": wasted,
        "connections": connections,
        "staleness": staleness,
    }


# ---------------------------------------------------------------------------
# ฝั่ง CMSP: server push ให้เอง client แค่รอรับ
# ---------------------------------------------------------------------------

def run_cmsp_leg(seconds, interval, symbol, port=0):
    srv = server_module.Server(host="127.0.0.1", port=port, mock=True)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    for _ in range(100):
        if srv.running and srv.port:
            break
        time.sleep(0.05)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect(("127.0.0.1", srv.port))

    frames = FrameBuffer()
    bytes_out = 0
    bytes_in = 0
    messages = 0
    updates = 0
    staleness = []

    for raw in (encode_request("HELLO", Client_Name="bench"),
                encode_request("AUTH", User="student", Token="1234"),
                encode_request("SUB", Symbol=symbol,
                               Interval="%ds" % int(interval))):
        sock.sendall(raw)
        bytes_out += len(raw)

    started = time.monotonic()
    sock.settimeout(0.5)
    while time.monotonic() - started < seconds:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            continue
        if not chunk:
            break
        bytes_in += len(chunk)
        for raw in frames.feed_raw(chunk):
            msg = protocol.decode(raw)
            messages += 1
            if msg.code == 110:
                updates += 1
                staleness.append(time.time() * 1000.0 - int(msg.get("Timestamp")))

    sock.close()
    srv.shutdown()
    return {
        "name": "CMSP push (raw socket)",
        "port": srv.port,
        "bytes": bytes_in + bytes_out,
        "messages": messages + 3,      # +3 คือ HELLO/AUTH/SUB ที่ส่งไปตอนเริ่ม
        "updates": updates,
        "wasted": 0,                   # push จะส่งก็ต่อเมื่อมีของใหม่จริง
        "connections": 1,              # ต่อครั้งเดียวตลอด session
        "staleness": staleness,
    }


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

def percentile(values, p):
    if not values:
        return float("nan")
    data = sorted(values)
    index = min(int(round((len(data) - 1) * p)), len(data) - 1)
    return data[index]


def show(result, seconds):
    stale = result["staleness"]
    print("  %-28s" % result["name"])
    print("     byte ที่วิ่งบนสาย   : %8d bytes  (%.1f bytes/วินาที)"
          % (result["bytes"], result["bytes"] / seconds))
    print("     จำนวน message      : %8d" % result["messages"])
    print("     TCP connection     : %8d  (= จำนวน 3-way handshake)"
          % result.get("connections", 1))
    print("     ข้อมูลใหม่ที่ได้จริง : %8d  (เสียเที่ยวเปล่า %d ครั้ง)"
          % (result["updates"], result["wasted"]))
    if result["updates"]:
        print("     byte ต่อ 1 update   : %8.1f bytes"
              % (result["bytes"] / result["updates"]))
    if stale:
        print("     ความเก่าของข้อมูล  : p50 %.0f ms   p95 %.0f ms   max %.0f ms"
              % (percentile(stale, 0.5), percentile(stale, 0.95), max(stale)))
    print()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="เทียบ CMSP push กับ HTTP polling baseline")
    parser.add_argument("--seconds", type=float, default=20.0)
    parser.add_argument("--interval", type=float, default=1.0,
                        help="รอบการ poll ของ HTTP และ Interval ที่ CMSP client ขอ")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--http-port", type=int, default=0,
                        help="ตรึง port ของขา keep-alive (0 = ให้ OS เลือก)")
    parser.add_argument("--http-close-port", type=int, default=0,
                        help="ตรึง port ของขาไม่มี keep-alive")
    parser.add_argument("--cmsp-port", type=int, default=0,
                        help="ตรึง port ของ CMSP server")
    parser.add_argument("--json", dest="json_out", default=None,
                        help="เขียนผลเป็น JSON ลงไฟล์ (ไว้ทำตาราง/กราฟ)")
    args = parser.parse_args(argv)

    print("=" * 74)
    print("Benchmark: CMSP push (raw socket) vs HTTP polling baseline")
    print("symbol=%s  เวลา=%.0fs  interval=%.0fs  แหล่งข้อมูล=mock feed ตัวเดียวกัน"
          % (args.symbol, args.seconds, args.interval))
    print("=" * 74)

    # ---- ขา 1: HTTP + keep-alive ----
    state = PriceState()
    feed = mock_feed.MockFeed(
        on_tick=lambda s, p, c, e: state.update(s, p, c, e),
        on_status=lambda *a: None)
    feed.open_symbol(args.symbol)
    feed.start()
    print("[1/3] กำลังวัด HTTP polling (keep-alive) ...", flush=True)
    http_result = run_http_leg(state, args.seconds, args.interval, args.symbol,
                               keep_alive=True, port=args.http_port)

    # ---- ขา 2: HTTP ไม่มี keep-alive ----
    print("[2/3] กำลังวัด HTTP polling (ไม่มี keep-alive) ...", flush=True)
    http_close_result = run_http_leg(state, args.seconds, args.interval, args.symbol,
                                     keep_alive=False, port=args.http_close_port)
    feed.stop()

    # ---- ขา 3: CMSP ----
    print("[3/3] กำลังวัด CMSP push ...", flush=True)
    server_module.log = lambda text: None      # ปิด log ไม่ให้กวนผลวัด
    cmsp_result = run_cmsp_leg(args.seconds, args.interval, args.symbol,
                               port=args.cmsp_port)

    print()
    show(http_result, args.seconds)
    show(http_close_result, args.seconds)
    show(cmsp_result, args.seconds)

    print("-" * 74)
    if http_result["bytes"]:
        ratio = http_result["bytes"] / max(cmsp_result["bytes"], 1)
        print("CMSP ใช้ byte น้อยกว่า HTTP polling %.1f เท่า" % ratio)
    if http_result["staleness"] and cmsp_result["staleness"]:
        print("ข้อมูลของ CMSP สดกว่า (p50 %.0f ms เทียบกับ %.0f ms) "
              "เพราะ push ทันทีที่มีของใหม่ ไม่ต้องรอรอบ poll"
              % (percentile(cmsp_result["staleness"], 0.5),
                 percentile(http_result["staleness"], 0.5)))
    print("HTTP เสียเที่ยวเปล่า %d ครั้งจาก %d ครั้งที่ยิงไป "
          "(poll แล้วได้ข้อมูลชุดเดิม)"
          % (http_result["wasted"], http_result["messages"]))
    print("การไม่ใช้ keep-alive ทำให้ต้อง handshake %d ครั้ง (เทียบกับ %d ครั้งของ CMSP) "
          "และใช้ byte เพิ่มเป็น %.2f เท่าของขา keep-alive"
          % (http_close_result["connections"], cmsp_result["connections"],
             http_close_result["bytes"] / max(http_result["bytes"], 1)))
    print("-" * 74)

    if args.json_out:
        payload = {
            "config": {"symbol": args.symbol, "seconds": args.seconds,
                       "interval": args.interval},
            "legs": [],
        }
        for result in (http_result, http_close_result, cmsp_result):
            stale = result["staleness"]
            payload["legs"].append({
                "name": result["name"],
                "port": result.get("port"),
                "app_bytes": result["bytes"],
                "messages": result["messages"],
                "connections": result.get("connections", 1),
                "updates": result["updates"],
                "wasted": result["wasted"],
                "bytes_per_update": (result["bytes"] / result["updates"]
                                     if result["updates"] else None),
                "staleness_p50": percentile(stale, 0.5) if stale else None,
                "staleness_p95": percentile(stale, 0.95) if stale else None,
                "staleness_max": max(stale) if stale else None,
                "staleness_samples": len(stale),
            })
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        print("เขียนผลเป็น JSON ลงที่ %s" % args.json_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
