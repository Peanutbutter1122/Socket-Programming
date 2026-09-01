"""
client.py - CMSP/1.0 client บน raw TCP socket + REPL

ใช้ stdlib ล้วน เรียก socket API เอง: socket() -> connect() -> sendall()/recv() -> close()

โครงสร้าง 2 เธรด
    main thread   : REPL อ่านคำสั่งจากผู้ใช้แล้วแปลงเป็น CMSP request
    reader thread : recv() + framing + พิมพ์ทุก message ที่ได้รับ

ต้องแยกเธรดเพราะ server ส่ง push (110/111/112) มาเองได้ตลอดเวลา
ถ้าอ่านใน thread เดียวกับ input() จะต้องรอผู้ใช้พิมพ์เสร็จก่อนถึงจะเห็นราคา
"""

import argparse
import socket
import sys
import threading
import time

import protocol
from protocol import FrameBuffer, MalformedMessage, MessageTooLarge, encode_request

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:      # pragma: no cover
    pass

RECV_SIZE = 4096
PROMPT = "cmsp> "

# ใช้ ANSI escape ได้เฉพาะตอน stdout เป็น terminal จริง
USE_ANSI = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

HELP_TEXT = """
คำสั่งที่ใช้ได้
  auth <user> <token>                    ยืนยันตัวตน            -> 200 / 401
  sub <SYMBOL> [1s|5s|10s]               subscribe ราคา         -> 201 / 400 / 404 / 409 / 429
  unsub <SYMBOL>                         ยกเลิก subscribe       -> 202 / 410
  alert set <SYMBOL> above|below <price> ตั้งเตือน (one-shot)   -> 203 / 400 / 404 / 429
  alert del <id>                         ลบเตือน                -> 204 / 411
  list sub | list alert                  ดูรายการ               -> 200
  stats                                  สถิติ server           -> 200
  pause | resume                         หยุด/ต่อการ push ราคา  -> 207 / 208 / 400 / 410
  ping                                   ทดสอบสาย               -> 205
  raw <COMMAND> [Key=Value ...]          ส่ง request ดิบเอง (ไว้ทดสอบ error)
  help | quit
"""


class Client:
    def __init__(self, host, port, name, verbose=False, burst=False, stats_latency=False):
        self.host = host
        self.port = port
        self.name = name
        self.verbose = verbose
        self.burst = burst
        self.stats_latency = stats_latency

        self.sock = None
        self.frames = FrameBuffer()
        self.running = False
        self.print_lock = threading.Lock()
        self.latencies = []        # (recv_time - Timestamp) ของทุก 110 DATA หน่วย ms
        self.burst_sent = False
        self.last_seq = {}         # symbol -> Seq ล่าสุด ไว้ตรวจว่ามี gap ไหม

    # -- logging ----------------------------------------------------------
    def log(self, text, redraw_prompt=False):
        with self.print_lock:
            # \r + ล้างบรรทัด (ANSI) เพื่อไม่ให้ push ที่เข้ามาทับบรรทัดที่ผู้ใช้กำลังพิมพ์
            # ใช้เฉพาะตอนออกหน้าจอจริง ถ้าถูก redirect ลงไฟล์จะได้ไม่มีอักขระควบคุมปน
            if USE_ANSI:
                sys.stdout.write("\r\033[K")
            sys.stdout.write("[CLIENT] %s  %s\n" % (self._hms(), text))
            if redraw_prompt:
                sys.stdout.write(PROMPT)
            sys.stdout.flush()

    @staticmethod
    def _hms():
        t = time.time()
        return time.strftime("%H:%M:%S", time.localtime(t)) + ".%03d" % int((t % 1) * 1000)

    # -- socket -----------------------------------------------------------
    def connect(self):
        # AF_INET = IPv4, SOCK_STREAM = TCP
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # connect = ทำ three-way handshake กับ server
        self.sock.connect((self.host, self.port))
        self.running = True
        self.log("CONNECT → %s:%d" % (self.host, self.port))

        threading.Thread(target=self._reader, name="reader", daemon=True).start()
        # ส่ง HELLO อัตโนมัติทันทีหลัง connect ผู้ใช้ไม่ต้องพิมพ์เอง
        self.send("HELLO", Client_Name=self.name)

    def send(self, command, **headers):
        raw = encode_request(command, **headers)
        self._write(raw)
        tail = " ".join("%s=%s" % (k.replace("_", "-"), v) for k, v in headers.items())
        self.log(("SEND → %s %s" % (command, tail)).rstrip(), redraw_prompt=False)

    def _write(self, raw):
        if not self.running:
            return
        try:
            self.sock.sendall(raw)
            if self.verbose:
                self.log("[RAW SEND] %s" % protocol.raw_repr(raw))
        except OSError as exc:
            self.log("ERROR send failed: %s" % exc)
            self.running = False

    def send_burst(self):
        """ยิง PING + STATS + LIST ใน sendall() ครั้งเดียว

        จุดประสงค์: พิสูจน์ว่า TCP ไม่มีขอบเขตข้อความ - ทั้งสาม message จะไหลไปใน
        segment เดียว (ยิ่งเปิด TCP_NODELAY ยิ่งชัดว่ามันเป็นการเขียนครั้งเดียว)
        แล้ว server ต้องใช้ FrameBuffer แยกออกมาเป็น 3 message ได้ครบ
        """
        raw = (encode_request("PING")
               + encode_request("STATS")
               + encode_request("LIST", Type="SUB"))
        self.log("BURST → PING + STATS + LIST in one sendall (%d bytes)" % len(raw))
        # ไม่ต้อง log [RAW SEND] ที่นี่ - _write() log ให้แล้วเมื่อเปิด --verbose
        self._write(raw)

    # -- reader thread ----------------------------------------------------
    def _reader(self):
        while self.running:
            try:
                chunk = self.sock.recv(RECV_SIZE)
            except OSError:
                break
            if not chunk:
                self.log("SERVER closed the connection", redraw_prompt=True)
                break
            if self.verbose:
                self.log("[RAW RECV] %s" % protocol.raw_repr(chunk))

            # framing ฝั่ง client ใช้ FrameBuffer ตัวเดียวกับ server (สเปกหัวข้อ 6.2)
            try:
                frames = self.frames.feed_raw(chunk)
            except MessageTooLarge as exc:
                self.log("ERROR server sent an oversized message (%s)" % exc.detail)
                break
            for raw in frames:
                try:
                    self._on_message(protocol.decode(raw))
                except MalformedMessage as exc:
                    self.log("ERROR malformed message from server: %s" % exc.detail)
        self.running = False

    def _on_message(self, msg):
        if msg.kind != "response":
            self.log("RECV ← unexpected request from server: %s" % msg.start_line,
                     redraw_prompt=True)
            return

        code = msg.code
        head = "%d %s" % (code, msg.phrase)

        if code == 110:
            symbol = msg.get("Symbol")
            price = msg.get("Price", "?")
            change = msg.get("Change24h", "?")
            seq = msg.get("Seq")
            self._track_latency(msg)
            gap = self._track_seq(symbol, seq)
            self.log("PUSH ← %s  %s  %s  (%s%%)%s"
                     % (head, symbol, self._group(price), change, gap),
                     redraw_prompt=True)
            return

        if code in (111, 112, 503, 504):
            self.log("PUSH ← %s  %s" % (head, self._tail(msg)), redraw_prompt=True)
            return

        self.log("RECV ← %s  %s" % (head, self._tail(msg)), redraw_prompt=True)

        # --burst ยิงทันทีหลัง AUTH ผ่าน (STATS/LIST ต้อง auth ก่อนถึงจะไม่โดน 401)
        if self.burst and not self.burst_sent and code == 200 and msg.has("User"):
            self.burst_sent = True
            self.send_burst()

    @staticmethod
    def _tail(msg):
        return " ".join("%s=%s" % kv for kv in msg.headers.items())

    @staticmethod
    def _group(price):
        """ใส่ comma คั่นหลักพันให้อ่านง่ายเวลาเดโม"""
        try:
            value = float(price)
        except (TypeError, ValueError):
            return str(price)
        return "{:,.2f}".format(value) if value >= 1 else "{:,.6f}".format(value)

    def _track_latency(self, msg):
        if not self.stats_latency:
            return
        try:
            sent_ms = int(msg.get("Timestamp"))
        except (TypeError, ValueError):
            return
        self.latencies.append(time.time() * 1000.0 - sent_ms)

    def _track_seq(self, symbol, seq):
        """Seq เดินทุก tick จาก upstream แต่ throttle ทำให้ client ได้ไม่ครบ
        ช่องที่หายไปคือ tick ที่ server ตั้งใจทิ้ง - แสดงให้เห็นว่า throttle ทำงานจริง
        """
        try:
            seq = int(seq)
        except (TypeError, ValueError):
            return ""
        previous = self.last_seq.get(symbol)
        self.last_seq[symbol] = seq
        if previous is not None and seq > previous + 1:
            return "  [seq %d, ข้าม %d tick]" % (seq, seq - previous - 1)
        return "  [seq %d]" % seq

    # -- latency report ---------------------------------------------------
    def print_latency(self):
        if not self.stats_latency:
            return
        if not self.latencies:
            print("\n[CLIENT] latency: ไม่มีข้อมูล (ยังไม่ได้รับ 110 DATA)")
            return
        data = sorted(self.latencies)

        def pct(p):
            index = min(int(round((len(data) - 1) * p)), len(data) - 1)
            return data[index]

        print("\n[CLIENT] latency ของ 110 DATA (recv_time - Timestamp) จาก %d ตัวอย่าง"
              % len(data))
        print("         p50 = %.1f ms   p95 = %.1f ms   max = %.1f ms"
              % (pct(0.50), pct(0.95), data[-1]))
        if data[0] < 0:
            # Timestamp มาจากฟิลด์ E ของ Binance ซึ่งอ้างอิงนาฬิกาของ Binance
            # ถ้านาฬิกาเครื่องเราช้ากว่า ตัวเลขจะติดลบ - ไม่ใช่ว่าข้อมูลมาถึงก่อนถูกส่ง
            print("         หมายเหตุ: มีค่าติดลบ แปลว่านาฬิกาเครื่องนี้ไม่ตรงกับนาฬิกาของ upstream")
            print("         (ตัวเลขนี้จึงวัด clock skew ปนมาด้วย ถ้าอยากได้ latency ล้วนให้วัดในโหมด --mock")
            print("          ซึ่ง Timestamp ถูกสร้างจากนาฬิกาเครื่องเดียวกัน)")

    # -- REPL -------------------------------------------------------------
    def repl(self):
        print(HELP_TEXT)
        while self.running:
            try:
                line = input(PROMPT)
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not self.running:
                break
            line = line.strip()
            if not line:
                continue
            if not self.handle_command(line):
                break
        self.close()

    def handle_command(self, line):
        """คืน False เมื่อจะออกจากโปรแกรม"""
        parts = line.split()
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd in ("help", "?"):
            print(HELP_TEXT)
        elif cmd == "auth":
            if len(args) != 2:
                print("ใช้: auth <user> <token>")
            else:
                self.send("AUTH", User=args[0], Token=args[1])
        elif cmd == "sub":
            if not args:
                print("ใช้: sub <SYMBOL> [1s|5s|10s]")
            else:
                headers = {"Symbol": args[0].upper()}
                headers["Interval"] = args[1] if len(args) > 1 else "1s"
                self.send("SUB", **headers)
        elif cmd == "unsub":
            if not args:
                print("ใช้: unsub <SYMBOL>")
            else:
                self.send("UNSUB", Symbol=args[0].upper())
        elif cmd == "alert":
            self._cmd_alert(args)
        elif cmd == "list":
            kind = (args[0].upper() if args else "SUB")
            self.send("LIST", Type=kind)
        elif cmd == "stats":
            self.send("STATS")
        elif cmd == "pause":
            self.send("PAUSE")
        elif cmd == "resume":
            self.send("RESUME")
        elif cmd == "ping":
            self.send("PING")
        elif cmd == "burst":
            self.send_burst()
        elif cmd == "raw":
            self._cmd_raw(args)
        elif cmd in ("quit", "exit"):
            self.send("QUIT")
            time.sleep(0.2)      # รอรับ 206 GOODBYE ก่อนปิด socket
            return False
        else:
            print("ไม่รู้จักคำสั่ง %r (พิมพ์ help)" % cmd)
        return True

    def _cmd_alert(self, args):
        if len(args) >= 4 and args[0].lower() == "set":
            self.send("ALERT", Action="SET", Symbol=args[1].upper(),
                      Condition=args[2].upper(), Value=args[3])
        elif len(args) == 2 and args[0].lower() == "del":
            self.send("ALERT", Action="DEL", Alert_Id=args[1])
        else:
            print("ใช้: alert set <SYMBOL> above|below <price>  |  alert del <id>")

    def _cmd_raw(self, args):
        """ส่ง request ดิบ เช่น  raw SUB Symbol=BTCUSDT Interval=3s
        มีไว้ทดสอบ error path ที่ REPL ปกติสร้างไม่ได้ (เช่น interval ผิด, header ขาด)
        """
        if not args:
            print("ใช้: raw <COMMAND> [Key=Value ...]")
            return
        headers = {}
        for item in args[1:]:
            if "=" not in item:
                print("header ต้องเป็นรูปแบบ Key=Value")
                return
            key, value = item.split("=", 1)
            headers[key] = value
        self.send(args[0].upper(), **headers)

    def close(self):
        self.running = False
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.print_latency()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="CMSP/1.0 client (raw TCP socket)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9009)
    parser.add_argument("--name", default="demo", help="ชื่อที่ส่งใน HELLO")
    parser.add_argument("--verbose", action="store_true", help="log raw bytes")
    parser.add_argument("--burst", action="store_true",
                        help="ส่ง PING, STATS, LIST ติดกันทันทีหลัง auth โดยไม่รอ response")
    parser.add_argument("--stats-latency", action="store_true",
                        help="เก็บ latency ของ 110 DATA แล้วพิมพ์ p50/p95/max ตอนออก")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    client = Client(args.host, args.port, args.name, verbose=args.verbose,
                    burst=args.burst, stats_latency=args.stats_latency)
    try:
        client.connect()
    except OSError as exc:
        print("ต่อ %s:%d ไม่ได้: %s" % (args.host, args.port, exc))
        return 1
    try:
        client.repl()
    except KeyboardInterrupt:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
