"""gui_client.py - แอปหน้าจอสำหรับเฝ้าราคาคริปโตผ่าน CMSP/1.0

ใช้ tkinter ซึ่งเป็น stdlib ของ Python ไม่ใช่ไลบรารีภายนอก
ช่องทางระหว่าง client กับ server ยังเป็น raw TCP socket 100% เหมือน client.py
และใช้ protocol.py ไฟล์เดียวกันในการ encode/decode จึงไม่มีการเขียน parser ซ้ำ

โครงสร้าง 2 เธรด (เหตุผลเดียวกับ client.py)
    main thread    : วง event loop ของ tkinter วาดหน้าจอและรับคำสั่งจากผู้ใช้
    reader thread  : recv() + framing แล้วโยน message เข้า queue

tkinter ห้ามถูกเรียกจากเธรดอื่น ทุก message ที่ reader thread รับได้จึงถูกส่งผ่าน
queue.Queue แล้วให้ main thread มาดึงไปวาดเองทุก 80 มิลลิวินาทีด้วย after()

วิธีรัน:
    python gui_client.py                       # ต่อ 127.0.0.1:9009
    python gui_client.py --host 10.0.0.5 --port 9009
"""

import argparse
import queue
import socket
import threading
import time
import tkinter as tk
from tkinter import ttk

import protocol
from protocol import FrameBuffer, MalformedMessage, MessageTooLarge, encode_request

RECV_SIZE = 4096
POLL_MS = 80

INK = "#1a2229"
MUTED = "#5a6772"
ACCENT = "#2f6f9f"
UP = "#1c7a4a"
DOWN = "#b4322f"
BG = "#f4f6f8"


# ---------------------------------------------------------------------------
# ชั้นเชื่อมต่อ: socket + reader thread (ไม่รู้จัก tkinter เลย)
# ---------------------------------------------------------------------------

class Connection:
    """ห่อ raw socket ให้ใช้ง่ายขึ้น แล้วส่งเหตุการณ์ออกทาง queue

    เหตุการณ์ที่ส่งออกมีสามแบบ
        ("msg", Message)   message ที่ decode แล้ว
        ("raw", bytes)     byte ดิบที่เพิ่งรับมา (ใช้โหมดดู raw)
        ("bye", str)       สายหลุดหรือปิด พร้อมเหตุผล
    """

    def __init__(self, events):
        self.events = events
        self.sock = None
        self.frames = FrameBuffer()
        self.running = False

    def connect(self, host, port, timeout=5.0):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect((host, port))
        self.sock.settimeout(None)
        # ปิด Nagle: message ของ CMSP สั้นและต้องถึงเร็ว ไม่ควรถูกหน่วงเพื่อรวม packet
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.frames.reset()
        self.running = True
        threading.Thread(target=self._reader, name="cmsp-reader", daemon=True).start()

    def send(self, command, **headers):
        if not self.running or self.sock is None:
            return False
        raw = encode_request(command, **headers)
        try:
            self.sock.sendall(raw)
        except OSError as exc:
            self.running = False
            self.events.put(("bye", "ส่งข้อมูลไม่สำเร็จ: %s" % exc))
            return False
        self.events.put(("sent", (command, headers, raw)))
        return True

    def close(self):
        self.running = False
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    # -- reader thread --------------------------------------------------
    def _reader(self):
        while self.running:
            try:
                chunk = self.sock.recv(RECV_SIZE)
            except OSError:
                break
            if not chunk:
                self.events.put(("bye", "server ปิด connection"))
                break
            self.events.put(("raw", chunk))
            try:
                frames = self.frames.feed_raw(chunk)
            except MessageTooLarge as exc:
                self.events.put(("bye", "server ส่ง message ใหญ่เกินกำหนด (%s)" % exc.detail))
                break
            for raw in frames:
                try:
                    self.events.put(("msg", protocol.decode(raw)))
                except MalformedMessage as exc:
                    self.events.put(("bye", "message ผิดรูปแบบ: %s" % exc.detail))
        self.running = False


# ---------------------------------------------------------------------------
# หน้าจอ
# ---------------------------------------------------------------------------

class App(tk.Tk):
    def __init__(self, host, port, name):
        super().__init__()
        self.title("CMSP Monitor - เฝ้าราคาคริปโตผ่าน CMSP/1.0")
        self.geometry("1040x660")
        self.minsize(900, 600)
        self.configure(bg=BG)

        self.events = queue.Queue()
        self.conn = Connection(self.events)
        self.default_host = host
        self.default_port = port
        self.client_name = name

        self.rows = {}            # symbol -> item id ในตารางราคา
        self.alert_rows = {}      # alert id -> item id ในตารางเตือน
        self.pending_alert = None  # เก็บค่าที่เพิ่งสั่งตั้ง เพื่อผูกกับ Alert-Id ที่ตอบกลับมา
        self.paused = False
        self.authed = False
        self.show_raw = tk.BooleanVar(value=False)
        self.messages_in = 0
        self.messages_out = 0

        self._build_style()
        self._build_widgets()
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.after(POLL_MS, self._drain_events)

    # -- หน้าตา ----------------------------------------------------------
    def _build_style(self):
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure(".", background=BG, foreground=INK, font=("Segoe UI", 10))
        style.configure("TLabelframe", background=BG, borderwidth=1)
        style.configure("TLabelframe.Label", background=BG, foreground=ACCENT,
                        font=("Segoe UI Semibold", 10))
        style.configure("Treeview", rowheight=24, fieldbackground="white",
                        background="white", font=("Segoe UI", 10))
        style.configure("Treeview.Heading", font=("Segoe UI Semibold", 10))
        style.configure("Status.TLabel", foreground=MUTED)
        style.configure("Accent.TButton", font=("Segoe UI Semibold", 10))

    def _build_widgets(self):
        # ---------- แถบเชื่อมต่อ ----------
        bar = ttk.Frame(self, padding=(10, 8))
        bar.pack(fill="x")

        ttk.Label(bar, text="Server").pack(side="left")
        self.host_var = tk.StringVar(value=self.default_host)
        ttk.Entry(bar, textvariable=self.host_var, width=14).pack(side="left", padx=(6, 2))
        self.port_var = tk.StringVar(value=str(self.default_port))
        ttk.Entry(bar, textvariable=self.port_var, width=6).pack(side="left")

        ttk.Label(bar, text="ผู้ใช้").pack(side="left", padx=(14, 0))
        self.user_var = tk.StringVar(value="student")
        ttk.Entry(bar, textvariable=self.user_var, width=10).pack(side="left", padx=(6, 2))
        self.token_var = tk.StringVar(value="1234")
        ttk.Entry(bar, textvariable=self.token_var, width=8, show="*").pack(side="left")

        self.connect_btn = ttk.Button(bar, text="เชื่อมต่อ", style="Accent.TButton",
                                      command=self.on_connect)
        self.connect_btn.pack(side="left", padx=12)

        self.state_label = ttk.Label(bar, text="● ยังไม่ได้เชื่อมต่อ", foreground=MUTED)
        self.state_label.pack(side="left")

        ttk.Checkbutton(bar, text="ดู raw bytes", variable=self.show_raw).pack(side="right")

        # ---------- โซนกลาง ----------
        middle = ttk.Frame(self, padding=(10, 0))
        middle.pack(fill="both", expand=True)
        middle.columnconfigure(0, weight=3)
        middle.columnconfigure(1, weight=2)
        middle.rowconfigure(0, weight=1)

        # ----- ตารางราคา -----
        price_box = ttk.Labelframe(middle, text="ราคาที่กำลังติดตาม", padding=8)
        price_box.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(6, 6))
        price_box.rowconfigure(1, weight=1)
        price_box.columnconfigure(0, weight=1)

        sub_bar = ttk.Frame(price_box)
        sub_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.symbol_var = tk.StringVar(value="BTCUSDT")
        ttk.Entry(sub_bar, textvariable=self.symbol_var, width=12).pack(side="left")
        self.interval_var = tk.StringVar(value="1s")
        ttk.Combobox(sub_bar, textvariable=self.interval_var, values=["1s", "5s", "10s"],
                     width=5, state="readonly").pack(side="left", padx=6)
        ttk.Button(sub_bar, text="เพิ่ม (SUB)", command=self.on_sub).pack(side="left")
        ttk.Button(sub_bar, text="เอาออก (UNSUB)", command=self.on_unsub).pack(side="left", padx=6)
        self.pause_btn = ttk.Button(sub_bar, text="พักการอัปเดต (PAUSE)", command=self.on_pause)
        self.pause_btn.pack(side="right")

        columns = ("symbol", "price", "change", "interval", "seq", "updated")
        self.price_tree = ttk.Treeview(price_box, columns=columns, show="headings", height=10)
        for key, text, width, anchor in (
                ("symbol", "Symbol", 90, "w"),
                ("price", "ราคา", 110, "e"),
                ("change", "24 ชม.", 80, "e"),
                ("interval", "ทุก", 55, "center"),
                ("seq", "Seq", 70, "e"),
                ("updated", "อัปเดตล่าสุด", 110, "center")):
            self.price_tree.heading(key, text=text)
            self.price_tree.column(key, width=width, anchor=anchor)
        self.price_tree.grid(row=1, column=0, sticky="nsew")
        self.price_tree.tag_configure("up", foreground=UP)
        self.price_tree.tag_configure("down", foreground=DOWN)

        # ----- โซนแจ้งเตือน -----
        alert_box = ttk.Labelframe(middle, text="แจ้งเตือนราคา (server เป็นคนเฝ้าให้)", padding=8)
        alert_box.grid(row=0, column=1, sticky="nsew", pady=(6, 6))
        alert_box.rowconfigure(1, weight=1)
        alert_box.columnconfigure(0, weight=1)

        alert_bar = ttk.Frame(alert_box)
        alert_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.alert_symbol_var = tk.StringVar(value="BTCUSDT")
        ttk.Entry(alert_bar, textvariable=self.alert_symbol_var, width=10).pack(side="left")
        self.condition_var = tk.StringVar(value="ABOVE")
        ttk.Combobox(alert_bar, textvariable=self.condition_var, values=["ABOVE", "BELOW"],
                     width=7, state="readonly").pack(side="left", padx=4)
        self.value_var = tk.StringVar()
        ttk.Entry(alert_bar, textvariable=self.value_var, width=11).pack(side="left")
        ttk.Button(alert_bar, text="ตั้งเตือน", command=self.on_alert_set).pack(side="left", padx=4)
        ttk.Button(alert_bar, text="ลบ", command=self.on_alert_del).pack(side="left")

        alert_cols = ("id", "symbol", "cond", "value", "status")
        self.alert_tree = ttk.Treeview(alert_box, columns=alert_cols, show="headings", height=8)
        for key, text, width, anchor in (
                ("id", "id", 36, "center"),
                ("symbol", "Symbol", 82, "w"),
                ("cond", "เงื่อนไข", 70, "center"),
                ("value", "ระดับราคา", 90, "e"),
                ("status", "สถานะ", 86, "center")):
            self.alert_tree.heading(key, text=text)
            self.alert_tree.column(key, width=width, anchor=anchor)
        self.alert_tree.grid(row=1, column=0, sticky="nsew")
        self.alert_tree.tag_configure("fired", foreground=DOWN, font=("Segoe UI Semibold", 10))

        self.banner = tk.Label(alert_box, text="", bg=BG, fg=DOWN, anchor="w",
                               font=("Segoe UI Semibold", 10), wraplength=330, justify="left")
        self.banner.grid(row=2, column=0, sticky="ew", pady=(6, 0))

        # ---------- log ของโปรโตคอล ----------
        log_box = ttk.Labelframe(self, text="บันทึกข้อความ CMSP ทุกอันที่รับและส่ง", padding=8)
        log_box.pack(fill="both", expand=True, padx=10, pady=(0, 6))
        log_box.rowconfigure(0, weight=1)
        log_box.columnconfigure(0, weight=1)

        self.log = tk.Text(log_box, height=10, wrap="none", bg="white", fg=INK,
                           font=("Consolas", 9), relief="flat", borderwidth=1)
        self.log.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(log_box, orient="vertical", command=self.log.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        self.log.configure(yscrollcommand=scroll.set, state="disabled")
        self.log.tag_configure("send", foreground=ACCENT)
        self.log.tag_configure("recv", foreground=INK)
        self.log.tag_configure("push", foreground=UP)
        self.log.tag_configure("error", foreground=DOWN)
        self.log.tag_configure("raw", foreground=MUTED)

        # ---------- แถบสถานะ ----------
        status = ttk.Frame(self, padding=(10, 4))
        status.pack(fill="x")
        self.status_label = ttk.Label(status, text="พร้อมใช้งาน", style="Status.TLabel")
        self.status_label.pack(side="left")
        self.counter_label = ttk.Label(status, text="ส่ง 0 / รับ 0 message", style="Status.TLabel")
        self.counter_label.pack(side="right")

        for widget in (self.pause_btn,):
            widget.state(["disabled"])

    # -- คำสั่งจากผู้ใช้ ---------------------------------------------------
    def on_connect(self):
        if self.conn.running:
            self.conn.send("QUIT")
            self.after(200, lambda: self._disconnected("ปิดการเชื่อมต่อเอง"))
            return
        host = self.host_var.get().strip() or "127.0.0.1"
        try:
            port = int(self.port_var.get())
        except ValueError:
            self.set_status("หมายเลข port ไม่ถูกต้อง", error=True)
            return
        try:
            self.conn.connect(host, port)
        except OSError as exc:
            self.set_status("ต่อ %s:%d ไม่ได้ (%s)" % (host, port, exc), error=True)
            return

        self.authed = False
        self.paused = False
        self.state_label.configure(text="● เชื่อมต่อแล้ว", foreground=ACCENT)
        self.connect_btn.configure(text="ตัดการเชื่อมต่อ")
        self.set_status("ต่อกับ %s:%d แล้ว กำลังทักทายด้วย HELLO" % (host, port))
        self.append_log("CONNECT → %s:%d" % (host, port), "send")
        # client ส่ง HELLO ให้เองทันทีตามสเปก แล้วตามด้วย AUTH
        self.conn.send("HELLO", Client_Name=self.client_name)
        self.conn.send("AUTH", User=self.user_var.get().strip(),
                       Token=self.token_var.get().strip())

    def on_sub(self):
        symbol = self.symbol_var.get().strip().upper()
        if not symbol:
            return
        self.conn.send("SUB", Symbol=symbol, Interval=self.interval_var.get())

    def on_unsub(self):
        selected = self.price_tree.selection()
        symbol = (self.price_tree.set(selected[0], "symbol") if selected
                  else self.symbol_var.get().strip().upper())
        if symbol:
            self.conn.send("UNSUB", Symbol=symbol)

    def on_pause(self):
        self.conn.send("RESUME" if self.paused else "PAUSE")

    def on_alert_set(self):
        symbol = self.alert_symbol_var.get().strip().upper()
        value = self.value_var.get().strip()
        if not symbol or not value:
            self.set_status("ใส่ symbol และระดับราคาก่อนตั้งเตือน", error=True)
            return
        self.pending_alert = (symbol, self.condition_var.get(), value)
        self.conn.send("ALERT", Action="SET", Symbol=symbol,
                       Condition=self.condition_var.get(), Value=value)

    def on_alert_del(self):
        selected = self.alert_tree.selection()
        if not selected:
            self.set_status("เลือกรายการเตือนที่จะลบก่อน", error=True)
            return
        alert_id = self.alert_tree.set(selected[0], "id")
        self.conn.send("ALERT", Action="DEL", Alert_Id=alert_id)

    def on_close(self):
        if self.conn.running:
            self.conn.send("QUIT")
            time.sleep(0.15)
        self.conn.close()
        self.destroy()

    # -- วนรับเหตุการณ์จาก reader thread ----------------------------------
    def _drain_events(self):
        try:
            while True:
                kind, payload = self.events.get_nowait()
                if kind == "msg":
                    self.messages_in += 1
                    self._handle_message(payload)
                elif kind == "sent":
                    command, headers, raw = payload
                    self.messages_out += 1
                    tail = " ".join("%s=%s" % (k.replace("_", "-"), v)
                                    for k, v in headers.items())
                    self.append_log(("SEND → %s %s" % (command, tail)).rstrip(), "send")
                    if self.show_raw.get():
                        self.append_log("   [RAW SEND] %s" % protocol.raw_repr(raw), "raw")
                elif kind == "raw":
                    if self.show_raw.get():
                        self.append_log("   [RAW RECV] %s" % protocol.raw_repr(payload), "raw")
                elif kind == "bye":
                    self._disconnected(payload)
                self.counter_label.configure(
                    text="ส่ง %d / รับ %d message" % (self.messages_out, self.messages_in))
        except queue.Empty:
            pass
        self.after(POLL_MS, self._drain_events)

    def _handle_message(self, msg):
        code = msg.code
        head = "%d %s" % (code, msg.phrase)
        tail = " ".join("%s=%s" % kv for kv in msg.headers.items())

        if code == 110:
            self._on_price(msg)
            self.append_log("PUSH ← %s  %s" % (head, tail), "push")
            return
        if code == 111:
            self._on_alert_fired(msg)
            self.append_log("PUSH ← %s  %s" % (head, tail), "error")
            return
        if code in (112, 503, 504):
            self.set_status("upstream: %s" % (msg.get("Detail") or head), error=(code != 112))
            self.append_log("PUSH ← %s  %s" % (head, tail), "error")
            return

        self.append_log("RECV ← %s  %s" % (head, tail), "error" if code >= 400 else "recv")

        if code == 200 and msg.has("User"):
            self.authed = True
            self.state_label.configure(text="● พร้อมใช้งาน", foreground=UP)
            self.set_status("ยืนยันตัวตนสำเร็จ เพิ่มเหรียญที่ต้องการติดตามได้เลย")
        elif code == 201:
            self._ensure_row(msg.get("Symbol"), msg.get("Interval") or "1s")
            self.pause_btn.state(["!disabled"])
            self.set_status("ติดตาม %s ทุก %s แล้ว" % (msg.get("Symbol"), msg.get("Interval")))
        elif code == 202:
            symbol = msg.get("Symbol")
            item = self.rows.pop(symbol, None)
            if item:
                self.price_tree.delete(item)
            self.set_status("เลิกติดตาม %s แล้ว" % symbol)
        elif code == 203:
            self._add_alert_row(msg)
        elif code == 204:
            alert_id = msg.get("Alert-Id")
            item = self.alert_rows.pop(alert_id, None)
            if item:
                self.alert_tree.delete(item)
            self.set_status("ลบการเตือน id=%s แล้ว" % alert_id)
        elif code == 207:
            self.paused = True
            self.pause_btn.configure(text="ดูราคาต่อ (RESUME)")
            self.set_status("พักการอัปเดตราคาแล้ว การแจ้งเตือนยังทำงานอยู่")
        elif code == 208:
            self.paused = False
            self.pause_btn.configure(text="พักการอัปเดต (PAUSE)")
            self.set_status("กลับมารับราคาแล้ว ระหว่างที่พักไปพลาด %s ครั้ง"
                            % msg.get("Missed-Ticks"))
        elif code == 206:
            self.set_status("server บอกลาแล้ว (206 GOODBYE)")
        elif code >= 400:
            self.set_status("%s — %s" % (head, msg.get("Detail") or ""), error=True)

    # -- อัปเดตหน้าจอ ------------------------------------------------------
    def _ensure_row(self, symbol, interval):
        if symbol in self.rows:
            self.price_tree.set(self.rows[symbol], "interval", interval)
            return self.rows[symbol]
        item = self.price_tree.insert("", "end", values=(symbol, "รอข้อมูล", "-", interval, "-", "-"))
        self.rows[symbol] = item
        return item

    def _on_price(self, msg):
        symbol = msg.get("Symbol")
        item = self._ensure_row(symbol, self.price_tree.set(self.rows[symbol], "interval")
                                if symbol in self.rows else "1s")
        try:
            price = float(msg.get("Price"))
            price_text = "{:,.2f}".format(price) if price >= 1 else "{:,.6f}".format(price)
        except (TypeError, ValueError):
            price_text = msg.get("Price", "-")
        change = msg.get("Change24h", "-")
        self.price_tree.item(item, values=(
            symbol, price_text, "%s%%" % change, self.price_tree.set(item, "interval"),
            msg.get("Seq", "-"), time.strftime("%H:%M:%S")))
        try:
            self.price_tree.item(item, tags=("up" if float(change) >= 0 else "down",))
        except (TypeError, ValueError):
            pass

    def _add_alert_row(self, msg):
        alert_id = msg.get("Alert-Id")
        symbol = msg.get("Symbol") or (self.pending_alert[0] if self.pending_alert else "-")
        condition = msg.get("Condition") or (self.pending_alert[1] if self.pending_alert else "-")
        value = msg.get("Value") or (self.pending_alert[2] if self.pending_alert else "-")
        item = self.alert_tree.insert("", "end", values=(alert_id, symbol, condition, value, "ARMED"))
        self.alert_rows[alert_id] = item
        self.pending_alert = None
        self.set_status("ตั้งเตือน %s %s %s แล้ว (server เป็นคนเฝ้าให้)" % (symbol, condition, value))

    def _on_alert_fired(self, msg):
        alert_id = msg.get("Alert-Id")
        item = self.alert_rows.get(alert_id)
        if item:
            self.alert_tree.set(item, "status", "TRIGGERED")
            self.alert_tree.item(item, tags=("fired",))
        text = "%s %s %s แล้ว ราคาล่าสุด %s" % (
            msg.get("Symbol"), "ทะลุขึ้นเหนือ" if msg.get("Condition") == "ABOVE" else "ลงต่ำกว่า",
            msg.get("Threshold"), msg.get("Price"))
        self.banner.configure(text="[ALERT] " + text)
        self.set_status("แจ้งเตือน: " + text)
        try:
            self.bell()
        except tk.TclError:
            pass

    def _disconnected(self, reason):
        self.conn.close()
        self.authed = False
        self.paused = False
        self.state_label.configure(text="● หลุดการเชื่อมต่อ", foreground=DOWN)
        self.connect_btn.configure(text="เชื่อมต่อ")
        self.pause_btn.state(["disabled"])
        self.pause_btn.configure(text="พักการอัปเดต (PAUSE)")
        self.set_status(reason, error=True)
        self.append_log("DISCONNECT — %s" % reason, "error")

    def append_log(self, text, tag="recv"):
        self.log.configure(state="normal")
        self.log.insert("end", "%s  %s\n" % (time.strftime("%H:%M:%S"), text), tag)
        # เก็บ log ไม่ให้ยาวเกินไปจนกินหน่วยความจำระหว่างเดโมยาวๆ
        if int(self.log.index("end-1c").split(".")[0]) > 800:
            self.log.delete("1.0", "200.0")
        self.log.see("end")
        self.log.configure(state="disabled")

    def set_status(self, text, error=False):
        self.status_label.configure(text=text, foreground=DOWN if error else MUTED)


def main():
    parser = argparse.ArgumentParser(description="CMSP Monitor - แอปหน้าจอสำหรับเฝ้าราคา")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9009)
    parser.add_argument("--name", default="gui", help="ชื่อที่ส่งใน HELLO")
    args = parser.parse_args()
    App(args.host, args.port, args.name).mainloop()


if __name__ == "__main__":
    main()
