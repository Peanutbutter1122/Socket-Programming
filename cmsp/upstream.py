"""
upstream.py - Binance WebSocket adapter

============================================================================
ไฟล์นี้เป็น 1 ใน 2 ไฟล์ที่ได้รับ "ยกเว้น" ให้ใช้ library ภายนอกได้
    ใช้: websocket-client (pip install websocket-client)

เหตุผลที่ยกเว้น: Binance ให้บริการผ่าน WebSocket over TLS (wss://) ซึ่งต้องทำ
HTTP Upgrade handshake, TLS, frame masking, ping/pong control frame ตาม RFC 6455
ซึ่งไม่ใช่หัวข้อที่วิชานี้ประเมิน และไม่เกี่ยวกับช่องทาง client<->server ของเราเลย

*** ช่องทางระหว่าง CMSP client กับ CMSP server ยังเป็น raw socket 100% ***
ไฟล์นี้อยู่ "หลัง" server เท่านั้น ทำหน้าที่แค่แปลง ticker ของ Binance
ให้เป็น tick กลางที่ server เข้าใจ แล้วส่งเข้า callback เดียวกับ mock_feed
============================================================================

ตรวจกับเอกสารทางการแล้ว (developers.binance.com/docs/binance-spot-api-docs/
web-socket-streams) ยืนยันว่า
    - base URL : wss://stream.binance.com:9443  (สำรอง: wss://stream.binance.com:443)
    - raw stream : /ws/<symbol ตัวพิมพ์เล็ก>@ticker
    - payload ของ <symbol>@ticker (24hr rolling window) มีฟิลด์ที่เราใช้ครบ
        e = event type ("24hrTicker")
        E = event time (Unix epoch มิลลิวินาที)
        s = symbol
        c = last price
        P = price change percent 24 ชั่วโมง
    - REST https://api.binance.com/api/v3/exchangeInfo คืน {"symbols":[{"symbol":..,
      "status":"TRADING"}, ...]} ใช้ตรวจว่า symbol มีอยู่จริงเพื่อแยก 404 ออกจาก 400
"""

import json
import threading
import time
import urllib.request

# endpoint หลักและสำรอง (บาง ISP/ประเทศบล็อกโดเมนหลัก จึงลองไล่ทีละตัว)
WS_ENDPOINTS = (
    "wss://stream.binance.com:9443/ws/%s@ticker",
    "wss://stream.binance.com:443/ws/%s@ticker",
    "wss://data-stream.binance.vision/ws/%s@ticker",
)
EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
EXCHANGE_INFO_TIMEOUT = 10.0
RECONNECT_DELAY = 5.0


class BinanceUpstream:
    """หน้าตาเหมือน MockFeed ทุกประการ server จึงสลับใช้ได้โดยไม่ต้องแก้โค้ด

        start() / stop() / open_symbol() / close_symbol() / has_symbol() / is_up()
    """

    name = "binance"

    # @ticker ส่งข้อมูลราววินาทีละครั้ง ถ้าเงียบเกิน 20 วิถือว่าผิดปกติ -> 504
    silence_timeout = 20.0

    def __init__(self, on_tick, on_status):
        self.on_tick = on_tick
        self.on_status = on_status

        self.lock = threading.Lock()
        self.streams = {}          # symbol -> WebSocketApp
        self.threads = {}          # symbol -> Thread
        self.wanted = set()        # symbol ที่ server สั่งเปิดค้างไว้
        self.symbols = None        # รายชื่อ symbol จาก exchangeInfo (None = ยังไม่รู้)
        self.up = True
        self.running = False

    # -- lifecycle --------------------------------------------------------
    def start(self):
        self.running = True
        # โหลดรายชื่อ symbol ไว้เบื้องหลัง เพื่อไม่ให้ server ค้างตอนเปิด
        threading.Thread(target=self._load_symbols, name="exchange-info",
                         daemon=True).start()

    def stop(self):
        self.running = False
        with self.lock:
            streams = list(self.streams.values())
            self.streams.clear()
        for ws in streams:
            try:
                ws.close()
            except Exception:
                pass

    # -- symbol management -------------------------------------------------
    def open_symbol(self, symbol):
        """server เรียกตอน refcount 0->1 = เปิด WebSocket ใหม่ 1 เส้นต่อ 1 symbol"""
        with self.lock:
            if symbol in self.wanted:
                return
            self.wanted.add(symbol)
        thread = threading.Thread(target=self._stream_loop, args=(symbol,),
                                  name="binance-%s" % symbol, daemon=True)
        with self.lock:
            self.threads[symbol] = thread
        thread.start()

    def close_symbol(self, symbol):
        """server เรียกตอน refcount 1->0 = ไม่มีใครดูแล้ว ปิดเส้นนี้ทิ้งเพื่อไม่เปลืองสาย"""
        with self.lock:
            self.wanted.discard(symbol)
            ws = self.streams.pop(symbol, None)
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def has_symbol(self, symbol):
        """ถ้ายังโหลด exchangeInfo ไม่ได้ ให้ผ่านไปก่อน แล้วไปตกที่ 503/504 แทนการโกหกว่า 404"""
        with self.lock:
            symbols = self.symbols
        if symbols is None:
            return True
        return symbol in symbols

    def known_symbols(self):
        with self.lock:
            return sorted(self.symbols) if self.symbols else []

    def is_up(self):
        return self.up

    # -- exchangeInfo (REST) ----------------------------------------------
    def _load_symbols(self):
        try:
            request = urllib.request.Request(
                EXCHANGE_INFO_URL, headers={"User-Agent": "cmsp-server/1.0"})
            with urllib.request.urlopen(request, timeout=EXCHANGE_INFO_TIMEOUT) as response:
                payload = json.loads(response.read().decode("utf-8"))
            symbols = {row["symbol"] for row in payload.get("symbols", [])
                       if row.get("status") == "TRADING"}
            with self.lock:
                self.symbols = symbols
        except Exception as exc:
            # ไม่ใช่เรื่องคอขาดบาดตาย: แค่แปลว่าเราแยก 404 ไม่ได้ ต้องปล่อยให้ SUB ผ่านไปก่อน
            self.on_status("up", "exchangeInfo unavailable (%s) - symbol check disabled" % exc)

    # -- WebSocket loop ----------------------------------------------------
    def _stream_loop(self, symbol):
        """1 เธรดต่อ 1 symbol: ต่อ endpoint ไล่ไปเรื่อยๆ หลุดแล้วต่อใหม่ทุก 5 วินาที"""
        try:
            import websocket        # websocket-client
        except ImportError:
            self.up = False
            self.on_status("down", "websocket-client is not installed (pip install -r requirements.txt)")
            return

        attempt = 0
        while self.running and symbol in self.wanted:
            url = WS_ENDPOINTS[attempt % len(WS_ENDPOINTS)] % symbol.lower()
            attempt += 1
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_message=lambda _ws, message: self._on_message(symbol, message),
                    on_error=lambda _ws, error: None,
                    on_close=lambda _ws, *_a: None,
                )
                with self.lock:
                    self.streams[symbol] = ws
                if not self.up:
                    self.up = True
                    self.on_status("up", "upstream reconnected")
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self.on_status("down", "upstream error: %s" % exc)

            if not (self.running and symbol in self.wanted):
                break
            # หลุดแล้ว: แจ้ง server ให้ push 112 + 503 ก่อน แล้วรอ 5 วิค่อยต่อใหม่
            if self.up:
                self.up = False
                self.on_status("down", "upstream disconnected (%s)" % symbol)
            time.sleep(RECONNECT_DELAY)

        with self.lock:
            self.streams.pop(symbol, None)

    def _on_message(self, symbol, message):
        """แปลง payload ของ Binance เป็น tick กลาง (ใช้เฉพาะฟิลด์ s, c, P, E)"""
        try:
            data = json.loads(message)
        except ValueError:
            return
        if data.get("e") != "24hrTicker":
            return
        try:
            price = float(data["c"])
            change = float(data["P"])
            event_ms = int(data["E"])
        except (KeyError, TypeError, ValueError):
            return
        if not self.up:
            self.up = True
            self.on_status("up", "upstream reconnected")
        self.on_tick(data.get("s", symbol), price, change, event_ms)
