"""
mock_feed.py - แหล่งราคาปลอมสำหรับเดโมและเทสต์ (stdlib ล้วน ไม่ต่อเน็ต)

ทำไมต้องมี: Binance ถูกบล็อกในบางประเทศ และตอนสอบ/อัดวิดีโออาจไม่มีเน็ต
ทุกฟีเจอร์ของ server ต้องเดโมได้ครบในโหมดนี้

MockFeed มี "หน้าตา" (interface) เหมือน BinanceUpstream ใน upstream.py เป๊ะ:
    start() / stop() / open_symbol() / close_symbol() / has_symbol() / is_up()
    แล้วยิงข้อมูลออกทาง callback สองตัวที่ server ส่งเข้ามาตอนสร้าง
server.py จึงไม่รู้เลยว่ากำลังคุยกับ mock หรือ Binance จริง - สลับกันได้ด้วย --mock
"""

import random
import threading
import time

# ราคาตั้งต้นให้ใกล้ของจริง เวลาเดโมจะได้ดูสมจริง
BASE_PRICES = {
    "BTCUSDT": 63000.0,
    "ETHUSDT": 2450.0,
    "BNBUSDT": 580.0,
    "SOLUSDT": 145.0,
    "XRPUSDT": 0.52,
}

# tick ถี่กว่า interval ที่ client ขอได้ (client ขอเร็วสุด 1s)
# ตั้งไว้ 4 ครั้ง/วินาที เพื่อให้เห็นผลของ throttle ชัดๆ ว่า server ทิ้ง tick ส่วนเกินจริง
TICKS_PER_SECOND = 4

# ความแรงของการแกว่งราคาต่อ 1 tick (0.08%) แรงพอให้ alert ที่ตั้งใกล้ราคาปัจจุบันยิงได้ในไม่กี่วินาที
VOLATILITY = 0.0008

# ตายแล้วกลับมาเองใน 5 วินาที (ใช้กับ --fail-after)
RECOVER_AFTER = 5.0


class MockFeed:
    """random walk generator ที่เลียนแบบ @ticker stream ของ Binance"""

    name = "mock"

    # ถ้าไม่มี tick นานเกินเท่านี้ server จะถือว่า upstream เงียบ -> 504 UPSTREAM TIMEOUT
    # ตั้งสั้น (3 วิ) เพราะช่วง --fail-after ดับแค่ 5 วิ จะได้เดโม 504 ได้จริงในโหมด mock
    silence_timeout = 3.0

    def __init__(self, on_tick, on_status, fail_after=None,
                 ticks_per_second=TICKS_PER_SECOND, seed=None):
        self.on_tick = on_tick        # callback(symbol, price, change24h, event_ms)
        self.on_status = on_status    # callback(state, detail) โดย state = "up" / "down"
        self.fail_after = fail_after
        self.interval = 1.0 / ticks_per_second
        self.random = random.Random(seed)

        self.lock = threading.Lock()
        self.open_symbols = set()     # symbol ที่ refcount > 0 (server สั่งเปิด)
        self.prices = dict(BASE_PRICES)
        self.ref_prices = dict(BASE_PRICES)   # ราคาอ้างอิงไว้คิด Change24h
        self.up = True

        self.running = False
        self.thread = None
        self.started_at = None
        self.down_until = None

    # -- lifecycle --------------------------------------------------------
    def start(self):
        self.running = True
        self.started_at = time.monotonic()
        self.thread = threading.Thread(target=self._run, name="mock-feed", daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread is not None:
            self.thread.join(timeout=2.0)

    # -- symbol management (server เรียกตอน refcount 0->1 และ 1->0) ---------
    def open_symbol(self, symbol):
        with self.lock:
            self.open_symbols.add(symbol)

    def close_symbol(self, symbol):
        with self.lock:
            self.open_symbols.discard(symbol)

    def has_symbol(self, symbol):
        """mock รู้จักแค่ 5 ตัว เพื่อให้ทดสอบ 404 SYMBOL NOT FOUND ได้"""
        return symbol in BASE_PRICES

    def known_symbols(self):
        return sorted(BASE_PRICES)

    def is_up(self):
        return self.up

    def last_price(self, symbol):
        with self.lock:
            return self.prices.get(symbol)

    def set_price(self, symbol, price):
        """ตั้งราคาตั้งต้นของ tick ถัดไป - มีไว้ให้เทสต์บังคับสถานการณ์ที่ต้องการ

        random walk เป็นการสุ่ม เทสต์เรื่อง alert จึงอาจตกเพราะ "ราคาไม่วิ่งไปทางนั้น"
        ทั้งที่โค้ดถูก ตัวนี้ให้เทสต์กำหนดราคาได้ตรงๆ แล้ว tick ถัดไปจะไหลออกทาง
        callback เส้นเดิมทุกประการ (ไม่ได้ข้ามขั้นตอนใดของ server)
        """
        with self.lock:
            self.prices[symbol] = float(price)

    # -- loop -------------------------------------------------------------
    def _run(self):
        while self.running:
            time.sleep(self.interval)
            now = time.monotonic()

            # จำลอง upstream ตายตาม --fail-after แล้วกลับมาเองหลัง 5 วินาที
            if self._maybe_fail(now):
                continue

            with self.lock:
                symbols = sorted(self.open_symbols)
            for symbol in symbols:
                price, change = self._step(symbol)
                # event time เป็น Unix epoch หน่วยมิลลิวินาที เหมือนฟิลด์ E ของ Binance
                self.on_tick(symbol, price, change, int(time.time() * 1000))

    def _maybe_fail(self, now):
        """คืน True ถ้าตอนนี้ upstream ถือว่าดับอยู่ (ต้องข้ามการยิง tick)"""
        if self.up and self.fail_after is not None and self.started_at is not None:
            if now - self.started_at >= self.fail_after:
                self.up = False
                self.down_until = now + RECOVER_AFTER
                # ดับครั้งเดียวพอสำหรับเดโม ล้าง fail_after ทิ้งเพื่อไม่ให้ดับซ้ำ
                self.fail_after = None
                self.on_status("down", "simulated upstream failure")
                return True

        if not self.up:
            if self.down_until is not None and now >= self.down_until:
                self.up = True
                self.down_until = None
                self.on_status("up", "upstream reconnected")
                return False
            return True

        return False

    def _step(self, symbol):
        """เดินราคาแบบ random walk 1 ก้าว แล้วคิด Change24h เทียบราคาอ้างอิง"""
        with self.lock:
            price = self.prices.get(symbol, 100.0)
            price *= 1.0 + self.random.gauss(0.0, VOLATILITY)
            price = max(price, 0.000001)
            self.prices[symbol] = price
            ref = self.ref_prices.get(symbol, price)
        change = (price / ref - 1.0) * 100.0
        return price, change
