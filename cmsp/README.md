# CMSP/1.0 — Crypto Market Streaming Protocol

Streaming server + client ที่คุยกันด้วย **application-layer protocol ที่ออกแบบเอง** บน **raw TCP socket**
โปรเจกต์วิชา Computer Networks หัวข้อ Socket Programming

```
Binance WebSocket  ──►  CMSP Server  ──►  CMSP Clients (หลายตัว)
   (แหล่งข้อมูล)          (raw socket + CMSP)
```

สิ่งที่ถูกประเมินคือทุกอย่างระหว่าง server กับ client — Binance เป็นแค่ที่มาของตัวเลข
และเปลี่ยนเป็น **mock feed** ที่ทำงานได้ครบทุกฟีเจอร์โดยไม่ต้องต่ออินเทอร์เน็ต

---

## 1. ติดตั้ง

ต้องใช้ Python 3.11+

```bash
pip install -r requirements.txt
```

`requirements.txt` มีแค่สองตัว

| package | ใช้ที่ไหน | จำเป็นไหม |
|---|---|---|
| `websocket-client` | `upstream.py` (ต่อ Binance จริง) | ไม่ต้องมีก็ได้ ถ้ารันโหมด `--mock` |
| `pytest` | รัน unit test ของ `protocol.py` | เฉพาะตอนเทสต์ |

โหมด `--mock` ใช้ stdlib ล้วน ไม่ต้องลงอะไรเลยก็รันได้

---

## 2. รัน server

```bash
# ค่าตั้งต้น = ต่อ Binance จริง (ต้องมีเน็ต)
python server.py

# โหมด mock (ไม่ต้องต่อเน็ต ใช้เดโมและอัดวิดีโอตอนเน็ตไม่พร้อม)
python server.py --mock

# จำลอง upstream ตายหลัง 30 วินาที แล้วกลับมาเองใน 5 วินาที
python server.py --mock --fail-after 30

# โชว์ raw bytes ของทุก message ที่รับส่ง
python server.py --mock --verbose
```

| option | ค่าตั้งต้น | ความหมาย |
|---|---|---|
| `--host HOST` | `0.0.0.0` | IP ที่จะ bind |
| `--port PORT` | `9009` | port ที่จะ listen |
| `--mock` | ปิด | ใช้ mock feed แทน Binance (ไม่ใส่ = ต่อ Binance จริง) |
| `--fail-after SEC` | — | (ใช้กับ `--mock`) จำลอง upstream ตายหลัง SEC วินาที |
| `--verbose` | ปิด | log raw bytes ของทุก message |
| `--max-subs N` | `5` | subscription สูงสุดต่อ client |
| `--max-alerts N` | `10` | alert สูงสุดต่อ client |

---

## 3. รัน client

มีสองแบบที่พูดโปรโตคอลเดียวกัน ใช้ `protocol.py` และ raw socket ชุดเดียวกัน

### 3.1 แอปหน้าจอ (แนะนำสำหรับใช้งานจริงและถ่ายวิดีโอ)

```bash
python gui_client.py                          # ต่อ 127.0.0.1:9009
python gui_client.py --host 10.0.0.5 --port 9009
```

หน้าจอเดียวทำได้ครบ: กรอก server/ผู้ใช้แล้วกด **เชื่อมต่อ** (แอปส่ง `HELLO` + `AUTH` ให้เอง),
เพิ่ม/เอาเหรียญออกพร้อมเลือกความถี่ 1s / 5s / 10s ต่อเหรียญ, ตั้งและลบการแจ้งเตือน,
พัก/ต่อการอัปเดต (บอกด้วยว่าระหว่างพักพลาดไปกี่ tick) และติ๊ก **ดู raw bytes** เพื่อดู byte จริงบนสาย
ช่องล่างสุดคือ log ของ message CMSP ทุกอันที่รับและส่ง

ใช้ `tkinter` ซึ่งเป็น stdlib ไม่ใช่ไลบรารีภายนอก และไม่ได้แตะช่องทางสื่อสาร —
ยังเป็น `socket` ดิบเหมือนเดิม โดยแยกเป็น 2 เธรด: reader thread รับ message แล้วโยนเข้า `queue.Queue`
ให้ event loop ของ tkinter มาดึงไปวาดเอง (tkinter ห้ามถูกเรียกข้ามเธรด)

### 3.2 client แบบ CLI (ใช้เดโม error path และวัด latency)

```bash
python client.py                              # ต่อ 127.0.0.1:9009
python client.py --name somchai --verbose
python client.py --burst                      # พิสูจน์ว่า 1 TCP segment มีได้หลาย message
python client.py --stats-latency              # สรุป p50/p95/max ตอนออก
```

| option | ค่าตั้งต้น | ความหมาย |
|---|---|---|
| `--host HOST` | `127.0.0.1` | IP ของ server |
| `--port PORT` | `9009` | port ของ server |
| `--name NAME` | `demo` | ชื่อที่ส่งใน `HELLO` |
| `--verbose` | ปิด | log raw bytes |
| `--burst` | ปิด | ยิง PING + STATS + LIST ใน `sendall()` ครั้งเดียวหลัง auth |
| `--stats-latency` | ปิด | เก็บ `recv_time - Timestamp` แล้วพิมพ์ p50/p95/max ตอนออก |

client ส่ง `HELLO` ให้อัตโนมัติทันทีหลัง connect ผู้ใช้ไม่ต้องพิมพ์เอง

### คำสั่งใน REPL

| พิมพ์ | ส่ง | response ที่คาด |
|---|---|---|
| `help` | — | แสดงรายการคำสั่ง |
| `auth <user> <token>` | AUTH | 200 / 401 |
| `sub <SYMBOL> [1s\|5s\|10s]` | SUB | 201 / 400 / 404 / 409 / 429 |
| `unsub <SYMBOL>` | UNSUB | 202 / 410 |
| `alert set <SYMBOL> above\|below <price>` | ALERT SET | 203 / 400 / 404 / 429 |
| `alert del <id>` | ALERT DEL | 204 / 411 |
| `list sub` / `list alert` | LIST | 200 |
| `stats` | STATS | 200 |
| `pause` / `resume` | PAUSE / RESUME | 207 / 208 / 400 / 410 |
| `ping` | PING | 205 |
| `quit` | QUIT | 206 |
| `raw <CMD> [Key=Value ...]` | ส่ง request ดิบ | ไว้ทดสอบ error path เอง |

บัญชีที่ใช้ได้ (`users.json`): `student` / `1234` และ `somchai` / `s3cr3t`

symbol ที่ mock feed รู้จัก: `BTCUSDT` `ETHUSDT` `BNBUSDT` `SOLUSDT` `XRPUSDT`
(นอกจากนี้จะได้ `404 SYMBOL NOT FOUND`)

### เดโมสั้นๆ

```
cmsp> auth student 1234
cmsp> sub BTCUSDT 1s
cmsp> alert set BTCUSDT above 63100
cmsp> pause            # หยุดรับราคา แต่ alert ยังเข้าอยู่
cmsp> resume           # ได้ 208 พร้อม Missed-Ticks
cmsp> stats
cmsp> quit
```

---

## 4. โปรโตคอล CMSP/1.0 โดยย่อ

Text-based line-oriented UTF-8 โครงสร้างยืมแนวคิดจาก HTTP/1.1

```
<start line>\r\n
<Key>: <Value>\r\n
\r\n                    <-- บรรทัดว่างคือจุดจบของ message
```

- Request: `SUB CMSP/1.0`  ตามด้วย header
- Response/Push: `CMSP/1.0 201 SUBSCRIBED` ตามด้วย header
- ขนาดสูงสุดต่อ message **8192 bytes** เกินแล้วตอบ `413` และปิด connection
- response ที่มีหลายรายการใช้ `Count` + `Item-N` เพื่อคงหลัก **1 message = 1 frame**

### Status code ทั้งหมด

| Code | Phrase | ใช้เมื่อ |
|---|---|---|
| 110 | DATA | push ราคา |
| 111 | ALERT TRIGGERED | alert เข้าเงื่อนไข |
| 112 | SERVER NOTICE | แจ้งสถานะ เช่น upstream หลุด/กลับมา |
| 200 | OK | สำเร็จทั่วไป (HELLO, AUTH, LIST, STATS) |
| 201 | SUBSCRIBED | subscribe สำเร็จ |
| 202 | UNSUBSCRIBED | unsubscribe สำเร็จ |
| 203 | ALERT CREATED | ตั้ง alert สำเร็จ |
| 204 | ALERT DELETED | ลบ alert สำเร็จ |
| 205 | PONG | ตอบ PING |
| 206 | GOODBYE | ตอบ QUIT |
| 207 | STREAM PAUSED | หยุด push ชั่วคราว |
| 208 | STREAM RESUMED | ส่งต่อ พร้อม header `Missed-Ticks` |
| 400 | BAD REQUEST | format ผิด, header ขาด, ค่าไม่ถูกต้อง, สั่งก่อน HELLO, RESUME ตอนไม่ได้ pause |
| 401 | UNAUTHORIZED | ยังไม่ AUTH หรือ user/token ผิด |
| 404 | SYMBOL NOT FOUND | รูปแบบ symbol ถูกแต่ไม่มีบน upstream |
| 405 | UNKNOWN COMMAND | คำสั่งไม่รู้จัก |
| 409 | ALREADY SUBSCRIBED | subscribe symbol ที่ sub อยู่แล้ว |
| 410 | NOT SUBSCRIBED | unsub สิ่งที่ไม่ได้ sub, หรือ PAUSE ตอนไม่มี subscription |
| 411 | ALERT NOT FOUND | ลบ alert ที่ไม่มี หรือไม่ใช่ของ client นี้ |
| 413 | MESSAGE TOO LARGE | เกิน 8192 bytes |
| 426 | VERSION NOT SUPPORTED | version ไม่ใช่ CMSP/1.0 |
| 429 | LIMIT EXCEEDED | เกิน limit ของ subscription หรือ alert (ระบุใน `Detail`) |
| 500 | INTERNAL ERROR | exception ที่ไม่คาดคิด |
| 503 | UPSTREAM UNAVAILABLE | ต่อ upstream ไม่ได้ |
| 504 | UPSTREAM TIMEOUT | upstream เงียบเกิน timeout |

### State machine

```
                                        ┌──PAUSE──┐
                                        ▼         │
NEW ──HELLO──► GREETED ──AUTH──► READY ──SUB──► STREAMING ⇄ PAUSED
 │                │                 │              │  └──RESUME──┘
 └────────────────┴─────────────────┴──────────────┴── QUIT / error ──► CLOSED
```

---

## 5. โครงไฟล์

| ไฟล์ | หน้าที่ |
|---|---|
| `protocol.py` | encode/decode message, `FrameBuffer`, ตาราง status code — **ทั้ง server และ client ใช้ไฟล์นี้ร่วมกัน** |
| `server.py` | raw socket server, subscription table, alert engine, throttle, refcount |
| `client.py` | raw socket client + REPL |
| `gui_client.py` | แอปหน้าจอ (tkinter) พูดโปรโตคอลเดียวกัน — reader thread ส่ง message เข้า queue แล้วให้ event loop มาวาด |
| `mock_feed.py` | random walk price generator (stdlib ล้วน) |
| `upstream.py` | Binance WebSocket adapter **[ยกเว้นให้ใช้ library]** |
| `bench_http.py` | HTTP polling baseline สำหรับวัดผล **[ยกเว้นให้ใช้ library]** |
| `users.json` | บัญชีผู้ใช้สำหรับ AUTH |
| `tests/test_protocol.py` | unit test ของ protocol/framing (pytest) |
| `tests/acceptance.py` | รัน test matrix ทั้งชุดอัตโนมัติ |

### จุดที่ต้องอธิบายในวิดีโอ (มี comment กำกับไว้ในโค้ดแล้ว)

- **socket call** — `server.py:serve_forever()` (socket/setsockopt/bind/listen/accept), `client.py:connect()` (socket/connect)
- **แอปที่ใช้งานได้จริง** — `gui_client.py` (queue ระหว่าง reader thread กับ event loop, การ map message 110/111/112 ไปเป็นสิ่งที่ผู้ใช้เห็นบนหน้าจอ)
- **framing loop** — `protocol.py:FrameBuffer` และ `server.py:_recv_loop()`
- **lock 2 ชั้น** — `send_lock` ต่อ client (`server.py:_write()`) กับ `state_lock` ตัวเดียวคุม shared state
- **refcount** — `server.py:_cmd_sub()`, `_remove_subscription()` (ลดเลขใน lock) และ `_apply_refcount_drop()` (ปิด stream จริงนอก lock)
- **throttle + alert** — `server.py:on_tick()` (ประเมิน alert ทุก tick แต่ push ตาม interval)
- **ลำดับ message** — `server.py:_cmd_sub()` ถือ `send_lock` คร่อมช่วงลงทะเบียน→ตอบ `201`
  ไม่งั้น feed thread แทรก `110` ออกไปก่อน `201` ได้ (เทสต์ข้อ 34 จับเคสนี้)

---

## 6. Thread model

| Thread | หน้าที่ |
|---|---|
| main | accept loop รับ connection ใหม่ |
| client reader (1 ต่อ client) | `recv()` + framing + จัดการคำสั่ง + ตอบ response |
| feed (+ dispatcher) | รับ tick → เพิ่ม `Seq` → throttle → push + ประเมิน alert |
| watchdog | เฝ้าว่า upstream เงียบเกิน timeout หรือไม่ (`504`) |

สเปกอนุญาตให้รวม feed กับ dispatcher เป็นเธรดเดียว — ที่นี่รวมไว้จริง (`on_tick()` ทำงานต่อในเธรดของ feed เลย)
เหตุผลเขียนไว้ในหัวไฟล์ `server.py`

**ล็อกสองชั้นที่ต้องมี**

1. `send_lock` **ต่อ client หนึ่งตัว** — reader thread (ตอบ response) กับ feed thread (push ราคา) เขียนลง socket เดียวกัน ถ้าไม่ล็อก byte ของสอง message จะปนกันจน frame พัง
2. `state_lock` (RLock) **ตัวเดียว** คุม `clients` / `subscriptions` / `alerts` / `upstream_refcount` / `seq_counter`

**กฎเหล็ก: ห้ามเรียก `sendall()` ขณะถือ `state_lock`** — `on_tick()` จึงคัดรายชื่อผู้รับออกมาก่อน แล้วออกจาก lock ค่อยส่ง
ไม่งั้น client ที่รับช้าตัวเดียวจะบล็อกทั้ง server

กฎนี้ใช้กับ **ทุก** handler ไม่ใช่แค่ตอน push: `_cmd_sub()` / `_cmd_unsub()` / `_cmd_pause()` /
`_cmd_resume()` / `_alert_set()` / `_alert_del()` ตัดสินใจใน lock แล้วเก็บผลไว้ในตัวแปร
ออกจาก lock ก่อนจึงค่อยตอบ response เช่นเดียวกับการปิด upstream stream (`_apply_refcount_drop()`)
ที่ต้องปิด socket ของ WebSocket ซึ่งบล็อกได้

---

## 7. เทสต์

### unit test (protocol + framing)

```bash
pytest -q                 # หรือ python -m pytest -q
```

ครอบคลุม: message เดียวสมบูรณ์, หลาย message ใน chunk เดียว, message เดียวถูกแบ่งหลาย chunk,
delimiter ถูกผ่ากลาง, ป้อนทีละ byte, ขนาดเกิน 8192, header ไม่มี colon, version ผิด

### acceptance test (test matrix ทั้งชุด)

```bash
python tests/acceptance.py              # เต็มรูปแบบ (ช่วง pause 15 วินาทีตามสเปก)
python tests/acceptance.py --quick      # ย่อช่วง pause เหลือ 6 วินาที
python tests/acceptance.py --server-log # โชว์ log ของ server ไปด้วย
```

รันอัตโนมัติได้ 19 ใน 20 ข้อของ test matrix บวกเคสเสริมอีก 14 ข้อ (รวม 33 ข้อ)
เหลือข้อ 12 (`--verbose` โชว์ raw bytes) ที่ต้องดูด้วยตาบน console จริง

หมายเหตุ: เคสที่ต้องรอ alert ทำงาน (ข้อ 3, 22, 24) จะรอให้ random walk ของ mock feed
วิ่งไปชน threshold เองก่อน 8 วินาที ถ้ายังไม่ชน เทสต์จะเรียก `force_price()` ตั้งราคาให้ทะลุ
แล้วปล่อยให้ tick ไหลผ่าน path เดิมทุกขั้นตอน (feed → `Server.on_tick()` → alert engine)
ทำแบบนี้เพื่อไม่ให้ผลเทสต์ขึ้นกับความสุ่มของราคา ไม่ได้ข้ามขั้นตอนไหนของ server

### ตรวจข้อ 12 และ 20 ด้วยตา

```bash
python server.py --mock --port 9019 --verbose      # หน้าต่างที่ 1
python client.py --port 9019 --burst               # หน้าต่างที่ 2 แล้วพิมพ์ auth student 1234
```

ฝั่ง server จะเห็นว่า **1 `recv()` ได้ 3 message ติดกัน** ซึ่งพิสูจน์ว่า TCP ไม่มีขอบเขตข้อความ

```
[RAW RECV] b'PING CMSP/1.0\r\n\r\nSTATS CMSP/1.0\r\n\r\nLIST CMSP/1.0\r\nType: SUB\r\n\r\n'
RECV ← 51600  PING
RECV ← 51600  STATS
RECV ← 51600  LIST Type=SUB
```

### เดโมกรณี upstream ตาย (503 / 504 / 112)

```bash
python server.py --mock --fail-after 20
```

client ที่ sub อยู่จะได้ `112 SERVER NOTICE` → `503 UPSTREAM UNAVAILABLE` → `504 UPSTREAM TIMEOUT`
(เพราะเงียบเกิน 3 วินาที) → แล้ว `112` อีกครั้งตอนกลับมา จากนั้นราคาไหลต่อตามปกติ

### benchmark เทียบกับ HTTP polling

```bash
python bench_http.py --seconds 20
```

วัด byte ที่วิ่งบนสาย, จำนวน message, และความเก่าของข้อมูล (staleness) ของสองวิธีบน mock feed ตัวเดียวกัน

---

## 8. ไฟล์ที่ได้รับยกเว้นให้ใช้ library ภายนอก

**ระหว่าง CMSP client กับ CMSP server เป็น raw socket 100%** ไม่มี library ใดๆ ทั้งสิ้น
มีสองไฟล์ที่อยู่ *นอก* เส้นทางนั้นและได้รับการยกเว้น

| ไฟล์ | ใช้อะไร | ทำไมถึงยกเว้น |
|---|---|---|
| `upstream.py` | `websocket-client` | Binance ให้บริการผ่าน WebSocket over TLS ซึ่งต้องทำ HTTP Upgrade handshake + TLS + frame masking ตาม RFC 6455 ไม่ใช่หัวข้อที่วิชานี้ประเมิน และอยู่หลัง server ไม่เกี่ยวกับช่องทาง client↔server |
| `bench_http.py` | `http.server`, `http.client` | เป็น "คู่เทียบ" ไม่ใช่ส่วนหนึ่งของระบบ ถ้าเขียน HTTP เองด้วย raw socket การเทียบจะไม่ยุติธรรม เพราะจะกลายเป็นการเทียบกับ HTTP เวอร์ชันที่เราทำเองแบบง่ายๆ |

ตรวจได้ด้วย

```bash
grep -rn "^import\|^from" --include="*.py" .
```

---

## 9. ข้อจำกัดที่รู้ตัว

- **ไม่มี TLS** — ทุกอย่างวิ่งเป็น plaintext รวมถึง token ตอน `AUTH` ของจริงต้องหุ้มด้วย TLS
- **auth แบบง่าย** — เทียบ token ตรงๆ กับ `users.json` ไม่มี hash ไม่มีวันหมดอายุ
- **state อยู่ในหน่วยความจำ** — server ดับแล้ว subscription และ alert หายหมด
- **1 client = 1 thread** — เหมาะกับ client หลักสิบตัว ถ้าเป็นหลักพันต้องเปลี่ยนไปใช้ `select`/`epoll`
- **client ที่รับช้าบล็อก dispatcher ได้** — เพราะ push เขียนตรงลง socket ทางแก้คือ queue ต่อ client ซึ่งเกินขอบเขตงานนี้
- **`Missed-Ticks` นับรวมทุก symbol** — เป็นตัวเลขต่อ session ตามที่สเปกกำหนด ไม่ได้แยกรายตัว
- **prompt ถูกทับ** — ราคาที่ push เข้ามาจะแทรกบรรทัดที่กำลังพิมพ์ บรรเทาด้วยการวาด prompt ใหม่หลัง push และคำสั่ง `pause`/`resume`

---

## 10. หมายเหตุเรื่อง `--stats-latency`

`--stats-latency` วัด `recv_time - Timestamp` โดย `Timestamp` คือ event time ที่ upstream ประทับมา

- **โหมด `--mock`** — timestamp สร้างจากนาฬิกาเครื่องเดียวกัน ตัวเลขจึงเป็น latency ของระบบเราล้วนๆ (วัดได้ราว 0–1 ms บน localhost)
- **โหมดต่อ Binance จริง** — `Timestamp` มาจากฟิลด์ `E` ซึ่งอ้างอิงนาฬิกาของ Binance ถ้านาฬิกาเครื่องเราไม่ตรง ค่าที่ได้จะรวม clock skew เข้าไปด้วย และอาจติดลบได้ (client จะเตือนให้เองเมื่อเจอค่าติดลบ)

ถ้าจะเอาตัวเลข latency ไปใส่รายงาน ให้ใช้ตัวเลขจากโหมด `--mock` หรือเทียบกับ `bench_http.py` ซึ่งวัดสองวิธีบนนาฬิกาเดียวกัน
