# -*- coding: utf-8 -*-
"""SECS/GEM 设备模拟器(raw socket 自研版, 字节级遵循 SEMI E37/E5)

HSMS 头(10字节): session u16 | stream u8(bit7=W) | function u8 | ptype u8 | stype u8 | system u32
stype: 0=Data 1=Select.req 2=Select.rsp 3=Linktest.req 4=Linktest.rsp 9=Separate.req
SECS-II item: format_byte=(code<<2)|nlen_bytes; code: List=0 B=8 A=16 F8=32 U4=44
行为: 应答 Select/Linktest/S1F13; 周期发 S6F11(W, 期望 S6F12); 应答 S2F41->S2F42(STOP/START 控制产出)
"""
import random
import socket
import struct
import threading
import time

PORT = 5020
producing = True

# ---------- SECS-II item 编码 ----------

def item(fmt_code, payload: bytes) -> bytes:
    n = len(payload)
    if n < 256:
        hdr = bytes([(fmt_code << 2) | 1, n])
    elif n < 65536:
        hdr = bytes([(fmt_code << 2) | 2]) + struct.pack(">H", n)
    else:
        hdr = bytes([(fmt_code << 2) | 3]) + struct.pack(">I", n)[1:]
    return hdr + payload


def li(items: bytes) -> bytes:
    return item(0, bytes([len(items)]) if False else _list_len(items) + items)


def _list_len(items: bytes) -> bytes:
    # List 的 payload 首字节为元素个数(1字节足够演示)
    return b""


def lst(count: int, body: bytes) -> bytes:
    return item(0, bytes([count]) + body)


def u4(v: int) -> bytes:
    return item(44, struct.pack(">I", v))


def f8(v: float) -> bytes:
    return item(32, struct.pack(">d", v))


def asc(s: str) -> bytes:
    return item(16, s.encode())


def bin_(b: bytes) -> bytes:
    return item(8, b)


# ---------- HSMS 帧 ----------

def hsms(session: int, stream: int, w: bool, function: int, stype: int, system: int, data: bytes = b"") -> bytes:
    header = struct.pack(">HBBBB I", session, (stream | (0x80 if w else 0)) & 0xFF, function, 0, stype, system)
    body = header + data
    return struct.pack(">I", len(body)) + body


def parse(buf: bytes):
    if len(buf) < 4:
        return None, buf
    (ln,) = struct.unpack(">I", buf[:4])
    if len(buf) < 4 + ln:
        return None, buf
    msg = buf[4:4 + ln]
    session, stream, function, ptype, stype, system = struct.unpack(">HBBBB I", msg[:10])
    return {"session": session, "stream": stream & 0x7F, "w": bool(stream & 0x80),
            "function": function, "ptype": ptype, "stype": stype, "system": system,
            "data": msg[10:]}, buf[4 + ln:]


SYSTEM = 0x10000000


def next_system():
    global SYSTEM
    SYSTEM += 1
    return SYSTEM


def s6f11(v: float, i: float) -> bytes:
    report = lst(2, u4(1001) + lst(2, f8(v) + f8(i)))
    body = lst(3, u4(0) + u4(1001) + lst(1, report))
    return body


def handle(conn: socket.socket):
    global producing
    buf = b""
    conn.settimeout(None)
    selected = False
    while True:
        try:
            chunk = conn.recv(4096)
        except OSError:
            return
        if not chunk:
            return
        buf += chunk
        while True:
            msg, buf = parse(buf)
            if msg is None:
                break
            st, sy = msg["stype"], msg["system"]
            if st == 1:  # Select.req
                conn.sendall(hsms(msg["session"], 0, False, 0, 2, sy, b"\x00"))
                selected = True
                print("[SIM] selected by host", flush=True)
            elif st == 3:  # Linktest.req
                conn.sendall(hsms(msg["session"], 0, False, 0, 4, sy))
            elif st == 0:
                s, f = msg["stream"], msg["function"]
                if s == 1 and f == 13:
                    conn.sendall(hsms(msg["session"], 1, False, 14, 0, sy, lst(2, bin_(b"\x00") + lst(0, b""))))
                    print("[SIM] S1F13 -> S1F14", flush=True)
                elif s == 2 and f == 41:
                    # data: L,2 [ A rcmd, L,0 ] 字节布局: 01 len 02 41 alen rcmd... 01 00
                    d = msg["data"]
                    rcmd = b""
                    if len(d) >= 6 and (d[0] & 0xFC) == 0x00 and (d[3] & 0xFC) == 0x40:
                        alen = d[4]
                        rcmd = d[5:5 + alen]
                    if rcmd == b"STOP":
                        producing = False
                    elif rcmd == b"START":
                        producing = True
                    print(f"[SIM] S2F41 rcmd={rcmd.decode()} -> S2F42", flush=True)
                    conn.sendall(hsms(msg["session"], 2, False, 42, 0, sy, lst(2, bin_(b"\x00") + lst(0, b""))))
                elif s == 6 and f == 12:
                    pass  # host ack


def producer(conn_holder: dict):
    while True:
        time.sleep(2)
        conn = conn_holder.get("conn")
        if not conn or not producing:
            continue
        v = round(random.uniform(200, 260), 2)
        i = round(random.uniform(10, 50), 2)
        try:
            conn.sendall(hsms(0, 6, True, 11, 0, next_system(), s6f11(v, i)))
            print(f"[SIM] S6F11 sent v={v} i={i}", flush=True)
        except OSError:
            conn_holder["conn"] = None


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", PORT))
    srv.listen(1)
    print(f"[SIM] raw SECS/GEM equipment on :{PORT}", flush=True)
    holder = {}
    threading.Thread(target=producer, args=(holder,), daemon=True).start()
    while True:
        conn, addr = srv.accept()
        holder["conn"] = conn
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
