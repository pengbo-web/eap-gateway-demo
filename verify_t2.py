# -*- coding: utf-8 -*-
"""T2 验收: SECS/GEM 主机通道(状态机/S6F11 接收/S2F41 下发)"""
import time

import requests

GW = "http://127.0.0.1:8090"


def secs_stats():
    return requests.get(GW + "/gateway/stats", timeout=5).json().get("SECS_GEM", 0)


def secs_state():
    return requests.get(GW + "/gateway/secs/status", timeout=5).json().get("DEV-SECS-01", {}).get("state")


# 1. HSMS 状态机到 ESTABLISHED
t0 = time.time()
while time.time() - t0 < 30 and secs_state() != "ESTABLISHED":
    time.sleep(1)
assert secs_state() == "ESTABLISHED", "HSMS 未建立: %s" % secs_state()
print("HSMS 状态: ESTABLISHED")

# 2. S6F11 事件流
c1 = secs_stats()
time.sleep(6)
c2 = secs_stats()
print(f"S6F11 事件: {c1} -> {c2}")
assert c2 > c1, "未收到 S6F11 事件"

# 3. S2F41 STOP -> 事件停
r = requests.post(GW + "/gateway/secs/command", params={"device": "DEV-SECS-01", "cmd": "STOP"}, timeout=10)
print("STOP ->", r.json())
time.sleep(3)
c3 = secs_stats()
time.sleep(6)
c4 = secs_stats()
print(f"STOP 后事件: {c3} -> {c4}")
assert c4 == c3, "STOP 后仍收到事件"

# 4. S2F41 START -> 事件恢复
r = requests.post(GW + "/gateway/secs/command", params={"device": "DEV-SECS-01", "cmd": "START"}, timeout=10)
print("START ->", r.json())
time.sleep(6)
c5 = secs_stats()
print(f"START 后事件: {c4} -> {c5}")
assert c5 > c4, "START 后事件未恢复"
print("T2 验收通过: HSMS 状态机 + S6F11 + S2F41/S2F42 全链路 OK")
