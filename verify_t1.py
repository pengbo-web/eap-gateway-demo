# -*- coding: utf-8 -*-
"""T1 验收: Modbus 采集流 + 断线重连恢复时间"""
import json
import time
import urllib.error
import urllib.request

import requests

GW = "http://127.0.0.1:8090"
MQ_API = "http://127.0.0.1:15672/api/queues/%2F/mes.integration"


def mq_stats(retry=30):
    import base64
    req = urllib.request.Request(MQ_API)
    req.add_header("Authorization", "Basic " + base64.b64encode(b"root:123456").decode())
    for _ in range(retry):
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                q = json.load(r)
                return q["messages"], q.get("message_stats", {}).get("publish", 0)
        except urllib.error.HTTPError:
            time.sleep(1)
    raise RuntimeError("队列始终不存在")


def wait_connected(code="DEV-MB-01", timeout=40):
    t0 = time.time()
    while time.time() - t0 < timeout:
        conn = requests.get(GW + "/gateway/connections", timeout=5).json()
        if conn.get(code, {}).get("connected"):
            return
        time.sleep(1)
    raise RuntimeError("设备未连接: " + code)


# 1. 启动从站模拟器并等待主站重连成功
print(requests.post(GW + "/sim/modbus/start", params={"port": 1502}, timeout=5).json())
wait_connected()
print("主站已连接从站")
time.sleep(3)

# 2. 采集流: 队列深度应持续增长
d1, p1 = mq_stats()
time.sleep(5)
d2, p2 = mq_stats()
print(f"队列深度: {d1} -> {d2}, 累计publish: {p1} -> {p2}")
assert p2 > p1, "采集事件未流入 MQ"

# 3. 断线重连: 停模拟器 -> 等退避 -> 起模拟器 -> 测恢复时间
requests.post(GW + "/sim/modbus/stop", timeout=5)
time.sleep(6)
t0 = time.time()
requests.post(GW + "/sim/modbus/start", params={"port": 1502}, timeout=5)
recovered = None
while time.time() - t0 < 40:
    conn = requests.get(GW + "/gateway/connections", timeout=5).json()
    if conn.get("DEV-MB-01", {}).get("connected"):
        recovered = time.time() - t0
        break
    time.sleep(1)
assert recovered is not None, "40秒内未重连恢复"
print(f"断线重连恢复时间: {recovered:.1f}s (含退避窗口)")

# 4. 恢复后采集流继续
d3, p3 = mq_stats()
time.sleep(6)
d4, p4 = mq_stats()
print(f"恢复后队列深度: {d3} -> {d4}, 累计publish: {p3} -> {p4}")
assert p4 > p3, "重连后采集未恢复"
print("T1 验收通过: 采集流 + 断线重连 OK")
