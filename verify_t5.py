# -*- coding: utf-8 -*-
"""T5 验收: 心跳离线判定 + 上线恢复 + Redis 配置缓存即时生效"""
import time

import requests

GW = "http://127.0.0.1:8090"


def devices():
    return requests.get(GW + "/gateway/devices", timeout=5).json()


def dstats():
    return requests.get(GW + "/gateway/stats/devices", timeout=5).json()


def delta(key, snap):
    return dstats().get(key, 0) - snap.get(key, 0)


# 0. 拉起 Modbus 从站模拟器(随网关进程, 重启后需恢复)并等待连接
requests.post(GW + "/sim/modbus/start", params={"port": 1502}, timeout=5)
t0 = time.time()
while time.time() - t0 < 30:
    if requests.get(GW + "/gateway/connections", timeout=5).json().get("DEV-MB-01", {}).get("connected"):
        break
    time.sleep(1)
time.sleep(3)

# 1. 在线设备清单
devs = devices()
assert "DEV-MB-01" in devs and devs["DEV-MB-01"]["state"] == "RUNNING", devs.keys()
print("设备清单:", list(devs.keys()), "| DEV-MB-01:", devs["DEV-MB-01"]["state"])

# 2. 停 Modbus 从站 -> 离线判定
snap = dstats()
requests.post(GW + "/sim/modbus/stop", timeout=5)
t0 = time.time()
offline_at = None
while time.time() - t0 < 20:
    if devices().get("DEV-MB-01", {}).get("state") == "OFFLINE":
        offline_at = time.time() - t0
        break
    time.sleep(1)
assert offline_at is not None, "20秒内未判定离线"
time.sleep(3)
off_events = delta("DEV-MB-01:MODBUS_TCP:OFFLINE", snap)
print(f"离线判定时延: {offline_at:.1f}s, OFFLINE 事件: {off_events}")
assert off_events >= 1, "未发 OFFLINE 事件"

# 3. 恢复从站 -> 上线事件
requests.post(GW + "/sim/modbus/start", params={"port": 1502}, timeout=5)
t0 = time.time()
online_at = None
snap = dstats()
while time.time() - t0 < 20:
    if devices().get("DEV-MB-01", {}).get("state") == "RUNNING":
        online_at = time.time() - t0
        break
    time.sleep(1)
assert online_at is not None, "20秒内未恢复上线"
time.sleep(2)
on_events = delta("DEV-MB-01:MODBUS_TCP:ONLINE", snap)
print(f"上线恢复时延: {online_at:.1f}s, ONLINE 事件: {on_events}")
assert on_events >= 1, "未发 ONLINE 事件"

# 4. 配置即时生效: 阈值 300(无报警) -> 205(大量报警) -> 还原 245
r = requests.post(GW + "/gateway/devices/DEV-MQ-01/config", json={"v": 300.0}, timeout=5)
print("配置更新:", r.json())
assert devices()["DEV-MQ-01"]["thresholds"]["v"] == 300.0, "配置未即时生效"
snap = dstats()
time.sleep(8)
alarms_high = delta("DEV-MQ-01:MQTT:PARAM_ALARM", snap)
print(f"阈值300期间报警数: {alarms_high}")
assert alarms_high == 0, "阈值300不应有报警"

requests.post(GW + "/gateway/devices/DEV-MQ-01/config", json={"v": 205.0}, timeout=5)
snap = dstats()
time.sleep(8)
alarms_low = delta("DEV-MQ-01:MQTT:PARAM_ALARM", snap)
print(f"阈值205期间报警数: {alarms_low}")
assert alarms_low > 0, "阈值205应产生报警"

requests.post(GW + "/gateway/devices/DEV-MQ-01/config", json={"v": 245.0}, timeout=5)
print("T5 验收通过: 离线/上线状态机 + 配置 Redis 缓存即时生效")
