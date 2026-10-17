# -*- coding: utf-8 -*-
"""T3b 验收: SPC X̄-R 管制图与判异规则
阶段1: 30 个子组稳定数据(220±0.1) -> 管制限生效且零误报
阶段2: 1 个子组整体漂移到 244(低于阈值245, 但远超 UCL) -> 命中 R1
说明: 以运行级设备名隔离历史状态, 判异断言按 本设备+时间窗 过滤, 不依赖网关重启。
"""
import json
import time
import uuid

import paho.mqtt.client as mqtt
import requests

GW = "http://127.0.0.1:8090"
DEVICE = f"DEV-SPC-{uuid.uuid4().hex[:6].upper()}"
TOPIC = f"eap/dev/{DEVICE}/telemetry"

client = mqtt.Client(client_id=f"spc-feeder-{uuid.uuid4().hex[:6]}")
client.connect("127.0.0.1", 1883, keepalive=60)
client.loop_start()
t0 = int(time.time() * 1000)


def send(v):
    client.publish(TOPIC, json.dumps({"v": v}), qos=1)


def spc_state():
    return requests.get(GW + "/gateway/spc/state", timeout=5).json().get(f"{DEVICE}:v", {})


def wait_subgroups(n, timeout=40):
    t = time.time()
    st = {}
    while time.time() - t < timeout:
        st = spc_state()
        if st.get("subgroups", 0) >= n:
            return st
        time.sleep(0.5)
    return st


def new_violations(since):
    v = requests.get(GW + "/gateway/spc/violations", timeout=5).json()
    return [x for x in v if x.get("deviceCode") == DEVICE and x.get("time", 0) >= since]


# 阶段1: 稳定相 30 子组 × 5(子组间 ±0.05 交替微偏, 模拟自然波动)
for g in range(30):
    off = 0.05 if g % 2 == 0 else -0.05
    for k in range(5):
        send(220.0 + off + (0.1 if k % 2 == 0 else -0.1))
    time.sleep(0.05)
series = wait_subgroups(30)
print("稳定相: device=%s subgroups=%s limitsValid=%s CL=%.3f UCL=%.3f LCL=%.3f" % (
    DEVICE, series.get("subgroups"), series.get("limitsValid"), series.get("cl", 0),
    series.get("ucl", 0), series.get("lcl", 0)))
assert series.get("limitsValid") is True, "管制限未生效"
assert series.get("ucl", 0) > series.get("cl", 0) > series.get("lcl", 0), "管制限顺序错误"
stable_v = new_violations(t0)
assert len(stable_v) == 0, f"稳定相不应判异: {stable_v}"

# 阶段2: 漂移子组(244 < 阈值245, 但 > UCL)
t_drift = int(time.time() * 1000)
for k in range(5):
    send(244.0)
wait_subgroups(31)
drift_v = new_violations(t_drift)
print("漂移相判异:", drift_v)
assert any(x.get("rule") == "R1" for x in drift_v), "R1 未命中"

client.loop_stop()
client.disconnect()
print("T3b 验收通过: 管制限估计正确 + 稳定零误报 + 漂移命中 R1")
