# -*- coding: utf-8 -*-
"""200 路设备负载发生器: 每设备 1Hz 发布带设备时间戳的 telemetry, 结束打印发送总数"""
import json
import random
import sys
import time

import paho.mqtt.client as mqtt

n_dev = int(sys.argv[1]) if len(sys.argv) > 1 else 200
seconds = int(sys.argv[2]) if len(sys.argv) > 2 else 60

client = mqtt.Client(client_id="load-feeder")
client.connect("127.0.0.1", 1883, keepalive=30)
client.loop_start()
sent = 0
infos = []
t0_run = int(time.time() * 1000)
t_end = time.time() + seconds
seq = [0] * (n_dev + 1)
run_id = int(time.time())
while time.time() < t_end:
    t0 = time.time()
    for d in range(1, n_dev + 1):
        seq[d] += 1
        payload = {"v": round(random.uniform(215, 225), 2), "ts": int(t0 * 1000),
                   "mid": f"{run_id}-{d}-{seq[d]}"}
        infos.append(client.publish(f"eap/dev/DEV-LOAD-{d:03d}/telemetry", json.dumps(payload), qos=1))
        sent += 1
    elapsed = time.time() - t0
    if elapsed < 1:
        time.sleep(1 - elapsed)
# 确定性收尾: 逐条等待 QoS1 PUBACK(公共 API, 兼容 paho 1.x/2.x), 最长 30s
# (固定 sleep 在高负载下可能未排空就 disconnect, 造成 1~2 条假性丢失)
deadline = time.time() + 30
for mi in infos:
    remain = deadline - time.time()
    if remain <= 0:
        break
    if not mi.is_published():
        try:
            mi.wait_for_publish(remain)
        except (ValueError, RuntimeError):
            pass
time.sleep(0.5)
client.loop_stop()
client.disconnect()
print(f"LOAD_FEEDER_SENT={sent}")
print(f"LOAD_FEEDER_RUN={run_id}")
print(f"LOAD_FEEDER_T0={t0_run}")
