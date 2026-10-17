# -*- coding: utf-8 -*-
"""T3 验收: TDengine 时序 sink(落库/标签/降采样查询耗时)"""
import base64
import json
import time
import urllib.request

import requests

GW = "http://127.0.0.1:8090"
TD = "http://127.0.0.1:6041/rest/sql/eap"
AUTH = "Basic " + base64.b64encode(b"root:taosdata").decode()

# 从站模拟器随网关进程, 重启后需重新拉起
requests.post(GW + "/sim/modbus/start", params={"port": 1502}, timeout=5)
time.sleep(5)


def q(sql):
    req = urllib.request.Request(TD, data=sql.encode(),
                                 headers={"Authorization": AUTH, "Content-Type": "text/plain"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


n1 = q("select count(*) from device_params")["data"][0][0]
time.sleep(6)
n2 = q("select count(*) from device_params")["data"][0][0]
print(f"时序行数: {n1} -> {n2}")
assert n2 > n1, "sink 未落库"

tags = q("select device_code, protocol, count(*) from device_params partition by device_code, protocol")
print("分设备/协议:", tags["data"][:6])
protos = {row[1] for row in tags["data"]}
assert {"SECS_GEM", "MODBUS_TCP", "MQTT"} <= protos, f"协议覆盖不全: {protos}"

t0 = time.time()
r = q("select avg(v), max(i) from device_params partition by device_code interval(1h)")
ms = (time.time() - t0) * 1000
print(f"INTERVAL(1h) 降采样查询: {ms:.0f}ms, 行数 {len(r['data'])}")
assert ms < 2000, "降采样查询过慢"
print("T3 验收通过: 落库/三协议标签/降采样查询 OK")
