# -*- coding: utf-8 -*-
"""OEE 验收: 独立公式手算 vs 接口返回"""
import sys
from datetime import datetime, timedelta

import requests

sys.path.insert(0, r"D:\2026项目\eap-gateway-demo")

BASE = "http://127.0.0.1:8188"
OFFLINE_MIN = 5

now = datetime.now()
day = now.replace(hour=0, minute=0, second=0, microsecond=0)
t0 = day.replace(hour=8)
t1 = day.replace(hour=9)
t2 = day.replace(hour=9, minute=30)
cut = min(now, t2 + timedelta(minutes=OFFLINE_MIN))  # 离线截断
run_min = (t1 - t0).total_seconds() / 60 + max(0, (cut - t2).total_seconds() / 60)
alarm_min = (t2 - t1).total_seconds() / 60
load_min = 1440
qty, defe = 150, 6
a = min(1.0, run_min / load_min)
p = min(1.0, 6 * qty / (run_min * 60))
q = (qty - defe) / qty
oee = a * p * q
r1 = lambda x: round(x * 1000) / 10

exp = {"runMinutes": round(run_min * 10) / 10, "alarmMinutes": round(alarm_min * 10) / 10,
       "availability": r1(a), "performance": r1(p), "quality": r1(q), "oee": r1(oee),
       "qty": qty, "defective": defe}

resp = requests.get(BASE + "/mes/eap/oee", params={"deviceCode": "DEV-OEE-TEST"}, timeout=10).json()
assert resp["code"] == 200, resp
got = resp["data"]
print(f"{'指标':<14}{'期望':>10}{'接口':>10}  判定")
ok = True
for k, v in exp.items():
    g = got.get(k)
    match = abs(float(g) - float(v)) <= 0.11
    ok &= match
    print(f"{k:<14}{v:>10}{g:>10}  {'OK' if match else 'NG'}")
summ = requests.get(BASE + "/mes/eap/oee/summary", timeout=10).json()
devs = [d["deviceCode"] for d in summ["data"]["devices"]]
print("summary 设备数:", len(devs), "| 工厂OEE:", summ["data"]["factoryOee"], "| 含测试设备:", "DEV-OEE-TEST" in devs)
assert "DEV-OEE-TEST" in devs
assert ok, "OEE 数值与手算不一致"
print("OEE 验收通过: 三率与 OEE 均与独立手算一致(误差<=0.1)")
