# -*- coding: utf-8 -*-
"""简历口径补测: (1) TDengine 10万点趋势查询耗时 (2) 50路并发 P99"""
import base64
import json
import subprocess
import time
import urllib.request

TD = "http://127.0.0.1:6041/rest/sql/eap"
AUTH = "Basic " + base64.b64encode(b"root:taosdata").decode()
GW = "http://127.0.0.1:8090"


def td(sql):
    req = urllib.request.Request(TD, data=sql.encode(), headers={"Authorization": AUTH})
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=30) as r:
        body = json.load(r)
    return body, (time.perf_counter() - t0) * 1000


# ---------- (1) 10万点趋势查询 ----------
total = td("SELECT COUNT(*) FROM device_params")[0]["data"][0][0]
print(f"device_params 总行数: {total}")
# 取行数最密集的时间窗: 直接用全表按 10s 降采样(扫描全部行 >= 10万)
times = []
for i in range(5):
    _, ms = td("SELECT _wstart, AVG(v), MAX(v), MIN(v), COUNT(*) FROM device_params "
               "WHERE ts >= NOW-12h INTERVAL(10s)")
    times.append(ms)
times.sort()
print(f"全表12h降采样趋势查询 x5: {[round(t) for t in times]} ms, 中位={round(times[2])}ms")
cnt = td("SELECT COUNT(*) FROM device_params WHERE ts >= NOW-12h")[0]["data"][0][0]
print(f"该窗口扫描点数: {cnt}")

# ---------- (2) 50 路并发 P99 ----------
urllib.request.urlopen(urllib.request.Request(GW + "/gateway/metrics/latency/reset", method="POST"), timeout=5).read()
proc = subprocess.run(
    [r"D:\2026项目\eap-gateway-demo\.venv\Scripts\python.exe", "-X", "utf8",
     r"D:\2026项目\eap-gateway-demo\load_feeder.py", "50", "60"],
    capture_output=True, text=True, timeout=180)
sent = [l for l in proc.stdout.splitlines() if l.startswith("LOAD_FEEDER_SENT")]
print("feeder:", sent[0] if sent else "无输出")
time.sleep(20)
lat = json.load(urllib.request.urlopen(GW + "/gateway/metrics/latency", timeout=10))
print("50路延迟分位:", lat)
