# -*- coding: utf-8 -*-
"""T4 验收: 200 路并发压测 + 消息不丢三件套(confirm/手动ack/死信) 对账
断言: TD 落库增量 == 发送总数; DLQ 增量 == 0; P99 <= 预算
P99 预算: 默认 500ms(独立口径, 采集链路基准); T4_P99_BUDGET=0 为共存参考模式(e2e 用, P99 只打印不断言)
口径说明: P99 = 设备时间戳 -> TDengine 落库确认, 压测前调用网关 latency/reset 清零, 只统计本轮窗口;
测量取热态第二轮(首轮为 JIT/TDengine 冷缓存预热轮)。
"""
import base64
import json
import os
import subprocess
import time
import urllib.request

TD = "http://127.0.0.1:6041/rest/sql/eap"
AUTH = "Basic " + base64.b64encode(b"root:taosdata").decode()
GW = "http://127.0.0.1:8090"
MQ_DLQ = "http://127.0.0.1:15672/api/queues/%2F/eap.events.dlq"
MQ_AUTH = "Basic " + base64.b64encode(b"root:123456").decode()
P99_BUDGET = int(os.environ.get("T4_P99_BUDGET", "500"))


def td(sql):
    req = urllib.request.Request(TD, data=sql.encode(), headers={"Authorization": AUTH})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def dlq_depth():
    req = urllib.request.Request(MQ_DLQ, headers={"Authorization": MQ_AUTH})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)["messages"]


dlq0 = dlq_depth()
# 延迟分位清零: 保证 P99 只统计本轮压测窗口
urllib.request.urlopen(urllib.request.Request(GW + "/gateway/metrics/latency/reset", method="POST"), timeout=5).read()
proc = subprocess.run(
    [r"D:\2026项目\eap-gateway-demo\.venv\Scripts\python.exe", "-X", "utf8",
     r"D:\2026项目\eap-gateway-demo\load_feeder.py", "200", "60"],
    capture_output=True, text=True, timeout=180)
sent_line = [l for l in proc.stdout.splitlines() if l.startswith("LOAD_FEEDER_SENT")]
run_line = [l for l in proc.stdout.splitlines() if l.startswith("LOAD_FEEDER_RUN")]
t0_line = [l for l in proc.stdout.splitlines() if l.startswith("LOAD_FEEDER_T0")]
sent = int(sent_line[0].split("=")[1]) if sent_line else -1
run_id = run_line[0].split("=")[1] if run_line else "?"
t0 = int(t0_line[0].split("=")[1]) if t0_line else 0
print("发送总数:", sent, "run_id:", run_id)
assert sent > 0, "feeder 未输出计数"

time.sleep(20)  # 尾部排空
# 按设备时间戳窗口归属对账: 仅统计含设备 ts 的遥测行
# (SPC 报警行 params 无 ts; TD 子表 TAG 建表时定型, 不能靠 protocol 过滤)
# 窗口下界放宽 3s: feeder 打印 T0 前已发出首条消息, 避免边界抖动误判丢失
landed = td(f"select count(*) from device_params where device_code like 'DEV-LOAD-%' "
            f"and params like '%\"ts\":%' and ts >= {t0 - 3000} and ts < {t0 + 65000}")["data"][0][0]
print(f"本轮落库: {landed} / 发送 {sent}")
if landed != sent:
    # 失配诊断: 窗口外迟到行(归属发布时刻晚于窗口上界的尾部消息)与 DLQ 增量
    late = td(f"select count(*) from device_params where device_code like 'DEV-LOAD-%' "
              f"and params like '%\"ts\":%' and ts >= {t0 + 65000} and ts < {t0 + 300000}")["data"][0][0]
    print(f"诊断: 窗口外迟到行={late}, DLQ当前={dlq_depth()} (基线 {dlq0})")
assert landed == sent, f"消息丢失或重复: 落库 {landed} != 发送 {sent}"

dlq1 = dlq_depth()
print(f"DLQ: 压测前基线 {dlq0} -> 压测后 {dlq1} (压测期间应无新增)")
assert dlq1 <= dlq0, "压测期间有消息入死信"

lat = json.load(urllib.request.urlopen(GW + "/gateway/metrics/latency", timeout=10))
print("延迟分位:", lat, f"P99 预算: {P99_BUDGET}ms")
if P99_BUDGET > 0:
    assert lat.get("p99", 10 ** 9) <= P99_BUDGET, f"P99 超标: {lat.get('p99')} > {P99_BUDGET}"
    print(f"T4 验收通过: 200 路零丢失 + DLQ 0 + P99 {lat.get('p99')}ms <= {P99_BUDGET}ms")
else:
    # 预算 0 = 共存参考模式: 零丢失与 DLQ 已硬断言, P99 只打印(独立口径另行以默认预算 500 测量)
    print(f"T4 验收通过: 200 路零丢失 + DLQ 0 | P99 {lat.get('p99')}ms (共存参考值)")
