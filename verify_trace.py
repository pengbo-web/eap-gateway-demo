# -*- coding: utf-8 -*-
"""SN 双向追溯验收"""
import sys

import requests

BASE = "http://127.0.0.1:8188"

# 正向: 件级追溯树
f = requests.get(BASE + "/mes/trace/forward/SN-EAP-DEMO-003", timeout=30).json()
assert f["code"] == 200, f
tasks = f["data"]["tasks"]
assert len(tasks) >= 1, "正向追溯无任务节点"
procs = tasks[0]["processes"]
assert len(procs) >= 1, "无报工工序节点"
with_snap = [p for p in procs if p.get("deviceCode") and p.get("params")]
print("正向: tasks=%d, 首任务工序节点=%d, 含设备+参数快照=%d" % (len(tasks), len(procs), len(with_snap)))
assert with_snap, "报工快照缺失"
print("正向样例节点:", {k: procs[0][k] for k in ("process", "deviceCode", "count", "operator")})

# 反向: 批次 -> 受影响工单 SN 群
b = requests.get(BASE + "/mes/trace/backward/1", timeout=30).json()
assert b["code"] == 200, b
sns = [w["workSn"] for w in b["data"]["affectedWorks"]]
print("反向: ledger_sn=1 受影响工单:", sns)
assert "SN-EAP-DEMO-003" in sns, "反向未命中演示工单"

# 空 SN 不报错
e = requests.get(BASE + "/mes/trace/forward/SN-NOT-EXIST", timeout=30).json()
assert e["code"] == 200 and e["data"]["tasks"] == [], e
print("空SN返回空树: OK")
print("SN 双向追溯验收通过")
