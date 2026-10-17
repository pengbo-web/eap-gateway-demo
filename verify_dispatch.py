# -*- coding: utf-8 -*-
"""派工规则引擎验收: 负载选站 -> 报工流转 -> 报警暂停改派"""
import json
import os
import subprocess
import sys

import requests

BASE = "http://127.0.0.1:8188"
KEY = {"X-EAP-Key": "eap-demo-key", "Content-Type": "application/json"}
FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def sql(q):
    out = subprocess.run(["docker", "exec", "hme-mysql", "mysql", "-uroot", "-p123456", "-N", "-e", q],
                         capture_output=True, text=True)
    return out.stdout.strip()


# 1. 下达派工: 首工序应落在低负载站 9201(9202 预置负载2)
r = requests.post(BASE + "/mes/dispatch/release/9007", timeout=10).json()
assert r["code"] == 200, r
t = r["data"]
print("release -> task", t["id"], "workplace", t["workplaceId"], "dispatch", t["workplaceDispatch"])
assert t["workplaceId"] == 9201, "应选低负载站9201"
assert t["workplaceDispatch"] == "RULE"

# 2. 派工计划透明化
p = requests.get(BASE + "/mes/dispatch/plan/9007", timeout=10).json()["data"]
assert len(p) == 2, p
c0 = [c["workplaceId"] for c in p[0]["candidates"]]
print("plan step1 candidates:", c0, "scores:", [c["score"] for c in p[0]["candidates"]])
assert c0[0] == 9201

# 3. 三次完工事件(neednum=3) -> 步骤1完工, 自动派工步骤2(9203/9204 平局取小id 9203)
for i in range(3):
    e = requests.post(BASE + "/mes/eap/event", data=open(os.path.join(FIX, "dispatch_event.json"), "rb").read(),
                      headers=KEY, timeout=10).json()
    print("event", i + 1, "->", e["msg"])
rows = sql("SELECT CONCAT(workplace_id,',',status,',',currproid) FROM liaoyun.mes_task WHERE sn='SN-DISPATCH-01' ORDER BY id")
print("tasks:", rows.splitlines())
lines = rows.splitlines()
assert len(lines) == 2, "应流转出第二工序任务"
assert lines[0].startswith("9201,2,"), "步骤1应完工于9201"
assert lines[1].startswith("9203,1,3002"), "步骤2应派至9203且进行中"

# 4. 报警(设备属于9203) -> 暂停9203 + 在制任务改派9204
a = requests.post(BASE + "/mes/eap/event", data=open(os.path.join(FIX, "dispatch_alarm.json"), "rb").read(),
                  headers=KEY, timeout=10).json()
print("alarm ->", a["msg"])
sus = sql("SELECT workplace_id FROM liaoyun.mes_workplace_suspend")
print("suspend rows:", sus)
assert "9203" in sus, "9203 应被暂停"
rows2 = sql("SELECT CONCAT(workplace_id,',',status,',',workplace_dispatch) FROM liaoyun.mes_task WHERE sn='SN-DISPATCH-01' ORDER BY id")
print("tasks after alarm:", rows2.splitlines())
assert rows2.splitlines()[1].startswith("9204,1,RULE"), "在制任务应改派至9204"
print("派工规则引擎验收通过: 负载选站/完工流转/报警改派 全链路OK")
