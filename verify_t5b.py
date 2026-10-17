# -*- coding: utf-8 -*-
"""T5b 验收: MTTR/MTBF 基于状态迁移区间统计"""
import time

import requests

GW = "http://127.0.0.1:8090"


def rel():
    return requests.get(GW + "/gateway/reliability", params={"device": "DEV-MB-01"}, timeout=5).json()


def wait_state(state, timeout=25):
    t0 = time.time()
    while time.time() - t0 < timeout:
        devs = requests.get(GW + "/gateway/devices", timeout=5).json()
        if devs.get("DEV-MB-01", {}).get("state") == state:
            return True
        time.sleep(1)
    return False


# 确保从站在跑
requests.post(GW + "/sim/modbus/start", params={"port": 1502}, timeout=5)
assert wait_state("RUNNING"), "从站未恢复"
base = rel().get("failures", 0)

# 两次 故障->修复 循环
for cycle in range(2):
    requests.post(GW + "/sim/modbus/stop", timeout=5)
    assert wait_state("OFFLINE"), f"cycle{cycle} 未离线"
    requests.post(GW + "/sim/modbus/start", params={"port": 1502}, timeout=5)
    assert wait_state("RUNNING"), f"cycle{cycle} 未恢复"
time.sleep(2)

r = rel()
print("可靠性报告:", r)
assert r["failures"] == base + 2, f"故障次数应为 {base + 2}, 实际 {r['failures']}"
mttr_s = r["mttrMinutes"] * 60
mtbf_s = r["mtbfMinutes"] * 60
assert 1 <= mttr_s <= 60, f"MTTR 异常: {mttr_s}s"
assert mtbf_s > 0, "MTBF 应大于0"
assert 0 < r["availability"] < 100, "可用度应在开区间"
# 自洽: 可用度 = up/(up+down), 用未舍入的毫秒值计算, 避免分钟级舍入误差
up = r["upMs"]
down = r["downMs"]
assert abs(r["availability"] - round(up * 100.0 / (up + down), 2)) <= 0.01, "可用度与UpDown不自洽"
print(f"T5b 验收通过: failures={r['failures']} MTTR={mttr_s:.0f}s MTBF={mtbf_s:.0f}s 可用度={r['availability']}%")
