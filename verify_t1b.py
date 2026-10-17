# -*- coding: utf-8 -*-
"""T1b 验收: MQTT 接入链路(发布模拟器 -> 网关订阅 -> 统一事件 -> MQ)"""
import time

import requests

GW = "http://127.0.0.1:8090"


def stats():
    return requests.get(GW + "/gateway/stats", timeout=5).json()


s1 = stats()
time.sleep(6)
s2 = stats()
print("分协议计数:", s1, "->", s2)
mqtt1 = s1.get("MQTT", 0)
mqtt2 = s2.get("MQTT", 0)
assert mqtt2 > mqtt1, "MQTT 事件未流入"
print("T1b 验收通过: MQTT 接入 %d -> %d" % (mqtt1, mqtt2))
