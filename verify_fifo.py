# -*- coding: utf-8 -*-
"""验证批次级 FIFO 出库分配: 出库40 -> 旧批30+新批10, 扣减行回填批次号"""
import sys

sys.path.insert(0, r"D:\2026项目\eap-gateway-demo")
import requests
from liao_api import login, BASE, CLIENT

tok = login()
h = {"Authorization": "Bearer " + tok, "clientid": CLIENT, "Content-Type": "application/json"}
r = requests.post(BASE + "/mes/outstock", json={"remark": "1", "proId": 7002, "count": 40, "kwid": 0}, headers=h)
print("出库响应:", r.text[:200])
assert r.json()["code"] == 200, r.text
