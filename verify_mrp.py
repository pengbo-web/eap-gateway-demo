# -*- coding: utf-8 -*-
"""验证 MRP 递归展开: 4层BOM夹具 + 净需求计算"""
import sys

sys.path.insert(0, r"D:\2026项目\eap-gateway-demo")
from liao_api import login, get

tok = login()
resp = get("/mes/product/getbom/9006", tok)
assert resp["code"] == 200, resp
items = resp.get("data") or []


def depth(nodes, d=1):
    if not nodes:
        return 0
    return max(d, max(depth(n.get("bomlist") or [], d + 1) for n in nodes))


def walk(nodes, ind=0):
    for n in nodes:
        print("  " * ind + f"- {n.get('tilte')} sn={n.get('sn')} 毛需求={n.get('ncout')} "
                           f"净需求={n.get('netcount')} 现有库存={n.get('nowstock')}")
        walk(n.get("bomlist") or [], ind + 1)


print("顶层物料数:", len(items))
print("展开深度:", depth(items), "(原实现为1层)")
walk(items)
assert depth(items) >= 4, "递归展开未达4层"
print("MRP 递归展开验证通过")
