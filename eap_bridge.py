# -*- coding: utf-8 -*-
"""EAP 网关演示桥接器

消费 hm-MES iot-component 采集组件写入 Redis 的设备上报队列(Break_Redis_test),
按规则转换为 EAP 设备事件并推送至 Liao-mes 的 EAP 集成接口:
  - 电压 v 超限      -> PARAM_ALARM     (参数超限报警, MES 生成异常报工单)
  - 否则             -> PROCESS_COMPLETE (加工完成, MES 自动报工并推进工单工序)

链路: 模拟断路器 --Netty--> iot采集组件 --Redis list--> 本桥接器(EAP网关演示) --HTTP--> Liao-mes /mes/eap/event
"""
import json
import os
import time

import redis
import requests

REDIS_HOST = os.environ.get("EAP_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("EAP_REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("EAP_REDIS_DB", "15"))
QUEUE_KEY = os.environ.get("EAP_QUEUE_KEY", "Break_Redis_test")
MES_URL = os.environ.get("EAP_MES_URL", "http://127.0.0.1:8188/mes/eap/event")
MES_KEY = os.environ.get("EAP_MES_KEY", "eap-demo-key")
# 电压上限(伏), 超限触发 PARAM_ALARM
V_LIMIT = float(os.environ.get("EAP_V_LIMIT", "245"))
# 设备编码 -> MES 流转批次号映射, 未配置的设备走 DEFAULT_SN
DEVICE_SN_MAP = json.loads(os.environ.get("EAP_DEVICE_SN_MAP", "{}"))
DEFAULT_SN = os.environ.get("EAP_DEFAULT_SN", "SN-EAP-DEMO-003")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def map_sn(device_code: str) -> str:
    return DEVICE_SN_MAP.get(device_code, DEFAULT_SN)


def build_event(data: dict) -> dict:
    device = str(data.get("sn") or "UNKNOWN")
    v = float(data.get("v") or 0)
    i = float(data.get("i") or 0)
    base = {
        "deviceCode": device,
        "sn": map_sn(device),
        "count": 1,
        "eventTime": int(time.time() * 1000),
    }
    if v > V_LIMIT:
        base.update(eventType="PARAM_ALARM", defective=1,
                    params=json.dumps({"v": v, "i": i, "limit": V_LIMIT}, ensure_ascii=False))
    else:
        base.update(eventType="PROCESS_COMPLETE", defective=0,
                    params=json.dumps({"v": v, "i": i}, ensure_ascii=False))
    return base


def main() -> None:
    print(f"[EAP-BRIDGE] 消费 {QUEUE_KEY} (db{REDIS_DB}) -> {MES_URL}, 电压上限 {V_LIMIT}V", flush=True)
    while True:
        item = r.brpop(QUEUE_KEY, timeout=5)
        if not item:
            continue
        _, raw = item
        try:
            data = json.loads(raw)
        except Exception as exc:  # 脏消息不阻塞主循环
            print(f"[EAP-BRIDGE] 报文解析失败: {exc} raw={raw[:120]}", flush=True)
            continue
        event = build_event(data)
        try:
            resp = requests.post(MES_URL, json=event,
                                 headers={"X-EAP-Key": MES_KEY}, timeout=10)
            print(f"[EAP-BRIDGE] 设备={event['deviceCode']} v={data.get('v')} "
                  f"-> {event['eventType']} | MES: {resp.text[:100]}", flush=True)
        except Exception as exc:
            print(f"[EAP-BRIDGE] 转发失败: {exc}", flush=True)


if __name__ == "__main__":
    main()
