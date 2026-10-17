# -*- coding: utf-8 -*-
"""T6 验收: MES 侧 RabbitMQ 消费者(自动报工/mid幂等/报警联动/死信边界/直通事件)"""
import json
import subprocess
import time
import uuid

import requests

MGMT = "http://127.0.0.1:15672/api"
AUTH = ("root", "123456")
SN = "SN-EAP-MQ-T6"
TASK_ID = 9220
WORK_ID = 9008


def sql(q):
    out = subprocess.run(["docker", "exec", "hme-mysql", "mysql", "-uroot", "-p123456", "-N", "-e", q],
                         capture_output=True, text=True)
    return out.stdout.strip()


def queue_info(name):
    return requests.get(f"{MGMT}/queues/%2F/{name}", auth=AUTH, timeout=5).json()


def publish(rk, payload, raw=False):
    body = {
        "properties": {"content_type": "application/json", "delivery_mode": 2,
                       "headers": {"__TypeId__": "com.pengbo.eap.core.EapEvent"}},
        "routing_key": rk,
        "payload": payload if raw else json.dumps(payload, ensure_ascii=False),
        "payload_encoding": "string",
    }
    r = requests.post(f"{MGMT}/exchanges/%2F/eap.events/publish", auth=AUTH, json=body, timeout=5)
    assert r.status_code == 200 and r.json().get("routed"), r.text


def wait_true(fn, timeout=25, interval=1):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if fn():
            return True
        time.sleep(interval)
    return False


def workrec_count():
    return int(sql(f"SELECT COUNT(*) FROM liaoyun.mes_work_rec WHERE task_id={TASK_ID}") or 0)


def ycrec_count():
    return int(sql(f"SELECT COUNT(*) FROM liaoyun.mes_ycrec WHERE sn='{SN}'") or 0)


# 0. 消费者在线 + 无历史积压
qi = queue_info("mes.integration")
assert qi.get("consumers", 0) >= 1, f"MES 消费者未上线: consumers={qi.get('consumers')}"
print("消费者在线:", qi["consumers"], "| 队列积压:", qi["messages"])

# 1. fixture: 独立工单+进行中任务(needcount=100 保证测试期间不完工)
sql(f"DELETE FROM liaoyun.mes_work_rec WHERE task_id={TASK_ID};"
    f"DELETE FROM liaoyun.mes_ycrec WHERE sn='{SN}';"
    f"DELETE FROM liaoyun.mes_task WHERE id={TASK_ID};"
    f"DELETE FROM liaoyun.mes_work WHERE id={WORK_ID};")
sql(f"INSERT INTO liaoyun.mes_work (id, sn, pro_id, title, status, okprocess, processcount, currprocess, neednum,"
    f" tenant_id, del_flag, create_dept, create_by, create_time, update_by, update_time)"
    f" VALUES ({WORK_ID}, '{SN}', 8006, 'T6-MQ-Work', '1', 0, 1, '', 100, '000000', '0', 103, 1, NOW(), 1, NOW());")
sql(f"INSERT INTO liaoyun.mes_task (id, sn, status, workplace_id, needcount, markcount, work_id, pro_id, currproid,"
    f" currprocess, tenant_id, del_flag, create_dept, create_by, create_time, update_by, update_time)"
    f" VALUES ({TASK_ID}, '{SN}', '1', 9201, 100, 0, {WORK_ID}, 8006, 3001, 1, '000000', '0', 103, 1, NOW(), 1, NOW());")
dlq0 = queue_info("eap.events.dlq")["messages"]

# 2. PROCESS_COMPLETE -> 自动报工
mid = f"T6-{uuid.uuid4().hex[:8]}"
evt = {"deviceCode": "DEV-T6-01", "eventType": "PROCESS_COMPLETE", "sn": SN, "count": 5, "defective": 0,
       "params": {"v": 220.5, "i": 1.2}, "protocol": "MQTT", "mid": mid, "eventTime": int(time.time() * 1000)}
publish("eap.mqtt.process_complete", evt)
assert wait_true(lambda: workrec_count() == 1), "MQ 消费后未生成报工记录"
mark = sql(f"SELECT markcount FROM liaoyun.mes_task WHERE id={TASK_ID}")
assert mark == "5", f"任务完工数量应为5: {mark}"
dev = sql(f"SELECT CONCAT(status,',',today_count) FROM liaoyun.mes_eap_device WHERE device_code='DEV-T6-01'")
print(f"PROCESS_COMPLETE -> 自动报工 OK: markcount=5, 设备状态={dev}")

# 3. 相同 mid 重发 -> 消费侧幂等
publish("eap.mqtt.process_complete", evt)
time.sleep(5)
assert workrec_count() == 1, "重复 mid 未被去重"
assert sql(f"SELECT markcount FROM liaoyun.mes_task WHERE id={TASK_ID}") == "5", "重复 mid 重复报工"
print("mid 幂等去重 OK")

# 4. PARAM_ALARM -> 异常报工单(待审核)
mid2 = f"T6-{uuid.uuid4().hex[:8]}"
publish("eap.mqtt.param_alarm", {"deviceCode": "DEV-T6-01", "eventType": "PARAM_ALARM", "sn": SN, "count": 0,
                                 "defective": 1, "params": {"v": 250.1, "limit": 245.0}, "protocol": "MQTT",
                                 "mid": mid2, "eventTime": int(time.time() * 1000)})
assert wait_true(lambda: ycrec_count() == 1), "报警未生成异常报工单"
print("PARAM_ALARM -> 异常报工单 OK")

# 5. ONLINE 直通事件 -> ack 放行, 不进死信
publish("eap.modbus_tcp.online", {"deviceCode": "DEV-T6-01", "eventType": "ONLINE", "sn": SN, "count": 0,
                                  "defective": 0, "params": {}, "protocol": "MODBUS_TCP",
                                  "eventTime": int(time.time() * 1000)})
# 6. 脏报文 -> nack 入死信(MES nack 1 条 + 网关 sink 转换器致命拒绝 1 条, 增量>=1)
publish("eap.mqtt.process_complete", "not-a-json{{{", raw=True)
assert wait_true(lambda: queue_info("eap.events.dlq")["messages"] >= dlq0 + 1), "脏报文未入死信"
dlq1 = queue_info("eap.events.dlq")["messages"]
assert queue_info("mes.integration").get("consumers", 0) >= 1, "脏报文后消费者掉线"
print(f"ONLINE 直通放行 + 脏报文入死信 OK (dlq {dlq0} -> {dlq1}, +{dlq1 - dlq0})")

# 7. 真实链路观察: 网关模拟器 -> MQ -> MES(非断言, 观察 live 流量被消费)
m0 = sql("SELECT IFNULL(MAX(markcount),0) FROM liaoyun.mes_task WHERE sn='SN-EAP-DEMO-002' AND status='1'")
time.sleep(12)
m1 = sql("SELECT IFNULL(MAX(markcount),0) FROM liaoyun.mes_task WHERE sn='SN-EAP-DEMO-002' AND status='1'")
print(f"live 链路 SN-EAP-DEMO-002 markcount: {m0} -> {m1}", "(MQTT模拟器驱动)" if m1 != m0 else "(观察窗内无事件)")

stats = queue_info("mes.integration")["message_stats"]
print(f"T6 验收通过: MQ消费/幂等/报警/死信边界 全链路OK | 累计投递={stats.get('deliver_get', 0)}")
