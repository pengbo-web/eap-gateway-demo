# -*- coding: utf-8 -*-
"""MQTT 设备模拟器: 周期发布 telemetry JSON 到 eap/dev/DEV-MQ-01/telemetry"""
import json
import random
import time

import paho.mqtt.client as mqtt

client = mqtt.Client(client_id="dev-mq-01-sim")
client.connect("127.0.0.1", 1883, keepalive=20)
client.loop_start()
print("[MQTT-SIM] publishing every 1s", flush=True)
while True:
    payload = {"v": round(random.uniform(200, 260), 2), "i": round(random.uniform(10, 50), 2)}
    client.publish("eap/dev/DEV-MQ-01/telemetry", json.dumps(payload), qos=1)
    time.sleep(1)
