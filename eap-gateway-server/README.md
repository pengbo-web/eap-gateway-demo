# EAP 数据采集网关(eap-gateway-server)

面向半导体/离散制造的轻量 EAP 采集网关(JDK 17 / Spring Boot 3.4 / Netty 4.1):
多协议设备接入 → 统一事件模型 → RabbitMQ 事件总线 → 时序落库 / SPC 判异 / 可靠性统计 → MES 集成。

## 1. 架构

```
 Modbus TCP 从站(模拟器内置) ──┐
 SECS/GEM 设备(HSMS-2 主动连接) ─┤   协议适配层        EventFactory            EapEventPublisher
 MQTT 遥测(mosquitto broker) ───┘  (Netty/paho)  →  阈值判定/批次映射/SPC  →  Redis SETNX 发布去重
                                                        │                        │
                                                        ▼                        ▼
                                              SpcMonitor X̄-R 判异        RabbitMQ topic: eap.events
                                              (判异→PARAM_ALARM 事件)       rk = eap.{protocol}.{eventType}
                                                                                 │
              ┌──────────────────────────────────────────────────────────────────┤
              ▼                                                                  ▼
   队列 eap.tdengine.sink                                              队列 mes.integration
   手动ack+死信, 消费幂等(SETNX)                                        Liao-MES 消费(手动ack+死信)
   TDengine 超级表 eap.device_params                                    自动报工/异常联动(消费幂等 SETNX)
   (DURATION 7 分区, 每设备子表)

 监控面: DeviceMonitorService 心跳状态机(ONLINE/OFFLINE 事件) | ReliabilityService MTTR/MTBF/可用度
        LatencyStats 端到端延迟分位(设备时间戳→落库) | DeviceRegistry 阈值/SN映射(Redis 缓存, 热更新)
```

三跳幂等: 发布侧 `eap:mid:` → 时序落库侧 `eap:sink:mid:` → MES 消费侧 `eap:mes:mid:`(Redis SETNX, 10min TTL),
配合 publisher-confirm、手动 ack、死信队列 `eap.events.dlq`,构成不丢不重链路。

## 2. 模块导览

| 包 | 职责 |
| --- | --- |
| `core` | EapEvent 统一事件模型、EventFactory 阈值/映射/SPC 触发、EapEventPublisher 发布+去重+计数 |
| `protocol.modbus` | 自写 MBAP 编解码(长度=2+data)、Master 客户端(事务号匹配/超时重发/断线重连)、从站模拟器、轮询器 |
| `protocol.secs` | 自写 HSMS-2/SECS-II 编解码(format_byte/10字节头/Select/Linktest 状态机)、S2F41 远程指令、S6F11 事件接收 |
| `protocol.mqtt` | paho 订阅 `eap/dev/+/telemetry`, QoS1 |
| `sink` | TDengine REST 落库(HikariCP 池化, epoch-long 时间戳绑定避免时区偏移) |
| `monitor` | 心跳离线状态机、SPC X̄-R(A2/D4 系数表, R1/R2/R3 判异)、MTTR/MTBF、延迟分位、模拟器控制 |

## 3. REST 接口(验收/运维)

| 接口 | 说明 |
| --- | --- |
| `GET /gateway/devices` | 设备清单: 状态/协议/静默时长/阈值/SN映射 |
| `POST /gateway/devices/{code}/config` | 阈值热更新(Redis 源+本地即时生效) |
| `GET /gateway/stats` `/gateway/stats/devices` | 分协议 / 设备×事件类型计数 |
| `GET /gateway/metrics/latency` | 端到端延迟 p50/p95/p99/max |
| `POST /gateway/metrics/latency/reset` | 延迟样本清零(压测前调用, 保证本轮测量口径) |
| `GET /gateway/spc/state` `/gateway/spc/violations` | SPC 管制图状态与判异记录 |
| `GET /gateway/reliability?device=` | failures/upMs/downMs/MTTR/MTBF/可用度 |
| `GET /gateway/secs/status` `POST /gateway/secs/command` | HSMS 通道状态 / S2F41 指令下发 |
| `POST /sim/modbus/start|stop` | 内置 Modbus 从站模拟器开关(故障注入用) |

## 4. 快速启动

依赖容器(端口/凭据与 `application.yml` 对应):

```bash
docker run -d --name hme-rabbit -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=root -e RABBITMQ_DEFAULT_PASS=123456 rabbitmq:3-management
docker run -d --name ecom-redis -p 6379:6379 redis:7
docker run -d --name hme-tdengine -p 6041:6041 tdengine/tdengine:3.3.2.0
docker run -d --name hme-mosquitto -p 1883:1883 eclipse-mosquitto:2 -c /mosquitto/config/mosquitto.conf  # allow_anonymous true
```

构建与启动顺序: 容器 → 网关 → Liao-MES → 外部模拟器(仓库根目录 `mqtt_pub_sim.py`, `secs_sim_raw.py`):

```bash
mvn package -DskipTests
java -jar target/eap-gateway-server-1.0.0.jar
# 内存受压的同机演示环境建议限堆, 减少换页对尾延迟的影响:
#   网关: java -Xms128m -Xmx512m -jar target/eap-gateway-server-1.0.0.jar
#   MES : java -Xms256m -Xmx1024m -jar liaoyun-admin.jar
```

关键配置(`application.yml`): `eap.gateway.modbus-targets`(host:port:unitId:pollMs)、`secs-targets`、
`device-sn-map`(设备→MES 流转批次)、`thresholds`(参数告警上限)、`offline-factor`(离线判定=pollMs×倍数)。

## 5. 验收结果(verify_t*.py 实测)

| 项 | 内容 | 实测 |
| --- | --- | --- |
| T1 | Modbus 断线重连 | 拔线→恢复 1.1s 内重连, 期间事件不丢 |
| T1b | 超时重发 | 事务号匹配, 慢响应触发重发不串包 |
| T2 | HSMS/SECS-II | Select.req→ESTABLISHED, S6F11 解析, S2F41 START/STOP 远控生效 |
| T3 | 三协议汇聚+落库 | Modbus/MQTT/SECS 同轨消费, 设备时间戳→TDengine INTERVAL 27ms |
| T3b | SPC X̄-R | CL=220.022 UCL=220.137 LCL=219.906(n=5, A2/D4), 注入 244 命中 R1 超管制界限 |
| T4 | 200 路压测 | 12000/12000 零丢失(发送=落库对账), DLQ +0; 独立口径 p50=127 p95=288 **p99=422ms** max=517ms(低负载时段曾测得 p99=244ms; 与 MES 同机共存时 P99 仅作参考输出, e2e 对零丢失与 DLQ 硬断言) |
| T5 | 心跳与配置 | 停从站 4.1s 判离线发 OFFLINE, 恢复发 ONLINE; 阈值 300→0 报警 / 205→7 报警, 热更即时生效 |
| T5b | MTTR/MTBF | 两轮故障-修复循环 failures+2, MTTR/MTBF/可用度与未舍入区间自洽 |
| T6 | MES MQ 集成 | 消费→自动报工/异常单, mid 幂等, ONLINE 直通, 脏报文 nack 入死信, HTTP 通道回归通过 |

一键回归: 仓库根目录 `python verify_e2e.py [--with-load]`(需 `.venv`, 见根目录 requirements.txt)。

## 6. 关键设计决策

- **MBAP 长度字段**: Modbus TCP 长度=unitId+PDU(2+data.len), 差 1 字节即粘包——以 17 字节探针报文实测校准;
- **自写 HSMS 编解码**: secs4j 等现成库在公网仓库不可得/年久失修, 按 SEMI E4/E5/E37 手写 format_byte、
  10 字节头与 Select/Linktest/Separate 状态机, 并用 raw-socket Python 设备模拟器交叉验证;
- **TDengine 时区陷阱**: REST 驱动对 `java.sql.Timestamp` 追加本地时区偏移(+8h), 改绑 epoch-long 毫秒;
  `DAYS` 关键字在 3.x 更名 `DURATION`;
- **落库连接池**: DriverManager 直连 p99=581ms, 换 HikariCP 后 p99=244ms(200 路并发);
- **P99 口径**: 以事件内设备时间戳(params.ts)到 TDengine 落库确认的间隔, 在 sink 消费点测量,
  含 broker 转发与排队, 不含采集轮询周期;
- **三跳幂等**: QoS1/at-least-once 下重投不可避免, 发布/落库/消费三点各以 SETNX 守一段,
  mid 缺失的监控事件(ONLINE/OFFLINE)天然免重(状态迁移才发布);
- **paho 重连不恢复订阅**: MQTT v3 客户端 `automaticReconnect` 只重建连接不重建订阅, broker 重启后
  订阅静默丢失(压测期间实测 12000 条全部未进 MQ 才发现), 改在 `MqttCallbackExtended.connectComplete`
  回调内补订阅, broker 重启后 4s 内恢复;
- **子表时间戳单调防护**: TDengine 同子表同 ts 为 upsert, 遥测与其触发的 SPC 报警事件同毫秒发布时
  会互相覆盖(200 路压测对账差 1 的根因, 随机值流下 R2/R3 判异高频命中), sink 写入前按子表
  `merge` 防护保证 ts 严格递增;
- **首建表双检锁**: 新设备突发首批消息被多线程并发消费时, CREATE TABLE 与 INSERT 竞态
  (Table does not exist → 假性死信); "已建表"集合仅在 CREATE 成功返回后标记,
  锁外检查 + 锁内二次检查, 并发线程等待建表完成后再写入;
- **P99 测量口径**: 压测前 `POST /gateway/metrics/latency/reset` 清零样本, 只统计本轮窗口;
  首轮为 JIT/TDengine 冷缓存预热轮不计成绩, 取第二轮热态值; 同机共存部署(MES 消费者在线)
  与独立部署分开报告, 不混口径; 压测发生器收尾以 `wait_for_publish` 等待全部 QoS1 PUBACK,
  杜绝发送侧假性丢失。
