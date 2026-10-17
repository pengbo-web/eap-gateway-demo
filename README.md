# EAP 设备数据采集网关(工业多协议接入 + 事件总线 + 时序落库 + SPC)

面向半导体/离散制造 **CIM·MES 岗位**的设备层数据采集项目:自写 Modbus TCP / SECS-GEM(HSMS-2) / MQTT
三种工业协议接入,统一事件模型经 RabbitMQ 事件总线扇出至 TDengine 时序库与 MES 消费端,
内置 SPC X̄-R 判异、设备心跳状态机、MTTR/MTBF 可靠性统计,配套三类设备模拟器与 14 项一键回归验收。

与 MES 二次开发项目([pengbo-web/EAP-MES](https://github.com/pengbo-web/EAP-MES),基于开源 Liao-MES)
通过 RabbitMQ 事件总线 + HTTP 双通道集成,构成"设备 → 采集 → 总线 → 业务"完整链路。

## 核心指标(全部可用仓库内脚本复现)

| 指标 | 实测 |
| --- | --- |
| 200 路设备并发压测 | **12000/12000 零丢失**(发送=落库对账),DLQ +0 |
| 端到端延迟(设备时间戳→落库) | 独立口径 **p99=422ms**(p50=127/p95=288),低负载时段曾测 p99=244ms |
| Modbus 断线重连 | **1.1s** 自动恢复,期间事件不丢 |
| 三协议汇聚落库间隔 | 中位 **27ms** |
| SPC X̄-R 管制限 | CL=220.022 / UCL=220.137 / LCL=219.906(n=5,A2/D4 系数表),注入漂移值 244 命中 R1 |
| 离线判定 / 上线恢复 | **4.1s** / 9.2s,状态迁移发 ONLINE/OFFLINE 事件 |
| MQTT broker 重启恢复 | **4s** 内自动补订阅(paho connectComplete 回调) |

## 架构

```
 Modbus TCP 从站(模拟器内置) ──┐
 SECS/GEM 设备(HSMS-2) ────────┤   协议适配层        EventFactory            EapEventPublisher
 MQTT 遥测(mosquitto) ─────────┘  (Netty/paho)  →  阈值判定/批次映射/SPC  →  Redis SETNX 发布去重
                                                        │                        │
                                                        ▼                        ▼
                                              SpcMonitor X̄-R 判异        RabbitMQ topic: eap.events
                                              (判异→PARAM_ALARM)          rk = eap.{protocol}.{eventType}
                                                                                 │
              ┌──────────────────────────────────────────────────────────────────┤
              ▼                                                                  ▼
   队列 eap.tdengine.sink                                              队列 mes.integration
   手动ack+死信, 消费幂等(SETNX)                                        EAP-MES 仓库消费(手动ack+死信+幂等)
   TDengine 超级表, 每设备子表                                           自动报工 / 报警转待审核异常单
   ts 单调防护 + 首建表双检锁

 监控面: 心跳离线状态机 | MTTR/MTBF/可用度 | 延迟分位(可清零) | 阈值/SN映射 Redis 缓存热更新
```

不丢不重三件套:发布方确认 + 手动 ack + 死信队列 `eap.events.dlq`;
三跳幂等:发布侧 `eap:mid:` → 落库侧 `eap:sink:mid:` → MES 消费侧 `eap:mes:mid:`(Redis SETNX)。

## 仓库结构

```
eap-gateway-server/        网关服务端(JDK17 / Spring Boot 3.4 / Netty 4.1, 详见其 README)
├── src/.../protocol/      modbus(MBAP编解码/主从模拟器) secs(HSMS-2/SECS-II 手写编解码) mqtt
├── src/.../core/          统一事件模型 / 事件工厂 / 发布器(去重+计数)
├── src/.../sink/          TDengine 落库(REST+HikariCP, ts 单调防护, 建表双检锁)
└── src/.../monitor/       心跳状态机 / SPC X̄-R / MTTR-MTBF / 延迟分位 / 模拟器控制接口

verify_t1 ~ verify_t6.py   分项验收脚本(断言实测, 详见下方验收体系)
verify_e2e.py              14 项一键回归(组件健康检查 → T1~T6 → MES 五模块)
mqtt_pub_sim.py            MQTT 设备模拟器(DEV-MQ-01 遥测, v 200~260 触发阈值报警)
secs_sim_raw.py            SECS/GEM 设备模拟器(原始套接字 HSMS 被动端, S6F11 周期上报)
secs_probe.py              HSMS 报文探针(排障用)
load_feeder.py             200 路并发压测发生器(QoS1 + wait_for_publish 确定性收尾)
eap_bridge.py              跨系统演示桥接器(消费 hm-MES iot 采集组件 Redis 队列 → HTTP 推送 MES)
liao_api.py                Liao-MES 登录助手(RSA+AES 加密通道)
fixtures/                  验收数据夹具 SQL/JSON(可重复执行)
docs/技术方案-项目一EAP网关.md   设计文档(11 节, 含 CIM 范围锁定增补)
requirements.txt           脚本依赖(requests/redis/paho-mqtt/pycryptodome 等)
```

## 快速启动

### 1. 依赖容器

```bash
docker run -d --name hme-rabbit  -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=root -e RABBITMQ_DEFAULT_PASS=123456 rabbitmq:3-management
docker run -d --name ecom-redis  -p 6379:6379 redis:7
docker run -d --name hme-tdengine -p 6041:6041 tdengine/tdengine:3.3.2.0
docker run -d --name hme-mosquitto -p 1883:1883 eclipse-mosquitto:2   # 配置 listener 1883 + allow_anonymous true
```

### 2. 网关

```bash
cd eap-gateway-server
mvn package -DskipTests
java -jar target/eap-gateway-server-1.0.0.jar
# 内存受压的同机演示环境建议限堆: java -Xms128m -Xmx512m -jar target/eap-gateway-server-1.0.0.jar
```

端口 **8090**;内置 Modbus 从站模拟器随应用启动(1502)。健康检查:`GET /gateway/devices`。

### 3. 外部设备模拟器(可选, 演示/验收用)

```bash
pip install -r requirements.txt
python mqtt_pub_sim.py     # MQTT 遥测设备 DEV-MQ-01
python secs_sim_raw.py     # SECS/GEM 设备 DEV-SECS-01(:5020, 网关主动连接)
python eap_bridge.py       # 跨系统桥接(需 hm-MES iot 采集组件在跑)
```

### 4. MES 集成(可选)

部署 [EAP-MES](https://github.com/pengbo-web/EAP-MES)(Liao-MES 二次开发,含数据库初始化脚本),
其 `EapMqConsumer` 消费队列 `mes.integration` 完成自动报工与报警联动;
设备编码→流转批次映射在网关 `application.yml` 的 `eap.gateway.device-sn-map` 配置。

## 验收体系

| 脚本 | 内容 |
| --- | --- |
| verify_t1 / t1b | Modbus 断线 1.1s 重连 / 超时重发不串包 |
| verify_t2 | HSMS Select→ESTABLISHED、S6F11 解析、S2F41 START/STOP 远控 |
| verify_t3 | 三协议同轨汇聚落库、超级表 TAG、降采样查询(INTERVAL 27ms) |
| verify_t3b | SPC 稳定相零误报 + 漂移命中 R1(运行级设备名隔离历史状态) |
| verify_t4 | 200 路 12000 条零丢失对账、DLQ 零新增、延迟分位(压测前自动清零口径) |
| verify_t5 / t5b | 离线判定/上线事件/阈值热更即时生效;MTTR/MTBF/可用度自洽 |
| verify_t6 | MES MQ 消费:自动报工/mid 幂等/报警单/脏报文入死信(需 EAP-MES 在跑) |
| verify_e2e | 组件健康检查 → T1~T6 → MES 五模块(MRP/FIFO/OEE/追溯/派工),夹具自动重置 |

```bash
python verify_e2e.py              # 常规回归
python verify_e2e.py --with-load  # 含 200 路压测(约 10 分钟)
```

说明:t6 与 e2e 的 MES 部分需要 EAP-MES 侧环境(数据库夹具见该仓库 `script/sql/secondary-dev/`);
纯网关侧验收(t1~t5b)仅依赖上述四个容器。压测 P99 口径:独立部署 500ms 门槛,
同机共存(MES 消费者在线)只作参考输出——两种口径不混用,详见 `eap-gateway-server/README.md`。

## 关键设计决策(摘要)

- **自写 HSMS/SECS-II 编解码**:secs4j 等现成库公网不可得/年久失修,按 SEMI E4/E5/E37 实现
  format_byte、10 字节头与 Select/Linktest/Separate 状态机,以原始套接字模拟器交叉验证;
- **MBAP 长度字段**:长度=unitId+PDU(2+data.len),差 1 字节即粘包,以 17 字节探针报文实测校准;
- **TDengine 三坑**:REST 驱动对 Timestamp 追加 +8h 时区偏移→改绑 epoch-long;3.x `DAYS`→`DURATION`;
  同子表同 ts 为 upsert→遥测与 SPC 报警同毫秒互相覆盖→写入前 ts 单调防护;
- **paho 重连不恢复订阅**:automaticReconnect 只重建连接,订阅在 connectComplete 回调补;
- **建表竞态双检锁**:"已建表"集合仅在 CREATE 成功返回后标记,否则并发消费假性死信;
- **压测对账方法论**:发生器 wait_for_publish 确定性收尾、时间戳窗口归属、压测前延迟样本清零。

完整决策与排障记录见 `eap-gateway-server/README.md` 与 `docs/技术方案-项目一EAP网关.md`。

## 技术栈

JDK 17 · Spring Boot 3.4 · Netty 4.1 · RabbitMQ 3.13 · Redis · TDengine 3.3 · Mosquitto(MQTT 3.1.1) · Python(paho/requests)
