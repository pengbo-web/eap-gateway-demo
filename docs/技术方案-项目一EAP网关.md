# 项目一 · EAP 设备数据采集网关 开发方案

- 版本:v1.0(2026-10-17)
- 目标:把简历项目一从"部署+胶水"形态升级为**自研网关**形态,补齐半导体 CIM 与通用 MES 两类 JD 的设备层硬词
- 代码位置:EAP-MES 仓库新增顶层模块 `eap-gateway-server/`(Java,独立 Spring Boot 应用);现 `eap-gateway/`(Python 桥接)保留为早期演示,改名为 `eap-gateway-bridge/`
- 分支:`feature/eap-gateway`(从 feature/mes-six-modules 拉出,继承项目二全部成果)

---

## 1. 岗位需求映射(为什么做这些)

| JD 硬词(半导体 CIM / 通用 MES) | 本方案落点 |
|---|---|
| SECS/GEM、HSMS、EAP、设备自动化 | T2:HSMS 状态机 + S6F11 事件上报 + S2F41 指令下发 |
| 设备数据采集、PLC/Modbus 通讯 | T1:自写 Netty Modbus TCP 客户端(FC3/FC4)+ 自写从站模拟器 |
| MQTT 接入 | T1b:paho client + mosquitto 容器 + 发布模拟器 |
| 时序数据库、历史数据回溯 | T3:TDengine 超级表 + 时间分区(DAYS)+ 降采样查询 |
| 消息队列、系统解耦 | T4:RabbitMQ topic 交换机 fan-out 到 MES 消费队列与时序 sink 队列 |
| 设备监控、告警、并发接入 | T5:心跳/离线检测 + 阈值告警 + Redis 设备配置缓存 + 压测数字 |
| 与 MES 集成 | T6:MES 侧 @RabbitListener 消费复用项目二 EapIntegrationService |

## 2. 现有资产盘点(零成本复用)

| 资产 | 用途 |
|---|---|
| RabbitMQ 容器 :5672/15672(root/123456) | T4 总线 |
| TDengine 容器 :6030/6041(root/taosdata) | T3 存储,用 **REST 驱动**(jdbc:TAOS-RS://127.0.0.1:6041),免装原生客户端 |
| Redis :6379(db15 已被 hm-MES 占用 → 网关用 **db14**) | T5 配置缓存/心跳 |
| Liao-MES :8188 `/mes/eap/event` + EapIntegrationService | T6 复用,新增 MQ 消费入口 |
| hm-MES iot-test 断路器模拟(5 路) | 对照参考与回归陪跑,不依赖 |
| eap_bridge.py + verify_*.py 模式 | T7 验收脚本模板 |
| 事件协议 EapDeviceEventBo 字段定义 | 网关事件模型直接沿用,保证两项目协议一致 |

## 3. 决策点(默认值已定,可否决)

| # | 决策 | 默认 | 理由 |
|---|---|---|---|
| D1 | MQ 到 MES 的传输口径 | **(a) MES 侧 @RabbitListener 直接消费队列** | 两份简历都写 RabbitMQ 且代码为真;HTTP 接口保留作旁路 |
| D2 | 简历时间线 | 项目一 2026.09-2026.11,项目二 2026.10-2026.11 | 与真实开发顺序一致,两项目咬合不穿帮 |
| D3 | SECS/GEM 实现路径 | T2.0 spike 顺序尝试:① JitPack `com.github.ostigter:secs4j` → ② 克隆源码 mvn install 到本地仓并 vendor 进仓库 libs/ → ③ **自写最小 HSMS-2 + SECS-II 编解码**(Netty) | secs4j 未上 Maven Central(已核实);路径③成本最高但简历故事最强,spike 限时 4h 内定档 |
| D4 | TDengine 写入方 | 网关内 sink 消费者(消费 MQ 队列 eap.tdengine.sink) | 演示 MQ fan-out 一源多消费 |
| D5 | 设备→工单 SN 映射 | 网关配置表(redis hash + 落库兜底),不查 MES | 解耦,EAP 不反向依赖 MES 可用性 |

## 4. 总体架构

```
[Modbus 从站模拟器(自写)] --TCP:1502--> \
[mosquitto 容器 + 发布模拟器]  --MQTT-->   [eap-gateway-server (JDK17, SpringBoot3)]
[SECS 模拟器(secs-simulator 或自写)] --HSMS:5020--> /
     协议适配层(modbus/secsgem/mqtt) → 统一事件模型 EapDeviceEvent
        ├── 心跳/离线检测 + 阈值告警(Redis db14: 配置缓存/last-seen)
        └── RabbitMQ topic exchange `eap.events`
              ├── rk eap.device.*.PROCESS_COMPLETE / PARAM_ALARM / HEARTBEAT
              ├── queue mes.integration   → [Liao-MES @RabbitListener] → EapIntegrationService(复用项目二)
              └── queue eap.tdengine.sink → [网关 sink] → TDengine 超级表 device_params
```

**事件模型**(沿用项目二协议 + 网关扩展):deviceCode、eventType(PROCESS_COMPLETE/PARAM_ALARM/ONLINE/OFFLINE)、sn(配置映射)、count、defective、params(JSON)、protocol、seq、eventTime。

**TDengine schema**:
```sql
CREATE DATABASE IF NOT EXISTS eap DAYS 7 KEEP 365;   -- DAYS=时间分区策略
CREATE STABLE eap.device_params (ts TIMESTAMP, v FLOAT, i FLOAT, power1 FLOAT, power2 FLOAT, params NCHAR(256))
  TAGS (device_code NCHAR(64), protocol NCHAR(16));
-- 每设备子表: eap.d_<code>; 降采样查询: SELECT AVG(v) FROM stable PARTITION BY device_code INTERVAL(1h)
```

**Redis key 设计**(db14):`eap:dev:cfg` hash(设备配置)、`eap:dev:last` hash(最后心跳)、`eap:dev:state` hash(运行/报警/离线)。

## 5. 任务分解

### T0 骨架与 spike(0.5d)
- 建 `eap-gateway-server/` Spring Boot 3 工程(JDK17),包结构 protocol/core/sink/monitor/config
- D3 spike:secs4j 可得性三路径定档
- 验收:工程启动空跑;spike 结论记录进 docs

### T1 Modbus TCP 自写采集(1.5d)
- Netty 客户端:MBAP 头编解码(事务号/协议号/长度/单元号)+ FC3 读保持寄存器 + FC4 读输入寄存器;轮询调度(每设备间隔可配);**断线指数退避重连**(1s→30s 封顶)
- 自写 Modbus 从站模拟器(test 模块,Netty server,寄存器区可注入数值)——兼作面试"我也写了模拟器"的素材
- 验收 `verify_t1.py`:起从站→网关采集→事件入 MQ;杀从站→日志重连退避序列→复活从站→恢复时间 ≤【X】s 断言

### T1b MQTT 接入(0.5d)
- paho client 订阅 `eap/dev/+/telemetry`;mosquitto 容器;发布模拟器脚本
- 验收:发布 JSON→网关事件→MQ 可见

### T2 SECS/GEM 接入(2~3d,按 D3 定档)
- 路径①/②:secs4j 作 HSMS 客户端连 secs-simulator:Select/Linktest/心跳、S6F11 事件报告接收、S2F41 远程指令下发(启停)
- 路径③:自写 HSMS-2 帧(10 字节头)+ SECS-II 子集编解码(S6F11/S2F41/S1F13/S1F14)+ 状态机(CONNECT→CONNECTED→SELECTED)
- 验收 `verify_t2.py`:模拟器发 S6F11→网关事件 eventType=PROCESS_COMPLETE;网关发 S2F41→模拟器回执 S2F42;断开→状态机回退日志

### T3 TDengine 时序 sink(1d)
- sink 消费者写超级表;DAYS 7 分区;降采样查询接口 GET /gateway/history?device&from&to&interval
- 验收 `verify_t3.py`:压入 10 万点→INTERVAL(1h) 查询耗时 ≤【X】ms;分区目录天数核对

### T4 事件总线与压测(1d)
- exchange/queue/绑定声明;publisher 确认(confirm)+ 消费端手动 ack + 死信队列(消费失败 3 次入 DLQ)——面试"消息不丢"三件套
- 压测:N 路从站模拟器(参数化 50/100/200)→ 统计端到端 P99(模拟器时间戳→MES work_rec 落库时间)
- 验收 `verify_t4.py`:200 路 10 分钟→P99 ≤【X】ms、DLQ=0、零丢失(发送计数=落库计数)

### T5 监控告警与配置缓存(1d)
- 心跳 last-seen(Redis TTL)+ 离线判定调度(超 3×间隔→OFFLINE 事件);阈值规则来自 `eap:dev:cfg`(Redis 缓存,变更发布刷新);告警→PARAM_ALARM 事件
- 管理接口 GET /gateway/devices(状态一览)、POST /gateway/devices/{code}/config(改阈值并验证缓存刷新)
- 验收 `verify_t5.py`:静默设备→OFFLINE 事件时延 ≤【X】s;改阈值→新规则 1 个心跳周期内生效

### T6 MES 侧 MQ 消费(0.5d)
- liaoyun-mes 加 spring-boot-starter-amqp;`@RabbitListener(queues="mes.integration")` → 反序列化 EapDeviceEvent → 复用 EapIntegrationService.handleEvent;HTTP 接口保留
- 验收:复用项目二派工/报工回归脚本 verify_dispatch.py 改走 MQ 触发,全绿

### T7 收尾(0.5d)
- 验收脚本汇总 e2e_run.sh;README(架构图+启动顺序);简历回填数字;commit 计划执行

**合计:8~9 个工作日**(D3 走路径③则 +2d)

## 6. 排期(按开工日 D 计)

D: T0 → D+1~2: T1 → D+2: T1b → D+3~5: T2 → D+6: T3 → D+7: T4 → D+8: T5 → D+8.5: T6 → D+9: T7
每日结束跑已完工任务回归脚本,防 T6 类跨模块回归。

## 7. 风险与对策

| 风险 | 对策 |
|---|---|
| secs4j 年久(2013)与 JDK17 不兼容 | spike 限时 4h;不兼容即降路径③,且③的自写编解码简历价值更高 |
| TDengine JDBC 原生驱动需本地库 | 强制 REST 驱动(TAOS-RS),已验证容器 6041 可用 |
| 压测 P99 不达标 | 先测 50 路基线;不达标则优化 publisher 批量确认,简历写实测值不写目标值 |
| 与项目二回归冲突 | T6 后全量跑 verify_mrp/fifo/oee/trace/dispatch 五脚本 |
| 时间超支 | T1b(MQTT)与 T3 降采样接口可裁剪,保 T1/T2/T4/T5/T6 主线 |

## 8. 简历映射与数字填空位

| 简历句 | 任务 | 填空数字来源 |
|---|---|---|
| Netty 多协议 + 重连恢复 ≤Xs | T1/T1b | verify_t1 恢复时间 |
| SECS/GEM HSMS/S6F11/S2F41 | T2 | verify_t2 日志 |
| TDengine 分区 + X 万点查询 ≤Xms | T3 | verify_t3 计时 |
| RabbitMQ 解耦 + X 路并发 P99 ≤Xms | T4 | verify_t4 统计 |
| 监控告警 + Redis 缓存 + 稳定接入 X 路 | T5 | verify_t5 + T4 压测上限 |
| (项目二)MQ 消费复用集成模块 | T6 | verify_dispatch(MQ 版) |

## 9. 提交计划(feature/eap-gateway 分支)

1. chore: 骨架与 spike 结论 2. feat(modbus): 客户端+从站模拟器 3. feat(mqtt) 4. feat(secsgem): HSMS/SECS-II 接入 5. feat(taos): 时序 sink 与分区 6. feat(mq): 总线/确认/死信+压测 7. feat(monitor): 心跳告警与配置缓存 8. feat(mes): MQ 消费接入 9. docs: README/方案/简历数字回填

## 10. 预期新增面试故事

HSMS 状态机迁移与 Linktest 心跳设计;Modbus MBAP 事务号匹配与超时重发;为什么 TDengine 用 DAYS 分区+超级表标签建模;MQ 不丢三件套(confirm/手动ack/死信);P99 测量口径(设备时间戳 vs 落库时间)的时钟偏差处理;secs4j 不可得时的技术选型决策过程。

---

## 11. 增补:CIM 投递已发生后的范围锁定(2026-10-17)

背景:用户已按 CIM 方向投递简历,所投版本项目一含 SECS/GEM、200 路并发 P99<500ms、TDengine 超级表、SPC X̄-R、OEE/MTTR/MTBF、自研模拟器等声称。范围锁定为"全部做实",增补任务:

### T3b SPC 统计过程控制(1d)
- 基于 TDengine 参数流按设备+参数计算 X̄-R 管制图(CL/UCL/LCL, 子组 n=5)
- 判异规则至少 3 条: 超 3σ、连续 7 点同侧、连续 6 点递增/递减
- 判异 → 发 PARAM_ALARM 事件(复用项目二待审核单闭环)
- 验收 verify_t3b.py: 注入受控序列无判异; 注入漂移/超限序列命中对应规则

### T5b MTTR/MTBF(0.5d)
- 基于 OFFLINE/PARAM_ALARM→恢复 事件区间: MTBF=总运行时长/故障次数, MTTR=总修复时长/故障次数
- 接口 GET /gateway/reliability?device= 输出双指标
- 验收 verify_t5b.py: 构造已知故障/修复区间, 手算对照

### T4 基线上调
- 压测 200 路并发, P99 目标 ≤500ms; 发送计数=落库计数 零丢失对账

### 修订排期(10~11d)
D0 T0 → D1-2 T1 → D2 T1b → D3-5 T2 → D6 T3 → D7 T3b → D8 T4 → D9 T5 → D9.5 T5b → D10 T6 → D10.5-11 T7+模拟面试启动

### 声称覆盖表(投递版简历 → 任务)
| 投递版声称 | 落实任务 |
|---|---|
| SECS/GEM HSMS、S6F11、S2F41 | T2 |
| Modbus TCP、粘包拆包、断线重连 | T1 |
| 200 路并发、P99<500ms | T4 |
| TDengine 超级表、时间分区 | T3 |
| SPC X̄-R 管制图、判异告警 | T3b |
| OEE/MTTR/MTBF | T5b + 项目二 OEE 引擎 |
| 自研多协议设备模拟器 | T1 从站模拟器 + T2 secs 模拟器适配 |
