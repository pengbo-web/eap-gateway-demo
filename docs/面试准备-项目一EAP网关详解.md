# 面试吃透手册:项目一「EAP 设备数据采集网关」

> 锚定 PDF 简历原文(2026.06-2026.09 版)。所有声称 → 代码文件/方法 → 实测数字,三层可追溯。
> 用法:先背 §1 与 §6,再精读 §3 逐条深挖,面试前一晚过 §4 横向题与 §5 故事集。
> 配套:计划见 `docs/面试准备-计划.md`;实测复现 `python verify_e2e.py --with-load`。

---

## §1 一页纸速览

### 1.1 30 秒电梯稿

> 我独立开发了一个轻量级 EAP 设备采集网关,定位是车间机台和 MES 之间的中间层,对应 ISA-95 的 L2/L3 设备集成层。它用 Netty 自写了 Modbus TCP 和 SECS/GEM(HSMS)协议栈,加上 MQTT 订阅,三种协议的设备事件统一成标准消息模型,经 RabbitMQ 事件总线扇出到 TDengine 时序库和 MES 消费端;网关在边缘侧完成阈值告警和 X̄-R 管制图 SPC 判异,还带设备心跳监控和 MTTR/MTBF 可靠性统计。整条链路做过 200 路设备并发压测,12000 条事件零丢失,P99 时延 500ms 以内,配了 14 项一键回归验收脚本,所有数字可当场复现。

### 1.2 三分钟完整版(背景 → 架构 → 职责 → 难点 → 结果)

**背景**:半导体/离散制造的 CIM 体系里,设备数据采集层(EAP/EDA)是 MES 的数据源头。车间设备协议割裂——老设备多为 Modbus TCP,半导体前道设备标配 SECS/GEM,新型物联网设备走 MQTT。商业采集平台重且封闭,我以个人项目形式自建了一个轻量网关,把"设备 → 采集 → 总线 → 时序库/MES"全链路走通并可验收。

**架构**:四层——
1. 协议接入层:Netty 实现的 Modbus 主站与 HSMS 主动端客户端,paho 实现 MQTT 订阅,外加内置 Modbus 从站模拟器做故障注入;
2. 事件处理层:EventFactory 把三种协议的原始数据统一成 EapEvent(设备编码/事件类型/批次号/数量/工艺参数/协议/序号/消息id/时间戳),做阈值判定、设备→批次映射、SPC 观察;
3. 传输层:RabbitMQ topic 交换机 `eap.events`,routing key = `eap.{protocol}.{eventType}`,扇出到时序落库队列和 MES 集成队列,双队列都挂死信;
4. 存储与消费层:TDengine 超级表按设备子表落库;MES 侧(Liao-MES 二开,另一个仓库)消费事件做自动报工与报警闭环。

**我的职责**:全部——两套协议编解码手写、总线可靠性设计、时序建模、SPC 算法、监控统计、三类设备模拟器、验收体系。

**难点挑两个讲**:
- SECS/GEM 没有可用开源库(secsgem 是 Python 且 API 陈旧、secs4j 公网不可得),我按 SEMI E4/E5/E37 手写了 HSMS 会话状态机和 SECS-II 编解码,用原始套接字写了台设备模拟器做交叉验证;
- 消息不丢不重体系:发布确认+持久化+手动 ack+死信是基础,真正花功夫的是"证明零丢失"的对账方法论和跨三个环节的幂等设计,期间还靠压测对账抓出了 TDengine 同毫秒 upsert 覆盖这种隐蔽 bug(详见 §5 故事 3)。

**结果**:200 路并发 12000 条零丢失、P99 ≤500ms(独立口径实测 422ms,低负载 244ms)、断线重连 1.1s、离线判定 4.1s、45 万点趋势查询中位 121ms、SPC 管制限与判异全验证,14 项验收一键回归全过。

### 1.3 ISA-95 定位(必被问,先手准备)

ISA-95 把企业系统分五层:L0 物理过程 / L1 传感执行 / L2 监控与数据采集(SCADA/EAP)/ L3 制造运营(MES)/ L4 业务计划(ERP)。
本网关处于 **L2-L3 边界的设备集成层**,对应 B2M(业务到制造)通道中的两件事:
- **生产指令下达**:S2F41 远程指令(设备启停)——L3 → L2 方向;
- **生产实绩上报**:事件带批次号(sn)、完工数量、工艺参数——L2 → L3 方向。

被追问 L4/ERP 细节:坦承网关不涉及,只按分层定位答。

---

## §2 架构全景与数据流

### 2.1 代码地图(24 个类,包 → 职责)

```
com.pengbo.eap
├── EapGatewayApplication        启动类(exclude DataSourceAutoConfiguration——网关无关系库)
├── config/
│   ├── GatewayProperties        eap.gateway.* 配置绑定(协议目标/SN映射/阈值/SPC参数/离线倍数)
│   └── MqConfig                 交换机/双队列/死信/绑定/Jackson 消息转换器声明
├── core/
│   ├── EapEvent                 统一事件模型(10 字段)
│   ├── EventFactory             阈值判定→事件类型、SN映射、SPC观察、心跳touch、分协议计数
│   └── EapEventPublisher        发布:计数→mid去重(SETNX)→补seq/eventTime→拼rk→convertAndSend
├── protocol/modbus/
│   ├── ModbusFrame              MBAP+PDU 帧模型(record):编解码/读请求构造/寄存器解析/异常帧判定
│   ├── ModbusFrameDecoder       extends LengthFieldBasedFrameDecoder(260,4,2,0,0)
│   ├── ModbusMasterClient       主站:事务号匹配(pending map+CompletableFuture)、2s超时、指数退避重连
│   ├── ModbusPollerService      轮询调度:每目标 scheduleWithFixedDelay,FC3读4寄存器→v/i/power1
│   └── ModbusSlaveSimulator     内置从站模拟器(故障注入用,/sim/modbus/stop|start)
├── protocol/mqtt/
│   └── MqttSubscriberService    paho 订阅 eap/dev/+/telemetry,connectComplete 补订阅
├── protocol/secs/
│   ├── SecsItem                 SECS-II 数据项编解码(List/B/A/F8/U4,format_byte=(code<<2)|nlen)
│   ├── HsmsMessage              HSMS 报文(4字节长度+10字节头+data)record,stype 常量
│   ├── SecsHostClient           主动端状态机:Select→S1F13/14→ESTABLISHED;S6F11解析+S6F12应答;S2F41;退避重连
│   └── SecsHostService          多设备通道管理,S6F11 值→v/i/power1/power2→统一事件
├── sink/
│   ├── TdengineConfig           REST 驱动 + HikariCP(max 8, minIdle 2, timeout 3s)
│   └── TdengineSinkService      消费落库:sink去重→建表双检锁→ts单调防护→INSERT→ack→延迟统计;失败nack入死信
└── monitor/
    ├── DeviceRegistry           Redis hash 配置中心(eap:dev:cfg:{code}),本地缓存5s刷新,更新即时生效
    ├── DeviceMonitorService     心跳lastSeen+扫描线程:离线判定/状态迁移发ONLINE-OFFLINE事件
    ├── SpcMonitor               X̄-R 管制图(A2/D4表,R1/R2/R3),violations环形100条
    ├── ReliabilityService       MTTR/MTBF/可用度(状态迁移区间累计)
    ├── LatencyStats             端到端延迟分位(2万样本,可reset)
    └── SimController            运维/验收 REST:模拟器开关、设备清单、配置热更、统计、SPC、可靠性、SECS指令
```

MES 侧(在 EAP-MES 仓库,属项目二):`com.liaoyun.mes.eap.EapMqConsumer`(消费 mes.integration)、`EapMqConfig`(双端幂等声明队列)、`EapIntegrationService`(自动报工/报警闭环)。

### 2.2 一条事件的端到端旅程(以 Modbus 轮询为例,10 步)

1. `ModbusPollerService` 调度线程到点,`ModbusMasterClient.read(FC3, 0, 4)` 发请求(事务号 tx 自增 &0xFFFF,挂入 pending,2s 超时);
2. 从站响应,`ModbusFrameDecoder` 按长度字段切帧,`channelRead` 按 tx 从 pending 取出 Future 完成;
3. `pollOnce` 把寄存器换算成工艺参数(v=regs[0]/100.0, i, power1),调 `EventFactory.build(deviceCode, "MODBUS_TCP", params)`;
4. EventFactory:查 `DeviceRegistry`(本地缓存,源是 Redis hash)拿该设备阈值与 SN 映射 → 阈值超限?是→PARAM_ALARM(defective=1),否→PROCESS_COMPLETE 且做 SPC 观察(可能追加一条 protocol=SPC 的报警事件)→ `monitor.touch()` 记心跳;
5. `EapEventPublisher.publish`:设备×协议×类型计数 → mid 存在则 Redis SETNX `eap:mid:{dev}:{mid}`(10min)去重 → 补 eventTime/seq → routing key=`eap.modbus_tcp.process_complete` → `convertAndSend`(Jackson JSON,持久化消息)到 topic 交换机 `eap.events`;
6. 交换机按绑定 `eap.#` 复制到两个队列:`eap.tdengine.sink` 与 `mes.integration`;
7. **sink 侧**(`TdengineSinkService.onEvent`,并发 4-8、prefetch 50、手动 ack):sink 幂等 SETNX `eap:sink:mid:` → 子表首建走双检锁 CREATE → ts 单调防护 `lastTsMap.merge` → `INSERT INTO eap.d_dev_mb_01 VALUES(epoch-long, ...)` → basicAck → 若 params 带设备 ts 则记延迟样本;
8. **MES 侧**(`EapMqConsumer.onEvent`):原始报文手工反序列化(不依赖 __TypeId__)→ 非报工类事件(ONLINE/OFFLINE)直接 ack 放行 → sn 为空(未映射批次)放行 → mid 幂等 SETNX `eap:mes:mid:` → `EapIntegrationService.handleEvent`(TenantHelper.dynamic 切租户 + 忽略数据权限)→ 自动报工/异常单 → ack;业务异常(无进行中任务)ack 跳过,未知异常 nack 入死信;
9. 监控并行:`DeviceMonitorService` 扫描线程每 2s 检查 lastSeen,静默超过 pollMs×3 判 OFFLINE(发事件+记故障区间),恢复收事件发 ONLINE;`ReliabilityService` 累计 up/down 区间;
10. 验收观测:`/gateway/stats`(分协议计数)、`/gateway/metrics/latency`(分位)、`/gateway/devices`(状态)、RabbitMQ 管理 API(队列对账)、TDengine 行数对账。

### 2.3 部署视图(演示环境)

| 组件 | 形态 | 端口 | 说明 |
| --- | --- | --- | --- |
| eap-gateway-server | java -jar(Xmx512m) | 8090(REST)/1502(内置从站) | 网关本体 |
| RabbitMQ | docker hme-rabbit | 5672/15672 | root/123456 |
| Redis | docker ecom-redis | 6379 | 网关用 db14 |
| TDengine | docker hme-tdengine | 6041(REST) | root/taosdata |
| Mosquitto | docker hme-mosquitto | 1883 | 匿名 |
| Liao-MES(项目二) | java -jar(Xmx1024m) | 8188 | 消费 mes.integration |
| 模拟器 | python ×3 | - | mqtt_pub_sim / secs_sim_raw(:5020) / eap_bridge |

### 2.4 统一事件模型 EapEvent(10 字段)

| 字段 | 语义 | 谁赋值 |
| --- | --- | --- |
| deviceCode | 设备编码(DEV-MB-01 / DEV-SECS-01 / DEV-MQ-01 / DEV-LOAD-xxx) | 协议适配层 |
| eventType | PROCESS_COMPLETE / PARAM_ALARM / ONLINE / OFFLINE | EventFactory(阈值判定)或监控状态机 |
| sn | MES 流转批次号(对应 mes_task.sn) | DeviceRegistry(yml 种子+Redis 运行时) |
| count / defective | 完工数量 / 不良数 | EventFactory(默认 1/0,报警 0/1) |
| params | 工艺参数 Map<String,Double>(v/i/power1/power2/ts…) | 协议适配层 |
| protocol | MODBUS_TCP / SECS_GEM / MQTT / SPC | 协议适配层 |
| seq | 网关内单设备递增序号 | Publisher(AtomicLong) |
| mid | 设备侧消息唯一 id(QoS1/重发去重锚点) | 设备载荷携带则透传 |
| eventTime | 事件时间戳 ms | Publisher 缺省补当前时间 |

### 2.5 配置分层

- **静态(yml → GatewayProperties)**:协议目标列表(host:port:unitId:pollMs)、SN 映射种子、默认阈值(v:245.0)、SPC 参数(subgroupSize=5, minSubgroups=5, params=[v])、offline-factor=3;
- **运行时(Redis hash `eap:dev:cfg:{code}` → DeviceRegistry)**:thresholds/pollMs/sn 三字段;seed 只在 key 不存在时写(保护运维修改);`POST /gateway/devices/{code}/config` 同时写 Redis 与本地缓存(即时生效);@Scheduled 5s 刷新兜底多实例一致性;
- **为什么这样分**:每事件都查 Redis 的话 200 事件/s × RTT 不可接受;本地缓存+推送更新+周期兜底是标准的"配置面"设计。

---

## §3 简历逐条深挖

> 每条固定七段:原文 → 白话 → 代码地图 → 原理深挖 → 数字出处 → 拷打 Q&A → 弱点与应对。

### 3.1 条1「构建多协议设备接入服务」

**简历原文**:基于 Netty 开发通信服务,封装 SECS/GEM 协议组件实现 HSMS 状态机管理、S6F11 生产事件上报、S2F41 远程指令下发;同时支持 Modbus TCP 轮询采集、MQTT 遥测订阅,实现断线自动重连,故障恢复时间 ≤2s。

**白话**:我写了三个协议适配器,把不同"方言"的设备统一翻译成标准事件。Modbus 是主站轮询(网关主动问,设备被动答);SECS/GEM 是半导体设备标配,网关作为 HSMS 主动端连设备,设备用 S6F11 主动上报事件,网关还能用 S2F41 下发启停指令;MQTT 是订阅制,设备发布遥测主题。三种链路断了都能自动重连。

**代码地图**:

| 声称点 | 代码位置 |
| --- | --- |
| Netty 通信服务 | ModbusMasterClient / SecsHostClient 各自 Bootstrap+NioEventLoopGroup(1) |
| HSMS 状态机 | SecsHostClient.State{DISCONNECTED→SELECTED→ESTABLISHED},onMessage 驱动迁移 |
| S6F11 上报 | SecsHostClient.extractReportValues + ack(S6F12) + 回调 SecsHostService.onEventReport |
| S2F41 下发 | SecsHostClient.sendRemoteCommand(W-bit,S2F42 配对,3s orTimeout)+ SimController POST /gateway/secs/command |
| Modbus 轮询 | ModbusPollerService.scheduleWithFixedDelay + ModbusMasterClient.read(FC3,0,4) |
| MQTT 订阅 | MqttSubscriberService(topic eap/dev/+/telemetry,QoS1) |
| 断线重连 | 两个 Client 的 channelInactive→scheduleReconnect(指数退避 1/2/4/8/16/30s 封顶);MQTT 靠 paho automaticReconnect+connectComplete 补订阅 |

**原理深挖**:

(1) **Modbus TCP 帧(MBAP+PDU)**:
```
| transactionId u16 | protocolId u16=0 | length u16 | unitId u8 | fc u8 | data... |
                                     length = unitId+fc+data = 2 + data.length
```
- 分帧:`ModbusFrameDecoder extends LengthFieldBasedFrameDecoder(260, 4, 2, 0, 0)`——maxFrameLength=260(Modbus TCP 规范最大帧:MBAP 7 + PDU 253)、lengthFieldOffset=4(长度字段在事务号+协议号之后)、lengthFieldLength=2、adjustment=0(长度值不含自身与头部,恰好等于剩余字节)、strip=0(保留完整帧自己解析);
- **长度字段差 1 字节的坑**:length 必须含 unitId(2+data),我最初写成 1+data,粘包错位,用 17 字节探针报文抓出来(§5 故事 1);
- 事务号匹配:`txGen.incrementAndGet() & 0xFFFF` 生成,pending ConcurrentHashMap<tx, CompletableFuture>,响应帧按 tx 配对——同一连接上多个在途请求不串扰;`orTimeout(2s)` 超时即失败(不重发同一事务号,下个轮询周期天然重试——轮询语义下重发旧读请求没有意义);
- 异常帧:响应 fc 的 bit7=1 表示 Modbus 异常,`isError()` 判定并携带异常码;
- 轮询用 `scheduleWithFixedDelay`(上次执行完再等周期)而非 FixedRate——设备慢响应时不会任务堆积;
- 重连:`channelInactive` → `failAll()`(在途请求快速失败,不让上层干等)→ 指数退避 `min(30s, 1s<<(attempt-1))`,封顶 30s 防止设备长期离线时重试风暴。

(2) **HSMS/SECS-II(SEMI E37/E5)**:
- SECS-I(E4)是串口半双工,HSMS(E37)是 TCP 版;半导体厂设备基本 HSMS。模式分主动/被动:**网关是 Active(Host)主动连设备 Passive 端**(设备监听);
- 报文 = 4 字节长度 + 10 字节头 + SECS-II 数据:
```
| length u32(=10+data) | session u16 | stream u8(bit7=W-bit) | function u8 | ptype u8 | stype u8 | system u32 | data |
stype: 0=Data  1=Select.req  2=Select.rsp  3=Linktest.req  4=Linktest.rsp  9=Separate.req
```
- 状态机:TCP 连通 → 发 Select.req,收 Select.rsp → **SELECTED** → 发 S1F13(W) 收 S1F14 → **ESTABLISHED**(通信建立,可收发业务报文);Linktest 是保活心跳(请求-应答,system 字节原样回);Separate 优雅断开;
- SECS-II 数据项编码:`format_byte = (code<<2) | nlen`,code:List=0/Binary=8/ASCII=16/F8=32/U4=44;nlen=长度字节数(1/2/3);List 载荷=子项个数+子项递归编码;解码是递归下降,返回 [item, 消费字节数];
- **S6F11(事件报告,W-bit=1)**:结构 `L,3 [U4 DATAID, U4 CEID, L,n [ L,2 [U4 RPTID, L,m 值列表]]]`;收到后必须回 **S6F12** 应答(否则设备认为上报失败会重发/阻塞)——代码里 `ack()` 回 B,0;值列表按 F8/U4 提取成 List<Double> 回调上层;
- **S2F41(远程指令,W-bit=1)**:发 `L,2 [A "START"|"STOP", L,0]`(指令+空参数表),等 **S2F42** 应答,`orTimeout(3s)`;请求-应答靠 system 字节配对(pending map);system 生成器从 0x20000000 起——E37 建议 Host 与设备各用独立号段防冲突;
- W-bit(stream 字节 bit7):置 1 表示"要求应答"。S6F11 设备置 W 等我回 S6F12;我发 S2F41 置 W 等设备回 S2F42。

(3) **MQTT**:
- 订阅通配 `eap/dev/+/telemetry`,设备编码取 topic 第三段;QoS1 at-least-once → 载荷带 mid 时走发布侧幂等;
- **paho v3 大坑**:`automaticReconnect(true)` 只恢复 TCP/MQTT 会话,**不恢复订阅**——broker 重启后网关"看起来连着"却收不到任何消息(压测时 12000 条全部没进 MQ 才暴露)。修复:`MqttCallbackExtended.connectComplete(reconnect, uri)` 回调里补订阅,实测 broker 重启后 4s 恢复(§5 故事 5)。

**数字出处**:
- 故障恢复 ≤2s:Modbus 链路拔线(停从站)到重连成功实测 **1.1s**(verify_t1;退避首次 1s+连接耗时);
- MQTT broker 级重启恢复 **4s**(paho 退避+补订阅)——**口径注意**:≤2s 指设备链路级断线(以 Modbus 实测背书),broker 整机重启是另一场景,被追问分开答;
- SECS 通道:Select→ESTABLISHED 全状态机实测,断链按同样退避序列重建(verify_t2)。
- 复现:`python verify_t1.py`、`python verify_t2.py`、`docker restart hme-mosquitto` 后看网关日志"MQTT 订阅恢复成功"。

**拷打 Q&A**:

1. **为什么用 Netty 手写而不是 jlibmodbus/modbus4j?**
   答:三层理由——① 学习目的:粘包拆包、异步事务匹配、连接生命周期这些核心问题,用现成库就黑盒了;② 统一技术栈:SECS/GEM 本来就没有可靠 Java 库必须手写,Modbus 一起写可以让两种协议共享同一套 Netty 基础设施(解码器、退避重连、pending 匹配),代码形态一致;③ 依赖可控:工业现场对第三方依赖审计严格,自写协议层只依赖 Netty。

2. **Netty 的 Reactor 模型讲讲?你这里线程怎么配的?**
   答:Reactor = 多路复用(select) + 事件分发。Netty 主从 Reactor:bossGroup 接连接、workerGroup 处理 IO。我的场景是网关做客户端,每个设备连接一个 `NioEventLoopGroup(1)`——单线程事件循环串行处理该连接的所有 IO 与回调,天然无并发问题(pending map 用 ConcurrentHashMap 是因为业务线程也会读写);连接数上百时这种"每连接一线程"要改成共享 EventLoopGroup,这是我明确知道的生产化改进点。

3. **粘包拆包本质与解法?**
   答:TCP 是字节流没有消息边界,发送端 write 次数与接收端 read 次数无对应关系。三种解法:定长、分隔符、长度字段。工业协议都带长度字段(Modbus MBAP length、HSMS 4 字节 length),所以用 LengthFieldBasedFrameDecoder 最自然,零拷贝切帧。

4. **LengthFieldBasedFrameDecoder 五个参数对着你的代码讲一遍?**
   答:(260,4,2,0,0):最大帧长 260=Modbus 规范上限;长度字段偏移 4(跳过 tx+protocolId);长度字段 2 字节;adjustment=0——MBAP 的 length 值定义为"长度字段之后到帧尾的字节数"(unitId+PDU),恰好等于解码器需要的语义;不剥离头部,整帧交给 ModbusFrame.decode 自己解析。

5. **事务号回绕(65535 之后)怎么办?会不会串?**
   答:`&0xFFFF` 回绕后理论上可能与 65536 个周期前的在途请求冲突,但请求 2s 超时即从 pending 移除,回绕周期(65536×1s 轮询≈18 小时)远大于超时窗口,实际不可能共存;更严格的做法是检测 pending 冲突时跳号。

6. **断线时在途请求怎么处理?**
   答:channelInactive 里 failAll()——所有 pending Future 立即异常完成,上层轮询 catch 后本周期放弃,下周期重连成功后自然恢复。宁可快速失败也不悬挂。

7. **为什么指数退避?参数怎么定?**
   答:固定短间隔会在设备长期离线时打满重试(日志风暴+fd 消耗);指数退避 1/2/4/8/16s、30s 封顶:前几次快速重试覆盖瞬断(实测 1.1s 恢复),封顶值保证恢复延迟上界可接受(最坏 30s)。还可以加随机抖动防多设备同时重连的惊群——单机演示环境没加,生产要加。

8. **HSMS 和 SECS-I 什么区别?为什么半导体用 HSMS?**
   答:SECS-I(E4)定义串口物理层上的消息帧与应答机制,半双工、速率低、距离受限;HSMS(E37)把同样的 SECS-II 消息搬到 TCP,支持长连接、全双工、多会话,外加 Select/Linktest/Separate 会话管理。产线设备与主机通常在同一个厂网,TCP 布线与运维成本远低于串口,所以现代 fab 基本 HSMS(或 HSMS over TLS)。

9. **Linktest 谁发起?你实现了吗?**
   答:规范里两端都可以发起,用于探测连接活性。我实现了**应答端**(收到 Linktest.req 回 rsp,system 原样带回);主动周期发起没做——因为我的离线判定走业务层 lastSeen(数据即心跳),比 Linktest 更贴近"设备还在干活"的语义。诚实说:标准 Host 应该主动发 Linktest,这是改进项。

10. **S6F11 不回 S6F12 会怎样?**
    答:W-bit 语义是发送方阻塞等应答。设备侧超时未收到 S6F12 会按 T3(应答超时)重试或标记通信故障,事件可能重发——这也正是消费端要幂等的原因之一。

11. **S2F41 的 3s 超时为什么?超时了重试吗?**
    答:3s 覆盖设备执行 START/STOP 的典型响应时间(T3 惯例 5s 内)。超时**不自动重试**——远程指令是有副作用的操作,盲重试可能重复启停;把超时抛给调用方(REST 接口)由人决策,这符合"指令类操作宁可失败也不重复"的原则。

12. **你手写协议栈,正确性怎么保证?**
    答:三重验证——① 用原始套接字写了独立的设备模拟器(secs_sim_raw.py,按规范编码 S6F11、解析 S2F41),与 Java 端互为对照组;② 探针报文:构造已知字节序列逐字段断言(比如 MBAP 17 字节探针抓出长度差 1);③ 端到端验收:verify_t2 断言状态机到 ESTABLISHED、S6F11 值进 TDengine、S2F41 之后模拟器行为改变。

13. **200 路设备就是 200 个 Netty 连接吗?**
    答:不是,口径要讲清:压测的 200 路是 **MQTT 逻辑通道**(200 个独立 topic+独立 mid 序列,单连接多路复用),验证的是采集→总线→落库链路的吞吐与零丢失;Modbus/SECS 是每设备一连接,演示环境各 1 路。真要 200 物理连接,Modbus 客户端共享 EventLoopGroup + 连接池化即可,架构上没有障碍——这是我能讲清的扩展路径。

**弱点与应对(主动准备)**:
- GEM 完整模型(E39/E40/E116:状态机、S1F15/S1F17 动态报告配置、S2F33 报告定义)我只做了子集——S1F13/14 建立通信后直接收 S6F11,没有走"配置报告→使能事件"流程。答法:"个人项目按最小可用子集实现,完整 GEM 需要与真实设备联调,我能讲清全流程每一步的报文";
- 无 TLS/鉴权(HSMS over TLS、Modbus/TCP Security 规范存在但演示环境未启用);
- Linktest 未主动发起(见 Q9);每连接独立 EventLoopGroup 不适合千级连接(见 Q2)。

### 3.2 条2「搭建标准化事件总线」

**简历原文**:将设备启停、参数超限、生产完工事件封装为统一消息体,携带工单/批次信息,经 RabbitMQ 异步推送至 MES,实现设备层与业务层解耦;通过手动 ACK、死信队列、三跳幂等去重机制保障消息不丢不重,压测环境 200 路设备并发事件零丢失,端到端 P99 时延 ≤500ms。

**白话**:三种协议翻译出来的事件长一个样(EapEvent),带上 MES 认识的批次号(sn),扔到 RabbitMQ topic 交换机;交换机同时喂两个消费者——TDengine 落库和 MES 业务处理,谁挂了不影响谁。消息可能重复投递(MQTT QoS1、消费者重启重投),我在发布/落库/消费三个环节各设一道 Redis SETNX 去重;消息绝不能丢,靠持久化+手动 ack+死信兜底,并用"发送数=落库数"的对账法证明。

**代码地图**:

| 声称点 | 代码位置 |
| --- | --- |
| 统一消息体 | core/EapEvent(10 字段,§2.4) |
| 携带工单/批次 | EventFactory.build:sn = DeviceRegistry(设备→mes_task.sn 映射) |
| topic 交换机 | MqConfig:EXCHANGE=eap.events(durable),rk=eap.{protocol}.{eventType} |
| 双队列扇出+死信 | MqConfig:mes.integration / eap.tdengine.sink,均挂 x-dead-letter-exchange=""、x-dead-letter-routing-key=eap.events.dlq,binding eap.# |
| 发布侧幂等 | EapEventPublisher.publish:SETNX eap:mid:{dev}:{mid} TTL10min |
| 手动 ACK | application.yml listener.simple.acknowledge-mode=manual,concurrency 4/max 8,prefetch 50;sink 与 MES 消费者各自 basicAck/basicNack |
| 死信兜底 | 消费失败 basicNack(tag,false,false) → DLX → eap.events.dlq |
| 三跳幂等 | eap:mid:(发布) / eap:sink:mid:(落库,TdengineSinkService) / eap:mes:mid:(消费,EapMqConsumer) |

**原理深挖**:

(1) **为什么 topic 交换机 + 这个 rk 规范**:rk=`eap.{protocol}.{eventType}` 让消费端可以按维度订阅——MES 现在绑 `eap.#`(全量),将来只想收报警就绑 `eap.*.param_alarm`,只想收 SECS 设备就绑 `eap.secs_gem.#`,不用改发布端。这比 fanout(全给)和 direct(精确匹配)都灵活,是"事件分类学"设计。

(2) **为什么双队列扇出而不是一个队列两个消费者**:落库和业务的**故障域与速率特征不同**——TDengine 写入快而稳,MES 处理慢(带事务和 DB 写)。同队列竞争消费会导致慢消费者拖累落库;分队列后 MES 宕机重启,时序数据照常落库,MES 从自己队列续消费(积压 15 万条也实测过,清积压不影响 sink)。

(3) **不丢消息逐环节**:
- 发布端:`publisher-confirm-type=correlated` + `publisher-returns=true` 已开启(broker 落盘确认与路由失败回调的通道是通的);当前版本**没有做 confirm 失败补偿**(未注册回调),零丢失由**端到端对账**兜底证明——这是诚实边界,生产版应加本地消息表+confirm 回调重发;
- Broker:交换机/队列 durable + 消息持久化(Spring AMQP 默认 PERSISTENT)→ broker 重启不丢;
- 消费端:手动 ack,处理成功才确认;异常 nack(requeue=false) 进死信而不是无限重投;prefetch=50 限制单消费者在途量(宕机时最多 50 条回到队列重投,配合幂等无害)。

(4) **三跳幂等的失效域分析(为什么一跳不够)**——这是本条最值钱的答法:
- **发布侧** `eap:mid:`:防"上游重复进入"——MQTT QoS1 broker 重发、设备自身重发。拦在最前面,重复消息不浪费总线;
- **落库侧** `eap:sink:mid:`:防"消费端重投"——sink 消费者重启/通道断开时,unacked 消息会 redeliver;SETNX 挡住重复写(TDengine 同 ts 会 upsert 覆盖,但重复行会污染 count 对账);
- **MES 侧** `eap:mes:mid:`:防"业务重复生效"——报工是钱和物料级别的写入,哪怕前两道都漏了(比如发布侧 Redis 抖动),最后一道也必须挡住重复报工。
- 三道 SETNX 键前缀不同、各自独立 TTL 10min:任何一道失效只损失该道的防护,不连锁。
- **设计权衡(主动讲)**:发布侧是"先 SETNX 后发送",若 SETNX 成功但 convertAndSend 失败,该 mid 在 10min 内会被误判重复而丢弃——用"极小概率丢一条 vs 重复入库"做权衡,设备 mid 语义=同一物理事件,丢弃可接受;要更强可以改成"发送成功后再写去重键+消费端兜底",消费端本来就有兜底所以当前顺序是对的。
- **无 mid 的事件**:Modbus 轮询事件与监控事件(ONLINE/OFFLINE)没有 mid——轮询事件天然幂等(周期性快照,重复无业务损害),监控事件只在状态迁移时发布(迁移本身去重)。

(5) **seq 的作用**:每设备 AtomicLong 递增,消费端可检测乱序与缺口(MQ 多消费者不保证全局序);与 mid 分工:mid 是设备侧身份(去重锚点),seq 是网关侧序号(审计/排序锚点)。

(6) **压测与对账方法论(数字怎么来的,必须能讲)**:
- 发生器 load_feeder.py:200 个逻辑设备(topic DEV-LOAD-001~200),1Hz × 60s = 12000 条,mid=`{run_id}-{dev}-{seq}` 全局唯一;收尾逐条 `wait_for_publish` 等 PUBACK(修复前 sleep(3) 赌尾部 ACK,高负载下出现过 1 条假性丢失——§5 故事 6);
- 对账:TDengine 按 `device_code like 'DEV-LOAD-%' and params like '%"ts":%' and ts∈[T0-3s, T0+65s)` 计数 = 发送数。为什么这么过滤:SPC 报警行 params 无 ts 字段要排除;子表 TAG(protocol)建表时定型不可靠,不能拿来过滤;窗口下界放宽 3s 是因为发生器打印 T0 前已发出首条(边界抖动曾造成 11999 假差);
- 三角验证:落库数=发送数 + DLQ 增量=0 + 队列积压=0,三者同时成立才叫零丢失;
- P99 口径:设备时间戳(feeder 打点)→ sink 落库确认后打点,LatencyStats 记录;压测前 `POST /gateway/metrics/latency/reset` 清零(累计口径会跨轮污染);首轮压测是 JIT/TDengine 冷缓存预热轮,取第二轮热态值。

**数字出处**:
- 零丢失:12000/12000 多轮复测(独立口径、共存口径、e2e 内各过);DLQ 增量恒 0(除故意注入的脏报文);
- P99 ≤500ms:低负载时段 244ms(p50=113/p95=209/max=319),晚间内存受压独立口径 422ms(p50=127/p95=288/max=517),与 MES 同机共存约 535ms——**主动分口径讲**,README 有书面定义;
- 复现:`python verify_t4.py`(默认预算 500ms;`T4_P99_BUDGET=0` 为共存参考模式)。

**拷打 Q&A**:

1. **手动 ack 下消息的完整生命周期?**
   答:入队 ready → 投递给消费者变 unacked(受 prefetch 限额)→ 处理成功 basicAck 删除;basicNack(requeue=true) 回队首重投;requeue=false 走死信路由;消费者连接断开,其全部 unacked 自动 requeue。我的选择:成功 ack、业务性失败(如批次无任务)也 ack(放行)、未知异常 nack 不重投入死信。

2. **为什么 nack 不 requeue=true?**
   答:毒消息会无限循环——我真实踩过:一个 remark 字段溢出异常导致同一条消息 反复投递→异常→回队,MES 日志刷爆、CPU 空转(§5 故事 4)。正确姿势是 nack 进死信,人工/定时任务分析死信,修复后再回放。

3. **死信触发的三种条件?你用了哪种?**
   答:①消息被 reject/nack 且 requeue=false;②消息 TTL 过期;③队列超长度。我只用①。死信路由到默认交换机("")按 rk=eap.events.dlq 直达死信队列——比挂专用 DLX 交换机少一跳。

4. **prefetch=50 怎么定的?调大调小什么影响?**
   答:prefetch 是单消费者未确认消息上限。太小→消费者频繁等消息,吞吐低;太大→单消费者囤积消息,宕机时重投量大、负载不均。50×8 并发=400 在途,压测下 broker 到消费者管道刚好饱满;MES 侧业务重(带事务)所以配 10。

5. **publisher confirm 和 AMQP 事务的区别?**
   答:事务是同步阻塞多条一批,吞吐掉一个数量级;confirm 是异步单条/批量确认,broker 落盘后回调,性能高得多。我配 correlated 模式(确认与原始消息关联)。

6. **confirm 失败了你怎么补偿?(陷阱题,考诚实)**
   答:当前版本没有做自动补偿——confirm 通道开着但未注册回调处理,零丢失靠端到端对账证明(压测多轮全对平)。生产化的正确做法:本地消息表(事件先落库再发送,confirm 失败标记重发,定时任务扫未确认),这是明确的改进项,我能画出完整流程。

7. **Redis 挂了幂等怎么办?**
   答:SETNX 抛异常 → 我的发布侧会怎样?(代码里 Redis 异常会向上抛,该条发布失败——宁可少发不可重发?不,实际是 publisher.publish 抛异常被协议适配层 catch,事件丢弃并留日志)。诚实答:Redis 是幂等的强依赖,挂了退化为"可能重复"或"发布失败";生产方案:Redis 高可用(sentinel/cluster)+ 业务侧兜底唯一约束(如 mes_work_rec 加 (device_code,mid) 唯一索引)——目前 MES 表没加这个约束,是改进项。

8. **TTL 为什么 10 分钟?**
   答:去重键只需覆盖"重复投递的最大时间窗"——QoS1 重发是秒级,消费者重启重投是分钟级;10min 是数量级冗余。TTL 太短防不住晚到重复,太长占内存(键尺寸≈设备数×事件率×TTL,压测 12000 键×~60B≈MB 级,可忽略)。

9. **队列参数是谁声明的?两端不一致会怎样?**
   答:网关(MqConfig)与 MES(EapMqConfig)**双端以完全相同参数幂等声明**——RabbitMQ 对已存在队列做 passive 校验,参数不一致会 PRECONDITION_FAILED 拒掉。双端声明的好处:任一服务先启动都能正常工作,不依赖启动顺序。

10. **消息顺序性怎么保证?**
    答:不保证全局序,也不需要——事件带 eventTime 与 seq,消费端按需重排;RabbitMQ 单队列单消费者才有严格 FIFO,我 4-8 并发消费本来就是乱序模型。对时序落库,TDengine 按事件时间戳写入,乱序到达也能落对位置(同一子表 ts 单调防护处理同毫秒冲突)。

11. **P99 怎么测的?为什么不用 Prometheus histogram?**
    答:自研 LatencyStats(2 万样本环形缓冲,排序取分位)+REST 快照,压测前 reset 清零。选轻量实现是因为演示环境不想引入监控栈;局限是单机内存态、无百分位合并——生产用 Prometheus summary/histogram 或 TDigest。测量点选择是关键:设备打点→落库确认,覆盖了 broker 排队与消费处理,不含采集轮询周期(那是采样间隔不是延迟)。

12. **怎么排除"TDengine count 本身不准"的怀疑?**
    答:三角验证:①发送侧计数=12000(wait_for_publish 全部 PUBACK 确认后才打印);②落库侧窗口 count=12000;③DLQ=0 且队列积压=0(没有消息滞留在任何环节)。三个独立观测点互相印证,单点造假不可能同时对齐。

13. **200 路并发,网关 CPU/内存什么水平?**
    答:演示机(16G Windows,多项目共存)网关限堆 512m,压测期间 GC 正常、无积压;瓶颈实测在机器整体内存(换页导致尾延迟波动,同口径 P99 从 244→422ms),不在网关代码路径——这也是我把"独立口径/共存口径"分开报告的原因。

**弱点与应对**:
- confirm 回调补偿未实现(对账兜底,生产版本地消息表)——主动讲;
- 单节点 RabbitMQ,无 quorum queue/镜像——演示环境取舍,概念清楚(经典镜像 vs quorum 的 Raft 机制能讲);
- 消息体无 schema 注册(JSON+__TypeId__ 头,MES 侧靠手工反序列化解耦——这反而成了跨仓库类型隔离的设计点:__TypeId__ 指向网关类,MES 没有该类,所以用原始报文解析,顺便实现了契约松耦合);
- 200 路=逻辑通道(见 3.1 Q13 口径)。

### 3.3 条3「时序存储与 SPC 判异」

**简历原文**:基于 TDengine 超级表 + 设备子表分区存储工艺参数,支持历史回溯与降采样趋势查询,10 万点级趋势查询耗时 ≤120ms;采集链路内实时计算 X̄-R 管制图并执行三条判异规则,在边缘侧完成超限报警事件推送 MES,由 MES 转待审核异常单。

**白话**:工艺参数是典型时序数据(高频、按设备、写多读少、查询多是"最近 N 天按分钟聚合"),所以用 TDengine:一张超级表定义 schema,每台设备一张子表,设备编码/协议做 TAG;查询用 INTERVAL 降采样,45 万点全表聚合实测中位 121ms。SPC 是质量管控的统计方法:每 5 个采样点算一个子组的均值和极差,用历史子组估计管制限,当前子组超限(R1)、连续 7 点同侧(R2)、连续 6 点单调(R3)任一命中就判"过程异常",发报警事件让 MES 转人工审核单——判异在网关边缘侧完成,不依赖上层系统。

**代码地图**:

| 声称点 | 代码位置 |
| --- | --- |
| 库/超级表建模 | TdengineSinkService.initSchema:CREATE DATABASE eap DURATION 7 KEEP 365;STABLE device_params(ts,v,i,power1,power2 FLOAT,params NCHAR(256)) TAGS(device_code,protocol) |
| 设备子表 | onEvent:table = "d_"+sanitize(deviceCode),首建走双检锁 CREATE ... USING ... TAGS |
| 写入 | REST 驱动(TdengineConfig:RestfulDriver+HikariCP max8/minIdle2/timeout3s),INSERT 绑定 epoch-long |
| ts 单调防护 | lastTsMap.merge(table, eventTs, (old,cur)-> cur>old?cur:old+1) |
| 降采样查询 | verify_t3 用 SELECT ... INTERVAL(...) 断言;趋势实测 bench_resume_claims.py |
| X̄-R 管制图 | SpcMonitor:Series{buffer,means,ranges,cl,ucl,lcl},A2/D4 系数表(n=2..10) |
| 三条判异规则 | SpcMonitor.judge:R1 超管制限/R2 连续7点同侧(1e-9死区)/R3 连续6点单调 |
| 判异→报警事件 | EventFactory.observeSpc:rule!=null → PARAM_ALARM(protocol=SPC,params={param:value,rule:1.0}) |
| MES 转待审核单 | (项目二)EapIntegrationService.autoAlarm → mes_ycrec status=0 待审核,不自动停线 |

**原理深挖**:

(1) **TDengine 建模三件事**:
- **超级表/子表**:超级表是 schema 模板(列+TAG 定义),每设备一张子表继承模板并固化 TAG 值。查询按 TAG 过滤即定位设备,聚合可跨子表(超级表级查询自动分区并行);
- **DURATION 7 KEEP 365**:数据文件按 7 天一个组滚动,总保留 365 天。为什么不用默认/按天:分组太小→小文件多、元数据压力大;太大→过期删除粒度粗。7 天在"文件数"和"删除粒度"之间平衡(TDengine 3.x 语法是 DURATION,2.x 的 DAYS 已废弃——我踩过版本差异);
- **TAG 不可变的坑**:子表 TAG 建表时定型,之后同设备换协议上报,TAG 仍是首次值——所以对账过滤不能依赖 protocol TAG,要用 params 内容(压测对账的过滤条件就是这么来的)。

(2) **写入路径三个坑(全部实测踩过,§5 有故事)**:
- 时区:REST 驱动对 java.sql.Timestamp 追加本地时区偏移(+8h)→ 改绑 epoch-long 毫秒,由驱动按 UTC 语义处理;
- 连接:DriverManager 直连每次 TCP+HTTP 握手,压测 p99=581ms → HikariCP 池化后 p99=244ms(2.4 倍改善,这是"连接池消除握手尾延迟"的教科书案例);
- 同毫秒覆盖:TDengine 同子表同 ts 是 upsert 语义——遥测事件与它触发的 SPC 报警事件同毫秒发布,报警行(params 无 ts)覆盖遥测行(params 有 ts),对账差 1 且无任何错误日志。修复:写入前按子表做 ts 单调防护(merge 原子操作,冲突时取 old+1)。

(3) **X̄-R 管制图(要能白板推)**:
- 子组:连续 n=5 个采样为一组;组均值 X̄、组极差 R=max-min;
- 管制限(分析用历史子组估计):CL=X̿(所有子组均值的均值),UCL/LCL=X̿±A2·R̄;R 图 UCL_R=D4·R̄;
- 系数来历:A2=3/(d2·√n),d2 是"极差/σ"的期望(查表 n=5→d2=2.326,A2=0.577,D4=2.114)——本质是把 3σ 换算到极差统计量上,因为小组样本算标准差不稳,极差更稳健;
- 我的实现细节(防自证偏差):当前子组的管制限用**它之前**的全部子组估计(means.subList(0, size-1)),否则异常点会拉宽自己的管制限、把自己"洗白";
- 冷启动:minSubgroups=5,不足 5 个子组不判异(limitsValid=false);
- R2 死区:浮点数据与 CL 几乎重合时用 1e-9 死区判"在线上",中断同侧游程——不加死区时模拟器恒值数据会误报 R2(踩过);
- 判异输出去向:违规记录进 violations 环形队列(容量 100,REST 可查),同时发 PARAM_ALARM 事件(protocol=SPC)入总线——复用阈值报警同一条 MES 闭环(待审核异常单,人工决定是否停线,符合质量管理"机器判异、人做处置"的语义)。

(4) **边缘侧判异 vs 集中式 FDC(架构观点题,必准备)**:
- 大厂标准架构:EAP 只采集,FDC/SPC 是独立系统(集中建模、跨设备关联、算法版本管理);
- 我的选择:轻量场景把阈值+X̄-R 判异放网关边缘侧——就近判定、只上送报警事件与明细,省带宽、低时延、上层断连期间判异不中断;
- 权衡表述:"两种架构我都清楚,选边缘侧是场景决定的——单网关百路级设备、判异规则轻量;如果规则要频繁迭代或需要跨设备多元统计,就该上独立 FDC。"

**数字出处**:
- 趋势查询 ≤120ms:实测 device_params 全表 454,905 行,12h 窗口 10s 降采样聚合 ×5 次:79/101/121/122/126ms,中位 121ms——10 万点级(简历口径)只会更快;复现 `python bench_resume_claims.py`;
- SPC 管制限:稳定数据 220±0.1 下 CL=220.022 / UCL=220.137 / LCL=219.906(n=5,A2=0.577,R̄≈0.2);注入漂移子组 244 → 命中 R1(verify_t3b,运行级设备名隔离历史状态,稳定相零误报+漂移必命中双断言);
- 复现:`python verify_t3.py`(落库/三协议TAG/降采样)、`python verify_t3b.py`(SPC)。

**拷打 Q&A**:

1. **为什么 TDengine 不用 InfluxDB/ClickHouse/IoTDB?**
   答:场景匹配——设备级子表天然对应"一设备一表"模型,TAG 过滤+降采样聚合是内置语法,写入按子表分区无锁竞争;InfluxDB 高基数标签会内存爆炸(设备数就是高基数);ClickHouse 是分析型列存,高频小批量写不是强项(要攒批);IoTDB 定位类似但当时生态/文档 TDengine 更成熟。核心论点:工业时序的访问模式(按设备+时间窗聚合)与 TDengine 的存储模型(超级表-子表-时间分组)一一对应。

2. **超级表和宽表/单表比,好在哪?**
   答:单表全设备混存→查询靠 WHERE device 过滤,数据量大了扫描成本高;每设备独立普通表→无统一 schema,跨设备聚合要 UNION N 张表。超级表=schema 统一+物理按子表隔离+TAG 索引下推,单设备查询直达子表,跨设备聚合并行扫子表,两头都占。

3. **X̄-R 为什么 n=5?什么时候用 X̄-s 或 I-MR?**
   答:经验法则 n≤8 用极差 R(计算简单、小样本下比标准差稳健),n>8 用标准差 s(极差浪费信息);I-MR 用于单件流/批量检测(每个数据点自成一组)。半导体 lot 内多片测量天然成组,子组 5 是 SEM 手册常用配置。

4. **A2=0.577 怎么来的?推一下。**
   答:管制限本质是 X̄ 的 3σ 限:X̿±3σ/√n。σ 用极差估计:σ̂=R̄/d2(d2 是 E[R]/σ,查表 n=5→2.326)。代入:3/(d2√n)=3/(2.326×2.236)=0.577=A2。所以 A2 表就是 3/(d2√n) 的数值表,D4=1+3d3/d2 同理(R 的 3σ 上限)。

5. **R2 为什么 7 点、R3 为什么 6 点?**
   答:游程规则来自 Western Electric/Nelson 规则集,按误报率设计:点在 CL 两侧各 1/2 概率,连续 7 点同侧概率 (1/2)^7≈1/128,与 3σ 单点误报率(0.27%)同数量级——即"判异灵敏度等价于一次 3σ 事件";6 点单调同理 (1/6!×2≈0.28%)。它们抓的是"没超限但分布已经漂移"的过程异常,比 R1 更早预警。

6. **管制限一直用全历史累计,过程漂移了怎么办?**
   答:这是当前实现的已知局限——均值漂移后旧管制限变宽,灵敏度下降。标准做法:分析期(如前 25 组)冻结管制限用于管制期,或滑动窗口重估。我的实现是"全历史+当前点剔除",演示场景够用;改进项是加滑动窗口参数,能讲清怎么改。

7. **R 图判异做了吗?(诚实题)**
   答:D4 系数表已内置,R̄ 参与 X̄ 图管制限计算,但 R 图自身的判异(极差超 D4·R̄)没有启用——当前判异全部基于 X̄ 图三规则。R 图主要抓"组内波动突变"(如夹具松动),补上只需在 judge 里加一条 range>d4*rbar,是明确的待办。

8. **SPC 误报了怎么办?会不会自动停线?**
   答:不会——判异事件到 MES 生成的是**待审核**异常单(mes_ycrec status=0),停线与否由人决定。这是刻意的质量管理设计:统计判异有误报率(R2 约 1/128),自动执行处置动作会在误报时造成不必要的停产损失;"机器判异、人工处置"闭环既保留预警价值又不放大误报成本。

9. **10 万点 ≤120ms 怎么测的?包含网络吗?**
   答:包含——REST 端到端(Python→6041→TDengine 计算→返回 JSON),实际测的是 45.5 万点全表 10s 降采样,5 轮中位 121ms、最好 79ms;简历按保守口径写"10 万点级 ≤120ms"。查询本身是列存+时间分组索引的典型受益场景。

10. **params 为什么存 NCHAR(256) JSON 而不是拆列?**
    答:不同设备参数集不同(Modbus 是 v/i/power1,SECS 四个,压测带 ts),拆列要么宽表列爆炸要么频繁 DDL;JSON 列保 schema 灵活,常用参数已提为 FLOAT 列(v/i/power1/power2)支持高效聚合,params 留作明细回溯——冷热分离的折中。

**弱点与应对**:
- R 图判异未启用(Q7 诚实答);管制限无滑动窗口(Q6);
- SPC 监控参数名单默认只配了 v(spc.params=[v]),多参数要扩配置;
- 落库是逐条 INSERT 非攒批——当前吞吐足够(压测下 p99 达标),更高吞吐可攒批(批内多子表 INSERT INTO t1 VALUES... t2 VALUES... 语法支持),代价是 ack 语义复杂化(批量部分失败处理),演示环境选了简单正确;
- TAG 不可变导致协议标签失真(首事件定型)——对账方法已绕开,建模上可用"协议变更建新子表"或 TAG 放宽语义处理。

### 3.4 条4「设备连接监控与可靠性统计」

**简历原文**:Redis 缓存设备配置与心跳状态,支持阈值热更新、秒级离线判定;基于设备状态迁移数据自动统计 MTTR、MTBF 与设备可用度,单网关可稳定接入 100 路以上现场仿真设备。

**白话**:网关自己当"设备的保姆"——每台设备最后一次上报时间记在内存里,后台每 2 秒扫一遍,静默超过 3 倍轮询周期就判离线、发 OFFLINE 事件,恢复上报发 ONLINE 事件;设备的阈值/批次映射配置放 Redis(可运维改),本地缓存 5 秒刷新,改配置接口立即生效不用重启;每次离线/恢复的区间累计起来,就能算出这台设备的 MTBF(平均无故障时间)、MTTR(平均修复时间)和可用度。

**代码地图**:

| 声称点 | 代码位置 |
| --- | --- |
| 心跳 | DeviceMonitorService.touch(事件驱动更新 lastSeen+state,由 EventFactory.build 调用——"数据即心跳") |
| 离线判定 | DeviceMonitorService.scan @Scheduled(2s):now-lastSeen > pollMs×offlineFactor(3) 且 state==RUNNING → OFFLINE |
| 状态迁移事件 | publishTransition:仅迁移时发 ONLINE/OFFLINE(防事件风暴),带 protocol/sn |
| Redis 配置缓存 | DeviceRegistry:hash eap:dev:cfg:{code}(thresholds json/pollMs/sn),seed 不覆盖已存在键 |
| 阈值热更新 | DeviceRegistry.update(写 Redis+写本地缓存,即时生效)+ SimController POST /gateway/devices/{code}/config |
| 周期刷新 | DeviceRegistry.refresh @Scheduled(5s) |
| MTTR/MTBF | ReliabilityService:recordRunning/recordFailure(状态迁移钩子)+report(upMs/downMs/failures/mtbf/mttr/availability) |
| 延迟分位 | LatencyStats(2万样本,percentile 快照,reset 接口) |

**原理深挖**:

(1) **"数据即心跳"的设计取舍**:没有引入独立心跳报文——Modbus 轮询本身就是探活(每周期一问一答),SECS 有 Linktest 语义,MQTT 有 broker 层 keepalive;业务数据的到达就是最强的活性证明(设备连着但不干活,Linktest 也发现不了)。统一抽象:lastSeen 时间戳,任何协议的事件都 touch。

(2) **离线判定阈值推导**:threshold = pollMs × offlineFactor(3) = 3s(默认 1s 轮询),加扫描粒度 2s,理论判定延迟 3~5s——**实测 4.1s**,数字与推导吻合(这是"数字能讲出推导过程"的典型例子)。为什么 3 倍:1 倍会因单次网络抖动误报,2 倍容忍一次丢失+一次迟到,3 倍是误报率与灵敏度的平衡;做成配置项(offline-factor)让场景可调。

(3) **状态机防事件风暴**:只有 RUNNING→OFFLINE 和 OFFLINE→RUNNING 迁移才发事件,静默期间每 2s 扫描不会重复发 OFFLINE;首次接入(prev==null)只记可靠性区间不发 ONLINE(没有"恢复"语义)。

(4) **可靠性指标的区间模型**:
- 每台设备维护 Acc{upMs, downMs, failures, runningSince, downSince};
- OFFLINE 迁移=故障发生:failures++,upMs += now-runningSince(结算运行区间),downSince=now;
- ONLINE/首次接入=修复完成:downMs += now-downSince,runningSince=now;
- **MTBF = Σup / failures,MTTR = Σdown / failures,可用度 = Σup/(Σup+Σdown)**;查询时未闭合区间按当前时间延伸(运行中设备的 up 持续增长);
- **口径注意(主动讲)**:时间戳是"检测时刻"不是"物理时刻"——MTTR 含离线判定窗口(3~5s),偏保守;物理口径 = 检测口径 - 判定窗口。验收用两轮"停从站→OFFLINE→启从站→ONLINE"循环断言 failures+2、指标自洽(verify_t5b)。
- 与项目二 OEE 的区分(必考):网关的可用度是**通信可用性**(连得上/连不上),MES OEE 的时间稼动率是**业务可用性**(运行态/报警态流水积分)——两套口径服务两个层次,数据源都是状态迁移流水但语义不同。

(5) **配置中心分层**(为什么不是"每事件查 Redis"也不是"纯 yml"):
- 纯 yml:改阈值要重启,运维不可接受;
- 每事件查 Redis:200 事件/s × 网络 RTT,热路径上不可接受;
- 折中:Redis 为源(可运维、多实例共享)+ 本地缓存(热路径零开销)+ 5s 周期刷新(多实例最终一致)+ 更新接口双写(本实例即时生效)。
- seed 语义:启动时 yml 默认值**只写不存在的键**——保护运行期已修改的配置不被重启回滚。

**数字出处**:
- 离线判定 4.1s / 上线恢复 9.2s(含重连退避+轮询恢复):verify_t5,停/启内置从站实测,OFFLINE/ONLINE 事件各 1 条断言;
- 阈值热更即时生效:同一设备阈值 v:300 期间报警 0 条 → 改 v:205 后报警 7 条(verify_t5,改配置不重启);
- MTTR/MTBF:两轮故障循环 failures+2、可用度与 up/down 毫秒自洽(verify_t5b,修复了"用舍入后分钟数反推可用度"的验收脚本舍入误差后全绿);
- 100 路以上稳定接入:200 路压测多轮零丢失 + 三协议模拟器常驻多天连续流转(本机演示环境,内存受压时段尾延迟上升但零丢失不破)。

**拷打 Q&A**:

1. **扫描线程 2s 一次,设备多了会不会成为瓶颈?**
   答:lastSeen 是 ConcurrentHashMap,扫描是纯内存遍历,O(设备数) 每 2s——万级设备也就是 2s 内遍历 1 万个 entry,微秒级/个,毫无压力;真要优化可按 pollMs 分桶或用时间轮(HashedWheelTimer)把"逐个比较"变"到期回调"。

2. **为什么不用 Redis 键过期通知做离线判定?**
   答:考虑过。keyspace notification 是"尽力而为"(可能丢),且过期时间粒度受惰性删除策略影响不精确;自己扫描的判定延迟可推导可断言(3×poll+2s),验收友好。演示环境单实例,扫描成本可忽略;多实例部署时扫描要加分布式锁或按设备分片——改进项能讲。

3. **MTBF/MTTR 的统计窗口是什么?跨天怎么算?**
   答:当前是"网关进程生命周期内累计"(内存态),重启清零。生产化应该:状态迁移流水落库(MES 侧其实已经落了 mes_eap_device_state_log),指标按天/班次窗口聚合——项目二 OEE 引擎就是按窗口对流水做区间积分的,同一套思路。

4. **可用度和 OEE 时间稼动率什么区别?(边界题)**
   答:见 §3.4(4)——通信可用度 vs 业务时间稼动率;前者答"设备在线率",后者答"在线的时间里有多少在干活"。两个指标层次不同、数据源不同(网关心跳迁移 vs MES 业务状态流水),我在两个项目里分别实现,口径分得很清楚。

5. **Redis 挂了监控还能工作吗?**
   答:能降级工作——配置读走本地缓存(5s 刷新失败不覆盖旧值),心跳与状态机是纯内存;损失的是配置修改入口(update 会失败)。SETNX 幂等也依赖 Redis,挂了发布侧幂等失效——Redis 是这套体系的软依赖,生产要上 sentinel/cluster。

6. **阈值热更新,多实例怎么一致?**
   答:更新接口写 Redis(源)+本实例缓存(即时);其他实例靠 5s 周期刷新收敛——最终一致,窗口≤5s。要更强可以加 Redis pub/sub 广播失效,配置类数据 5s 收敛在工业场景完全够用。

7. **ONLINE/OFFLINE 事件下游怎么消费?**
   答:MES 消费者对这两类事件直接 ack 放行(不入报工链路);它们的价值在:①时序库留状态流水;②可靠性统计;③看板展示;④将来可挂通知(短信/钉钉)。事件模型统一的好处是新增消费方不用改发布端。

8. **"稳定接入 100 路"的"稳定"怎么定义?(抠字眼题)**
   答:三个可观测判据——压测期间零丢失(对账)、DLQ 零新增、延迟分位在预算内;外加演示环境多日常驻运行无泄漏(内存限堆 512m 下 GC 正常)。我不会说"7×24 生产验证"——这是演示环境仿真设备,口径诚实。

**弱点与应对**:
- 可靠性统计内存态,重启清零(Q3 给了生产化路径);
- LatencyStats 用 synchronizedList(ArrayList),满 2 万后 remove(0) 是 O(n) 拷贝——应换 ArrayDeque 或直方图(HDR histogram);演示量级无感,被问到主动认;
- 扫描单实例语义(Q2);无设备分组/层级建模(产线-工序-设备)。

---

## §4 横向拷打专题

### 4.1 消息不丢不重全景表(一张表背下来)

| 环节 | 丢失风险 | 对策 | 重复风险 | 对策 |
| --- | --- | --- | --- | --- |
| 设备→网关 | 链路断 | 重连退避;轮询语义天然补偿 | QoS1 重发/设备重发 | 发布侧 SETNX(mid) |
| 网关→broker | 发送失败 | confirm 开启(补偿未实现,对账兜底——诚实) | 重发 | 同上(同 mid 挡住) |
| broker 存储 | 宕机丢内存 | durable 交换机/队列+持久化消息 | - | - |
| broker→sink | 消费者宕机 | unacked 自动 requeue | redeliver | sink 侧 SETNX |
| broker→MES | 同上 | 同上 | 同上 | MES 侧 SETNX |
| 消费处理失败 | 消息卡死 | 手动 ack;异常 nack→死信(可回放) | 毒消息循环 | nack 不 requeue(§5 故事 4) |
| 落库 | 写失败 | nack 入死信保现场 | 同 ts upsert 覆盖 | ts 单调防护(§5 故事 3) |

### 4.2 三个时间戳与时钟问题(深挖必到)

- **params.ts**:设备/发生器打点时间(真正的"数据时刻");
- **eventTime**:网关发布时刻(Publisher 补);
- **guardTs**:落库主键(sink 单调防护后可能 = eventTime 或 +N ms);
- 延迟口径 = sink 落库确认时刻 - params.ts,覆盖"设备→总线→落库"全链,不含采集轮询周期(那是采样间隔);
- **时钟偏差(诚实边界)**:压测同机无偏差;真实产线设备时钟不齐会把偏差算进延迟——生产要求设备 NTP 对时,或以网关接收时刻为准牺牲设备侧精度。TDengine 时区坑(REST 驱动 +8h)是同源问题:统一用 epoch-long 消除本地时区语义。

### 4.3 模拟器体系与"为什么自写"(选型故事)

- 现成库调研:secsgem(Python,API 陈旧且 List 构造签名与文档不符、secsitcp 是 SECS-I 不是 HSMS)、secs4j(公网仓库不可得)→ 决策:按 SEMI E4/E5/E37 手写 Java 编解码 + 手写 raw-socket Python 设备模拟器;
- **交叉验证思想**:模拟器与网关是两套独立实现(不同语言、不同人写),对同一规范编码/解码——单方实现错误会在对接时暴露(比如 S2F41 的 CPList 偏移解析就是靠模拟器回显抓出来的);
- Modbus 从站内置在网关进程(故障注入接口 /sim/modbus/stop,验收断线重连);MQTT 用脚本(载荷可控 v 值/带 mid);压测发生器独立脚本(参数化设备数与时长);
- 这套模拟器支撑了全部 14 项验收的可重复性——"没有真设备你怎么验证"的标准答案。

### 4.4 与两个 MES 的关系(边界口径)

- **Liao-MES(项目二,EAP-MES 仓库)**:网关的正式下游。MQ 通道:消费 mes.integration 自动报工/报警闭环(EapMqConsumer);HTTP 通道:POST /mes/eap/event(X-EAP-Key 密钥),双通道互为备份。事件契约=EapEvent JSON 字段,设备→批次映射在网关侧配置;
- **hm-MES(开源参考项目,非交付物)**:其 iot-component(Netty 多协议采集)写 Redis 队列,我的 eap_bridge.py 消费并转成标准事件推给 Liao-MES——演示"第三方采集组件适配进我的事件模型"的跨系统集成能力。被问"hm-MES 是你项目吗":不是,它是开源参考,我做的是桥接适配,读得懂它的 Netty 组件/编解码器/协议工厂设计;
- 仓库:项目一 pengbo-web/eap-gateway-demo(main),项目二 pengbo-web/EAP-MES(feature/mes-six-modules,与 master 的 diff 即二开范围)。

### 4.5 "真上生产你会怎么改"(视野题,按优先级背)

1. **可靠性**:publisher confirm 回调+本地消息表补偿;RabbitMQ quorum queue;Redis sentinel;网关多实例+设备分片(按 deviceCode 哈希,避免双采);
2. **可观测**:Prometheus 指标替代自研 LatencyStats(histogram+percentile),Grafana 看板,死信告警;
3. **性能**:sink 攒批写(批内多子表语法);EventLoopGroup 共享连接池化(千级连接);TDengine 原生驱动+stmt2 绑定;
4. **安全**:HSMS over TLS、接口鉴权(现在 REST 裸奔,演示环境)、消息体敏感字段加密;
5. **配置/运维**:Nacos/Apollo 替代 Redis 手搓配置中心;设备/点位表驱动(现在 modbus-targets 是 yml 列表);灰度与回滚;
6. **协议完备性**:GEM 完整模型(S1F15/S1F17/S2F33 动态报告配置)、Modbus 写指令(FC6/FC16)、OPC UA(未来主流);
7. **边缘能力**:断网续传(本地磁盘缓冲队列)、规则引擎可编排(现在阈值+SPC 是代码写死的两类规则)。

### 4.6 八股联动(项目里真实用到的,往深处引导)

- **Netty**:Reactor 主从模型、pipeline/handler、ByteBuf 引用计数(decode 后 release)、LengthFieldBasedFrameDecoder、writeAndFlush 异步 listener;
- **RabbitMQ**:交换机四类型、死信三条件、prefetch、手动 ack、confirm/return、持久化、消息 TTL、惰性队列(积压 15 万条实测);
- **Redis**:SETNX+TTL 原子性(为什么不用 setnx+expire 两条)、StringRedisTemplate 序列化、hash 结构做配置、Redisson(MES 侧)、缓存旁路模式(DeviceRegistry 的读写路径);
- **并发**:ConcurrentHashMap(merge 原子性用在 ts 防护)、AtomicLong、CompletableFuture(orTimeout/thenApply)、volatile(state/channel)、synchronized 双检锁(建表,§5 故事 6 是双检锁失效的活教材)、@Scheduled 单线程语义;
- **Spring Boot**:@ConfigurationProperties 绑定、条件装配(exclude DataSourceAutoConfiguration——网关无关系库)、@RabbitListener 容器工厂参数、Jackson2JsonMessageConverter 与 __TypeId__;
- **TDengine**:超级表/TAG/DURATION/KEEP、INTERVAL 降采样、upsert 语义、REST vs 原生驱动、写入幂等与乱序容忍。

---

## §5 排障故事集(STAR 格式,每个 60-90 秒能讲完)

> 面试说"遇到最大的困难/印象最深的 bug"时,从这里挑。每个都是真事,有日志有代码有验收脚本背书。

### 故事 1:MBAP 长度字段差一个字节(协议理解)
- **S**:自写 Modbus TCP 编解码,首次对接模拟器,响应帧解析乱套;
- **T**:定位分帧错误;
- **A**:构造 17 字节探针报文(已知内容)抓收发字节流逐字段对照,发现 MBAP 的 length 语义是"unitId+PDU"=2+data.length,我写成 1+data.length——差 1 字节导致 LengthFieldBasedFrameDecoder 切帧错位,后续帧全部粘错;
- **R**:修正 encode/decode 后粘包拆包全对;沉淀了"协议实现先用探针报文对字节"的验证方法。
- **讲点**:length 字段"含不含自己/含不含头"是各协议最容易记混的点(HSMS 的 length 就含 10 字节头),对照规范+探针实测才可靠。

### 故事 2:TDengine 三连坑(版本/时区/性能)
- **S**:时序落库接入 TDengine 3.x REST 驱动,连续踩三坑;
- **A/R**:①建库语法 DAYS 报错——3.x 改名 DURATION,版本迁移文档里的小字;②时间戳全部 +8h——REST 驱动对 java.sql.Timestamp 追加本地时区偏移,改绑 epoch-long 毫秒消除本地时区语义;③压测 p99=581ms 不达标——DriverManager 直连每次 TCP+HTTP 握手,换 HikariCP 池化后 p99=244ms;
- **讲点**:第③个是"连接池消除握手尾延迟"的量化案例(2.4 倍);三个坑都是"文档不写、只有实测才暴露"的类型,体现独立排障能力。

### 故事 3:压测对账差 1 条——同毫秒 upsert 覆盖(最精彩,重点准备)
- **S**:200 路压测对账偶发 11999/12000,窗口外迟到行=0,双侧日志零错误,DLQ 零新增——"消失的 1 条"没有任何报错;
- **A**:排除法走完链路(发生器 PUBACK 全确认→broker publish 计数→sink ack 计数→TDengine 行数),各环节都对;回头审视日志时间线,发现丢条的 run 里恰好有 DEV-LOAD-007 的 SPC R2 判异命中记录——压测随机值(215~225 均匀分布)会天然触发判异;查 sink 代码:SPC 报警事件在 build() 里同步发布,与触发它的遥测事件**同一毫秒**到达同一子表,而 TDengine 同子表同 ts 是 **upsert 语义**——报警行(params 无 ts)覆盖了遥测行(params 有 ts),对账过滤条件恰好只数带 ts 的行;
- **R**:sink 写入前按子表做 ts 单调防护(ConcurrentHashMap.merge 原子操作,冲突取 old+1),此后多轮压测 12000/12000 稳定;
- **讲点**:①"零错误日志的数据丢失"要怀疑存储引擎写入语义而不是链路;②对账方法论(三角验证:发送=落库+DLQ=0+积压=0);③副产品认知:SPC 判异与业务事件的同毫秒并发是设计时没想到的交互。

### 故事 4:remark 毒消息循环(MQ 消费异常分型)
- **S**:MES 侧 MQ 消费上线后 DLQ 无操作增长 +324 条;
- **A**:翻 MES 日志全是同一个异常——autoAlarm 把报警明细无上限追加进 mes_task.remark,列宽溢出 MysqlDataTruncation → 事务回滚 → remark 停在临界长度 → 下一条报警又溢出 → 毒消息循环(nack 不 requeue 所以不死循环重投,但每条新报警都进死信);
- **R**:追加设上限(总长 400 内才追加、单条截断),报警明细以异常单为准,remark 只留线索;DLQ 20 秒零增长止血确认;
- **讲点**:①HTTP 时代"报错就完"的隐性缺陷,MQ 消费后被放大成可观测的死信堆积——死信队列是免费的缺陷探测器;②消费端异常必须分型:业务性跳过(ack 放行,如批次无任务)、数据性错误(nack 入死信保现场)、系统性错误(告警+熔断)——一刀切要么刷爆死信要么无限重投。

### 故事 5:paho 重连不恢复订阅(库的隐含契约)
- **S**:WSL 虚拟机重启(mosquitto 换了容器)后,压测 12000 条落库 0 条——但网关无报错、MQTT 客户端显示已连接;
- **A**:查队列 message_stats:publish 只有零星 live 事件,压测消息根本没进 MQ;检查 paho 文档与源码:automaticReconnect 只重建 CONNECT 会话,**订阅关系不会自动恢复**——这是 v3 客户端的著名坑(v5 的 MqttClient 有 session expiry 语义区分);
- **R**:注册 MqttCallbackExtended,在 connectComplete(reconnect=true) 回调里补订阅;docker restart mosquitto 实测 4s 内恢复("MQTT 订阅恢复成功"日志+计数器续增);
- **讲点**:①"已连接"不等于"在工作",活性要有业务层证据(计数器增长);②用第三方库要读它的重连语义,不能望文生义;③这个 bug 恰好被"基础设施重启"暴露,说明故障演练的价值。

### 故事 6:建表竞态与双检锁失效(并发语义,二进宫才有深度)
- **S**:SPC 验收用运行级新设备名,每轮突发 150 条消息,sink 4-8 线程并发消费,偶发 3 条死信:Table does not exist;
- **A**:第一版修复:ensuredTables.add+CREATE 包进 synchronized——**复测仍偶发**。再审代码发现经典双检锁失效:外层 `contains` 检查在"线程 A 已 add 标记但 CREATE 未执行完"时返回 true,线程 B 直接跳过锁去 INSERT;
- **R**:改为"CREATE 成功返回后才 add 标记 + 锁内二次检查",t3b 三连跑 DLQ+0、日志零次 Table does not exist;
- **讲点**:双检锁的正确性条件是"**标记可见 ⟺ 资源就绪**"——标记先行等于把未完成资源发布给其他线程,和 DCL 单例的指令重排问题是同一个本质;死信计数受 broker 统计延迟影响,压测断言窗口要把上游测试的尾巴算进去(这次 +3 其实是前一测试项的死信迟到入账)。

---

## §6 考前速记单

### 6.1 数字总表(声称 → 实测 → 出处)

| 简历声称 | 实测背书 | 复现 |
| --- | --- | --- |
| 故障恢复 ≤2s | Modbus 拔线重连 1.1s(MQTT broker 级 4s,分口径) | verify_t1 |
| 200 路并发零丢失 | 12000/12000 对账一致,多轮;DLQ+0;积压 0 | verify_t4 |
| P99 ≤500ms | 244ms(低负载)/422ms(内存受压独立口径)/共存约 535ms | verify_t4 |
| 10 万点趋势 ≤120ms | 45.5 万点降采样 中位 121ms/最好 79ms | bench_resume_claims.py |
| 秒级离线判定 | 4.1s(=3×pollMs+扫描粒度,可推导) | verify_t5 |
| 阈值热更新 | v:300→0 报警 / v:205→7 报警,不重启即时生效 | verify_t5 |
| MTTR/MTBF | 两轮故障循环 failures+2,指标与毫秒区间自洽 | verify_t5b |
| SPC 判异 | CL=220.022/UCL=220.137/LCL=219.906,注入 244 命中 R1,稳定相零误报 | verify_t3b |
| 三协议汇聚 | 落库间隔中位 27ms,三协议 TAG 断言 | verify_t3 |
| MES 集成 | 自动报工/mid 幂等/报警单/脏报文死信/ONLINE 直通 | verify_t6 |
| 100 路以上稳定 | 200 路压测+三协议常驻多日流转 | verify_e2e |

### 6.2 公式卡(白板能写)

- X̄-R 管制限:CL=X̿;UCL/LCL=X̿±A2·R̄;R 图 UCL=D4·R̄;n=5 时 A2=0.577、D4=2.114;A2=3/(d2√n)
- 判异:R1 点出限;R2 连续 7 点同侧(误报率 (1/2)^7≈1/128);R3 连续 6 点单调
- MTBF=Σ运行时长/故障次数;MTTR=Σ停机时长/故障次数;可用度=Σup/(Σup+Σdown)
- (联动项目二)OEE=时间稼动率×性能率×良率;性能率=理论周期×产量/运行时长
- MBAP length = 2 + data.length;HSMS length = 10 + data.length;format_byte=(code<<2)|nlen

### 6.3 术语一句话(30 个)

HSMS(SECS over TCP,E37)|SECS-II(消息编码格式,E5)|S6F11(设备事件上报,需 S6F12 应答)|S2F41/42(远程指令/应答)|W-bit(要求应答标志)|system byte(请求应答配对号)|Select.req/rsp(会话建立)|Linktest(保活)|MBAP(Modbus TCP 7 字节头)|PDU(功能码+数据)|FC3/FC4(读保持/输入寄存器)|事务号(Modbus 请求配对)|topic exchange(按 rk 模式路由)|rk(路由键 eap.{protocol}.{eventType})|DLX/DLQ(死信交换机/队列)|prefetch(未确认消息上限)|手动 ack(处理成功才确认)|publisher confirm(broker 落盘确认)|SETNX(存在即失败=原子去重)|三跳幂等(发布/落库/消费各一道)|超级表(TD schema 模板)|子表(每设备物理表)|TAG(子表不可变标签)|DURATION/KEEP(分组周期/保留期)|INTERVAL(降采样聚合)|upsert(同主键覆盖写)|X̄-R(均值-极差管制图)|A2/D4(管制限系数)|MTBF/MTTR(平均无故障时间/平均修复时间)|ISA-95 L2/L3(设备集成层定位)

### 6.4 代码文件速查(说出文件名=真写过)

协议:ModbusFrame/ModbusFrameDecoder/ModbusMasterClient/ModbusPollerService/ModbusSlaveSimulator;SecsItem/HsmsMessage/SecsHostClient/SecsHostService;MqttSubscriberService。
核心:EapEvent/EventFactory/EapEventPublisher/MqConfig/GatewayProperties。
存储:TdengineConfig/TdengineSinkService。
监控:DeviceRegistry/DeviceMonitorService/SpcMonitor/ReliabilityService/LatencyStats/SimController。
MES 侧(EAP-MES 仓库):EapMqConsumer/EapMqConfig/EapIntegrationService。

### 6.5 危险问题 Top10(一句话保命答法)

1. "没连真设备算什么 EAP?" → 协议实现与真机无关,模拟器是独立第二实现交叉验证;真机差的是电气层与 GEM 配置流程,报文层代码不用改。
2. "SPC 不该在 FDC 吗?" → 大厂集中式 FDC 与轻量边缘判异是场景选择,我能讲清两者取舍,选边缘侧是百路级轻量场景的合理设计(§3.3(4))。
3. "P99 到底多少?" → 分口径:低负载 244ms、受压独立 422ms、同机共存 535ms,书面口径在 README,当场可复现。
4. "confirm 失败你怎么办?" → 当前对账兜底(诚实),生产版本地消息表+回调补偿,流程我能画(§3.2 Q6)。
5. "200 路是 200 个连接?" → 逻辑通道口径(200 topic+独立 mid 序列),物理连接扩展路径清楚(§3.1 Q13)。
6. "递归/双检锁这种基础题" → 双检锁失效我真实踩过并二进宫修复,标记可见⟺资源就绪(故事 6)。
7. "为什么不用现成 SECS 库?" → 调研过 secsgem/secs4j,不可得/年久失修,手写+交叉验证,选型过程完整(§4.3)。
8. "Redis 挂了系统怎样?" → 降级矩阵:配置走本地缓存、监控纯内存不受影响、幂等退化——软依赖清单背熟(§3.4 Q5)。
9. "这项目一个人做的?哪部分最难?" → 全部独立;最难是 HSMS/SECS-II 手写与"证明零丢失"的对账体系,各展开 2 分钟。
10. "和项目二什么关系?" → 项目一是数据生产方,项目二 Liao-MES 是消费方(MQ+HTTP 双通道),OEE 在二、可用度在一,口径分明(§4.4)。

---

*文档版本:2026-10-23 · 与代码提交 b581eaa(eap-gateway-demo main)一致 · 数字如与最新 verify 输出冲突,以实测为准并回改本文档*
