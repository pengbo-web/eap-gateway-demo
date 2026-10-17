package com.pengbo.eap.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pengbo.eap.config.MqConfig;
import com.pengbo.eap.core.EapEvent;
import com.rabbitmq.client.Channel;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 时序 sink: 消费 eap.tdengine.sink 队列, 工艺参数写 TDengine 超级表
 * <p>
 * 库级时间分区 DAYS 7; 每设备一张子表(USING 超级表 TAGS); 消费失败 3 次入死信。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Service
public class TdengineSinkService {

    private final JdbcTemplate taos;
    private final org.springframework.data.redis.core.StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Set<String> ensuredTables = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, Long> lastTsMap = new ConcurrentHashMap<>();
    private final com.pengbo.eap.monitor.LatencyStats latencyStats;

    public TdengineSinkService(@Qualifier("taosJdbcTemplate") JdbcTemplate taos,
                               org.springframework.data.redis.core.StringRedisTemplate redisTemplate,
                               com.pengbo.eap.monitor.LatencyStats latencyStats) {
        this.taos = taos;
        this.redisTemplate = redisTemplate;
        this.latencyStats = latencyStats;
    }

    @PostConstruct
    public void initSchema() {
        try {
            taos.execute("CREATE DATABASE IF NOT EXISTS eap DURATION 7 KEEP 365");
            taos.execute("CREATE STABLE IF NOT EXISTS eap.device_params ("
                + "ts TIMESTAMP, v FLOAT, i FLOAT, power1 FLOAT, power2 FLOAT, params NCHAR(256)) "
                + "TAGS (device_code NCHAR(64), protocol NCHAR(16))");
            log.info("TDengine 库/超级表就绪: eap.device_params (DAYS 7)");
        } catch (Exception e) {
            log.warn("TDengine 初始化失败(容器未起时允许降级): {}", e.getMessage());
        }
    }

    @RabbitListener(queues = MqConfig.QUEUE_TAOS)
    public void onEvent(EapEvent event, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) {
        try {
            // 落库侧幂等: MQ at-least-once 重投递防护(与发布侧去重各守一段)
            if (event.getMid() != null && !event.getMid().isBlank()) {
                String dedup = "eap:sink:mid:" + event.getDeviceCode() + ":" + event.getMid();
                Boolean first = redisTemplate.opsForValue()
                    .setIfAbsent(dedup, "1", java.time.Duration.ofMinutes(10));
                if (Boolean.FALSE.equals(first)) {
                    channel.basicAck(tag, false);
                    return;
                }
            }
            String table = "d_" + sanitize(event.getDeviceCode());
            if (!ensuredTables.contains(table)) {
                // 首建表双检锁: 集合只在 CREATE 成功返回后标记, 锁内二次检查,
                // 否则并发消费者会在建表完成前 INSERT(Table does not exist -> 假性死信)
                synchronized (ensuredTables) {
                    if (!ensuredTables.contains(table)) {
                        taos.execute("CREATE TABLE IF NOT EXISTS eap." + table
                            + " USING eap.device_params TAGS ('" + event.getDeviceCode() + "', '" + event.getProtocol() + "')");
                        ensuredTables.add(table);
                    }
                }
            }
            Map<String, Double> p = event.getParams() == null ? Map.of() : event.getParams();
            String paramsJson = objectMapper.writeValueAsString(p);
            long eventTs = event.getEventTime() == null ? System.currentTimeMillis() : event.getEventTime();
            // TDengine 同子表同 ts 为 upsert: 遥测与 SPC 报警/状态事件同毫秒会互相覆盖(压测对账差1的根因),
            // 单调防护保证同子表写入 ts 严格递增
            long guardTs = lastTsMap.merge(table, eventTs, (old, cur) -> cur > old ? cur : old + 1);
            // 直接绑定 epoch 毫秒 long: TD REST 驱动对 java.sql.Timestamp 会加本地时区偏移
            taos.update("INSERT INTO eap." + table + " VALUES (?, ?, ?, ?, ?, ?)",
                guardTs,
                p.getOrDefault("v", 0d).floatValue(),
                p.getOrDefault("i", 0d).floatValue(),
                p.getOrDefault("power1", 0d).floatValue(),
                p.getOrDefault("power2", 0d).floatValue(),
                paramsJson);
            channel.basicAck(tag, false);
            Object ts = p.get("ts");
            if (ts instanceof Number n) {
                latencyStats.record(System.currentTimeMillis() - n.longValue());
            }
        } catch (Exception e) {
            log.warn("TDengine 写入失败, 入死信: device={} err={}", event.getDeviceCode(), e.getMessage());
            try {
                channel.basicNack(tag, false, false);
            } catch (Exception ignored) {
                // 通道异常忽略
            }
        }
    }

    private String sanitize(String code) {
        return code == null ? "unknown" : code.replaceAll("[^A-Za-z0-9]", "_").toLowerCase();
    }
}
