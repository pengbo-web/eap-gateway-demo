package com.pengbo.eap.core;

import com.pengbo.eap.config.MqConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * EAP 事件发布器: 统一事件模型 -> RabbitMQ topic 交换机
 * <p>
 * routing key: eap.{protocol}.{eventType}; 单设备事件序号递增, 供消费端幂等/排序。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EapEventPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final org.springframework.data.redis.core.StringRedisTemplate redisTemplate;
    private final ConcurrentHashMap<String, AtomicLong> seqMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> deviceTypeCounters = new ConcurrentHashMap<>();

    public void publish(EapEvent event) {
        deviceTypeCounters.computeIfAbsent(
            event.getDeviceCode() + ":" + event.getProtocol() + ":" + event.getEventType(),
            k -> new AtomicLong()).incrementAndGet();
        // 消费侧幂等: 设备 mid 存在时以 Redis SETNX 去重(QoS1 at-least-once 重投递防护)
        if (event.getMid() != null && !event.getMid().isBlank()) {
            String key = "eap:mid:" + event.getDeviceCode() + ":" + event.getMid();
            Boolean first = redisTemplate.opsForValue()
                .setIfAbsent(key, "1", java.time.Duration.ofMinutes(10));
            if (Boolean.FALSE.equals(first)) {
                log.debug("重复消息丢弃: device={} mid={}", event.getDeviceCode(), event.getMid());
                return;
            }
        }
        if (event.getEventTime() == null) {
            event.setEventTime(System.currentTimeMillis());
        }
        if (event.getSeq() == null) {
            event.setSeq(seqMap.computeIfAbsent(event.getDeviceCode(), k -> new AtomicLong()).incrementAndGet());
        }
        String rk = "eap." + (event.getProtocol() == null ? "unknown" : event.getProtocol().toLowerCase())
            + "." + event.getEventType();
        rabbitTemplate.convertAndSend(MqConfig.EXCHANGE, rk, event);
        log.debug("事件发布: rk={}, device={}, seq={}", rk, event.getDeviceCode(), event.getSeq());
    }

    /**
     * 设备×事件类型计数(覆盖全部发布路径: 工厂/监控状态机/SPC)
     */
    public Map<String, Long> deviceStats() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        deviceTypeCounters.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }
}
