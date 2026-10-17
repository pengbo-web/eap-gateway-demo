package com.pengbo.eap.core;

import com.pengbo.eap.config.GatewayProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统一事件工厂: 各协议适配层将工艺参数交由本工厂生成标准事件
 * <p>
 * 职责: 阈值判定(PROCESS_COMPLETE / PARAM_ALARM)、设备->MES 批次映射、协议标记、分协议计数(验收用)。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Component
@RequiredArgsConstructor
public class EventFactory {

    private final GatewayProperties properties;
    private final EapEventPublisher publisher;
    private final com.pengbo.eap.monitor.SpcMonitor spcMonitor;
    private final com.pengbo.eap.monitor.DeviceRegistry registry;
    private final com.pengbo.eap.monitor.DeviceMonitorService monitor;
    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> deviceTypeCounters = new ConcurrentHashMap<>();

    public EapEvent build(String deviceCode, String protocol, Map<String, Double> params) {
        com.pengbo.eap.monitor.DeviceRegistry.DeviceConfig cfg = registry.get(deviceCode);
        EapEvent event = new EapEvent();
        event.setDeviceCode(deviceCode);
        event.setProtocol(protocol);
        event.setParams(params);
        event.setSn(cfg.sn != null ? cfg.sn : properties.getDeviceSnMap().get(deviceCode));
        event.setCount(1L);
        event.setDefective(0);
        if (exceedsThreshold(cfg.thresholds, params) != null) {
            event.setEventType(EapEvent.TYPE_PARAM_ALARM);
            event.setDefective(1);
        } else {
            event.setEventType(EapEvent.TYPE_PROCESS_COMPLETE);
            observeSpc(deviceCode, params);
        }
        monitor.touch(deviceCode, protocol);
        counters.computeIfAbsent(protocol, k -> new AtomicLong()).incrementAndGet();
        deviceTypeCounters.computeIfAbsent(deviceCode + ":" + event.getEventType(),
            k -> new AtomicLong()).incrementAndGet();
        return event;
    }

    /**
     * SPC 观察: 判异命中时追加一条 SPC 报警事件(复用 PARAM_ALARM 通道)
     */
    private void observeSpc(String deviceCode, Map<String, Double> params) {
        for (String param : properties.getSpc().getParams()) {
            Double value = params.get(param);
            if (value == null) {
                continue;
            }
            String rule = spcMonitor.observe(deviceCode, param, value);
            if (rule != null) {
                EapEvent alarm = new EapEvent();
                alarm.setDeviceCode(deviceCode);
                alarm.setEventType(EapEvent.TYPE_PARAM_ALARM);
                alarm.setSn(properties.getDeviceSnMap().get(deviceCode));
                alarm.setCount(0L);
                alarm.setDefective(1);
                alarm.setParams(Map.of(param, value, rule, 1.0));
                alarm.setProtocol("SPC");
                publisher.publish(alarm);
            }
        }
    }

    public Map<String, Long> stats() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        counters.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }

    public Map<String, Long> deviceStats() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        deviceTypeCounters.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }

    private String exceedsThreshold(Map<String, Double> thresholds, Map<String, Double> params) {
        for (Map.Entry<String, Double> th : thresholds.entrySet()) {
            Double v = params.get(th.getKey());
            if (v != null && v > th.getValue()) {
                return th.getKey();
            }
        }
        return null;
    }
}
