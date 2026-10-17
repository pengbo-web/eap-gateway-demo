package com.pengbo.eap.monitor;

import com.pengbo.eap.config.GatewayProperties;
import com.pengbo.eap.core.EapEvent;
import com.pengbo.eap.core.EapEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备连接监控: 心跳 last-seen + 离线/上线状态机
 * <p>
 * 离线判定: 超过 pollMs × offlineFactor 未收到事件 -> 发 OFFLINE 事件;
 * 恢复收到事件 -> 发 ONLINE 事件。状态迁移才发, 避免事件风暴。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceMonitorService {

    public enum State {RUNNING, OFFLINE}

    private final GatewayProperties properties;
    private final DeviceRegistry registry;
    private final EapEventPublisher publisher;
    private final ReliabilityService reliability;
    private final ConcurrentHashMap<String, Long> lastSeen = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, State> states = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> protocols = new ConcurrentHashMap<>();

    /**
     * 事件驱动心跳(由 EventFactory 调用)
     */
    public void touch(String deviceCode, String protocol) {
        lastSeen.put(deviceCode, System.currentTimeMillis());
        protocols.put(deviceCode, protocol);
        State prev = states.put(deviceCode, State.RUNNING);
        if (prev == State.OFFLINE) {
            reliability.recordRunning(deviceCode, System.currentTimeMillis());
            publishTransition(deviceCode, EapEvent.TYPE_ONLINE);
            log.info("设备上线: {}", deviceCode);
        } else if (prev == null) {
            reliability.recordRunning(deviceCode, System.currentTimeMillis());
            log.info("设备首次接入: {} protocol={}", deviceCode, protocol);
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void scan() {
        long now = System.currentTimeMillis();
        lastSeen.forEach((code, ts) -> {
            long offlineMs = registry.get(code).pollMs * properties.getOfflineFactor();
            if (now - ts > offlineMs && states.get(code) == State.RUNNING) {
                states.put(code, State.OFFLINE);
                reliability.recordFailure(code, now);
                publishTransition(code, EapEvent.TYPE_OFFLINE);
                log.warn("设备离线判定: {} 静默 {}ms > 阈值 {}ms", code, now - ts, offlineMs);
            }
        });
    }

    private void publishTransition(String code, String type) {
        EapEvent event = new EapEvent();
        event.setDeviceCode(code);
        event.setEventType(type);
        event.setProtocol(protocols.getOrDefault(code, "UNKNOWN"));
        event.setSn(registry.get(code).sn);
        event.setCount(0L);
        event.setDefective(0);
        event.setParams(Map.of());
        publisher.publish(event);
    }

    public Map<String, Object> devices() {
        Map<String, Object> result = new LinkedHashMap<>();
        lastSeen.forEach((code, ts) -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("state", states.getOrDefault(code, State.RUNNING).name());
            item.put("protocol", protocols.get(code));
            item.put("lastSeen", ts);
            item.put("silentMs", System.currentTimeMillis() - ts);
            item.put("thresholds", registry.get(code).thresholds);
            item.put("sn", registry.get(code).sn);
            result.put(code, item);
        });
        return result;
    }
}
