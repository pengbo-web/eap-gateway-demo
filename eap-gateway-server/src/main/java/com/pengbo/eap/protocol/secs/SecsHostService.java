package com.pengbo.eap.protocol.secs;

import com.pengbo.eap.config.GatewayProperties;
import com.pengbo.eap.core.EapEventPublisher;
import com.pengbo.eap.core.EventFactory;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SECS/GEM 主机服务: 管理多台设备连接, S6F11 事件转统一事件模型
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SecsHostService {

    private static final String[] VALUE_NAMES = {"v", "i", "power1", "power2"};

    private final GatewayProperties properties;
    private final EventFactory eventFactory;
    private final EapEventPublisher publisher;
    private final Map<String, SecsHostClient> clients = new LinkedHashMap<>();

    @PostConstruct
    public void start() {
        int idx = 0;
        for (String target : properties.getSecsTargets()) {
            String[] p = target.split(":");
            String deviceCode = String.format("DEV-SECS-%02d", ++idx);
            SecsHostClient client = new SecsHostClient(p[0], Integer.parseInt(p[1]),
                values -> onEventReport(deviceCode, values));
            clients.put(deviceCode, client);
            client.connect();
            log.info("SECS/GEM 主机通道注册: {} -> {}:{}", deviceCode, p[0], p[1]);
        }
    }

    private void onEventReport(String deviceCode, List<Double> values) {
        Map<String, Double> params = new HashMap<>();
        for (int i = 0; i < values.size() && i < VALUE_NAMES.length; i++) {
            params.put(VALUE_NAMES[i], values.get(i));
        }
        publisher.publish(eventFactory.build(deviceCode, "SECS_GEM", params));
    }

    public String sendCommand(String deviceCode, String command) throws Exception {
        SecsHostClient client = clients.get(deviceCode);
        if (client == null) {
            throw new IllegalArgumentException("未知设备: " + deviceCode);
        }
        client.sendRemoteCommand(command).get();
        return "S2F42 已确认";
    }

    public Map<String, Object> status() {
        Map<String, Object> result = new LinkedHashMap<>();
        clients.forEach((code, client) -> result.put(code, Map.of(
            "state", client.getState().name(),
            "lastRecover", client.getLastRecoverMillis())));
        return result;
    }

    @PreDestroy
    public void stop() {
        clients.values().forEach(SecsHostClient::close);
    }
}
