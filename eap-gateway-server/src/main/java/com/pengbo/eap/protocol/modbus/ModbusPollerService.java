package com.pengbo.eap.protocol.modbus;

import com.pengbo.eap.config.GatewayProperties;
import com.pengbo.eap.core.EapEvent;
import com.pengbo.eap.core.EapEventPublisher;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Modbus 轮询采集服务: 周期读寄存器 -> 工艺参数 -> 阈值判定 -> 发布统一事件
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ModbusPollerService {

    private final GatewayProperties properties;
    private final EapEventPublisher publisher;
    private final com.pengbo.eap.core.EventFactory eventFactory;
    private final Map<String, ModbusMasterClient> clients = new HashMap<>();
    private ScheduledExecutorService scheduler;

    @PostConstruct
    public void start() {
        scheduler = Executors.newScheduledThreadPool(4);
        int idx = 0;
        for (String target : properties.getModbusTargets()) {
            String[] p = target.split(":");
            String host = p[0];
            int port = Integer.parseInt(p[1]);
            int unit = p.length > 2 ? Integer.parseInt(p[2]) : 1;
            long pollMs = p.length > 3 ? Long.parseLong(p[3]) : 1000;
            String deviceCode = String.format("DEV-MB-%02d", ++idx);
            ModbusMasterClient client = new ModbusMasterClient(host, port, unit);
            clients.put(deviceCode, client);
            client.connect();
            scheduler.scheduleWithFixedDelay(() -> pollOnce(deviceCode, client), pollMs, pollMs, TimeUnit.MILLISECONDS);
            log.info("Modbus 采集任务注册: {} -> {}:{} unit={} poll={}ms", deviceCode, host, port, unit, pollMs);
        }
    }

    private void pollOnce(String deviceCode, ModbusMasterClient client) {
        try {
            int[] regs = client.read(ModbusFrame.FC_READ_HOLDING, 0, 4).get(2, TimeUnit.SECONDS);
            Map<String, Double> params = new HashMap<>();
            params.put("v", regs[0] / 100.0);
            params.put("i", regs[1] / 100.0);
            params.put("power1", regs[2] / 100.0);
            publisher.publish(eventFactory.build(deviceCode, "MODBUS_TCP", params));
        } catch (Exception e) {
            log.debug("Modbus 轮询失败(设备可能离线): {} {}", deviceCode, e.getMessage());
        }
    }

    private String exceedsThreshold(Map<String, Double> params) {
        for (Map.Entry<String, Double> th : properties.getThresholds().entrySet()) {
            Double v = params.get(th.getKey());
            if (v != null && v > th.getValue()) {
                return th.getKey();
            }
        }
        return null;
    }

    public ModbusMasterClient clientOf(String deviceCode) {
        return clients.get(deviceCode);
    }

    @PreDestroy
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        clients.values().forEach(ModbusMasterClient::close);
    }
}
