package com.pengbo.eap.monitor;

import com.pengbo.eap.protocol.modbus.ModbusMasterClient;
import com.pengbo.eap.protocol.modbus.ModbusPollerService;
import com.pengbo.eap.protocol.modbus.ModbusSlaveSimulator;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 模拟器与连接状态控制接口(验收/运维用)
 *
 * @author pengbo
 * @date 2026-10-17
 */
@RestController
@RequiredArgsConstructor
public class SimController {

    private final ModbusSlaveSimulator simulator;
    private final ModbusPollerService pollerService;
    private final com.pengbo.eap.core.EventFactory eventFactory;
    private final SpcMonitor spcMonitor;
    private final LatencyStats latencyStats;
    private final DeviceMonitorService monitor;
    private final DeviceRegistry registry;
    private final com.pengbo.eap.core.EapEventPublisher publisher;
    private final ReliabilityService reliabilityService;
    private final com.pengbo.eap.protocol.secs.SecsHostService secsHostService;

    @PostMapping("/sim/modbus/start")
    public Map<String, Object> start(@RequestParam(defaultValue = "1502") int port) {
        boolean ok = simulator.start(port);
        return Map.of("started", ok, "port", port);
    }

    @PostMapping("/sim/modbus/stop")
    public Map<String, Object> stop() {
        return Map.of("stopped", simulator.stop());
    }

    @GetMapping("/sim/modbus/status")
    public Map<String, Object> status() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("simulatorRunning", simulator.isRunning());
        Map<String, Object> conns = new LinkedHashMap<>();
        result.put("connections", conns);
        return result;
    }

    @GetMapping("/gateway/connections")
    public Map<String, Object> connections() {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String code : new String[]{"DEV-MB-01", "DEV-MB-02", "DEV-MB-03", "DEV-MB-04"}) {
            ModbusMasterClient client = pollerService.clientOf(code);
            if (client != null) {
                result.put(code, Map.of("connected", client.isConnected(), "lastRecover", client.getLastRecoverMillis()));
            }
        }
        return result;
    }

    /**
     * 分协议事件计数(验收用)
     */
    @GetMapping("/gateway/stats")
    public Map<String, Long> stats() {
        return eventFactory.stats();
    }

    /**
     * SPC 管制图状态
     */
    @GetMapping("/gateway/spc/state")
    public Map<String, Object> spcState() {
        return spcMonitor.state();
    }

    /**
     * SPC 判异记录
     */
    @GetMapping("/gateway/spc/violations")
    public java.util.List<Map<String, Object>> spcViolations() {
        return spcMonitor.violations();
    }

    /**
     * 端到端延迟分位(设备时间戳 -> 时序落库)
     */
    @GetMapping("/gateway/metrics/latency")
    public Map<String, Object> latency() {
        return latencyStats.snapshot();
    }

    /**
     * 延迟分位重置(压测前调用, 保证本轮测量口径)
     */
    @PostMapping("/gateway/metrics/latency/reset")
    public Map<String, Object> resetLatency() {
        latencyStats.reset();
        return Map.of("reset", true);
    }

    /**
     * 设备清单: 状态/协议/静默时长/阈值/批次映射
     */
    @GetMapping("/gateway/devices")
    public Map<String, Object> devices() {
        return monitor.devices();
    }

    /**
     * 修改设备阈值配置(Redis 源 + 本地即时生效)
     */
    @PostMapping("/gateway/devices/{code}/config")
    public Map<String, Object> updateConfig(@PathVariable String code,
                                            @RequestBody Map<String, Double> thresholds) {
        registry.update(code, thresholds);
        return Map.of("code", code, "thresholds", thresholds, "effective", "immediate");
    }

    /**
     * 设备×事件类型计数(验收用)
     */
    @GetMapping("/gateway/stats/devices")
    public Map<String, Long> deviceStats() {
        return publisher.deviceStats();
    }

    /**
     * 可靠性指标 MTTR/MTBF/可用度
     */
    @GetMapping("/gateway/reliability")
    public Map<String, Object> reliability(@RequestParam(required = false) String device) {
        return device == null ? reliabilityService.reportAll() : reliabilityService.report(device);
    }

    /**
     * SECS/GEM 远程指令下发(S2F41)
     */
    @PostMapping("/gateway/secs/command")
    public Map<String, Object> secsCommand(@RequestParam String device, @RequestParam String cmd) throws Exception {
        return Map.of("result", secsHostService.sendCommand(device, cmd));
    }

    /**
     * SECS/GEM 通道状态
     */
    @GetMapping("/gateway/secs/status")
    public Map<String, Object> secsStatus() {
        return secsHostService.status();
    }
}
