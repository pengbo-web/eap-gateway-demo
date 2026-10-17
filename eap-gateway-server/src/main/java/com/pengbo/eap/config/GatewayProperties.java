package com.pengbo.eap.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 网关配置绑定
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Data
@Component
@ConfigurationProperties(prefix = "eap.gateway")
public class GatewayProperties {

    /**
     * Modbus 目标: host:port:unitId:pollMs
     */
    private List<String> modbusTargets = new ArrayList<>();

    /**
     * SECS/GEM 目标: host:port
     */
    private List<String> secsTargets = new ArrayList<>();

    private Mqtt mqtt = new Mqtt();

    /**
     * 离线判定倍数(超过 pollInterval × factor 未上报)
     */
    private int offlineFactor = 3;

    /**
     * 设备编码 -> MES 流转批次号
     */
    private Map<String, String> deviceSnMap = new HashMap<>();

    /**
     * 阈值告警: 参数名 -> 上限
     */
    private Map<String, Double> thresholds = new HashMap<>();

    private Spc spc = new Spc();

    @Data
    public static class Spc {
        /**
         * 子组大小
         */
        private int subgroupSize = 5;
        /**
         * 管制限生效所需最小子组数
         */
        private int minSubgroups = 5;
        /**
         * 监控参数名单
         */
        private List<String> params = new ArrayList<>(List.of("v"));
    }

    @Data
    public static class Mqtt {
        private String broker = "tcp://localhost:1883";
        private String topic = "eap/dev/+/telemetry";
    }
}
