package com.pengbo.eap.monitor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pengbo.eap.config.GatewayProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备配置注册中心: Redis hash 为源, 本地缓存 5s 刷新, 改配置接口即时生效
 * <p>
 * key: eap:dev:cfg:{code} -> hash{thresholds(json), pollMs, sn}
 * 首次出现的设备以 yml 默认值落 Redis(配置可运维化, 减少逐事件查库/查配置开销)。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceRegistry {

    private static final String KEY_PREFIX = "eap:dev:cfg:";

    private final StringRedisTemplate redis;
    private final GatewayProperties properties;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, DeviceConfig> cache = new ConcurrentHashMap<>();

    public static class DeviceConfig {
        public Map<String, Double> thresholds = new HashMap<>();
        public long pollMs = 1000;
        public String sn;
    }

    @PostConstruct
    public void seed() {
        // yml 中的默认阈值/映射作为种子写入 Redis(已存在则不覆盖, 保留运维修改)
        properties.getDeviceSnMap().forEach((code, sn) -> {
            String key = KEY_PREFIX + code;
            if (Boolean.FALSE.equals(redis.hasKey(key))) {
                DeviceConfig cfg = new DeviceConfig();
                cfg.thresholds = new HashMap<>(properties.getThresholds());
                cfg.sn = sn;
                write(code, cfg);
            }
        });
        refresh();
    }

    public DeviceConfig get(String code) {
        DeviceConfig cfg = cache.get(code);
        if (cfg == null) {
            cfg = read(code);
            if (cfg == null) {
                cfg = new DeviceConfig();
                cfg.thresholds = new HashMap<>(properties.getThresholds());
                cfg.sn = properties.getDeviceSnMap().get(code);
                write(code, cfg);
            }
            cache.put(code, cfg);
        }
        return cfg;
    }

    public void update(String code, Map<String, Double> thresholds) {
        DeviceConfig cfg = get(code);
        cfg.thresholds = new HashMap<>(thresholds);
        write(code, cfg);
        cache.put(code, cfg);
        log.info("设备配置已更新并即时生效: {} thresholds={}", code, thresholds);
    }

    @Scheduled(fixedDelay = 5000)
    public void refresh() {
        for (String code : cache.keySet()) {
            DeviceConfig cfg = read(code);
            if (cfg != null) {
                cache.put(code, cfg);
            }
        }
    }

    public Map<String, DeviceConfig> all() {
        return new HashMap<>(cache);
    }

    private void write(String code, DeviceConfig cfg) {
        Map<String, String> fields = new HashMap<>();
        try {
            fields.put("thresholds", objectMapper.writeValueAsString(cfg.thresholds));
            fields.put("pollMs", String.valueOf(cfg.pollMs));
            fields.put("sn", cfg.sn == null ? "" : cfg.sn);
        } catch (Exception e) {
            log.warn("配置序列化失败: {}", e.getMessage());
            return;
        }
        redis.opsForHash().putAll(KEY_PREFIX + code, fields);
    }

    private DeviceConfig read(String code) {
        Map<Object, Object> entries = redis.opsForHash().entries(KEY_PREFIX + code);
        if (entries.isEmpty()) {
            return null;
        }
        DeviceConfig cfg = new DeviceConfig();
        try {
            Object th = entries.get("thresholds");
            if (th != null) {
                cfg.thresholds = objectMapper.readValue(th.toString(),
                    new TypeReference<Map<String, Double>>() { });
            }
            Object poll = entries.get("pollMs");
            if (poll != null && !poll.toString().isBlank()) {
                cfg.pollMs = Long.parseLong(poll.toString());
            }
            Object sn = entries.get("sn");
            cfg.sn = sn == null || sn.toString().isBlank() ? null : sn.toString();
        } catch (Exception e) {
            log.warn("配置反序列化失败, 回退默认: {}", e.getMessage());
        }
        return cfg;
    }
}
