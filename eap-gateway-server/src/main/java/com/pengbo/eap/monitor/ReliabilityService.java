package com.pengbo.eap.monitor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 可靠性指标 MTTR/MTBF(二次开发)
 * <p>
 * 以设备状态迁移为故障/修复边界: OFFLINE 迁移=故障发生, ONLINE 迁移=修复完成;
 * MTBF = 累计运行时长 / 故障次数; MTTR = 累计停机时长 / 故障次数;
 * 可用度 = 运行 / (运行+停机)。统计口径为"检测时刻", 含离线判定窗口(默认3倍轮询间隔)。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Service
public class ReliabilityService {

    private static class Acc {
        long upMs;
        long downMs;
        int failures;
        Long runningSince;
        Long downSince;
    }

    private final ConcurrentHashMap<String, Acc> accs = new ConcurrentHashMap<>();

    public synchronized void recordRunning(String deviceCode, long ts) {
        Acc a = accs.computeIfAbsent(deviceCode, k -> new Acc());
        if (a.downSince != null) {
            a.downMs += ts - a.downSince;
            a.downSince = null;
        }
        a.runningSince = ts;
    }

    public synchronized void recordFailure(String deviceCode, long ts) {
        Acc a = accs.computeIfAbsent(deviceCode, k -> new Acc());
        a.failures++;
        if (a.runningSince != null) {
            a.upMs += ts - a.runningSince;
            a.runningSince = null;
        }
        a.downSince = ts;
    }

    public Map<String, Object> report(String deviceCode) {
        Acc a = accs.get(deviceCode);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deviceCode", deviceCode);
        if (a == null) {
            result.put("failures", 0);
            return result;
        }
        long up = a.upMs + (a.runningSince == null ? 0 : System.currentTimeMillis() - a.runningSince);
        long down = a.downMs + (a.downSince == null ? 0 : System.currentTimeMillis() - a.downSince);
        int failures = a.failures;
        result.put("failures", failures);
        result.put("upMs", up);
        result.put("downMs", down);
        result.put("upMinutes", round(up / 60000.0));
        result.put("downMinutes", round(down / 60000.0));
        result.put("mtbfMinutes", failures == 0 ? null : round(up / 60000.0 / failures));
        result.put("mttrMinutes", failures == 0 ? null : round(down / 60000.0 / failures));
        result.put("availability", up + down == 0 ? null : round(up * 100.0 / (up + down)));
        return result;
    }

    public Map<String, Object> reportAll() {
        Map<String, Object> result = new LinkedHashMap<>();
        accs.keySet().forEach(code -> result.put(code, report(code)));
        return result;
    }

    private double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
