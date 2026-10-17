package com.pengbo.eap.monitor;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 端到端延迟统计(设备时间戳 -> 时序落库完成), 保留最近 2 万样本算分位
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Component
public class LatencyStats {

    private static final int CAP = 20000;
    private final List<Long> samples = Collections.synchronizedList(new ArrayList<>(CAP));

    public void record(long millis) {
        synchronized (samples) {
            if (samples.size() >= CAP) {
                samples.remove(0);
            }
            samples.add(millis);
        }
    }

    /**
     * 清空样本(压测前调用, 保证分位数只统计本轮窗口)
     */
    public void reset() {
        synchronized (samples) {
            samples.clear();
        }
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> result = new LinkedHashMap<>();
        List<Long> copy;
        synchronized (samples) {
            copy = new ArrayList<>(samples);
        }
        if (copy.isEmpty()) {
            result.put("count", 0);
            return result;
        }
        Collections.sort(copy);
        result.put("count", copy.size());
        result.put("p50", copy.get((int) (copy.size() * 0.50)));
        result.put("p95", copy.get((int) (copy.size() * 0.95)));
        result.put("p99", copy.get(Math.min(copy.size() - 1, (int) (copy.size() * 0.99))));
        result.put("max", copy.get(copy.size() - 1));
        return result;
    }
}
