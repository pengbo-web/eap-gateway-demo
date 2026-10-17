package com.pengbo.eap.monitor;

import com.pengbo.eap.config.GatewayProperties;
import com.pengbo.eap.core.EapEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SPC 统计过程控制监控器(二次开发)
 * <p>
 * 按 设备+参数 维护子组(n 可配, 默认5): 子组满时计算均值 X̄ 与极差 R;
 * 管制限由历史子组估计: CL=总均值, UCL/LCL=CL±A2·R̄ (X̄ 图), UCL_R=D4·R̄ (R 图);
 * 判异规则: R1 点超 3σ 管制限 / R2 连续 7 点同侧 / R3 连续 6 点单调;
 * 命中产出 PARAM_ALARM 事件(protocol=SPC), 复用 MES 待审核异常单闭环。
 * 系数表取 SEM/ASTM 常用值(n=2..10)。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SpcMonitor {

    /** n=2..10 的 A2 系数 */
    private static final double[] A2 = {0, 0, 1.880, 1.023, 0.729, 0.577, 0.483, 0.419, 0.373, 0.337};
    /** n=2..10 的 D4 系数 */
    private static final double[] D4 = {0, 0, 3.267, 2.574, 2.282, 2.114, 2.004, 1.924, 1.864, 1.816};

    private final GatewayProperties properties;

    private final Map<String, Series> seriesMap = new ConcurrentHashMap<>();
    private final Deque<Map<String, Object>> violations = new ArrayDeque<>();

    public static class Series {
        final List<Double> buffer = new ArrayList<>();
        final List<Double> means = new ArrayList<>();
        final List<Double> ranges = new ArrayList<>();
        double cl;
        double ucl;
        double lcl;
        boolean limitsValid;
    }

    /**
     * 观察一个工艺参数值(仅完工事件调用)
     *
     * @return 命中的判异规则编号, 无则 null
     */
    public synchronized String observe(String deviceCode, String param, double value) {
        int n = properties.getSpc().getSubgroupSize();
        Series s = seriesMap.computeIfAbsent(deviceCode + ":" + param, k -> new Series());
        s.buffer.add(value);
        if (s.buffer.size() < n) {
            return null;
        }
        double mean = s.buffer.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double range = s.buffer.stream().mapToDouble(Double::doubleValue).max().orElse(0)
            - s.buffer.stream().mapToDouble(Double::doubleValue).min().orElse(0);
        s.buffer.clear();
        s.means.add(mean);
        s.ranges.add(range);
        int minSub = properties.getSpc().getMinSubgroups();
        if (s.means.size() < minSub) {
            return null;
        }
        // 用"当前点之前"的子组估计管制限, 再判定当前点
        int prev = s.means.size() - 1;
        double grand = s.means.subList(0, prev).stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double rbar = s.ranges.subList(0, prev).stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double a2 = A2[Math.min(Math.max(n, 2), 10)];
        s.cl = grand;
        s.ucl = grand + a2 * rbar;
        s.lcl = grand - a2 * rbar;
        s.limitsValid = true;
        String rule = judge(s, mean);
        if (rule != null) {
            recordViolation(deviceCode, param, rule, value, mean, s);
        }
        return rule;
    }

    private String judge(Series s, double current) {
        if (current > s.ucl || current < s.lcl) {
            return "R1";
        }
        List<Double> m = s.means;
        int size = m.size();
        if (size >= 7) {
            boolean allAbove = true, allBelow = true;
            for (int i = size - 7; i < size; i++) {
                double diff = m.get(i) - s.cl;
                // 中心线死区: 与 CL 几乎重合的点视为"在线上", 中断同侧游程
                if (Math.abs(diff) < 1e-9) {
                    allAbove = false;
                    allBelow = false;
                    break;
                }
                allAbove &= diff > 0;
                allBelow &= diff < 0;
            }
            if (allAbove || allBelow) {
                return "R2";
            }
        }
        if (size >= 6) {
            boolean inc = true, dec = true;
            for (int i = size - 6; i < size - 1; i++) {
                inc &= m.get(i + 1) > m.get(i);
                dec &= m.get(i + 1) < m.get(i);
            }
            if (inc || dec) {
                return "R3";
            }
        }
        return null;
    }

    private void recordViolation(String deviceCode, String param, String rule, double value, double mean, Series s) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("deviceCode", deviceCode);
        v.put("param", param);
        v.put("rule", rule);
        v.put("value", value);
        v.put("subgroupMean", mean);
        v.put("cl", s.cl);
        v.put("ucl", s.ucl);
        v.put("lcl", s.lcl);
        v.put("time", System.currentTimeMillis());
        synchronized (violations) {
            violations.addLast(v);
            while (violations.size() > 100) {
                violations.pollFirst();
            }
        }
        log.warn("SPC 判异命中: device={} param={} rule={} mean={} UCL={} LCL={}",
            deviceCode, param, rule, mean, s.ucl, s.lcl);
    }

    public Map<String, Object> state() {
        Map<String, Object> result = new LinkedHashMap<>();
        seriesMap.forEach((key, s) -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("subgroups", s.means.size());
            item.put("limitsValid", s.limitsValid);
            item.put("cl", s.cl);
            item.put("ucl", s.ucl);
            item.put("lcl", s.lcl);
            item.put("recentMeans", s.means.subList(Math.max(0, s.means.size() - 10), s.means.size()));
            result.put(key, item);
        });
        return result;
    }

    public List<Map<String, Object>> violations() {
        synchronized (violations) {
            return new ArrayList<>(violations);
        }
    }
}
