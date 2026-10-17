package com.pengbo.eap.core;

import lombok.Data;

import java.util.Map;

/**
 * EAP 统一设备事件模型(与 Liao-MES 集成接口协议一致, 扩展 protocol/seq 字段)
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Data
public class EapEvent {

    public static final String TYPE_PROCESS_COMPLETE = "PROCESS_COMPLETE";
    public static final String TYPE_PARAM_ALARM = "PARAM_ALARM";
    public static final String TYPE_ONLINE = "ONLINE";
    public static final String TYPE_OFFLINE = "OFFLINE";

    /**
     * 设备编码
     */
    private String deviceCode;

    /**
     * 事件类型
     */
    private String eventType;

    /**
     * MES 流转批次号(网关配置映射)
     */
    private String sn;

    /**
     * 本次完成数量
     */
    private Long count;

    /**
     * 不良数
     */
    private Integer defective;

    /**
     * 工艺参数
     */
    private Map<String, Double> params;

    /**
     * 接入协议 MODBUS_TCP / SECS_GEM / MQTT
     */
    private String protocol;

    /**
     * 网关内事件序号(单设备递增)
     */
    private Long seq;

    /**
     * 设备侧消息唯一标识(用于消费幂等, QoS1 at-least-once 去重)
     */
    private String mid;

    /**
     * 事件时间戳(毫秒)
     */
    private Long eventTime;
}
