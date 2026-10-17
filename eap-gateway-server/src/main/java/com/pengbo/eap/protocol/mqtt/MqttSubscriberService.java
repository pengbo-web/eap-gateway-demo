package com.pengbo.eap.protocol.mqtt;

import com.pengbo.eap.config.GatewayProperties;
import com.pengbo.eap.core.EapEvent;
import com.pengbo.eap.core.EapEventPublisher;
import com.pengbo.eap.core.EventFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * MQTT 设备接入: 订阅 telemetry 主题, JSON 载荷转统一事件
 * <p>
 * 主题约定 eap/dev/{deviceCode}/telemetry; 断线由 paho 自动重连(maxInFlight/keepAlive 调优)。
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MqttSubscriberService {

    private final GatewayProperties properties;
    private final EventFactory eventFactory;
    private final EapEventPublisher publisher;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private MqttClient client;

    @PostConstruct
    public void start() {
        GatewayProperties.Mqtt cfg = properties.getMqtt();
        try {
            client = new MqttClient(cfg.getBroker(), "eap-gateway-" + System.currentTimeMillis(), new MemoryPersistence());
            MqttConnectOptions opts = new MqttConnectOptions();
            opts.setAutomaticReconnect(true);
            opts.setConnectionTimeout(5);
            opts.setKeepAliveInterval(20);
            org.eclipse.paho.client.mqttv3.IMqttMessageListener listener = (topic, message) -> {
                String deviceCode = extractDeviceCode(topic);
                try {
                    JsonNode node = objectMapper.readTree(message.getPayload());
                    Map<String, Double> params = new HashMap<>();
                    node.fields().forEachRemaining(e -> {
                        if (e.getValue().isNumber()) {
                            params.put(e.getKey(), e.getValue().asDouble());
                        }
                    });
                    EapEvent event = eventFactory.build(deviceCode, "MQTT", params);
                    JsonNode midNode = node.get("mid");
                    if (midNode != null && !midNode.isNull()) {
                        event.setMid(midNode.asText());
                    }
                    publisher.publish(event);
                } catch (Exception e) {
                    log.warn("MQTT 载荷解析失败: topic={} err={}", topic, e.getMessage());
                }
            };
            // paho v3 的 automaticReconnect 只恢复连接不恢复订阅, 必须在 connectComplete 里补订阅
            client.setCallback(new org.eclipse.paho.client.mqttv3.MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    try {
                        client.subscribe(cfg.getTopic(), 1, listener);
                        log.info("MQTT 订阅{}成功: broker={} topic={}", reconnect ? "恢复" : "", cfg.getBroker(), cfg.getTopic());
                    } catch (Exception e) {
                        log.warn("MQTT 订阅失败: {}", e.getMessage());
                    }
                }

                @Override
                public void connectionLost(Throwable cause) {
                    log.warn("MQTT 连接断开, 等待自动重连: {}", cause == null ? "?" : cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) {
                    // 消息经订阅级 listener 处理, 此处不使用
                }

                @Override
                public void deliveryComplete(org.eclipse.paho.client.mqttv3.IMqttDeliveryToken token) {
                    // 发布侧无需处理
                }
            });
            client.connect(opts);
        } catch (Exception e) {
            log.warn("MQTT 连接失败(容器未起时允许降级): {}", e.getMessage());
        }
    }

    private String extractDeviceCode(String topic) {
        String[] parts = topic.split("/");
        return parts.length >= 3 ? parts[2] : topic;
    }

    @PreDestroy
    public void stop() {
        try {
            if (client != null && client.isConnected()) {
                client.disconnect();
            }
        } catch (Exception ignored) {
            // 关闭期忽略
        }
    }
}
