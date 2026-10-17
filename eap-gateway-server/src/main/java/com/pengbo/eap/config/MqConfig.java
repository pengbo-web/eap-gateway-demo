package com.pengbo.eap.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * EAP 事件总线拓扑: topic 交换机 fan-out 到 MES 消费队列与时序 sink 队列, 带死信
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Configuration
public class MqConfig {

    public static final String EXCHANGE = "eap.events";
    public static final String QUEUE_MES = "mes.integration";
    public static final String QUEUE_TAOS = "eap.tdengine.sink";
    public static final String QUEUE_DLQ = "eap.events.dlq";
    public static final String RK_ALL = "eap.#";

    @Bean
    public TopicExchange eapExchange() {
        return new TopicExchange(EXCHANGE, true, false);
    }

    /**
     * 事件以 JSON 序列化(默认 SimpleMessageConverter 不支持非 Serializable POJO)
     */
    @Bean
    public org.springframework.amqp.support.converter.MessageConverter jsonMessageConverter() {
        return new org.springframework.amqp.support.converter.Jackson2JsonMessageConverter();
    }

    @Bean
    public Queue mesQueue() {
        return QueueBuilder.durable(QUEUE_MES)
            .withArgument("x-dead-letter-exchange", "")
            .withArgument("x-dead-letter-routing-key", QUEUE_DLQ)
            .build();
    }

    @Bean
    public Queue taosQueue() {
        return QueueBuilder.durable(QUEUE_TAOS)
            .withArgument("x-dead-letter-exchange", "")
            .withArgument("x-dead-letter-routing-key", QUEUE_DLQ)
            .build();
    }

    @Bean
    public Queue dlq() {
        return QueueBuilder.durable(QUEUE_DLQ).build();
    }

    @Bean
    public Binding mesBinding() {
        return BindingBuilder.bind(mesQueue()).to(eapExchange()).with(RK_ALL);
    }

    @Bean
    public Binding taosBinding() {
        return BindingBuilder.bind(taosQueue()).to(eapExchange()).with(RK_ALL);
    }
}
