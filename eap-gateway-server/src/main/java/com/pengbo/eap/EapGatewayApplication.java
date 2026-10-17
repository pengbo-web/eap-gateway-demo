package com.pengbo.eap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * EAP 设备数据采集网关启动类
 *
 * @author pengbo
 * @date 2026-10-17
 */
@EnableScheduling
@SpringBootApplication(exclude = {org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class})
public class EapGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(EapGatewayApplication.class, args);
    }
}
