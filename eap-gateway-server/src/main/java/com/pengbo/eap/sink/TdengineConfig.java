package com.pengbo.eap.sink;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * TDengine 数据源(REST 驱动, 免原生客户端)
 *
 * @author pengbo
 * @date 2026-10-17
 */
@Slf4j
@Configuration
public class TdengineConfig {

    @Value("${eap.tdengine.url}")
    private String url;

    @Value("${eap.tdengine.username}")
    private String username;

    @Value("${eap.tdengine.password}")
    private String password;

    @Bean(name = "taosJdbcTemplate")
    public JdbcTemplate taosJdbcTemplate() {
        // HikariCP 连接池: TD REST 驱动每次新建连接含 TCP+HTTP 握手, 池化消除尾延迟
        com.zaxxer.hikari.HikariConfig hc = new com.zaxxer.hikari.HikariConfig();
        hc.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        hc.setJdbcUrl(url);
        hc.setUsername(username);
        hc.setPassword(password);
        hc.setMaximumPoolSize(8);
        hc.setMinimumIdle(2);
        hc.setConnectionTimeout(3000);
        return new JdbcTemplate(new com.zaxxer.hikari.HikariDataSource(hc));
    }
}
