package com.sharkchili.minimq.broker.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "base")
@Data
public class BaseConfig {
    private String mqHome;




}