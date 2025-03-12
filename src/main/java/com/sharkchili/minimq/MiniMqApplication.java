package com.sharkchili.minimq;

import cn.hutool.extra.spring.SpringUtil;
import com.sharkchili.minimq.broker.config.BaseConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class MiniMqApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(MiniMqApplication.class, args);

    }

}
