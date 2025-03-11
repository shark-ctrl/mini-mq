package com.sharkchili.minimq;

import cn.hutool.extra.spring.SpringUtil;
import com.sharkchili.minimq.broker.core.MappedFile;
import com.sharkchili.minimq.broker.core.MappedFileCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class MiniMqApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(MiniMqApplication.class, args);

    }

}
