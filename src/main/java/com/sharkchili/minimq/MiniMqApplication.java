package com.sharkchili.minimq;

import cn.hutool.extra.spring.SpringUtil;
import com.sharkchili.minimq.broker.MappedFile;
import com.sharkchili.minimq.broker.core.MappedFileCache;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
@Slf4j
public class MiniMqApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(MiniMqApplication.class, args);


        MappedFile mappedFile = new MappedFile("F:\\tmp\\0000000", 0, 10 * 1024);
        MappedFileCache mappedFileCache = SpringUtil.getBean(MappedFileCache.class);
        mappedFileCache.put("test-topic", mappedFile);

        MappedFile mappedFile1 = mappedFileCache.get("test-topic");
        mappedFile1.write("this is test msg".getBytes(),true);

        byte[] read = mappedFile1.read(0, 16);
        log.info("输出结果：{}", new String(read));

    }

}
