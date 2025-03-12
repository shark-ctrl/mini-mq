package com.sharkchili.minimq.broker.config;

import cn.hutool.core.thread.ThreadFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Configuration
public class ThreadPoolConfig {
    @Bean
    public ThreadPoolExecutor flushTopicListScheduler() {
        return new ThreadPoolExecutor(
                1,
                1,
                60,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1024),
                ThreadFactoryBuilder.create().setNamePrefix("flush-cron-").build(),
                new ThreadPoolExecutor.DiscardOldestPolicy());
    }
}
