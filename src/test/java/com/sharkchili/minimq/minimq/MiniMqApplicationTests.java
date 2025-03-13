package com.sharkchili.minimq.minimq;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.thread.ThreadUtil;
import com.sharkchili.minimq.broker.core.CommitLogHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@Slf4j
class MiniMqApplicationTests {

    @Autowired
    private CommitLogHandler commitLogHandler;


    @Test
    void contextLoads() {
    }


    @Test
    public void testMmapAppend() throws Exception {
        ExecutorService threadPool = Executors.newFixedThreadPool(100);
        CountDownLatch countDownLatch = new CountDownLatch(100);
        //清空测试文件
        FileUtil.writeBytes("".getBytes(), "F:\\tmp\\broker\\store\\test-topic\\00000000");
        commitLogHandler.loadCommitLogFile("test-topic", "F:\\tmp\\broker\\store\\test-topic\\00000000");

        for (int i = 0; i < 100; i++) {
            threadPool.execute(()-> {
                try {
                    commitLogHandler.appendMsgCommitLog("test-topic", "0123456789012345");
                    countDownLatch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

        }

        countDownLatch.await();
        byte[] bytes = commitLogHandler.readCommitLog("test-topic");
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int length = byteBuffer.getInt();


        log.info("the commit log message length :{}", length);
        log.info("the commit log message :{}", new String(byteBuffer.get(bytes, 4, length).array()));

        //释放mmap内存映射
        commitLogHandler.cleanCommitLog("test-topic");

        //commitLog进度刷盘
        ThreadUtil.sleep(1,TimeUnit.MINUTES);

    }

}
