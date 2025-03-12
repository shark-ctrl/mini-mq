package com.sharkchili.minimq.minimq;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.thread.ThreadUtil;
import com.sharkchili.minimq.broker.core.CommitLogHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.ByteBuffer;
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
        //清空测试文件
        FileUtil.writeBytes("".getBytes(), "F:\\tmp\\broker\\store\\test-topic\\00000000");
        commitLogHandler.loadCommitLogFile("test-topic", "F:\\tmp\\broker\\store\\test-topic\\00000000");
        for (int i = 0; i < 6; i++) {
            commitLogHandler.appendMsgCommitLog("test-topic", "0123456789");
        }


        byte[] bytes = commitLogHandler.readCommitLog("test-topic");
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int length = byteBuffer.getInt();

        log.info("the commit log message length :{}", length);
        log.info("the commit log message :{}", new String(byteBuffer.get(bytes, 4, length).array()));


        commitLogHandler.cleanCommitLog("test-topic");

    }

}
