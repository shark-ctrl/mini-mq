package com.sharkchili.minimq.broker.core;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.sharkchili.minimq.broker.cache.TopicJSONCache;
import com.sharkchili.minimq.broker.config.BaseConfig;
import com.sharkchili.minimq.broker.entity.*;
import com.sun.org.apache.bcel.internal.generic.RETURN;
import lombok.Data;
import lombok.SneakyThrows;
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.sharkchili.minimq.broker.constants.BrokerConstants.COMMIT_LOG_DEFAULT_MMAP_SIZE;


@Data
public class ConsumeQueueMappedFile {


    private File file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    private String topicName;
    private int queueId;
    private ByteBuffer byteBuffer;


    public ConsumeQueueMappedFile() {

    }


    public ConsumeQueueMappedFile(File file, String topicName, int queueId) {
        this.file = file;
        this.topicName = topicName;
        this.queueId = queueId;
    }


    @SneakyThrows
    public void loadFileWithMmap(String topicName, int offset, int size) {
        //获取可写入的文件路径
        String latestCommitLogPath = getLatestConsumeQueuePath(topicName);
        //不存在则抛异常
        if (!FileUtil.exist(latestCommitLogPath)) {
            throw new FileNotFoundException(latestCommitLogPath);
        }
        //基于commitLog建立文件映射
        doLoadFileWithMmap(latestCommitLogPath, offset, size);
    }

    private String getLatestConsumeQueuePath(String topicName) throws Exception {
        if (StrUtil.isEmpty(topicName)) {
            throw new IllegalArgumentException("topic name is empty");
        }

        Topic topic = SpringUtil.getBean(TopicJSONCache.class).getTopic(topicName);
        Queue queue = topic.getQueueList().get(queueId);

        if (queue.getMaxOffset() >= queue.getLimit()) {
            createConsumeQueueFile(queue.getFileName());
        }


        return SpringUtil.getBean(BaseConfig.class).getConsumeQueuePath() +
                File.separator +
                topicName +
                File.separator +
                queue.getFileName();

    }


    private void doLoadFileWithMmap(String fileName, int offset, int size) throws Exception {
        this.file = new File(fileName);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, size);
        this.byteBuffer = mappedByteBuffer.slice();
    }


    private void mmapNewConsumeQueueIfNeeded() throws Exception {
        TopicJSONCache topicJSONCache = SpringUtil.getBean(TopicJSONCache.class);
        if (!topicJSONCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }

        Topic topic = topicJSONCache.getTopic(topicName);
        Queue queue = topic.getQueueList().get(queueId);
        if (queue.getMaxOffset() <= queue.getLimit()) {
            return;
        }

        String newCommitLogFile = createConsumeQueueFile(queue.getFileName());
        doLoadFileWithMmap(newCommitLogFile, 0, COMMIT_LOG_DEFAULT_MMAP_SIZE);

        queue.setFileName(newCommitLogFile.substring(newCommitLogFile.length() - 8));
        queue.setMinOffset(0);
        queue.setMaxOffset(0);
        queue.setLimit(1 * 1024 * 1024);

    }


    public byte[] read(int offset) {
        byte[] bytes = new byte[12];
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(offset);
        byteBuffer.get(bytes, 0, 12);
        return bytes;

    }


    public int write(ConsumeQueue consumeQueue) throws Exception {
        return write(consumeQueue, false);
    }


    public synchronized int write(ConsumeQueue consumeQueue, boolean flush) throws Exception {
        mmapNewConsumeQueueIfNeeded();

        byte[] bytes = consumeQueue.convert2Bytes();
        this.mappedByteBuffer.put(bytes);


        if (!SpringUtil.getBean(TopicJSONCache.class).containsTopic(topicName)) {
            throw new RuntimeException("topic file not exist");
        }

        Queue queue = SpringUtil.getBean(TopicJSONCache.class).getTopic(topicName).getQueueList().get(queueId);
        queue.setMaxOffset(queue.getMaxOffset() + bytes.length);


        if (flush) {
            mappedByteBuffer.force();
        }

        return bytes.length;
    }


    public void cleaner() {
        Cleaner cleaner = ReflectUtil.invoke(this.mappedByteBuffer, "cleaner");
        cleaner.clean();
    }


    private String createConsumeQueueFile(String currentCommitLogName) throws IOException {
        int no = NumberUtil.parseNumber(currentCommitLogName).intValue();
        String newFilePath = SpringUtil.getBean(BaseConfig.class).getConsumeQueuePath() +
                File.separator +
                topicName +
                File.separator +
                String.format("%08d", ++no);

        File newFile = FileUtil.touch(newFilePath);

        return newFile.getPath();
    }

}
