package com.sharkchili.minimq.broker.core;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.sharkchili.minimq.broker.cache.MappedFileCache;
import com.sharkchili.minimq.broker.cache.TopicCache;
import com.sharkchili.minimq.broker.model.CommitLog;
import com.sharkchili.minimq.broker.model.Message;
import com.sharkchili.minimq.broker.model.Topic;
import org.springframework.util.ResourceUtils;
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.sharkchili.minimq.broker.constants.BrokerConstants.*;


public class MappedFile {

    private File file;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    private String topicName;


    public MappedFile() {

    }

    public MappedFile(String path) {
        this(new File(path));
    }

    public MappedFile(File file) {
        this.file = file;
    }


    public void loadFileWithMmap(String topicName, int offset, int size) throws Exception {
        String latestCommitLogPath = getLatestCommitLogPath(topicName);
        if (!FileUtil.exist(latestCommitLogPath)) {
            throw new FileNotFoundException(latestCommitLogPath);
        }

        this.topicName = topicName;
        doLoadFileWithMmap(latestCommitLogPath, offset, size);
    }


    private void doLoadFileWithMmap(String fileName, int offset, int size) throws Exception {
        this.file = new File(fileName);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, size);
    }


    private void mapNewCommitLogIfNeeded() throws Exception {
        TopicCache topicCache = SpringUtil.getBean(TopicCache.class);
        if (!topicCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }

        Topic topic = topicCache.getTopic(topicName);
        CommitLog commitLog = topic.getCommitLog();
        if (commitLog.getLimit() - commitLog.getOffset() >= 0) {
            return;
        }

        String newCommitLogFile = createNewCommitLogFile(topicName, commitLog.getFileName());
        doLoadFileWithMmap(newCommitLogFile, 0, COMMIT_LOG_DEFAULT_MMAP_SIZE);


    }


    public byte[] read(int offset, int size) throws IOException {
        byte[] bytes = new byte[size];
        this.mappedByteBuffer.position(offset);
        this.mappedByteBuffer.get(bytes, 0, size);
        return bytes;
    }


    public void write(Message message) throws Exception {
        write(message, false);
    }


    public void write(Message message, boolean flush) throws Exception {

        mapNewCommitLogIfNeeded();

        this.mappedByteBuffer.put(message.convert2Bytes());
        if (flush) {
            mappedByteBuffer.force();
        }
    }


    public void cleaner() {
        Cleaner cleaner = ReflectUtil.invoke(this.mappedByteBuffer, "cleaner");
        cleaner.clean();

    }

    public String getLatestCommitLogPath(String topicName) throws Exception {
        if (StrUtil.isEmpty(topicName)) {
            throw new IllegalArgumentException("topic name is empty");
        }

        Topic topic = SpringUtil.getBean(TopicCache.class).getTopic(topicName);
        long diff = topic.getCommitLog().getLimit() - topic.getCommitLog().getOffset();
        if (diff <= 0) {
            createNewCommitLogFile(topicName, topic.getCommitLog().getFileName());
        }


        return ResourceUtils.getFile(BASE_STORE_PATH).getPath() +
                File.separator +
                topicName +
                File.separator +
                topic.getCommitLog().getFileName();

    }

    private String createNewCommitLogFile(String topicName, String currentCommitLogName) throws IOException {
        int no = NumberUtil.parseNumber(currentCommitLogName).intValue();
        String newFilePath = ResourceUtils.getFile(BASE_STORE_PATH).getPath() +
                File.separator +
                topicName +
                File.separator +
                String.format("%08d", ++no);

        File newFile = FileUtil.touch(newFilePath);

        return newFile.getPath();
    }

}
