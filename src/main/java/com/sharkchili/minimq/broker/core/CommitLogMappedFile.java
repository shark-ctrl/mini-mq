package com.sharkchili.minimq.broker.core;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.json.JSONUtil;
import com.sharkchili.minimq.broker.cache.ConsumeQueueMappedFileCache;
import com.sharkchili.minimq.broker.cache.TopicJSONCache;
import com.sharkchili.minimq.broker.config.BaseConfig;
import com.sharkchili.minimq.broker.entity.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import static com.sharkchili.minimq.broker.constants.BrokerConstants.*;


@Slf4j
public class CommitLogMappedFile {

    /**
     * 对应的commitLog实际路径地址
     */
    private File file;
    private FileChannel fileChannel;
    /**
     * 对应的mmap映射对象
     */
    private MappedByteBuffer mappedByteBuffer;

    private String topicName;


    public CommitLogMappedFile() {

    }

    public CommitLogMappedFile(String path) {
        this(new File(path));
    }

    public CommitLogMappedFile(File file) {
        this.file = file;
    }


    public void loadFileWithMmap(String topicName, int offset, int size) throws Exception {
        //获取可写入的文件路径
        String latestCommitLogPath = getLatestCommitLogPath(topicName);
        //不存在则抛异常
        if (!FileUtil.exist(latestCommitLogPath)) {
            throw new FileNotFoundException(latestCommitLogPath);
        }

        this.topicName = topicName;
        //基于commitLog建立文件映射
        doLoadFileWithMmap(latestCommitLogPath, offset, size);
    }

    private String getLatestCommitLogPath(String topicName) throws Exception {
        if (StrUtil.isEmpty(topicName)) {
            throw new IllegalArgumentException("topic name is empty");
        }
        //从缓存中拉取当前topic信息
        Topic topic = SpringUtil.getBean(TopicJSONCache.class).getTopic(topicName);
        //计算可写入的大小
        long writableSize = topic.getCommitLog().getLimit() - topic.getCommitLog().getOffset();
        //如果不够写创建新文件并返回文件名
        if (writableSize <= 0) {
            createNewCommitLogFile(topicName, topic.getCommitLog().getFileName());
        }

        //反之返回当前文件全路径
        return SpringUtil.getBean(BaseConfig.class).getBrokerConfPath() +
                "store" +
                File.separator +
                topicName +
                File.separator +
                topic.getCommitLog().getFileName();

    }


    private void doLoadFileWithMmap(String fileName, int offset, int size) throws Exception {
        this.file = new File(fileName);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, size);
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


    public synchronized void write(Message message, boolean flush) throws Exception {
        //检查当前commitLog是否够写，如果不够则创建新文件并建立mmap映射
        mmapNewCommitLogIfNeeded();
        //将消息转为byte数组
        byte[] bytes = message.convert2Bytes();
        //追加到映射内存中
        log.info("写入数据到:{},数据大小:{}", topicName, bytes.length);
        this.mappedByteBuffer.put(bytes);


        if (!SpringUtil.getBean(TopicJSONCache.class).containsTopic(topicName)) {
            throw new RuntimeException("topic file not exist");
        }
        //更新缓存中commitLog的offset信息，当前增加了bytes长度的数据
        CommitLog commitLog = SpringUtil.getBean(TopicJSONCache.class).getTopic(topicName).getCommitLog();
        //消息分发
        dispatcher(bytes, commitLog);

        commitLog.setOffset(commitLog.getOffset() + bytes.length);

        //如果flush为true则执行刷盘逻辑
        if (flush) {
            mappedByteBuffer.force();
        }
    }

    private void mmapNewCommitLogIfNeeded() throws Exception {
        TopicJSONCache topicJSONCache = SpringUtil.getBean(TopicJSONCache.class);
        if (!topicJSONCache.containsTopic(topicName)) {
            throw new RuntimeException("topic not exist");
        }

        Topic topic = topicJSONCache.getTopic(topicName);
        CommitLog commitLog = topic.getCommitLog();
        //如果当前commitLog可写数据则直接返回
        if (commitLog.getLimit() - commitLog.getOffset() > 0) {
            return;
        }
        //如果不够写则创建新的commitLog并通过mmap建立映射
        String newCommitLogFile = createNewCommitLogFile(topicName, commitLog.getFileName());
        log.info("原有commit log文件空间不足，创建新文件:{}", newCommitLogFile);
        doLoadFileWithMmap(newCommitLogFile, 0, COMMIT_LOG_DEFAULT_MMAP_SIZE);
        //更新缓存中commitLog信息为新建的commitLog信息
        commitLog.setFileName(newCommitLogFile.substring(newCommitLogFile.length() - 8));
        commitLog.setOffset(0);
        commitLog.setLimit(100);

        log.info("更新topicName:{}对应的commitLog:{}", topicName, commitLog);


    }


    private void dispatcher(byte[] msg, CommitLog commitLog) throws Exception {
        ConsumeQueue consumeQueue = new ConsumeQueue();
        consumeQueue.setCommitLogNo(NumberUtil.binaryToInt(commitLog.getFileName()));
        consumeQueue.setMsgIndex(commitLog.getOffset());
        consumeQueue.setMsgLen(msg.length);

        log.info("分发写入消息物理地址信息topicName:{},consumeQueue:{}", topicName, consumeQueue);

        int queueId = 0;
        List<ConsumeQueueMappedFile> consumeQueueMappedFileList = SpringUtil.getBean(ConsumeQueueMappedFileCache.class).get(topicName);
        ConsumeQueueMappedFile consumeQueueMappedFile = consumeQueueMappedFileList.stream()
                .filter(q -> queueId == q.getQueueId())
                .findFirst()
                .get();

        int writeLen = consumeQueueMappedFile.write(consumeQueue);
        log.info("将消息写入consumeQueue物理文件中,queue id :{}consumeQueue file info:{}", queueId, JSONUtil.toJsonStr(consumeQueueMappedFile));

        Queue queue = SpringUtil.getBean(TopicJSONCache.class).getTopic(topicName).getQueueList().stream()
                .filter(q -> q.getId() == queueId)
                .findFirst()
                .get();
        log.info("更新mq-topic.json中队列信息了,queue:{},写入长度:{}", JSONUtil.toJsonStr(queue), writeLen);
    }


    public void cleaner() {
        Cleaner cleaner = ReflectUtil.invoke(this.mappedByteBuffer, "cleaner");
        cleaner.clean();

    }


    private String createNewCommitLogFile(String topicName, String currentCommitLogName) throws IOException {
        int no = NumberUtil.parseNumber(currentCommitLogName).intValue();
        String newFilePath = SpringUtil.getBean(BaseConfig.class).getBrokerConfPath() +
                "store" +
                File.separator +
                topicName +
                File.separator +
                String.format("%08d", ++no);

        File newFile = FileUtil.touch(newFilePath);

        return newFile.getPath();
    }

}
