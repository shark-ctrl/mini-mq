package com.sharkchili.minimq.broker.entity;

import lombok.Data;

@Data
public class CommitLog {
    /**
     * commitLog对应的文件名
     */
    private String fileName;
    /**
     * 下一个可写入的偏移量
     */
    private int offset;
    /**
     * 最大可写入的范围
     */
    private int limit;
}
