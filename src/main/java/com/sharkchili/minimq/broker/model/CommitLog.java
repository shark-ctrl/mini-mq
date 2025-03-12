package com.sharkchili.minimq.broker.model;

import lombok.Data;

@Data
public class CommitLog {
    private String fileName;
    private int offset;
    private int limit;
}
