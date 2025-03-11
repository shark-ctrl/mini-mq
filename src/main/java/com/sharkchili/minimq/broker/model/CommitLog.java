package com.sharkchili.minimq.broker.model;

import lombok.Data;

@Data
public class CommitLog {
    private String fileName;
    private Long offset;
    private Long limit;
}
