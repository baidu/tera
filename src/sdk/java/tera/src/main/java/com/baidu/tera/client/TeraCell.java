package com.baidu.tera.client;

public class TeraCell {
    private int status = -1;
    private String rowKey;
    private String family;
    private String qualifier;
    private String value;
    private long timeStamp;

    public TeraCell(String rowKey, String family, String qualifier, String value, long timeStamp) {
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
        this.timeStamp = timeStamp;
        status = 0;
    }
}
