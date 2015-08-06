package com.baidu.tera.client;

import java.util.List;

public class ScanDescriptor {
    public String startRowKey;
    public String endRowKey;
    public List cfList;
    public long startTimeStamp;
    public long endTimeStamp;
    public int maxVersion;

    public ScanDescriptor(String startRowKey, String endRowKey, int maxVersion) {
        this.startRowKey = startRowKey;
        this.endRowKey = endRowKey;
        this.maxVersion = maxVersion;
        startTimeStamp = 0;
        endTimeStamp = 0;
        cfList.clear();
    }
    public ScanDescriptor(String startRowKey, String endRowKey) {
        this(startRowKey, endRowKey, 0);
    }
    public ScanDescriptor(String startRowKey) {
        this(startRowKey, "", 0);
    }
    public ScanDescriptor() {
        this("", "", 0);
    }
    public void setStartKey(String startRowKey) {
        this.startRowKey = startRowKey;
    }
    public void setEnd(String endRowKey) {
        this.endRowKey = endRowKey;
    }
    public void addFamily(String cf) {
        cfList.add(cf);
    }
    public void addColumn(String cf, String qualifier) {
        cfList.add(cf + ":" + qualifier);
    }
    public void setMaxVersions(int maxVersion) {
        this.maxVersion = maxVersion;
    }
    public void setTimeRange(long end, long start) {
        this.startTimeStamp = start;
        this.endTimeStamp = end;
    }
}
