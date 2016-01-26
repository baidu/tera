package com.baidu.tera.client;

import java.io.IOException;

public class TeraClientImpl extends TeraBase {
    private long nativeTeraClientPointer;
    private long nativeTeraTablePointer;

    private native long nativeNewClient(String confPath);
    private native boolean nativeDeleteClient(long nativeClientPtr);
    private native boolean nativeCreateTable(long nativeClientPtr, String tableName, String tableSchema);
    private native boolean nativeDeleteTable(long nativeClientPtr, String tableName);
    private native boolean nativeEnableTable(long nativeClientPtr, String tableName);
    private native boolean nativeDisableTable(long nativeClientPtr, String tableName);
    private native boolean nativeIsTableExist(long nativeClientPtr, String tableName);
    private native boolean nativeIsTableEnabled(long nativeClientPtr, String tableName);
    private native boolean nativeIsTableEmpty(long nativeClientPtr, String tableName);
    private native long nativeOpenTable(long nativeClientPtr, String tableName);
    private native String nativeGetTableDescriptor(long nativeClientPtr, String tableName);
    private native String[] nativeListTables(long nativeClientPtr);

    public TeraClientImpl(String confPath) throws IOException {
        try {
            if (confPath.length() == 0) {
                confPath = System.getenv("TERA_CONF_PATH");
                if (confPath == null) {
                    confPath = "./tera.flag";
                }
            }
            nativeTeraClientPointer = nativeNewClient(confPath);
            if (nativeTeraClientPointer <= 0) {
                LOG_ERROR("JAVA: failed to create new tera client.");
                throw new IOException();
            }
        } catch(Exception e) {
            LOG_ERROR("JAVA: IOException occured: " + e.toString());
        }
    }

    public TeraClientImpl() throws IOException {
        this("");
    }

    public boolean createTable(String tableName, String tableSchema) {
        VLOG(10, "create table:" + tableName);
        boolean retval = nativeCreateTable(nativeTeraClientPointer, tableName, tableSchema);
        return retval;
    }

    public boolean createTable(String tableName) {
        return createTable(tableName, "");
    }

    public boolean dropTable(String tableName) throws InterruptedException {
        VLOG(10, "drop table:" + tableName);
        int i = 60;
        while (i-- > 0) {
            if (nativeDeleteTable(nativeTeraClientPointer, tableName)) {
                return true;
            }
            Thread.sleep(1000);
        }
        LOG_ERROR("JAVA: " + getNativeMessage());
        return false;
    }

    public boolean enableTable(String tableName) {
        VLOG(10, "enable table:" + tableName);
        return nativeEnableTable(nativeTeraClientPointer, tableName);
    }

    public boolean disableTable(String tableName) {
        VLOG(10, "disable table:" + tableName);
        return nativeDisableTable(nativeTeraClientPointer, tableName);
    }

    public boolean isTableExist(String tableName) {
        return nativeIsTableExist(nativeTeraClientPointer, tableName);
    }

    public boolean isTableEnabled(String tableName) {
        return nativeIsTableEnabled(nativeTeraClientPointer, tableName);
    }

    public boolean isTableEmpty(String tableName) {
        return nativeIsTableEmpty(nativeTeraClientPointer, tableName);
    }

    public String getTableDescriptor(String tableName) {
        return nativeGetTableDescriptor(nativeTeraClientPointer, tableName);
    }

    public String[] listTables() {
        return nativeListTables(nativeTeraClientPointer);
    }

    public long openTable(String tableName) throws IOException {
        nativeTeraTablePointer = nativeOpenTable(nativeTeraClientPointer, tableName);
        if (nativeTeraTablePointer == 0) {
            LOG_ERROR("JAVA: failed to open table: " + tableName);
            throw new IOException();
        }
        return nativeTeraTablePointer;
    }

    public void finalize() throws Throwable {
        if (nativeTeraTablePointer != 0) {
            nativeDeleteClient(nativeTeraClientPointer);
        }
    }
}
