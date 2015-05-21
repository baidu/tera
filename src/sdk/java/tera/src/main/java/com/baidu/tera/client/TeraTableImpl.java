package com.baidu.tera.client;

import java.io.IOException;

public class TeraTableImpl extends TeraBase {
    private native boolean nativePut(long nativeTablePtr, String rowKey, String family, String qualifier, String value, long timeStamp);
    private native String nativeGet(long nativeTablePtr, String rowKey, String family, String qualifier, long timeStamp);
    private native long nativeNewMutation(long nativeTablePtr, byte[] rowKey);
    private native boolean nativeApplyMutation(long nativeTablePtr, long nativeMutationPointer);
    private native void nativeFlushCommits(long nativeTablePtr);
    private native long nativeNewReader(long nativeTablePtr, byte[] rowKey);
    private native long nativeApplyReader(long nativeTablePtr, long nativeReaderPointer);
    private native long nativeScan(long nativeTablePtr, long nativeScanDescPointer);
    private long nativeTablePointer;

    private TeraClientImpl client;

    public TeraTableImpl(String tableName, String confPath) throws IOException {
        client = new TeraClientImpl(confPath);
        nativeTablePointer = client.openTable(tableName);
        if (nativeTablePointer == 0) {
            LOG_ERROR("JAVA: failed to open tera table: " + tableName);
            throw new IOException();
        }
    }

    public TeraTableImpl(String tableName) throws IOException {
        this(tableName, "");
    }

    public boolean put(String rowKey, String family, String qualifier, String value, long timeStamp) {
        VLOG(10, "JAVA: put: " + rowKey);
        return nativePut(nativeTablePointer, rowKey, family, qualifier, value, timeStamp);
    }

    public boolean put(String rowKey, String family, String qualifier, String value) {
        return put(rowKey, family, qualifier, value, 0);
    }

    public boolean put(String rowKey, String value) {
        return put(rowKey, "", "", value, 0);
    }

    public String get(String rowKey, String family, String qualifier, long timeStamp) {
        VLOG(10, "JAVA: get: " + rowKey);
        return nativeGet(nativeTablePointer, rowKey, family, qualifier, timeStamp);
    }

    public String get(String rowKey, String family, String qualifier) {
        return get(rowKey, family, qualifier, 0);
    }

    public String get(String rowKey) {
        return get(rowKey, "", "", 0);
    }

    public ScanResultStreamImpl scan(TeraScanImpl scanImpl) {
        long nativeScanDescPointer = scanImpl.getNativeScanDescPointer();
        long nativeResultStreamPointer = nativeScan(nativeTablePointer, nativeScanDescPointer);
        return new ScanResultStreamImpl(nativeResultStreamPointer);
    }

    public TeraMutationImpl newMutation(byte[] rowKey) {
        long nativeMutationPtr = nativeNewMutation(nativeTablePointer, rowKey);
        return new TeraMutationImpl(nativeMutationPtr, rowKey);
    }

    public void applyMutation(TeraMutationImpl mutation) {
        long nativeMutationPtr = mutation.getNativeMutationPointer();
        nativeApplyMutation(nativeTablePointer, nativeMutationPtr);
    }

    public void flushCommits() {
        nativeFlushCommits(nativeTablePointer);
    }

    public TeraReaderImpl newReader(byte[] rowKey) {
        long nativeReaderPtr = nativeNewReader(nativeTablePointer, rowKey);
        return new TeraReaderImpl(nativeReaderPtr, rowKey);
    }

    public TeraResultImpl applyReader(TeraReaderImpl reader) {
        long nativeReaderPtr = reader.getNativeReaderPointer();
        nativeApplyReader(nativeTablePointer, nativeReaderPtr);
        TeraResultImpl resultImpl = new TeraResultImpl(reader);
        return resultImpl;
    }
}
