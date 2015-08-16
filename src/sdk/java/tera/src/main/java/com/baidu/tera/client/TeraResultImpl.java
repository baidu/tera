package com.baidu.tera.client;

import java.io.IOException;
import java.util.Arrays;

public class TeraResultImpl extends TeraBase {
    private long nativeReaderPtr;
    private TeraReaderImpl teraReaderImpl;
    private native boolean nativeReaderDone(long nativeReaderPtr);
    private native void nativeReaderNext(long nativeReaderPtr);
    private native byte[] nativeGetRow(long nativeReaderPtr);
    private native byte[] nativeGetFamily(long nativeReaderPtr);
    private native byte[] nativeGetColumn(long nativeReaderPtr);
    private native long nativeGetTimeStamp(long nativeReaderPtr);
    private native byte[] nativeGetValue(long nativeReaderPtr);

    public TeraResultImpl(TeraReaderImpl reader) {
        try {
            this.teraReaderImpl = reader;
            this.nativeReaderPtr = teraReaderImpl.getNativeReaderPointer();
            if (nativeReaderPtr == 0) {
                throw new IOException();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void next() {
        nativeReaderNext(nativeReaderPtr);
    }

    public boolean done() {
        return nativeReaderDone(nativeReaderPtr);
    }

    public byte[] getRow() {
        return nativeGetRow(nativeReaderPtr);
    }

    public byte[] getFamily() {
        return nativeGetFamily(nativeReaderPtr);
    }

    public byte[] getColumn() {
        return nativeGetColumn(nativeReaderPtr);
    }

    public long getTimeStamp() {
        return nativeGetTimeStamp(nativeReaderPtr);
    }

    public byte[] getValue() {
        return nativeGetValue(nativeReaderPtr);
    }

    public boolean equal(byte[] row, byte[] family, byte[] column, byte[] value) {
        if (Arrays.equals(getRow(), row) &&
                Arrays.equals(getFamily(), family) &&
                Arrays.equals(getColumn(), column) &&
                Arrays.equals(getValue(), value)) {
            return true;
        }
        return false;
    }
}
