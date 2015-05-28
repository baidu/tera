package com.baidu.tera.client;

public class TeraReaderImpl {
    private native boolean nativeAddColumn(long teraMutationPointer, byte[] family, byte[] column);
    private native boolean nativeAddFamily(long teraMutationPointer, byte[] family);
    private native boolean nativeDeleteReader(long nativeTeraMutationPointer);
    private long nativeReaderPointer;

    private byte[] rowKey;

    public TeraReaderImpl(long nativeTeraReaderPointer, byte[] rowKey) {
        this.nativeReaderPointer = nativeTeraReaderPointer;
        this.rowKey = rowKey;
    }

    public boolean add(byte[] family, byte[] column) {
        if (column == null) {
            return nativeAddFamily(nativeReaderPointer, family);
        } else {
            return nativeAddColumn(nativeReaderPointer, family, column);
        }
    }

    public long getNativeReaderPointer() {
        return nativeReaderPointer;
    }

    public void finalize() throws Throwable {
        nativeDeleteReader(nativeReaderPointer);
        nativeReaderPointer = 0;
    }
}
