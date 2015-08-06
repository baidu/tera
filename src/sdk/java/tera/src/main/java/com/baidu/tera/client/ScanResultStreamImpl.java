package com.baidu.tera.client;

public class ScanResultStreamImpl extends TeraBase {
    private long nativeStreamPointer;

    private native boolean nativeDone(long nativeStreamPointer);
    private native void nativeNext(long nativeStreamPointer);
    private native byte[] nativeGetRow(long nativeStreamPointer);
    private native byte[] nativeGetFamily(long nativeStreamPointer);
    private native byte[] nativeGetColumn(long nativeStreamPointer);
    private native long nativeGetTimeStamp(long nativeStreamPointer);
    private native byte[] nativeGetValue(long nativeStreamPointer);
    private native void nativeDeleteResultStream(long nativeStreamPointer);

    public ScanResultStreamImpl(long nativeStreamPtr) {
        if (nativeStreamPtr != 0) {
            this.nativeStreamPointer = nativeStreamPtr;
        }
    }

    public boolean done() {
        return nativeDone(nativeStreamPointer);
    }

    public void next() {
        nativeNext(nativeStreamPointer);
    }

    public byte[] getRow() {
        return nativeGetRow(nativeStreamPointer);
    }

    public byte[] getFamily() {
        return nativeGetFamily(nativeStreamPointer);
    }

    public byte[] getColumn() {
        return nativeGetColumn(nativeStreamPointer);
    }

    public byte[] getValue() {
        return nativeGetValue(nativeStreamPointer);
    }

    public long getTimeStamp() {
        return nativeGetTimeStamp(nativeStreamPointer);
    }

    public void finalize() {
        if (nativeStreamPointer != 0) {
            nativeDeleteResultStream(nativeStreamPointer);
        }
    }
}
