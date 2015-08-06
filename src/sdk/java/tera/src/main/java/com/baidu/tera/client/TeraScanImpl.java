package com.baidu.tera.client;

import java.io.IOException;

public class TeraScanImpl extends TeraBase {
    private long nativeScanDescPointer;

    private native long nativeNewScanDesc(byte[] startKey, byte[] endKey);
    private native boolean nativeAddFamily(long nativeScanDescPointer, byte[] family);
    private native boolean nativeAddColumn(long nativeScanDescPointer, byte[] family, byte[] column);
    private native void nativeDeleteScanDesc(long nativeScanDescPointer);

    public TeraScanImpl(byte[] startKey, byte[] endKey) throws IOException {
        nativeScanDescPointer = nativeNewScanDesc(startKey, endKey);
        if (nativeScanDescPointer == 0) {
            throw new IOException();
        }
    }

    public boolean add(byte[] family, byte[] column) {
        if (column == null) {
            return nativeAddFamily(nativeScanDescPointer, family);
        } else {
            return nativeAddColumn(nativeScanDescPointer, family, column);
        }
    }

    public long getNativeScanDescPointer() {
        return nativeScanDescPointer;
    }

    public void finalize() throws Throwable {
        if (nativeScanDescPointer != 0) {
            nativeDeleteScanDesc(nativeScanDescPointer);
            nativeScanDescPointer = 0;
        }
    }
}
