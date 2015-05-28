package com.baidu.tera.client;

public class TeraMutationImpl {
    private native boolean nativeAdd(long teraMutationPointer, byte[] family, byte[] qualifier, byte[] value);
    private native boolean nativeDeleteRow(long teraMutationPointer);
    private native boolean nativeDeleteFamily(long teraMutationPointer, byte[] family);
    private native boolean nativeDeleteColumn(long teraMutationPointer, byte[] family, byte[] column);
    private native boolean nativeDeleteColumns(long teraMutationPointer, byte[] family, byte[] column);
    private long nativeMutationPointer;

    private byte[] rowKey;

    public TeraMutationImpl(long nativeTeraMutationPointer, byte[] rowKey) {
        this.nativeMutationPointer = nativeTeraMutationPointer;
        this.rowKey = rowKey;
    }

    public byte[] getRow() {
        return rowKey;
    }

    public boolean add(byte[] family, byte[] column, byte[] value) {
        return nativeAdd(nativeMutationPointer, family, column, value);
    }

    public boolean deleteRow() {
        return nativeDeleteRow(nativeMutationPointer);
    }

    public boolean deleteFamily(byte[] family) {
        return nativeDeleteFamily(nativeMutationPointer, family);
    }

    public boolean deleteColumn(byte[] family, byte[] column) {
        return nativeDeleteColumn(nativeMutationPointer, family, column);
    }

    public boolean deleteColumns(byte[] family, byte[] column) {
        return nativeDeleteColumns(nativeMutationPointer, family, column);
    }

    public long getNativeMutationPointer() {
        return nativeMutationPointer;
    }
}
