package com.baidu.tera.client;

public class TeraBase {
    static {
        try {
            System.loadLibrary("jni_tera");
            System.out.println("JAVA: Load library.");
        } catch(Exception e) {
            System.out.println("JAVA: TeraBase: failed to load tera jni library.");
        }
    }

    private native void nativeInitGlog(String logFilePrefix);
    private native void nativeGlog(String type, String log);
    private native void nativeVlog(long level, String log);

    private String nativeMsg;

    public String getNativeMessage() {
        return nativeMsg;
    }

    public void InitGlog(String logFilePrefix) {
        nativeInitGlog(logFilePrefix);
    }

    public void LOG(String type, String log) {
        if (type.equals("INFO") || type.equals("info")) {
            nativeGlog("INFO", log);
        } else if (type.equals("WARNING") || type.equals("warning")) {
            nativeGlog("WARNING", log);
        } else if (type.equals("ERROR") || type.equals("error")) {
            nativeGlog("ERROR",log);
        } else {
            nativeGlog("FATAL", log);
        }
    }

    public void LOG_INFO(String log) {
        LOG("INFO", log);
    }

    public void LOG_WARNING(String log) {
        LOG("WARNING", log);
    }

    public void LOG_ERROR(String log) {
        LOG("ERROR", log);
    }

    public void LOG_FATAL(String log) {
        LOG("FATAL", log);
    }

    public void VLOG(long level, String log) {
        nativeVlog(level, log);
    }
}
