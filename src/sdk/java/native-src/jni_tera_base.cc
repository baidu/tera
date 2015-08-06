// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:
//

#include "jni_tera_base.h"
#include "jni_tera_common.h"

#include <iostream>
#include <string>

#include "glog/logging.h"

#define NativeInitGlog \
    JNICALL Java_com_baidu_tera_client_TeraBase_nativeInitGlog
#define NativeGlog \
    JNICALL Java_com_baidu_tera_client_TeraBase_nativeGlog
#define NativeVlog \
    JNICALL Java_com_baidu_tera_client_TeraBase_nativeVlog

JNIEXPORT void NativeInitGlog(JNIEnv *env, jobject jobj,
                              jstring jprefix) {
    const char* prefix =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jprefix, NULL));
    InitGlog(prefix);
}

JNIEXPORT void NativeGlog(JNIEnv *env, jobject jobj,
                          jstring jtype,
                          jstring jlog) {
    std::string type =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtype, NULL));
    std::string log =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jlog, NULL));

    if (type == "INFO") {
        LOG(INFO) << log;
    } else if (type == "WARNING") {
        LOG(WARNING) << log;
    } else if (type == "ERROR") {
        LOG(ERROR) << log;
    } else {
        LOG(FATAL) << log;
    }
}

JNIEXPORT void NativeVlog(JNIEnv *env, jobject jobj,
                          jlong jlevel,
                          jstring jlog) {
    std::string log =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jlog, NULL));
    VLOG(jlevel) << log;
}
