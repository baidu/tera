// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:

#include "jni_tera_result_stream.h"
#include "jni_tera_common.h"

#include "sdk/tera.h"

#define NativeDone \
    JNICALL Java_com_baidu_tera_client_ScanResultStreamImpl_nativeDone
#define NativeNext \
    JNICALL Java_com_baidu_tera_client_ScanResultStreamImpl_nativeNext
#define NativeGetRow \
    JNICALL Java_com_baidu_tera_client_ScanResultStreamImpl_nativeGetRow
#define NativeGetFamily \
    JNICALL Java_com_baidu_tera_client_ScanResultStreamImpl_nativeGetFamily
#define NativeGetColumn \
    JNICALL Java_com_baidu_tera_client_ScanResultStreamImpl_nativeGetColumn
#define NativeGetTimeStamp \
    JNICALL Java_com_baidu_tera_client_ScanResultStreamImpl_nativeGetTimeStamp
#define NativeGetValue \
    JNICALL Java_com_baidu_tera_client_ScanResultStreamImpl_nativeGetValue
#define NativeDeleteResultStream \
    JNICALL Java_com_baidu_tera_client_ScanResultStreamImpl_nativeDeleteResultStream

JNIEXPORT jboolean NativeDone(JNIEnv *env, jobject jobj, jlong jresult) {
    tera::ResultStream* result = reinterpret_cast<tera::ResultStream*>(jresult);
    if (result == NULL) {
        std::string msg = "result not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_TRUE;
    }
    if (result->Done()) {
        return JNI_TRUE;
    } else {
        return JNI_FALSE;
    }
}

JNIEXPORT void NativeNext(JNIEnv *env, jobject jobj, jlong jresult) {
    tera::ResultStream* result = reinterpret_cast<tera::ResultStream*>(jresult);
    if (result == NULL) {
        std::string msg = "result not initialized.";
        SendErrorJ(env, jobj, msg);
        return;
    }
    result->Next();
}

JNIEXPORT jbyteArray NativeGetRow(JNIEnv *env, jobject jobj, jlong jresult) {
    tera::ResultStream* result = reinterpret_cast<tera::ResultStream*>(jresult);
    jbyteArray jrow;
    if (result == NULL) {
        std::string msg = "result not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }
    std::string row = result->RowName();
    StringToJByteArray(env, row, &jrow);
    return jrow;
}

JNIEXPORT jbyteArray NativeGetFamily(JNIEnv *env, jobject jobj, jlong jresult) {
    tera::ResultStream* result = reinterpret_cast<tera::ResultStream*>(jresult);
    jbyteArray jfamily;
    if (result == NULL) {
        std::string msg = "result not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }
    std::string family = result->Family();
    StringToJByteArray(env, family, &jfamily);
    return jfamily;
}

JNIEXPORT jbyteArray NativeGetColumn(JNIEnv *env, jobject jobj, jlong jresult) {
    tera::ResultStream* result = reinterpret_cast<tera::ResultStream*>(jresult);
    jbyteArray jcolumn;
    if (result == NULL) {
        std::string msg = "result not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }
    std::string column = result->Qualifier();
    StringToJByteArray(env, column, &jcolumn);
    return jcolumn;
}

JNIEXPORT jlong NativeGetTimeStamp(JNIEnv *env, jobject jobj, jlong jresult) {
    tera::ResultStream* result = reinterpret_cast<tera::ResultStream*>(jresult);
    jlong ts = -1;
    if (result == NULL) {
        std::string msg = "result not initialized.";
        SendErrorJ(env, jobj, msg);
        return ts;
    }
    ts = result->Timestamp();
    return ts;
}

JNIEXPORT jbyteArray NativeGetValue(JNIEnv *env, jobject jobj, jlong jresult) {
    tera::ResultStream* result = reinterpret_cast<tera::ResultStream*>(jresult);
    jbyteArray jvalue;
    if (result == NULL) {
        std::string msg = "result not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }
    std::string value = result->Value();
    StringToJByteArray(env, value, &jvalue);
    return jvalue;
}

JNIEXPORT void NativeDeleteResultStream(JNIEnv *env, jobject jobj, jlong jresult) {
    tera::ResultStream* result = reinterpret_cast<tera::ResultStream*>(jresult);
    if (result == NULL) {
        std::string msg = "result not initialized.";
        SendErrorJ(env, jobj, msg);
        return;
    }
    delete result;
}
