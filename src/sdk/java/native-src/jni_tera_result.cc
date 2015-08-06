// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:

#include "jni_tera_result.h"
#include "jni_tera_common.h"

#define NativeReaderDone \
    JNICALL Java_com_baidu_tera_client_TeraResultImpl_nativeReaderDone
#define NativeReaderNext \
    JNICALL Java_com_baidu_tera_client_TeraResultImpl_nativeReaderNext
#define NativeGetRow \
    JNICALL Java_com_baidu_tera_client_TeraResultImpl_nativeGetRow
#define NativeGetFamily \
    JNICALL Java_com_baidu_tera_client_TeraResultImpl_nativeGetFamily
#define NativeGetColumn \
    JNICALL Java_com_baidu_tera_client_TeraResultImpl_nativeGetColumn
#define NativeGetTimeStamp \
    JNICALL Java_com_baidu_tera_client_TeraResultImpl_nativeGetTimeStamp
#define NativeGetValue \
    JNICALL Java_com_baidu_tera_client_TeraResultImpl_nativeGetValue

JNIEXPORT jboolean NativeReaderDone(JNIEnv *env, jobject jobj,
                                    jlong jreader) {
    tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);
    if (reader == NULL) {
        std::string msg = "reader not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_TRUE;
    }
    if (reader->Done()) {
        return JNI_TRUE;
    } else {
        return JNI_FALSE;
    }
}

JNIEXPORT void NativeReaderNext(JNIEnv *env, jobject jobj,
                                jlong jreader) {
    tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);
    if (reader == NULL) {
        std::string msg = "reader not initialized.";
        SendErrorJ(env, jobj, msg);
        return;
    }
    reader->Next();
}

JNIEXPORT jbyteArray NativeGetRow(JNIEnv *env, jobject jobj,
                                  jlong jreader) {
    tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);
    jbyteArray jrow;
    if (reader == NULL) {
        std::string msg = "reader not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }
    std::string row = reader->RowName();
    StringToJByteArray(env, row, &jrow);
    return jrow;
}

JNIEXPORT jbyteArray NativeGetFamily(JNIEnv *env, jobject jobj,
                                     jlong jreader) {
    tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);
    jbyteArray jfamily;
    if (reader == NULL) {
        std::string msg = "reader not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }
    std::string family = reader->Family();
    StringToJByteArray(env, family, &jfamily);
    return jfamily;
}

JNIEXPORT jbyteArray NativeGetColumn(JNIEnv *env, jobject jobj,
                                     jlong jreader) {
    tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);
    jbyteArray jcolumn;
    if (reader == NULL) {
        std::string msg = "reader not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }
    std::string column = reader->Qualifier();
    StringToJByteArray(env, column, &jcolumn);
    return jcolumn;
}

JNIEXPORT jlong NativeGetTimeStamp(JNIEnv *env, jobject jobj,
                                   jlong jreader) {
    tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);
    jlong ts = -1;
    if (reader == NULL) {
        std::string msg = "reader not initialized.";
        SendErrorJ(env, jobj, msg);
        return ts;
    }
    ts = reader->Timestamp();
    return ts;
}

JNIEXPORT jbyteArray NativeGetValue(JNIEnv *env, jobject jobj,
                                    jlong jreader) {
    tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);
    jbyteArray jvalue;
    if (reader == NULL) {
        std::string msg = "reader not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }
    std::string value = reader->Value();
    StringToJByteArray(env, value, &jvalue);
    return jvalue;
}
