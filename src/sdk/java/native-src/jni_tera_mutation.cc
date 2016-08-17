// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:

#include "jni_tera_mutation.h"
#include "jni_tera_common.h"

#include "tera.h"

#define NativeAdd \
    JNICALL Java_com_baidu_tera_client_TeraMutationImpl_nativeAdd
#define NativeDeleteRow \
    JNICALL Java_com_baidu_tera_client_TeraMutationImpl_nativeDeleteRow
#define NativeDeleteFamily \
    JNICALL Java_com_baidu_tera_client_TeraMutationImpl_nativeDeleteFamily
#define NativeDeleteColumn \
    JNICALL Java_com_baidu_tera_client_TeraMutationImpl_nativeDeleteColumn
#define NativeDeleteColumns \
    JNICALL Java_com_baidu_tera_client_TeraMutationImpl_nativeDeleteColumns
#define NativeDeleteMutation \
    JNICALL Java_com_baidu_tera_client_TeraMutationImpl_nativeDeleteMutation

JNIEXPORT jboolean NativeAdd(JNIEnv *env, jobject jobj,
                             jlong jmutation,
                             jbyteArray jfamily,
                             jbyteArray jqualifier,
                             jbyteArray jvalue) {
    tera::RowMutation* mutation = reinterpret_cast<tera::RowMutation*>(jmutation);
    if (mutation == NULL) {
        std::string msg = "mutation not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    std::string family, qualifier, value;
    JByteArrayToString(env, jfamily, &family);
    JByteArrayToString(env, jqualifier, &qualifier);
    JByteArrayToString(env, jvalue, &value);
    mutation->Put(family, qualifier, value);
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeDeleteRow(JNIEnv *env, jobject jobj,
                                   jlong jmutation) {
    tera::RowMutation* mutation = reinterpret_cast<tera::RowMutation*>(jmutation);
    if (mutation == NULL) {
        std::string msg = "mutation not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    mutation->DeleteRow();
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeDeleteFamily(JNIEnv *env, jobject jobj,
                                      jlong jmutation,
                                      jbyteArray jfamily) {
    tera::RowMutation* mutation = reinterpret_cast<tera::RowMutation*>(jmutation);
    if (mutation == NULL) {
        std::string msg = "mutation not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    std::string family;
    JByteArrayToString(env, jfamily, &family);
    mutation->DeleteFamily(family);
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeDeleteColumn(JNIEnv *env, jobject jobj,
                                      jlong jmutation,
                                      jbyteArray jfamily,
                                      jbyteArray jcolumn) {
    tera::RowMutation* mutation = reinterpret_cast<tera::RowMutation*>(jmutation);
    if (mutation == NULL) {
        std::string msg = "mutation not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    std::string family, column;
    JByteArrayToString(env, jfamily, &family);
    JByteArrayToString(env, jcolumn, &column);
    mutation->DeleteColumn(family, column);
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeDeleteColumns(JNIEnv *env, jobject jobj,
                                       jlong jmutation,
                                       jbyteArray jfamily,
                                       jbyteArray jcolumn) {
    tera::RowMutation* mutation = reinterpret_cast<tera::RowMutation*>(jmutation);
    if (mutation == NULL) {
        std::string msg = "mutation not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    std::string family, column;
    JByteArrayToString(env, jfamily, &family);
    JByteArrayToString(env, jcolumn, &column);
    mutation->DeleteColumns(family, column);
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeDeleteMutation(JNIEnv *env, jobject jobj,
                                        jlong jmutation) {
    tera::RowMutation* mutation = reinterpret_cast<tera::RowMutation*>(jmutation);

    if (mutation != NULL) {
        delete mutation;
    }
    return JNI_TRUE;
}
