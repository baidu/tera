// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:
//

#include "jni_tera_client.h"
#include "jni_tera_common.h"

#include "glog/logging.h"

#include "sdk/tera.h"
#include "sdk/sdk_utils.h"

#define NativeNewClient \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeNewClient
#define NativeDeleteClient \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeDeleteClient
#define NativeCreateTable \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeCreateTable
#define NativeDeleteTable \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeDeleteTable
#define NativeEnableTable \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeEnableTable
#define NativeDisableTable \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeDisableTable
#define NativeIsTableExist \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeIsTableExist
#define NativeIsTableEnabled \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeIsTableEnabled
#define NativeIsTableEmpty \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeIsTableEmpty
#define NativeOpenTable \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeOpenTable
#define NativeGetTableDescriptor \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeGetTableDescriptor
#define NativeListTables \
    JNICALL Java_com_baidu_tera_client_TeraClientImpl_nativeListTables

JNIEXPORT jlong NativeNewClient(JNIEnv *env, jobject jobj,
                                jstring jconfpath) {
    std::string confpath =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jconfpath, NULL));
    tera::ErrorCode error_code;
    std::string msg;
    tera::Client* client = tera::Client::NewClient(confpath, "teracli_java", &error_code);
    if (client == NULL) {
        msg = "failed to create tera client, error_code: " + error_code.GetReason();
        SendErrorJ(env, jobj, msg);
        return 0;
    }
    jlong jclient_ptr = reinterpret_cast<uint64_t>(client);
    return jclient_ptr;
}

JNIEXPORT jboolean NativeDeleteClient(JNIEnv *env, jobject jobj,
                                      jlong jclient_ptr) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    if (client == NULL) {
        LOG(WARNING) << "tera client not initialized.";
        return JNI_TRUE;
    }
    delete client;
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeCreateTable(JNIEnv *env, jobject jobj,
                                     jlong jclient_ptr,
                                     jstring jtablename,
                                     jstring jtableschema) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    std::string tablename =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtablename, NULL));
    std::string tableschema =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtableschema, NULL));
    tera::ErrorCode error_code;
    std::string msg;

    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    tera::TableDescriptor desc(tablename);
    if (!ParseTableSchema(tableschema, &desc, &error_code)) {
        msg = "failed to parse input table schema.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    if (!client->CreateTable(desc, &error_code)) {
        msg = "failed to create table, reason: " + error_code.GetReason();
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeDeleteTable(JNIEnv *env, jobject jobj,
                                     jlong jclient_ptr,
                                     jstring jtablename) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    std::string tablename =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtablename, NULL));
    tera::ErrorCode error_code;
    std::string msg;

    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    if (!client->DeleteTable(tablename, &error_code)) {
        msg = "failed to delete table, reason: " + error_code.GetReason();
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeEnableTable(JNIEnv *env, jobject jobj,
                                     jlong jclient_ptr,
                                     jstring jtablename) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    std::string tablename =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtablename, NULL));
    tera::ErrorCode error_code;
    std::string msg;

    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    if (!client->EnableTable(tablename, &error_code)) {
        msg = "failed to enable table, reason: " + error_code.GetReason();
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeDisableTable(JNIEnv *env, jobject jobj,
                                      jlong jclient_ptr,
                                      jstring jtablename) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    std::string tablename =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtablename, NULL));
    tera::ErrorCode error_code;
    std::string msg;

    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    if (!client->DisableTable(tablename, &error_code)) {
        msg = "failed to disable table, reason: " + error_code.GetReason();
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    return JNI_TRUE;
}

JNIEXPORT jboolean NativeIsTableExist(JNIEnv *env, jobject jobj,
                                      jlong jclient_ptr,
                                      jstring jtablename) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    std::string tablename =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtablename, NULL));
    tera::ErrorCode error_code;
    std::string msg;

    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    bool ret = client->IsTableExist(tablename, &error_code);
    if (ret) {
        return JNI_TRUE;
    } else {
        return JNI_FALSE;
    }
}

JNIEXPORT jboolean NativeIsTableEnabled(JNIEnv *env, jobject jobj,
                                        jlong jclient_ptr,
                                        jstring jtablename) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    std::string tablename =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtablename, NULL));
    tera::ErrorCode error_code;
    std::string msg;

    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    bool ret = client->IsTableEnabled(tablename, &error_code);
    if (ret) {
        return JNI_TRUE;
    } else {
        return JNI_FALSE;
    }
}

JNIEXPORT jboolean NativeIsTableEmpty(JNIEnv *env, jobject jobj,
                                      jlong jclient_ptr,
                                      jstring jtablename) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    std::string tablename =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtablename, NULL));
    tera::ErrorCode error_code;
    std::string msg;

    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return JNI_FALSE;
    }
    bool ret = client->IsTableEmpty(tablename, &error_code);
    if (ret) {
        return JNI_TRUE;
    } else {
        return JNI_FALSE;
    }
}

JNIEXPORT jlong NativeOpenTable(JNIEnv *env, jobject jobj,
                                jlong jclient_ptr,
                                jstring jtablename) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    tera::Table* table;
    std::string tablename =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtablename, NULL));
    tera::ErrorCode error_code;
    std::string msg;
    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return 0;
    }
    table = client->OpenTable(tablename, &error_code);
    if (table == NULL) {
        msg = "failed to open table, reason: " + error_code.GetReason();
        SendErrorJ(env, jobj, msg);
        return 0;
    }
    jlong jtable = reinterpret_cast<uint64_t>(table);
    return jtable;
}

JNIEXPORT jstring NativeGetTableDescriptor (JNIEnv *env, jobject jobj,
                                            jlong jclient_ptr,
                                            jstring jtablename) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    std::string tablename =
        reinterpret_cast<const char*>(env->GetStringUTFChars(jtablename, NULL));
    tera::ErrorCode error_code;
    std::string msg;
    jstring jdesc;

    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }

    tera::TableInfo table_info = {NULL, ""};
    if (!client->List(tablename, &table_info, NULL, &error_code)) {
        LOG(ERROR) << "fail to get meta data from tera.";
        return NULL;
    }
    if (table_info.table_desc == NULL) {
        return NULL;
    }
    std::string schema;
    int cf_num = table_info.table_desc->ColumnFamilyNum();
    for (int cf_no = 0; cf_no < cf_num; ++cf_no) {
        const tera::ColumnFamilyDescriptor* cf_desc =
            table_info.table_desc->ColumnFamily(cf_no);
        if (cf_no > 0) {
            schema.append(",");
        }
        schema.append(cf_desc->Name());
    }

    jdesc = env->NewStringUTF(schema.data());
    return jdesc;
}

JNIEXPORT jobjectArray NativeListTables(JNIEnv *env, jobject jobj,
                                   jlong jclient_ptr) {
    tera::Client* client = reinterpret_cast<tera::Client*>(jclient_ptr);
    tera::ErrorCode error_code;
    std::string msg;
    if (client == NULL) {
        msg = "tera client not initialized.";
        SendErrorJ(env, jobj, msg);
        return NULL;
    }
    std::vector<tera::TableInfo> table_list;
    if (!client->List(&table_list, &error_code)) {
        msg = "failed to list tables";
        SendErrorJ(env, jobj, msg);
    }
    jobjectArray jtable_list;
    int32_t table_num = table_list.size();
    if (table_num <= 0) {
        return NULL;
    }
    jtable_list = (jobjectArray)env->NewObjectArray(table_num,
                                                    env->FindClass("Ljava/lang/String;"),
                                                    NULL);
    for (int32_t i = 0; i < table_num; ++i) {
        tera::TableDescriptor* desc = table_list[i].table_desc;
        std::string schema = ConvertDescToString(desc);
        delete desc;
        env->SetObjectArrayElement(jtable_list, i, env->NewStringUTF(schema.c_str()));
    }

    return jtable_list;
}
