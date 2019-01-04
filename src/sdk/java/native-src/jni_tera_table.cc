// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:

#include "jni_tera_table.h"
#include "jni_tera_common.h"

#include <string>

#include "glog/logging.h"

#include "tera.h"

#define NativePut JNICALL Java_com_baidu_tera_client_TeraTableImpl_nativePut
#define NativeGet JNICALL Java_com_baidu_tera_client_TeraTableImpl_nativeGet
#define NativeNewMutation JNICALL Java_com_baidu_tera_client_TeraTableImpl_nativeNewMutation
#define NativeApplyMutation JNICALL Java_com_baidu_tera_client_TeraTableImpl_nativeApplyMutation
#define NativeFlushCommits JNICALL Java_com_baidu_tera_client_TeraTableImpl_nativeFlushCommits
#define NativeNewReader JNICALL Java_com_baidu_tera_client_TeraTableImpl_nativeNewReader
#define NativeApplyReader JNICALL Java_com_baidu_tera_client_TeraTableImpl_nativeApplyReader
#define NativeScan JNICALL Java_com_baidu_tera_client_TeraTableImpl_nativeScan

JNIEXPORT jboolean NativePut(JNIEnv* env, jobject jobj, jlong jtable_ptr, jstring jrowkey,
                             jstring jfamily, jstring jqualifier, jstring jvalue, jlong timestamp) {
  tera::Table* table = reinterpret_cast<tera::Table*>(jtable_ptr);
  std::string rowkey = reinterpret_cast<const char*>(env->GetStringUTFChars(jrowkey, NULL));
  std::string family = reinterpret_cast<const char*>(env->GetStringUTFChars(jfamily, NULL));
  std::string qualifier = reinterpret_cast<const char*>(env->GetStringUTFChars(jqualifier, NULL));
  std::string value = reinterpret_cast<const char*>(env->GetStringUTFChars(jvalue, NULL));
  tera::ErrorCode error_code;
  std::string msg;

  if (table == NULL) {
    msg = "table not initialized.";
    SendErrorJ(env, jobj, msg);
    return JNI_FALSE;
  }

  if (timestamp == 0) {
    if (!table->Put(rowkey, family, qualifier, value, &error_code)) {
      msg = "failed to put record to table, reason: " + error_code.GetReason();
      SendErrorJ(env, jobj, msg);
      return JNI_FALSE;
    }
  } else {
    if (!table->Put(rowkey, family, qualifier, value, timestamp, &error_code)) {
      msg = "failed to put record to table, reason: " + error_code.GetReason();
      SendErrorJ(env, jobj, msg);
      return JNI_FALSE;
    }
  }

  return JNI_TRUE;
}

JNIEXPORT jstring NativeGet(JNIEnv* env, jobject jobj, jlong jtable_ptr, jstring jrowkey,
                            jstring jfamily, jstring jqualifier, jlong timestamp) {
  tera::Table* table = reinterpret_cast<tera::Table*>(jtable_ptr);
  std::string rowkey = reinterpret_cast<const char*>(env->GetStringUTFChars(jrowkey, NULL));
  std::string family = reinterpret_cast<const char*>(env->GetStringUTFChars(jfamily, NULL));
  std::string qualifier = reinterpret_cast<const char*>(env->GetStringUTFChars(jqualifier, NULL));
  std::string value;
  tera::ErrorCode error_code;
  jstring jvalue;
  if (table == NULL) {
    SendErrorJ(env, jobj, "table not initialized.");
    return NULL;
  }

  if (!table->Get(rowkey, family, qualifier, &value, &error_code)) {
    std::string msg = "failed to get record from table, reason: " + error_code.GetReason();
    SendErrorJ(env, jobj, msg);
    return NULL;
  }

  jvalue = env->NewStringUTF(value.c_str());
  return jvalue;
}

JNIEXPORT jlong NativeNewMutation(JNIEnv* env, jobject jobj, jlong jtable_ptr, jbyteArray jrowkey) {
  tera::Table* table = reinterpret_cast<tera::Table*>(jtable_ptr);
  tera::ErrorCode error_code;
  std::string rowkey;

  if (table == NULL) {
    SendErrorJ(env, jobj, "table not initialized.");
    return 0;
  }
  JByteArrayToString(env, jrowkey, &rowkey);
  tera::RowMutation* mutation = table->NewRowMutation(rowkey);
  if (mutation == NULL) {
    SendErrorJ(env, jobj, "failed to new mutation: ");
    return 0;
  }
  jlong jmutation_ptr = reinterpret_cast<jlong>(mutation);
  return jmutation_ptr;
}

void MutationCallBack(tera::RowMutation* mutation) {
  const tera::ErrorCode& error_code = mutation->GetError();
  if (error_code.GetType() != tera::ErrorCode::kOK) {
    LOG(ERROR) << "exception occured, reason:" << error_code.GetReason();
  }

  delete mutation;
}

JNIEXPORT jboolean
NativeApplyMutation(JNIEnv* env, jobject jobj, jlong jtable_ptr, jlong jmutation_ptr) {
  tera::Table* table = reinterpret_cast<tera::Table*>(jtable_ptr);
  tera::RowMutation* mutation = reinterpret_cast<tera::RowMutation*>(jmutation_ptr);
  tera::ErrorCode error_code;

  if (table == NULL || mutation == NULL) {
    SendErrorJ(env, jobj, "table or mutation not initialized.");
    return JNI_FALSE;
  }
  if (mutation->MutationNum() <= 0) {
    return JNI_TRUE;
  }

  //    mutation->SetCallBack(MutationCallBack);
  table->ApplyMutation(mutation);
  return JNI_TRUE;
}

JNIEXPORT void NativeFlushCommits(JNIEnv* env, jobject jobj, jlong jtable_ptr) {
  tera::Table* table = reinterpret_cast<tera::Table*>(jtable_ptr);
  tera::ErrorCode error_code;

  if (table == NULL) {
    SendErrorJ(env, jobj, "table not initialized.");
    return;
  }

  while (!table->IsPutFinished()) {
    LOG(INFO) << "wainting for flush finished ..";
    usleep(100000);
  }
}

JNIEXPORT jlong NativeNewReader(JNIEnv* env, jobject jobj, jlong jtable_ptr, jbyteArray jrowkey) {
  tera::Table* table = reinterpret_cast<tera::Table*>(jtable_ptr);
  tera::ErrorCode error_code;
  std::string rowkey;

  if (table == NULL) {
    SendErrorJ(env, jobj, "table not initialized.");
    return 0;
  }
  JByteArrayToString(env, jrowkey, &rowkey);
  tera::RowReader* reader = table->NewRowReader(rowkey);
  if (reader == NULL) {
    SendErrorJ(env, jobj, "failed to new reader: ");
    return 0;
  }
  jlong jreader_ptr = reinterpret_cast<jlong>(reader);
  return jreader_ptr;
}

JNIEXPORT jlong NativeApplyReader(JNIEnv* env, jobject jobj, jlong jtable_ptr, jlong jreader_ptr) {
  tera::Table* table = reinterpret_cast<tera::Table*>(jtable_ptr);
  tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader_ptr);
  tera::ErrorCode error_code;

  if (table == NULL || reader == NULL) {
    SendErrorJ(env, jobj, "table or reader not initialized.");
    return 0;
  }
  table->Get(reader);

  return reinterpret_cast<jlong>(reader);
}

JNIEXPORT jlong NativeScan(JNIEnv* env, jobject jobj, jlong jtable, jlong jdesc) {
  tera::Table* table = reinterpret_cast<tera::Table*>(jtable);
  tera::ScanDescriptor* desc = reinterpret_cast<tera::ScanDescriptor*>(jdesc);
  tera::ErrorCode error_code;

  if (table == NULL || desc == NULL) {
    SendErrorJ(env, jobj, "table or scan descriptor not initialized.");
    return 0;
  }

  tera::ResultStream* result_stream;
  if ((result_stream = table->Scan(*desc, &error_code)) == NULL) {
    LOG(ERROR) << "fail to scan records from table: " << table->GetName();
    return 0;
  }

  return reinterpret_cast<jlong>(result_stream);
}
