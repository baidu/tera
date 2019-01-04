// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:

#include "jni_tera_reader.h"
#include "jni_tera_common.h"

#include "tera.h"

#define NativeAddColumn JNICALL Java_com_baidu_tera_client_TeraReaderImpl_nativeAddColumn
#define NativeAddFamily JNICALL Java_com_baidu_tera_client_TeraReaderImpl_nativeAddFamily
#define NativeDeleteReader JNICALL Java_com_baidu_tera_client_TeraReaderImpl_nativeDeleteReader

JNIEXPORT jboolean
NativeAddColumn(JNIEnv* env, jobject jobj, jlong jreader, jbyteArray jfamily, jbyteArray jcolumn) {
  tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);
  if (reader == NULL) {
    std::string msg = "reader not initialized.";
    SendErrorJ(env, jobj, msg);
    return JNI_FALSE;
  }
  std::string family, column;
  JByteArrayToString(env, jfamily, &family);
  JByteArrayToString(env, jcolumn, &column);
  reader->AddColumn(family, column);
  return JNI_TRUE;
}

JNIEXPORT jboolean NativeAddFamily(JNIEnv* env, jobject jobj, jlong jreader, jbyteArray jfamily) {
  tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);
  if (reader == NULL) {
    std::string msg = "reader not initialized.";
    SendErrorJ(env, jobj, msg);
    return JNI_FALSE;
  }
  std::string family;
  JByteArrayToString(env, jfamily, &family);
  reader->AddColumnFamily(family);
  return JNI_TRUE;
}

JNIEXPORT jboolean NativeDeleteReader(JNIEnv* env, jobject jobj, jlong jreader) {
  tera::RowReader* reader = reinterpret_cast<tera::RowReader*>(jreader);

  if (reader != NULL) {
    delete reader;
  }
  return JNI_TRUE;
}
