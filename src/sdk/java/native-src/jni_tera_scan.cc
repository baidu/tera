// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:

#include "jni_tera_scan.h"
#include "jni_tera_common.h"

#include "tera.h"

#define NativeNewScanDesc JNICALL Java_com_baidu_tera_client_TeraScanImpl_nativeNewScanDesc
#define NativeAddFamily JNICALL Java_com_baidu_tera_client_TeraScanImpl_nativeAddFamily
#define NativeAddColumn JNICALL Java_com_baidu_tera_client_TeraScanImpl_nativeAddColumn
#define NativeDeleteScanDesc JNICALL Java_com_baidu_tera_client_TeraScanImpl_nativeDeleteScanDesc

JNIEXPORT jlong
NativeNewScanDesc(JNIEnv* env, jobject jobj, jbyteArray jstartkey, jbyteArray jendkey) {
  std::string startkey, endkey;
  JByteArrayToString(env, jstartkey, &startkey);
  JByteArrayToString(env, jendkey, &endkey);

  tera::ScanDescriptor* desc = new tera::ScanDescriptor(startkey);
  desc->SetEnd(endkey);

  jlong jdesc = reinterpret_cast<uint64_t>(desc);
  return jdesc;
}

JNIEXPORT jboolean NativeAddFamily(JNIEnv* env, jobject jobj, jlong jdesc, jbyteArray jfamily) {
  tera::ScanDescriptor* desc = reinterpret_cast<tera::ScanDescriptor*>(jdesc);
  if (desc == NULL) {
    std::string msg = "descriptor not initialized.";
    SendErrorJ(env, jobj, msg);
    return JNI_FALSE;
  }
  std::string family;
  JByteArrayToString(env, jfamily, &family);
  desc->AddColumnFamily(family);
  return JNI_TRUE;
}

JNIEXPORT jboolean
NativeAddColumn(JNIEnv* env, jobject jobj, jlong jdesc, jbyteArray jfamily, jbyteArray jcolumn) {
  tera::ScanDescriptor* desc = reinterpret_cast<tera::ScanDescriptor*>(jdesc);
  if (desc == NULL) {
    std::string msg = "descriptor not initialized.";
    SendErrorJ(env, jobj, msg);
    return false;
  }
  std::string family, column;
  JByteArrayToString(env, jfamily, &family);
  JByteArrayToString(env, jcolumn, &column);
  desc->AddColumn(family, column);
  return true;
}

JNIEXPORT void NativeDeleteScanDesc(JNIEnv* env, jobject jobj, jlong jdesc) {
  tera::ScanDescriptor* desc = reinterpret_cast<tera::ScanDescriptor*>(jdesc);
  if (desc != NULL) {
    delete desc;
  }
}
