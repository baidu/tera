// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:
//

#ifndef _JAVATERA_NATIVE_SRC_JNI_TERA_COMMON_H_
#define _JAVATERA_NATIVE_SRC_JNI_TERA_COMMON_H_

#include <jni.h>

#include <map>
#include <string>

#include "sdk/tera.h"

#pragma GCC visibility push(default)

void SendErrorJ(JNIEnv *env, jobject jobj, std::string msg);

void InitFlags(std::string confpath);

void InitGlog(std::string prefix);

std::string ConvertDescToString(tera::TableDescriptor* desc);

void JByteArrayToString(JNIEnv *env, jbyteArray& jbarray, std::string* str);

void StringToJByteArray(JNIEnv *env, const std::string& str, jbyteArray* jbarray);

#pragma GCC visibility pop

#endif // _JAVATERA_NATIVE_SRC_JNI_TERA_COMMON_H_
