// Copyright (C) 2014, Baidu Inc.
// Author: Xu Peilin (xupeilin@baidu.com)
//
// Description:
//

#include "jni_tera_common.h"

#include "common/file/file_path.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "utils/utils_cmd.h"

DECLARE_string(flagfile);
DECLARE_string(log_dir);

DECLARE_string(tera_user_identity);
DECLARE_string(tera_user_passcode);

void SendErrorJ(JNIEnv *env, jobject jobj, std::string msg) {
    jclass jc = env->GetObjectClass(jobj);
    jstring jmsg = env->NewStringUTF(msg.c_str());
    env->SetObjectField(jobj,
                        env->GetFieldID(jc, "nativeMsg", "Ljava/lang/String;"),
                        jmsg);
    VLOG(10) << msg;
}

void InitFlags(std::string confpath) {
    // init FLAGS_flagfile
    if (IsExist(confpath)) {
        FLAGS_flagfile = confpath;
    } else if (IsExist("./tera.flag")) {
        FLAGS_flagfile = "./tera.flag";
    } else {
        LOG(FATAL) << "tera.flag not exist, job failed.";
    }

    // init user identity & role
    std::string cur_identity = tera::utils::GetValueFromEnv("USER");
    if (cur_identity.empty()) {
        cur_identity = "other";
    }
    if (FLAGS_tera_user_identity.empty()) {
        FLAGS_tera_user_identity = cur_identity;
    }

    // init log dir
    if (FLAGS_log_dir.empty()) {
        FLAGS_log_dir = "./";
    }
    ::google::ReadFromFlagsFile(FLAGS_flagfile, NULL, true);
    LOG(INFO) << "USER = " << FLAGS_tera_user_identity;
    LOG(INFO) << "Load config file: " << FLAGS_flagfile;
}

void InitGlog(std::string prefix) {
    ::google::InitGoogleLogging(prefix.c_str());
}

std::string ConvertDescToString(tera::TableDescriptor* desc) {
    return "desc";

}

void JByteArrayToString(JNIEnv *env, jbyteArray& jbarray, std::string* str) {
    const char* str_ptr =
        reinterpret_cast<const char*>(env->GetByteArrayElements(jbarray, 0));
    size_t str_len =
        static_cast<size_t>(env->GetArrayLength(jbarray));
    str->assign(str_ptr, str_len);
}

void StringToJByteArray(JNIEnv *env, const std::string& str, jbyteArray* jbarray) {
    size_t str_len = str.size();
    *jbarray = env->NewByteArray(str_len);
    jbyte* buffer = env->GetByteArrayElements(*jbarray, 0);
    memcpy(buffer, str.data(), str_len);
    env->SetByteArrayRegion(*jbarray, 0, str_len, buffer);
}
