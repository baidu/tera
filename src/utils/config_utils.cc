// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "utils/config_utils.h"

#include "common/file/file_path.h"
#include "gflags/gflags.h"

namespace tera {
namespace utils {

bool LoadFlagFile(const std::string& file) {
    if (!IsExist(file)) {
        return false;
    }
    std::string flag = "--flagfile=" + file;
    int argc = 2;
    char** argv = new char*[3];
    argv[0] = const_cast<char*>("dummy");
    argv[1] = const_cast<char*>(flag.c_str());
    argv[2] = NULL;
    ::google::ParseCommandLineFlags(&argc, &argv, false);
    delete[] argv;
    return true;
}

} // namespace utils
} // namespace tera
