// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_UTILS_UTILS_CMD_H_
#define TERA_UTILS_UTILS_CMD_H_

#include <map>
#include <string>

#include "leveldb/env.h"

namespace tera {
namespace utils {

std::string GetBinaryLocationDir();

std::string GetCurrentLocationDir();

std::string GetValueFromEnv(const std::string& env_name);

std::string ConvertByteToString(const uint64_t size);

std::string GetLocalHostAddr();

std::string GetLocalHostName();

bool ExecuteShellCmd(const std::string cmd,
                     std::string* ret_str = NULL);

void SetupLog(const std::string& program_name);

} // namespace utils
} // namespace tera

#endif // TERA_UTILS_UTILS_CMD_H_
