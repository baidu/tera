// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_UTILS_CONFIG_UTILS_H_
#define TERA_UTILS_CONFIG_UTILS_H_

#include <string>

namespace tera {
namespace utils {

// `file' should be path/to/file, like "../conf/tera.flag"
bool LoadFlagFile(const std::string& file);

} // namespace utils
} // namespace tera

#endif // TERA_UTILS_CONFIG_UTILS_H_
