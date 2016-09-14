// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_UTILS_NETWORK_UTILS_H_
#define TERA_UTILS_NETWORK_UTILS_H_

#include <string>

#include "sofa/pbrpc/pbrpc.h"

namespace tera {
namespace utils {

std::string GetRemoteAddress(google::protobuf::RpcController* controller);

} // namespace utils
} // namespace tera


#endif // TERA_UTILS_NETWORK_UTILS_H_
