// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "utils/network_utils.h"

namespace tera {
namespace utils {

std::string GetRemoteAddress(google::protobuf::RpcController* controller) {
  assert(controller != NULL);
  sofa::pbrpc::RpcController* cntl = static_cast<sofa::pbrpc::RpcController*>(controller);
  return cntl->RemoteAddress();
}

}  // namespace utils
}  // namepsace tera
