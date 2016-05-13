// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>

#include "proto/proto_helper.h"
#include "proto/master_rpc.pb.h"

namespace tera {

std::string StatusCodeToString(int32_t status) {
    return StatusCode_Name(static_cast<StatusCode>(status));
}

std::string StatusCodeToString(TabletNodeStatus status) {
    return TabletNodeStatus_Name(status);
}
} // namespace tera
