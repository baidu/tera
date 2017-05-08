// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tso/tso_server_impl.h"

#include "proto/status_code.pb.h"

namespace tera {
namespace tso {

TsoServerImpl::TsoServerImpl() {}

TsoServerImpl::~TsoServerImpl() {}

void TsoServerImpl::GetTimestamp(google::protobuf::RpcController* controller,
                                 const GetTimestampRequest* request,
                                 GetTimestampResponse* response,
                                 google::protobuf::Closure* done) {
    uint32_t number = request->number();
    if (request->number() > 0) {
        int64_t ts = last_timestamp_.Add(request->number()) + 1 - number;
        response->set_start_timestamp(ts);
    }
    response->set_number(number);
    response->set_status(kTabletNodeOk);
    done->Run();
}

} // namespace tso
} // namespace tera
