// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TSO_TSO_SERVER_IMPL_H_
#define TERA_TSO_TSO_SERVER_IMPL_H_

#include "common/counter.h"

#include "proto/tso.pb.h"

namespace tera {
namespace tso {

class TsoServerImpl : public TimestampOracleServer {
public:
    TsoServerImpl();
    virtual ~TsoServerImpl();

    virtual void GetTimestamp(google::protobuf::RpcController* controller,
                              const GetTimestampRequest* request,
                              GetTimestampResponse* response,
                              google::protobuf::Closure* done);

private:
    common::Counter last_timestamp_;
};

} // namespace tso
} // namespace tera

#endif // TERA_TSO_TSO_SERVER_IMPL_H_
