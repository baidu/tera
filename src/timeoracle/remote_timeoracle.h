// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TIMEORACLE_REMOTE_TIMEORACLE_H
#define TERA_TIMEORACLE_REMOTE_TIMEORACLE_H

#include <sofa/pbrpc/pbrpc.h>
#include "common/thread_pool.h"
#include "proto/timeoracle_rpc.pb.h"
#include "timeoracle/timeoracle.h"

namespace tera {
namespace timeoracle {

class ClosureGuard {
public:
    ClosureGuard(::google::protobuf::Closure* done) : done_(done) {
    }

    ~ClosureGuard() {
        if (done_) {
            done_->Run();
        }
    }

    ::google::protobuf::Closure* release() {
        auto done = done_;
        done_ = nullptr;
        return done;
    }

private:
    ClosureGuard(const ClosureGuard&) = delete;
private:
    ::google::protobuf::Closure* done_;
};

class RemoteTimeoracle : public TimeoracleServer {
public:
    RemoteTimeoracle(int64_t start_timestamp) : timeoracle_(start_timestamp) {
    }

    virtual void GetTimestamp(::google::protobuf::RpcController* controller,
                              const ::tera::GetTimestampRequest* request,
                              ::tera::GetTimestampResponse* response,
                              ::google::protobuf::Closure* done) {
        ClosureGuard    closure_guard(done);

        int64_t count = request->count();
        int64_t start_timestamp = timeoracle_.GetTimestamp(count);

        if (start_timestamp) {
            response->set_start_timestamp(start_timestamp);
            response->set_count(count);
            response->set_status(kTimeoracleOk);
        } else {
            response->set_status(kTimeoracleBusy);
        }
    }

    Timeoracle* GetTimeoracle() {
        return &timeoracle_;
    }

private:
    Timeoracle      timeoracle_;
};

} // namespace timeoracle
} // namespace tera

#endif // TERA_TIMEORACLE_REMOTE_TIMEORACLE_H
