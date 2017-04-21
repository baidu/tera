// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tso/tso_client_impl.h"

#include <gflags/gflags.h>

#include "common/event.h"
#include "tso/tso_rpc_client.h"

DEFINE_string(tera_tso_host, "127.0.0.1", "host of timestamp oracle");
DEFINE_string(tera_tso_port, "50000", "port of timestamp oracle");

DECLARE_string(tera_tso_host);
DECLARE_string(tera_tso_port);

using namespace std::placeholders;

namespace tera {
namespace tso {

common::Counter TsoClientImpl::last_sequence_;
common::ThreadPool TsoClientImpl::thread_pool_;

void TmpSetTimestamp(common::AutoResetEvent* event, int64_t* out, int64_t in) {
    *out = in;
    event->Set();
}

int64_t TsoClientImpl::GetTimestamp() {
    common::AutoResetEvent event;
    int64_t start_timestamp = 0;
    GetTimestamp(std::bind(&TmpSetTimestamp, &event, &start_timestamp, _1));
    event.Wait();
    return start_timestamp;
}

void TsoClientImpl::GetTimestamp(std::function<void (int64_t)> callback) {
    TsoRpcClient rpc_client(FLAGS_tera_tso_host + ":" + FLAGS_tera_tso_port, &thread_pool_);
    GetTimestampRequest* request = new GetTimestampRequest;
    request->set_sequence_id(last_sequence_.Inc());
    request->set_number(1);
    GetTimestampResponse* response = new GetTimestampResponse;
    Closure<void, GetTimestampRequest*, GetTimestampResponse*, bool, int>* closure =
        NewClosure(this, &TsoClientImpl::GetTimestampCallback, callback);
    rpc_client.GetTimestamp(request, response, closure);
}

void TsoClientImpl::GetTimestampCallback(std::function<void (int64_t)> callback,
                                         GetTimestampRequest* request,
                                         GetTimestampResponse* response,
                                         bool failed, int error_code) {
    if (failed) {
        response->set_status(kRPCError);
    }

    int64_t start_timestamp = 0;
    if (response->status() != kTabletNodeOk) {
        start_timestamp = -1;
    } else if (response->number() != 1) {
        start_timestamp = -2;
    } else {
        start_timestamp = response->start_timestamp();
    }

    delete request;
    delete response;
    callback(start_timestamp);
}

} // namespace tso
} // namespace tera
