// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TSO_TSO_CLIENT_IMPL_H_
#define TERA_TSO_TSO_CLIENT_IMPL_H_

#include "common/counter.h"
#include "common/thread_pool.h"
#include "tso/tso.h"

namespace tera {

class GetTimestampRequest;
class GetTimestampResponse;

namespace tso {

class TsoClientImpl {
public:
    TsoClientImpl() {}
    ~TsoClientImpl() {}
    int64_t GetTimestamp();
    void GetTimestamp(std::function<void (int64_t)> callback);

private:
    void GetTimestampCallback(std::function<void (int64_t)> callback,
                              GetTimestampRequest* request,
                              GetTimestampResponse* response,
                              bool failed, int error_code);
    static common::Counter last_sequence_;
    static common::ThreadPool thread_pool_;
};

} // namespace tso
} // namespace tera

#endif // TERA_TSO_TSO_CLIENT_IMPL_H_
