// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_EXECUTOR_FAKE_ROWLOCK_CLIENT_H_
#define TERA_OBSERVER_EXECUTOR_FAKE_ROWLOCK_CLIENT_H_

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include "proto/rpc_client.h"
#include "sdk/rowlock_client.h"

namespace tera {
namespace observer {

class FakeRowlockClient : public RowlockClient {
public:
    FakeRowlockClient() : RowlockClient("127.0.0.1:22222") {};
    ~FakeRowlockClient() {}

    virtual bool TryLock(const RowlockRequest* request,
                         RowlockResponse* response,
                         std::function<void (const RowlockRequest*, RowlockResponse*, bool, int)> done = NULL) {
        response->set_lock_status(kLockSucc);
        if (done != NULL) {
            bool failed = true;
            int error_code = 0;
            done(request, response, failed, error_code);
        }
        return true;
    }

    virtual bool UnLock(const RowlockRequest* request,
                        RowlockResponse* response,
                        std::function<void (RowlockRequest*, RowlockResponse*, bool, int)> done = NULL) {
        response->set_lock_status(kLockSucc);

        return true;
    }
};

} // namespace observer
} // namespace tera
#endif  // TERA_OBSERVER_EXECUTOR_FAKE_ROWLOCK_CLIENT_H_


