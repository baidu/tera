// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/rpc_client.h"

namespace tera {

static RpcClientBase* default_rpc_client_base = NULL;
static bool default_rpc_client_base_is_init = false;
static Mutex default_rpc_client_base_mutex;

void InitDefaultRpcClientBase(ThreadPool* thread_pool, int32_t thread_num) {
    MutexLock l(&default_rpc_client_base_mutex);
    if (!default_rpc_client_base_is_init) {
        default_rpc_client_base = new RpcClientBase(thread_pool, thread_num);
        default_rpc_client_base_is_init = true;
    }
}

void SetDefaultRpcClientBaseOption(int32_t max_inflow, int32_t max_outflow,
                                   int32_t pending_buffer_size) {
    CHECK_NOTNULL(default_rpc_client_base);
    default_rpc_client_base->SetOption(max_inflow, max_outflow, pending_buffer_size);
}

RpcClientBase* GetDefaultRpcClientBase() {
    CHECK_NOTNULL(default_rpc_client_base);
    return default_rpc_client_base;
}

} // namespace tera
