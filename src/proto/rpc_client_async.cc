// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/rpc_client_async.h"

namespace tera {

sofa::pbrpc::RpcChannelOptions RpcClientAsyncBase::m_channel_options;
std::map<std::string, sofa::pbrpc::RpcChannel*> RpcClientAsyncBase::m_rpc_channel_list;
sofa::pbrpc::RpcClientOptions RpcClientAsyncBase::m_rpc_client_options;
sofa::pbrpc::RpcClient RpcClientAsyncBase::m_rpc_client;
Mutex RpcClientAsyncBase::m_mutex;

} // namespace tera
