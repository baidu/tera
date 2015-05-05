// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/rpc_client.h"

namespace tera {

sofa::pbrpc::RpcChannelOptions RpcClientBase::m_channel_options;
std::map<std::string, sofa::pbrpc::RpcChannel*> RpcClientBase::m_rpc_channel_list;
sofa::pbrpc::RpcClientOptions RpcClientBase::m_rpc_client_options;
sofa::pbrpc::RpcClient RpcClientBase::m_rpc_client;
Mutex RpcClientBase::m_mutex;

} // namespace tera
