// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/rpc_client.h"

namespace tera {

sofa::pbrpc::RpcChannelOptions RpcClientBase::channel_options_;
std::map<std::string, sofa::pbrpc::RpcChannel*> RpcClientBase::rpc_channel_list_;
sofa::pbrpc::RpcClientOptions RpcClientBase::rpc_client_options_;
sofa::pbrpc::RpcClient RpcClientBase::rpc_client_;
Mutex RpcClientBase::mutex_;

}  // namespace tera
