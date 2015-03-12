// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gflags/gflags.h>

#include "tera/tabletnode_client_sdk.h"

DECLARE_string(tera_tabletnode_addr);
DECLARE_string(tera_tabletnode_port);
DECLARE_int32(tera_tabletnode_connect_retry_times);
DECLARE_int32(tera_tabletnode_connect_retry_period);
DECLARE_int32(tera_tabletnode_connect_timeout_period);

namespace tera {
namespace tabletnode {


TabletNodeClientSDK::TabletNodeClientSDK(sofa::pbrpc::RpcClient* pbrpc_client)
    : RpcClientSDK<TabletNodeServer::Stub>(pbrpc_client,
                FLAGS_tera_tabletnode_addr + ":" + FLAGS_tera_tabletnode_port,
                FLAGS_tera_tabletnode_connect_retry_period,
                FLAGS_tera_tabletnode_connect_timeout_period,
                FLAGS_tera_tabletnode_connect_retry_times) {}

TabletNodeClientSDK::~TabletNodeClientSDK() {}

void TabletNodeClientSDK::ResetTabletNodeClient(const std::string& server_addr) {
    ResetClient(server_addr);
}

bool TabletNodeClientSDK::LoadTablet(const LoadTabletRequest* request,
                                  LoadTabletResponse* response,
                                  google::protobuf::Closure* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::LoadTablet,
                                request, response, done,
                                "LoadTablet");
}

bool TabletNodeClientSDK::UnloadTablet(const UnloadTabletRequest* request,
                                    UnloadTabletResponse* response,
                                    google::protobuf::Closure* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::UnloadTablet,
                                request, response, done,
                                "UnloadTablet");
}

bool TabletNodeClientSDK::ReadTablet(const ReadTabletRequest* request,
                                  ReadTabletResponse* response,
                                  google::protobuf::Closure* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ReadTablet,
                                request, response, done,
                                "ReadTablet");
}

bool TabletNodeClientSDK::WriteTablet(const WriteTabletRequest* request,
                                   WriteTabletResponse* response,
                                   google::protobuf::Closure* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::WriteTablet,
                                request, response, done,
                                "WriteTablet");
}

bool TabletNodeClientSDK::ScanTablet(const ScanTabletRequest* request,
                                  ScanTabletResponse* response,
                                  google::protobuf::Closure* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ScanTablet,
                                request, response, done,
                                "ScanTablet");
}

bool TabletNodeClientSDK::Query(const QueryRequest* request,
                                QueryResponse* response,
                                google::protobuf::Closure* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::Query,
                                request, response, done,
                                "Query");
}

bool TabletNodeClientSDK::SplitTablet(const SplitTabletRequest* request,
                                   SplitTabletResponse* response,
                                   google::protobuf::Closure* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::SplitTablet,
                                request, response, done,
                                "SplitTablet");
}

bool TabletNodeClientSDK::IsRetryStatus(const StatusCode& status) {
    return (status == kTabletNodeNotInited
            || status == kTabletNodeIsBusy);
}

} // namespace tabletnode
} // namespace tera
