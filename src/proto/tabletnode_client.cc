// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: An Qin (qinan@baidu.com)

#include <gflags/gflags.h>

#include "proto/tabletnode_client.h"

DECLARE_string(tera_tabletnode_port);
DECLARE_int32(tera_tabletnode_connect_retry_times);
DECLARE_int32(tera_tabletnode_connect_retry_period);
DECLARE_int32(tera_tabletnode_connect_timeout_period);

namespace tera {
namespace tabletnode {


TabletNodeClient::TabletNodeClient()
    : RpcClient<TabletNodeServer::Stub>(
                "", FLAGS_tera_tabletnode_connect_retry_period,
                FLAGS_tera_tabletnode_connect_timeout_period,
                FLAGS_tera_tabletnode_connect_retry_times) {}

TabletNodeClient::TabletNodeClient(int32_t wait_time, int32_t rpc_timeout,
                                   int32_t retry_times)
    : RpcClient<TabletNodeServer::Stub>(
            "", wait_time, rpc_timeout, retry_times) {}


TabletNodeClient::~TabletNodeClient() {}

void TabletNodeClient::ResetTabletNodeClient(const std::string& server_addr) {
    ResetClient(server_addr);
}

bool TabletNodeClient::LoadTablet(const LoadTabletRequest* request,
                                  LoadTabletResponse* response) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::LoadTablet,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "LoadTablet");
}

bool TabletNodeClient::UnloadTablet(const UnloadTabletRequest* request,
                                    UnloadTabletResponse* response) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::UnloadTablet,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "UnloadTablet");
}

bool TabletNodeClient::ReadTablet(const ReadTabletRequest* request,
                                  ReadTabletResponse* response) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ReadTablet,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "ReadTablet");
}

bool TabletNodeClient::WriteTablet(const WriteTabletRequest* request,
                                   WriteTabletResponse* response) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::WriteTablet,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "WriteTablet");
}

bool TabletNodeClient::ScanTablet(const ScanTabletRequest* request,
                                  ScanTabletResponse* response) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ScanTablet,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "ScanTablet");
}

bool TabletNodeClient::Query(const QueryRequest* request,
                             QueryResponse* response) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::Query,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "Query");
}

bool TabletNodeClient::SplitTablet(const SplitTabletRequest* request,
                                   SplitTabletResponse* response) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::SplitTablet,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "SplitTablet");
}

bool TabletNodeClient::MergeTablet(const MergeTabletRequest* request,
                                   MergeTabletResponse* response) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::MergeTablet,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "MergeTablet");
}

bool TabletNodeClient::CompactTablet(const CompactTabletRequest* request,
                                     CompactTabletResponse* response) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::CompactTablet,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "CompactTablet");
}

bool TabletNodeClient::IsRetryStatus(const StatusCode& status) {
    return (status == kTabletNodeNotInited
            || status == kTabletNodeIsBusy
            || status == kTabletNodeIsIniting
            || status == kTabletOnLoad
            || status == kTabletOnSplit
            || status == kTabletUnLoading);
}

} // namespace tabletnode
} // namespace tera
