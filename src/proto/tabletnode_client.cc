// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/tabletnode_client.h"

namespace tera {
namespace tabletnode {

ThreadPool* TabletNodeClient::m_thread_pool = NULL;

void TabletNodeClient::SetThreadPool(ThreadPool* thread_pool) {
    m_thread_pool = thread_pool;
}

void TabletNodeClient::SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                         int32_t pending_buffer_size, int32_t thread_num) {
    RpcClientBase::SetOption(max_inflow, max_outflow,
                             pending_buffer_size, thread_num);
}

TabletNodeClient::TabletNodeClient(const std::string& server_addr,
                                             int32_t rpc_timeout)
    : RpcClient<TabletNodeServer::Stub>(server_addr),
      m_rpc_timeout(rpc_timeout) {}

TabletNodeClient::~TabletNodeClient() {}

bool TabletNodeClient::LoadTablet(const LoadTabletRequest* request,
                                       LoadTabletResponse* response,
                                       Closure<void, LoadTabletRequest*, LoadTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::LoadTablet,
                                request, response, done, "LoadTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::UnloadTablet(const UnloadTabletRequest* request,
                                         UnloadTabletResponse* response,
                                         Closure<void, UnloadTabletRequest*, UnloadTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::UnloadTablet,
                                request, response, done, "UnloadTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::ReadTablet(const ReadTabletRequest* request,
                                       ReadTabletResponse* response,
                                       Closure<void, ReadTabletRequest*, ReadTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ReadTablet,
                                request, response, done, "ReadTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::WriteTablet(const WriteTabletRequest* request,
                                        WriteTabletResponse* response,
                                        Closure<void, WriteTabletRequest*, WriteTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::WriteTablet,
                                request, response, done, "WriteTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::ScanTablet(const ScanTabletRequest* request,
                                       ScanTabletResponse* response,
                                       Closure<void, ScanTabletRequest*, ScanTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ScanTablet,
                                request, response, done, "ScanTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::GetSnapshot(const SnapshotRequest* request,
                                        SnapshotResponse* response,
                                        Closure<void, SnapshotRequest*, SnapshotResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::GetSnapshot,
                                request, response, done, "GetSnapshot",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::ReleaseSnapshot(const ReleaseSnapshotRequest* request,
                                            ReleaseSnapshotResponse* response,
                                            Closure<void, ReleaseSnapshotRequest*, ReleaseSnapshotResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ReleaseSnapshot,
                                request, response, done, "ReleaseSnapshot",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::Rollback(const SnapshotRollbackRequest* request,
                                SnapshotRollbackResponse* response,
                                Closure<void, SnapshotRollbackRequest*, SnapshotRollbackResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::Rollback,
                                request, response, done, "Rollback",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::Query(const QueryRequest* request,
                                  QueryResponse* response,
                                  Closure<void, QueryRequest*, QueryResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::Query,
                                request, response, done, "Query",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::SplitTablet(const SplitTabletRequest* request,
                                        SplitTabletResponse* response,
                                        Closure<void, SplitTabletRequest*, SplitTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::SplitTablet,
                                request, response, done, "SplitTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClient::CompactTablet(const CompactTabletRequest* request,
                                          CompactTabletResponse* response,
                                          Closure<void, CompactTabletRequest*, CompactTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::CompactTablet,
                                request, response, done, "CompactTablet",
                                m_rpc_timeout, m_thread_pool);
}

} // namespace tabletnode
} // namespace tera
