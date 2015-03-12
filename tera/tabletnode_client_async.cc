// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/tabletnode_client_async.h"

namespace tera {
namespace tabletnode {

ThreadPool* TabletNodeClientAsync::m_thread_pool = NULL;

void TabletNodeClientAsync::SetThreadPool(ThreadPool* thread_pool) {
    m_thread_pool = thread_pool;
}

void TabletNodeClientAsync::SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                         int32_t pending_buffer_size, int32_t thread_num) {
    RpcClientAsyncBase::SetOption(max_inflow, max_outflow,
                                  pending_buffer_size, thread_num);
}

TabletNodeClientAsync::TabletNodeClientAsync(const std::string& server_addr,
                                             int32_t rpc_timeout)
    : RpcClientAsync<TabletNodeServer::Stub>(server_addr),
      m_rpc_timeout(rpc_timeout) {}

TabletNodeClientAsync::~TabletNodeClientAsync() {}

bool TabletNodeClientAsync::LoadTablet(const LoadTabletRequest* request,
                                       LoadTabletResponse* response,
                                       Closure<void, LoadTabletRequest*, LoadTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::LoadTablet,
                                request, response, done, "LoadTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::UnloadTablet(const UnloadTabletRequest* request,
                                         UnloadTabletResponse* response,
                                         Closure<void, UnloadTabletRequest*, UnloadTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::UnloadTablet,
                                request, response, done, "UnloadTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::ReadTablet(const ReadTabletRequest* request,
                                       ReadTabletResponse* response,
                                       Closure<void, ReadTabletRequest*, ReadTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ReadTablet,
                                request, response, done, "ReadTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::WriteTablet(const WriteTabletRequest* request,
                                        WriteTabletResponse* response,
                                        Closure<void, WriteTabletRequest*, WriteTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::WriteTablet,
                                request, response, done, "WriteTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::ScanTablet(const ScanTabletRequest* request,
                                       ScanTabletResponse* response,
                                       Closure<void, ScanTabletRequest*, ScanTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ScanTablet,
                                request, response, done, "ScanTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::GetSnapshot(const SnapshotRequest* request,
                                        SnapshotResponse* response,
                                        Closure<void, SnapshotRequest*, SnapshotResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::GetSnapshot,
                                request, response, done, "GetSnapshot",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::ReleaseSnapshot(const ReleaseSnapshotRequest* request,
                                            ReleaseSnapshotResponse* response,
                                            Closure<void, ReleaseSnapshotRequest*, ReleaseSnapshotResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::ReleaseSnapshot,
                                request, response, done, "ReleaseSnapshot",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::Query(const QueryRequest* request,
                                  QueryResponse* response,
                                  Closure<void, QueryRequest*, QueryResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::Query,
                                request, response, done, "Query",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::SplitTablet(const SplitTabletRequest* request,
                                        SplitTabletResponse* response,
                                        Closure<void, SplitTabletRequest*, SplitTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::SplitTablet,
                                request, response, done, "SplitTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::MergeTablet(const MergeTabletRequest* request,
                                        MergeTabletResponse* response,
                                        Closure<void, MergeTabletRequest*, MergeTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::MergeTablet,
                                request, response, done, "MergeTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::CompactTablet(const CompactTabletRequest* request,
                                          CompactTabletResponse* response,
                                          Closure<void, CompactTabletRequest*, CompactTabletResponse*, bool, int>* done) {
    return SendMessageWithRetry(&TabletNodeServer::Stub::CompactTablet,
                                request, response, done, "CompactTablet",
                                m_rpc_timeout, m_thread_pool);
}

bool TabletNodeClientAsync::IsRetryStatus(const StatusCode& status) {
    return (status == kTabletNodeNotInited
            || status == kTabletNodeIsBusy);
}

} // namespace tabletnode
} // namespace tera
