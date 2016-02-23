// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLETNODE_CLIENT_ASYNC_H_
#define TERA_TABLETNODE_TABLETNODE_CLIENT_ASYNC_H_

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include "common/base/closure.h"

#include "proto/tabletnode_rpc.pb.h"
#include "proto/rpc_client.h"

DECLARE_int32(tera_rpc_timeout_period);

class ThreadPool;

namespace tera {
namespace tabletnode {

class TabletNodeClient : public RpcClient<TabletNodeServer::Stub> {
public:
    static void SetThreadPool(ThreadPool* thread_pool);

    static void SetRpcOption(int32_t max_inflow = -1, int32_t max_outflow = -1,
                             int32_t pending_buffer_size = -1,
                             int32_t thread_num = -1);

    TabletNodeClient(const std::string& addr = "",
                     int32_t rpc_timeout = FLAGS_tera_rpc_timeout_period);

    ~TabletNodeClient();

    bool LoadTablet(const LoadTabletRequest* request,
                    LoadTabletResponse* response,
                    Closure<void, LoadTabletRequest*, LoadTabletResponse*, bool, int>* done = NULL);

    bool UnloadTablet(const UnloadTabletRequest* request,
                      UnloadTabletResponse* response,
                      Closure<void, UnloadTabletRequest*, UnloadTabletResponse*, bool, int>* done = NULL);

    bool ReadTablet(const ReadTabletRequest* request,
                    ReadTabletResponse* response,
                    Closure<void, ReadTabletRequest*, ReadTabletResponse*, bool, int>* done = NULL);

    bool WriteTablet(const WriteTabletRequest* request,
                     WriteTabletResponse* response,
                     Closure<void, WriteTabletRequest*, WriteTabletResponse*, bool, int>* done = NULL);

    bool ScanTablet(const ScanTabletRequest* request,
                    ScanTabletResponse* response,
                    Closure<void, ScanTabletRequest*, ScanTabletResponse*, bool, int>* done = NULL);

    bool GetSnapshot(const SnapshotRequest* request,
                     SnapshotResponse* response,
                     Closure<void, SnapshotRequest*, SnapshotResponse*, bool, int>* done = NULL);
    bool ReleaseSnapshot(const ReleaseSnapshotRequest* request,
                         ReleaseSnapshotResponse* response,
                         Closure<void, ReleaseSnapshotRequest*, ReleaseSnapshotResponse*, bool, int>* done = NULL);
    bool Rollback(const SnapshotRollbackRequest* request,
                  SnapshotRollbackResponse* response,
                  Closure<void, SnapshotRollbackRequest*, SnapshotRollbackResponse*, bool, int>* done = NULL);


    bool Query(ThreadPool* thread_pool, const QueryRequest* request,
               QueryResponse* response,
               Closure<void, QueryRequest*, QueryResponse*, bool, int>* done = NULL);

    bool SplitTablet(const SplitTabletRequest* request,
                     SplitTabletResponse* response,
                     Closure<void, SplitTabletRequest*, SplitTabletResponse*, bool, int>* done = NULL);

    bool CompactTablet(const CompactTabletRequest* request,
                       CompactTabletResponse* response,
                       Closure<void, CompactTabletRequest*, CompactTabletResponse*, bool, int>* done = NULL);
    bool CmdCtrl(const TsCmdCtrlRequest* request,
                 TsCmdCtrlResponse* response,
                 Closure<void, TsCmdCtrlRequest*, TsCmdCtrlResponse*, bool, int>* done = NULL);

    bool Update(const UpdateRequest* request,
                      UpdateResponse* response,
                      Closure<void, UpdateRequest*, UpdateResponse*, bool, int>* done = NULL);

private:
    int32_t m_rpc_timeout;
    static ThreadPool* m_thread_pool;
};

} // namespace sdk
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_CLIENT_ASYNC_H_
