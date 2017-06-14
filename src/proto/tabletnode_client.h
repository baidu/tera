// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLETNODE_CLIENT_ASYNC_H_
#define TERA_TABLETNODE_TABLETNODE_CLIENT_ASYNC_H_

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

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
                    std::function<void (LoadTabletRequest*, LoadTabletResponse*, bool, int)> done = NULL);

    bool UnloadTablet(const UnloadTabletRequest* request,
                      UnloadTabletResponse* response,
                      std::function<void (UnloadTabletRequest*, UnloadTabletResponse*, bool, int)> done = NULL);

    bool ReadTablet(const ReadTabletRequest* request,
                    ReadTabletResponse* response,
                    std::function<void (ReadTabletRequest*, ReadTabletResponse*, bool, int)> done = NULL);

    bool WriteTablet(const WriteTabletRequest* request,
                     WriteTabletResponse* response,
                     std::function<void (WriteTabletRequest*, WriteTabletResponse*, bool, int)> done = NULL);

    bool ScanTablet(const ScanTabletRequest* request,
                    ScanTabletResponse* response,
                    std::function<void (ScanTabletRequest*, ScanTabletResponse*, bool, int)> done = NULL);

    bool GetSnapshot(const SnapshotRequest* request,
                     SnapshotResponse* response,
                     std::function<void (SnapshotRequest*, SnapshotResponse*, bool, int)> done = NULL);
    bool ReleaseSnapshot(const ReleaseSnapshotRequest* request,
                         ReleaseSnapshotResponse* response,
                         std::function<void (ReleaseSnapshotRequest*, ReleaseSnapshotResponse*, bool, int)> done = NULL);
    bool Rollback(const SnapshotRollbackRequest* request,
                  SnapshotRollbackResponse* response,
                  std::function<void (SnapshotRollbackRequest*, SnapshotRollbackResponse*, bool, int)> done = NULL);


    bool Query(ThreadPool* thread_pool, const QueryRequest* request,
               QueryResponse* response,
               std::function<void (QueryRequest*, QueryResponse*, bool, int)> done = NULL);

    bool SplitTablet(const SplitTabletRequest* request,
                     SplitTabletResponse* response,
                     std::function<void (SplitTabletRequest*, SplitTabletResponse*, bool, int)> done = NULL);

    bool CompactTablet(const CompactTabletRequest* request,
                       CompactTabletResponse* response,
                       std::function<void (CompactTabletRequest*, CompactTabletResponse*, bool, int)> done = NULL);
    bool CmdCtrl(const TsCmdCtrlRequest* request,
                 TsCmdCtrlResponse* response,
                 std::function<void (TsCmdCtrlRequest*, TsCmdCtrlResponse*, bool, int)> done = NULL);

    bool Update(const UpdateRequest* request,
                      UpdateResponse* response,
                      std::function<void (UpdateRequest*, UpdateResponse*, bool, int)> done = NULL);

private:
    int32_t rpc_timeout_;
    static ThreadPool* thread_pool_;
};

} // namespace sdk
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_CLIENT_ASYNC_H_
