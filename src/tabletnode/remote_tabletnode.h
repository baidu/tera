// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_REMOTE_TABLETNODE_H_
#define TERA_TABLETNODE_REMOTE_TABLETNODE_H_

#include "common/base/scoped_ptr.h"
#include "common/thread_pool.h"

#include "proto/tabletnode_rpc.pb.h"
#include "tabletnode/rpc_schedule.h"
#include "utils/rpc_timer_list.h"

namespace tera {
namespace tabletnode {

class TabletNodeImpl;

class RemoteTabletNode : public TabletNodeServer {
public:
    explicit RemoteTabletNode(TabletNodeImpl* tabletnode_impl);
    ~RemoteTabletNode();

    void LoadTablet(google::protobuf::RpcController* controller,
                    const LoadTabletRequest* request,
                    LoadTabletResponse* response,
                    google::protobuf::Closure* done);

    void UnloadTablet(google::protobuf::RpcController* controller,
                      const UnloadTabletRequest* request,
                      UnloadTabletResponse* response,
                      google::protobuf::Closure* done);

    void ReadTablet(google::protobuf::RpcController* controller,
                    const ReadTabletRequest* request,
                    ReadTabletResponse* response,
                    google::protobuf::Closure* done);

    void WriteTablet(google::protobuf::RpcController* controller,
                     const WriteTabletRequest* request,
                     WriteTabletResponse* response,
                     google::protobuf::Closure* done);

    void ScanTablet(google::protobuf::RpcController* controller,
                    const ScanTabletRequest* request,
                    ScanTabletResponse* response,
                    google::protobuf::Closure* done);

    void GetSnapshot(google::protobuf::RpcController* controller,
                     const SnapshotRequest* request,
                     SnapshotResponse* response,
                     google::protobuf::Closure* done);

    void ReleaseSnapshot(google::protobuf::RpcController* controller,
                         const ReleaseSnapshotRequest* request,
                         ReleaseSnapshotResponse* response,
                         google::protobuf::Closure* done);

    void Rollback(google::protobuf::RpcController* controller,
                  const SnapshotRollbackRequest* request,
                  SnapshotRollbackResponse* response,
                  google::protobuf::Closure* done);

    void Query(google::protobuf::RpcController* controller,
               const QueryRequest* request,
               QueryResponse* response,
               google::protobuf::Closure* done);

    void SplitTablet(google::protobuf::RpcController* controller,
                     const SplitTabletRequest* request,
                     SplitTabletResponse* response,
                     google::protobuf::Closure* done);

    void CompactTablet(google::protobuf::RpcController* controller,
                       const CompactTabletRequest* request,
                       CompactTabletResponse* response,
                       google::protobuf::Closure* done);

    void CmdCtrl(google::protobuf::RpcController* controller,
                 const TsCmdCtrlRequest* request,
                 TsCmdCtrlResponse* response,
                 google::protobuf::Closure* done);

    void Update(google::protobuf::RpcController* controller,
                const UpdateRequest* request,
                UpdateResponse* response,
                google::protobuf::Closure* done);
    std::string ProfilingLog();
private:
    void DoLoadTablet(google::protobuf::RpcController* controller,
                      const LoadTabletRequest* request,
                      LoadTabletResponse* response,
                      google::protobuf::Closure* done);

    void DoUnloadTablet(google::protobuf::RpcController* controller,
                        const UnloadTabletRequest* request,
                        UnloadTabletResponse* response,
                        google::protobuf::Closure* done);

    void DoReadTablet(google::protobuf::RpcController* controller,
                      int64_t start_micros,
                      const ReadTabletRequest* request,
                      ReadTabletResponse* response,
                      google::protobuf::Closure* done,
                      ReadRpcTimer* timer = NULL);

    void DoWriteTablet(google::protobuf::RpcController* controller,
                       const WriteTabletRequest* request,
                       WriteTabletResponse* response,
                       google::protobuf::Closure* done,
                       WriteRpcTimer* timer = NULL);

    void DoGetSnapshot(google::protobuf::RpcController* controller,
                       const SnapshotRequest* request,
                       SnapshotResponse* response,
                       google::protobuf::Closure* done);

    void DoReleaseSnapshot(google::protobuf::RpcController* controller,
                           const ReleaseSnapshotRequest* request,
                           ReleaseSnapshotResponse* response,
                           google::protobuf::Closure* done);

    void DoRollback(google::protobuf::RpcController* controller,
                    const SnapshotRollbackRequest* request,
                    SnapshotRollbackResponse* response,
                    google::protobuf::Closure* done);

    void DoQuery(google::protobuf::RpcController* controller,
                 const QueryRequest* request, QueryResponse* response,
                 google::protobuf::Closure* done);

    void DoScanTablet(google::protobuf::RpcController* controller,
                      const ScanTabletRequest* request,
                      ScanTabletResponse* response,
                      google::protobuf::Closure* done);

    void DoSplitTablet(google::protobuf::RpcController* controller,
                       const SplitTabletRequest* request,
                       SplitTabletResponse* response,
                       google::protobuf::Closure* done);

    void DoMergeTablet(google::protobuf::RpcController* controller,
                       const MergeTabletRequest* request,
                       MergeTabletResponse* response,
                       google::protobuf::Closure* done);

    void DoCompactTablet(google::protobuf::RpcController* controller,
                         const CompactTabletRequest* request,
                         CompactTabletResponse* response,
                         google::protobuf::Closure* done);

    void DoCmdCtrl(google::protobuf::RpcController* controller,
                   const TsCmdCtrlRequest* request,
                   TsCmdCtrlResponse* response,
                   google::protobuf::Closure* done);

    void DoUpdate(google::protobuf::RpcController* controller,
                  const UpdateRequest* request,
                  UpdateResponse* response,
                  google::protobuf::Closure* done);
    void DoScheduleRpc(RpcSchedule* rpc_schedule);

private:
    TabletNodeImpl* m_tabletnode_impl;
    scoped_ptr<ThreadPool> m_ctrl_thread_pool;
    scoped_ptr<ThreadPool> m_write_thread_pool;
    scoped_ptr<ThreadPool> m_read_thread_pool;
    scoped_ptr<ThreadPool> m_scan_thread_pool;
    scoped_ptr<ThreadPool> m_compact_thread_pool;
    scoped_ptr<RpcSchedule> m_read_rpc_schedule;
    scoped_ptr<RpcSchedule> m_scan_rpc_schedule;
};

} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_REMOTE_TABLETNODE_H_
