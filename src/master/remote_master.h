// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_REMOTE_MASTER_H_
#define TERA_MASTER_REMOTE_MASTER_H_

#include "common/base/scoped_ptr.h"
#include "common/thread_pool.h"

#include "proto/master_rpc.pb.h"

namespace tera {
namespace master {

class MasterImpl;

class RemoteMaster : public MasterServer {
public:
    explicit RemoteMaster(MasterImpl* master_impl);
    ~RemoteMaster();

    void GetSnapshot(google::protobuf::RpcController* controller,
                     const GetSnapshotRequest* request,
                     GetSnapshotResponse* response,
                     google::protobuf::Closure* done);

    void DelSnapshot(google::protobuf::RpcController* controller,
                     const DelSnapshotRequest* request,
                     DelSnapshotResponse* response,
                     google::protobuf::Closure* done);

    void CreateTable(google::protobuf::RpcController* controller,
                     const CreateTableRequest* request,
                     CreateTableResponse* response,
                     google::protobuf::Closure* done);

    void DeleteTable(google::protobuf::RpcController* controller,
                     const DeleteTableRequest* request,
                     DeleteTableResponse* response,
                     google::protobuf::Closure* done);

    void DisableTable(google::protobuf::RpcController* controller,
                     const DisableTableRequest* request,
                     DisableTableResponse* response,
                     google::protobuf::Closure* done);

    void EnableTable(google::protobuf::RpcController* controller,
                     const EnableTableRequest* request,
                     EnableTableResponse* response,
                     google::protobuf::Closure* done);

    void UpdateTable(google::protobuf::RpcController* controller,
                     const UpdateTableRequest* request,
                     UpdateTableResponse* response,
                     google::protobuf::Closure* done);

    void CompactTable(google::protobuf::RpcController* controller,
                      const CompactTableRequest* request,
                      CompactTableResponse* response,
                      google::protobuf::Closure* done);

    void SearchTable(google::protobuf::RpcController* controller,
                     const SearchTableRequest* request,
                     SearchTableResponse* response,
                     google::protobuf::Closure* done);

    void ShowTables(google::protobuf::RpcController* controller,
                    const ShowTablesRequest* request,
                    ShowTablesResponse* response,
                    google::protobuf::Closure* done);

    void ShowTabletNodes(google::protobuf::RpcController* controller,
                         const ShowTabletNodesRequest* request,
                         ShowTabletNodesResponse* response,
                         google::protobuf::Closure* done);

    void CmdCtrl(google::protobuf::RpcController* controller,
                 const CmdCtrlRequest* request,
                 CmdCtrlResponse* response,
                 google::protobuf::Closure* done);

    void OperateUser(google::protobuf::RpcController* controller,
                     const OperateUserRequest* request,
                     OperateUserResponse* response,
                     google::protobuf::Closure* done);
private:
    void DoGetSnapshot(google::protobuf::RpcController* controller,
                       const GetSnapshotRequest* request,
                       GetSnapshotResponse* response,
                       google::protobuf::Closure* done);
    void DoDelSnapshot(google::protobuf::RpcController* controller,
                       const DelSnapshotRequest* request,
                       DelSnapshotResponse* response,
                       google::protobuf::Closure* done);
    void DoCreateTable(google::protobuf::RpcController* controller,
                       const CreateTableRequest* request,
                       CreateTableResponse* response,
                       google::protobuf::Closure* done);

    void DoDeleteTable(google::protobuf::RpcController* controller,
                       const DeleteTableRequest* request,
                       DeleteTableResponse* response,
                       google::protobuf::Closure* done);

    void DoDisableTable(google::protobuf::RpcController* controller,
                        const DisableTableRequest* request,
                        DisableTableResponse* response,
                        google::protobuf::Closure* done);

    void DoEnableTable(google::protobuf::RpcController* controller,
                       const EnableTableRequest* request,
                       EnableTableResponse* response,
                       google::protobuf::Closure* done);

    void DoUpdateTable(google::protobuf::RpcController* controller,
                       const UpdateTableRequest* request,
                       UpdateTableResponse* response,
                       google::protobuf::Closure* done);

    void DoCompactTable(google::protobuf::RpcController* controller,
                        const CompactTableRequest* request,
                        CompactTableResponse* response,
                        google::protobuf::Closure* done);

    void DoSearchTable(google::protobuf::RpcController* controller,
                       const SearchTableRequest* request,
                       SearchTableResponse* response,
                       google::protobuf::Closure* done);

    void DoShowTables(google::protobuf::RpcController* controller,
                      const ShowTablesRequest* request,
                      ShowTablesResponse* response,
                      google::protobuf::Closure* done);

    void DoShowTabletNodes(google::protobuf::RpcController* controller,
                           const ShowTabletNodesRequest* request,
                           ShowTabletNodesResponse* response,
                           google::protobuf::Closure* done);

    void DoCmdCtrl(google::protobuf::RpcController* controller,
                   const CmdCtrlRequest* request,
                   CmdCtrlResponse* response,
                   google::protobuf::Closure* done);

    void DoOperateUser(google::protobuf::RpcController* controller,
                       const OperateUserRequest* request,
                       OperateUserResponse* response,
                       google::protobuf::Closure* done);
private:
    MasterImpl* m_master_impl;
    scoped_ptr<ThreadPool> m_thread_pool;
};


} // namespace master
} // namespace tera

#endif // TERA_MASTER_REMOTE_MASTER_H_
