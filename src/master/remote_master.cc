// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/remote_master.h"

#include <functional>

#include "common/base/closure.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master/master_impl.h"
#include "utils/crypt.h"
#include "utils/network_utils.h"

DECLARE_int32(tera_master_thread_min_num);
DECLARE_int32(tera_master_thread_max_num);
DECLARE_string(tera_master_stat_table_name);

namespace tera {
namespace master {

RemoteMaster::RemoteMaster(MasterImpl* master_impl)
    : master_impl_(master_impl),
      thread_pool_(new ThreadPool(FLAGS_tera_master_thread_max_num)) {}

RemoteMaster::~RemoteMaster() {}

void RemoteMaster::GetSnapshot(google::protobuf::RpcController* controller,
                               const GetSnapshotRequest* request,
                               GetSnapshotResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (GetSnapshot): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoGetSnapshot, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::DelSnapshot(google::protobuf::RpcController* controller,
                               const DelSnapshotRequest* request,
                               DelSnapshotResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (DelSnapshot): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoDelSnapshot, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::GetRollback(google::protobuf::RpcController* controller,
                               const RollbackRequest* request,
                               RollbackResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (Rollback): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoRollback, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::CreateTable(google::protobuf::RpcController* controller,
                               const CreateTableRequest* request,
                               CreateTableResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (CreateTable): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoCreateTable, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::DeleteTable(google::protobuf::RpcController* controller,
                               const DeleteTableRequest* request,
                               DeleteTableResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (DeleteTable): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoDeleteTable, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::DisableTable(google::protobuf::RpcController* controller,
                                const DisableTableRequest* request,
                                DisableTableResponse* response,
                                google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (DisableTable): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoDisableTable, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::EnableTable(google::protobuf::RpcController* controller,
                               const EnableTableRequest* request,
                               EnableTableResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (EnableTable): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoEnableTable, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::UpdateTable(google::protobuf::RpcController* controller,
                               const UpdateTableRequest* request,
                               UpdateTableResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (UpdateTable): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoUpdateTable, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::UpdateCheck(google::protobuf::RpcController* controller,
                               const UpdateCheckRequest* request,
                               UpdateCheckResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (UpdateCheck): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoUpdateCheck, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::CompactTable(google::protobuf::RpcController* controller,
                                const CompactTableRequest* request,
                                CompactTableResponse* response,
                                google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (CompactTable): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoCompactTable, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::SearchTable(google::protobuf::RpcController* controller,
                               const SearchTableRequest* request,
                               SearchTableResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (SearchTable): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoSearchTable, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::ShowTables(google::protobuf::RpcController* controller,
                              const ShowTablesRequest* request,
                              ShowTablesResponse* response,
                              google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (ShowTables): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoShowTables, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::ShowTabletNodes(google::protobuf::RpcController* controller,
                                   const ShowTabletNodesRequest* request,
                                   ShowTabletNodesResponse* response,
                                   google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (ShowTabletNodes): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoShowTabletNodes, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::CmdCtrl(google::protobuf::RpcController* controller,
                           const CmdCtrlRequest* request,
                           CmdCtrlResponse* response,
                           google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (CmdCtrl): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoCmdCtrl, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::OperateUser(google::protobuf::RpcController* controller,
                               const OperateUserRequest* request,
                               OperateUserResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (OperateUser): " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteMaster::DoOperateUser, this, controller, request, response, done);
    thread_pool_->AddTask(callback);
}

void RemoteMaster::RenameTable(google::protobuf::RpcController* controller,
                               const RenameTableRequest* request,
                               RenameTableResponse* response,
                               google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (RenameTable): " << tera::utils::GetRemoteAddress(controller);
    master_impl_->RenameTable(request, response, done);
    LOG(INFO) << "finish RPC (RenameTable)";
}

// internal

void RemoteMaster::DoGetSnapshot(google::protobuf::RpcController* controller,
                                 const GetSnapshotRequest* request,
                                 GetSnapshotResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (GetSnapshot)";
    master_impl_->GetSnapshot(request, response, done);
    LOG(INFO) << "finish RPC (GetSnapshot)";
}

void RemoteMaster::DoDelSnapshot(google::protobuf::RpcController* controller,
                                 const DelSnapshotRequest* request,
                                 DelSnapshotResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (DelSnapshot)";
    master_impl_->DelSnapshot(request, response, done);
    LOG(INFO) << "finish RPC (DelSnapshot)";
}

void RemoteMaster::DoRollback(google::protobuf::RpcController* controller,
                             const RollbackRequest* request,
                             RollbackResponse* response,
                             google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (Rollback)";
    master_impl_->GetRollback(request, response, done);
    LOG(INFO) << "finish RPC (Rollback)";
}

void RemoteMaster::DoCreateTable(google::protobuf::RpcController* controller,
                                 const CreateTableRequest* request,
                                 CreateTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (CreateTable)";
    master_impl_->CreateTable(request, response, done);
    LOG(INFO) << "finish RPC (CreateTable)";
}

void RemoteMaster::DoDeleteTable(google::protobuf::RpcController* controller,
                                 const DeleteTableRequest* request,
                                 DeleteTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (DeleteTable)";
    master_impl_->DeleteTable(request, response, done);
    LOG(INFO) << "finish RPC (DeleteTable)";
}

void RemoteMaster::DoDisableTable(google::protobuf::RpcController* controller,
                                  const DisableTableRequest* request,
                                  DisableTableResponse* response,
                                  google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (DisableTable)";
    master_impl_->DisableTable(request, response, done);
    LOG(INFO) << "finish RPC (DisableTable)";
}

void RemoteMaster::DoEnableTable(google::protobuf::RpcController* controller,
                                 const EnableTableRequest* request,
                                 EnableTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (EnableTable)";
    master_impl_->EnableTable(request, response, done);
    LOG(INFO) << "finish RPC (EnableTable)";
}

void RemoteMaster::DoUpdateTable(google::protobuf::RpcController* controller,
                                 const UpdateTableRequest* request,
                                 UpdateTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (UpdateTable)";
    master_impl_->UpdateTable(request, response, done);
    LOG(INFO) << "finish RPC (UpdateTable)";
}

void RemoteMaster::DoUpdateCheck(google::protobuf::RpcController* controller,
                                 const UpdateCheckRequest* request,
                                 UpdateCheckResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (UpdateCheck)";
    master_impl_->UpdateCheck(request, response, done);
    LOG(INFO) << "finish RPC (UpdateCheck)";
}

void RemoteMaster::DoCompactTable(google::protobuf::RpcController* controller,
                                  const CompactTableRequest* request,
                                  CompactTableResponse* response,
                                  google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (CompactTable)";
    master_impl_->CompactTable(request, response, done);
    LOG(INFO) << "finish RPC (CompactTable)";
}

void RemoteMaster::DoSearchTable(google::protobuf::RpcController* controller,
                                 const SearchTableRequest* request,
                                 SearchTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (SearchTable)";
    master_impl_->SearchTable(request, response, done);
    LOG(INFO) << "finish RPC (SearchTable)";
}

void RemoteMaster::DoShowTables(google::protobuf::RpcController* controller,
                                const ShowTablesRequest* request,
                                ShowTablesResponse* response,
                                google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (ShowTables)";
    if (request->has_all_brief() && request->all_brief()) {
        master_impl_->ShowTablesBrief(request, response, done);
    } else {
        master_impl_->ShowTables(request, response, done);
    }
    LOG(INFO) << "finish RPC (ShowTables)";
}

void RemoteMaster::DoShowTabletNodes(google::protobuf::RpcController* controller,
                                     const ShowTabletNodesRequest* request,
                                     ShowTabletNodesResponse* response,
                                     google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (ShowTabletNodes)";
    master_impl_->ShowTabletNodes(request, response, done);
    LOG(INFO) << "finish RPC (ShowTabletNodes)";
}

void RemoteMaster::DoCmdCtrl(google::protobuf::RpcController* controller,
                             const CmdCtrlRequest* request,
                             CmdCtrlResponse* response,
                             google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (CmdCtrl)";
    master_impl_->CmdCtrl(request, response);
    LOG(INFO) << "finish RPC (CmdCtrl)";

    done->Run();
}

void RemoteMaster::DoOperateUser(google::protobuf::RpcController* controller,
                                 const OperateUserRequest* request,
                                 OperateUserResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "run RPC (OperateUser)";
    master_impl_->OperateUser(request, response, done);
    LOG(INFO) << "finish RPC (OperateUser)";
}

} // namespace master
} // namespace tera
