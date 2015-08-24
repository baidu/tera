// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/remote_master.h"

#include <boost/bind.hpp>

#include "common/base/closure.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master/master_impl.h"
#include "utils/crypt.h"

DECLARE_int32(tera_master_thread_min_num);
DECLARE_int32(tera_master_thread_max_num);
DECLARE_string(tera_master_stat_table_name);

namespace tera {
namespace master {

RemoteMaster::RemoteMaster(MasterImpl* master_impl)
    : m_master_impl(master_impl),
      m_thread_pool(new ThreadPool(FLAGS_tera_master_thread_max_num)) {}

RemoteMaster::~RemoteMaster() {}

void RemoteMaster::GetSnapshot(google::protobuf::RpcController* controller,
                               const GetSnapshotRequest* request,
                               GetSnapshotResponse* response,
                               google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoGetSnapshot, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::DelSnapshot(google::protobuf::RpcController* controller,
                               const DelSnapshotRequest* request,
                               DelSnapshotResponse* response,
                               google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoDelSnapshot, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::CreateTable(google::protobuf::RpcController* controller,
                               const CreateTableRequest* request,
                               CreateTableResponse* response,
                               google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoCreateTable, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::DeleteTable(google::protobuf::RpcController* controller,
                               const DeleteTableRequest* request,
                               DeleteTableResponse* response,
                               google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoDeleteTable, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::DisableTable(google::protobuf::RpcController* controller,
                                const DisableTableRequest* request,
                                DisableTableResponse* response,
                                google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoDisableTable, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::EnableTable(google::protobuf::RpcController* controller,
                               const EnableTableRequest* request,
                               EnableTableResponse* response,
                               google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoEnableTable, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::UpdateTable(google::protobuf::RpcController* controller,
                               const UpdateTableRequest* request,
                               UpdateTableResponse* response,
                               google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoUpdateTable, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::CompactTable(google::protobuf::RpcController* controller,
                                const CompactTableRequest* request,
                                CompactTableResponse* response,
                                google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoCompactTable, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::SearchTable(google::protobuf::RpcController* controller,
                               const SearchTableRequest* request,
                               SearchTableResponse* response,
                               google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoSearchTable, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::ShowTables(google::protobuf::RpcController* controller,
                              const ShowTablesRequest* request,
                              ShowTablesResponse* response,
                              google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoShowTables, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::ShowTabletNodes(google::protobuf::RpcController* controller,
                                   const ShowTabletNodesRequest* request,
                                   ShowTabletNodesResponse* response,
                                   google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoShowTabletNodes, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::CmdCtrl(google::protobuf::RpcController* controller,
                           const CmdCtrlRequest* request,
                           CmdCtrlResponse* response,
                           google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoCmdCtrl, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteMaster::OperateUser(google::protobuf::RpcController* controller,
                               const OperateUserRequest* request,
                               OperateUserResponse* response,
                               google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteMaster::DoOperateUser, this, controller,
                    request, response, done);
    m_thread_pool->AddTask(callback);
}

// internal

void RemoteMaster::DoGetSnapshot(google::protobuf::RpcController* controller,
                                 const GetSnapshotRequest* request,
                                 GetSnapshotResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (GetSnapshot)";
    m_master_impl->GetSnapshot(request, response, done);
    LOG(INFO) << "finish RPC (GetSnapshot)";
}

void RemoteMaster::DoDelSnapshot(google::protobuf::RpcController* controller,
                                 const DelSnapshotRequest* request,
                                 DelSnapshotResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (DelSnapshot)";
    m_master_impl->DelSnapshot(request, response, done);
    LOG(INFO) << "finish RPC (DelSnapshot)";
}

void RemoteMaster::DoCreateTable(google::protobuf::RpcController* controller,
                                 const CreateTableRequest* request,
                                 CreateTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (CreateTable)";
    m_master_impl->CreateTable(request, response, done);
    LOG(INFO) << "finish RPC (CreateTable)";
}

void RemoteMaster::DoDeleteTable(google::protobuf::RpcController* controller,
                                 const DeleteTableRequest* request,
                                 DeleteTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (DeleteTable)";
    m_master_impl->DeleteTable(request, response, done);
    LOG(INFO) << "finish RPC (DeleteTable)";
}

void RemoteMaster::DoDisableTable(google::protobuf::RpcController* controller,
                                  const DisableTableRequest* request,
                                  DisableTableResponse* response,
                                  google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (DisableTable)";
    m_master_impl->DisableTable(request, response, done);
    LOG(INFO) << "finish RPC (DisableTable)";
}

void RemoteMaster::DoEnableTable(google::protobuf::RpcController* controller,
                                 const EnableTableRequest* request,
                                 EnableTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (EnableTable)";
    m_master_impl->EnableTable(request, response, done);
    LOG(INFO) << "finish RPC (EnableTable)";
}

void RemoteMaster::DoUpdateTable(google::protobuf::RpcController* controller,
                                 const UpdateTableRequest* request,
                                 UpdateTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (UpdateTable)";
    m_master_impl->UpdateTable(request, response, done);
    LOG(INFO) << "finish RPC (UpdateTable)";
}

void RemoteMaster::DoCompactTable(google::protobuf::RpcController* controller,
                                  const CompactTableRequest* request,
                                  CompactTableResponse* response,
                                  google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (CompactTable)";
    m_master_impl->CompactTable(request, response, done);
    LOG(INFO) << "finish RPC (CompactTable)";
}

void RemoteMaster::DoSearchTable(google::protobuf::RpcController* controller,
                                 const SearchTableRequest* request,
                                 SearchTableResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (SearchTable)";
    m_master_impl->SearchTable(request, response, done);
    LOG(INFO) << "finish RPC (SearchTable)";
}

void RemoteMaster::DoShowTables(google::protobuf::RpcController* controller,
                                const ShowTablesRequest* request,
                                ShowTablesResponse* response,
                                google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (ShowTables)";
    m_master_impl->ShowTables(request, response, done);
    LOG(INFO) << "finish RPC (ShowTables)";
}

void RemoteMaster::DoShowTabletNodes(google::protobuf::RpcController* controller,
                                     const ShowTabletNodesRequest* request,
                                     ShowTabletNodesResponse* response,
                                     google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (ShowTabletNodes)";
    m_master_impl->ShowTabletNodes(request, response, done);
    LOG(INFO) << "finish RPC (ShowTabletNodes)";
}

void RemoteMaster::DoCmdCtrl(google::protobuf::RpcController* controller,
                             const CmdCtrlRequest* request,
                             CmdCtrlResponse* response,
                             google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (CmdCtrl)";
    m_master_impl->CmdCtrl(request, response);
    LOG(INFO) << "finish RPC (CmdCtrl)";

    done->Run();
}

void RemoteMaster::DoOperateUser(google::protobuf::RpcController* controller,
                                 const OperateUserRequest* request,
                                 OperateUserResponse* response,
                                 google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (OperateUser)";
    m_master_impl->OperateUser(request, response, done);
    LOG(INFO) << "finish RPC (OperateUser)";
}

} // namespace master
} // namespace tera
