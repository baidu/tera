// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string>

#include <gflags/gflags.h>

#include "proto/master_client.h"


DECLARE_int32(tera_master_connect_retry_times);
DECLARE_int32(tera_master_connect_retry_period);
DECLARE_int32(tera_master_connect_timeout_period);

namespace tera {
namespace master {

MasterClient::MasterClient(const std::string& server_addr,
                           int32_t rpc_timeout)
    : RpcClient<MasterServer::Stub>(server_addr),
      m_rpc_timeout(rpc_timeout) {}

MasterClient::~MasterClient() {}

bool MasterClient::GetSnapshot(const GetSnapshotRequest* request,
                               GetSnapshotResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::GetSnapshot,
                                request, response,
                                (Closure<void, GetSnapshotRequest*, GetSnapshotResponse*, bool, int>*)NULL,
                                "GetSnapshot", m_rpc_timeout);
}

bool MasterClient::DelSnapshot(const DelSnapshotRequest* request,
                               DelSnapshotResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::DelSnapshot,
                                request, response,
                                (Closure<void, DelSnapshotRequest*, DelSnapshotResponse*, bool, int>*)NULL,
                                "DelSnapshot", m_rpc_timeout);
}

bool MasterClient::Rollback(const RollbackRequest* request,
                            RollbackResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::Rollback,
                                request, response,
                                (Closure<void, RollbackRequest*, RollbackResponse*, bool, int>*)NULL,
                                "Rollback", m_rpc_timeout);
}

bool MasterClient::CreateTable(const CreateTableRequest* request,
                               CreateTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::CreateTable,
                                request, response,
                                (Closure<void, CreateTableRequest*, CreateTableResponse*, bool, int>*)NULL,
                                "CreateTable", m_rpc_timeout);
}

bool MasterClient::DeleteTable(const DeleteTableRequest* request,
                               DeleteTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::DeleteTable,
                                request, response,
                                (Closure<void, DeleteTableRequest*, DeleteTableResponse*, bool, int>*)NULL,
                                "DeleteTable", m_rpc_timeout);
}

bool MasterClient::DisableTable(const DisableTableRequest* request,
                                DisableTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::DisableTable,
                                request, response,
                                (Closure<void, DisableTableRequest*, DisableTableResponse*, bool, int>*)NULL,
                                "DisableTable", m_rpc_timeout);
}

bool MasterClient::EnableTable(const EnableTableRequest* request,
                               EnableTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::EnableTable,
                                request, response,
                                (Closure<void, EnableTableRequest*, EnableTableResponse*, bool, int>*)NULL,
                                "EnableTable", m_rpc_timeout);
}

bool MasterClient::UpdateTable(const UpdateTableRequest* request,
                               UpdateTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::UpdateTable,
                                request, response,
                                (Closure<void, UpdateTableRequest*, UpdateTableResponse*, bool, int>*)NULL,
                                "UpdateTable", m_rpc_timeout);
}

bool MasterClient::SearchTable(const SearchTableRequest* request,
                               SearchTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::SearchTable,
                                request, response,
                                (Closure<void, SearchTableRequest*, SearchTableResponse*, bool, int>*)NULL,
                                "SearchTable", m_rpc_timeout);
}

bool MasterClient::CompactTable(const CompactTableRequest* request,
                                CompactTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::CompactTable,
                                request, response,
                                (Closure<void, CompactTableRequest*, CompactTableResponse*, bool, int>*)NULL,
                                "CompactTable", m_rpc_timeout);
}

bool MasterClient::ShowTables(const ShowTablesRequest* request,
                              ShowTablesResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::ShowTables,
                                request, response,
                                (Closure<void, ShowTablesRequest*, ShowTablesResponse*, bool, int>*)NULL,
                                "ShowTables", m_rpc_timeout);
}

bool MasterClient::ShowTabletNodes(const ShowTabletNodesRequest* request,
                                   ShowTabletNodesResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::ShowTabletNodes,
                                request, response,
                                (Closure<void, ShowTabletNodesRequest*, ShowTabletNodesResponse*, bool, int>*)NULL,
                                "ShowTabletNodes", m_rpc_timeout);
}

bool MasterClient::Register(const RegisterRequest* request,
                            RegisterResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::Register,
                                request, response,
                                (Closure<void, RegisterRequest*, RegisterResponse*, bool, int>*)NULL,
                                "Register", m_rpc_timeout);
}

bool MasterClient::Report(const ReportRequest* request,
                          ReportResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::Report,
                                request, response,
                                (Closure<void, ReportRequest*, ReportResponse*, bool, int>*)NULL,
                                "Report", m_rpc_timeout);
}

bool MasterClient::CmdCtrl(const CmdCtrlRequest* request,
                           CmdCtrlResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::CmdCtrl,
                                request, response,
                                (Closure<void, CmdCtrlRequest*, CmdCtrlResponse*, bool, int>*)NULL,
                                "CmdCtrl", m_rpc_timeout);
}

bool MasterClient::RenameTable(const RenameTableRequest* request, 
                               RenameTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::RenameTable,
                                request, response,
                                (Closure<void, RenameTableRequest*, 
                                         RenameTableResponse*, bool, int>*)NULL,
                                "RenameTable", m_rpc_timeout);
}

} // namespace master
} // namespace tera
