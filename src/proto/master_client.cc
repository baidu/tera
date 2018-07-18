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
      rpc_timeout_(rpc_timeout) {}

MasterClient::~MasterClient() {}

bool MasterClient::CreateTable(const CreateTableRequest* request,
                               CreateTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::CreateTable,
                                request, response,
                                (std::function<void (CreateTableRequest*, CreateTableResponse*, bool, int)>)NULL,
                                "CreateTable", rpc_timeout_);
}

bool MasterClient::DeleteTable(const DeleteTableRequest* request,
                               DeleteTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::DeleteTable,
                                request, response,
                                (std::function<void (DeleteTableRequest*, DeleteTableResponse*, bool, int)>)NULL,
                                "DeleteTable", rpc_timeout_);
}

bool MasterClient::DisableTable(const DisableTableRequest* request,
                                DisableTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::DisableTable,
                                request, response,
                                (std::function<void (DisableTableRequest*, DisableTableResponse*, bool, int)>)NULL,
                                "DisableTable", rpc_timeout_);
}

bool MasterClient::EnableTable(const EnableTableRequest* request,
                               EnableTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::EnableTable,
                                request, response,
                                (std::function<void (EnableTableRequest*, EnableTableResponse*, bool, int)>)NULL,
                                "EnableTable", rpc_timeout_);
}

bool MasterClient::UpdateTable(const UpdateTableRequest* request,
                               UpdateTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::UpdateTable,
                                request, response,
                                (std::function<void (UpdateTableRequest*, UpdateTableResponse*, bool, int)>)NULL,
                                "UpdateTable", rpc_timeout_);
}

bool MasterClient::UpdateCheck(const UpdateCheckRequest* request,
                               UpdateCheckResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::UpdateCheck,
                                request, response,
                                (std::function<void (UpdateCheckRequest*, UpdateCheckResponse*, bool, int)>)NULL,
                                "UpdateCheck", rpc_timeout_);
}

bool MasterClient::SearchTable(const SearchTableRequest* request,
                               SearchTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::SearchTable,
                                request, response,
                                (std::function<void (SearchTableRequest*, SearchTableResponse*, bool, int)>)NULL,
                                "SearchTable", rpc_timeout_);
}

bool MasterClient::ShowTables(const ShowTablesRequest* request,
                              ShowTablesResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::ShowTables,
                                request, response,
                                (std::function<void (ShowTablesRequest*, ShowTablesResponse*, bool, int)>)NULL,
                                "ShowTables", rpc_timeout_);
}

bool MasterClient::ShowTabletNodes(const ShowTabletNodesRequest* request,
                                   ShowTabletNodesResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::ShowTabletNodes,
                                request, response,
                                (std::function<void (ShowTabletNodesRequest*, ShowTabletNodesResponse*, bool, int)>)NULL,
                                "ShowTabletNodes", rpc_timeout_);
}

bool MasterClient::Register(const RegisterRequest* request,
                            RegisterResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::Register,
                                request, response,
                                (std::function<void (RegisterRequest*, RegisterResponse*, bool, int)>)NULL,
                                "Register", rpc_timeout_);
}

bool MasterClient::Report(const ReportRequest* request,
                          ReportResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::Report,
                                request, response,
                                (std::function<void (ReportRequest*, ReportResponse*, bool, int)>)NULL,
                                "Report", rpc_timeout_);
}

bool MasterClient::CmdCtrl(const CmdCtrlRequest* request,
                           CmdCtrlResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::CmdCtrl,
                                request, response,
                                (std::function<void (CmdCtrlRequest*, CmdCtrlResponse*, bool, int)>)NULL,
                                "CmdCtrl", rpc_timeout_);
}

bool MasterClient::OperateUser(const OperateUserRequest* request,
                               OperateUserResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::OperateUser,
                                request, response,
                                (std::function<void (OperateUserRequest*, OperateUserResponse*, bool, int)>)NULL,
                                "OperateUser", rpc_timeout_);
}

} // namespace master
} // namespace tera
