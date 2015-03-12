// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string>

#include <gflags/gflags.h>

#include "tera/master_client.h"


DECLARE_string(tera_master_addr);
DECLARE_string(tera_master_port);
DECLARE_int32(tera_master_connect_retry_times);
DECLARE_int32(tera_master_connect_retry_period);
DECLARE_int32(tera_master_connect_timeout_period);

namespace tera {
namespace master {

MasterClient::MasterClient()
    : RpcClient<MasterServer::Stub>(FLAGS_tera_master_addr + ":" + FLAGS_tera_master_port,
                                    FLAGS_tera_master_connect_retry_period,
                                    FLAGS_tera_master_connect_timeout_period,
                                    FLAGS_tera_master_connect_retry_times) {}

MasterClient::~MasterClient() {}

void MasterClient::ResetMasterClient(const std::string& server_addr) {
    ResetClient(server_addr);
}

bool MasterClient::GetSnapshot(const GetSnapshotRequest* request,
                               GetSnapshotResponse* response) {
  return SendMessageWithRetry(&MasterServer::Stub::GetSnapshot,
                              request, response,
                              (google::protobuf::Closure*)NULL,
                              "GetSnapshot");
}

bool MasterClient::DelSnapshot(const DelSnapshotRequest* request,
                               DelSnapshotResponse* response) {
  return SendMessageWithRetry(&MasterServer::Stub::DelSnapshot,
                              request, response,
                              (google::protobuf::Closure*)NULL,
                              "DelSnapshot");
}

bool MasterClient::CreateTable(const CreateTableRequest* request,
                               CreateTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::CreateTable,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "CreateTable");
}

bool MasterClient::DeleteTable(const DeleteTableRequest* request,
                               DeleteTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::DeleteTable,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "DeleteTable");
}

bool MasterClient::DisableTable(const DisableTableRequest* request,
                                DisableTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::DisableTable,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "DisableTable");
}

bool MasterClient::EnableTable(const EnableTableRequest* request,
                               EnableTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::EnableTable,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "EnableTable");
}

bool MasterClient::UpdateTable(const UpdateTableRequest* request,
                               UpdateTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::UpdateTable,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "UpdateTable");
}

bool MasterClient::SearchTable(const SearchTableRequest* request,
                               SearchTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::SearchTable,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "SearchTable");
}

bool MasterClient::CompactTable(const CompactTableRequest* request,
                                CompactTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::CompactTable,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "CompactTable");
}

bool MasterClient::ShowTables(const ShowTablesRequest* request,
                              ShowTablesResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::ShowTables,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "ShowTables");
}

bool MasterClient::ShowTabletNodes(const ShowTabletNodesRequest* request,
                                   ShowTabletNodesResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::ShowTabletNodes,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "ShowTabletNodes");
}

bool MasterClient::MergeTable(const MergeTableRequest* request,
                              MergeTableResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::MergeTable,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "MergeTable");
}

bool MasterClient::Register(const RegisterRequest* request,
                            RegisterResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::Register,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "Register");
}

bool MasterClient::Report(const ReportRequest* request,
                          ReportResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::Report,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "Report");
}

bool MasterClient::CmdCtrl(const CmdCtrlRequest* request,
                           CmdCtrlResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::CmdCtrl,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "CmdCtrl");
}

bool MasterClient::PollAndResetServerAddr() {
    // connect ZK to update master addr
    return true;
}

bool MasterClient::IsRetryStatus(const StatusCode& status) {
    return (status == kMasterNotInited
            || status == kMasterIsBusy
            || status == kMasterIsSecondary);
}

} // namespace master
} // namespace tera
