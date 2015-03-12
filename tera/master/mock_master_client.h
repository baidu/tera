// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_MOCK_MASTER_CLIENT_H
#define TERA_MASTER_MOCK_MASTER_CLIENT_H

#include "tera/master/master_client.h"

#include "gmock/gmock.h"

#include "tera/proto/master_rpc.pb.h"
#include "tera/rpc_client.h"

namespace tera {
namespace master {

class MockMasterClient : public MasterClient {
public:
    MOCK_METHOD1(ResetMasterClient,
        void(const std::string& server_addr));
    MOCK_METHOD2(CreateTable,
        bool(const CreateTableRequest* request,
             CreateTableResponse* response));
    MOCK_METHOD2(DeleteTable,
        bool(const DeleteTableRequest* request,
             DeleteTableResponse* response));
    MOCK_METHOD2(DisableTable,
        bool(const DisableTableRequest* request,
             DisableTableResponse* response));
    MOCK_METHOD2(EnableTable,
        bool(const EnableTableRequest* request,
             EnableTableResponse* response));
    MOCK_METHOD2(UpdateTable,
        bool(const UpdateTableRequest* request,
             UpdateTableResponse* response));
    MOCK_METHOD2(SearchTable,
        bool(const SearchTableRequest* request,
             SearchTableResponse* response));
    MOCK_METHOD2(CompactTable,
        bool(const CompactTableRequest* request,
             CompactTableResponse* response));
    MOCK_METHOD2(ShowTables,
        bool(const ShowTablesRequest* request,
             ShowTablesResponse* response));
    MOCK_METHOD2(MergeTable,
        bool(const MergeTableRequest* request,
             MergeTableResponse* response));
    MOCK_METHOD2(Register,
        bool(const RegisterRequest* request,
             RegisterResponse* response));
    MOCK_METHOD2(Report,
        bool(const ReportRequest* request,
             ReportResponse* response));
    MOCK_METHOD2(CmdCtrl,
        bool(const CmdCtrlRequest* request,
             CmdCtrlResponse* response));
    };

}  // namespace master
}  // namespace tera

#endif // TERA_MASTER_MOCK_MASTER_CLIENT_H
