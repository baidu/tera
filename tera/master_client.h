// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_MASTER_CLIENT_H
#define TERA_MASTER_MASTER_CLIENT_H

#include <string>

#include "tera/proto/master_rpc.pb.h"
#include "tera/rpc_client.h"

namespace tera {
namespace master {

class MasterClient : public RpcClient<MasterServer::Stub> {
public:
    MasterClient();
    virtual ~MasterClient();

    virtual void ResetMasterClient(const std::string& server_addr);

    virtual bool GetSnapshot(const GetSnapshotRequest* request,
                             GetSnapshotResponse* response);

    virtual bool DelSnapshot(const DelSnapshotRequest* request,
                             DelSnapshotResponse* response);

    virtual bool CreateTable(const CreateTableRequest* request,
                             CreateTableResponse* response);

    virtual bool DeleteTable(const DeleteTableRequest* request,
                             DeleteTableResponse* response);

    virtual bool DisableTable(const DisableTableRequest* request,
                              DisableTableResponse* response);

    virtual bool EnableTable(const EnableTableRequest* request,
                             EnableTableResponse* response);

    virtual bool UpdateTable(const UpdateTableRequest* request,
                             UpdateTableResponse* response);

    virtual bool SearchTable(const SearchTableRequest* request,
                             SearchTableResponse* response);

    virtual bool CompactTable(const CompactTableRequest* request,
                              CompactTableResponse* response);

    virtual bool ShowTables(const ShowTablesRequest* request,
                            ShowTablesResponse* response);

    virtual bool ShowTabletNodes(const ShowTabletNodesRequest* request,
                                 ShowTabletNodesResponse* response);

    virtual bool MergeTable(const MergeTableRequest* request,
                            MergeTableResponse* response);

    virtual bool Register(const RegisterRequest* request,
                          RegisterResponse* response);

    virtual bool Report(const ReportRequest* request,
                        ReportResponse* response);

    virtual bool CmdCtrl(const CmdCtrlRequest* request,
                         CmdCtrlResponse* response);

private:
    bool PollAndResetServerAddr();
    bool IsRetryStatus(const StatusCode& status);
};

} // namespace
} // namespace

#endif // TERA_MASTER_MASTER_CLIENT_H
