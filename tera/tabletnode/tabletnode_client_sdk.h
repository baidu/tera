// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLETNODE_CLIENT_SDK_H
#define TERA_TABLETNODE_TABLETNODE_CLIENT_SDK_H

#include "thirdparty/sofa/pbrpc/pbrpc.h"

#include "tera/proto/tabletnode_rpc.pb.h"
#include "tera/rpc_client_sdk.h"

namespace tera {
namespace tabletnode {

class TabletNodeClientSDK : public RpcClientSDK<TabletNodeServer::Stub> {
public:
    TabletNodeClientSDK(sofa::pbrpc::RpcClient* pbrpc_client);
    ~TabletNodeClientSDK();

    void ResetTabletNodeClient(const std::string& server_addr);

    bool LoadTablet(const LoadTabletRequest* request,
                    LoadTabletResponse* response,
                    google::protobuf::Closure* done);

    bool UnloadTablet(const UnloadTabletRequest* request,
                      UnloadTabletResponse* response,
                      google::protobuf::Closure* done);

    bool ReadTablet(const ReadTabletRequest* request,
                    ReadTabletResponse* response,
                    google::protobuf::Closure* done);

    bool WriteTablet(const WriteTabletRequest* request,
                     WriteTabletResponse* response,
                     google::protobuf::Closure* done);

    bool ScanTablet(const ScanTabletRequest* request,
                    ScanTabletResponse* response,
                    google::protobuf::Closure* done);

    bool Query(const QueryRequest* request, QueryResponse* response,
               google::protobuf::Closure* done);

    bool SplitTablet(const SplitTabletRequest* request,
                     SplitTabletResponse* response,
                     google::protobuf::Closure* done);

private:
    bool IsRetryStatus(const StatusCode& status);
};

} // namespace sdk
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_CLIENT_SDK_H
