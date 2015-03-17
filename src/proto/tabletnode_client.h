// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLETNODE_CLIENT_H
#define TERA_TABLETNODE_TABLETNODE_CLIENT_H

#include "proto/rpc_client.h"
#include "proto/tabletnode_rpc.pb.h"

namespace tera {
namespace tabletnode {

class TabletNodeClient : public RpcClient<TabletNodeServer::Stub> {
public:
    TabletNodeClient();
    TabletNodeClient(int32_t wait_time, int32_t rpc_timeout, int32_t retry_times);
    ~TabletNodeClient();

    void ResetTabletNodeClient(const std::string& server_addr);

    bool LoadTablet(const LoadTabletRequest* request,
                    LoadTabletResponse* response);

    bool UnloadTablet(const UnloadTabletRequest* request,
                      UnloadTabletResponse* response);

    bool ReadTablet(const ReadTabletRequest* request,
                    ReadTabletResponse* response);

    bool WriteTablet(const WriteTabletRequest* request,
                     WriteTabletResponse* response);

    bool ScanTablet(const ScanTabletRequest* request,
                    ScanTabletResponse* response);

    bool Query(const QueryRequest* request, QueryResponse* response);

    bool SplitTablet(const SplitTabletRequest* request,
                     SplitTabletResponse* response);

    bool MergeTablet(const MergeTabletRequest* request,
                     MergeTabletResponse* response);

    bool CompactTablet(const CompactTabletRequest* request,
                       CompactTabletResponse* response);

private:
    bool IsRetryStatus(const StatusCode& status);
};

} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_CLIENT_H
