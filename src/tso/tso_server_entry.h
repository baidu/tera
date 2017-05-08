// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TSO_TSO_SERVER_ENTRY_H_
#define TERA_TSO_TSO_SERVER_ENTRY_H_

#include <sofa/pbrpc/pbrpc.h>

#include "tera_entry.h"
#include "tso/tso_server_impl.h"

namespace tera {
namespace tso {

class TsoServerImpl;

class TsoServerEntry : public TeraEntry {
public:
    TsoServerEntry();
    ~TsoServerEntry();

    bool StartServer();
    bool Run();
    void ShutdownServer();

private:
    TsoServerImpl* tso_server_impl_;
    sofa::pbrpc::RpcServer* rpc_server_;
};

} // namespace tso
} // namespace tera

#endif // TERA_TSO_TSO_SERVER_ENTRY_H_
