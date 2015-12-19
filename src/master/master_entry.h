// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_MASTER_ENTRY_H_
#define TERA_MASTER_MASTER_ENTRY_H_

#include <sofa/pbrpc/pbrpc.h>

#include "common/base/scoped_ptr.h"
#include "tera_entry.h"

namespace tera {
namespace master {

class MasterImpl;
class RemoteMaster;

class MasterEntry : public TeraEntry {
public:
    MasterEntry();
    ~MasterEntry();

    bool StartServer();
    bool Run();
    void ShutdownServer();

private:
    bool InitZKAdaptor();

private:
    scoped_ptr<MasterImpl> m_master_impl;
    // scoped_ptr<RemoteMaster> m_remote_master;
    RemoteMaster* m_remote_master;
    scoped_ptr<sofa::pbrpc::RpcServer> m_rpc_server;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_MASTER_ENTRY_H_
