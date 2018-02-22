// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TIMEORACLE_TIMEORACLE_ENTRY_H_
#define TERA_TIMEORACLE_TIMEORACLE_ENTRY_H_

#include <sofa/pbrpc/pbrpc.h>

#include "tera_entry.h"
#include <thread>
#include <atomic>
#include <memory>

namespace tera {
namespace timeoracle {

class RemoteTimeoracle;
class TimeoracleZkAdapterBase;

class TimeoracleEntry : public TeraEntry {
public:
    TimeoracleEntry();
    ~TimeoracleEntry();


    virtual bool Start() override;
    virtual bool Run() override;
    virtual void ShutdownServer() override;

private:
    bool InitZKAdaptor();
    bool StartServer();
    void LeaseThread();

private:
    std::string                                 local_addr_;
    RemoteTimeoracle*                           remote_timeoracle_;
    std::unique_ptr<sofa::pbrpc::RpcServer>     sofa_pbrpc_server_;
    int64_t                                     startup_timestamp_;
    std::unique_ptr<TimeoracleZkAdapterBase>    zk_adapter_;
    std::thread                                 lease_thread_;
    std::atomic<bool>                           need_quit_;
};

} // namespace timeoracle
} // namespace tera

#endif // TERA_TIMEORACLE_TIMEORACLE_ENTRY_H_
