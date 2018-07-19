// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_LB_ENTRY_H_
#define TERA_LOAD_BALANCER_LB_ENTRY_H_

#include <memory>

#include "sofa/pbrpc/pbrpc.h"

#include "tera_entry.h"

namespace tera {
namespace load_balancer {

class LBServiceImpl;
class LBImpl;

class LBEntry : public TeraEntry {
public:
    LBEntry();
    virtual ~LBEntry();

    virtual bool StartServer();
    virtual bool Run();
    virtual void ShutdownServer();

private:
    std::unique_ptr<sofa::pbrpc::RpcServer> rpc_server_;
    LBServiceImpl* lb_service_impl_;
    std::shared_ptr<LBImpl> lb_impl_;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_LB_ENTRY_H_
