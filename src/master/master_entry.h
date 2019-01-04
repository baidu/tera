// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_MASTER_ENTRY_H_
#define TERA_MASTER_MASTER_ENTRY_H_

#include <memory>

#include <sofa/pbrpc/pbrpc.h>

#include "common/base/scoped_ptr.h"
#include "common/metric/metric_http_server.h"
#include "common/thread_pool.h"
#include "tera/tera_entry.h"

namespace tera {
namespace master {

class MasterImpl;
class RemoteMaster;
class RemoteMultiTenancyService;
class MultiTenacyServiceImpl;

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
  scoped_ptr<MasterImpl> master_impl_;
  scoped_ptr<MultiTenacyServiceImpl> multi_tenancy_service_impl_;
  std::shared_ptr<ThreadPool> thread_pool_;
  // scoped_ptr<RemoteMaster> remote_master_;
  RemoteMaster* remote_master_;
  RemoteMultiTenancyService* remote_multi_tenancy_service_;
  scoped_ptr<sofa::pbrpc::RpcServer> rpc_server_;
  scoped_ptr<tera::MetricHttpServer> metric_http_server_;
};

}  // namespace master
}  // namespace tera

#endif  // TERA_MASTER_MASTER_ENTRY_H_
