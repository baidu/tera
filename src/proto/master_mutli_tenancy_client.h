// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <gflags/gflags.h>
#include <string>
#include "proto/master_rpc.pb.h"
#include "proto/rpc_client.h"

DECLARE_int32(tera_rpc_timeout_period);

namespace tera {
namespace master {

class MasterMultiTenancyClient : public RpcClient<MasterMultiTenancyService::Stub> {
 public:
  MasterMultiTenancyClient(const std::string& server_addr = "",
                           int32_t rpc_timeout = FLAGS_tera_rpc_timeout_period);
  virtual ~MasterMultiTenancyClient();
  virtual bool UpdateUgi(const UpdateUgiRequest* request, UpdateUgiResponse* response);
  virtual bool ShowUgi(const ShowUgiRequest* request, ShowUgiResponse* response);
  virtual bool UpdateAuth(const UpdateAuthRequest* request, UpdateAuthResponse* response);
  virtual bool ShowAuth(const ShowAuthRequest* request, ShowAuthResponse* response);

  virtual bool SetAuthPolicy(const SetAuthPolicyRequest* request, SetAuthPolicyResponse* response);
  virtual bool ShowAuthPolicy(const ShowAuthPolicyRequest* request,
                              ShowAuthPolicyResponse* response);

  virtual bool SetQuota(const SetQuotaRequest* request, SetQuotaResponse* response);
  virtual bool ShowQuota(const ShowQuotaRequest* request, ShowQuotaResponse* response);

 private:
  int32_t rpc_timeout_;
};
}
}
