// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master_mutli_tenancy_client.h"

namespace tera {
namespace master {

MasterMultiTenancyClient::MasterMultiTenancyClient(const std::string& server_addr,
                                                   int32_t rpc_timeout)
    : RpcClient<MasterMultiTenancyService::Stub>(server_addr), rpc_timeout_(rpc_timeout) {}

MasterMultiTenancyClient::~MasterMultiTenancyClient() {}

bool MasterMultiTenancyClient::UpdateUgi(const UpdateUgiRequest* request,
                                         UpdateUgiResponse* response) {
  return SendMessageWithRetry(
      &MasterMultiTenancyService::Stub::UpdateUgi, request, response,
      (std::function<void(UpdateUgiRequest*, UpdateUgiResponse*, bool, int)>)NULL, "UpdateUgi",
      rpc_timeout_);
}

bool MasterMultiTenancyClient::ShowUgi(const ShowUgiRequest* request, ShowUgiResponse* response) {
  return SendMessageWithRetry(
      &MasterMultiTenancyService::Stub::ShowUgi, request, response,
      (std::function<void(ShowUgiRequest*, ShowUgiResponse*, bool, int)>)NULL, "ShowUgi",
      rpc_timeout_);
}

bool MasterMultiTenancyClient::UpdateAuth(const UpdateAuthRequest* request,
                                          UpdateAuthResponse* response) {
  return SendMessageWithRetry(
      &MasterMultiTenancyService::Stub::UpdateAuth, request, response,
      (std::function<void(UpdateAuthRequest*, UpdateAuthResponse*, bool, int)>)NULL, "UpdateAuth",
      rpc_timeout_);
}

bool MasterMultiTenancyClient::ShowAuth(const ShowAuthRequest* request,
                                        ShowAuthResponse* response) {
  return SendMessageWithRetry(
      &MasterMultiTenancyService::Stub::ShowAuth, request, response,
      (std::function<void(ShowAuthRequest*, ShowAuthResponse*, bool, int)>)NULL, "ShowAuth",
      rpc_timeout_);
}

bool MasterMultiTenancyClient::SetAuthPolicy(const SetAuthPolicyRequest* request,
                                             SetAuthPolicyResponse* response) {
  return SendMessageWithRetry(
      &MasterMultiTenancyService::Stub::SetAuthPolicy, request, response,
      (std::function<void(SetAuthPolicyRequest*, SetAuthPolicyResponse*, bool, int)>)NULL,
      "SetAuthPolicy", rpc_timeout_);
}

bool MasterMultiTenancyClient::ShowAuthPolicy(const ShowAuthPolicyRequest* request,
                                              ShowAuthPolicyResponse* response) {
  return SendMessageWithRetry(
      &MasterMultiTenancyService::Stub::ShowAuthPolicy, request, response,
      (std::function<void(ShowAuthPolicyRequest*, ShowAuthPolicyResponse*, bool, int)>)NULL,
      "ShowAuthPolicy", rpc_timeout_);
}

bool MasterMultiTenancyClient::SetQuota(const SetQuotaRequest* request,
                                        SetQuotaResponse* response) {
  return SendMessageWithRetry(
      &MasterMultiTenancyService::Stub::SetQuota, request, response,
      (std::function<void(SetQuotaRequest*, SetQuotaResponse*, bool, int)>)NULL, "SetQuota",
      rpc_timeout_);
}

bool MasterMultiTenancyClient::ShowQuota(const ShowQuotaRequest* request,
                                         ShowQuotaResponse* response) {
  return SendMessageWithRetry(
      &MasterMultiTenancyService::Stub::ShowQuota, request, response,
      (std::function<void(ShowQuotaRequest*, ShowQuotaResponse*, bool, int)>)NULL, "ShowQuota",
      rpc_timeout_);
}
}
}
