// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <memory>
#include "common/thread_pool.h"
#include "proto/master_rpc.pb.h"

namespace tera {
namespace master {

class MultiTenacyServiceImpl;

class RemoteMultiTenancyService : public MasterMultiTenancyService {
 public:
  explicit RemoteMultiTenancyService(MultiTenacyServiceImpl* multi_tenancy_service_impl,
                                     std::shared_ptr<ThreadPool> thread_pool);
  ~RemoteMultiTenancyService();

  void UpdateUgi(google::protobuf::RpcController* controller, const UpdateUgiRequest* request,
                 UpdateUgiResponse* response, google::protobuf::Closure* done);

  void ShowUgi(google::protobuf::RpcController* controller, const ShowUgiRequest* request,
               ShowUgiResponse* response, google::protobuf::Closure* done);

  void UpdateAuth(google::protobuf::RpcController* controller, const UpdateAuthRequest* request,
                  UpdateAuthResponse* response, google::protobuf::Closure* done);

  void ShowAuth(google::protobuf::RpcController* controller, const ShowAuthRequest* request,
                ShowAuthResponse* response, google::protobuf::Closure* done);

  void SetAuthPolicy(google::protobuf::RpcController* controller,
                     const SetAuthPolicyRequest* request, SetAuthPolicyResponse* response,
                     google::protobuf::Closure* done);

  void ShowAuthPolicy(google::protobuf::RpcController* controller,
                      const ShowAuthPolicyRequest* request, ShowAuthPolicyResponse* response,
                      google::protobuf::Closure* done);

  void SetQuota(google::protobuf::RpcController* controller, const SetQuotaRequest* request,
                SetQuotaResponse* response, google::protobuf::Closure* done);

  void ShowQuota(google::protobuf::RpcController* controller, const ShowQuotaRequest* request,
                 ShowQuotaResponse* response, google::protobuf::Closure* done);

 private:
  void DoUpdateUgi(google::protobuf::RpcController* controller, const UpdateUgiRequest* request,
                   UpdateUgiResponse* response, google::protobuf::Closure* done);
  void DoShowUgi(google::protobuf::RpcController* controller, const ShowUgiRequest* request,
                 ShowUgiResponse* response, google::protobuf::Closure* done);
  void DoUpdateAuth(google::protobuf::RpcController* controller, const UpdateAuthRequest* request,
                    UpdateAuthResponse* response, google::protobuf::Closure* done);
  void DoShowAuth(google::protobuf::RpcController* controller, const ShowAuthRequest* request,
                  ShowAuthResponse* response, google::protobuf::Closure* done);
  void DoSetAuthPolicy(google::protobuf::RpcController* controller,
                       const SetAuthPolicyRequest* request, SetAuthPolicyResponse* response,
                       google::protobuf::Closure* done);
  void DoShowAuthPolicy(google::protobuf::RpcController* controller,
                        const ShowAuthPolicyRequest* request, ShowAuthPolicyResponse* response,
                        google::protobuf::Closure* done);
  void DoSetQuota(google::protobuf::RpcController* controller, const SetQuotaRequest* request,
                  SetQuotaResponse* response, google::protobuf::Closure* done);
  void DoShowQuota(google::protobuf::RpcController* controller, const ShowQuotaRequest* request,
                   ShowQuotaResponse* response, google::protobuf::Closure* done);

 private:
  MultiTenacyServiceImpl* multi_tenancy_service_impl_;
  std::shared_ptr<ThreadPool> thread_pool_;
};
}
}
