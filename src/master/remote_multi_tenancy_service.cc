// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/remote_multi_tenancy_service.h"
#include <functional>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "master/multi_tenancy_service_impl.h"
#include "utils/network_utils.h"

namespace tera {
namespace master {

RemoteMultiTenancyService::RemoteMultiTenancyService(
    MultiTenacyServiceImpl* multi_tenancy_service_impl, std::shared_ptr<ThreadPool> thread_pool)
    : multi_tenancy_service_impl_(multi_tenancy_service_impl), thread_pool_(thread_pool) {}

RemoteMultiTenancyService::~RemoteMultiTenancyService() {}

void RemoteMultiTenancyService::UpdateUgi(google::protobuf::RpcController* controller,
                                          const UpdateUgiRequest* request,
                                          UpdateUgiResponse* response,
                                          google::protobuf::Closure* done) {
  LOG(INFO) << "accept RPC (UpdateUgi): " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback =
      std::bind(&RemoteMultiTenancyService::DoUpdateUgi, this, controller, request, response, done);
  thread_pool_->AddTask(callback);
}

void RemoteMultiTenancyService::ShowUgi(google::protobuf::RpcController* controller,
                                        const ShowUgiRequest* request, ShowUgiResponse* response,
                                        google::protobuf::Closure* done) {
  LOG(INFO) << "accept RPC (ShowUgi): " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback =
      std::bind(&RemoteMultiTenancyService::DoShowUgi, this, controller, request, response, done);
  thread_pool_->AddTask(callback);
}

void RemoteMultiTenancyService::UpdateAuth(google::protobuf::RpcController* controller,
                                           const UpdateAuthRequest* request,
                                           UpdateAuthResponse* response,
                                           google::protobuf::Closure* done) {
  LOG(INFO) << "accept RPC (UpdateAuth): " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback = std::bind(&RemoteMultiTenancyService::DoUpdateAuth, this, controller,
                                        request, response, done);
  thread_pool_->AddTask(callback);
}

void RemoteMultiTenancyService::ShowAuth(google::protobuf::RpcController* controller,
                                         const ShowAuthRequest* request, ShowAuthResponse* response,
                                         google::protobuf::Closure* done) {
  LOG(INFO) << "accept RPC (ShowAuth): " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback =
      std::bind(&RemoteMultiTenancyService::DoShowAuth, this, controller, request, response, done);
  thread_pool_->AddTask(callback);
}

void RemoteMultiTenancyService::SetAuthPolicy(google::protobuf::RpcController* controller,
                                              const SetAuthPolicyRequest* request,
                                              SetAuthPolicyResponse* response,
                                              google::protobuf::Closure* done) {
  LOG(INFO) << "accept RPC (SetAuthPolicy): " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback = std::bind(&RemoteMultiTenancyService::DoSetAuthPolicy, this,
                                        controller, request, response, done);
  thread_pool_->AddTask(callback);
}

void RemoteMultiTenancyService::ShowAuthPolicy(google::protobuf::RpcController* controller,
                                               const ShowAuthPolicyRequest* request,
                                               ShowAuthPolicyResponse* response,
                                               google::protobuf::Closure* done) {
  LOG(INFO) << "accept RPC (ShowAuthPolicy): " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback = std::bind(&RemoteMultiTenancyService::DoShowAuthPolicy, this,
                                        controller, request, response, done);
  thread_pool_->AddTask(callback);
}

void RemoteMultiTenancyService::SetQuota(google::protobuf::RpcController* controller,
                                         const SetQuotaRequest* request, SetQuotaResponse* response,
                                         google::protobuf::Closure* done) {
  LOG(INFO) << "accept RPC (SetQuota): " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback =
      std::bind(&RemoteMultiTenancyService::DoSetQuota, this, controller, request, response, done);
  thread_pool_->AddTask(callback);
}

void RemoteMultiTenancyService::ShowQuota(google::protobuf::RpcController* controller,
                                          const ShowQuotaRequest* request,
                                          ShowQuotaResponse* response,
                                          google::protobuf::Closure* done) {
  LOG(INFO) << "accept RPC (ShowQuota): " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback =
      std::bind(&RemoteMultiTenancyService::DoShowQuota, this, controller, request, response, done);
  thread_pool_->AddTask(callback);
}

// private

void RemoteMultiTenancyService::DoUpdateUgi(google::protobuf::RpcController* controller,
                                            const UpdateUgiRequest* request,
                                            UpdateUgiResponse* response,
                                            google::protobuf::Closure* done) {
  LOG(INFO) << "run RPC (UpdateUgi)";
  multi_tenancy_service_impl_->UpdateUgi(request, response, done);
  LOG(INFO) << "finish RPC (UpdateUgi)";
}

void RemoteMultiTenancyService::DoShowUgi(google::protobuf::RpcController* controller,
                                          const ShowUgiRequest* request, ShowUgiResponse* response,
                                          google::protobuf::Closure* done) {
  LOG(INFO) << "run RPC (ShowUgi)";
  multi_tenancy_service_impl_->ShowUgi(request, response, done);
  LOG(INFO) << "finish RPC (ShowUgi)";
}

void RemoteMultiTenancyService::DoUpdateAuth(google::protobuf::RpcController* controller,
                                             const UpdateAuthRequest* request,
                                             UpdateAuthResponse* response,
                                             google::protobuf::Closure* done) {
  LOG(INFO) << "run RPC (UpdateAuth)";
  multi_tenancy_service_impl_->UpdateAuth(request, response, done);
  LOG(INFO) << "finish RPC (UpdateAuth)";
}

void RemoteMultiTenancyService::DoShowAuth(google::protobuf::RpcController* controller,
                                           const ShowAuthRequest* request,
                                           ShowAuthResponse* response,
                                           google::protobuf::Closure* done) {
  LOG(INFO) << "run RPC (ShowAuth)";
  multi_tenancy_service_impl_->ShowAuth(request, response, done);
  LOG(INFO) << "finish RPC (ShowAuth)";
}

void RemoteMultiTenancyService::DoSetAuthPolicy(google::protobuf::RpcController* controller,
                                                const SetAuthPolicyRequest* request,
                                                SetAuthPolicyResponse* response,
                                                google::protobuf::Closure* done) {
  LOG(INFO) << "run RPC (SetAuthPolicy)";
  multi_tenancy_service_impl_->SetAuthPolicy(request, response, done);
  LOG(INFO) << "finish RPC (SetAuthPolicy)";
}

void RemoteMultiTenancyService::DoShowAuthPolicy(google::protobuf::RpcController* controller,
                                                 const ShowAuthPolicyRequest* request,
                                                 ShowAuthPolicyResponse* response,
                                                 google::protobuf::Closure* done) {
  LOG(INFO) << "run RPC (ShowAuthPolicy)";
  multi_tenancy_service_impl_->ShowAuthPolicy(request, response, done);
  LOG(INFO) << "finish RPC (ShowAuthPolicy)";
}

void RemoteMultiTenancyService::DoSetQuota(google::protobuf::RpcController* controller,
                                           const SetQuotaRequest* request,
                                           SetQuotaResponse* response,
                                           google::protobuf::Closure* done) {
  LOG(INFO) << "run RPC (SetQuota)";
  multi_tenancy_service_impl_->SetQuota(request, response, done);
  LOG(INFO) << "finish RPC (SetQuota)";
}

void RemoteMultiTenancyService::DoShowQuota(google::protobuf::RpcController* controller,
                                            const ShowQuotaRequest* request,
                                            ShowQuotaResponse* response,
                                            google::protobuf::Closure* done) {
  LOG(INFO) << "run RPC (ShowQuota)";
  multi_tenancy_service_impl_->ShowQuota(request, response, done);
  LOG(INFO) << "finish RPC (ShowQuota)";
}
}
}
