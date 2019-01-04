// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/multi_tenancy_service_impl.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "access/access_entry.h"
#include "quota/quota_entry.h"
#include "master/master_env.h"
#include "master/update_auth_procedure.h"
#include "master/set_quota_procedure.h"
#include "access/helpers/access_utils.h"
#include "quota/helpers/quota_utils.h"

namespace tera {
namespace master {

MultiTenacyServiceImpl::MultiTenacyServiceImpl(
    const std::shared_ptr<auth::AccessEntry>& access_entry,
    const std::shared_ptr<quota::MasterQuotaEntry>& quota_entry)
    : access_entry_(access_entry),
      quota_entry_(quota_entry),
      thread_pool_(MasterEnv().GetThreadPool()) {}

void MultiTenacyServiceImpl::UpdateUgi(const UpdateUgiRequest* request, UpdateUgiResponse* response,
                                       google::protobuf::Closure* done) {
  response->set_sequence_id(request->sequence_id());
  MasterStatus master_status = MasterEnv().GetMaster()->GetMasterStatus();
  if (master_status != kIsRunning) {
    LOG(ERROR) << "master is not ready, status_ = "
               << StatusCodeToString(static_cast<StatusCode>(master_status));
    response->set_status(static_cast<StatusCode>(master_status));
    done->Run();
    return;
  }

  // TODO: doesn't need access verify at first

  // Parse ugi info to MetaWriteRecord
  std::unique_ptr<MetaWriteRecord> meta_write_record(
      auth::AccessUtils::NewMetaRecord(access_entry_, request->update_info()));
  if (!meta_write_record) {
    response->set_status(kMismatchAuthType);
    done->Run();
    return;
  }
  std::shared_ptr<Procedure> proc(new UpdateAuthProcedure<UpdateUgiPair>(
      request, response, done, thread_pool_.get(), access_entry_, meta_write_record,
      auth::AccessUpdateType::UpdateUgi));
  MasterEnv().GetExecutor()->AddProcedure(proc);
}

void MultiTenacyServiceImpl::ShowUgi(const ShowUgiRequest* request, ShowUgiResponse* response,
                                     google::protobuf::Closure* done) {
  response->set_sequence_id(request->sequence_id());
  MasterStatus master_status = MasterEnv().GetMaster()->GetMasterStatus();
  if (master_status != kIsRunning) {
    LOG(ERROR) << "master is not ready, status_ = "
               << StatusCodeToString(static_cast<StatusCode>(master_status));
    response->set_status(static_cast<StatusCode>(master_status));
    done->Run();
    return;
  }

  // TODO: doesn't need access verify at first

  access_entry_->GetAccessUpdater().ShowUgiInfo(response);
  response->set_status(kMasterOk);
  done->Run();
}

void MultiTenacyServiceImpl::UpdateAuth(const UpdateAuthRequest* request,
                                        UpdateAuthResponse* response,
                                        google::protobuf::Closure* done) {
  response->set_sequence_id(request->sequence_id());
  MasterStatus master_status = MasterEnv().GetMaster()->GetMasterStatus();
  if (master_status != kIsRunning) {
    LOG(ERROR) << "master is not ready, status_ = "
               << StatusCodeToString(static_cast<StatusCode>(master_status));
    response->set_status(static_cast<StatusCode>(master_status));
    done->Run();
    return;
  }

  // TODO: doesn't need access verify at first

  // Update Auth
  std::unique_ptr<MetaWriteRecord> meta_write_record(
      auth::AccessUtils::NewMetaRecord(access_entry_, request->update_info()));
  if (!meta_write_record) {
    response->set_status(kMismatchAuthType);
    done->Run();
    return;
  }
  std::shared_ptr<Procedure> proc(new UpdateAuthProcedure<UpdateAuthPair>(
      request, response, done, thread_pool_.get(), access_entry_, meta_write_record,
      auth::AccessUpdateType::UpdateAuth));
  MasterEnv().GetExecutor()->AddProcedure(proc);
}

void MultiTenacyServiceImpl::ShowAuth(const ShowAuthRequest* request, ShowAuthResponse* response,
                                      google::protobuf::Closure* done) {
  response->set_sequence_id(request->sequence_id());
  MasterStatus master_status = MasterEnv().GetMaster()->GetMasterStatus();
  if (master_status != kIsRunning) {
    LOG(ERROR) << "master is not ready, status_ = "
               << StatusCodeToString(static_cast<StatusCode>(master_status));
    response->set_status(static_cast<StatusCode>(master_status));
    done->Run();
    return;
  }

  // TODO: doesn't need access verify at first
  access_entry_->GetAccessUpdater().ShowAuthInfo(response);
  response->set_status(kMasterOk);
  done->Run();
}

void MultiTenacyServiceImpl::SetAuthPolicy(const SetAuthPolicyRequest* request,
                                           SetAuthPolicyResponse* response,
                                           google::protobuf::Closure* done) {
  MasterStatus master_status = MasterEnv().GetMaster()->GetMasterStatus();
  if (master_status != kIsRunning) {
    LOG(ERROR) << "master is not ready, status_ = "
               << StatusCodeToString(static_cast<StatusCode>(master_status));
    response->set_status(static_cast<StatusCode>(master_status));
    done->Run();
    return;
  }
  response->set_status(kMasterOk);
  done->Run();
}

void MultiTenacyServiceImpl::ShowAuthPolicy(const ShowAuthPolicyRequest* request,
                                            ShowAuthPolicyResponse* response,
                                            google::protobuf::Closure* done) {
  MasterStatus master_status = MasterEnv().GetMaster()->GetMasterStatus();
  if (master_status != kIsRunning) {
    LOG(ERROR) << "master is not ready, status_ = "
               << StatusCodeToString(static_cast<StatusCode>(master_status));
    response->set_status(static_cast<StatusCode>(master_status));
    done->Run();
    return;
  }

  // TODO: doesn't need access verify at first

  response->set_status(kMasterOk);
  done->Run();
}

void MultiTenacyServiceImpl::SetQuota(const SetQuotaRequest* request, SetQuotaResponse* response,
                                      google::protobuf::Closure* done) {
  response->set_sequence_id(request->sequence_id());
  MasterStatus master_status = MasterEnv().GetMaster()->GetMasterStatus();
  if (master_status != kIsRunning) {
    LOG(ERROR) << "master is not ready, status_ = "
               << StatusCodeToString(static_cast<StatusCode>(master_status));
    response->set_status(static_cast<StatusCode>(master_status));
    done->Run();
    return;
  }

  // TODO: doesn't need access verify at first

  // build meta_write_record for quota
  // Should get quota in master memory and then merge the TableQuota
  std::unique_ptr<TableQuota> target_table_quota(new TableQuota);
  if (!quota_entry_->GetTableQuota(request->table_quota().table_name(), target_table_quota.get())) {
    // brand new table_quota, should set all default value
    target_table_quota->set_table_name(request->table_quota().table_name());
    target_table_quota->set_type(TableQuota::kSetQuota);
    // peroid = 1, limit = -1
    quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                  kQuotaWriteReqs);
    quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                  kQuotaWriteBytes);
    quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                  kQuotaReadReqs);
    quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                  kQuotaReadBytes);
    quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                  kQuotaScanReqs);
    quota::MasterQuotaHelper::SetDefaultQuotaInfo(target_table_quota->add_quota_infos(),
                                                  kQuotaScanBytes);
  }
  quota::MasterQuotaHelper::MergeTableQuota(request->table_quota(), target_table_quota.get());

  std::unique_ptr<MetaWriteRecord> meta_write_record(
      quota::MasterQuotaHelper::NewMetaRecordFromQuota(*target_table_quota));
  if (!meta_write_record) {
    response->set_status(kQuotaInvalidArg);
    done->Run();
    return;
  }
  std::shared_ptr<Procedure> proc(new SetQuotaProcedure(request, response, done, thread_pool_.get(),
                                                        quota_entry_, meta_write_record));
  MasterEnv().GetExecutor()->AddProcedure(proc);
}

void MultiTenacyServiceImpl::ShowQuota(const ShowQuotaRequest* request, ShowQuotaResponse* response,
                                       google::protobuf::Closure* done) {
  response->set_sequence_id(request->sequence_id());
  MasterStatus master_status = MasterEnv().GetMaster()->GetMasterStatus();
  if (master_status != kIsRunning) {
    LOG(ERROR) << "master is not ready, status_ = "
               << StatusCodeToString(static_cast<StatusCode>(master_status));
    response->set_status(static_cast<StatusCode>(master_status));
    done->Run();
    return;
  }

  // TODO: doesn't need access verify at first
  quota_entry_->ShowQuotaInfo(response, request->brief_show());
  response->set_status(kMasterOk);
  done->Run();
}
}
}
