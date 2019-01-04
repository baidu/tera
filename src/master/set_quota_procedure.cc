// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/set_quota_procedure.h"
#include "quota/helpers/quota_utils.h"

DECLARE_int32(tera_master_meta_retry_times);

namespace tera {
namespace master {

std::map<SetQuotaPhase, SetQuotaProcedure::SetQuotaPhaseHandler> SetQuotaProcedure::phase_handlers_{
    {SetQuotaPhase::kSetMeta, std::bind(&SetQuotaProcedure::SetMetaHandler, _1, _2)},
    {SetQuotaPhase::kEofPhase, std::bind(&SetQuotaProcedure::EofPhaseHandler, _1, _2)},
};

SetQuotaProcedure::SetQuotaProcedure(const SetQuotaRequest* request, SetQuotaResponse* response,
                                     google::protobuf::Closure* closure, ThreadPool* thread_pool,
                                     const std::shared_ptr<quota::MasterQuotaEntry>& quota_entry,
                                     std::unique_ptr<MetaWriteRecord>& meta_write_record)
    : request_(request),
      response_(response),
      rpc_closure_(closure),
      thread_pool_(thread_pool),
      done_(false),
      update_meta_(false),
      quota_entry_(quota_entry),
      meta_write_record_(std::move(meta_write_record)) {
  PROC_LOG(INFO) << "begin quota update prepare";
  SetNextPhase(SetQuotaPhase::kSetMeta);
}

std::string SetQuotaProcedure::ProcId() const {
  std::string prefix = std::string("SetQuota:");
  return prefix + quota::MasterQuotaHelper::GetTableNameFromMetaKey(meta_write_record_->key);
}

void SetQuotaProcedure::RunNextStage() {
  SetQuotaPhase phase = GetCurrentPhase();
  auto it = phase_handlers_.find(phase);
  PROC_CHECK(it != phase_handlers_.end()) << "illegal phase: " << phase
                                          << ", table_name: " << meta_write_record_->key;
  SetQuotaPhaseHandler handler = it->second;
  handler(this, phase);
}

void SetQuotaProcedure::SetMetaHandler(const SetQuotaPhase& phase) {
  if (update_meta_) {
    return;
  }
  update_meta_.store(true);
  PROC_LOG(INFO) << "set quota meta begin [table : " << meta_write_record_->key << "]";
  UpdateMetaClosure closure = std::bind(&SetQuotaProcedure::SetMetaDone, this, _1);
  MasterEnv().BatchWriteMetaTableAsync(*meta_write_record_, closure,
                                       FLAGS_tera_master_meta_retry_times);
}

void SetQuotaProcedure::SetMetaDone(bool succ) {
  if (!succ) {
    PROC_LOG(ERROR) << "update meta failed";
    EnterPhaseWithResponseStatus(kMetaTabletError, SetQuotaPhase::kEofPhase);
    return;
  }
  // update master mem quota info at last
  PROC_LOG(INFO) << "set quota info to meta succ";

  // meta_write_record_->is_delete will allways be false. Quota doesn't have delete
  if (!quota_entry_->AddRecord(meta_write_record_->key, meta_write_record_->value)) {
    PROC_LOG(ERROR) << "Set quota failed!";
  }
  EnterPhaseWithResponseStatus(kMasterOk, SetQuotaPhase::kEofPhase);
}

void SetQuotaProcedure::EofPhaseHandler(const SetQuotaPhase&) {
  done_.store(true);
  PROC_LOG(INFO) << "set quota finish";
  rpc_closure_->Run();
}

std::ostream& operator<<(std::ostream& o, const SetQuotaPhase& phase) {
  static const char* msg[] = {"SetQuotaPhase::kSetMeta", "SetQuotaPhase::kEofPhase",
                              "SetQuotaPhase::kUnknown"};
  static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
  typedef std::underlying_type<SetQuotaPhase>::type UnderType;
  uint32_t index = static_cast<UnderType>(phase) - static_cast<UnderType>(SetQuotaPhase::kSetMeta);
  index = index < msg_size ? index : msg_size - 1;
  o << msg[index];
  return o;
}
}
}
