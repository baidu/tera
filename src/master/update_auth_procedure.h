// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include "common/mutex.h"
#include "access/access_entry.h"
#include "master/master_env.h"
#include "master/procedure.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"
#include "proto/access_control.pb.h"
#include "proto/master_rpc.pb.h"
#include "proto/status_code.pb.h"
#include "access/helpers/access_utils.h"

DECLARE_int32(tera_master_meta_retry_times);

namespace tera {
namespace master {

enum class UpdateAuthPhase { kUpdatePrepare, kUpdateMeta, kEofPhase };

struct UpdateUgiPair {
  using Req = UpdateUgiRequest;
  using Res = UpdateUgiResponse;
};

struct UpdateAuthPair {
  using Req = UpdateAuthRequest;
  using Res = UpdateAuthResponse;
};

template <typename UpdatePair>
class UpdateAuthProcedure : public Procedure {
 public:
  explicit UpdateAuthProcedure(const typename UpdatePair::Req* request,
                               typename UpdatePair::Res* response,
                               google::protobuf::Closure* closure, ThreadPool* thread_pool,
                               const std::shared_ptr<auth::AccessEntry>& access_entry,
                               std::unique_ptr<master::MetaWriteRecord>& meta_write_record,
                               auth::AccessUpdateType access_update_type);

  virtual ~UpdateAuthProcedure() {}

  virtual std::string ProcId() const;

  virtual void RunNextStage();

  virtual bool Done() { return done_.load(); }

 private:
  using UpdateAuthPhaseHandler = std::function<void(UpdateAuthProcedure*, const UpdateAuthPhase&)>;

  void SetNextPhase(const UpdateAuthPhase& phase) {
    MutexLock l(&phase_mutex_);
    phases_.emplace_back(phase);
  }

  UpdateAuthPhase GetCurrentPhase() {
    MutexLock l(&phase_mutex_);
    return phases_.back();
  }

  void EnterPhaseWithResponseStatus(StatusCode code, UpdateAuthPhase phase) {
    response_->set_status(code);
    SetNextPhase(phase);
  }

  void PrepareHandler(const UpdateAuthPhase& phase);

  void UpdateMetaHandler(const UpdateAuthPhase& phase);

  void UpdateMetaDone(bool succ);

  void EofPhaseHandler(const UpdateAuthPhase&);

 private:
  const typename UpdatePair::Req* request_;
  typename UpdatePair::Res* response_;
  google::protobuf::Closure* rpc_closure_;
  ThreadPool* thread_pool_;
  std::atomic<bool> done_;
  std::atomic<bool> update_meta_;
  std::shared_ptr<auth::AccessEntry> access_entry_;
  std::unique_ptr<master::MetaWriteRecord> meta_write_record_;
  auth::AccessUpdateType access_update_type_;

  std::vector<UpdateAuthPhase> phases_;
  mutable Mutex phase_mutex_;
  static std::map<UpdateAuthPhase, UpdateAuthPhaseHandler> phase_handlers_;
};

std::ostream& operator<<(std::ostream& o, const UpdateAuthPhase& phase);

template <typename UpdatePair>
std::map<UpdateAuthPhase, typename UpdateAuthProcedure<UpdatePair>::UpdateAuthPhaseHandler>
    UpdateAuthProcedure<UpdatePair>::phase_handlers_{
        {UpdateAuthPhase::kUpdatePrepare,
         std::bind(&UpdateAuthProcedure<UpdatePair>::PrepareHandler, _1, _2)},
        {UpdateAuthPhase::kUpdateMeta,
         std::bind(&UpdateAuthProcedure<UpdatePair>::UpdateMetaHandler, _1, _2)},
        {UpdateAuthPhase::kEofPhase,
         std::bind(&UpdateAuthProcedure<UpdatePair>::EofPhaseHandler, _1, _2)},
    };

template <typename UpdatePair>
UpdateAuthProcedure<UpdatePair>::UpdateAuthProcedure(
    const typename UpdatePair::Req* request, typename UpdatePair::Res* response,
    google::protobuf::Closure* closure, ThreadPool* thread_pool,
    const std::shared_ptr<auth::AccessEntry>& access_entry,
    std::unique_ptr<master::MetaWriteRecord>& meta_write_record,
    auth::AccessUpdateType access_update_type)
    : request_(request),
      response_(response),
      rpc_closure_(closure),
      thread_pool_(thread_pool),
      done_(false),
      update_meta_(false),
      access_entry_(access_entry),
      meta_write_record_(std::move(meta_write_record)),
      access_update_type_(access_update_type) {
  PROC_LOG(INFO) << "begin auth update procedure";
  SetNextPhase(UpdateAuthPhase::kUpdatePrepare);
}

template <typename UpdatePair>
std::string UpdateAuthProcedure<UpdatePair>::ProcId() const {
  std::string prefix =
      (access_update_type_ == auth::AccessUpdateType::UpdateUgi) ? "UpdateUgi:" : "UpdateAuth:";
  return prefix + auth::AccessUtils::GetNameFromMetaKey(meta_write_record_->key);
}

template <typename UpdatePair>
void UpdateAuthProcedure<UpdatePair>::RunNextStage() {
  UpdateAuthPhase phase = GetCurrentPhase();
  auto it = phase_handlers_.find(phase);
  PROC_CHECK(it != phase_handlers_.end()) << "illegal phase: " << phase
                                          << ", key name: " << meta_write_record_->key;
  UpdateAuthPhaseHandler handler = it->second;
  handler(this, phase);
}

template <typename UpdatePair>
void UpdateAuthProcedure<UpdatePair>::PrepareHandler(const UpdateAuthPhase& phase) {
  SetNextPhase(UpdateAuthPhase::kUpdateMeta);
}

template <typename UpdatePair>
void UpdateAuthProcedure<UpdatePair>::UpdateMetaHandler(const UpdateAuthPhase& phase) {
  if (update_meta_) {
    // procedure will invoke in cycle
    return;
  }
  update_meta_.store(true);
  std::string type = meta_write_record_->is_delete ? std::string("delete") : std::string("update");
  PROC_LOG(INFO) << "update auth meta begin [key name : " << meta_write_record_->key
                 << ", value = " << meta_write_record_->value << ", type = " << type << "]";
  UpdateMetaClosure closure = std::bind(&UpdateAuthProcedure<UpdatePair>::UpdateMetaDone, this, _1);
  MasterEnv().BatchWriteMetaTableAsync(*meta_write_record_, closure,
                                       FLAGS_tera_master_meta_retry_times);
}

template <typename UpdatePair>
void UpdateAuthProcedure<UpdatePair>::UpdateMetaDone(bool succ) {
  if (!succ) {
    PROC_LOG(ERROR) << "update meta failed";
    EnterPhaseWithResponseStatus(kMetaTabletError, UpdateAuthPhase::kEofPhase);
    return;
  }
  // update master mem auth info at last
  PROC_LOG(INFO) << "update auth info to meta succ";
  if (!meta_write_record_->is_delete) {
    if (!access_entry_->GetAccessUpdater().AddRecord(meta_write_record_->key,
                                                     meta_write_record_->value)) {
      PROC_LOG(INFO) << "Mismatch update auth type, not ugi && none";
    }
  } else {
    if (!access_entry_->GetAccessUpdater().DelRecord(meta_write_record_->key)) {
      PROC_LOG(INFO) << "Mismatch update auth type, not ugi && none";
    }
  }
  EnterPhaseWithResponseStatus(kMasterOk, UpdateAuthPhase::kEofPhase);
}

template <typename UpdatePair>
void UpdateAuthProcedure<UpdatePair>::EofPhaseHandler(const UpdateAuthPhase&) {
  done_.store(true);
  PROC_LOG(INFO) << "update auth finish";
  rpc_closure_->Run();
}
}
}
