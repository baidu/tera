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
#include "master/master_env.h"
#include "master/procedure.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"
#include "proto/quota.pb.h"
#include "proto/master_rpc.pb.h"
#include "proto/status_code.pb.h"
#include "quota/master_quota_entry.h"

namespace tera {
namespace master {

enum class SetQuotaPhase { kSetMeta, kEofPhase };

std::ostream& operator<<(std::ostream& o, const SetQuotaPhase& phase);

class SetQuotaProcedure : public Procedure {
 public:
  SetQuotaProcedure(const SetQuotaRequest* request, SetQuotaResponse* response,
                    google::protobuf::Closure* closure, ThreadPool* thread_pool,
                    const std::shared_ptr<quota::MasterQuotaEntry>& quota_entry,
                    std::unique_ptr<MetaWriteRecord>& meta_write_record);
  virtual ~SetQuotaProcedure() {}
  virtual std::string ProcId() const;
  virtual void RunNextStage();
  virtual bool Done() { return done_.load(); }

 private:
  using SetQuotaPhaseHandler = std::function<void(SetQuotaProcedure*, const SetQuotaPhase&)>;

  void SetNextPhase(const SetQuotaPhase& phase) {
    MutexLock l(&phase_mutex_);
    phases_.emplace_back(phase);
  }

  SetQuotaPhase GetCurrentPhase() {
    MutexLock l(&phase_mutex_);
    return phases_.back();
  }

  void EnterPhaseWithResponseStatus(StatusCode code, SetQuotaPhase phase) {
    response_->set_status(code);
    SetNextPhase(phase);
  }
  void PrepareHandler(const SetQuotaPhase& phase);
  void SetMetaHandler(const SetQuotaPhase& phase);
  void SetMetaDone(bool succ);
  void EofPhaseHandler(const SetQuotaPhase&);

 private:
  const SetQuotaRequest* request_;
  SetQuotaResponse* response_;
  google::protobuf::Closure* rpc_closure_;
  ThreadPool* thread_pool_;
  std::atomic<bool> done_;
  std::atomic<bool> update_meta_;
  std::vector<SetQuotaPhase> phases_;
  mutable Mutex phase_mutex_;
  static std::map<SetQuotaPhase, SetQuotaPhaseHandler> phase_handlers_;
  std::shared_ptr<quota::MasterQuotaEntry> quota_entry_;
  std::unique_ptr<MetaWriteRecord> meta_write_record_;
};
}
}
