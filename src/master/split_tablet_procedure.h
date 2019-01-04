// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include "master/procedure.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"

namespace tera {
namespace master {

enum class SplitTabletPhase {
  kPreSplitTablet,
  kUnLoadTablet,
  kPostUnLoadTablet,
  kUpdateMeta,
  kLoadTablets,
  kFaultRecover,
  kEofPhase,
};

std::ostream& operator<<(std::ostream& o, const SplitTabletPhase& phase);

typedef std::function<void(SplitTabletRequest*, SplitTabletResponse*, bool, int)>
    ComputeSplitKeyClosure;

class SplitTabletProcedure : public Procedure {
 public:
  explicit SplitTabletProcedure(TabletPtr tablet, ThreadPool* thread_pool)
      : SplitTabletProcedure(tablet, std::string(""), thread_pool) {}

  explicit SplitTabletProcedure(TabletPtr tablet, std::string, ThreadPool* thread_pool);

  virtual ~SplitTabletProcedure() {}

  virtual std::string ProcId() const;

  virtual bool Done() { return done_; }

  virtual void RunNextStage();

 private:
  typedef std::function<void(SplitTabletProcedure*, const SplitTabletPhase&)>
      SplitTabletPhaseHandler;

  SplitTabletPhase GetCurrentPhase() {
    std::lock_guard<std::mutex> lock(mutex_);
    return phases_.back();
  }

  void SetNextPhase(SplitTabletPhase phase) {
    std::lock_guard<std::mutex> lock(mutex_);
    phases_.emplace_back(phase);
  }

  void PreSplitTabletPhaseHandler(const SplitTabletPhase&);
  void UnloadTabletPhaseHandler(const SplitTabletPhase&);
  void PostUnloadTabletPhaseHandler(const SplitTabletPhase&);
  void UpdateMetaPhaseHandler(const SplitTabletPhase&);
  void LoadTabletsPhaseHandler(const SplitTabletPhase&);
  void FaultRecoverPhaseHandler(const SplitTabletPhase&);
  void EOFPhaseHandler(const SplitTabletPhase&);

  void ComputeSplitKeyAsync();

  void ComputeSplitKeyCallback(SplitTabletRequest* request, SplitTabletResponse* response,
                               bool failed, int error_code);

  bool TabletStatusCheck();

  void UpdateMeta();
  void UpdateMetaDone(bool);

 private:
  const std::string id_;
  std::mutex mutex_;
  TabletPtr tablet_;
  bool done_ = false;
  std::string split_key_;
  bool dispatch_split_key_request_ = false;
  std::shared_ptr<Procedure> unload_proc_;

  TabletPtr child_tablets_[2];
  std::shared_ptr<Procedure> load_procs_[2];
  std::vector<SplitTabletPhase> phases_;

  std::shared_ptr<Procedure> recover_proc_;
  static std::map<SplitTabletPhase, SplitTabletPhaseHandler> phase_handlers_;
  ThreadPool* thread_pool_;
};
}
}
