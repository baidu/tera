// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <functional>
#include <mutex>
#include <memory>
#include <vector>
#include "master/procedure.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"

namespace tera {
namespace master {

enum class MoveTabletPhase {
  kUnLoadTablet,
  kLoadTablet,
  kEofPhase,
};

std::ostream& operator<<(std::ostream& o, const MoveTabletPhase& phase);

// Notice that MoveTabletProcedure is splitted into a UnloadTabletProcedure and
// a LoadTabletProcedure,
// so MoveTabletProcedure do not deal with tablet state transition directly
class MoveTabletProcedure : public Procedure {
 public:
  MoveTabletProcedure(TabletPtr tablet, TabletNodePtr node, ThreadPool* thread_pool);

  virtual ~MoveTabletProcedure() {}

  virtual std::string ProcId() const;

  virtual void RunNextStage();

  virtual bool Done() { return done_; }

 private:
  typedef std::function<void(MoveTabletProcedure*, const MoveTabletPhase&)> MoveTabletPhaseHandler;

  MoveTabletPhase GetCurrentPhase() { return phases_.back(); }

  void SetNextPhase(const MoveTabletPhase& phase) { phases_.emplace_back(phase); }

  void UnloadTabletPhaseHandler(const MoveTabletPhase& phase);
  void LoadTabletPhaseHandler(const MoveTabletPhase& phase);
  void EOFPhaseHandler(const MoveTabletPhase& phase);

 private:
  const std::string id_;
  std::mutex mutex_;
  TabletPtr tablet_;
  TabletNodePtr dest_node_;
  bool done_;
  std::shared_ptr<Procedure> unload_proc_;
  std::shared_ptr<Procedure> load_proc_;
  std::vector<MoveTabletPhase> phases_;
  static std::map<MoveTabletPhase, MoveTabletPhaseHandler> phase_handlers_;
  ThreadPool* thread_pool_;
};
}
}
