// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <glog/logging.h>
#include "common/thread_pool.h"
#include "master/procedure.h"

namespace tera {
namespace master {

class ProcedureExecutor;

class ProcedureWrapper {
 public:
  explicit ProcedureWrapper(std::shared_ptr<Procedure> proc)
      : scheduling_(false), proc_(proc), got_lock_(false) {}
  void RunNextStage(std::shared_ptr<ProcedureExecutor> proc_executor);

  bool Done() { return proc_->Done(); }

  std::string ProcId() { return proc_->ProcId(); }

  bool TrySchedule() {
    if (!TryGetLock()) {
      return false;
    }
    if (proc_->Done() || scheduling_) {
      return false;
    }
    scheduling_.store(true);
    return true;
  }

  bool TryGetLock() {
    if (got_lock_) {
      return true;
    }
    if (!ProcedureLimiter::Instance().GetLock(proc_->GetLockType())) {
      return false;
    }
    got_lock_ = true;
    return true;
  }

  std::atomic<bool> scheduling_;
  std::shared_ptr<Procedure> proc_;
  bool got_lock_;
};

class ProcedureExecutor : public std::enable_shared_from_this<ProcedureExecutor> {
 public:
  ProcedureExecutor();

  ~ProcedureExecutor() { Stop(); }

  bool Start();
  void Stop();

  uint64_t AddProcedure(std::shared_ptr<Procedure> proc);

  void ScheduleProcedures();

 private:
  bool RemoveProcedure(const std::string& proc_id);
  friend class ProcedureWrapper;

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> running_;

  uint64_t proc_index_;
  std::map<std::string, uint64_t> procedure_indexs_;
  // use integer as map key thus we can schedule Procedures
  // according to the order as they are added to the map
  std::map<uint64_t, std::shared_ptr<ProcedureWrapper>> procedures_;

  // ThreadPool used to run
  std::shared_ptr<ThreadPool> thread_pool_;
  // polling all Procedures and add Procedure can be scheduled to thread_pool,
  // running the
  // Procedure background in thread_pool_
  // A procedure may be scheduled several times until the Procedure is Done
  std::thread schedule_thread_;
};
}
}
