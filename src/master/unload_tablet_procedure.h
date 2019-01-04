// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include "master/procedure.h"
#include "master/tabletnode_manager.h"
#include "master/tablet_manager.h"

namespace tera {
namespace master {

class UnloadTabletProcedure : public Procedure,
                              public std::enable_shared_from_this<UnloadTabletProcedure> {
 public:
  UnloadTabletProcedure(TabletPtr tablet, ThreadPool* thread_pool, bool is_sub_proc = false);

  virtual ~UnloadTabletProcedure(){};

  virtual std::string ProcId() const;

  virtual void RunNextStage();

  virtual bool Done() { return done_.load(); }

  virtual ProcedureLimiter::LockType GetLockType() override {
    if (is_sub_proc_) {
      return ProcedureLimiter::LockType::kNoLimit;
    } else {
      return ProcedureLimiter::LockType::kUnload;
    }
  }

 private:
  typedef std::function<void(UnloadTabletProcedure*, const TabletEvent&)> UnloadTabletEventHandler;

  typedef std::function<void(UnloadTabletRequest*, UnloadTabletResponse*, bool, int)> UnloadClosure;

  TabletEvent GenerateEvent();

  static void UnloadTabletAsyncWrapper(std::weak_ptr<UnloadTabletProcedure> weak_proc);

  static void UnloadTabletCallbackWrapper(std::weak_ptr<UnloadTabletProcedure> weak_proc,
                                          UnloadTabletRequest* request,
                                          UnloadTabletResponse* response, bool failed,
                                          int error_code);

  void UnloadTabletAsync();

  void UnloadTabletCallback(UnloadTabletRequest* request, UnloadTabletResponse* response,
                            bool failed, int error_code);

  void TsUnloadBusyHandler(const TabletEvent& event);
  void UnloadTabletHandler(const TabletEvent& event);
  void WaitRpcResponseHandler(const TabletEvent&);
  void UnloadTabletSuccHandler(const TabletEvent& event);
  void UnloadTabletFailHandler(const TabletEvent& event);
  void EOFHandler(const TabletEvent&);

 private:
  const std::string id_;
  std::mutex mutex_;
  TabletPtr tablet_;
  int32_t unload_retrys_;
  std::atomic<bool> unload_request_dispatching_;
  std::atomic<bool> kick_ts_succ_;
  std::atomic<bool> done_;
  bool ts_unload_busying_;
  bool is_sub_proc_;
  static std::map<TabletEvent, UnloadTabletEventHandler> event_handlers_;
  ThreadPool* thread_pool_;
};
}
}
