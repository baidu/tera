// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <functional>
#include <map>
#include "master/master_env.h"
#include "master/procedure.h"
#include "master/tablet_manager.h"
#include "proto/master_rpc.pb.h"

namespace tera {
namespace master {

enum class DeleteTablePhase {
  kPrepare,
  kDeleteTable,
  kUpdateMeta,
  kEofPhase,
};

std::ostream& operator<<(std::ostream& o, const DeleteTablePhase& phase);

class DeleteTableProcedure : public Procedure {
 public:
  DeleteTableProcedure(TablePtr table, const DeleteTableRequest* request,
                       DeleteTableResponse* response, google::protobuf::Closure* closure,
                       ThreadPool* thread_pool);

  virtual ~DeleteTableProcedure() {}

  virtual std::string ProcId() const;

  virtual void RunNextStage();

  virtual bool Done() { return done_.load(); }

 private:
  typedef std::function<void(DeleteTableProcedure*, const DeleteTablePhase&)>
      DeleteTablePhaseHandler;

  void SetNextPhase(const DeleteTablePhase& phase) {
    std::lock_guard<std::mutex> lock_guard(phase_mutex_);
    phases_.push_back(phase);
  }

  DeleteTablePhase GetCurrentPhase() {
    std::lock_guard<std::mutex> lock_guard(phase_mutex_);
    return phases_.back();
  }

  void EnterPhaseWithResponseStatus(StatusCode status, DeleteTablePhase phase) {
    response_->set_status(status);
    SetNextPhase(phase);
  }

  void PrepareHandler(const DeleteTablePhase& phase);
  void DeleteTableHandler(const DeleteTablePhase& phase);
  void UpdateMetaHandler(const DeleteTablePhase& phase);
  void EofPhaseHandler(const DeleteTablePhase& phase);

  void UpdateMetaDone(bool succ);

 private:
  TablePtr table_;
  const DeleteTableRequest* request_;
  DeleteTableResponse* response_;
  google::protobuf::Closure* rpc_closure_;
  std::string table_name_;
  std::atomic<bool> update_meta_;
  std::atomic<bool> done_;
  std::vector<MetaWriteRecord> meta_records_;
  std::mutex phase_mutex_;
  std::vector<DeleteTablePhase> phases_;
  static std::map<DeleteTablePhase, DeleteTablePhaseHandler> phase_handlers_;
  ThreadPool* thread_pool_;
};
}
}
