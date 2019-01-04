// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <glog/logging.h>
#include "unload_tablet_procedure.h"
#include "proto/tabletnode_client.h"
#include "master/master_env.h"
#include "types.h"

DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_master_meta_table_path);
DECLARE_int32(tera_master_unload_rpc_timeout);
DECLARE_int32(tera_master_impl_retry_times);
DECLARE_int32(tera_master_control_tabletnode_retry_period);
// DECLARE_bool(tera_master_kick_tabletnode_enabled);

namespace tera {
namespace master {

std::map<TabletEvent,
         UnloadTabletProcedure::UnloadTabletEventHandler> UnloadTabletProcedure::event_handlers_{
    {TabletEvent::kTsUnloadBusy, std::bind(&UnloadTabletProcedure::TsUnloadBusyHandler, _1, _2)},
    {TabletEvent::kUnLoadTablet, std::bind(&UnloadTabletProcedure::UnloadTabletHandler, _1, _2)},
    {TabletEvent::kWaitRpcResponse,
     std::bind(&UnloadTabletProcedure::WaitRpcResponseHandler, _1, _2)},
    {TabletEvent::kTsUnLoadSucc,
     std::bind(&UnloadTabletProcedure::UnloadTabletSuccHandler, _1, _2)},
    {TabletEvent::kTsOffline, std::bind(&UnloadTabletProcedure::UnloadTabletSuccHandler, _1, _2)},
    {TabletEvent::kTsUnLoadFail,
     std::bind(&UnloadTabletProcedure::UnloadTabletFailHandler, _1, _2)},
    {TabletEvent::kEofEvent, std::bind(&UnloadTabletProcedure::EOFHandler, _1, _2)}};

UnloadTabletProcedure::UnloadTabletProcedure(TabletPtr tablet, ThreadPool* thread_pool,
                                             bool is_sub_proc)
    : id_(std::string("UnloadTablet:") + tablet->GetPath() + ":" + TimeStamp()),
      tablet_(tablet),
      unload_retrys_(0),
      unload_request_dispatching_(false),
      kick_ts_succ_(true),
      done_(false),
      ts_unload_busying_(false),
      is_sub_proc_(is_sub_proc),
      thread_pool_(thread_pool) {
  PROC_LOG(INFO) << "unload tablet begin, tablet: " << tablet_;
  // I played a trick here by setting tablet status to kTableReady when user
  // want to unload a
  // tablet currently in status kTableUnloading
  if (tablet_->GetStatus() == TabletMeta::kTabletUnloading ||
      tablet_->GetStatus() == TabletMeta::kTabletUnloadFail) {
    tablet_->SetStatus(TabletMeta::kTabletReady);
  }
  if (tablet_->GetStatus() == TabletMeta::kTabletDelayOffline) {
    tablet_->DoStateTransition(TabletEvent::kTsOffline);
  }
}

std::string UnloadTabletProcedure::ProcId() const { return id_; }

TabletEvent UnloadTabletProcedure::GenerateEvent() {
  if (tablet_->GetStatus() == TabletMeta::kTabletOffline ||
      tablet_->GetStatus() == TabletMeta::kTabletUnloadFail) {
    return TabletEvent::kEofEvent;
  }
  if (tablet_->GetTabletNode()->NodeDown()) {
    return TabletEvent::kTsOffline;
  }
  if (tablet_->GetStatus() == TabletMeta::kTabletReady) {
    if (!tablet_->GetTabletNode()->CanUnload()) {
      return TabletEvent::kTsUnloadBusy;
    }
    return TabletEvent::kUnLoadTablet;
  }

  if (tablet_->GetStatus() == TabletMeta::kTabletUnloading) {
    if (unload_request_dispatching_) {
      return TabletEvent::kWaitRpcResponse;
    }
    if (unload_retrys_ <= FLAGS_tera_master_impl_retry_times) {
      return TabletEvent::kTsUnLoadSucc;
    }
    // if exhausted all unload retries, wait kick result
    if (kick_ts_succ_) {
      return TabletEvent::kWaitRpcResponse;
    }
    return TabletEvent::kTsUnLoadFail;
  }
  return TabletEvent::kEofEvent;
}

void UnloadTabletProcedure::RunNextStage() {
  TabletEvent event = GenerateEvent();
  auto it = event_handlers_.find(event);
  PROC_CHECK(it != event_handlers_.end()) << "illegal event: " << event << ", tablet: " << tablet_;
  UnloadTabletEventHandler handler = it->second;
  handler(this, event);
}

void UnloadTabletProcedure::TsUnloadBusyHandler(const TabletEvent& event) {
  // only log once the first time we enter this handler, avoiding redundant
  // logging
  PROC_VLOG_IF(23, !ts_unload_busying_) << "ts unload busy, tablet: " << tablet_;
  ts_unload_busying_ = true;
}

void UnloadTabletProcedure::UnloadTabletHandler(const TabletEvent& event) {
  tablet_->DoStateTransition(event);
  unload_request_dispatching_.store(true);
  PROC_LOG(INFO) << "dispatch UNLOAD tablet request to ts, tablet: " << tablet_
                 << ", server: " << tablet_->GetServerAddr();
  UnloadTabletAsync();
}

void UnloadTabletProcedure::WaitRpcResponseHandler(const TabletEvent&) {}

void UnloadTabletProcedure::UnloadTabletSuccHandler(const TabletEvent& event) {
  tablet_->DoStateTransition(event);
  tablet_->GetTabletNode()->FinishUnload();
  PROC_LOG(INFO) << "tablet unload success, tablet: " << tablet_;
}

// currently we will never enter this handler as we always kick tabletnode once
// all unload tries failed
void UnloadTabletProcedure::UnloadTabletFailHandler(const TabletEvent& event) {
  tablet_->GetTabletNode()->FinishUnload();
  tablet_->DoStateTransition(event);
  PROC_LOG(ERROR) << "tablet unload fail finally, tablet: " << tablet_;
}

void UnloadTabletProcedure::EOFHandler(const TabletEvent&) {
  PROC_LOG(INFO) << "UnloadTabletProcedure Done " << tablet_;
  if (!is_sub_proc_) {
    tablet_->UnlockTransition();
  }
  done_.store(true);
}

void UnloadTabletProcedure::UnloadTabletAsyncWrapper(
    std::weak_ptr<UnloadTabletProcedure> weak_proc) {
  auto proc = weak_proc.lock();
  if (!proc) {
    LOG(WARNING) << "weak_ptr expired, giveup the unloadtabletasync";
    return;
  }
  return proc->UnloadTabletAsync();
}

void UnloadTabletProcedure::UnloadTabletAsync() {
  TabletNodePtr node = tablet_->GetTabletNode();
  if (node->NodeDown()) {
    PROC_LOG(WARNING) << "tabletnode down ,giveup this unload rpc try, tablet: " << tablet_;
    return;
  }
  tabletnode::TabletNodeClient node_client(thread_pool_, tablet_->GetServerAddr(),
                                           FLAGS_tera_master_unload_rpc_timeout);
  UnloadTabletRequest* request = new UnloadTabletRequest;
  UnloadTabletResponse* response = new UnloadTabletResponse;
  request->set_sequence_id(MasterEnv().SequenceId().Inc());
  request->set_tablet_name(tablet_->GetTableName());
  request->mutable_key_range()->set_key_start(tablet_->GetKeyStart());
  request->mutable_key_range()->set_key_end(tablet_->GetKeyEnd());
  request->set_session_id(node->uuid_);

  PROC_LOG(INFO) << "UnloadTabletAsync id: " << request->sequence_id() << ", " << tablet_;
  // the explicit cast from shared_ptr to weak_ptr is necessary!
  UnloadClosure done =
      std::bind(&UnloadTabletProcedure::UnloadTabletCallbackWrapper,
                std::weak_ptr<UnloadTabletProcedure>(shared_from_this()), _1, _2, _3, _4);
  node_client.UnloadTablet(request, response, done);
}

void UnloadTabletProcedure::UnloadTabletCallbackWrapper(
    std::weak_ptr<UnloadTabletProcedure> weak_proc, UnloadTabletRequest* request,
    UnloadTabletResponse* response, bool failed, int error_code) {
  auto proc = weak_proc.lock();
  if (!proc) {
    LOG(WARNING) << "weak_ptr expired, giveup the unloadtabletcallback";
    return;
  }
  return proc->UnloadTabletCallback(request, response, failed, error_code);
}

void UnloadTabletProcedure::UnloadTabletCallback(UnloadTabletRequest* request,
                                                 UnloadTabletResponse* response, bool failed,
                                                 int error_code) {
  StatusCode status = response->status();
  std::unique_ptr<UnloadTabletRequest> request_holder(request);
  std::unique_ptr<UnloadTabletResponse> response_holder(response);
  uint64_t sequence_id = request->sequence_id();
  TabletNodePtr node = tablet_->GetTabletNode();
  // we regard TS offline/restart as TabletUnload succ
  if (node->NodeDown() || (!failed && (status == kTabletNodeOk || status == kKeyNotInRange))) {
    unload_request_dispatching_.store(false);
    PROC_LOG(INFO) << "id: " << sequence_id << ", unload tablet succ, tablet: " << tablet_;
    return;
  }

  bool ts_ctrl_busy = (!failed && status == kTabletNodeIsBusy);
  unload_retrys_ = (ts_ctrl_busy ? unload_retrys_ : unload_retrys_ + 1);
  std::string errmsg =
      (failed ? sofa::pbrpc::RpcErrorCodeToString(error_code) : StatusCodeToString(status));
  PROC_LOG_IF(WARNING, ts_ctrl_busy)
      << "id: " << sequence_id
      << ", ts is too busy, unload request is rejected and will retry later, "
         "tablet: " << tablet_;
  PROC_LOG_IF(WARNING, !ts_ctrl_busy) << "id: " << sequence_id << ", unload tablet failed, "
                                      << tablet_ << ", " << unload_retrys_
                                      << "th attemp, error: " << errmsg;

  // failed and has no more retry times, return immediately
  if (unload_retrys_ > FLAGS_tera_master_impl_retry_times) {
    unload_request_dispatching_.store(false);
    kick_ts_succ_ = MasterEnv().GetMaster()->TryKickTabletNode(tablet_->GetTabletNode());

    PROC_LOG_IF(ERROR, kick_ts_succ_) << kSms << "unload tablet failed finally, " << tablet_
                                      << " kick ts succ: " << tablet_->GetTabletNode()->GetAddr();
    PROC_LOG_IF(ERROR, !kick_ts_succ_)
        << kSms << "Unload tablet failed finally, kick ts failed too, " << tablet_;
    return;
  }
  // the explicit cast from shared_ptr to weak_ptr is necessary!
  ThreadPool::Task retry_task = std::bind(&UnloadTabletProcedure::UnloadTabletAsyncWrapper,
                                          std::weak_ptr<UnloadTabletProcedure>(shared_from_this()));
  MasterEnv().GetThreadPool()->DelayTask(FLAGS_tera_master_control_tabletnode_retry_period,
                                         retry_task);
}
}
}
