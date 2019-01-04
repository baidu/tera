// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <glog/logging.h>
#include "db/filename.h"
#include "io/utils_leveldb.h"
#include "load_tablet_procedure.h"
#include "master/master_env.h"
#include "master/merge_tablet_procedure.h"
#include "master/procedure_executor.h"
#include "proto/tabletnode_client.h"
#include "unload_tablet_procedure.h"

DECLARE_double(tera_master_workload_merge_threshold);
DECLARE_string(tera_tabletnode_path_prefix);

namespace tera {
namespace master {

std::map<MergeTabletPhase, MergeTabletProcedure::MergeTabletPhaseHandler>
    MergeTabletProcedure::phase_handlers_{
        {MergeTabletPhase::kUnLoadTablets,
         std::bind(&MergeTabletProcedure::UnloadTabletsPhaseHandler, _1, _2)},
        {MergeTabletPhase::kPostUnLoadTablets,
         std::bind(&MergeTabletProcedure::PostUnloadTabletsPhaseHandler, _1, _2)},
        {MergeTabletPhase::kUpdateMeta,
         std::bind(&MergeTabletProcedure::UpdateMetaPhaseHandler, _1, _2)},
        {MergeTabletPhase::kLoadMergedTablet,
         std::bind(&MergeTabletProcedure::LoadMergedTabletPhaseHandler, _1, _2)},
        {MergeTabletPhase::kFaultRecover,
         std::bind(&MergeTabletProcedure::FaultRecoverPhaseHandler, _1, _2)},
        {MergeTabletPhase::kEofPhase, std::bind(&MergeTabletProcedure::EOFPhaseHandler, _1, _2)}};

MergeTabletProcedure::MergeTabletProcedure(TabletPtr first, TabletPtr second,
                                           ThreadPool* thread_pool)
    : Procedure(ProcedureLimiter::LockType::kMerge),
      id_(std::string("MergeTablet:") + first->GetPath() + ":" + second->GetPath() + ":" +
          TimeStamp()),
      tablets_{first, second},
      thread_pool_(thread_pool) {
  PROC_LOG(INFO) << "merge tablet begin, tablets, first: " << tablets_[0]
                 << ", second: " << tablets_[1];
  if (tablets_[0]->GetStatus() != TabletMeta::kTabletReady ||
      tablets_[1]->GetStatus() != TabletMeta::kTabletReady) {
    PROC_LOG(WARNING) << "tablets not ready, giveup this merge";
    SetNextPhase(MergeTabletPhase::kEofPhase);
    return;
  }
  // check KeyRange
  if (tablets_[0]->GetKeyEnd() != tablets_[1]->GetKeyStart() &&
      tablets_[1]->GetKeyEnd() != tablets_[0]->GetKeyStart()) {
    PROC_LOG(WARNING) << "invalid merge peers: first: " << tablets_[0]
                      << ", second: " << tablets_[1];
    SetNextPhase(MergeTabletPhase::kEofPhase);
    return;
  }
  SetNextPhase(MergeTabletPhase::kUnLoadTablets);
}

std::string MergeTabletProcedure::ProcId() const { return id_; }

void MergeTabletProcedure::RunNextStage() {
  MergeTabletPhase phase = GetCurrentPhase();
  auto it = phase_handlers_.find(phase);
  PROC_CHECK(it != phase_handlers_.end()) << "illegal phase: " << phase
                                          << ", tablet: " << tablets_[0];
  MergeTabletPhaseHandler handler = it->second;
  handler(this, phase);
}

void MergeTabletProcedure::UnloadTabletsPhaseHandler(const MergeTabletPhase&) {
  if (!unload_procs_[0] && !unload_procs_[1]) {
    unload_procs_[0].reset(new UnloadTabletProcedure(tablets_[0], thread_pool_, true));
    unload_procs_[1].reset(new UnloadTabletProcedure(tablets_[1], thread_pool_, true));
    PROC_LOG(INFO) << "Generate UnloadTablet SubProcedure1: " << unload_procs_[0]->ProcId()
                   << " SubProcedure2: " << unload_procs_[1]->ProcId();
    MasterEnv().GetExecutor()->AddProcedure(unload_procs_[0]);
    MasterEnv().GetExecutor()->AddProcedure(unload_procs_[1]);
  }
  // both unload_procs_[0] and unload_procs_[1] should not be nullptr
  PROC_CHECK(unload_procs_[0] && unload_procs_[1]);
  // wait both tablets unload finish
  if (!unload_procs_[0]->Done() || !unload_procs_[1]->Done()) {
    return;
  }
  TabletMeta::TabletStatus status0 = tablets_[0]->GetStatus();
  TabletMeta::TabletStatus status1 = tablets_[1]->GetStatus();
  if (status0 != TabletMeta::kTabletOffline || status1 != TabletMeta::kTabletOffline) {
    PROC_LOG(WARNING) << "unload tablets not ok, tablet: " << tablets_[0]
                      << ", status: " << StatusCodeToString(status0) << ", tablet: " << tablets_[1]
                      << ", status: " << StatusCodeToString(status1);
    SetNextPhase(MergeTabletPhase::kEofPhase);
  } else {
    SetNextPhase(MergeTabletPhase::kPostUnLoadTablets);
  }
}

void MergeTabletProcedure::PostUnloadTabletsPhaseHandler(const MergeTabletPhase&) {
  if (!TabletStateCheck()) {
    SetNextPhase(MergeTabletPhase::kFaultRecover);
    return;
  }
  SetNextPhase(MergeTabletPhase::kUpdateMeta);
}

void MergeTabletProcedure::UpdateMetaPhaseHandler(const MergeTabletPhase&) {
  // update meta asynchronously
  if (!merged_) {
    UpdateMeta();
  }
}

void MergeTabletProcedure::LoadMergedTabletPhaseHandler(const MergeTabletPhase&) {
  if (!load_proc_) {
    load_proc_.reset(new LoadTabletProcedure(merged_, dest_node_, thread_pool_, true));
    PROC_LOG(INFO) << "Generate LoadTablet SubProcedure: " << load_proc_->ProcId()
                   << "merged: " << merged_ << ", destnode: " << dest_node_->GetAddr();
    MasterEnv().GetExecutor()->AddProcedure(load_proc_);
  }
  SetNextPhase(MergeTabletPhase::kEofPhase);
}

void MergeTabletProcedure::FaultRecoverPhaseHandler(const MergeTabletPhase&) {
  PROC_CHECK(phases_.size() >= 2 && GetCurrentPhase() == MergeTabletPhase::kFaultRecover);
  if (!recover_procs_[0]) {
    recover_procs_[0].reset(
        new LoadTabletProcedure(tablets_[0], tablets_[0]->GetTabletNode(), thread_pool_, true));
    recover_procs_[1].reset(
        new LoadTabletProcedure(tablets_[1], tablets_[1]->GetTabletNode(), thread_pool_, true));
    MasterEnv().GetExecutor()->AddProcedure(recover_procs_[0]);
    MasterEnv().GetExecutor()->AddProcedure(recover_procs_[1]);
    PROC_LOG(INFO) << "[merge] rollback " << tablets_[0]
                   << ", SubProcedure: " << recover_procs_[0]->ProcId();
    PROC_LOG(INFO) << "[merge] rollback " << tablets_[1]
                   << ", SubProcedure: " << recover_procs_[1]->ProcId();
    return;
  }
  SetNextPhase(MergeTabletPhase::kEofPhase);
}

void MergeTabletProcedure::EOFPhaseHandler(const MergeTabletPhase&) {
  if (!recover_procs_[0]) {
    tablets_[0]->UnlockTransition();
  }
  if (!recover_procs_[1]) {
    tablets_[1]->UnlockTransition();
  }
  PROC_LOG_IF(INFO, !merged_) << "merge finished abort, first: " << tablets_[0]
                              << ", second: " << tablets_[1];
  PROC_LOG_IF(INFO, merged_) << "merge finished done, merged: " << merged_
                             << "first: " << tablets_[0] << ", second: " << tablets_[1];
  done_ = true;
}

bool MergeTabletProcedure::TabletStateCheck() {
  leveldb::Env* env = io::LeveldbBaseEnv();
  for (size_t i = 0; i < sizeof(tablets_) / sizeof(TabletPtr); ++i) {
    std::vector<std::string> children;
    std::string tablet_path = FLAGS_tera_tabletnode_path_prefix + "/" + tablets_[i]->GetPath();
    // NOTICE:
    env->GetChildren(tablet_path, &children);
    leveldb::Status status = env->GetChildren(tablet_path, &children);
    if (!status.ok()) {
      PROC_LOG(WARNING) << "[merge] abort, " << tablets_[i]
                        << ", tablet status check error: " << status.ToString();
      return false;
    }
    for (size_t j = 0; j < children.size(); ++j) {
      leveldb::FileType type = leveldb::kUnknown;
      uint64_t number = 0;
      if (ParseFileName(children[j], &number, &type) && type == leveldb::kLogFile) {
        PROC_LOG(WARNING) << "[merge] abort, " << tablets_[i] << ", tablet log not clear.";
        return false;
      }
    }
  }
  return true;
}

void MergeTabletProcedure::UpdateMeta() {
  std::vector<MetaWriteRecord> records;
  PackMetaWriteRecords(tablets_[0], true, records);
  PackMetaWriteRecords(tablets_[1], true, records);

  TabletMeta new_meta;
  // <tablets_[1], tablets_[0]>
  if (tablets_[0]->GetKeyStart() == tablets_[1]->GetKeyEnd() && tablets_[0]->GetKeyStart() != "") {
    tablets_[1]->ToMeta(&new_meta);
    new_meta.mutable_key_range()->set_key_end(tablets_[0]->GetKeyEnd());
    new_meta.clear_parent_tablets();
    new_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(tablets_[1]->GetPath()));
    new_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(tablets_[0]->GetPath()));
  }
  // <tablets_[0], tablets_[1]>
  else if (tablets_[0]->GetKeyEnd() == tablets_[1]->GetKeyStart()) {
    tablets_[0]->ToMeta(&new_meta);
    new_meta.mutable_key_range()->set_key_end(tablets_[1]->GetKeyEnd());
    new_meta.clear_parent_tablets();
    new_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(tablets_[0]->GetPath()));
    new_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(tablets_[1]->GetPath()));
  } else {
    PROC_LOG(FATAL) << "tablet range error, cannot be merged" << tablets_[0] << ", " << tablets_[1];
  }

  new_meta.set_status(TabletMeta::kTabletOffline);
  std::string new_path = leveldb::GetChildTabletPath(tablets_[0]->GetPath(),
                                                     tablets_[0]->GetTable()->GetNextTabletNo());
  new_meta.set_path(new_path);
  new_meta.set_size(tablets_[0]->GetDataSize() + tablets_[1]->GetDataSize());
  uint64_t version = tablets_[0]->Version() > tablets_[1]->Version() ? tablets_[0]->Version()
                                                                     : tablets_[1]->Version();
  new_meta.set_version(version + 1);
  merged_.reset(new Tablet(new_meta, tablets_[0]->GetTable()));

  dest_node_ =
      (tablets_[0]->GetDataSize() > tablets_[1]->GetDataSize() ? tablets_[0]->GetTabletNode()
                                                               : tablets_[1]->GetTabletNode());
  PackMetaWriteRecords(merged_, false, records);
  UpdateMetaClosure done = std::bind(&MergeTabletProcedure::MergeUpdateMetaDone, this, _1);
  PROC_LOG(INFO) << "[merge] update meta, tablet: [" << tablets_[0]->GetPath() << ", "
                 << tablets_[1]->GetPath() << "]";
  // update meta table asynchronously until meta write ok.
  MasterEnv().BatchWriteMetaTableAsync(records, done, -1);
}

// will be called when update meta finish successfully, and set next process
// phase be LOAD_MERGED_TABLET
void MergeTabletProcedure::MergeUpdateMetaDone(bool) {
  tablets_[0]->DoStateTransition(TabletEvent::kFinishMergeTablet);
  tablets_[1]->DoStateTransition(TabletEvent::kFinishMergeTablet);
  TabletMeta new_meta;
  merged_->ToMeta(&new_meta);
  TablePtr table = merged_->GetTable();
  merged_->LockTransition();
  if (tablets_[0]->GetKeyStart() == merged_->GetKeyStart()) {
    // <tablets_[0], tablets_[1]>
    table->MergeTablets(tablets_[0], tablets_[1], new_meta, &merged_);
  } else {
    // <tablets_[1], tablets_[0]>
    table->MergeTablets(tablets_[1], tablets_[0], new_meta, &merged_);
  }
  SetNextPhase(MergeTabletPhase::kLoadMergedTablet);
}

std::ostream& operator<<(std::ostream& o, const MergeTabletPhase& phase) {
  static const char* msg[] = {
      "MergeTabletPhase::kUnLoadTablets", "MergeTabletPhase::kPostUnLoadTablets",
      "MergeTabletPhase::kUpdateMeta",    "MergeTabletPhase::kLoadMergedTablet",
      "MergeTabletPhase::kFaultRecover",  "MergeTabletPhase::kEofPhase",
      "MergeTabletPhase::UNKNOWN"};
  static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
  typedef std::underlying_type<MergeTabletPhase>::type UnderType;
  uint32_t index =
      static_cast<UnderType>(phase) - static_cast<UnderType>(MergeTabletPhase::kUnLoadTablets);
  index = index < msg_size ? index : msg_size - 1;
  o << msg[index];
  return o;
}
}
}
