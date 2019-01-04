// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <glog/logging.h>
#include "db/filename.h"
#include "io/utils_leveldb.h"
#include "proto/status_code.pb.h"
#include "master/create_table_procedure.h"
#include "master/load_tablet_procedure.h"
#include "master/master_env.h"
#include "master/procedure_executor.h"

DECLARE_int32(tera_max_pre_assign_tablet_num);
DECLARE_int64(tera_tablet_write_block_size);
DECLARE_int32(tera_master_meta_retry_times);
DECLARE_bool(tera_acl_enabled);
DECLARE_bool(tera_only_root_create_table);

namespace tera {
namespace master {

std::map<CreateTablePhase,
         CreateTableProcedure::CreateTablePhaseHandler> CreateTableProcedure::phase_handlers_{
    {CreateTablePhase::kPrepare, std::bind(&CreateTableProcedure::PreCheckHandler, _1, _2)},
    {CreateTablePhase::kUpdateMeta, std::bind(&CreateTableProcedure::UpdateMetaHandler, _1, _2)},
    {CreateTablePhase::kLoadTablets, std::bind(&CreateTableProcedure::LoadTabletsHandler, _1, _2)},
    {CreateTablePhase::kEofPhase, std::bind(&CreateTableProcedure::EofHandler, _1, _2)}};

CreateTableProcedure::CreateTableProcedure(const CreateTableRequest* request,
                                           CreateTableResponse* response,
                                           google::protobuf::Closure* closure,
                                           ThreadPool* thread_pool)
    : request_(request),
      response_(response),
      rpc_closure_(closure),
      table_name_(request_->table_name()),
      update_meta_(false),
      done_(false),
      thread_pool_(thread_pool) {
  PROC_LOG(INFO) << "create table: " << table_name_ << " begin";
  SetNextPhase(CreateTablePhase::kPrepare);
}

std::string CreateTableProcedure::ProcId() const {
  static std::string prefix("CreateTable:");
  return prefix + std::string(table_name_);
}

void CreateTableProcedure::RunNextStage() {
  CreateTablePhase phase = GetCurrentPhase();
  auto it = phase_handlers_.find(phase);
  PROC_CHECK(it != phase_handlers_.end()) << "illegal phase: " << phase
                                          << ", table: " << table_name_;
  CreateTablePhaseHandler handler = it->second;
  handler(this, phase);
}

void CreateTableProcedure::PreCheckHandler(const CreateTablePhase&) {
  {
    TablePtr table;
    if (MasterEnv().GetTabletManager()->FindTable(request_->table_name(), &table)) {
      PROC_LOG(ERROR) << "Fail to create table: " << request_->table_name()
                      << ", table already exist";
      EnterEofPhaseWithResponseStatus(kTableExist);
      return;
    }
    if (FLAGS_tera_acl_enabled && !MasterEnv().GetMaster()->IsRootUser(request_->user_token()) &&
        FLAGS_tera_only_root_create_table) {
      EnterEofPhaseWithResponseStatus(kNotPermission);
      return;
    }
  }

  // try clean env, if there is a dir same as table_name, delete it first
  if (!io::MoveEnvDirToTrash(request_->table_name())) {
    PROC_LOG(ERROR) << "Fail to create table: " << request_->table_name()
                    << ", cannot move old table dir to trash";
    EnterEofPhaseWithResponseStatus(kTableExist);
    return;
  }

  int32_t tablet_num = request_->delimiters_size() + 1;
  bool delivalid = true;
  for (int32_t i = 1; i < tablet_num - 1; i++) {
    // TODO: Use user defined comparator
    if (request_->delimiters(i) <= request_->delimiters(i - 1)) {
      delivalid = false;
      break;
    }
  }
  if (tablet_num > FLAGS_tera_max_pre_assign_tablet_num || !delivalid ||
      request_->schema().locality_groups_size() < 1) {
    if (tablet_num > FLAGS_tera_max_pre_assign_tablet_num) {
      PROC_LOG(WARNING) << "Too many pre-create tablets " << tablet_num;
    } else if (!delivalid) {
      PROC_LOG(WARNING) << "Invalid delimiters for " << request_->table_name();
    } else {
      PROC_LOG(WARNING) << "No LocalityGroupSchema for " << request_->table_name();
    }
    EnterEofPhaseWithResponseStatus(kInvalidArgument);
    return;
  }

  const std::string& table_name = request_->table_name();
  StatusCode status = kMasterOk;
  tablets_.reserve(tablet_num);
  meta_records_.reserve(tablet_num + 1);

  table_ = TabletManager::CreateTable(table_name, request_->schema(), kTableEnable);
  table_->LockTransition();
  PackMetaWriteRecords(table_, false, meta_records_);
  for (int32_t i = 1; i <= tablet_num; ++i) {
    std::string path = leveldb::GetTabletPathFromNum(request_->table_name(), i);
    const std::string& start_key = (i == 1) ? "" : request_->delimiters(i - 2);
    const std::string& end_key = (i == tablet_num) ? "" : request_->delimiters(i - 1);
    TabletMeta meta;
    TabletManager::PackTabletMeta(&meta, table_name, start_key, end_key, path, "",
                                  TabletMeta::kTabletOffline,
                                  FLAGS_tera_tablet_write_block_size * 1024);
    TabletPtr tablet = table_->AddTablet(meta, &status);
    if (!tablet) {
      PROC_LOG(WARNING) << "add tablet failed" << meta.path()
                        << ", errcode: " << StatusCodeToString(status);
      EnterEofPhaseWithResponseStatus(status);
      MasterEnv().GetTabletManager()->DeleteTable(table_name_, &status);
      return;
    }

    tablet->LockTransition();
    PackMetaWriteRecords(tablet, false, meta_records_);
    tablets_.emplace_back(tablet);
  }
  if (!MasterEnv().GetTabletManager()->AddTable(table_, &status)) {
    PROC_LOG(ERROR) << "Fail to create table: " << request_->table_name()
                    << ", table already exist";
    EnterEofPhaseWithResponseStatus(kTableExist);
    return;
  }

  const LocalityGroupSchema& lg0 = request_->schema().locality_groups(0);
  PROC_LOG(INFO) << "Begin to create table: " << request_->table_name()
                 << ", store_medium: " << lg0.store_type() << ", compress: " << lg0.compress_type()
                 << ", raw_key: " << request_->schema().raw_key() << ", has " << tablet_num
                 << " tablets, schema: " << request_->schema().ShortDebugString();
  SetNextPhase(CreateTablePhase::kUpdateMeta);
}

void CreateTableProcedure::UpdateMetaHandler(const CreateTablePhase&) {
  if (update_meta_) {
    return;
  }
  update_meta_.store(true);
  PROC_LOG(INFO) << "table: " << table_name_ << " begin to update meta";
  UpdateMetaClosure closure = std::bind(&CreateTableProcedure::UpdateMetaDone, this, _1);
  MasterEnv().BatchWriteMetaTableAsync(meta_records_, closure, FLAGS_tera_master_meta_retry_times);
}

void CreateTableProcedure::UpdateMetaDone(bool succ) {
  if (!succ) {
    PROC_LOG(WARNING) << "fail to update meta";
    EnterEofPhaseWithResponseStatus(kMetaTabletError);
    return;
  }
  LOG(INFO) << "create table " << table_->GetTableName() << " update meta success";
  EnterPhaseAndResponseStatus(kMasterOk, CreateTablePhase::kLoadTablets);
}

void CreateTableProcedure::LoadTabletsHandler(const CreateTablePhase&) {
  Scheduler* size_scheduler = MasterEnv().GetSizeScheduler().get();
  for (size_t i = 0; i < tablets_.size(); i++) {
    CHECK(tablets_[i]->GetStatus() == TabletMeta::kTabletOffline) << tablets_[i]->GetPath();
    TabletNodePtr dest_node;
    if (!MasterEnv().GetTabletNodeManager()->ScheduleTabletNodeOrWait(
            size_scheduler, table_name_, tablets_[i], false, &dest_node)) {
      LOG(ERROR) << "no available tabletnode, abort load tablet: " << tablets_[i];
      continue;
    }
    std::shared_ptr<LoadTabletProcedure> proc(
        new LoadTabletProcedure(tablets_[i], dest_node, thread_pool_));
    MasterEnv().GetExecutor()->AddProcedure(proc);
  }
  SetNextPhase(CreateTablePhase::kEofPhase);
}

void CreateTableProcedure::EofHandler(const CreateTablePhase&) {
  PROC_LOG(INFO) << "create table: " << table_name_ << " finish";
  done_.store(true);
  // unlike DisableTableProcedure, here we do not ensure that all tablets been
  // loaded successfully
  // and just finish the CreateTableProcedure early as all LoadTabletProcedure
  // been added to ProcedureExecutor.
  if (table_ && table_->InTransition()) {
    table_->UnlockTransition();
  }
  rpc_closure_->Run();
}

bool CreateTableProcedure::Done() { return done_; }

std::ostream& operator<<(std::ostream& o, const CreateTablePhase& phase) {
  static const char* msg[] = {"CreateTablePhase::kPrepare", "CreateTablePhase::kUpdateMeta",
                              "CreateTablePhase::kLoadTablets", "CreateTablePhase::kEofPhase",
                              "CreateTablePhase::kUnknown"};
  static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
  typedef std::underlying_type<CreateTablePhase>::type UnderType;
  uint32_t index =
      static_cast<UnderType>(phase) - static_cast<UnderType>(CreateTablePhase::kPrepare);
  index = index < msg_size ? index : msg_size - 1;
  o << msg[index];
  return o;
}
}
}
