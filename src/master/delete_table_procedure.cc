// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/delete_table_procedure.h"
#include "master/master_env.h"

DECLARE_int32(tera_master_meta_retry_times);

namespace tera {
namespace master {

std::map<DeleteTablePhase,
         DeleteTableProcedure::DeleteTablePhaseHandler> DeleteTableProcedure::phase_handlers_{
    {DeleteTablePhase::kPrepare, std::bind(&DeleteTableProcedure::PrepareHandler, _1, _2)},
    {DeleteTablePhase::kDeleteTable, std::bind(&DeleteTableProcedure::DeleteTableHandler, _1, _2)},
    {DeleteTablePhase::kUpdateMeta, std::bind(&DeleteTableProcedure::UpdateMetaHandler, _1, _2)},
    {DeleteTablePhase::kEofPhase, std::bind(&DeleteTableProcedure::EofPhaseHandler, _1, _2)}};

DeleteTableProcedure::DeleteTableProcedure(TablePtr table, const DeleteTableRequest* request,
                                           DeleteTableResponse* response,
                                           google::protobuf::Closure* closure,
                                           ThreadPool* thread_pool)
    : table_(table),
      request_(request),
      response_(response),
      rpc_closure_(closure),
      update_meta_(false),
      done_(false),
      thread_pool_(thread_pool) {
  PROC_LOG(INFO) << "begin delete table: " << table_->GetTableName();
  SetNextPhase(DeleteTablePhase::kPrepare);
}

std::string DeleteTableProcedure::ProcId() const {
  static std::string prefix("DeleteTable:");
  return prefix + table_->GetTableName();
}

void DeleteTableProcedure::RunNextStage() {
  DeleteTablePhase phase = GetCurrentPhase();
  auto it = phase_handlers_.find(phase);
  PROC_CHECK(it != phase_handlers_.end()) << "illegal phase:" << phase
                                          << ", table: " << table_->GetTableName();
  DeleteTablePhaseHandler handler = it->second;
  handler(this, phase);
}

void DeleteTableProcedure::PrepareHandler(const DeleteTablePhase&) {
  if (!MasterEnv().GetMaster()->HasPermission(request_, table_, "delete table")) {
    EnterPhaseWithResponseStatus(kNotPermission, DeleteTablePhase::kEofPhase);
    return;
  }
  SetNextPhase(DeleteTablePhase::kDeleteTable);
}

void DeleteTableProcedure::DeleteTableHandler(const DeleteTablePhase&) {
  std::vector<TabletPtr> tablets;
  table_->GetTablet(&tablets);
  for (size_t i = 0; i < tablets.size(); ++i) {
    TabletPtr tablet = tablets[i];
    if (tablet->GetStatus() != TabletMeta::kTabletDisable) {
      PROC_LOG(WARNING) << "tablet: " << tablet << " not in disabled status, "
                        << StatusCodeToString(tablet->GetStatus());
      EnterPhaseWithResponseStatus(StatusCode(tablet->GetStatus()), DeleteTablePhase::kEofPhase);
      return;
    }
    PackMetaWriteRecords(tablet, true, meta_records_);
  }
  if (!table_->DoStateTransition(TableEvent::kDeleteTable)) {
    PROC_LOG(WARNING) << "table: " << table_->GetTableName()
                      << ", current status: " << StatusCodeToString(table_->GetStatus());
    EnterPhaseWithResponseStatus(kTableNotSupport, DeleteTablePhase::kEofPhase);
    return;
  }
  PackMetaWriteRecords(table_, true, meta_records_);

  // delete quota setting store in meta table
  quota::MasterQuotaHelper::PackDeleteQuotaRecords(table_->GetTableName(), meta_records_);

  SetNextPhase(DeleteTablePhase::kUpdateMeta);
}

void DeleteTableProcedure::UpdateMetaHandler(const DeleteTablePhase&) {
  if (update_meta_) {
    return;
  }
  update_meta_.store(true);
  PROC_LOG(INFO) << "table: " << table_->GetTableName() << "begin to update meta";
  UpdateMetaClosure closure = std::bind(&DeleteTableProcedure::UpdateMetaDone, this, _1);
  MasterEnv().BatchWriteMetaTableAsync(meta_records_, closure, FLAGS_tera_master_meta_retry_times);
}

void DeleteTableProcedure::EofPhaseHandler(const DeleteTablePhase&) {
  done_.store(true);
  if (table_->InTransition()) {
    table_->UnlockTransition();
  }
  PROC_LOG(INFO) << "delete table: " << table_->GetTableName() << " finish";
  rpc_closure_->Run();
}

void DeleteTableProcedure::UpdateMetaDone(bool succ) {
  if (!succ) {
    PROC_LOG(WARNING) << "table: " << table_->GetTableName() << " update meta fail";
    EnterPhaseWithResponseStatus(kMetaTabletError, DeleteTablePhase::kEofPhase);
    return;
  }
  PROC_LOG(INFO) << "table: " << table_->GetTableName() << " update meta succ";
  if (!MasterEnv().GetQuotaEntry()->DelRecord(table_->GetTableName())) {
    PROC_LOG(WARNING) << "table: " << table_->GetTableName()
                      << " delete master memory quota cache failed";
  }
  StatusCode code;
  MasterEnv().GetTabletManager()->DeleteTable(table_->GetTableName(), &code);
  EnterPhaseWithResponseStatus(kMasterOk, DeleteTablePhase::kEofPhase);
}

std::ostream& operator<<(std::ostream& o, const DeleteTablePhase& phase) {
  static const char* msg[] = {"DeleteTablePhase::kPrepare", "DeleteTablePhase::kDeleteTable",
                              "DeleteTablePhase::kUpdateMeta", "DeleteTablePhase::kEofPhase",
                              "DeleteTablePhase::kUnknown"};
  static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
  typedef std::underlying_type<DeleteTablePhase>::type UnderType;
  uint32_t index =
      static_cast<UnderType>(phase) - static_cast<UnderType>(DeleteTablePhase::kPrepare);
  index = index < msg_size ? index : msg_size - 1;
  o << msg[index];
  return o;
}
}
}
