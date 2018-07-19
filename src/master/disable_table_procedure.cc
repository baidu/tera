// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/disable_table_procedure.h"
#include "master/unload_tablet_procedure.h"
#include "master/master_env.h"
#include "master/master_impl.h"

DECLARE_int32(tera_master_meta_retry_times);

namespace tera {
namespace master {

std::map<DisableTablePhase, DisableTableProcedure::DisableTablePhaseHandler> DisableTableProcedure::phase_handlers_ {
    {DisableTablePhase::kPrepare, 
            std::bind(&DisableTableProcedure::PrepareHandler, _1, _2)},
    {DisableTablePhase::kDisableTable, 
            std::bind(&DisableTableProcedure::DisableTableHandler, _1, _2)},
    {DisableTablePhase::kUpdateMeta, 
            std::bind(&DisableTableProcedure::UpdateMetaHandler, _1, _2)},
    {DisableTablePhase::kDisableTablets, 
            std::bind(&DisableTableProcedure::DisableTabletsHandler, _1, _2)},
    {DisableTablePhase::kEofPhase, 
            std::bind(&DisableTableProcedure::EofPhaseHandler, _1, _2)}
};

DisableTableProcedure::DisableTableProcedure(TablePtr table, 
        const DisableTableRequest* request, DisableTableResponse* response, 
        google::protobuf::Closure* closure, ThreadPool* thread_pool) : 
    table_(table), 
    request_(request), 
    response_(response), 
    rpc_closure_(closure), 
    update_meta_(false),
    done_(false),
    thread_pool_(thread_pool) {
    PROC_LOG(INFO) << "begin disable table: " << table_->GetTableName();
    SetNextPhase(DisableTablePhase::kPrepare);
}

std::string DisableTableProcedure::ProcId() const {
    static std::string prefix("DisableTable:");
    return prefix + table_->GetTableName();
}

void DisableTableProcedure::RunNextStage() {
    DisableTablePhase phase = GetCurrentPhase();
    auto it = phase_handlers_.find(phase);
    PROC_CHECK(it != phase_handlers_.end()) << "illegal phase: " << phase << ", table: " << table_;
    DisableTablePhaseHandler handler = it->second;
    handler(this, phase);
}

void DisableTableProcedure::PrepareHandler(const DisableTablePhase&) {
    if (!MasterEnv().GetMaster()->HasPermission(request_, table_, "disable table")) {
        PROC_LOG(WARNING) << "disable table: " << table_->GetTableName() << " abort, permission denied";
        EnterPhaseWithResponseStatus(kNotPermission, DisableTablePhase::kEofPhase);
        return;
    }
    SetNextPhase(DisableTablePhase::kDisableTable);
}

void DisableTableProcedure::DisableTableHandler(const DisableTablePhase&) {
    if (!table_->DoStateTransition(TableEvent::kDisableTable)) {
        PROC_LOG(WARNING) << table_->GetTableName() 
            << " current state: " << table_->GetStatus() << ", disable failed";
        EnterPhaseWithResponseStatus(
                static_cast<StatusCode>(table_->GetStatus()), DisableTablePhase::kEofPhase);
        return;
    }
    SetNextPhase(DisableTablePhase::kUpdateMeta);
}

void DisableTableProcedure::UpdateMetaHandler(const DisableTablePhase&) {
    if (!update_meta_) {
        update_meta_.store(true);
        MetaWriteRecord record = PackMetaWriteRecord(table_, false);
        PROC_LOG(INFO) << "table: " << table_->GetTableName() 
            << " begin to update table disable info to meta";
        UpdateMetaClosure closure = std::bind(&DisableTableProcedure::UpdateMetaDone, this, _1);
        MasterEnv().BatchWriteMetaTableAsync(record, closure, FLAGS_tera_master_meta_retry_times);
    }
}

void DisableTableProcedure::UpdateMetaDone(bool succ) {
    if (!succ) {
        // disable failed because meta write fail, revert table's status to kTableEnable
        PROC_CHECK(table_->DoStateTransition(TableEvent::kEnableTable));
        PROC_LOG(WARNING) << "fail to update meta";
        EnterPhaseWithResponseStatus(kMetaTabletError, DisableTablePhase::kEofPhase);
        return;
    }
    PROC_LOG(INFO) << "update disable table info to meta succ";
    EnterPhaseWithResponseStatus(kMasterOk, DisableTablePhase::kDisableTablets);
}

void DisableTableProcedure::DisableTabletsHandler(const DisableTablePhase&) {
    std::vector<TabletPtr> tablet_meta_list;
    table_->GetTablet(&tablet_meta_list);
    int in_transition_tablet_cnt = 0;
    for (uint32_t i = 0; i < tablet_meta_list.size(); ++i) {
        TabletPtr tablet = tablet_meta_list[i];
        if (tablet->GetStatus() == TabletMeta::kTabletDisable) {
            continue;
        }
        if (tablet->LockTransition()) {
            if (tablet->GetStatus() == TabletMeta::kTabletOffline || 
                    tablet->GetStatus() == TabletMeta::kTabletLoadFail) {
                tablet->DoStateTransition(TabletEvent::kTableDisable);
                tablet->UnlockTransition();
                continue;
            }
            std::shared_ptr<Procedure> proc(new UnloadTabletProcedure(tablet, thread_pool_, false));
            MasterEnv().GetExecutor()->AddProcedure(proc);
            in_transition_tablet_cnt++;
        }
        else {
            in_transition_tablet_cnt++;
        }
    }
    PROC_VLOG(23) << "table: " << table_->GetTableName() 
        << ", in transition num: " << in_transition_tablet_cnt;
    if (in_transition_tablet_cnt == 0) {
        SetNextPhase(DisableTablePhase::kEofPhase);
        return;
    }
}

void DisableTableProcedure::EofPhaseHandler(const DisableTablePhase&) {
    done_.store(true);
    if (table_ && table_->InTransition()) {
        table_->UnlockTransition();
    }
    PROC_LOG(INFO) << "disable table: " << table_->GetTableName() << " finish";
    rpc_closure_->Run();
}

std::ostream& operator<< (std::ostream& o, const DisableTablePhase& phase) {
    static const char* msg[] = {"DisableTablePhase::kPrepare", 
                                "DisableTablePhase::kDisableTable", 
                                "DisableTablePhase::kUpdateMeta", 
                                "DisableTablePhase::kDisableTablets", 
                                "DisableTablePhase::kEofPhase", 
                                "DisableTablePhase::kUnknown"};
    static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
    typedef std::underlying_type<DisableTablePhase>::type UnderType;
    uint32_t index = static_cast<UnderType>(phase) - static_cast<UnderType>(DisableTablePhase::kPrepare);
    index = index < msg_size ? index : msg_size - 1;
    o << msg[index];
    return o;
}

}
}
