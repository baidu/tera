// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/enable_table_procedure.h"
#include "master/load_tablet_procedure.h"
#include "master/master_env.h"

DECLARE_int32(tera_master_meta_retry_times);

namespace tera {
namespace master {

std::map<EnableTablePhase, EnableTableProcedure::EnableTablePhaseHandler> EnableTableProcedure::phase_handlers_ {
    {EnableTablePhase::kPrepare, 
            std::bind(&EnableTableProcedure::PrepareHandler, _1, _2)},
    {EnableTablePhase::kEnableTable, 
            std::bind(&EnableTableProcedure::EnableTableHandler, _1, _2)},
    {EnableTablePhase::kUpdateMeta, 
            std::bind(&EnableTableProcedure::UpdateMetaHandler, _1, _2)},
    {EnableTablePhase::kEnableTablets, 
            std::bind(&EnableTableProcedure::EnableTabletsHandler, _1, _2)},
    {EnableTablePhase::kEofPhase, 
            std::bind(&EnableTableProcedure::EofPhaseHandler, _1, _2)},
};

EnableTableProcedure::EnableTableProcedure(TablePtr table, 
        const EnableTableRequest* request, 
        EnableTableResponse* response, 
        google::protobuf::Closure* closure,
        ThreadPool* thread_pool) :
    table_(table),
    request_(request),
    response_(response), 
    rpc_closure_(closure), 
    update_meta_(false), 
    done_(false),
    thread_pool_(thread_pool) {
    PROC_LOG(INFO) << "enable table: " << table_->GetTableName() << " begin";
    SetNextPhase(EnableTablePhase::kPrepare);
}

std::string EnableTableProcedure::ProcId() const {
    std::string prefix("EnableTable:");
    return prefix + table_->GetTableName();
}

void EnableTableProcedure::RunNextStage() {
    EnableTablePhase phase = GetCurrentPhase();
    auto it = phase_handlers_.find(phase);
    PROC_CHECK(it != phase_handlers_.end()) 
            << "illegeal phase:" << phase << ", table:" << table_->GetTableName();
    EnableTablePhaseHandler handler = it->second;
    handler(this, phase);
}

void EnableTableProcedure::PrepareHandler(const EnableTablePhase&) {
    if (!MasterEnv().GetMaster()->HasPermission(request_, table_, "enable table")) {
        PROC_LOG(WARNING) << "enable table: " << table_->GetTableName() << "abort, permission denied";
        EnterPhaseWithResponseStatus(kNotPermission, EnableTablePhase::kEofPhase);
        return;
    }
    SetNextPhase(EnableTablePhase::kEnableTable);
}

void EnableTableProcedure::EnableTableHandler(const EnableTablePhase&) {
    if (!table_->DoStateTransition(TableEvent::kEnableTable)) {
        PROC_LOG(WARNING) << table_->GetTableName() 
            << "current state: " << table_->GetStatus() << ", enable failed";
        EnterPhaseWithResponseStatus(
                static_cast<StatusCode>(table_->GetStatus()), EnableTablePhase::kEofPhase);
        return;
    }
    SetNextPhase(EnableTablePhase::kUpdateMeta);
}

void EnableTableProcedure::UpdateMetaHandler(const EnableTablePhase&) {
    if (update_meta_) {
        return;
    }
    update_meta_.store(true);
    MetaWriteRecord record = PackMetaWriteRecord(table_, false);
    PROC_LOG(INFO) << "table: " << table_->GetTableName() 
        << " begin to update table enable info to meta";
    UpdateMetaClosure closure = std::bind(&EnableTableProcedure::UpdateMetaDone, this, _1);
    MasterEnv().BatchWriteMetaTableAsync(record, closure, FLAGS_tera_master_meta_retry_times);
}

void EnableTableProcedure::EnableTabletsHandler(const EnableTablePhase&) {
    std::vector<TabletPtr> tablets;
    table_->GetTablet(&tablets);
    for (std::size_t i = 0; i < tablets.size(); ++i) {
        TabletPtr tablet = tablets[i];
        PROC_CHECK(tablet->LockTransition()) << tablet->GetPath() << " in another tansition";
        PROC_CHECK(tablet->DoStateTransition(TabletEvent::kTableEnable)) 
                << tablet->GetPath() << ", current status: " << tablet->GetStatus();
        std::shared_ptr<Procedure> proc(new LoadTabletProcedure(tablet, tablet->GetTabletNode(), thread_pool_));
        MasterEnv().GetExecutor()->AddProcedure(proc);
    }
    SetNextPhase(EnableTablePhase::kEofPhase);
}

void EnableTableProcedure::EofPhaseHandler(const EnableTablePhase&) {
    done_.store(true);
    PROC_LOG(INFO) << "table: " << table_->GetTableName() << ", status: " << table_->GetStatus();
    if (table_ && table_->InTransition()) {
        table_->UnlockTransition();
    }
    rpc_closure_->Run();
}

void EnableTableProcedure::UpdateMetaDone(bool succ) {
    if (!succ) {
        PROC_LOG(WARNING) << "enable table: " << table_->GetTableName() << " update meta fail";
        PROC_CHECK(table_->DoStateTransition(TableEvent::kDisableTable));
        EnterPhaseWithResponseStatus(kMetaTabletError, EnableTablePhase::kEofPhase);
        return;
    }
    PROC_LOG(INFO) << "update enable table info to meta succ";
    EnterPhaseWithResponseStatus(kMasterOk, EnableTablePhase::kEnableTablets);
}

std::ostream& operator<< (std::ostream& o, const EnableTablePhase& phase) {
    static const char* msg[] = {"EnableTablePhase::kPrepare", 
                                "EnableTablePhase::kEnableTable", 
                                "EnableTablePhase::kUpdateMeta",
                                "EnableTablePhase::kEnableTablets",
                                "EnableTablePhase::kEofPhase", 
                                "EnableTablePhase::kUnknown"};
    static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
    typedef std::underlying_type<EnableTablePhase>::type UnderType;
    uint32_t index = static_cast<UnderType>(phase) - static_cast<UnderType>(EnableTablePhase::kPrepare);
    index = index < msg_size ? index : msg_size - 1;
    o << msg[index];
    return o;

}


}
}
