// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/load_tablet_procedure.h"
#include "update_table_procedure.h"
#include "utils/schema_utils.h"

DECLARE_int32(tera_master_meta_retry_times);
DECLARE_bool(tera_online_schema_update_enabled);
DECLARE_int32(tera_master_schema_update_retry_times);
DECLARE_int32(tera_master_schema_update_retry_period);

namespace tera {
namespace master {

std::map<UpdateTablePhase, UpdateTableProcedure::UpdateTablePhaseHandler> 
    UpdateTableProcedure::phase_handlers_ {
        {UpdateTablePhase::kPrepare, 
                std::bind(&UpdateTableProcedure::PrepareHandler, _1, _2)},
        {UpdateTablePhase::kUpdateMeta, 
                std::bind(&UpdateTableProcedure::UpdateMetaHandler, _1, _2)},
        {UpdateTablePhase::kTabletsSchemaSyncing, 
                std::bind(&UpdateTableProcedure::SyncTabletsSchemaHandler, _1, _2)},
        {UpdateTablePhase::kEofPhase, 
                std::bind(&UpdateTableProcedure::EofPhaseHandler, _1, _2)},
};

UpdateTableProcedure::UpdateTableProcedure(TablePtr table, 
        const UpdateTableRequest* request, 
        UpdateTableResponse* response, 
        google::protobuf::Closure* closure,
        ThreadPool* thread_pool) :
    table_(table),
    request_(request),
    response_(response),
    rpc_closure_(closure),
    update_meta_(false),
    sync_tablets_schema_(false),
    done_(false), 
    tablet_sync_cnt_(0),
    thread_pool_(thread_pool) {
    PROC_LOG(INFO) << "update schema begin, table: " << table_->GetTableName();
    SetNextPhase(UpdateTablePhase::kPrepare);
}

std::string UpdateTableProcedure::ProcId() const {
    std::string prefix("UpdateTable:");
    return prefix + table_->GetTableName();
}

void UpdateTableProcedure::RunNextStage() {
    UpdateTablePhase phase = GetCurrentPhase();
    auto it = phase_handlers_.find(phase);
    PROC_CHECK(it != phase_handlers_.end()) << "illegal phase: " << phase << ", table: " << table_;
    UpdateTablePhaseHandler handler = it->second;
    handler(this, phase);
}

void UpdateTableProcedure::PrepareHandler(const UpdateTablePhase&) {
    if (!MasterEnv().GetMaster()->HasPermission(request_, table_, "update table")) {
        EnterPhaseWithResponseStatus(kNotPermission, UpdateTablePhase::kEofPhase);
        return;
    }

    if (request_->schema().locality_groups_size() < 1) {
        PROC_LOG(WARNING) << "No LocalityGroupSchema for " << request_->table_name();
        EnterPhaseWithResponseStatus(kInvalidArgument, UpdateTablePhase::kEofPhase);
        return;
    }
    if (!table_->PrepareUpdate(request_->schema())) {
        // another schema-update is doing...
        PROC_LOG(INFO) << "[update] no concurrent schema-update, table:" << table_;
        EnterPhaseWithResponseStatus(kTableNotSupport, UpdateTablePhase::kEofPhase);
        return;
    }
    if (FLAGS_tera_online_schema_update_enabled && table_->GetStatus() == kTableEnable) {
        table_->GetTablet(&tablet_list_);
        for (std::size_t i = 0; i < tablet_list_.size(); ++i) {
            TabletPtr tablet = tablet_list_[i];
            // no other tablet transition procedure is allowed while tablet is updating schema
            // Should be very carefully with tablet's transition lock
            if (!tablet->LockTransition()) {
                PROC_LOG(WARNING) << "abort update online table schema, tablet: " 
                        << tablet->GetPath() << " in transition"; 
                for (std::size_t j = 0; j < i; ++j) {
                    tablet = tablet_list_[j];
                    tablet->UnlockTransition();
                }
                table_->AbortUpdate();
                EnterPhaseWithResponseStatus(kTableNotSupport, UpdateTablePhase::kEofPhase);
                return;
            }
            TabletMeta::TabletStatus  status = tablet->GetStatus();
            if (status != TabletMeta::kTabletReady && status != TabletMeta::kTabletOffline) {
                PROC_LOG(WARNING) << "abort update online table schema, tablet: " 
                    << tablet->GetPath() << " not in ready status, status: " << StatusCodeToString(status);
                for (std::size_t j = 0; j <= i; ++j) {
                    tablet = tablet_list_[j];
                    tablet->UnlockTransition();
                }
                table_->AbortUpdate();
                EnterPhaseWithResponseStatus(kTableNotSupport, UpdateTablePhase::kEofPhase);
                return;
            }
           
        }
    }
    SetNextPhase(UpdateTablePhase::kUpdateMeta);
}

void UpdateTableProcedure::UpdateMetaHandler(const UpdateTablePhase&) {
    if (update_meta_) {
        return;
    }
    update_meta_.store(true);
    PROC_LOG(INFO) << "table: " << table_->GetTableName() << " update meta begin";
    MetaWriteRecord record = PackMetaWriteRecord(table_, false);
    UpdateMetaClosure closure = std::bind(&UpdateTableProcedure::UpdateMetaDone, this, _1);
    MasterEnv().BatchWriteMetaTableAsync(record, closure, FLAGS_tera_master_meta_retry_times);
}

static bool IsUpdateCf(TablePtr table) {
    TableSchema schema;
    if (table->GetOldSchema(&schema)) {
        return IsSchemaCfDiff(table->GetSchema(), schema);
    }
    return true;
}


void UpdateTableProcedure::UpdateMetaDone(bool succ) {
    if (!succ) {
        PROC_LOG(WARNING) << "fail to update meta";
        table_->AbortUpdate();
        EnterPhaseWithResponseStatus(kMetaTabletError, UpdateTablePhase::kEofPhase);
        return;
    }
    PROC_LOG(INFO) << "update table info to meta succ";
    if (FLAGS_tera_online_schema_update_enabled && 
        table_->GetStatus() == kTableEnable && 
        IsUpdateCf(table_)) {
        SetNextPhase(UpdateTablePhase::kTabletsSchemaSyncing);
        return;
    }
    else {
        table_->CommitUpdate();
        EnterPhaseWithResponseStatus(kMasterOk, UpdateTablePhase::kEofPhase);
        return;
    }
}

void UpdateTableProcedure::SyncTabletsSchemaHandler(const UpdateTablePhase&) {
    if (sync_tablets_schema_) {
        return;
    }
    sync_tablets_schema_.store(true);
    PROC_LOG(INFO) << "begin sync tablets schema";
    tablet_sync_cnt_++;
    // No LoadTabletProcedure will be issued once the tablet falls into kTabletOffline status while 
    // UpdateTableProcedure is running, because UpdateTableProcedure has got the tablet's TransitionLocks. 
    // So UpdateTableProcedure should takes of those offline tablets by issue LoadTabletProcedure for
    // those tablets at the right point of time.
    for (std::size_t i = 0; i < tablet_list_.size(); ++i) {
        TabletPtr tablet = tablet_list_[i];
        if (tablet->GetStatus() == TabletMeta::kTabletOffline) {
            offline_tablets_.emplace_back(tablet);
            continue;
        }
        UpdateClosure done = std::bind(
                &UpdateTableProcedure::UpdateTabletSchemaCallback, this, tablet, 0, _1, _2, _3, _4);
        NoticeTabletSchemaUpdate(tablet_list_[i], done);
        tablet_sync_cnt_++;
    }
    if (--tablet_sync_cnt_ == 0) {
        RecoverOfflineTablets();
        table_->CommitUpdate();
        table_->ClearSchemaSyncLock();
        PROC_VLOG(23) << "sync tablets schema finished";
        EnterPhaseWithResponseStatus(kMasterOk, UpdateTablePhase::kEofPhase);
    }

}

void UpdateTableProcedure::NoticeTabletSchemaUpdate(TabletPtr tablet, UpdateClosure done) {

   tabletnode::TabletNodeClient node_client(thread_pool_, tablet->GetServerAddr());
    UpdateRequest* request = new UpdateRequest;
    UpdateResponse* response = new UpdateResponse;
    request->set_sequence_id(MasterEnv().SequenceId().Inc());
    request->mutable_schema()->CopyFrom(tablet->GetSchema());
    request->set_tablet_name(tablet->GetTableName());
    request->mutable_key_range()->set_key_start(tablet->GetKeyStart());
    request->mutable_key_range()->set_key_end(tablet->GetKeyEnd());
    node_client.Update(request, response, done);
}

void UpdateTableProcedure::UpdateTabletSchemaCallback(TabletPtr tablet, 
        int32_t retry_times, 
        UpdateRequest* request, 
        UpdateResponse* response, 
        bool rpc_failed, 
        int status_code) {
    std::unique_ptr<UpdateRequest> request_holder(request);
    std::unique_ptr<UpdateResponse> response_holder(response);
    StatusCode status = response_holder->status();
    TabletNodePtr node = tablet->GetTabletNode();
    PROC_VLOG(23) << "tablet: " << tablet->GetPath() 
        << ", update schema callback returned, remain cnt: " << tablet_sync_cnt_;
    if (tablet->GetStatus() == TabletMeta::kTabletOffline || 
        (!rpc_failed && status == kTabletNodeOk)) {
        if (tablet->GetStatus() == TabletMeta::kTabletOffline) {
            offline_tablets_.emplace_back(tablet);
        }
        // do not unlock offline tablets' TransitionLock. After all online tablets UpdateTabletSchema RPC
        // has been collected, UpdateTableProcedure will issue LoadTabletProcedure for those offline tablets
        // and those offline tablets will be locked until their LoadTabletProcedure finished.
        else {
            tablet->UnlockTransition();
        }
        if (--tablet_sync_cnt_ == 0) {
            PROC_VLOG(23) << "sync tablets schema finished";
            RecoverOfflineTablets();
            table_->CommitUpdate();
            EnterPhaseWithResponseStatus(kMasterOk, UpdateTablePhase::kEofPhase);
        }
        return;
    }

    if (rpc_failed || status != kTabletNodeOk) {
        if (rpc_failed) {
            PROC_LOG(WARNING) << "[update] fail to update schema: "
                << sofa::pbrpc::RpcErrorCodeToString(status_code)
                << ": " << tablet;
        } else {
            PROC_LOG(WARNING) << "[update] fail to update schema: " << StatusCodeToString(status)
                << ": " << tablet;
        }
        if (retry_times > FLAGS_tera_master_schema_update_retry_times) {
            PROC_LOG(ERROR) << "[update] retry " << retry_times << " times, kick "
                << tablet->GetServerAddr();
            // we ensure tablet's schema been updated by kickoff the hosting tabletnode if all 
            // UpdateTabletSchema RPC tries failed
            tablet->UnlockTransition();
            MasterEnv().GetMaster()->TryKickTabletNode(tablet->GetServerAddr());
            if (--tablet_sync_cnt_ == 0) {
                RecoverOfflineTablets();
                table_->CommitUpdate();
                EnterPhaseWithResponseStatus(kMasterOk, UpdateTablePhase::kEofPhase);
            }
        } else {
            UpdateClosure done =
                std::bind(&UpdateTableProcedure::UpdateTabletSchemaCallback, this, tablet,
                        retry_times + 1, _1, _2, _3, _4);
            ThreadPool::Task task =
                std::bind(&UpdateTableProcedure::NoticeTabletSchemaUpdate, this, tablet, done);
            MasterEnv().GetThreadPool()->DelayTask(
                    FLAGS_tera_master_schema_update_retry_period * 1000, task);
        }
        return;
    }
}

void UpdateTableProcedure::RecoverOfflineTablets() {
    for (auto tablet : offline_tablets_) {
        std::shared_ptr<Procedure> proc(new LoadTabletProcedure(tablet, tablet->GetTabletNode(), thread_pool_));
        MasterEnv().GetExecutor()->AddProcedure(proc);
    }
}

void UpdateTableProcedure::EofPhaseHandler(const UpdateTablePhase&) {
    done_.store(true);
    PROC_LOG(INFO) << "update table finish";
    if (table_ && table_->InTransition()) {
        table_->UnlockTransition();
    }
    rpc_closure_->Run();
}

std::ostream& operator<< (std::ostream& o, const UpdateTablePhase& phase) { 
    static const char* msg[] = {"UpdateTablePhase::kPrepare", 
                                "UpdateTablePhase::kUpdateMeta", 
                                "UpdateTablePhase::kTabletsSchemaSyncing", 
                                "UpdateTablePhase::kEofPhase", 
                                "UpdateTablePhase::kUnknown"};
    static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
    typedef std::underlying_type<UpdateTablePhase>::type UnderType;
    uint32_t index = static_cast<UnderType>(phase) - static_cast<UnderType>(UpdateTablePhase::kPrepare);
    index = index < msg_size ? index : msg_size - 1;
    o << msg[index];
    return o;

}

}
}
