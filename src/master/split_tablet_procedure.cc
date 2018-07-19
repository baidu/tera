// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <memory>
#include <glog/logging.h>
#include "db/filename.h"
#include "io/utils_leveldb.h"
#include "load_tablet_procedure.h"
#include "master/master_env.h"
#include "master/procedure_executor.h"
#include "master/split_tablet_procedure.h"
#include "proto/kv_helper.h"
#include "proto/tabletnode_client.h"
#include "unload_tablet_procedure.h"

DECLARE_int32(tera_master_split_rpc_timeout);
DECLARE_string(tera_tabletnode_path_prefix);
namespace tera {
namespace master {

std::map<SplitTabletPhase, SplitTabletProcedure::SplitTabletPhaseHandler> 
    SplitTabletProcedure::phase_handlers_ {
        {SplitTabletPhase::kPreSplitTablet,   std::bind(&SplitTabletProcedure::PreSplitTabletPhaseHandler, _1, _2)},
        {SplitTabletPhase::kUnLoadTablet,     std::bind(&SplitTabletProcedure::UnloadTabletPhaseHandler, _1, _2)},
        {SplitTabletPhase::kPostUnLoadTablet, std::bind(&SplitTabletProcedure::PostUnloadTabletPhaseHandler, _1, _2)},
        {SplitTabletPhase::kUpdateMeta,       std::bind(&SplitTabletProcedure::UpdateMetaPhaseHandler, _1, _2)},
        {SplitTabletPhase::kLoadTablets,      std::bind(&SplitTabletProcedure::LoadTabletsPhaseHandler, _1, _2)},
        {SplitTabletPhase::kFaultRecover,     std::bind(&SplitTabletProcedure::FaultRecoverPhaseHandler, _1, _2)},
        {SplitTabletPhase::kEofPhase,         std::bind(&SplitTabletProcedure::EOFPhaseHandler, _1, _2)}
};

SplitTabletProcedure::SplitTabletProcedure(TabletPtr tablet, std::string split_key, ThreadPool* thread_pool) : 
    id_(std::string("SplitTablet:") + tablet->GetPath() + ":" + TimeStamp()),
    tablet_(tablet), split_key_(split_key), thread_pool_(thread_pool) {
    PROC_LOG(INFO) << "split tablet begin, tablet: " << tablet_->GetPath();
    if (tablet_->GetStatus() != TabletMeta::kTabletReady) {
        SetNextPhase(SplitTabletPhase::kEofPhase);
        PROC_LOG(WARNING) << "tablet is not ready, give up split, tablet: " << tablet_;
        return;
    }
    SetNextPhase(SplitTabletPhase::kPreSplitTablet);
}

std::string SplitTabletProcedure::ProcId() const {
    return id_;
}

void SplitTabletProcedure::RunNextStage() {
    SplitTabletPhase phase = GetCurrentPhase();
    auto it = phase_handlers_.find(phase);
    PROC_CHECK (it != phase_handlers_.end()) << "illegal phase: " << phase << ", tablet: " << tablet_;
    SplitTabletPhaseHandler handler = it->second;
    handler(this, phase);
}

void SplitTabletProcedure::PreSplitTabletPhaseHandler(const SplitTabletPhase&) {
    if (!split_key_.empty()) {
        if ((!tablet_->GetKeyStart().empty() && split_key_ <= tablet_->GetKeyStart()) || 
            (!tablet_->GetKeyEnd().empty() && split_key_ >= tablet_->GetKeyEnd())) {
            PROC_LOG(WARNING) << "invalid split key: " << split_key_ << ", tablet: " << tablet_;
            SetNextPhase(SplitTabletPhase::kEofPhase);
            return;
        }
        SetNextPhase(SplitTabletPhase::kUnLoadTablet);
    }
    else if (dispatch_split_key_request_) {
        // waiting RPC response
        return;
    }
    else {
        dispatch_split_key_request_ = true;
        ComputeSplitKeyAsync();
    }    
}

void SplitTabletProcedure::UnloadTabletPhaseHandler(const SplitTabletPhase&) {
    if (!unload_proc_) {
        unload_proc_.reset(new UnloadTabletProcedure(tablet_, thread_pool_, true));
        PROC_LOG(INFO) << "Generate UnloadTablet SubProcedure: " << unload_proc_->ProcId();
        MasterEnv().GetExecutor()->AddProcedure(unload_proc_);
    }
    if (!unload_proc_->Done()) {
        return;
    }
    TabletNodePtr node = tablet_->GetTabletNode();
    if (tablet_->GetStatus() == TabletMeta::kTabletOffline) {
        SetNextPhase(SplitTabletPhase::kPostUnLoadTablet);
    }
    else {
        SetNextPhase(SplitTabletPhase::kEofPhase);
    }
}

void SplitTabletProcedure::PostUnloadTabletPhaseHandler(const SplitTabletPhase&) {
    if (!TabletStatusCheck()) {
        SetNextPhase(SplitTabletPhase::kFaultRecover);
    }
    else {
        SetNextPhase(SplitTabletPhase::kUpdateMeta);    
    }
}

void SplitTabletProcedure::UpdateMetaPhaseHandler(const SplitTabletPhase&) {
    if (!child_tablets_[0]) {
        UpdateMeta();
    }   
}

void SplitTabletProcedure::LoadTabletsPhaseHandler(const SplitTabletPhase&) {
    if (!load_procs_[0] && !load_procs_[1]) {
        TabletNodePtr node = tablet_->GetTabletNode();
        // try load tablet at the origin tabletnode considering cache locality
        load_procs_[0].reset(new LoadTabletProcedure(child_tablets_[0], node, thread_pool_/*, true*/));
        load_procs_[1].reset(new LoadTabletProcedure(child_tablets_[1], node, thread_pool_/*, true*/));
        PROC_LOG(INFO) << "Generate LoadTablet SubProcedure1: " << load_procs_[0]->ProcId();
        PROC_LOG(INFO) << "Generate LoadTablet SubProcedure2, " << load_procs_[1]->ProcId();
        MasterEnv().GetExecutor()->AddProcedure(load_procs_[0]);
        MasterEnv().GetExecutor()->AddProcedure(load_procs_[1]);
    }
    PROC_CHECK(load_procs_[0] && load_procs_[1]);
    SetNextPhase(SplitTabletPhase::kEofPhase);
}

void SplitTabletProcedure::FaultRecoverPhaseHandler(const SplitTabletPhase&) {
    PROC_CHECK(phases_.size() >= 2 && GetCurrentPhase() == SplitTabletPhase::kFaultRecover);
    SplitTabletPhase fault_phase = phases_.at(phases_.size() - 2);
    PROC_CHECK(fault_phase == SplitTabletPhase::kPostUnLoadTablet);
    if (!recover_proc_) {
        recover_proc_.reset(new LoadTabletProcedure(tablet_, tablet_->GetTabletNode(), thread_pool_/*, true*/));
        MasterEnv().GetExecutor()->AddProcedure(recover_proc_);
        return;
    }
    SetNextPhase(SplitTabletPhase::kEofPhase);
}

void SplitTabletProcedure::EOFPhaseHandler(const SplitTabletPhase&) {
    PROC_LOG(INFO) << "split tablet finish, tablet: " << tablet_->GetPath();
    // If parent tablet not in transition, unlock it's transition lock
    if (!recover_proc_) {
        tablet_->UnlockTransition();
    }
    done_ = true;
}

void SplitTabletProcedure::ComputeSplitKeyAsync() {
    SplitTabletRequest* request = new SplitTabletRequest;
    SplitTabletResponse* response = new SplitTabletResponse;
    
    request->set_sequence_id(MasterEnv().SequenceId().Inc());
    request->set_tablet_name(tablet_->GetTableName());
    request->mutable_key_range()->set_key_start(tablet_->GetKeyStart());
    request->mutable_key_range()->set_key_end(tablet_->GetKeyEnd());
    tabletnode::TabletNodeClient node_client(thread_pool_, tablet_->GetServerAddr(),
            FLAGS_tera_master_split_rpc_timeout);
    PROC_LOG(INFO) << "ComputeSplitKeyAsync id: " << request->sequence_id() << ", " << tablet_;
    ComputeSplitKeyClosure done = 
        std::bind(&SplitTabletProcedure::ComputeSplitKeyCallback, this, _1, _2, _3, _4);
    node_client.ComputeSplitKey(request, response, done);
}

void SplitTabletProcedure::ComputeSplitKeyCallback(SplitTabletRequest* request, 
        SplitTabletResponse* response, 
        bool failed, 
        int error_code) {
    std::unique_ptr<SplitTabletRequest> request_deleter(request);
    std::unique_ptr<SplitTabletResponse> response_deleter(response);
    StatusCode status = response->status();
    if (failed || status != kTabletNodeOk) {
        std::string errmsg = (failed ? 
                sofa::pbrpc::RpcErrorCodeToString(error_code) : StatusCodeToString(status));
        PROC_LOG(WARNING) << "cannot get split key from ts, abort tablet split, " 
            << tablet_ << ", error: " << errmsg;
        SetNextPhase(SplitTabletPhase::kEofPhase);
        return;
    }
    split_key_ = response->split_keys(0);
    SetNextPhase(SplitTabletPhase::kUnLoadTablet);
}

void SplitTabletProcedure::UpdateMeta() {
    std::vector<MetaWriteRecord> records;
    
    std::string parent_path = tablet_->GetPath();
    TablePtr table = tablet_->GetTable();
    std::string child_key_start = tablet_->GetKeyStart();
    std::string child_key_end = split_key_;
    for (int i = 0; i < 2; ++i) {
        TabletMeta child_meta;
        tablet_->ToMeta(&child_meta);
        child_meta.clear_parent_tablets();
        child_meta.set_status(TabletMeta::kTabletOffline);
        child_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(parent_path));
        child_meta.set_path(leveldb::GetChildTabletPath(parent_path, table->GetNextTabletNo()));
        child_meta.mutable_key_range()->set_key_start(child_key_start);
        child_meta.mutable_key_range()->set_key_end(child_key_end);
        child_meta.set_size(tablet_->GetDataSize() / 2);
        child_tablets_[i].reset(new Tablet(child_meta, table));
        child_key_start = child_key_end;
        child_key_end = tablet_->GetKeyEnd();
        PackMetaWriteRecords(child_tablets_[i], false, records);
    }
    
    UpdateMetaClosure done = std::bind(&SplitTabletProcedure::UpdateMetaDone, this, _1);
    PROC_LOG(INFO) << "[split] update meta async: " << tablet_ ;
    MasterEnv().BatchWriteMetaTableAsync(records, done, -1);

}

void SplitTabletProcedure::UpdateMetaDone(bool) {
    TabletMeta first_meta, second_meta;
    child_tablets_[0]->ToMeta(&first_meta);
    first_meta.set_status(TabletMeta::kTabletOffline);
    child_tablets_[1]->ToMeta(&second_meta);
    second_meta.set_status(TabletMeta::kTabletOffline);
    TablePtr table = tablet_->GetTable();
    child_tablets_[0]->LockTransition();
    child_tablets_[1]->LockTransition();
    
    table->SplitTablet(tablet_, first_meta, second_meta, &child_tablets_[0], &child_tablets_[1]);
    PROC_LOG(INFO) << "split finish, " << tablet_ << ", try load child tablet,"
            << "\nfirst: " << child_tablets_[0] 
            << "\nsecond: " << child_tablets_[1];
    SetNextPhase(SplitTabletPhase::kLoadTablets);
}

bool SplitTabletProcedure::TabletStatusCheck() {
    leveldb::Env* env = io::LeveldbBaseEnv();
    std::vector<std::string> children;
    std::string tablet_path = FLAGS_tera_tabletnode_path_prefix + "/" + tablet_->GetPath();
    leveldb::Status status = env->GetChildren(tablet_path, &children);
    if (!status.ok()) {
        PROC_LOG(WARNING) << "[split] abort, " << tablet_ 
            << ", tablet status check error: " << status.ToString();
        return false;
    }
    for (size_t i = 0; i < children.size(); ++i) {
        leveldb::FileType type = leveldb::kUnknown;
        uint64_t number = 0;
        if (ParseFileName(children[i], &number, &type) &&
                type == leveldb::kLogFile)  {
            PROC_LOG(WARNING) << "[split] abort, " << tablet_ << ", tablet log not clear.";
            return false;
        }
    }
    return true;
}

std::ostream& operator<< (std::ostream& o, const SplitTabletPhase& phase) {
    static const char* msg[] = {"SplitTabletPhase::kPreSplitTablet", 
                                "SplitTabletPhase::kUnLoadTablet", 
                                "SplitTabletPhase::kPostUnLoadTablet", 
                                "SplitTabletPhase::kUpdateMeta", 
                                "SplitTabletPhase::kLoadTablets", 
                                "SplitTabletPhase::kFaultRecover", 
                                "SplitTabletPhase::kEofPhase", 
                                "SplitTabletPhase::UNKNOWN"};
    static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
    typedef std::underlying_type<SplitTabletPhase>::type UnderType;
    uint32_t index = static_cast<UnderType>(phase) - static_cast<UnderType>(SplitTabletPhase::kPreSplitTablet);
    index = index < msg_size ? index : msg_size - 1;
    o << msg[index];
    return o;
}


}
}
