// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <glog/logging.h>
#include "load_tablet_procedure.h"
#include "move_tablet_procedure.h"
#include "proto/tabletnode_client.h"
#include "master/master_env.h"
#include "master/procedure_executor.h"
#include "unload_tablet_procedure.h"

namespace tera {
namespace master {

std::map<MoveTabletPhase, MoveTabletProcedure::MoveTabletPhaseHandler> MoveTabletProcedure::phase_handlers_ {
    {MoveTabletPhase::kUnLoadTablet, std::bind(&MoveTabletProcedure::UnloadTabletPhaseHandler, _1, _2)},
    {MoveTabletPhase::kLoadTablet,   std::bind(&MoveTabletProcedure::LoadTabletPhaseHandler, _1, _2)},
    {MoveTabletPhase::kEofPhase,     std::bind(&MoveTabletProcedure::EOFPhaseHandler, _1, _2)}
};

MoveTabletProcedure::MoveTabletProcedure(TabletPtr tablet, TabletNodePtr node, ThreadPool* thread_pool) : 
    id_(std::string("MoveTablet:") + tablet->GetPath() + ":" + TimeStamp()),
    tablet_(tablet), 
    dest_node_(node), 
    done_(false),
    thread_pool_(thread_pool) {
    PROC_LOG(INFO) << "move tablet begin, tablet: " << tablet_->GetPath();
    if (dest_node_) {
        // PlanToMoveIn field should be removed in the future as it is the LoadBalanceModule's duty
        // to avoid move many tablets to the same TS.
        dest_node_->PlanToMoveIn();
    }
    if (tablet_->GetStatus() == TabletMeta::kTabletReady) {
        SetNextPhase(MoveTabletPhase::kUnLoadTablet);
    }
    else if (tablet_->GetStatus() == TabletMeta::kTabletOffline || 
            tablet_->GetStatus() == TabletMeta::kTabletLoadFail) {
        SetNextPhase(MoveTabletPhase::kLoadTablet);
    }
    else {
        SetNextPhase(MoveTabletPhase::kEofPhase);
    }
}

std::string MoveTabletProcedure::ProcId() const {
    return id_;
}

void MoveTabletProcedure::RunNextStage() {
    MoveTabletPhase phase = GetCurrentPhase();
    auto it = phase_handlers_.find(phase);
    PROC_CHECK(it != phase_handlers_.end()) << "illegal phase: " << phase << ", tablet: " << tablet_;
    MoveTabletPhaseHandler handler = it->second;
    handler(this, phase);
}

void MoveTabletProcedure::UnloadTabletPhaseHandler(const MoveTabletPhase&) {
    if (!unload_proc_) {
        PROC_LOG(INFO) << "MoveTablet: Unload: " << tablet_; 
        unload_proc_.reset(new UnloadTabletProcedure(tablet_, thread_pool_, true));
        MasterEnv().GetExecutor()->AddProcedure(unload_proc_);
    }
    // currently tablet unloading operation has not finished yet, return and check
    // status in next schedule cycle
    if (!unload_proc_->Done()) {
        return;
    }
    if (tablet_->GetStatus() != TabletMeta::kTabletOffline) {
        // currently if unload fail, we directory abort the MoveTabletProcedure. U should also 
        // notice that if master_kick_tabletnode is enabled, we will never fall into unload 
        // fail position because we can always unload the tablet succ by kick off the TS
        // TODO: if dfs directory lock is enabled, we can enter LOAD_TABLET phase directly
        // as directory lock ensures we can avoid multi-load problem
        SetNextPhase(MoveTabletPhase::kEofPhase);
        return;
    }
    SetNextPhase(MoveTabletPhase::kLoadTablet);
}

void MoveTabletProcedure::LoadTabletPhaseHandler(const MoveTabletPhase&) {
    if (!load_proc_) {
        load_proc_.reset(new LoadTabletProcedure(tablet_, dest_node_, thread_pool_));
        PROC_LOG(INFO) << "MoveTablet: generate async LoadTabletProcedure: " 
            << load_proc_->ProcId() << "tablet " << tablet_;
        MasterEnv().GetExecutor()->AddProcedure(load_proc_);
    }
    SetNextPhase(MoveTabletPhase::kEofPhase);
}

void MoveTabletProcedure::EOFPhaseHandler(const MoveTabletPhase&) {
    PROC_LOG(INFO) << "tablet: " << tablet_->GetPath() << "move EOF_PHASE";
    if (dest_node_) {
        dest_node_->DoneMoveIn();
    }
    // record last move time, avoiding move a tablet too frequently
    tablet_->SetLastMoveTime(get_micros());
    done_ = true;
}

std::ostream& operator<< (std::ostream& o, const MoveTabletPhase& phase) {
    static const char* msg[] = {"MoveTabletPhase::kUnLoadTablet", 
                                "MoveTabletPhase::kLoadTablet", 
                                "MoveTabletPhase::kEofPhase", 
                                "MoveTabletPhase::Unknown"};
    static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
    typedef std::underlying_type<MoveTabletPhase>::type UnderType;
    uint32_t index = static_cast<UnderType>(phase) - static_cast<UnderType>(MoveTabletPhase::kUnLoadTablet);
    index = index < msg_size ? index : msg_size - 1;
    o << msg[index];
    return o;
}

}
}
