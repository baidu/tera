// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <functional>
#include <map>
#include "master/master_env.h"
#include "master/procedure.h"
#include "proto/master_rpc.pb.h"

namespace tera {
namespace master {

enum class UpdateTablePhase{
    kPrepare,
    kUpdateMeta,
    kTabletsSchemaSyncing,
    kEofPhase,
};

std::ostream& operator<< (std::ostream& o, const UpdateTablePhase& phase);

class UpdateTableProcedure : public Procedure {
public:
    UpdateTableProcedure(TablePtr table, 
            const UpdateTableRequest* request, 
            UpdateTableResponse* response, 
            google::protobuf::Closure* closure,
            ThreadPool* thread_pool);

    virtual std::string ProcId() const;
    
    virtual void RunNextStage();

    virtual bool Done() {return done_.load();}

    virtual ~UpdateTableProcedure() {}
private:
typedef std::function<void (UpdateRequest*, UpdateResponse*, bool, int)> UpdateClosure;

    typedef std::function<void (UpdateTableProcedure*, const UpdateTablePhase&)> UpdateTablePhaseHandler;

    void SetNextPhase(const UpdateTablePhase& phase) {phases_.emplace_back(phase);}

    void EnterPhaseWithResponseStatus(StatusCode code, UpdateTablePhase phase) {
        response_->set_status(code);
        SetNextPhase(phase);
    }

    UpdateTablePhase GetCurrentPhase() {return phases_.back();}

    void PrepareHandler(const UpdateTablePhase& phase);

    void UpdateMetaHandler(const UpdateTablePhase& phase);

    void UpdateMetaDone(bool succ);
    
    void SyncTabletsSchemaHandler(const UpdateTablePhase& phase);

    void EofPhaseHandler(const UpdateTablePhase&);

    void NoticeTabletSchemaUpdate(TabletPtr tablet, UpdateClosure done);

    void UpdateTabletSchemaCallback(TabletPtr tablet, 
            int32_t retry_times,
            UpdateRequest* request, 
            UpdateResponse* response, 
            bool fail, 
            int status_code);
    
    void RecoverOfflineTablets();

private:
    TablePtr table_;
    const UpdateTableRequest* request_;
    UpdateTableResponse* response_;
    google::protobuf::Closure* rpc_closure_;
    std::atomic<bool> update_meta_;
    std::atomic<bool> sync_tablets_schema_;
    std::atomic<bool> done_;
    std::vector<UpdateTablePhase> phases_;
    std::vector<TabletPtr> tablet_list_;
    std::vector<TabletPtr> offline_tablets_;
    std::atomic<uint32_t> tablet_sync_cnt_;
    static std::map<UpdateTablePhase, UpdateTablePhaseHandler> phase_handlers_;
    ThreadPool* thread_pool_;
};

}
}

