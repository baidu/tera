// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <functional>
#include <map>
#include "master/procedure.h"
#include "master/tablet_manager.h"
#include "proto/master_rpc.pb.h"

namespace tera {
namespace master {

enum class DisableTablePhase {
    kPrepare,
    kDisableTable,
    kUpdateMeta,
    kDisableTablets,
    kEofPhase
};

std::ostream& operator<< (std::ostream& o, const DisableTablePhase& phase);

class DisableTableProcedure : public Procedure {
public:
    DisableTableProcedure(TablePtr table_, 
                          const DisableTableRequest* request_, 
                          DisableTableResponse* response, 
                          google::protobuf::Closure* closure,
                          ThreadPool* thread_pool);

    virtual std::string ProcId() const;

    virtual void RunNextStage();

    virtual bool Done() {return done_.load();}

    virtual ~DisableTableProcedure() {}
private:
    typedef std::function<void (DisableTableProcedure*, const DisableTablePhase&)> DisableTablePhaseHandler;

    void SetNextPhase(const DisableTablePhase& phase) {phases_.push_back(phase);}
    
    void EnterPhaseWithResponseStatus(StatusCode code, DisableTablePhase phase) {
        response_->set_status(code);
        SetNextPhase(phase);
    }

    DisableTablePhase GetCurrentPhase() {return phases_.back();}
    
    void PrepareHandler(const DisableTablePhase&);
    void DisableTableHandler(const DisableTablePhase&);
    void UpdateMetaHandler(const DisableTablePhase&);
    void DisableTabletsHandler(const DisableTablePhase&);
    void EofPhaseHandler(const DisableTablePhase&);

    void UpdateMetaDone(bool succ);

private:
    TablePtr table_;
    const DisableTableRequest* request_;
    DisableTableResponse* response_;
    google::protobuf::Closure* rpc_closure_;
    std::atomic<bool> update_meta_;
    std::vector<DisableTablePhase> phases_;
    std::atomic<bool> done_;
    static std::map<DisableTablePhase, DisableTablePhaseHandler> phase_handlers_;
    ThreadPool* thread_pool_;

};

}
}
