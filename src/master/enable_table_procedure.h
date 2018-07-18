// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <functional>
#include <map>
#include "master/master_env.h"
#include "master/procedure.h"
#include "master/tablet_manager.h"
#include "proto/master_rpc.pb.h"

namespace tera {
namespace master {

enum class EnableTablePhase {
    kPrepare,
    kEnableTable,
    kUpdateMeta,
    kEnableTablets,
    kEofPhase,
};

std::ostream& operator<< (std::ostream& o, const EnableTablePhase& phase);

class EnableTableProcedure : public Procedure {
public:
    EnableTableProcedure(TablePtr table, 
                         const EnableTableRequest* request, 
                         EnableTableResponse* response, 
                         google::protobuf::Closure* closure,
                         ThreadPool* thread_pool);
    virtual ~EnableTableProcedure() {}

    virtual std::string ProcId() const;

    virtual void RunNextStage();

    virtual bool Done() {return done_.load();}
private:
    typedef std::function<void (EnableTableProcedure*, const EnableTablePhase&)> EnableTablePhaseHandler;
    
    void SetNextPhase(const EnableTablePhase& phase) {phases_.emplace_back(phase);}
    
    void EnterPhaseWithResponseStatus(StatusCode status, EnableTablePhase phase) {
        response_->set_status(status);
        SetNextPhase(phase);
    }

    EnableTablePhase GetCurrentPhase() {return phases_.back();}

    void PrepareHandler(const EnableTablePhase&);
    
    void EnableTableHandler(const EnableTablePhase&);
    
    void UpdateMetaHandler(const EnableTablePhase&);
    
    void EnableTabletsHandler(const EnableTablePhase&);
    
    void EofPhaseHandler(const EnableTablePhase&);
    
    void UpdateMetaDone(bool succ);

private:
    TablePtr table_;
    const EnableTableRequest* request_;
    EnableTableResponse* response_;
    google::protobuf::Closure* rpc_closure_;
    std::atomic<bool> update_meta_;
    std::atomic<bool> done_;
    std::vector<EnableTablePhase> phases_;
    static std::map<EnableTablePhase, EnableTablePhaseHandler> phase_handlers_;
    ThreadPool* thread_pool_;
};

}
}
