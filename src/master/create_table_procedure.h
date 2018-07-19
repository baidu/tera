// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <functional>
#include <iostream>
#include <map>
#include <vector>
#include "proto/master_rpc.pb.h"
#include "master/procedure.h"
#include "master/master_env.h"

namespace tera {
namespace master {

enum class CreateTablePhase {
    kPrepare,
    kUpdateMeta,
    kLoadTablets,
    kEofPhase
};

std::ostream& operator<< (std::ostream& o, const CreateTablePhase& phase);

class CreateTableProcedure : public Procedure {
public:
    CreateTableProcedure(const CreateTableRequest* request, 
            CreateTableResponse* response, 
            google::protobuf::Closure* closure,
            ThreadPool* thread_pool);

    virtual std::string ProcId() const;

    virtual void RunNextStage();

    virtual ~CreateTableProcedure() {}

    virtual bool Done();

private:
    typedef std::function<void (CreateTableProcedure*, const CreateTablePhase&)> CreateTablePhaseHandler; 

    void SetNextPhase(const CreateTablePhase& phase) {phases_.emplace_back(phase);}

    CreateTablePhase GetCurrentPhase() {return phases_.back();}

    void EnterPhaseAndResponseStatus(StatusCode status, const CreateTablePhase& phase) {
        response_->set_status(status);
        SetNextPhase(phase);
    }

    void EnterEofPhaseWithResponseStatus(StatusCode status) {
        EnterPhaseAndResponseStatus(status, CreateTablePhase::kEofPhase);
    }

    void PreCheckHandler(const CreateTablePhase&);
    void UpdateMetaHandler(const CreateTablePhase&);
    void LoadTabletsHandler(const CreateTablePhase&);
    void EofHandler(const CreateTablePhase&);
    
    void UpdateMetaDone(bool succ);

private:
    const CreateTableRequest* request_;
    CreateTableResponse* response_;
    google::protobuf::Closure* rpc_closure_;
    std::string table_name_;
    TablePtr table_;
    std::vector<TabletPtr> tablets_;
    std::vector<MetaWriteRecord> meta_records_;
    std::atomic<bool> update_meta_;
    std::atomic<bool> done_ ;
    std::vector<CreateTablePhase> phases_;
    static std::map<CreateTablePhase, CreateTablePhaseHandler> phase_handlers_;
    ThreadPool* thread_pool_;
};

}
}
