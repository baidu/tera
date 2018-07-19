// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include "master/procedure.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"

namespace tera {
namespace master {
    
enum class MergeTabletPhase{
    kUnLoadTablets,
    kPostUnLoadTablets,
    kUpdateMeta,
    kLoadMergedTablet,
    kFaultRecover,
    kEofPhase,
};

std::ostream& operator<< (std::ostream& o, const MergeTabletPhase& phase);

class MergeTabletProcedure : public Procedure {
public:
    
    MergeTabletProcedure(TabletPtr left, TabletPtr right, ThreadPool* thread_pool);

    virtual ~MergeTabletProcedure() {}

    virtual std::string ProcId() const;

    virtual void RunNextStage();

    virtual bool Done() {return done_;}

private: 
    typedef std::function<void (MergeTabletProcedure*, const MergeTabletPhase&)> MergeTabletPhaseHandler;

    MergeTabletPhase GetCurrentPhase() {
        std::lock_guard<std::mutex> lock(mutex_);
        return phases_.back();
    }

    void SetNextPhase(MergeTabletPhase phase) {
        std::lock_guard<std::mutex> lock(mutex_);
        phases_.emplace_back(phase);
    }

    bool TabletStateCheck();
    
    void UpdateMetaTable();

    void UpdateMeta();

    void MergeUpdateMetaDone(bool);
    
    void UnloadTabletsPhaseHandler(const MergeTabletPhase&);
    void PostUnloadTabletsPhaseHandler(const MergeTabletPhase&);
    void UpdateMetaPhaseHandler(const MergeTabletPhase&);
    void LoadMergedTabletPhaseHandler(const MergeTabletPhase&);
    void FaultRecoverPhaseHandler(const MergeTabletPhase&);
    void EOFPhaseHandler(const MergeTabletPhase&);

private:
    const std::string id_;
    std::mutex mutex_;
    bool done_ = false;
    TabletPtr tablets_[2];
    TabletPtr merged_;
    TabletNodePtr dest_node_;

    std::shared_ptr<Procedure> unload_procs_[2];
    std::shared_ptr<Procedure> load_proc_;

    std::vector<MergeTabletPhase> phases_;
    std::shared_ptr<Procedure> recover_procs_[2];
    static std::map<MergeTabletPhase, MergeTabletPhaseHandler> phase_handlers_;
    ThreadPool* thread_pool_;
};

}
}

