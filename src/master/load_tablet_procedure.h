// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <atomic>
#include <mutex>
#include "master/procedure.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"
#include "master/tablet_state_machine.h"

namespace tera {
namespace master {

class LoadTabletProcedure : public Procedure, public std::enable_shared_from_this<LoadTabletProcedure> {
public:
    LoadTabletProcedure(TabletPtr tablet, ThreadPool* thread_pool);
    
    LoadTabletProcedure(TabletPtr tablet, TabletNodePtr dest_node,
                        ThreadPool* thread_pool);

    virtual std::string ProcId() const {
        //std::string id = std::string("LoadTablet:") + tablet_->GetPath();
        return id_;
    }
    virtual void RunNextStage();
        
    virtual ~LoadTabletProcedure() {}

    virtual bool Done() {
        return done_.load();
    }

private:
    typedef std::function<void (LoadTabletProcedure*, const TabletEvent&)> TabletLoadEventHandler;
    
    typedef std::function<void (LoadTabletRequest*, LoadTabletResponse*, bool, int)> LoadClosure;
    
    TabletEvent GenerateEvent();
    
    TabletEvent GenerateTabletOffLineEvent();

    TabletEvent GenerateTabletOnLoadEvent();

    TabletEvent GenerateTsDownEvent();

    // unique events we need to process
    bool IsNewEvent(TabletEvent event);

    void UpdateMetaDone(bool);
    
    static void LoadTabletAsyncWrapper(std::weak_ptr<LoadTabletProcedure> weak_proc, TabletNodePtr dest_node);
 
    static void LoadTabletCallbackWrapper(std::weak_ptr<LoadTabletProcedure> weak_proc, 
                        TabletNodePtr node, 
                        LoadTabletRequest* request, 
                        LoadTabletResponse* response, 
                        bool failed, 
                        int error_code);
    
    void LoadTabletAsync(TabletNodePtr dest_node);

    void LoadTabletCallback(TabletNodePtr node, 
                        LoadTabletRequest* request, 
                        LoadTabletResponse* response, 
                        bool failed, 
                        int error_code);
    
    // EventHandlers
    void TabletNodeOffLineHandler(const TabletEvent& event);
    void TabletNodeRestartHandler(const TabletEvent& event);
    void TabletNodeBusyHandler(const TabletEvent& event);
    void TabletPendOffLineHandler(const TabletEvent& event);
    void UpdateMetaHandler(const TabletEvent& event);
    void LoadTabletHandler(const TabletEvent& event);
    void WaitRpcResponseHandler(const TabletEvent& event);
    void TabletNodeLoadSuccHandler(const TabletEvent& event);
    void TabletNodeLoadFailHandler(const TabletEvent& event);
    void TabletLoadFailHandler(const TabletEvent& event);
    void EOFHandler(const TabletEvent& event);

private: 
    const std::string id_;
    TabletPtr tablet_;
    TabletNodePtr dest_node_;
    // following counters or flags way be accessed from different threads concurrently, 
    // so std::atomic is used to ensure ordered access to those variables from different threads
    std::atomic<bool> done_;
    std::atomic<bool> load_request_dispatching_;
    std::atomic<int32_t> load_retrys_;
    std::atomic<int32_t> slow_load_retrys_;
    std::atomic<bool> update_meta_done_;
    std::vector<TabletEvent> events_;
    TabletNodePtr restarted_dest_node_;
    static std::map<TabletEvent, TabletLoadEventHandler> event_handlers_;
    ThreadPool* thread_pool_;
};

}
}
