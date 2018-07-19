// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include <glog/logging.h>
#include "master/load_tablet_procedure.h"
#include "master/move_tablet_procedure.h"
#include "master/master_env.h"
#include "proto/tabletnode_client.h"

DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_master_meta_table_path);
DECLARE_int32(tera_master_load_rpc_timeout);
DECLARE_int32(tera_master_control_tabletnode_retry_period);
DECLARE_int32(tera_master_impl_retry_times);
DECLARE_int32(tera_master_load_slow_retry_times);
DECLARE_bool(tera_stat_table_enabled);
DEFINE_int32(tablet_load_max_tried_ts, 3, "max number of tabletnodes "
        "a tablet can try to load on before it finally enter status kTableLoadFail");

namespace tera {
namespace master {

std::map<TabletEvent, LoadTabletProcedure::TabletLoadEventHandler> LoadTabletProcedure::event_handlers_ {
    {TabletEvent::kTsOffline,   std::bind(&LoadTabletProcedure::TabletNodeOffLineHandler, _1, _2)},
    {TabletEvent::kTsRestart,   std::bind(&LoadTabletProcedure::TabletNodeRestartHandler, _1, _2)},
    {TabletEvent::kTsLoadBusy,  std::bind(&LoadTabletProcedure::TabletNodeBusyHandler, _1, _2)},
    {TabletEvent::kTsDelayOffline,   std::bind(&LoadTabletProcedure::TabletPendOffLineHandler, _1, _2)},
    {TabletEvent::kUpdateMeta,  std::bind(&LoadTabletProcedure::UpdateMetaHandler, _1, _2)},
    {TabletEvent::kLoadTablet,  std::bind(&LoadTabletProcedure::LoadTabletHandler, _1, _2)},
    {TabletEvent::kWaitRpcResponse, std::bind(&LoadTabletProcedure::WaitRpcResponseHandler, _1, _2)},
    {TabletEvent::kTsLoadSucc,  std::bind(&LoadTabletProcedure::TabletNodeLoadSuccHandler, _1, _2)},
    {TabletEvent::kTsLoadFail,  std::bind(&LoadTabletProcedure::TabletNodeLoadFailHandler, _1, _2)},
    {TabletEvent::kTabletLoadFail,      std::bind(&LoadTabletProcedure::TabletLoadFailHandler, _1, _2)},
    {TabletEvent::kEofEvent,    std::bind(&LoadTabletProcedure::EOFHandler, _1, _2)}
};

LoadTabletProcedure::LoadTabletProcedure(TabletPtr tablet, ThreadPool* thread_pool) : 
    LoadTabletProcedure(tablet, TabletNodePtr(nullptr), thread_pool) {
}

LoadTabletProcedure::LoadTabletProcedure(TabletPtr tablet, TabletNodePtr dest_node,
                                         ThreadPool* thread_pool) :
    id_(std::string("LoadTablet:") + tablet->GetPath() + ":" + TimeStamp()),
    tablet_(tablet), 
    dest_node_(dest_node),
    done_(false),
    load_request_dispatching_(false),
    load_retrys_(0), 
    slow_load_retrys_(0),
    update_meta_done_(false),
    thread_pool_(thread_pool) {
    PROC_LOG(INFO) << "load tablet begin, tablet: " << tablet_->GetPath();
    PROC_CHECK(tablet_->GetStatus() == TabletMeta::kTabletOffline || 
        tablet_->GetStatus() == TabletMeta::kTabletDelayOffline ||
        tablet_->GetStatus() == TabletMeta::kTabletLoadFail);
    // corrupted DB need recovery,  set its's status from kTabletLoadFail to kTabletOffline thus 
    // we can recover it from status kTabletOffline
    if (tablet_->GetStatus() == TabletMeta::kTabletLoadFail && tablet_->HasErrorIgnoredLGs()) {
        tablet_->SetStatus(TabletMeta::kTabletOffline);   
    }
    if (dest_node_) {
        BindTabletToTabletNode(tablet_, dest_node_);
    }
}

// UpdateMeta & LoadTabletAsync is regard as an undivided process phase  and we issued LoadTabletAsync 
// immediately in UpdateMetaDone callback. 
// As currently we ensure that UpdateMeta will succ finally by 
void LoadTabletProcedure::UpdateMetaDone(bool) {
    PROC_LOG(INFO) << "update meta tablet success, " << tablet_;
    update_meta_done_.store(true);
}

void LoadTabletProcedure::LoadTabletAsyncWrapper(std::weak_ptr<LoadTabletProcedure> weak_proc, TabletNodePtr dest_node) {
    auto proc = weak_proc.lock();
    if (!proc) {
        LOG(WARNING) << "weak_ptr expired, giveup this loadtabletasync try";
        return;
    }
    return proc->LoadTabletAsync(dest_node);
}

void LoadTabletProcedure::LoadTabletAsync(TabletNodePtr dest_node) {
    if (dest_node->NodeDown()) {
        LOG(WARNING) << "dest node offline, giveup this load try node: " << dest_node->GetAddr();
        return;
    }

    tabletnode::TabletNodeClient node_client(thread_pool_, dest_node->GetAddr(),
            FLAGS_tera_master_load_rpc_timeout);
    LoadTabletRequest* request = new LoadTabletRequest;
    LoadTabletResponse* response = new LoadTabletResponse;
    request->set_tablet_name(tablet_->GetTableName());
    request->set_sequence_id(MasterEnv().SequenceId().Inc());
    request->mutable_key_range()->set_key_start(tablet_->GetKeyStart());
    request->mutable_key_range()->set_key_end(tablet_->GetKeyEnd());
    request->set_path(tablet_->GetPath());
    request->mutable_schema()->CopyFrom(tablet_->GetSchema());
    request->set_session_id(dest_node->uuid_);

    TablePtr table = tablet_->GetTable();
    TabletMeta meta;
    tablet_->ToMeta(&meta);
    PROC_CHECK(meta.parent_tablets_size() <= 2)
        << "too many parents tablets: " << meta.parent_tablets_size();
    for (int32_t i = 0; i < meta.parent_tablets_size(); ++i) {
        request->add_parent_tablets(meta.parent_tablets(i));
    }
    
    std::vector<std::string> ignore_err_lgs;
    tablet_->GetErrorIgnoredLGs(&ignore_err_lgs);
    for (uint32_t i = 0; i < ignore_err_lgs.size(); ++i) { 
        PROC_VLOG(6) << "tablet:" << tablet_->GetPath() << " Add ignore err lg to request :" << ignore_err_lgs[i];
        request->add_ignore_err_lgs(ignore_err_lgs[i]);
    }
    tablet_->SetErrorIgnoredLGs(); // clean error lg, only for this request once

    PROC_LOG(INFO) << "LoadTabletAsync id: " << request->sequence_id() << ", "
        << tablet_;
    // Bind "dest_node" to the first parameter of LoadTabletCallback as dest_node may decay to kOffLine state 
    // and we initiate RPC to a new started TabletNode on the same IP:PORT before we apperceive it
    // under which case we may have a success rpc with status code being "kIllegalAccess"
    //NOTICE: explicit cast from shared_ptr to weak_ptr is necessary!
    LoadClosure done = std::bind(&LoadTabletProcedure::LoadTabletCallbackWrapper, 
            std::weak_ptr<LoadTabletProcedure>(shared_from_this()), dest_node, _1, _2, _3, _4);
    node_client.LoadTablet(request, response, done);
}

void LoadTabletProcedure::LoadTabletCallbackWrapper(std::weak_ptr<LoadTabletProcedure> weak_proc,
                                            TabletNodePtr node, 
                                            LoadTabletRequest* request, 
                                            LoadTabletResponse* response, 
                                            bool failed, 
                                            int error_code) {
    auto proc = weak_proc.lock();
    if (!proc) {
        LOG(WARNING) << "weak_ptr expired, giveup this loadtabletcallback";
        return;
    }
    return proc->LoadTabletCallback(node, request, response, failed, error_code);
}


//NOTICE: please do not process tabletserver down/restart event in this CallBack as this CallBack should only
//focus on RPC callback related logic
void LoadTabletProcedure::LoadTabletCallback(TabletNodePtr node, 
                                            LoadTabletRequest* request, 
                                            LoadTabletResponse* response, 
                                            bool failed, 
                                            int error_code) {
    std::unique_ptr<LoadTabletRequest> request_holder(request);
    std::unique_ptr<LoadTabletResponse> response_holder(response);
    PROC_VLOG(23) << "load tablet: " << tablet_ << " callback: " << request_holder->sequence_id();
    if (tablet_->GetStatus() != TabletMeta::kTabletLoading) {
        load_request_dispatching_.store(false);
        PROC_LOG(WARNING) << "tablet: " << tablet_ << " fall into state: " 
            << StatusCodeToString(tablet_->GetStatus()) <<", discard this load tablet callback";
        return;
    }
    
    CHECK(tablet_->GetStatus() == TabletMeta::kTabletLoading);
    StatusCode status = response->status();
    // dest_node is still alive && rpc succ && return status is OK, then this load attempts is regarded as success
    if (!node->NodeDown() && !failed && (status == kTabletNodeOk || status == kTabletReady)) {
        PROC_LOG(INFO) << "id: " << request_holder->sequence_id() << "load tablet success, " << tablet_;
        load_request_dispatching_.store(false);
        if (FLAGS_tera_stat_table_enabled) {
            MasterEnv().GetStatTable()->ErasureTabletCorrupt(request_holder->path());
        }
        return;
    }
    // load failed
    //  TabletNode is down. Once the "node" is down, we regard the load attempt as fail and return immedialtely.
    //  No load retry will be issued unless a new TabletNode restarted on the same IP:PORT soonly or a 
    //  new TabletNode with different IP:PORT is scheduled for this LoadProcedure
    if (node->NodeDown()) {
        PROC_LOG(INFO) << "id: " << request_holder->sequence_id() << 
            "load tablet: " << tablet_ << "dest_node is down: " << node->GetAddr();
        load_retrys_++;
        return;
    }

    // TabletNode is not down but RPC failed: 
    // 1. RPC failed. 
    //    rpc connect failed, timeout, .etc. Or,
    // 2. tabletserver return inappropriate status such as
    //    kIOError, nfs error or tablet db corruption
    //    kTableOnLoad, tablet load very slowly with the first load failed with RPC_ERROR_REQUEST_TIMEOUT
    //    and succeeding load request return with kTableOnLoad until the tablet loaded successful finally. etc
    if (failed)  {
        PROC_LOG(WARNING) << "load tablet: "<< load_retrys_ << "th try failed, error: "  
            << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << tablet_; 
        load_retrys_++;
    } else if (status == kTabletOnLoad) {
        // 10 times for slow tablet
        if (++slow_load_retrys_ % 10 == 0) {
            PROC_LOG(ERROR) << kSms << "slow load, retry: " << load_retrys_ << ", " << tablet_;
            load_retrys_++;
        }
    } else { // maybe IOError as error happens when TabletNode access nfs
         PROC_LOG(WARNING) << "load tablet " << load_retrys_ << "th try failed, error: " 
            << StatusCodeToString(status) << ", " << tablet_;
        load_retrys_++;
    }
    // record detail fail msg to stat table
    if (response_holder->has_detail_fail_msg() && FLAGS_tera_stat_table_enabled) {
        std::string msg = response_holder->detail_fail_msg();
        MasterEnv().GetStatTable()->RecordTabletCorrupt(request_holder->path(), msg);
    }
    
    if (load_retrys_ > FLAGS_tera_master_impl_retry_times) {
        PROC_LOG(WARNING) << kSms << "load tablet: " << tablet_ << " failed at tabletnode: " << node->GetAddr();
        return;
    }

    //NOTICE: bind parameter "node" rather than member variable "dest_node_" to LoadTabletAsync
    //because we should only send retry request to the previous TS which we communicated failed and
    //succedent retries should be abandoned once TS is down. But dest_node_ maybe modified by 
    //TabletNodeOffLineHandler, leading unappropriate RPC retries to "dest_node_" 
    //if we pass "dest_node_" to LoadTabletAsync
    //NOTICE: explicit cast from shared_ptr to weak_ptr is necessary!
    ThreadPool::Task task = std::bind(&LoadTabletProcedure::LoadTabletAsyncWrapper, 
            std::weak_ptr<LoadTabletProcedure>(shared_from_this()), node);
    MasterEnv().GetThreadPool()->DelayTask(FLAGS_tera_master_control_tabletnode_retry_period, task);
    return;
}

TabletEvent LoadTabletProcedure::GenerateEvent() {
    if (tablet_->GetStatus() == TabletMeta::kTabletReady || 
        tablet_->GetStatus() == TabletMeta::kTabletLoadFail) {
        return TabletEvent::kEofEvent;
    }
    // we regard kTabletPending as a special status equivalent to kTableOffLine only with extra 
    // consideration for cache locality, so they both call GenrateTabletOffLineEvent()
    if (tablet_->GetStatus() == TabletMeta::kTabletOffline ||
        tablet_->GetStatus() == TabletMeta::kTabletDelayOffline) {
        return GenerateTabletOffLineEvent();
    }
    if (tablet_->GetStatus() == TabletMeta::kTabletLoading) {
        return GenerateTabletOnLoadEvent();
    }
    return TabletEvent::kEofEvent;
}

TabletEvent LoadTabletProcedure::GenerateTabletOffLineEvent() {
    if (!dest_node_) {
        return TabletEvent::kTsOffline;
    }
    if (dest_node_->NodeDown()) {
        return GenerateTsDownEvent();
    }
    // dest TS is alive, and load concurrency allows the tablet to be load, so generate load event
    else {
        // meta table always load immediately
        if (tablet_->GetTableName() == FLAGS_tera_master_meta_table_name) {
            return TabletEvent::kLoadTablet;
        }
        if (!update_meta_done_) {
            return TabletEvent::kUpdateMeta;
        }
        if (dest_node_->TryLoad(tablet_)) {
            return TabletEvent::kLoadTablet;
        }
        return TabletEvent::kTsLoadBusy;
    }
}

TabletEvent LoadTabletProcedure::GenerateTabletOnLoadEvent() {
    // all load attempts on the same TS (IP:PORT) all failed, return TS_LOAD_FAIL
    // should clear load_retrys_, dest_node_, loading_ and change tablet's state to kTableOffLine
    if (load_retrys_ > FLAGS_tera_master_impl_retry_times) {
        return TabletEvent::kTsLoadFail;
    }
    // 
    if (dest_node_->NodeDown()) {
        return GenerateTsDownEvent();
    }
    if (load_request_dispatching_) {
        return TabletEvent::kWaitRpcResponse;
    }
    else {
        return TabletEvent::kTsLoadSucc;
    }
}

TabletEvent LoadTabletProcedure::GenerateTsDownEvent() {
    // if meta tablet's dest_node down,  always return kTsOffLine, 
    // thus we can reschedule an available ts for meta table immediate
    if (tablet_->GetTableName() == FLAGS_tera_master_meta_table_name) {
        return TabletEvent::kTsOffline;
    }
    if (MasterEnv().GetTabletNodeManager()->FindTabletNode(dest_node_->GetAddr(), &restarted_dest_node_)) {
        return TabletEvent::kTsRestart;
    }
    if (dest_node_->GetState() == kPendingOffLine) {
        return TabletEvent::kTsDelayOffline;
    }
    // we should schedule an avail tabletnode for the tablet
    return TabletEvent::kTsOffline;
}

bool LoadTabletProcedure::IsNewEvent(TabletEvent event) {
    // except TabletEvent::kTsRestart, other two continuous equal events is indeed the same event
    // thus we can deal only once
    if (events_.empty() || events_.back() != event || 
        event == TabletEvent::kTsRestart || event == TabletEvent::kTsOffline) {
        if (!dest_node_) {
            PROC_VLOG(23) << "tablet: " << tablet_->GetPath() << ", event: " << event;
        }
        else {
            PROC_VLOG(23) << "tablet: " << tablet_->GetPath() 
                << ", event: " << event << ", dest_node: " << dest_node_->GetAddr();
        }
        events_.emplace_back(event);
        return true;
    }
    return false;
}

void LoadTabletProcedure::RunNextStage() {
    if (!IsNewEvent(GenerateEvent())) {
        return;
    }
    TabletEvent event = events_.back();
    auto it = event_handlers_.find(event);
    PROC_LOG_IF(FATAL, it == event_handlers_.end()) << "illegal event: " << event << ", tablet: " << tablet_;
    TabletLoadEventHandler handler = it->second;
    handler(this, event);
}

void LoadTabletProcedure::TabletNodeOffLineHandler(const TabletEvent& event) {
    tablet_->DoStateTransition(event);
    load_request_dispatching_.store(false);
    PROC_LOG(INFO) << "try schedule a tabletnode for tablet: " << tablet_->GetPath();
    const std::string table_name = tablet_->GetTableName();
    TabletNodePtr node;
    Scheduler* size_scheduler = MasterEnv().GetSizeScheduler().get();
    while (!node) {
        if (!MasterEnv().GetTabletNodeManager()->
                ScheduleTabletNodeOrWait(size_scheduler, table_name, false, &node)) {
            PROC_LOG(ERROR) << kSms << "fatal, cannot schedule tabletnode for tablet: " << tablet_;
            continue;
        }
        if (node->GetState() == kOffLine) {
            continue;
        }
    }
    dest_node_.swap(node);
    PROC_LOG(INFO) << "tablet: " << tablet_->GetPath() << ", pick destnode: " << dest_node_->GetAddr();
    BindTabletToTabletNode(tablet_, dest_node_);
    // reset 
    load_retrys_ = 0;
    slow_load_retrys_ = 0;
    update_meta_done_.store(false);
}

void LoadTabletProcedure::TabletNodeRestartHandler(const TabletEvent& event) {
    tablet_->DoStateTransition(event);
    load_request_dispatching_.store(false);
    dest_node_.swap(restarted_dest_node_);
    BindTabletToTabletNode(tablet_, dest_node_);
}

void LoadTabletProcedure::TabletNodeBusyHandler(const TabletEvent& event) {
    PROC_LOG(INFO) << "tabletnode: " << dest_node_->GetAddr() 
            << ", delay load tablet: " << tablet_->GetPath();
}

void LoadTabletProcedure::TabletPendOffLineHandler(const TabletEvent& event) {
    PROC_LOG(INFO) << "tablet: " << tablet_->GetPath() << ", current event: " << event 
                << " considering cache locality, tabletnode: " << dest_node_->GetAddr();
    tablet_->DoStateTransition(event);      
}

void LoadTabletProcedure::UpdateMetaHandler(const TabletEvent&) {
    MetaWriteRecord record = PackMetaWriteRecord(tablet_, false);
    UpdateMetaClosure done = std::bind(&LoadTabletProcedure::UpdateMetaDone, this, _1);
    MasterEnv().BatchWriteMetaTableAsync(record, done, -1);
}

void LoadTabletProcedure::LoadTabletHandler(const TabletEvent& event) {
    tablet_->DoStateTransition(event);
    load_request_dispatching_.store(true);
    PROC_LOG(INFO) << "load tablet: " << tablet_->GetPath() << ", destnode: " << dest_node_->GetAddr();
    LoadTabletAsync(dest_node_);
}

void LoadTabletProcedure::WaitRpcResponseHandler(const TabletEvent&) {
    return;
}

void LoadTabletProcedure::TabletNodeLoadSuccHandler(const TabletEvent& event) {
    tablet_->DoStateTransition(event);
    dest_node_->FinishLoad(tablet_);
    tablet_->ClearLoadFailedCount();
}

void LoadTabletProcedure::TabletNodeLoadFailHandler(const TabletEvent& event) {
    tablet_->DoStateTransition(event);
    dest_node_->FinishLoad(tablet_);
    tablet_->IncLoadFailedCount();
}

void LoadTabletProcedure::TabletLoadFailHandler(const TabletEvent& event) {
    tablet_->DoStateTransition(event);
    PROC_LOG(ERROR) << "tablet: " << tablet_
                << "event: "<< event << ", exhausted all load retries, abort load" ;
}

void LoadTabletProcedure::EOFHandler(const TabletEvent& event) {
    PROC_LOG(INFO) << "tablet: " << tablet_ << "," << event;
    if (tablet_->LoadFailedCount() >= FLAGS_tablet_load_max_tried_ts) {
        PROC_LOG(ERROR) << "tablet: " << tablet_
                << "event: "<< event << ", exhausted all load retries, abort load" ;
    }
    // we still have opportunities to load the failed tablet on other tabletnodes, so we generate 
    // a new MoveTabletProcedure and finish current LoadTabletProcedure. 
    // Notice do not unlock tablet TransitionLock if a MoveTabletProcedure is generated.
    if (tablet_->GetStatus() != TabletMeta::kTabletReady && 
        tablet_->LoadFailedCount() < FLAGS_tablet_load_max_tried_ts) {
        // Since LoadTablet is executed asynchronously on TS, master cannot get tablet's exact status 
        // on the remote TS when master got a LoadTablet failed RPC response. For example, master will 
        // tread tablet as loaded failed with RPC error code "RPC_TIMEOUT" if LoadTablet RPC request is 
        // delayed by TS for a long time, but TS may finally execute LoadTablet successfully without master
        // perception. So here master tread the loaded failed tablet as Ready and issue a Move action
        // (which consists of an Unload and a following Load sub actions) for the tablet.
        PROC_LOG(INFO) << "try move to recover the load failed tablet: " << tablet_;
        tablet_->SetStatus(TabletMeta::kTabletReady);
        std::shared_ptr<Procedure> move_proc(new MoveTabletProcedure(tablet_, 
                    TabletNodePtr(nullptr), MasterEnv().GetThreadPool().get()));
        if (!MasterEnv().GetExecutor()->AddProcedure(move_proc)) {
            PROC_LOG(WARNING) << "add move tablet procedure failed, tablet: " << tablet_;
        }
    }
    else {
        tablet_->UnlockTransition();
    }
    done_.store(true);


}

}
}
