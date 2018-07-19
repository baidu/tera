// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_TABLETNODE_MANAGER_H_
#define TERA_MASTER_TABLETNODE_MANAGER_H_

#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/mutex.h"
#include "common/thread_pool.h"
#include "master/tablet_manager.h"
#include "proto/proto_helper.h"

namespace tera {
namespace master {

class Tablet;
typedef std::shared_ptr<Tablet> TabletPtr;

enum NodeState {
    kReady = kTabletNodeReady,
    kOffLine = kTabletNodeOffLine, // before first query succe
    kPendingOffLine = kTabletNodePendingOffLine,
    kOnKick = kTabletNodeOnKick,
    kWaitKick = kTabletNodeWaitKick
};


std::string NodeStateToString(NodeState state);

struct TabletNode {
    mutable Mutex mutex_;
    std::string addr_;
    std::string uuid_;
    NodeState state_;
    // state timestamp
    int64_t timestamp_;

    // updated by query
    StatusCode report_status_;
    TabletNodeInfo info_;
    uint64_t data_size_;
    uint64_t qps_;
    uint64_t load_;
    uint64_t update_time_;
    std::map<std::string, uint64_t> table_size_;
    std::map<std::string, uint64_t> table_qps_;

    struct MutableCounter {
        uint64_t read_pending_;
        uint64_t write_pending_;
        uint64_t scan_pending_;
        uint64_t row_read_delay_; // micros

        MutableCounter() {
            memset(this, 0, sizeof(MutableCounter));
        }
    };
    MutableCounter average_counter_;
    MutableCounter accumulate_counter_;
    std::list<MutableCounter> counter_list_;

    uint32_t query_fail_count_;
    uint32_t onload_count_;
    uint32_t unloading_count_;
    uint32_t onsplit_count_;
    uint32_t plan_move_in_count_;
    //std::list<TabletPtr> wait_load_list_;
    //std::list<std::pair<TabletPtr, std::string> > wait_split_list_; // (tablet, split_key)

    // The start time of recent load operation.
    // Used to tell if node load too many tablets within short time.
    // Keep FLAGS_tera_master_max_load_concurrency items at maximum.
    std::list<int64_t> recent_load_time_list_;

    TabletNode();
    TabletNode(const std::string& addr, const std::string& uuid);
    TabletNode(const TabletNode& t);
    ~TabletNode();

    TabletNodeInfo GetInfo();
    const std::string& GetAddr();
    const std::string& GetId();

    // table_name == "" means all tables
    uint64_t GetSize(const std::string& table_name = "");
    uint64_t GetQps(const std::string& table_name = "");
    uint64_t GetReadPending();
    uint64_t GetWritePending();
    uint64_t GetScanPending();
    uint64_t GetRowReadDelay();

    uint32_t GetPlanToMoveInCount();
    void PlanToMoveIn();
    void DoneMoveIn();

    // To tell if node load too many tablets within short time.
    bool MayLoadNow();

    void UpdateSize(TabletPtr tablet);

    bool TryLoad(TabletPtr tablet);
    void BeginLoad();
    bool FinishLoad(TabletPtr tablet);

    bool TrySplit(TabletPtr tablet, const std::string& split_key = "");
    bool FinishSplit();

    bool CanUnload();
    void FinishUnload();

    NodeState GetState();
    bool NodeDown() {
        MutexLock lock(&mutex_);
        if (state_ == kOffLine) {
            return true;
        }
        return false;
    }

    bool SetState(NodeState new_state, NodeState* old_state);
    bool CheckStateSwitch(NodeState old_state, NodeState new_state);

    uint32_t GetQueryFailCount();
    uint32_t IncQueryFailCount();
    void ResetQueryFailCount();
    int64_t GetTimeStamp() {return timestamp_;}    

private:
    TabletNode& operator=(const TabletNode& t);
};

typedef std::shared_ptr<TabletNode> TabletNodePtr;

void BindTabletToTabletNode(TabletPtr tablet, TabletNodePtr node);

class WorkloadGetter;
class Scheduler;
class MasterImpl;

class TabletNodeManager {
public:
    explicit TabletNodeManager(MasterImpl* master_impl);
    ~TabletNodeManager();

    TabletNodePtr AddTabletNode(const std::string& addr, const std::string& uuid);

    // return the deleted tabletnode, if not exists return TabletNodePtr(nullptr)
    TabletNodePtr DelTabletNode(const std::string& addr);
    void UpdateTabletNode(const std::string& addr, const TabletNode& info);
    TabletNodePtr FindTabletNode(const std::string& addr, TabletNodePtr* info);
    void GetAllTabletNodeAddr(std::vector<std::string>* addr_array);
    void GetAllTabletNodeId(std::map<std::string, std::string>* id_map);
    void GetAllTabletNodeInfo(std::vector<TabletNodePtr>* info_array);
    bool ScheduleTabletNode(Scheduler* scheduler, const std::string& table_name,
                            bool is_move, TabletNodePtr* node);
    bool ScheduleTabletNodeOrWait(Scheduler* scheduler, const std::string& table_name,
                            bool is_move, TabletNodePtr* node);
    bool ShouldMoveData(Scheduler* scheduler, const std::string& table_name,
                        TabletNodePtr src_node, TabletNodePtr dst_node,
                        const std::vector<TabletPtr>& tablet_candidates,
                        size_t* tablet_index);
    bool CheckStateSwitch(NodeState old_state, NodeState new_state);

    void GetTablets(const std::string& server_addr, std::vector<TabletPtr>* tablet_list);

private:
    bool ScheduleTabletNode(Scheduler* scheduler, const std::string& table_name,
                            bool is_move, TabletNodePtr* node, bool wait);

    mutable Mutex mutex_;
    CondVar tabletnode_added_;
    MasterImpl* master_impl_;

    typedef std::map<std::string, TabletNodePtr> TabletNodeList;
    TabletNodeList tabletnode_list_;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_TABLETNODE_MANAGER_H_
