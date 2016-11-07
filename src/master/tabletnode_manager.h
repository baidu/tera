// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_TABLETNODE_MANAGER_H_
#define TERA_MASTER_TABLETNODE_MANAGER_H_

#include <list>
#include <map>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

#include "common/mutex.h"
#include "common/thread_pool.h"

#include "master/tablet_manager.h"
#include "proto/proto_helper.h"

namespace tera {
namespace master {

enum NodeState {
    kReady = kTabletNodeReady,
    kOffLine = kTabletNodeOffLine, // before first query succe
    kOnKick = kTabletNodeOnKick,
    kWaitKick = kTabletNodeWaitKick
};

std::string NodeStateToString(NodeState state);

struct TabletNode {
    mutable Mutex mutex_;
    std::string addr_;
    std::string uuid_;
    NodeState state_;

    // updated by query
    TabletNodeStatus report_status_;
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
    uint32_t onsplit_count_;
    uint32_t plan_move_in_count_;
    std::list<TabletPtr> wait_load_list_;
    std::list<TabletPtr> wait_split_list_;

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

    bool TryLoad(TabletPtr tablet);
    void BeginLoad();
    bool FinishLoad(TabletPtr tablet);
    bool LoadNextWaitTablet(TabletPtr* tablet);

    bool TrySplit(TabletPtr tablet);
    bool FinishSplit(TabletPtr tablet);
    bool SplitNextWaitTablet(TabletPtr* tablet);

    NodeState GetState();
    bool SetState(NodeState new_state, NodeState* old_state);
    bool CheckStateSwitch(NodeState old_state, NodeState new_state);

    uint32_t GetQueryFailCount();
    uint32_t IncQueryFailCount();
    void ResetQueryFailCount();

private:
    TabletNode& operator=(const TabletNode& t);
};

typedef boost::shared_ptr<TabletNode> TabletNodePtr;

class WorkloadGetter;
class Scheduler;
class MasterImpl;

class TabletNodeManager {
public:
    explicit TabletNodeManager(MasterImpl* master_impl);
    ~TabletNodeManager();

    void AddTabletNode(const std::string& addr, const std::string& uuid);
    void DelTabletNode(const std::string& addr);
    void UpdateTabletNode(const std::string& addr, const TabletNode& info);
    bool FindTabletNode(const std::string& addr, TabletNodePtr* info);
    void GetAllTabletNodeAddr(std::vector<std::string>* addr_array);
    void GetAllTabletNodeId(std::map<std::string, std::string>* id_map);
    void GetAllTabletNodeInfo(std::vector<TabletNodePtr>* info_array);
    bool ScheduleTabletNode(Scheduler* scheduler, const std::string& table_name,
                            bool is_move, std::string* node_addr);
    bool ScheduleTabletNode(Scheduler* scheduler, const std::string& table_name,
                            bool is_move, TabletNodePtr* node);
    bool ShouldMoveData(Scheduler* scheduler, const std::string& table_name,
                        TabletNodePtr src_node, TabletNodePtr dst_node,
                        const std::vector<TabletPtr>& tablet_candidates,
                        size_t* tablet_index);
    bool CheckStateSwitch(NodeState old_state, NodeState new_state);

private:
    mutable Mutex mutex_;
    MasterImpl* master_impl_;

    typedef std::map<std::string, TabletNodePtr> TabletNodeList;
    TabletNodeList tabletnode_list_;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_TABLETNODE_MANAGER_H_
