// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_TABLETNODE_MANAGER_H_
#define TERA_MASTER_TABLETNODE_MANAGER_H_

#include <list>
#include <map>
#include <string>
#include <vector>

#include <sofa/pbrpc/smart_ptr/shared_ptr.hpp>

#include "common/mutex.h"
#include "common/thread_pool.h"

#include "master/tablet_manager.h"
#include "master/task_spatula.h"
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
    mutable Mutex m_mutex;
    std::string m_addr;
    std::string m_uuid;
    NodeState m_state;

    // updated by query
    TabletNodeStatus m_report_status;
    TabletNodeInfo m_info;
    uint64_t m_data_size;
    uint64_t m_load;
    uint64_t m_update_time;
    std::map<std::string, uint64_t> m_table_size;

    uint32_t m_query_fail_count;
    uint32_t m_plan_move_in_count;
    
    TaskSpatula m_load_spatula;
    TaskSpatula m_unload_spatula;
    TaskSpatula m_split_spatula;

    // The start time of recent load operation.
    // Used to tell if node load too many tablets within short time.
    // Keep FLAGS_tera_master_max_load_concurrency items at maximum.
    std::list<int64_t> m_recent_load_time_list;

    TabletNode();
    TabletNode(const std::string& addr, const std::string& uuid);
    ~TabletNode();

    TabletNodeInfo GetInfo();
    const std::string& GetAddr();
    const std::string& GetId();
    uint64_t GetTableSize(const std::string& table_name);
    uint64_t GetSize();

    // for adjusting tabletnode data size & m_recent_load_time_list
    void AddTablet(const TabletPtr tablet);

    // for adjusting tabletnode data size
    void DeleteTablet(const TabletPtr tablet);

    uint32_t GetPlanToMoveInCount();
    void PlanToMoveIn();
    void DoneMoveIn();

    // To tell if node load too many tablets within short time.
    bool MayLoadNow();

    NodeState GetState();
    bool SetState(NodeState new_state, NodeState* old_state);
    bool CheckStateSwitch(NodeState old_state, NodeState new_state);

    uint32_t GetQueryFailCount();
    uint32_t IncQueryFailCount();
    void ResetQueryFailCount();

private:
    TabletNode(const TabletNode& t);
    TabletNode& operator=(const TabletNode& t);
};

typedef sofa::pbrpc::shared_ptr<TabletNode> TabletNodePtr;

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
    bool ScheduleTabletNode(Scheduler* scheduler, std::string* node_addr);
    bool ScheduleTabletNode(Scheduler* scheduler, const std::string& table_name,
                            std::string* node_addr);
    bool CheckStateSwitch(NodeState old_state, NodeState new_state);
    bool IsNodeOverloadThanAverage(const std::string& node_addr);
    bool IsNodeOverloadThanLeast(const std::string& node_addr);
    bool ShouldMoveToLeastNode(const std::string& node_addr,
                               uint64_t move_data_size);
    bool ShouldMoveData(const std::string& src_node_addr,
                        const std::string& dst_node_addr,
                        const std::string& table_name,
                        uint64_t move_data_size);

private:
    bool ShouldMoveData(TabletNodePtr src_node, TabletNodePtr dst_node,
                        const std::string& table_name,
                        uint64_t move_data_size);

    mutable Mutex m_mutex;
    MasterImpl* m_master_impl;

    typedef std::map<std::string, TabletNodePtr> TabletNodeList;
    TabletNodeList m_tabletnode_list;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_TABLETNODE_MANAGER_H_
