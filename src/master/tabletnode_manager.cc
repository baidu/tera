// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/tabletnode_manager.h"

#include "master/master_impl.h"
#include "master/workload_scheduler.h"
#include "utils/timer.h"

DECLARE_string(tera_master_meta_table_name);
DECLARE_int32(tera_master_max_load_concurrency);
DECLARE_int32(tera_master_max_split_concurrency);
DECLARE_int32(tera_master_load_interval);
DECLARE_bool(tera_master_meta_isolate_enabled);

namespace tera {
namespace master {

TabletNode::TabletNode() : m_state(kOffLine),
    m_report_status(kTabletNodeInit), m_data_size(0), m_load(0),
    m_update_time(0), m_query_fail_count(0), m_onload_count(0),
    m_onsplit_count(0), m_plan_move_in_count(0) {
    m_info.set_addr("");
}

TabletNode::TabletNode(const std::string& addr, const std::string& uuid)
    : m_addr(addr), m_uuid(uuid), m_state(kOffLine),
      m_report_status(kTabletNodeInit), m_data_size(0), m_load(0),
      m_update_time(0), m_query_fail_count(0), m_onload_count(0),
      m_onsplit_count(0), m_plan_move_in_count(0) {
    m_info.set_addr(addr);
}

TabletNode::~TabletNode() {}

TabletNodeInfo TabletNode::GetInfo() {
    MutexLock lock(&m_mutex);
    return m_info;
}

const std::string& TabletNode::GetAddr() {
    return m_addr;
}

const std::string& TabletNode::GetId() {
    return m_uuid;
}

uint64_t TabletNode::GetSize(const std::string& table_name) {
    MutexLock lock(&m_mutex);
    if (table_name.empty()) {
        return m_data_size;
    }
    uint64_t table_size = 0;
    std::map<std::string, uint64_t>::iterator it = m_table_size.find(table_name);
    if (it != m_table_size.end()) {
        table_size = it->second;
    }
    return table_size;
}

uint64_t TabletNode::GetQps(const std::string& table_name) {
    MutexLock lock(&m_mutex);
    if (table_name.empty()) {
        return m_qps;
    }
    uint64_t table_qps = 0;
    std::map<std::string, uint64_t>::iterator it = m_table_qps.find(table_name);
    if (it != m_table_qps.end()) {
        table_qps = it->second;
    }
    return table_qps;
}

uint64_t TabletNode::GetReadPending() {
    MutexLock lock(&m_mutex);
    return m_average_counter.m_read_pending;
}

uint64_t TabletNode::GetRowReadDelay() {
    MutexLock lock(&m_mutex);
    return m_average_counter.m_row_read_delay;
}

uint32_t TabletNode::GetPlanToMoveInCount() {
    MutexLock lock(&m_mutex);
    VLOG(8) << "GetPlanToMoveInCount: " << m_addr << " " << m_plan_move_in_count;
    return m_plan_move_in_count;
}

void TabletNode::PlanToMoveIn() {
    MutexLock lock(&m_mutex);
    m_plan_move_in_count++;
    VLOG(8) << "PlanToMoveIn: " << m_addr << " " << m_plan_move_in_count;
}

void TabletNode::DoneMoveIn() {
    MutexLock lock(&m_mutex);
    // TODO (likang): If node restart just before a tablet move in,
    // this count will be reset to 0. So we have to make sure it is greater
    // than 0 before dec.
    if (m_plan_move_in_count > 0) {
        m_plan_move_in_count--;
    }
    VLOG(8) << "DoneMoveIn: " << m_addr << " " << m_plan_move_in_count;
}

bool TabletNode::MayLoadNow() {
    MutexLock lock(&m_mutex);
    if (m_recent_load_time_list.size() < static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency)) {
        return true;
    }
    if (m_recent_load_time_list.front() + FLAGS_tera_master_load_interval * 1000000
        <= get_micros()) {
        return true;
    }
    VLOG(8) << "MayLoadNow() " << m_addr << " last load time: "
            << (get_micros() - m_recent_load_time_list.front()) / 1000000 << " seconds ago";
    return false;
}

bool TabletNode::TryLoad(TabletPtr tablet) {
    MutexLock lock(&m_mutex);
    m_data_size += tablet->GetDataSize();
    if (m_table_size.find(tablet->GetTableName()) != m_table_size.end()) {
        m_table_size[tablet->GetTableName()] += tablet->GetDataSize();
    } else {
        m_table_size[tablet->GetTableName()] = tablet->GetDataSize();
    }
    m_qps += tablet->GetAverageCounter().read_rows();
    if (m_table_qps.find(tablet->GetTableName()) != m_table_qps.end()) {
        m_table_qps[tablet->GetTableName()] += tablet->GetAverageCounter().read_rows();
    } else {
        m_table_qps[tablet->GetTableName()] = tablet->GetAverageCounter().read_rows();
    }
    //VLOG(5) << "load on: " << m_addr << ", size: " << tablet->GetDataSize()
    //      << ", total size: " << m_data_size;
    if (m_wait_load_list.empty()
        && m_onload_count < static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency)) {
        BeginLoad();
        return true;
    }
    m_wait_load_list.push_back(tablet);
    return false;
}

void TabletNode::BeginLoad() {
    ++m_onload_count;
    m_recent_load_time_list.push_back(get_micros());
    uint32_t list_size = m_recent_load_time_list.size();
    if (list_size > static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency)) {
        CHECK_EQ(list_size - 1, static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency));
        m_recent_load_time_list.pop_front();
    }
}

bool TabletNode::FinishLoad(TabletPtr tablet) {
    MutexLock lock(&m_mutex);
    assert(m_onload_count > 0);
    --m_onload_count;
    return true;
}

bool TabletNode::LoadNextWaitTablet(TabletPtr* tablet) {
    MutexLock lock(&m_mutex);
    if (m_onload_count >= static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency)) {
        return false;
    }
    std::list<TabletPtr>::iterator it = m_wait_load_list.begin();
    if (it == m_wait_load_list.end()) {
        return false;
    }
    *tablet = *it;
    m_wait_load_list.pop_front();
    BeginLoad();
    return true;
}

bool TabletNode::TrySplit(TabletPtr tablet) {
    MutexLock lock(&m_mutex);
    m_data_size -= tablet->GetDataSize();
//    VLOG(5) << "split on: " << m_addr << ", size: " << tablet->GetDataSize()
//        << ", total size: " << m_data_size;
    if (m_wait_split_list.empty()
        && m_onsplit_count < static_cast<uint32_t>(FLAGS_tera_master_max_split_concurrency)) {
        ++m_onsplit_count;
        return true;
    }
    if (std::find(m_wait_split_list.begin(), m_wait_split_list.end(), tablet) ==
        m_wait_split_list.end()) {
        m_wait_split_list.push_back(tablet);
    }
    return false;
}

bool TabletNode::FinishSplit(TabletPtr tablet) {
    MutexLock lock(&m_mutex);
    --m_onsplit_count;
    return true;
}

bool TabletNode::SplitNextWaitTablet(TabletPtr* tablet) {
    MutexLock lock(&m_mutex);
    if (m_onsplit_count >= static_cast<uint32_t>(FLAGS_tera_master_max_split_concurrency)) {
        return false;
    }
    std::list<TabletPtr>::iterator it = m_wait_split_list.begin();
    if (it == m_wait_split_list.end()) {
        return false;
    }
    *tablet = *it;
    m_wait_split_list.pop_front();
    ++m_onsplit_count;
    return true;
}

NodeState TabletNode::GetState() {
    MutexLock lock(&m_mutex);
    return m_state;
}

bool TabletNode::SetState(NodeState new_state, NodeState* old_state) {
    MutexLock lock(&m_mutex);
    if (NULL != old_state) {
        *old_state = m_state;
    }
    if (CheckStateSwitch(m_state, new_state)) {
        LOG(INFO) << m_addr << " state switch "
            << StatusCodeToString(m_state) << " to "
            << StatusCodeToString(new_state);
        m_state = new_state;
        return true;
    }
    VLOG(5) << m_addr << " not support state switch "
        << StatusCodeToString(m_state) << " to "
        << StatusCodeToString(new_state);
    return false;
}


bool TabletNode::CheckStateSwitch(NodeState old_state, NodeState new_state) {
    switch (old_state) {
    case kReady:
        if (new_state == kOffLine || new_state == kWaitKick) {
            return true;
        }
        break;
    case kOffLine:
        if (new_state == kReady || new_state == kWaitKick) {
            return true;
        }
        break;
    case kWaitKick:
        if (new_state == kOnKick) {
            return true;
        }
        break;
    default:
        break;
    }
    return false;
}

uint32_t TabletNode::GetQueryFailCount() {
    MutexLock lock(&m_mutex);
    return m_query_fail_count;
}

uint32_t TabletNode::IncQueryFailCount() {
    MutexLock lock(&m_mutex);
    return ++m_query_fail_count;
}

void TabletNode::ResetQueryFailCount() {
    MutexLock lock(&m_mutex);
    m_query_fail_count = 0;
}

TabletNodeManager::TabletNodeManager(MasterImpl* master_impl)
    : m_master_impl(master_impl) {}

TabletNodeManager::~TabletNodeManager() {
    MutexLock lock(&m_mutex);
}

void TabletNodeManager::AddTabletNode(const std::string& addr,
                                      const std::string& uuid) {
    MutexLock lock(&m_mutex);
    TabletNodePtr null_ptr;
    std::pair<TabletNodeList::iterator, bool> ret = m_tabletnode_list.insert(
        std::pair<std::string, TabletNodePtr>(addr, null_ptr));
    if (!ret.second) {
        LOG(ERROR) << "tabletnode [" << addr << "] exists";
        return;
    }
    TabletNodePtr& state = ret.first->second;
    state.reset(new TabletNode(addr, uuid));
    LOG(INFO) << "add tabletnode : " << addr << ", id : " << uuid;
}

void TabletNodeManager::DelTabletNode(const std::string& addr) {
    TabletNodePtr state;
    {
        MutexLock lock(&m_mutex);
        TabletNodeList::iterator it = m_tabletnode_list.find(addr);
        if (it == m_tabletnode_list.end()) {
            LOG(ERROR) << "tabletnode [" << addr << "] does not exist";
            return;
        }
        state = it->second;
        m_tabletnode_list.erase(it);
    }
    // delete node may block, so we'd better release the mutex before that
    LOG(INFO) << "delete tabletnode : " << addr;
}

void TabletNodeManager::UpdateTabletNode(const std::string& addr,
                                         const TabletNode& state) {
    MutexLock lock(&m_mutex);
    TabletNodeList::iterator it = m_tabletnode_list.find(addr);
    if (it == m_tabletnode_list.end()) {
        LOG(ERROR) << "tabletnode [" << addr << "] does not exist";
        return;
    }
    TabletNode* node = it->second.get();
    MutexLock node_lock(&node->m_mutex);
    node->m_report_status = state.m_report_status;
    node->m_data_size = state.m_data_size;
    node->m_qps = state.m_qps;
    node->m_info = state.m_info;
    node->m_info.set_addr(addr);
    node->m_load = state.m_load;
    node->m_update_time = state.m_update_time;
    node->m_table_size = state.m_table_size;
    node->m_table_qps = state.m_table_qps;

    node->m_info.set_status_m(NodeStateToString(node->m_state));
    node->m_info.set_tablet_onload(node->m_onload_count);
    node->m_info.set_tablet_onsplit(node->m_onsplit_count);

    node->m_average_counter.m_read_pending =
        CounterWeightedSum(state.m_info.read_pending(),
                           node->m_average_counter.m_read_pending);
    node->m_average_counter.m_row_read_delay =
        CounterWeightedSum(state.m_info.extra_info(1).value(),
                           node->m_average_counter.m_row_read_delay);
    VLOG(15) << "update tabletnode : " << addr;
}

void TabletNodeManager::GetAllTabletNodeAddr(std::vector<std::string>* addr_array) {
    MutexLock lock(&m_mutex);
    TabletNodeList::iterator it = m_tabletnode_list.begin();
    for (; it != m_tabletnode_list.end(); ++it) {
        addr_array->push_back(it->first);
    }
}

void TabletNodeManager::GetAllTabletNodeId(std::map<std::string, std::string>* id_map) {
    MutexLock lock(&m_mutex);
    TabletNodeList::iterator it = m_tabletnode_list.begin();
    for (; it != m_tabletnode_list.end(); ++it) {
        TabletNodePtr node = it->second;
        MutexLock lock2(&node->m_mutex);
        (*id_map)[it->first] = node->m_uuid;
    }
}

void TabletNodeManager::GetAllTabletNodeInfo(std::vector<TabletNodePtr>* array) {
    MutexLock lock(&m_mutex);
    TabletNodeList::iterator it = m_tabletnode_list.begin();
    for (; it != m_tabletnode_list.end(); ++it) {
        array->push_back(it->second);
    }
}

bool TabletNodeManager::FindTabletNode(const std::string& addr,
                                       TabletNodePtr* state) {
    MutexLock lock(&m_mutex);
    TabletNodeList::iterator it = m_tabletnode_list.find(addr);
    if (it == m_tabletnode_list.end()) {
        LOG(WARNING) << "tabletnode [" << addr << "] does not exist";
        return false;
    }
    if (NULL != state) {
        *state = it->second;
    }
    return true;
}

bool TabletNodeManager::ScheduleTabletNode(Scheduler* scheduler, const std::string& table_name,
                                           bool is_move, std::string* node_addr) {
    TabletNodePtr node;
    if (ScheduleTabletNode(scheduler, table_name, is_move, &node)) {
        *node_addr = node->GetAddr();
        return true;
    }
    return false;
}

bool TabletNodeManager::ScheduleTabletNode(Scheduler* scheduler, const std::string& table_name,
                                           bool is_move, TabletNodePtr* node) {
    VLOG(7) << "ScheduleTabletNode()";
    MutexLock lock(&m_mutex);
    std::string meta_node_addr;
    m_master_impl->GetMetaTabletAddr(&meta_node_addr);

    TabletNodePtr null_ptr, meta_node;
    std::vector<TabletNodePtr> candidates;
    std::vector<TabletNodePtr> slow_candidates;

    TabletNodeList::const_iterator it = m_tabletnode_list.begin();
    for (; it != m_tabletnode_list.end(); ++it) {
        TabletNodePtr tablet_node = it->second;
        if (tablet_node->m_state != kReady) {
            continue;
        }
        if (FLAGS_tera_master_meta_isolate_enabled
            && tablet_node->m_addr == meta_node_addr) {
            meta_node = tablet_node;
            continue;
        }
        if (is_move) {
            if (!tablet_node->MayLoadNow()) {
                continue;
            }
            if (tablet_node->GetPlanToMoveInCount() > 0) {
                continue;
            }
        }
        if (tablet_node->m_average_counter.m_read_pending < 100) {
            candidates.push_back(tablet_node);
        } else {
            slow_candidates.push_back(tablet_node);
        }
    }
    if (candidates.size() == 0) {
        candidates = slow_candidates;
    }
    if (candidates.size() == 0) {
        if (meta_node != null_ptr) {
            *node = meta_node;
            return true;
        } else {
            return false;
        }
    }

    size_t best_index = 0;
    if (scheduler->FindBestNode(candidates, table_name, &best_index)) {
        *node = candidates[best_index];
        return true;
    }
    return false;
}

bool TabletNodeManager::ShouldMoveData(Scheduler* scheduler, const std::string& table_name,
                                       TabletNodePtr src_node, TabletNodePtr dst_node,
                                       const std::vector<TabletPtr>& tablet_candidates,
                                       size_t* tablet_index) {
    VLOG(7) << "ShouldMoveData()";
    MutexLock lock(&m_mutex);
    if (tablet_candidates.size() == 0) {
        return false;
    }
    if (src_node == dst_node) {
        return false;
    }
    if (dst_node->GetState() != kReady) {
        return false;
    }
    if (dst_node->m_average_counter.m_read_pending > 100) {
        return false;
    }
    if (!dst_node->MayLoadNow()) {
        return false;
    }
    if (dst_node->GetPlanToMoveInCount() > 0) {
        return false;
    }
    if (FLAGS_tera_master_meta_isolate_enabled) {
        std::string meta_node_addr;
        m_master_impl->GetMetaTabletAddr(&meta_node_addr);
        if (dst_node->GetAddr() == meta_node_addr) {
            return false;
        }
        if (src_node->GetAddr() == meta_node_addr) {
            *tablet_index = 0;
            return true;
        }
    }
    return scheduler->FindBestTablet(src_node, dst_node, tablet_candidates,
                                     table_name, tablet_index);
}

std::string NodeStateToString(NodeState state) {
    switch (state) {
        case kReady:
            return "kReady";
        case kOffLine:
            return "kOffLine";
        case kOnKick:
            return "kOnKick";
        case kWaitKick:
            return "kWaitKick";
        default:
            return "";
    }
}

} // namespace master
} // namespace tera
