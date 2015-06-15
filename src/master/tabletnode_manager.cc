// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/tabletnode_manager.h"

#include "master/scheduler.h"
#include "master/master_impl.h"
#include "utils/timer.h"

DECLARE_string(tera_master_meta_table_name);
DECLARE_int32(tera_master_max_load_concurrency);
DECLARE_int32(tera_master_max_split_concurrency);
DECLARE_int32(tera_master_max_unload_concurrency);
DECLARE_int32(tera_master_load_interval);
DECLARE_double(tera_master_load_balance_size_overload_ratio);
DECLARE_bool(tera_master_meta_isolate_enabled);

namespace tera {
namespace master {

TabletNode::TabletNode() : m_state(kOffLine),
    m_report_status(kTabletNodeInit), m_data_size(0), m_load(0),
    m_update_time(0), m_query_fail_count(0), m_plan_move_in_count(0),
    m_load_spatula(FLAGS_tera_master_max_load_concurrency),
    m_unload_spatula(FLAGS_tera_master_max_unload_concurrency),
    m_split_spatula(FLAGS_tera_master_max_split_concurrency) {
    m_info.set_addr("");
}

TabletNode::TabletNode(const std::string& addr, const std::string& uuid)
    : m_addr(addr), m_uuid(uuid), m_state(kOffLine),
      m_report_status(kTabletNodeInit), m_data_size(0), m_load(0),
      m_update_time(0), m_query_fail_count(0), m_plan_move_in_count(0),
      m_load_spatula(FLAGS_tera_master_max_load_concurrency),
      m_unload_spatula(FLAGS_tera_master_max_unload_concurrency),
      m_split_spatula(FLAGS_tera_master_max_split_concurrency) {
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

uint64_t TabletNode::GetTableSize(const std::string& table_name) {
    MutexLock lock(&m_mutex);
    uint64_t table_size = 0;
    std::map<std::string, uint64_t>::iterator it = m_table_size.find(table_name);
    if (it != m_table_size.end()) {
        table_size = it->second;
    }
    return table_size;
}

uint64_t TabletNode::GetSize() {
    MutexLock lock(&m_mutex);
    return m_data_size;
}

uint32_t TabletNode::GetPlanToMoveInCount() {
    MutexLock lock(&m_mutex);
    VLOG(7) << "GetPlanToMoveInCount: " << m_addr << " " << m_plan_move_in_count;
    return m_plan_move_in_count;
}

void TabletNode::PlanToMoveIn() {
    MutexLock lock(&m_mutex);
    m_plan_move_in_count++;
    VLOG(7) << "PlanToMoveIn: " << m_addr << " " << m_plan_move_in_count;
}

void TabletNode::DoneMoveIn() {
    MutexLock lock(&m_mutex);
    // TODO (likang): If node restart just before a tablet move in,
    // this count will be reset to 0. So we have to make sure it is greater
    // than 0 before dec.
    if (m_plan_move_in_count > 0) {
        m_plan_move_in_count--;
    }
    VLOG(7) << "DoneMoveIn: " << m_addr << " " << m_plan_move_in_count;
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
    return false;
}

void TabletNode::DeleteTablet(const TabletPtr tablet) {
    MutexLock lock(&m_mutex);
    if (m_table_size.find(tablet->GetTableName()) != m_table_size.end()) {
        m_data_size -= tablet->GetDataSize();
        m_table_size[tablet->GetTableName()] -= tablet->GetDataSize();
    } else {
        LOG(ERROR) << "invalid tablet: " << tablet;
    }
}

void TabletNode::AddTablet(const TabletPtr tablet) {
    MutexLock lock(&m_mutex);
    m_data_size += tablet->GetDataSize();
    if (m_table_size.find(tablet->GetTableName()) != m_table_size.end()) {
        m_table_size[tablet->GetTableName()] += tablet->GetDataSize();
    } else {
        m_table_size[tablet->GetTableName()] = tablet->GetDataSize();
    }

    m_recent_load_time_list.push_back(get_micros());
    uint32_t list_size = m_recent_load_time_list.size();
    if (list_size > static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency)) {
        CHECK_EQ(list_size - 1, static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency));
        m_recent_load_time_list.pop_front();
    }
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
    node->m_info = state.m_info;
    node->m_info.set_addr(addr);
    node->m_load = state.m_load;
    node->m_update_time = state.m_update_time;
    node->m_table_size = state.m_table_size;

    node->m_info.set_status_m(NodeStateToString(node->m_state));
    node->m_info.set_tablet_onload(node->m_load_spatula.GetRunningCount());
    node->m_info.set_tablet_unloading(node->m_unload_spatula.GetRunningCount());
    node->m_info.set_tablet_onsplit(node->m_split_spatula.GetRunningCount());
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

bool TabletNodeManager::ScheduleTabletNode(Scheduler* scheduler,
                                           std::string* node_addr) {
    return ScheduleTabletNode(scheduler, "", node_addr);
}

bool TabletNodeManager::ScheduleTabletNode(Scheduler* scheduler,
                                           const std::string& table_name,
                                           std::string* node_addr) {
    MutexLock lock(&m_mutex);
    std::string meta_node_addr;
    m_master_impl->GetMetaTabletAddr(&meta_node_addr);

    TabletNodePtr null_ptr, meta_node;
    std::vector<TabletNodePtr> candidates;

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
        candidates.push_back(tablet_node);
    }
    if (candidates.size() == 0) {
        if (meta_node != null_ptr) {
            node_addr->assign(meta_node->m_addr);
            return true;
        } else {
            return false;
        }
    }

    return scheduler->FindBestNode(candidates, table_name, node_addr);
}

bool TabletNodeManager::IsNodeOverloadThanAverage(const std::string& node_addr) {
    bool found = false;
    uint64_t total_data_size = 0, server_data_size = 0;

    MutexLock lock(&m_mutex);
    TabletNodeList::iterator it = m_tabletnode_list.begin();
    for (; it != m_tabletnode_list.end(); ++it) {
        TabletNodePtr node = it->second;
        MutexLock lock2(&node->m_mutex);
        total_data_size += node->m_data_size;
        if (node_addr == node->m_addr) {
            server_data_size = node->m_data_size;
            found = true;
        }
    }
    if (!found) {
        return false;
    }
    total_data_size -= server_data_size;
    double average_data_size = (double)total_data_size / m_tabletnode_list.size();
    if ((double)server_data_size
        > (double)average_data_size * FLAGS_tera_master_load_balance_size_overload_ratio) {
        return true;
    }
    return false;
}

bool TabletNodeManager::IsNodeOverloadThanLeast(const std::string& node_addr) {
    std::string meta_node_addr;
    m_master_impl->GetMetaTabletAddr(&meta_node_addr);

    MutexLock lock(&m_mutex);
    if (FLAGS_tera_master_meta_isolate_enabled && node_addr == meta_node_addr) {
        // to isolate meta, we assume meta node is overload
        return m_tabletnode_list.size() > 1;
    }

    bool found = false;
    uint64_t least_data_size = (uint64_t)-1, server_data_size = 0;

    TabletNodeList::iterator it = m_tabletnode_list.begin();
    for (; it != m_tabletnode_list.end(); ++it) {
        TabletNodePtr node = it->second;
        MutexLock lock2(&node->m_mutex);
        if ((!FLAGS_tera_master_meta_isolate_enabled
            || node->m_addr != meta_node_addr)
            && least_data_size >= node->m_data_size) {
            least_data_size = node->m_data_size;
        }
        if (node_addr == node->m_addr) {
            server_data_size = node->m_data_size;
            found = true;
        }
    }
    if (!found) {
        return false;
    }
    if ((double)server_data_size
        > (double)least_data_size * FLAGS_tera_master_load_balance_size_overload_ratio) {
        return true;
    }
    return false;
}

bool TabletNodeManager::ShouldMoveToLeastNode(const std::string& node_addr,
                                              uint64_t move_data_size) {
    std::string meta_node_addr;
    m_master_impl->GetMetaTabletAddr(&meta_node_addr);

    MutexLock lock(&m_mutex);
    /*
    if (FLAGS_tera_master_meta_isolate_enabled && node_addr == meta_node_addr) {
        return m_tabletnode_list.size() > 1;
    }
    */

    uint64_t least_data_size = (uint64_t)-1;
    TabletNodePtr this_node, least_node;

    TabletNodeList::iterator it = m_tabletnode_list.begin();
    for (; it != m_tabletnode_list.end(); ++it) {
        TabletNodePtr node = it->second;
        MutexLock lock2(&node->m_mutex);
        if ((!FLAGS_tera_master_meta_isolate_enabled
            || node->m_addr != meta_node_addr)
            && least_data_size >= node->m_data_size) {
            least_data_size = node->m_data_size;
            least_node = node;
        }
        if (node_addr == node->m_addr) {
            this_node = node;
        }
    }

    TabletNodePtr null_ptr;
    if (this_node == null_ptr) {
        return false;
    }
    if (least_node == null_ptr) {
        // there is only one node
        return false;
    }
    return ShouldMoveData(this_node, least_node, "", move_data_size);
}

bool TabletNodeManager::ShouldMoveData(const std::string& src_node_addr,
                                       const std::string& dst_node_addr,
                                       const std::string& table_name,
                                       uint64_t move_data_size) {
    MutexLock lock(&m_mutex);
    TabletNodeList::iterator it = m_tabletnode_list.find(src_node_addr);
    if (it == m_tabletnode_list.end()) {
        return false;
    }
    TabletNodePtr src_node = it->second;
    it = m_tabletnode_list.find(dst_node_addr);
    if (it == m_tabletnode_list.end()) {
        return false;
    }
    TabletNodePtr dst_node = it->second;
    return ShouldMoveData(src_node, dst_node, table_name, move_data_size);
}

bool TabletNodeManager::ShouldMoveData(TabletNodePtr src_node,
                                       TabletNodePtr dst_node,
                                       const std::string& table_name,
                                       uint64_t move_data_size) {
    if (src_node == dst_node) {
        return false;
    }
    if (dst_node->GetState() != kReady) {
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
            return true;
        }
    }

    uint64_t src_node_data_size = 0, dst_node_data_size = 0;
    MutexLock lock(&src_node->m_mutex);
    if (table_name.empty()) {
        src_node_data_size = src_node->m_data_size;
    } else {
        std::map<std::string, uint64_t>::iterator it =
            src_node->m_table_size.find(table_name);
        if (it != src_node->m_table_size.end()) {
            src_node_data_size = it->second;
        }
    }
    MutexLock lock2(&dst_node->m_mutex);
    if (table_name.empty()) {
        dst_node_data_size = dst_node->m_data_size;
    } else {
        std::map<std::string, uint64_t>::iterator it =
            dst_node->m_table_size.find(table_name);
        if (it != dst_node->m_table_size.end()) {
            dst_node_data_size = it->second;
        }
    }

    const double& ratio = FLAGS_tera_master_load_balance_size_overload_ratio;
    // avoid move back and forth repeatedly
    if (src_node_data_size < move_data_size
        || (double)src_node_data_size <= (double)dst_node_data_size * ratio
        || dst_node_data_size + move_data_size > src_node_data_size - move_data_size) {
        return false;
    }
    return true;
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
