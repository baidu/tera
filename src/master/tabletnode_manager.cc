// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/tabletnode_manager.h"

#include "master/master_impl.h"
#include "master/workload_scheduler.h"
#include "common/timer.h"

DECLARE_string(tera_master_meta_table_name);
DECLARE_int32(tera_master_max_load_concurrency);
DECLARE_int32(tera_master_max_split_concurrency);
DECLARE_int32(tera_master_load_interval);
DECLARE_bool(tera_master_meta_isolate_enabled);
DECLARE_int32(tera_master_tabletnode_timeout);
DECLARE_int32(tera_master_max_unload_concurrency);
DECLARE_bool(tera_master_support_isomerism);

namespace tera {
namespace master {

static TabletNode::TSStateTransitionRulesType s_ts_state_transition_rules;

const TabletNode::TSStateTransitionRulesType TabletNode::state_transitions_(std::move(
    s_ts_state_transition_rules.AddTransitionRule(kOffline, NodeEvent::kZkNodeCreated, kReady)
        .AddTransitionRule(kReady, NodeEvent::kZkSessionTimeout, kOffline)
        .AddTransitionRule(kReady, NodeEvent::kPrepareKickTs, kWaitKick)
        .AddTransitionRule(kWaitKick, NodeEvent::kCancelKickTs, kReady)
        .AddTransitionRule(kWaitKick, NodeEvent::kZkKickNodeCreated, kKicked)
        .AddTransitionRule(kWaitKick, NodeEvent::kZkSessionTimeout, kOffline)
        .AddTransitionRule(kKicked, NodeEvent::kZkSessionTimeout, kOffline)));

void BindTabletToTabletNode(TabletPtr tablet, TabletNodePtr node) {
  tablet->AssignTabletNode(node);
  node->UpdateSize(tablet);
}

TabletNode::TabletNode()
    : state_(kOffline),
      report_status_(kTabletNodeIsRunning),
      data_size_(0),
      qps_(0),
      load_(0),
      persistent_cache_size_(0),
      update_time_(0),
      query_fail_count_(0),
      onload_count_(0),
      unloading_count_(0),
      onsplit_count_(0),
      plan_move_in_count_(0) {
  info_.set_addr("");
  info_.set_status_m(NodeStateToString(state_));
  info_.set_timestamp(get_micros());
  timestamp_ = get_millis();
  // ref_count_.Inc();
}

TabletNode::TabletNode(const std::string& addr, const std::string& uuid)
    : addr_(addr),
      uuid_(uuid),
      state_(kOffline),
      report_status_(kTabletNodeIsRunning),
      data_size_(0),
      qps_(0),
      load_(0),
      persistent_cache_size_(0),
      update_time_(0),
      query_fail_count_(0),
      onload_count_(0),
      unloading_count_(0),
      onsplit_count_(0),
      plan_move_in_count_(0) {
  info_.set_addr(addr);
  info_.set_status_m(NodeStateToString(state_));
  info_.set_timestamp(get_micros());
  timestamp_ = get_millis();
}

TabletNode::TabletNode(const TabletNode& t) {
  MutexLock lock(&t.mutex_);
  addr_ = t.addr_;
  // state_ = kOffLine;
  uuid_ = t.uuid_;
  state_ = t.state_;
  timestamp_ = t.timestamp_;
  report_status_ = t.report_status_;
  info_ = t.info_;
  data_size_ = t.data_size_;
  qps_ = t.qps_;
  load_ = t.load_;
  persistent_cache_size_ = t.persistent_cache_size_;
  update_time_ = t.update_time_;
  table_size_ = t.table_size_;
  table_qps_ = t.table_qps_;
  average_counter_ = t.average_counter_;
  accumulate_counter_ = t.accumulate_counter_;
  counter_list_ = t.counter_list_;
  query_fail_count_ = t.query_fail_count_;
  onload_count_ = t.onload_count_;
  unloading_count_ = t.unloading_count_;
  onsplit_count_ = t.onsplit_count_;
  plan_move_in_count_ = t.plan_move_in_count_;
  recent_load_time_list_ = t.recent_load_time_list_;
}

TabletNode::~TabletNode() {}

TabletNodeInfo TabletNode::GetInfo() {
  MutexLock lock(&mutex_);
  return info_;
}

const std::string& TabletNode::GetAddr() { return addr_; }

const std::string& TabletNode::GetId() { return uuid_; }

uint64_t TabletNode::GetSize(const std::string& table_name) {
  MutexLock lock(&mutex_);
  if (table_name.empty()) {
    return data_size_;
  }
  uint64_t table_size = 0;
  std::map<std::string, uint64_t>::iterator it = table_size_.find(table_name);
  if (it != table_size_.end()) {
    table_size = it->second;
  }
  return table_size;
}

uint64_t TabletNode::GetQps(const std::string& table_name) {
  MutexLock lock(&mutex_);
  if (table_name.empty()) {
    return qps_;
  }
  uint64_t table_qps = 0;
  std::map<std::string, uint64_t>::iterator it = table_qps_.find(table_name);
  if (it != table_qps_.end()) {
    table_qps = it->second;
  }
  return table_qps;
}

uint64_t TabletNode::GetReadPending() {
  MutexLock lock(&mutex_);
  return average_counter_.read_pending_;
}

uint64_t TabletNode::GetWritePending() {
  MutexLock lock(&mutex_);
  return average_counter_.write_pending_;
}

uint64_t TabletNode::GetScanPending() {
  MutexLock lock(&mutex_);
  return average_counter_.scan_pending_;
}

uint64_t TabletNode::GetRowReadDelay() {
  MutexLock lock(&mutex_);
  return average_counter_.row_read_delay_;
}

uint64_t TabletNode::GetPersistentCacheSize() {
  MutexLock lock(&mutex_);
  return persistent_cache_size_;
}

uint32_t TabletNode::GetPlanToMoveInCount() {
  MutexLock lock(&mutex_);
  VLOG(16) << "GetPlanToMoveInCount: " << addr_ << " " << plan_move_in_count_;
  return plan_move_in_count_;
}

void TabletNode::PlanToMoveIn() {
  MutexLock lock(&mutex_);
  plan_move_in_count_++;
  VLOG(16) << "PlanToMoveIn: " << addr_ << " " << plan_move_in_count_;
}

void TabletNode::DoneMoveIn() {
  MutexLock lock(&mutex_);
  // TODO (likang): If node restart just before a tablet move in,
  // this count will be reset to 0. So we have to make sure it is greater
  // than 0 before dec.
  if (plan_move_in_count_ > 0) {
    plan_move_in_count_--;
  }
  VLOG(16) << "DoneMoveIn: " << addr_ << " " << plan_move_in_count_;
}

bool TabletNode::MayLoadNow() {
  MutexLock lock(&mutex_);
  if (recent_load_time_list_.size() <
      static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency)) {
    return true;
  }
  if (recent_load_time_list_.front() + FLAGS_tera_master_load_interval * 1000000 <= get_micros()) {
    return true;
  }
  VLOG(16) << "MayLoadNow() " << addr_
           << " last load time: " << (get_micros() - recent_load_time_list_.front()) / 1000000
           << " seconds ago";
  return false;
}

void TabletNode::UpdateSize(TabletPtr tablet) {
  MutexLock lock(&mutex_);
  data_size_ += tablet->GetDataSize();
  if (table_size_.find(tablet->GetTableName()) != table_size_.end()) {
    table_size_[tablet->GetTableName()] += tablet->GetDataSize();
  } else {
    table_size_[tablet->GetTableName()] = tablet->GetDataSize();
  }
  qps_ += tablet->GetQps();
  if (table_qps_.find(tablet->GetTableName()) != table_qps_.end()) {
    table_qps_[tablet->GetTableName()] += tablet->GetQps();
  } else {
    table_qps_[tablet->GetTableName()] = tablet->GetQps();
  }
}

bool TabletNode::TryLoad(TabletPtr tablet) {
  MutexLock lock(&mutex_);
  if (onload_count_ < static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency)) {
    BeginLoad();
    return true;
  }
  return false;
}

void TabletNode::BeginLoad() {
  ++onload_count_;
  recent_load_time_list_.push_back(get_micros());
  uint32_t list_size = recent_load_time_list_.size();
  if (list_size > static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency)) {
    CHECK_EQ(list_size - 1, static_cast<uint32_t>(FLAGS_tera_master_max_load_concurrency));
    recent_load_time_list_.pop_front();
  }
}

bool TabletNode::FinishLoad(TabletPtr tablet) {
  MutexLock lock(&mutex_);
  // assert(onload_count_ > 0);
  if (onload_count_ > 0) {
    --onload_count_;
  }
  return true;
}

bool TabletNode::TrySplit(TabletPtr tablet, const std::string& split_key) {
  MutexLock lock(&mutex_);
  // data_size_ should be modified by LoadTabletProcedure UnloadTabletProcedure
  // and QueryCallback
  // should not be modified by TrySplit
  if (onsplit_count_ < static_cast<uint32_t>(FLAGS_tera_master_max_split_concurrency)) {
    ++onsplit_count_;
    data_size_ -= tablet->GetDataSize();
    return true;
  }
  return false;
}

bool TabletNode::FinishSplit() {
  MutexLock lock(&mutex_);
  --onsplit_count_;
  return true;
}

bool TabletNode::CanUnload() {
  MutexLock lock(&mutex_);
  if (unloading_count_ < static_cast<uint32_t>(FLAGS_tera_master_max_unload_concurrency)) {
    ++unloading_count_;
    return true;
  }
  return false;
}

void TabletNode::FinishUnload() {
  MutexLock lock(&mutex_);
  --unloading_count_;
}

NodeState TabletNode::GetState() {
  MutexLock lock(&mutex_);
  if (state_ == kOffline && get_millis() - timestamp_ < FLAGS_tera_master_tabletnode_timeout) {
    return kPendingOffline;
  }
  return state_;
}

bool TabletNode::DoStateTransition(NodeEvent event) {
  MutexLock lock(&mutex_);
  NodeState post_state;
  if (!state_transitions_.DoStateTransition(state_, event, &post_state)) {
    LOG(WARNING) << "node: " << addr_ << ", uuid: " << uuid_
                 << ", illegal transition state: " << StatusCodeToString((StatusCode)state_)
                 << ", event: " << event;
    return false;
  }
  LOG(INFO) << "node: " << addr_ << ", uuid: " << uuid_ << ", state switch: "
            << ", event: " << event << StatusCodeToString((StatusCode)state_)
            << ", post state: " << StatusCodeToString((StatusCode)post_state);
  state_ = post_state;
  return true;
}

uint32_t TabletNode::GetQueryFailCount() {
  MutexLock lock(&mutex_);
  return query_fail_count_;
}

uint32_t TabletNode::IncQueryFailCount() {
  MutexLock lock(&mutex_);
  return ++query_fail_count_;
}

void TabletNode::ResetQueryFailCount() {
  MutexLock lock(&mutex_);
  query_fail_count_ = 0;
}

TabletNodeManager::TabletNodeManager(MasterImpl* master_impl)
    : tabletnode_added_(&mutex_), master_impl_(master_impl) {}

TabletNodeManager::~TabletNodeManager() { MutexLock lock(&mutex_); }

TabletNodePtr TabletNodeManager::AddTabletNode(const std::string& addr, const std::string& uuid) {
  MutexLock lock(&mutex_);
  TabletNodePtr null_ptr;
  std::pair<TabletNodeList::iterator, bool> ret =
      tabletnode_list_.insert(std::pair<std::string, TabletNodePtr>(addr, null_ptr));
  TabletNodePtr& state = ret.first->second;
  // already has one TS at the same IP:PORT addr, return the existing
  // TabletNodePtr
  if (!ret.second) {
    TabletNodePtr existing_node = ret.first->second;
    LOG(ERROR) << "tabletnode [" << addr << " exist, existing uuid: " << existing_node->uuid_
               << ", to be added uuid: " << uuid;
    return existing_node;
  } else {
    LOG(INFO) << "add tabletnode : " << addr << ", id : " << uuid;
    state.reset(new TabletNode(addr, uuid));
  }
  // kReady represent heartbeat status
  // state->SetState(kReady, NULL);
  state->DoStateTransition(NodeEvent::kZkNodeCreated);
  tabletnode_added_.Broadcast();
  return state;
}

TabletNodePtr TabletNodeManager::DelTabletNode(const std::string& addr) {
  TabletNodePtr state(nullptr);

  MutexLock lock(&mutex_);
  TabletNodeList::iterator it = tabletnode_list_.find(addr);
  if (it == tabletnode_list_.end()) {
    LOG(ERROR) << "tabletnode [" << addr << "] does not exist";
    return state;
  }
  state = it->second;
  state->DoStateTransition(NodeEvent::kZkSessionTimeout);
  // state->SetState(kOffLine, NULL);
  tabletnode_list_.erase(it);

  // delete node may block, so we'd better release the mutex before that
  LOG(INFO) << "delete tabletnode: " << addr << ", uuid: " << state->uuid_;
  return state;
}

void TabletNodeManager::UpdateTabletNode(const std::string& addr, const TabletNode& state) {
  MutexLock lock(&mutex_);
  TabletNodeList::iterator it = tabletnode_list_.find(addr);
  if (it == tabletnode_list_.end()) {
    LOG(ERROR) << "tabletnode [" << addr << "] does not exist";
    return;
  }
  TabletNode* node = it->second.get();
  MutexLock node_lock(&node->mutex_);
  node->report_status_ = state.report_status_;
  node->data_size_ = state.data_size_;
  node->qps_ = state.qps_;
  node->info_ = state.info_;
  node->info_.set_addr(addr);
  node->load_ = state.load_;
  node->persistent_cache_size_ = state.persistent_cache_size_;
  node->update_time_ = state.update_time_;
  node->table_size_ = state.table_size_;
  node->table_qps_ = state.table_qps_;

  node->info_.set_status_m(NodeStateToString(node->state_));
  node->info_.set_tablet_onload(node->onload_count_);
  node->info_.set_tablet_onsplit(node->onsplit_count_);
  node->info_.set_tablet_unloading(node->unloading_count_);

  node->average_counter_.read_pending_ =
      CounterWeightedSum(state.info_.read_pending(), node->average_counter_.read_pending_);
  node->average_counter_.write_pending_ =
      CounterWeightedSum(state.info_.write_pending(), node->average_counter_.write_pending_);
  node->average_counter_.scan_pending_ =
      CounterWeightedSum(state.info_.scan_pending(), node->average_counter_.scan_pending_);
  node->average_counter_.row_read_delay_ =
      CounterWeightedSum(state.info_.extra_info_size() > 1 ? state.info_.extra_info(1).value() : 0,
                         node->average_counter_.row_read_delay_);
  VLOG(15) << "update tabletnode : " << addr;
}

void TabletNodeManager::GetAllTabletNodeAddr(std::vector<std::string>* addr_array) {
  MutexLock lock(&mutex_);
  TabletNodeList::iterator it = tabletnode_list_.begin();
  for (; it != tabletnode_list_.end(); ++it) {
    addr_array->push_back(it->first);
  }
}

void TabletNodeManager::GetAllTabletNodeId(std::map<std::string, std::string>* id_map) {
  MutexLock lock(&mutex_);
  TabletNodeList::iterator it = tabletnode_list_.begin();
  for (; it != tabletnode_list_.end(); ++it) {
    TabletNodePtr node = it->second;
    MutexLock lock2(&node->mutex_);
    (*id_map)[it->first] = node->uuid_;
  }
}

void TabletNodeManager::GetAllTabletNodeInfo(std::vector<TabletNodePtr>* array) {
  MutexLock lock(&mutex_);
  TabletNodeList::iterator it = tabletnode_list_.begin();
  for (; it != tabletnode_list_.end(); ++it) {
    array->push_back(it->second);
  }
}

TabletNodePtr TabletNodeManager::FindTabletNode(const std::string& addr, TabletNodePtr* state) {
  TabletNodePtr node;
  MutexLock lock(&mutex_);
  TabletNodeList::iterator it = tabletnode_list_.find(addr);
  if (it == tabletnode_list_.end()) {
    // LOG(WARNING) << "tabletnode [" << addr << "] does not exist";
    return node;
  }
  node = it->second;
  if (NULL != state) {
    *state = it->second;
  }
  return node;
}

bool TabletNodeManager::ScheduleTabletNodeOrWait(Scheduler* scheduler,
                                                 const std::string& table_name,
                                                 const TabletPtr& tablet, bool is_move,
                                                 TabletNodePtr* node) {
  return ScheduleTabletNode(scheduler, table_name, tablet, is_move, true, node);
}

bool TabletNodeManager::ScheduleTabletNode(Scheduler* scheduler, const std::string& table_name,
                                           const TabletPtr& tablet, bool is_move,
                                           TabletNodePtr* node) {
  return ScheduleTabletNode(scheduler, table_name, tablet, is_move, false, node);
}

void TabletNodeManager::WaitTabletNodeReconnect(const std::string& addr, const std::string& uuid,
                                                int64_t reconn_timeout_taskid) {
  MutexLock lock(&mutex_);
  CHECK(reconnecting_ts_list_.find(addr) == reconnecting_ts_list_.end());

  reconnecting_ts_list_[addr] = reconn_timeout_taskid;
  LOG(INFO) << "tabletnode addr: " << addr << " wait reconnect taskid : " << reconn_timeout_taskid;
}

int64_t TabletNodeManager::PopTabletNodeReconnectTaskID(const std::string& addr) {
  MutexLock lock(&mutex_);
  if (reconnecting_ts_list_.find(addr) == reconnecting_ts_list_.end()) {
    return 0;
  }
  int64_t task_id = reconnecting_ts_list_[addr];
  reconnecting_ts_list_.erase(addr);
  return task_id;
}

bool TabletNodeManager::IsFlashSizeEnough(const TabletPtr& tablet, const TabletNodePtr& node) {
  if (!FLAGS_tera_master_support_isomerism) {
    return true;
  }

  if (tablet->GetTableName() == FLAGS_tera_master_meta_table_name &&
      node->GetPersistentCacheSize() == 0) {
    return false;
  }

  if (tablet->HasFlashLg() && node->GetPersistentCacheSize() == 0) {
    return false;
  }

  return true;
}

bool TabletNodeManager::ScheduleTabletNode(Scheduler* scheduler, const std::string& table_name,
                                           const TabletPtr& tablet, bool is_move, bool wait,
                                           TabletNodePtr* node) {
  MutexLock lock(&mutex_);
  std::string meta_node_addr;
  master_impl_->GetMetaTabletAddr(&meta_node_addr);

  TabletNodePtr null_ptr, meta_node;
  std::vector<TabletNodePtr> candidates;
  std::vector<TabletNodePtr> slow_candidates;
  while (tabletnode_list_.empty()) {
    if (!wait) {
      LOG(WARNING) << "currently no available tabletnode";
      return false;
    }
    // If tabletnode_list is empty, we should hang and wait
    // TabletNodeManager::AddTabletNode wake us
    LOG(WARNING) << "currently no available tabletnode, ScheduleTabletNode suspended";
    tabletnode_added_.Wait();
  }

  TabletNodeList::const_iterator it = tabletnode_list_.begin();
  for (; it != tabletnode_list_.end(); ++it) {
    TabletNodePtr tablet_node = it->second;
    if (tablet_node->GetState() != kReady) {
      continue;
    }
    if (FLAGS_tera_master_meta_isolate_enabled && tablet_node->addr_ == meta_node_addr) {
      meta_node = tablet_node;
      continue;
    }

    if (tablet && !IsFlashSizeEnough(tablet, tablet_node)) {
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
    if (tablet_node->average_counter_.read_pending_ < 100) {
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
  VLOG(16) << "ShouldMoveData()";
  MutexLock lock(&mutex_);
  if (tablet_candidates.size() == 0) {
    return false;
  }
  if (src_node == dst_node) {
    return false;
  }
  if (dst_node->GetState() != kReady) {
    return false;
  }
  if (dst_node->average_counter_.read_pending_ > 100) {
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
    master_impl_->GetMetaTabletAddr(&meta_node_addr);
    if (dst_node->GetAddr() == meta_node_addr) {
      return false;
    }
    if (src_node->GetAddr() == meta_node_addr) {
      *tablet_index = 0;
      return true;
    }
  }
  return scheduler->FindBestTablet(src_node, dst_node, tablet_candidates, table_name, tablet_index);
}

std::string NodeStateToString(NodeState state) {
  switch (state) {
    case kReady:
      return "kReady";
    case kOffline:
      return "kOffline";
    case kPendingOffline:
      return "kPendingOffline";
    case kOnKick:
      return "kOnKick";
    case kWaitKick:
      return "kWaitKick";
    case kKicked:
      return "kKicked";
    default:
      return "";
  }
}

std::ostream& operator<<(std::ostream& o, const NodeEvent event) {
  static const char* msg[] = {"NodeEvent::kZkNodeCreated",     "NodeEvent::kZkSessionTimeout",
                              "NodeEvent::kPrepareKickTs",     "NodeEvent::kCancelKickTs",
                              "NodeEvent::kZkKickNodeCreated", "NodeEvent::kUnknown"};
  static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
  typedef std::underlying_type<NodeEvent>::type UnderType;
  uint32_t index =
      static_cast<UnderType>(event) - static_cast<UnderType>(NodeEvent::kZkNodeCreated);
  index = index < msg_size ? index : msg_size - 1;
  o << msg[index];
  return o;
}

}  // namespace master
}  // namespace tera
