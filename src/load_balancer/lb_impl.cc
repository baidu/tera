// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "load_balancer/lb_impl.h"

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "load_balancer/meta_balancer.h"
#include "load_balancer/unity_balancer.h"
#include "proto/tabletnode.pb.h"
#include "tera.h"
#include "common/timer.h"

DECLARE_bool(tera_lb_meta_isolate_enabled);
DECLARE_string(tera_lb_meta_table_name);
DECLARE_int32(tera_lb_impl_thread_num);
DECLARE_bool(tera_lb_by_table);
DECLARE_int32(tera_lb_meta_balance_period_s);
DECLARE_int32(tera_lb_load_balance_period_s);
DECLARE_int32(tera_lb_max_compute_steps);
DECLARE_int32(tera_lb_max_compute_steps_per_tablet);
DECLARE_int32(tera_lb_max_compute_time_ms);
DECLARE_double(tera_lb_min_cost_need_balance);
DECLARE_double(tera_lb_bad_node_safemode_percent);
DECLARE_double(tera_lb_move_count_cost_weight);
DECLARE_int32(tera_lb_meta_balance_max_move_num);
DECLARE_int32(tera_lb_tablet_max_move_num);
DECLARE_int32(tera_lb_tablet_move_too_frequently_threshold_s);
DECLARE_double(tera_lb_abnormal_node_ratio);
DECLARE_double(tera_lb_tablet_count_cost_weight);
DECLARE_double(tera_lb_size_cost_weight);
DECLARE_double(tera_lb_flash_size_cost_weight);
DECLARE_double(tera_lb_read_load_cost_weight);
DECLARE_double(tera_lb_write_load_cost_weight);
DECLARE_double(tera_lb_scan_load_cost_weight);
DECLARE_double(tera_lb_lread_cost_weight);
DECLARE_double(tera_lb_heavy_read_pending_threshold);
DECLARE_double(tera_lb_heavy_write_pending_threshold);
DECLARE_double(tera_lb_heavy_scan_pending_threshold);
DECLARE_double(tera_lb_heavy_lread_threshold);
DECLARE_double(tera_lb_read_pending_factor);
DECLARE_double(tera_lb_write_pending_factor);
DECLARE_double(tera_lb_scan_pending_factor);
DECLARE_bool(tera_lb_debug_mode_enabled);

using tera::master::NodeState;
using tera::master::Table;
using tera::master::TablePtr;
using tera::master::Tablet;
using tera::master::TabletPtr;
using tera::master::TabletNode;
using tera::master::TabletNodePtr;

namespace tera {
namespace load_balancer {

LBImpl::LBImpl()
    : thread_pool_(new ThreadPool(FLAGS_tera_lb_impl_thread_num)),
      sdk_client_(nullptr),
      safemode_(false),
      lb_debug_mode_(FLAGS_tera_lb_debug_mode_enabled) {}

LBImpl::~LBImpl() {}

bool LBImpl::Init() {
  if (lb_debug_mode_) {
    LOG(INFO) << "[lb] debug mode enabled";
  }

  // tera_entry has init glog
  Client::SetGlogIsInitialized();
  uint32_t log_v = FLAGS_v;

  sdk_client_.reset(Client::NewClient());
  if (!sdk_client_) {
    LOG(ERROR) << "[lb] init sdk client fail";
    return false;
  }

  // avoid sdk change log level of load balancer
  FLAGS_v = log_v;

  InitOptions();

  if (FLAGS_tera_lb_meta_isolate_enabled) {
    ScheduleMetaBalance();
  }

  ScheduleUnityBalance();

  return true;
}

void LBImpl::InitOptions() {
  MutexLock lock(&mutex_);
  lb_options_.max_compute_steps = FLAGS_tera_lb_max_compute_steps;
  lb_options_.max_compute_steps_per_tablet = FLAGS_tera_lb_max_compute_steps_per_tablet;
  lb_options_.max_compute_time_ms = FLAGS_tera_lb_max_compute_time_ms;
  lb_options_.min_cost_need_balance = FLAGS_tera_lb_min_cost_need_balance;
  lb_options_.bad_node_safemode_percent = FLAGS_tera_lb_bad_node_safemode_percent;
  lb_options_.move_count_cost_weight = FLAGS_tera_lb_move_count_cost_weight;
  lb_options_.meta_balance_max_move_num = FLAGS_tera_lb_meta_balance_max_move_num;
  lb_options_.tablet_max_move_num = FLAGS_tera_lb_tablet_max_move_num;
  lb_options_.tablet_move_too_frequently_threshold_s =
      FLAGS_tera_lb_tablet_move_too_frequently_threshold_s;
  lb_options_.abnormal_node_ratio = FLAGS_tera_lb_abnormal_node_ratio;
  lb_options_.tablet_count_cost_weight = FLAGS_tera_lb_tablet_count_cost_weight;
  lb_options_.size_cost_weight = FLAGS_tera_lb_size_cost_weight;
  lb_options_.flash_size_cost_weight = FLAGS_tera_lb_flash_size_cost_weight;
  lb_options_.read_load_cost_weight = FLAGS_tera_lb_read_load_cost_weight;
  lb_options_.write_load_cost_weight = FLAGS_tera_lb_write_load_cost_weight;
  lb_options_.scan_load_cost_weight = FLAGS_tera_lb_scan_load_cost_weight;
  lb_options_.lread_cost_weight = FLAGS_tera_lb_lread_cost_weight;
  lb_options_.heavy_read_pending_threshold = FLAGS_tera_lb_heavy_read_pending_threshold;
  lb_options_.heavy_write_pending_threshold = FLAGS_tera_lb_heavy_write_pending_threshold;
  lb_options_.heavy_scan_pending_threshold = FLAGS_tera_lb_heavy_scan_pending_threshold;
  lb_options_.heavy_lread_threshold = FLAGS_tera_lb_heavy_lread_threshold;
  lb_options_.read_pending_factor = FLAGS_tera_lb_read_pending_factor;
  lb_options_.write_pending_factor = FLAGS_tera_lb_write_pending_factor;
  lb_options_.scan_pending_factor = FLAGS_tera_lb_scan_pending_factor;
  lb_options_.meta_table_isolate_enabled = FLAGS_tera_lb_meta_isolate_enabled;
  lb_options_.meta_table_name = FLAGS_tera_lb_meta_table_name;
  lb_options_.meta_table_node_addr = meta_node_addr_;
  lb_options_.debug_mode_enabled = lb_debug_mode_;
}

void LBImpl::ScheduleMetaBalance() {
  int schedule_period_ms = FLAGS_tera_lb_meta_balance_period_s * 1000;
  VLOG(5) << "[lb] MetaBalance will be scheduled in: " << schedule_period_ms / 1000 << "s";
  thread_pool_->DelayTask(schedule_period_ms, [this](int64_t) {
    DoMetaBalance();
    ScheduleMetaBalance();
  });
}

void LBImpl::ScheduleUnityBalance() {
  int schedule_period = FLAGS_tera_lb_load_balance_period_s * 1000;
  VLOG(5) << "[lb] UnityBalance will be scheduled in: " << FLAGS_tera_lb_load_balance_period_s
          << "s";
  thread_pool_->DelayTask(schedule_period, [this](int64_t) {
    DoUnityBalance();
    ScheduleUnityBalance();
  });
}

void LBImpl::DoMetaBalance() {
  static uint64_t round = 0;
  VLOG(5) << "[lb] MetaBalance begin round: " << ++round;
  int64_t start_time = get_micros();

  std::vector<TabletNodePtr> tablet_nodes;
  std::vector<TablePtr> tables;
  std::vector<TabletPtr> tablets;
  if (!Collect(&tablet_nodes, &tables, &tablets)) {
    return;
  }

  std::vector<std::shared_ptr<LBTabletNode>> lb_nodes;
  CreateLBInput(tables, tablet_nodes, tablets, &lb_nodes);

  {
    MutexLock lock(&mutex_);
    lb_options_.meta_table_node_addr = meta_node_addr_;
  }

  std::shared_ptr<Balancer> balancer = std::make_shared<MetaBalancer>(lb_options_);
  std::vector<Plan> plans;
  if (!balancer->BalanceCluster(lb_nodes, &plans)) {
    LOG(WARNING) << "[lb] DoBalance failed";
    return;
  }

  ExecutePlan(plans);

  int64_t cost_time = get_micros() - start_time;
  VLOG(5) << "[lb] MetaBalance end round: " << round << ", cost: " << cost_time / 1000.0 << "ms";
}

void LBImpl::DoUnityBalance() {
  static uint64_t round = 0;
  VLOG(5) << "[lb] UnityBalance begin round: " << ++round;
  int64_t start_time = get_micros();

  std::vector<TabletNodePtr> tablet_nodes;
  std::vector<TablePtr> tables;
  std::vector<TabletPtr> tablets;
  if (!Collect(&tablet_nodes, &tables, &tablets)) {
    return;
  }

  {
    MutexLock lock(&mutex_);
    lb_options_.meta_table_node_addr = meta_node_addr_;
  }

  std::shared_ptr<Balancer> balancer = std::make_shared<UnityBalancer>(lb_options_);
  std::vector<Plan> plans;

  if (FLAGS_tera_lb_by_table) {
    std::map<std::string, std::vector<std::shared_ptr<LBTabletNode>>> nodes_by_table;
    CreateLBInputByTable(tables, tablet_nodes, tablets, &nodes_by_table);
    if (!BlanceClusterByTable(balancer, nodes_by_table, &plans)) {
      return;
    }
  } else {
    std::vector<std::shared_ptr<LBTabletNode>> lb_nodes;
    CreateLBInput(tables, tablet_nodes, tablets, &lb_nodes);

    if (!balancer->BalanceCluster(lb_nodes, &plans)) {
      LOG(WARNING) << "[lb] DoBalance failed";
      return;
    }
  }

  ExecutePlan(plans);

  int64_t cost_time = get_micros() - start_time;
  VLOG(5) << "[lb] UnityBalance end round: " << round << ", cost: " << cost_time / 1000.0 << "ms";
}

bool LBImpl::BlanceClusterByTable(
    const std::shared_ptr<Balancer>& balancer,
    std::map<std::string, std::vector<std::shared_ptr<LBTabletNode>>>& nodes_by_table,
    std::vector<Plan>* plans) {
  std::vector<std::string> tables;
  for (const auto& pair : nodes_by_table) {
    tables.emplace_back(pair.first);
  }
  std::random_shuffle(tables.begin(), tables.end());

  for (const auto& table : tables) {
    if (table == FLAGS_tera_lb_meta_table_name) {
      continue;
    }
    if (!balancer->BalanceCluster(table, nodes_by_table[table], plans)) {
      LOG(WARNING) << "[lb] balance table " << table << " failed";
      return false;
    }
  }

  return true;
}

bool LBImpl::CreateLBInput(const std::vector<TablePtr>& tables,
                           const std::vector<TabletNodePtr>& nodes,
                           const std::vector<TabletPtr>& tablets,
                           std::vector<std::shared_ptr<LBTabletNode>>* lb_nodes) {
  lb_nodes->clear();

  std::map<std::string, std::shared_ptr<LBTabletNode>> nodes_map;
  for (const auto& node : nodes) {
    LBTabletNode* p_lb_node = new LBTabletNode();
    p_lb_node->tablet_node_ptr = node;
    nodes_map[node->GetAddr()].reset(p_lb_node);
  }

  for (const auto& tablet : tablets) {
    std::string addr = tablet->GetServerAddr();
    if (nodes_map.find(addr) != nodes_map.end()) {
      LBTablet* p_lb_tablet = new LBTablet();
      p_lb_tablet->tablet_ptr = tablet;
      std::shared_ptr<LBTablet> lb_tablet(p_lb_tablet);
      nodes_map[addr]->tablets.emplace_back(lb_tablet);
    } else {
      // TODO
      // unassigned tablet, skip now
    }
  }

  lb_nodes->reserve(nodes_map.size());
  for (const auto& pair : nodes_map) {
    lb_nodes->emplace_back(pair.second);
  }

  if (lb_debug_mode_) {
    DebugLBNode(*lb_nodes);
  }

  return true;
}

bool LBImpl::CreateLBInputByTable(
    const std::vector<TablePtr>& tables, const std::vector<TabletNodePtr>& nodes,
    const std::vector<TabletPtr>& tablets,
    std::map<std::string, std::vector<std::shared_ptr<LBTabletNode>>>* nodes_by_table) {
  nodes_by_table->clear();

  std::map<std::string, std::map<std::string, std::shared_ptr<LBTabletNode>>> nodes_by_table_intr;
  for (const auto& table : tables) {
    std::string table_name = table->GetTableName();
    for (const auto& node : nodes) {
      std::string addr = node->GetAddr();
      LBTabletNode* p_lb_node = new LBTabletNode();
      p_lb_node->tablet_node_ptr = node;
      nodes_by_table_intr[table_name][addr].reset(p_lb_node);
    }
  }

  for (const auto& tablet : tablets) {
    std::string table_name = tablet->GetTableName();
    std::string addr = tablet->GetServerAddr();
    if (nodes_by_table_intr.find(table_name) == nodes_by_table_intr.end()) {
      LOG(WARNING) << "table " << table_name << " of tablet " << tablet->GetPath()
                   << " does not exist";
      continue;  // skip tablet which has no table
    }
    if (nodes_by_table_intr[table_name].find(addr) == nodes_by_table_intr[table_name].end()) {
      LOG(WARNING) << "server " << addr << " of tablet " << tablet->GetPath() << " does not exist";
      continue;  // skip tablet which has no server
    }

    LBTablet* p_lb_tablet = new LBTablet();
    p_lb_tablet->tablet_ptr = tablet;
    nodes_by_table_intr[table_name][addr]->tablets.emplace_back(p_lb_tablet);
  }

  for (const auto& table : nodes_by_table_intr) {
    std::string table_name = table.first;
    for (const auto& node : table.second) {
      (*nodes_by_table)[table_name].emplace_back(node.second);
    }
  }

  if (lb_debug_mode_) {
    DebugLBNodeByTable(*nodes_by_table);
  }

  return true;
}

bool LBImpl::Collect(std::vector<TabletNodePtr>* nodes, std::vector<TablePtr>* tables,
                     std::vector<TabletPtr>* tablets) {
  if (nodes == nullptr || tables == nullptr || tablets == nullptr) {
    return false;
  }
  nodes->clear();
  tables->clear();
  tablets->clear();

  int64_t start_time = get_micros();

  if (!CollectNodes(nodes)) {
    LOG(ERROR) << "[lb] collect nodes fail";
    return false;
  }

  if (!CollectTablets(tables, tablets)) {
    LOG(ERROR) << "[lb] collect tablets fail";
    return false;
  }

  int64_t cost_time = get_micros() - start_time;
  VLOG(5) << "[lb] Collect cost: " << cost_time / 1000.0 << "ms";

  if (lb_debug_mode_) {
    DebugCollect(*nodes, *tables, *tablets);
  }

  return true;
}

bool LBImpl::CollectNodes(std::vector<TabletNodePtr>* nodes) {
  std::shared_ptr<tera::ClientImpl> client_impl(
      (static_cast<ClientWrapper*>(sdk_client_.get()))->GetClientImpl());
  std::vector<TabletNodeInfo> infos;
  ErrorCode err;
  if (!client_impl->ShowTabletNodesInfo(&infos, &err)) {
    LOG(ERROR) << "[lb] fail to get TabletNodeInfo, err: " << err.ToString();
    return false;
  }

  for (const auto& info : infos) {
    TabletNodePtr node(new TabletNode());
    NodeInfoToNode(info, node);
    nodes->push_back(node);
  }

  VLOG(5) << "[lb] collected node size: " << nodes->size();

  return true;
}

bool LBImpl::NodeInfoToNode(const TabletNodeInfo& info, TabletNodePtr node) {
  node->info_ = info;

  node->addr_ = info.addr();
  node->state_ = StringToNodeState(info.status_m());
  node->data_size_ = info.load();
  node->persistent_cache_size_ = info.persistent_cache_size();
  node->average_counter_.read_pending_ = info.read_pending();
  node->average_counter_.write_pending_ = info.write_pending();
  node->average_counter_.scan_pending_ = info.scan_pending();

  return true;
}

NodeState LBImpl::StringToNodeState(const std::string& str) {
  if (str == "kReady") {
    return tera::master::kReady;
  } else if (str == "kOffline" || str == "kOffLine") {
    return tera::master::kOffline;
  } else if (str == "kOnKick") {
    return tera::master::kOnKick;
  } else if (str == "kWaitKick") {
    return tera::master::kWaitKick;
  } else if (str == "kKicked") {
    return tera::master::kKicked;
  } else {
    return tera::master::kOffline;
  }
}

bool LBImpl::CollectTablets(std::vector<TablePtr>* tables, std::vector<TabletPtr>* tablets) {
  std::shared_ptr<tera::ClientImpl> client_impl(
      (static_cast<ClientWrapper*>(sdk_client_.get()))->GetClientImpl());
  TableMetaList table_list;
  TabletMetaList tablet_list;
  bool is_brief = false;
  ErrorCode err;
  if (!client_impl->ShowTablesInfo(&table_list, &tablet_list, is_brief, &err)) {
    LOG(ERROR) << "[lb] fail to get tablets, err: " << err.ToString();
    return false;
  }

  std::map<std::string, TablePtr> table_name_to_ptr;

  for (int i = 0; i < table_list.meta_size(); ++i) {
    const TableMeta& meta = table_list.meta(i);
    if (meta.status() != kTableEnable) {
      VLOG(10) << "[lb] skip table:" << meta.table_name()
               << ", status:" << StatusCodeToString(meta.status());
      continue;
    }
    const std::string& table_name = meta.table_name();
    TablePtr table(new tera::master::Table(table_name, meta.schema(), meta.status()));
    tables->push_back(table);

    if (table_name_to_ptr.find(table_name) == table_name_to_ptr.end()) {
      table_name_to_ptr[table_name] = table;
    }
  }

  if (tablet_list.meta_size() != tablet_list.counter_size()) {
    LOG(ERROR) << "[lb] invalid TabletMetaList, meta size: " << tablet_list.meta_size()
               << " counter size: " << tablet_list.counter_size();
    return false;
  }
  for (int i = 0; i < tablet_list.meta_size(); ++i) {
    std::string table_name = tablet_list.meta(i).table_name();
    if (table_name_to_ptr.find(table_name) == table_name_to_ptr.end()) {
      LOG(WARNING) << "[lb] tablet's table not exist "
                   << "tablet path: " << tablet_list.meta(i).path() << "table: " << table_name;
      continue;
    }
    TabletPtr tablet(new tera::master::Tablet(tablet_list.meta(i), table_name_to_ptr[table_name]));
    tablet->SetCounter(tablet_list.counter(i));
    if (tablet_list.meta(i).has_last_move_time_us()) {
      tablet->SetLastMoveTime(tablet_list.meta(i).last_move_time_us());
    } else {
      // !!! compatible with old master
      // !!! set last move time to 0 will disable the MoveFrequencyCostFunction
      // strategy
      tablet->SetLastMoveTime(0);
    }
    if (tablet_list.meta(i).has_data_size_on_flash()) {
      tablet->SetDataSizeOnFlash(tablet_list.meta(i).data_size_on_flash());
    } else {
      tablet->SetDataSizeOnFlash(0);
    }
    tablets->push_back(tablet);

    if (table_name == FLAGS_tera_lb_meta_table_name) {
      SetMetaNodeAddr(tablet->GetServerAddr());
      VLOG(5) << "[lb] meta table node addr: " << GetMetaNodeAddr();
    }
  }

  VLOG(5) << "[lb] collected table size: " << tables->size();
  VLOG(5) << "[lb] collected tablet size: " << tablets->size();

  return true;
}

void LBImpl::DebugCollect(const std::vector<TabletNodePtr>& nodes,
                          const std::vector<TablePtr>& tables,
                          const std::vector<TabletPtr>& tablets) {
  LOG(INFO) << "";
  LOG(INFO) << "[lb] DebugCollect begin -----";

  LOG(INFO) << "[lb] " << tables.size() << " table:";
  for (const auto& table : tables) {
    LOG(INFO) << "table:" + table->GetTableName()
              << " status:" << StatusCodeToString(table->GetStatus());
  }

  LOG(INFO) << "[lb] " << nodes.size() << " node:";
  for (const auto& node : nodes) {
    LOG(INFO) << "addr:" + node->GetAddr()
              << " state:" << tera::master::NodeStateToString(node->GetState())
              << " size:" << node->GetSize() << "B"
              << " persistent_cache_size:" << node->GetPersistentCacheSize() << "B"
              << " r_pending:" << node->GetReadPending() << " w_pending:" << node->GetWritePending()
              << " s_pending:" << node->GetScanPending()
              << " lread:" << node->info_.low_read_cell();
  }

  LOG(INFO) << "[lb] " << tablets.size() << " tablet:";
  for (const auto& tablet : tablets) {
    LOG(INFO) << "path:" << tablet->GetPath()
              << " status:" << StatusCodeToString(tablet->GetStatus())
              << " server:" << tablet->GetServerAddr() << " table:" << tablet->GetTableName()
              << " size:" << tablet->GetDataSize() << " flash size:" << tablet->GetDataSizeOnFlash()
              << " lread:" << tablet->GetLRead() << " last_move_time_us:" << tablet->LastMoveTime();
  }

  LOG(INFO) << "[lb] DebugCollect end -----";
  LOG(INFO) << "";
}

void LBImpl::DebugLBNode(const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes) {
  LOG(INFO) << "";
  LOG(INFO) << "[lb] DebugLBNode begin -----";
  LOG(INFO) << "[lb] " << lb_nodes.size() << " lb_nodes:";

  for (const auto& node : lb_nodes) {
    LOG(INFO) << "[lb] " << node->tablet_node_ptr->GetAddr() << ":";
    for (const auto& lb_tablet : node->tablets) {
      LOG(INFO) << "[lb] " << lb_tablet->tablet_ptr->GetPath();
    }
  }

  LOG(INFO) << "[lb] DebugLBNode end -----";
  LOG(INFO) << "";
}

void LBImpl::DebugLBNodeByTable(
    const std::map<std::string, std::vector<std::shared_ptr<LBTabletNode>>>& nodes_by_table) {
  LOG(INFO) << "";
  LOG(INFO) << "[lb] DebugLBNodeByTable begin -----";

  for (const auto& table : nodes_by_table) {
    LOG(INFO) << "[lb] table " << table.first;
    for (const auto& lb_node : table.second) {
      LOG(INFO) << "[lb] node " << lb_node->tablet_node_ptr->GetAddr();
      for (const auto& tablet : lb_node->tablets) {
        LOG(INFO) << "[lb] tablet " << tablet->tablet_ptr->GetPath();
      }
    }
  }

  LOG(INFO) << "[lb] DebugLBNodeByTable end -----";
  LOG(INFO) << "";
}

void LBImpl::DebugPlan(const std::vector<Plan>& plans) {
  VLOG(5) << "";
  VLOG(5) << "[lb] DebugPlan begin ----";
  VLOG(5) << plans.size() << " plans:";

  for (const auto& plan : plans) {
    VLOG(5) << "[lb] " + plan.ToString();
  }

  VLOG(5) << "[lb] DebugPlan end ----";
  VLOG(5) << "";
}

void LBImpl::ExecutePlan(const std::vector<Plan>& plans) {
  if (lb_debug_mode_) {
    DebugPlan(plans);
  }

  if (IsSafemode()) {
    VLOG(5) << "[lb] skip execute plan in safe mode";
    return;
  }

  bool master_safe_mode = true;
  bool get_success = GetMasterSafemode(&master_safe_mode);
  if (!get_success) {
    VLOG(5) << "[lb] skip execute plan due to fail to get master safe mode";
    return;
  } else {
    if (master_safe_mode) {
      VLOG(5) << "[lb] skip execute plan due to master is in safe mode";
      return;
    } else {
      // will execute plan
    }
  }

  std::shared_ptr<tera::ClientImpl> client_impl(
      (static_cast<ClientWrapper*>(sdk_client_.get()))->GetClientImpl());
  for (const auto& plan : plans) {
    std::string tablet_path = plan.TabletPath();
    std::string dest_addr = plan.DestAddr();

    std::vector<std::string> arg_list;
    arg_list.emplace_back("move");
    arg_list.emplace_back(tablet_path);
    arg_list.emplace_back(dest_addr);

    ErrorCode err;
    if (!client_impl->CmdCtrl("tablet", arg_list, nullptr, nullptr, &err)) {
      LOG(ERROR) << "[lb] fail to execute plan:" << plan.ToString() << err.ToString();
    } else {
      VLOG(5) << "[lb] execute plan success:" << plan.ToString();
    }
  }
}

bool LBImpl::IsSafemode() const {
  MutexLock lock(&mutex_);
  return safemode_;
}

bool LBImpl::SetSafemode(bool value) {
  MutexLock lock(&mutex_);
  safemode_ = value;

  if (value) {
    LOG(INFO) << "[lb] LoadBanlacer enter safemode";
  } else {
    LOG(INFO) << "[lb] LoadBanlacer leave safemode";
  }

  return true;
}

bool LBImpl::GetMasterSafemode(bool* safe_mode) {
  if (safe_mode == nullptr) {
    return false;
  }

  std::string op = "get";
  std::vector<std::string> arg_list;
  arg_list.push_back(op);

  std::shared_ptr<tera::ClientImpl> client_impl(
      (static_cast<ClientWrapper*>(sdk_client_.get()))->GetClientImpl());
  ErrorCode err;
  if (!client_impl->CmdCtrl("safemode", arg_list, safe_mode, NULL, &err)) {
    LOG(ERROR) << "[lb] fail to " << op << " master safemode" << err.ToString();
    return false;
  }

  VLOG(20) << "[lb] master safemode: " << *safe_mode;
  return true;
}

std::string LBImpl::GetMetaNodeAddr() const {
  MutexLock lock(&mutex_);
  return meta_node_addr_;
}

bool LBImpl::SetMetaNodeAddr(const std::string& addr) {
  MutexLock lock(&mutex_);
  meta_node_addr_ = addr;
  return true;
}

void LBImpl::CmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response,
                     google::protobuf::Closure* done) {
  std::string cmd_line;
  for (int32_t i = 0; i < request->arg_list_size(); i++) {
    cmd_line += request->arg_list(i);
    if (i != request->arg_list_size() - 1) {
      cmd_line += " ";
    }
  }
  LOG(INFO) << "[lb] receive cmd: " << request->command() << " " << cmd_line;

  response->set_sequence_id(request->sequence_id());

  if (request->command() == "safemode") {
    SafeModeCmdCtrl(request, response);
  } else {
    response->set_status(kInvalidArgument);
  }

  done->Run();
  return;
}

void LBImpl::SafeModeCmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response) {
  if (request->arg_list_size() != 1) {
    response->set_status(kInvalidArgument);
    return;
  }

  if (request->arg_list(0) == "enter") {
    SetSafemode(true);
    response->set_status(kLoadBalancerOk);
  } else if (request->arg_list(0) == "leave") {
    SetSafemode(false);
    response->set_status(kLoadBalancerOk);
  } else if (request->arg_list(0) == "get") {
    response->set_bool_result(IsSafemode());
    response->set_status(kLoadBalancerOk);
  } else {
    response->set_status(kInvalidArgument);
  }
}

}  // namespace load_balancer
}  // namespace tera
