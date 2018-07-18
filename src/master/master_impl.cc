// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/master_impl.h"
#include "tabletnode/tablet_manager.h"

#include <algorithm>
#include <fstream>
#include <functional>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gperftools/malloc_extension.h>

#include "db/filename.h"
#include "io/io_utils.h"
#include "io/utils_leveldb.h"
#include "leveldb/status.h"
#include "master/master_zk_adapter.h"
#include "master/workload_scheduler.h"
#include "proto/kv_helper.h"
#include "proto/master_client.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode_client.h"
#include "sdk/stat_table.h"
#include "utils/config_utils.h"
#include "utils/schema_utils.h"
#include "utils/string_util.h"
#include "common/timer.h"
#include "utils/utils_cmd.h"
#include "master_env.h"
#include "master/create_table_procedure.h"
#include "master/delete_table_procedure.h"
#include "master/disable_table_procedure.h"
#include "master/enable_table_procedure.h"
#include "master/load_tablet_procedure.h"
#include "master/move_tablet_procedure.h"
#include "master/merge_tablet_procedure.h"
#include "master/split_tablet_procedure.h"
#include "master/unload_tablet_procedure.h"
#include "master/update_table_procedure.h"

DECLARE_string(tera_master_port);
DECLARE_bool(tera_master_meta_recovery_enabled);
DECLARE_string(tera_master_meta_recovery_file);

DECLARE_bool(tera_master_cache_check_enabled);
DECLARE_int32(tera_master_cache_release_period);

DECLARE_int32(tera_master_impl_thread_max_num);
DECLARE_int32(tera_master_impl_query_thread_num);
DECLARE_int32(tera_master_impl_retry_times);

DECLARE_int32(tera_master_query_tabletnode_period);

DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_master_meta_table_path);
DECLARE_int32(tera_master_meta_retry_times);

DECLARE_string(tera_coord_type);
DECLARE_bool(tera_zk_enabled);
DECLARE_bool(tera_mock_zk_enabled);

DECLARE_double(tera_master_workload_split_threshold);
DECLARE_double(tera_master_workload_merge_threshold);
DECLARE_int64(tera_master_split_tablet_size);
DECLARE_int64(tera_master_min_split_size);
DECLARE_double(tera_master_min_split_ratio);
DECLARE_int64(tera_master_merge_tablet_size);
DECLARE_bool(tera_master_kick_tabletnode_enabled);
DECLARE_int32(tera_master_kick_tabletnode_query_fail_times);

DECLARE_double(tera_safemode_tablet_locality_ratio);
DECLARE_int32(tera_master_collect_info_timeout);
DECLARE_int32(tera_master_collect_info_retry_period);
DECLARE_int32(tera_master_collect_info_retry_times);
DECLARE_int32(tera_master_control_tabletnode_retry_period);
DECLARE_int32(tera_master_load_balance_period);
DECLARE_bool(tera_master_load_balance_table_grained);
DECLARE_int32(tera_master_load_rpc_timeout);
DECLARE_int32(tera_master_unload_rpc_timeout);
DECLARE_int32(tera_master_tabletnode_timeout);
DECLARE_bool(tera_master_move_tablet_enabled);
DECLARE_int32(tera_master_max_move_concurrency);

DECLARE_int32(tera_max_pre_assign_tablet_num);
DECLARE_int64(tera_tablet_write_block_size);

DECLARE_int32(tera_master_gc_period);
DECLARE_bool(tera_master_gc_trash_enabled);
DECLARE_int64(tera_master_gc_trash_clean_period_s);

DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_string(tera_leveldb_env_type);

DECLARE_string(tera_zk_root_path);
DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_local_addr);
DECLARE_bool(tera_ins_enabled);
DECLARE_bool(tera_mock_ins_enabled);

DECLARE_int64(tera_sdk_perf_counter_log_interval);

DECLARE_bool(tera_acl_enabled);
DECLARE_bool(tera_only_root_create_table);
DECLARE_string(tera_master_gc_strategy);

DECLARE_string(flagfile);
DECLARE_bool(tera_online_schema_update_enabled);
DECLARE_int32(tera_master_schema_update_retry_period);
DECLARE_int32(tera_master_schema_update_retry_times);

DECLARE_int64(tera_master_availability_check_period);
DECLARE_bool(tera_master_availability_check_enabled);

DECLARE_bool(tera_stat_table_enabled);

using namespace std::placeholders;

namespace tera {
namespace master {

MasterImpl::MasterImpl()
    : status_(kNotInited),
      thread_pool_(new ThreadPool(FLAGS_tera_master_impl_thread_max_num)),
      restored_(false),
      tablet_manager_(new TabletManager(&this_sequence_id_, this, thread_pool_.get())),
      tabletnode_manager_(new TabletNodeManager(this)),
      user_manager_(new UserManager),
      zk_adapter_(NULL),
      size_scheduler_(new SizeScheduler),
      load_scheduler_(new LoadScheduler),
      release_cache_timer_id_(kInvalidTimerId),
      query_enabled_(false),
      query_thread_pool_(new ThreadPool(FLAGS_tera_master_impl_query_thread_num)),
      start_query_time_(0),
      query_tabletnode_timer_id_(kInvalidTimerId),
      load_balance_scheduled_(false),
      load_balance_enabled_(false),
      gc_trash_clean_enabled_(false),
      gc_trash_clean_timer_id_(kInvalidTimerId),
      gc_enabled_(false),
      gc_timer_id_(kInvalidTimerId),
      gc_query_enable_(false),
      executor_(new ProcedureExecutor),
      tablet_availability_(new TabletAvailability(tablet_manager_)) {
    if (FLAGS_tera_master_cache_check_enabled) {
        EnableReleaseCacheTimer();
    }
    if (FLAGS_tera_local_addr == "") {
        local_addr_ = utils::GetLocalHostName()+ ":" + FLAGS_tera_master_port;
    } else {
        local_addr_ = FLAGS_tera_local_addr + ":" + FLAGS_tera_master_port;
    }

    if (FLAGS_tera_master_gc_strategy == "default") {
        LOG(INFO) << "[gc] gc strategy is BatchGcStrategy";
        gc_strategy_ = std::shared_ptr<GcStrategy>(new BatchGcStrategy(tablet_manager_));
    } else if (FLAGS_tera_master_gc_strategy == "trackable") {
        LOG(INFO) << "[gc] gc strategy is Trackable";
    } else {
        LOG(ERROR) << "Unknown gc strategy: " << FLAGS_tera_master_gc_strategy
            << ", exit";
        exit(EXIT_FAILURE);
    }
    executor_->Start();
    if (FLAGS_tera_stat_table_enabled) {
        stat_table_.reset(new sdk::StatTable(thread_pool_.get(),
                                             sdk::StatTableCustomer::kMaster,
                                             local_addr_));
    }
}

MasterImpl::~MasterImpl() {
    LOG(INFO) << "begin destory impl";
    executor_->Stop();
    executor_.reset();
    stat_table_.reset();
    zk_adapter_.reset();
    LOG(INFO) << "end destory impl";
}

bool MasterImpl::Init() {
    if (FLAGS_tera_leveldb_env_type != "local") {
        io::InitDfsEnv();
    }
    if (FLAGS_tera_coord_type.empty()) {
        LOG(ERROR) << "Note: We don't recommend that use '"
                   << "--tera_[zk|ins|mock_zk|mock_ins]_enabled' flag for your cluster coord"
                   << " replace by '--tera_coord_type=[zk|ins|mock_zk|mock_ins|fake_zk]'"
                   << " flag is usually recommended.";
    }
    if (FLAGS_tera_coord_type == "zk"
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_zk_enabled)) {
        zk_adapter_.reset(new MasterZkAdapter(this, local_addr_));
    } else if (FLAGS_tera_coord_type == "ins"
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_ins_enabled)) {
        LOG(INFO) << "ins mode" ;
        zk_adapter_.reset(new InsMasterZkAdapter(this, local_addr_));
    } else if (FLAGS_tera_coord_type == "mock_zk"
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_mock_zk_enabled)) {
        LOG(INFO) << "mock zk mode" ;
        zk_adapter_.reset(new MockMasterZkAdapter(this, local_addr_));
    } else if (FLAGS_tera_coord_type == "mock_ins"
            || (FLAGS_tera_coord_type.empty() && FLAGS_tera_mock_ins_enabled)) {
        LOG(INFO) << "mock ins mode" ;
        zk_adapter_.reset(new MockInsMasterZkAdapter(this, local_addr_));
    } else if (FLAGS_tera_coord_type == "fake_zk"
            || FLAGS_tera_coord_type.empty()) {
        LOG(INFO) << "fake zk mode!";
        zk_adapter_.reset(new FakeMasterZkAdapter(this, local_addr_));
    }

    MasterEnv().Init(this,
            tabletnode_manager_,
            tablet_manager_,
            size_scheduler_,
            load_scheduler_,
            thread_pool_,
            executor_,
            tablet_availability_,
            stat_table_);

    LOG(INFO) << "[acl] " << (FLAGS_tera_acl_enabled ? "enabled" : "disabled");
    SetMasterStatus(kIsSecondary);
    thread_pool_->AddTask(std::bind(&MasterImpl::InitAsync, this));
    return true;
}

void MasterImpl::InitAsync() {
    std::string meta_tablet_addr;
    std::map<std::string, std::string> tabletnode_list;
    bool safe_mode = false;

    // Make sure tabletnode_list will not change
    // during restore process.
    MutexLock lock(&tabletnode_mutex_);

    while (!zk_adapter_->Init(&meta_tablet_addr, &tabletnode_list,
                               &safe_mode)) {
        LOG(ERROR) << kSms << "zookeeper error, please check!";
    }

    Restore(tabletnode_list);
}

bool MasterImpl::Restore(const std::map<std::string, std::string>& tabletnode_list) {
    tabletnode_mutex_.AssertHeld();
    CHECK(!restored_);

    if (tabletnode_list.size() == 0) {
        SetMasterStatus(kOnWait);
        LOG(ERROR) << kSms << "no available tabletnode";
        return false;
    }

    SetMasterStatus(kOnRestore);

    std::vector<TabletMeta> tablet_list;
    CollectAllTabletInfo(tabletnode_list, &tablet_list);

    if (!RestoreMetaTablet(tablet_list)) {
        SetMasterStatus(kOnWait);
        return false;
    }

    SetMasterStatus(kIsReadonly);

    user_manager_->SetupRootUser();

    RestoreUserTablet(tablet_list);

    TryLeaveSafeMode();
    EnableAvailabilityCheck();
    RefreshTableCounter();

    // restore success
    restored_ = true;
    return true;
}

void MasterImpl::CollectAllTabletInfo(const std::map<std::string, std::string>& tabletnode_list,
                                      std::vector<TabletMeta>* tablet_list) {
    Mutex mutex;
    sem_t finish_counter;
    sem_init(&finish_counter, 0, 0);
    tablet_list->clear();
    uint32_t tabletnode_num = tabletnode_list.size();
    std::map<std::string, std::string>::const_iterator it = tabletnode_list.begin();
    for (; it != tabletnode_list.end(); ++it) {
        const std::string& addr = it->first;
        const std::string& uuid = it->second;
        tabletnode_manager_->AddTabletNode(addr, uuid);

        QueryClosure done =
            std::bind(&MasterImpl::CollectTabletInfoCallback, this, addr,
                       tablet_list, &finish_counter, &mutex, _1, _2, _3, _4);
        QueryTabletNodeAsync(addr, FLAGS_tera_master_collect_info_timeout, false, done);
    }

    uint32_t i = 0;
    while (i++ < tabletnode_num) {
        sem_wait(&finish_counter);
    }
    sem_destroy(&finish_counter);
}

bool MasterImpl::RestoreMetaTablet(const std::vector<TabletMeta>& tablet_list) {
                                   //std::string* meta_tablet_addr) {
    // find the unique loaded complete meta tablet
    // if meta_tablet is loaded by more than one tabletnode, unload them all
    // if meta_tablet is incomplete (not from "" to ""), unload it
    bool loaded_twice = false;
    bool loaded = false;
    TabletMeta meta_tablet_meta;
    std::vector<TabletMeta>::const_iterator it = tablet_list.begin();
    for (; it != tablet_list.end(); ++it) {
        StatusCode status = kTabletNodeOk;
        const TabletMeta& meta = *it;
        if (meta.table_name() == FLAGS_tera_master_meta_table_name) {
            const std::string& key_start = meta.key_range().key_start();
            const std::string& key_end = meta.key_range().key_end();
            if (loaded_twice) {
                if (!UnloadTabletSync(FLAGS_tera_master_meta_table_name,
                                      key_start, key_end, meta.server_addr(),
                                      &status)) {
                    TryKickTabletNode(meta.server_addr());
                }
            } else if (!key_start.empty() || !key_end.empty()) {
                // unload incomplete meta tablet
                if (!UnloadTabletSync(FLAGS_tera_master_meta_table_name,
                                      key_start, key_end, meta.server_addr(),
                                      &status)) {
                    TryKickTabletNode(meta.server_addr());
                }
            } else if (loaded) {
                // more than one meta tablets are loaded
                loaded_twice = true;
                if (!UnloadTabletSync(FLAGS_tera_master_meta_table_name,
                                      key_start, key_end, meta.server_addr(),
                                      &status)) {
                    TryKickTabletNode(meta.server_addr());
                }
                if (!UnloadTabletSync(FLAGS_tera_master_meta_table_name, "", "",
                                      meta_tablet_meta.server_addr(), &status)) {
                    TryKickTabletNode(meta.server_addr());
                }
            } else {
                loaded = true;
                meta_tablet_meta.CopyFrom(meta);
            }
        }
    }
    std::string meta_tablet_addr;
    if (loaded && !loaded_twice) {
        meta_tablet_addr.assign(meta_tablet_meta.server_addr());
    } else if (!LoadMetaTablet(&meta_tablet_addr)) {
        return false;
    }
    // meta table has been loaded up by now
    if (FLAGS_tera_master_meta_recovery_enabled) {
        const std::string& filename = FLAGS_tera_master_meta_recovery_file;
        while (!LoadMetaTableFromFile(filename)) {
            LOG(ERROR) << kSms << "fail to recovery meta table from backup";
            ThisThread::Sleep(60 * 1000);
        }
        // load MetaTablet, clear all data in MetaTablet and dump current memory snapshot to MetaTable
        while (!tablet_manager_->ClearMetaTable(meta_tablet_addr)
               || !tablet_manager_->DumpMetaTable(meta_tablet_addr)) {
            TryKickTabletNode(meta_tablet_addr);
            if (!LoadMetaTablet(&meta_tablet_addr)) {
                return false;
            }
        }
        TabletNodePtr meta_node = tabletnode_manager_->FindTabletNode(meta_tablet_addr, NULL);
        meta_tablet_ = tablet_manager_->AddMetaTablet(meta_node, zk_adapter_);
        LOG(INFO) << "recovery meta table from backup file success";
        return true;
    }

    StatusCode status = kTabletNodeOk;
    while (!LoadMetaTable(meta_tablet_addr, &status)) {
        TryKickTabletNode(meta_tablet_addr);
        if (!LoadMetaTablet(&meta_tablet_addr)) {
            return false;
        }
    }
    return true;
}

void MasterImpl::RestoreUserTablet(const std::vector<TabletMeta>& report_meta_list) {
    std::vector<TabletMeta>::const_iterator meta_it = report_meta_list.begin();
    std::set<TablePtr> disabled_tables;
    for (; meta_it != report_meta_list.end(); ++meta_it) {
        const TabletMeta& meta = *meta_it;
        const std::string& table_name = meta.table_name();
        if (table_name == FLAGS_tera_master_meta_table_name) {
            continue;
        }
        const std::string& key_start = meta.key_range().key_start();
        const std::string& key_end = meta.key_range().key_end();
        const std::string& path = meta.path();
        const std::string& server_addr = meta.server_addr();
        TabletNodePtr node = tabletnode_manager_->FindTabletNode(meta.server_addr(), NULL);
        CompactStatus compact_status = meta.compact_status();
        TabletMeta::TabletStatus status = meta.status();

        TabletPtr tablet;
        if (!tablet_manager_->FindTablet(table_name, key_start, &tablet)
            || !tablet->Verify(table_name, key_start, key_end, path, server_addr)) {
            LOG(INFO) << "unload unexpected table: " << path << ", server: "
                << server_addr;
            TabletMeta unknown_meta = meta;
            unknown_meta.set_status(TabletMeta::kTabletUnloading);
            TabletPtr unknown_tablet(new Tablet(unknown_meta));
            unknown_tablet->AssignTabletNode(node);
            TryUnloadTablet(unknown_tablet);
        } else {
            tablet->AssignTabletNode(node);
            tablet->SetStatus(TabletMeta::kTabletReady);
            // tablets of a table may be partially disabled before master deaded, so we need try disable
            // the table once more on master restarted
            if (tablet->GetTable()->GetStatus() == kTableDisable) {
                disabled_tables.insert(tablet->GetTable());
                continue;
            }
            tablet->UpdateSize(meta);
            tablet->SetCompactStatus(compact_status);
            // if the actual status of a tablet reported by ts is unloading, try move it to make sure it be loaded finally
            if (status == TabletMeta::kTabletUnloading || status == TabletMeta::kTabletUnloading2) {
                TryMoveTablet(tablet);
                continue;
            }
        }
    }

    std::vector<TabletPtr> all_tablet_list;
    tablet_manager_->ShowTable(NULL, &all_tablet_list);
    std::vector<TabletPtr>::iterator it;
    for (it = all_tablet_list.begin(); it != all_tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->GetTableName() == FLAGS_tera_master_meta_table_name) {
            continue;
        }
        // there may exists in transition tablets here as we may have a MoveTabletProcedure for it
        // if its reported status is unloading
        if (tablet->InTransition()) {
            LOG(WARNING) << "give up restore in transition tablet, tablet: " << tablet;
            continue;
        }
        const std::string& server_addr = tablet->GetServerAddr();
        if (tablet->GetStatus() == TabletMeta::kTabletReady) {
            VLOG(8) << "READY Tablet, " << tablet;
            continue;
        }
        if (tablet->GetStatus() != TabletMeta::kTabletOffline) {
            LOG(ERROR) << kSms << "tablet " << tablet
                << ", unexpected status: " << StatusCodeToString(tablet->GetStatus());
            continue;
        }
        if (tablet->GetTable()->GetStatus() == kTableDisable) {
            disabled_tables.insert(tablet->GetTable());
            continue;
        }

        TabletNodePtr node;
        if (server_addr.empty()) {
            VLOG(8) << "OFFLINE Tablet with empty addr, " << tablet;
        } else if (!tabletnode_manager_->FindTabletNode(server_addr, &node)) {
            VLOG(8) << "OFFLINE Tablet of Dead TS, " << tablet;
        } else if (node->state_ == kReady) {
            VLOG(8) << "OFFLINE Tablet of Alive TS, " << tablet;
            TryLoadTablet(tablet, node);
        } else {
            // Ts not response, we count its tablets as Ready and wait for it to be kicked.
            tablet->SetStatus(TabletMeta::kTabletReady);
            VLOG(8) << "UNKNOWN Tablet of No-Response TS, " << tablet;
        }
    }
    for (auto& table : disabled_tables) {
        if (table->LockTransition()) {
            DisableAllTablets(table);
        }
    }
}

void MasterImpl::DisableAllTablets(TablePtr table) {
    std::vector<TabletPtr> tablet_meta_list;
    table->GetTablet(&tablet_meta_list);
    int in_transition_tablet_cnt = 0;
    for (uint32_t i = 0; i < tablet_meta_list.size(); ++i) {
        TabletPtr tablet = tablet_meta_list[i];
        if (tablet->GetStatus() == TabletMeta::kTabletDisable) {
            continue;
        }
        if (tablet->LockTransition()) {
            if (tablet->GetStatus() == TabletMeta::kTabletOffline) {
                tablet->DoStateTransition(TabletEvent::kTableDisable);
                tablet->UnlockTransition();
                continue;
            }
            tablet->UnlockTransition();
            if (TryUnloadTablet(tablet)) {
                in_transition_tablet_cnt++;
            }
        }
        else {
            in_transition_tablet_cnt++;
        }
    }
    VLOG(23) << "table: " << table->GetTableName() << ", in transition num: " << in_transition_tablet_cnt;
    if (in_transition_tablet_cnt == 0) {
        table->UnlockTransition();
        return;
    }
    ThreadPool::Task task = std::bind(&MasterImpl::DisableAllTablets, this, table);
    thread_pool_->DelayTask(500, task); // magic number 500ms
}

bool MasterImpl::LoadMetaTablet(std::string* server_addr) {
    TabletMeta meta;
    meta.set_table_name(FLAGS_tera_master_meta_table_name);
    meta.set_path(FLAGS_tera_master_meta_table_path);
    meta.mutable_key_range()->set_key_start("");
    meta.mutable_key_range()->set_key_end("");
    TableSchema schema;
    schema.set_name(FLAGS_tera_master_meta_table_name);
    schema.set_kv_only(true);
    LocalityGroupSchema* lg_schema = schema.add_locality_groups();
    lg_schema->set_compress_type(false);
    lg_schema->set_store_type(MemoryStore);

    TabletNodePtr node;
    while (tabletnode_manager_->ScheduleTabletNode(size_scheduler_.get(), "",
                                                    false, &node)) {
        *server_addr = node->GetAddr();
        meta.set_server_addr(*server_addr);
        StatusCode status = kTabletNodeOk;
        if (LoadTabletSync(meta, schema, &status)) {
            LOG(INFO) << "load meta tablet on node: " << *server_addr;
            return true;
        }
        LOG(ERROR) << "fail to load meta tablet on node: " << *server_addr
            << ", status: " << StatusCodeToString(status);
        TryKickTabletNode(*server_addr);
    }
    LOG(ERROR) << "no live node to load meta tablet";
    return false;
}

void MasterImpl::UnloadMetaTablet(const std::string& server_addr) {
    StatusCode status = kTabletNodeOk;
    if (!UnloadTabletSync(FLAGS_tera_master_meta_table_name, "", "", server_addr,
                          &status)) {
        LOG(ERROR) << "fail to unload meta tablet on node: " << server_addr;
        TryKickTabletNode(server_addr);
    }
}

bool MasterImpl::IsRootUser(const std::string& token) {
    return user_manager_->UserNameToToken("root") == token;
}

// user is admin or user is in admin_group
bool MasterImpl::CheckUserPermissionOnTable(const std::string& token, TablePtr table) {
   std::string group_name = table->GetSchema().admin_group();
   std::string user_name = user_manager_->TokenToUserName(token);
   return (user_manager_->IsUserInGroup(user_name, group_name)
           || (table->GetSchema().admin() == user_manager_->TokenToUserName(token)));
}

bool MasterImpl::LoadMetaTable(const std::string& meta_tablet_addr,
                               StatusCode* ret_status) {
    tablet_manager_->ClearTableList();
    ScanTabletRequest request;
    ScanTabletResponse response;
    request.set_sequence_id(this_sequence_id_.Inc());
    request.set_table_name(FLAGS_tera_master_meta_table_name);
    request.set_start("");
    request.set_end("");
    tabletnode::TabletNodeClient meta_node_client(thread_pool_.get(), meta_tablet_addr);
    while (meta_node_client.ScanTablet(&request, &response)) {
        if (response.status() != kTabletNodeOk) {
            SetStatusCode(response.status(), ret_status);
            LOG(ERROR) << "fail to load meta table: "
                << StatusCodeToString(response.status());
            tablet_manager_->ClearTableList();
            return false;
        }
        if (response.results().key_values_size() <= 0) {
            LOG(INFO) << "load meta table success";
            TabletNodePtr meta_node = tabletnode_manager_->FindTabletNode(meta_tablet_addr, NULL);
            meta_tablet_ = tablet_manager_->AddMetaTablet(meta_node, zk_adapter_);
            return true;
        }
        uint32_t record_size = response.results().key_values_size();
        LOG(INFO) << "load meta table: " << record_size << " records";

        std::string last_record_key;
        for (uint32_t i = 0; i < record_size; i++) {
            const KeyValuePair& record = response.results().key_values(i);
            last_record_key = record.key();
            char first_key_char = record.key()[0];
            if (first_key_char == '~') {
                user_manager_->LoadUserMeta(record.key(), record.value());
            } else if (first_key_char == '@') {
                tablet_manager_->LoadTableMeta(record.key(), record.value());
            } else if (first_key_char > '@') {
                tablet_manager_->LoadTabletMeta(record.key(), record.value());
            } else {
                continue;
            }
        }
        std::string next_record_key = NextKey(last_record_key);
        request.set_start(next_record_key);
        request.set_end("");
        request.set_sequence_id(this_sequence_id_.Inc());
        response.Clear();
    }
    SetStatusCode(kRPCError, ret_status);
    LOG(ERROR) << "fail to load meta table: " << StatusCodeToString(kRPCError);
    tablet_manager_->ClearTableList();
    return false;
}

bool MasterImpl::LoadMetaTableFromFile(const std::string& filename,
                                          StatusCode* ret_status) {
    tablet_manager_->ClearTableList();
    std::ifstream ifs(filename.c_str(), std::ofstream::binary);
    if (!ifs.is_open()) {
        LOG(ERROR) << "fail to open file " << filename << " for read";
        SetStatusCode(kIOError, ret_status);
        return false;
    }

    uint64_t count = 0;
    std::string key, value;
    while (ReadFromStream(ifs, &key, &value)) {
        if (key.empty()) {
            return true;
        }

        char first_key_char = key[0];
        if (first_key_char == '~') {
            user_manager_->LoadUserMeta(key, value);
        } else if (first_key_char == '@') {
            tablet_manager_->LoadTableMeta(key, value);
        } else if (first_key_char > '@') {
            tablet_manager_->LoadTabletMeta(key, value);
        } else {
            continue;
        }

        count++;
    }
    tablet_manager_->ClearTableList();
    SetStatusCode(kIOError, ret_status);
    LOG(ERROR) << "fail to load meta table: " << StatusCodeToString(kIOError);
    return false;
}

bool MasterImpl::ReadFromStream(std::ifstream& ifs,
                                std::string* key,
                                std::string* value) {
    uint32_t key_size = 0, value_size = 0;
    ifs.read((char*)&key_size, sizeof(key_size));
    if (ifs.eof() && ifs.gcount() == 0) {
        key->clear();
        value->clear();
        return true;
    }
    key->resize(key_size);
    ifs.read((char*)key->data(), key_size);
    if (ifs.fail()) {
        return false;
    }
    ifs.read((char*)&value_size, sizeof(value_size));
    if (ifs.fail()) {
        return false;
    }
    value->resize(value_size);
    ifs.read((char*)value->data(), value_size);
    if (ifs.fail()) {
        return false;
    }
    return true;
}

/////////////  RPC interface //////////////

void MasterImpl::CreateTable(const CreateTableRequest* request,
                             CreateTableResponse* response,
                             google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    std::shared_ptr<Procedure> proc(new CreateTableProcedure(request, response, done, thread_pool_.get()));
    MasterEnv().GetExecutor()->AddProcedure(proc);
}

void MasterImpl::DeleteTable(const DeleteTableRequest* request,
                             DeleteTableResponse* response,
                             google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    TablePtr table;
    if (!tablet_manager_->FindTable(request->table_name(), &table) || !table->LockTransition()) {
        LOG_IF(ERROR, !table) << "fail to delete table: " << request->table_name()
            << ", table not exist";
        LOG_IF(ERROR, table) << "fail to delete table: " << request->table_name()
            << ", current in another state transition";
        StatusCode code = !table ? kTableNotFound : kTableNotSupport;
        response->set_status(code);
        done->Run();
        return;
    }
    std::shared_ptr<Procedure> proc(new DeleteTableProcedure(table, request, response, done, thread_pool_.get()));
    MasterEnv().GetExecutor()->AddProcedure(proc);
}

void MasterImpl::DisableTable(const DisableTableRequest* request,
                              DisableTableResponse* response,
                              google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }
    TablePtr table;
    if (!tablet_manager_->FindTable(request->table_name(), &table) || !table->LockTransition()) {
        LOG_IF(ERROR, !table) << "fail to disable table: " << request->table_name()
            << ", table not exist";
        LOG_IF(ERROR, table) << "fail to disable table: " << request->table_name()
            << ", current in another state transition";
        StatusCode code = !table ? kTableNotFound : kTableNotSupport;
        response->set_status(code);
        done->Run();
        return;
    }

    std::shared_ptr<Procedure> proc(new DisableTableProcedure(table, request, response, done, thread_pool_.get()));
    MasterEnv().GetExecutor()->AddProcedure(proc);
}

void MasterImpl::EnableTable(const EnableTableRequest* request,
                             EnableTableResponse* response,
                             google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    TablePtr table;
    if (!tablet_manager_->FindTable(request->table_name(), &table) || !table->LockTransition()) {
        LOG_IF(ERROR, !table) << "fail to enable table: " << request->table_name()
            << ", table not exist";
        LOG_IF(ERROR, table) << "fail to enable table: " << request->table_name()
            << ", current in another state transition";
        StatusCode code = !table ? kTableNotFound : kTableNotSupport;
        response->set_status(code);
        done->Run();
        return;
    }
    std::shared_ptr<Procedure> proc(new EnableTableProcedure(table, request, response, done, thread_pool_.get()));
    MasterEnv().GetExecutor()->AddProcedure(proc);
}

void MasterImpl::UpdateCheck(const UpdateCheckRequest* request,
                             UpdateCheckResponse* response,
                             google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    TablePtr table;
    if (!tablet_manager_->FindTable(request->table_name(), &table)) {
        LOG(ERROR) << "[update] fail to update-check table: " << request->table_name()
            << ", table not exist";
        response->set_status(kTableNotExist);
        done->Run();
        return;
    }
    if (!HasPermission(request, table, "update-check table")) {
        response->set_status(kNotPermission);
        done->Run();
        return;
    }
    if (!FLAGS_tera_online_schema_update_enabled) {
        LOG(INFO) << "[update] online-schema-change is disabled";
        response->set_status(kInvalidArgument);
    } else if (table->GetSchemaIsSyncing()) {
        response->set_done(false);
        response->set_status(kMasterOk);
    } else {
        response->set_done(true);
        response->set_status(kMasterOk);
    }
    done->Run();
}

void MasterImpl::UpdateTable(const UpdateTableRequest* request,
                             UpdateTableResponse* response,
                             google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    TablePtr table;
    if (!tablet_manager_->FindTable(request->table_name(), &table) || !table->LockTransition()) {
        LOG_IF(ERROR, !table) << "fail to update table: " << request->table_name()
            << ", table not exist";
        LOG_IF(ERROR, table) << "fail to update table: " << request->table_name()
            << ", current in another state transition";
        StatusCode code = !table ? kTableNotFound : kTableNotSupport;
        response->set_status(code);
        done->Run();
        return;
    }

    std::shared_ptr<Procedure> proc(new UpdateTableProcedure(table, request, response, done, thread_pool_.get()));
    MasterEnv().GetExecutor()->AddProcedure(proc);
}

void MasterImpl::SearchTable(const SearchTableRequest* request,
                             SearchTableResponse* response,
                             google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    std::string start_table_name = request->prefix_table_name();
    if (request->has_start_table_name()) {
        start_table_name = request->start_table_name();
    }
    std::string start_tablet_key;
    if (request->has_start_tablet_key()) {
        start_tablet_key = request->start_tablet_key();
    }
    uint32_t max_found = std::numeric_limits<unsigned int>::max();
    if (request->has_max_num()) {
        max_found = request->max_num();
    }
    StatusCode status = kMasterOk;
    std::vector<TabletPtr> tablet_list;
    int64_t found_num = tablet_manager_->SearchTable(&tablet_list,
                        request->prefix_table_name(), start_table_name,
                        start_tablet_key, max_found, &status);
    if (found_num >= 0) {
        TabletMetaList* ret_meta_list = response->mutable_meta_list();
        for (uint32_t i = 0; i < tablet_list.size(); ++i) {
            TabletPtr tablet = tablet_list[i];
            tablet->ToMeta(ret_meta_list->add_meta());
        }
        response->set_is_more(found_num == max_found);
    } else {
        LOG(ERROR) << "fail to find tablet meta for: " << request->prefix_table_name()
            << ", status_: " << StatusCodeToString(status);
    }

    response->set_status(status);
    done->Run();
}

void MasterImpl::CopyTableMetaToUser(TablePtr table, TableMeta* meta_ptr) {
    TableSchema old_schema;
    if (table->GetOldSchema(&old_schema)) {
        TableMeta meta;
        table->ToMeta(&meta);
        meta.mutable_schema()->CopyFrom(old_schema);
        meta_ptr->CopyFrom(meta);
    } else {
        table->ToMeta(meta_ptr);
    }
}

void MasterImpl::ShowTables(const ShowTablesRequest* request,
                            ShowTablesResponse* response,
                            google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning && master_status != kIsReadonly) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    std::string start_table_name;
    if (request->has_start_table_name()) {
        start_table_name = request->start_table_name();
    }
    std::string start_tablet_key;
    if (request->has_start_tablet_key()) {
        start_tablet_key = request->start_tablet_key();
    }
    uint32_t max_table_found = std::numeric_limits<unsigned int>::max();
    if (request->has_max_table_num()) {
        max_table_found = request->max_table_num();
    }
    uint32_t max_tablet_found = std::numeric_limits<unsigned int>::max();
    if (request->has_max_tablet_num()) {
        max_tablet_found = request->max_tablet_num();
    }

    StatusCode status = kMasterOk;
    std::vector<TablePtr> table_list;
    std::vector<TabletPtr> tablet_list;
    bool is_more = false;
    bool ret =
        tablet_manager_->ShowTable(&table_list, &tablet_list,
                                    start_table_name, start_tablet_key,
                                    max_table_found, max_tablet_found,
                                    &is_more, &status);
    if (ret) {
        TableMetaList* table_meta_list = response->mutable_table_meta_list();
        for (uint32_t i = 0; i < table_list.size(); ++i) {
            TablePtr table = table_list[i];
            CopyTableMetaToUser(table, table_meta_list->add_meta());
        }
        TabletMetaList* tablet_meta_list = response->mutable_tablet_meta_list();
        for (uint32_t i = 0; i < tablet_list.size(); ++i) {
            TabletPtr tablet = tablet_list[i];
            TabletMeta meta;
            tablet->ToMeta(&meta);
            meta.set_last_move_time_us(tablet->LastMoveTime());
            tablet_meta_list->add_meta()->CopyFrom(meta);
            tablet_meta_list->add_counter()->CopyFrom(tablet->GetCounter());
            tablet_meta_list->add_timestamp(tablet->UpdateTime());
        }
        response->set_is_more(is_more);
    } else {
        LOG(ERROR) << "fail to show all tables, status_: "
            << StatusCodeToString(status);
    }

    response->set_status(status);
    done->Run();
}

void MasterImpl::ShowTablesBrief(const ShowTablesRequest* request,
                                ShowTablesResponse* response,
                                google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning && master_status != kIsReadonly) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    std::vector<TablePtr> table_list;
    tablet_manager_->ShowTable(&table_list, NULL);

    TableMetaList* table_meta_list = response->mutable_table_meta_list();
    for (uint32_t i = 0; i < table_list.size(); ++i) {
        TablePtr table = table_list[i];
        table->ToMeta(table_meta_list->add_meta());
        table_meta_list->add_counter()->CopyFrom(table->GetCounter());
    }

    response->set_all_brief(true);
    response->set_status(kMasterOk);
    done->Run();
}

void MasterImpl::ShowTabletNodes(const ShowTabletNodesRequest* request,
                                 ShowTabletNodesResponse* response,
                                 google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning && master_status != kIsReadonly) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    if (request->has_is_showall() && request->is_showall()) {
        // show all tabletnodes
        std::vector<TabletNodePtr> tabletnode_array;
        tabletnode_manager_->GetAllTabletNodeInfo(&tabletnode_array);
        for (size_t i = 0; i < tabletnode_array.size(); ++i) {
            response->add_tabletnode_info()->CopyFrom(tabletnode_array[i]->GetInfo());
        }
        response->set_status(kMasterOk);
        done->Run();
        return;
    } else {
        TabletNodePtr tabletnode;
        if (!tabletnode_manager_->FindTabletNode(request->addr(), &tabletnode)) {
            response->set_status(kTabletNodeNotRegistered);
            done->Run();
            return;
        }
        response->add_tabletnode_info()->CopyFrom(tabletnode->GetInfo());
        std::vector<TabletPtr> tablet_list;
        tablet_manager_->FindTablet(request->addr(),
                                    &tablet_list,
                                    false);  // don't need disabled tables/tablets
        for (size_t i = 0; i < tablet_list.size(); ++i) {
            TabletMeta* meta = response->mutable_tabletmeta_list()->add_meta();
            TabletCounter* counter = response->mutable_tabletmeta_list()->add_counter();
            tablet_list[i]->ToMeta(meta);
            counter->CopyFrom(tablet_list[i]->GetCounter());
        }

        response->set_status(kMasterOk);
        done->Run();
        return;
    }
}

void MasterImpl::KickTabletNodeCmdCtrl(const CmdCtrlRequest* request,
                                       CmdCtrlResponse* response) {
    if (request->arg_list_size() == 1) {
        TryKickTabletNode(request->arg_list(0));
        response->set_status(kMasterOk);
        return;
    } else {
        response->set_status(kInvalidArgument);
        return;
    }
}

void MasterImpl::CmdCtrl(const CmdCtrlRequest* request,
                         CmdCtrlResponse* response) {
    std::string cmd_line;
    for (int32_t i = 0; i < request->arg_list_size(); i++) {
        cmd_line += request->arg_list(i);
        if (i != request->arg_list_size() - 1) {
            cmd_line += " ";
        }
    }
    LOG(INFO) << "receive cmd: " << request->command() << " " << cmd_line;

    response->set_sequence_id(request->sequence_id());
    if (request->command() == "safemode") {
        SafeModeCmdCtrl(request, response);
    } else if (request->command() == "tablet") {
        TabletCmdCtrl(request, response);
    } else if (request->command() == "meta") {
        MetaCmdCtrl(request, response);
    } else if (request->command() == "reload config") {
        ReloadConfig(response);
    } else if (request->command() == "kick") {
        KickTabletNodeCmdCtrl(request, response);
    } else if (request->command() == "table") {
        TableCmdCtrl(request, response);
    } else {
        response->set_status(kInvalidArgument);
    }
}

void MasterImpl::AddUserInfoToMetaCallback(UserPtr user_ptr,
                                           const OperateUserRequest* rpc_request,
                                           OperateUserResponse* rpc_response,
                                           google::protobuf::Closure* rpc_done,
                                           bool succ) {
    if (!succ) {
        rpc_response->set_status(kMetaTabletError);
        rpc_done->Run();
        return;
    }
    rpc_response->set_status(kMasterOk);
    rpc_done->Run();
    LOG(INFO) << "[user-manager] write user info to meta table done: "
            << user_ptr->GetUserInfo().user_name();
    std::string user_name = user_ptr->GetUserInfo().user_name();
    UserOperateType op_type = rpc_request->op_type();
    if (op_type == kDeleteUser) {
        user_manager_->DeleteUser(user_name);
    } else if (op_type == kCreateUser){
        user_manager_->AddUser(user_name, user_ptr->GetUserInfo());
    } else if (op_type == kChangePwd) {
        user_manager_->SetUserInfo(user_name, user_ptr->GetUserInfo());
    } else if (op_type == kAddToGroup) {
        user_manager_->SetUserInfo(user_name, user_ptr->GetUserInfo());
    } else if (op_type == kDeleteFromGroup) {
        user_manager_->SetUserInfo(user_name, user_ptr->GetUserInfo());
    } else {
        LOG(ERROR) << "[user-manager] unknown operate type: " << op_type;
    }
    LOG(INFO) << "[user-manager] " << user_ptr->DebugString();
}

void MasterImpl::OperateUser(const OperateUserRequest* request,
                             OperateUserResponse* response,
                             google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(ERROR) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }
    if (!request->has_user_info()
        || !request->user_info().has_user_name()
        || !request->has_op_type()) {
        response->set_status(kInvalidArgument);
        done->Run();
        return;
    }
    /*
     * for (change password), (add user to group), (delete user from group),
     * we get the original UserInfo(including token & group),
     * do some modification according to the RPC request on the original UserInfo,
     * and rewrite it to meta table.
     */
    UserInfo operated_user = request->user_info();
    std::string user_name = operated_user.user_name();
    std::string token; // caller of this request
    token = request->has_user_token() ? request->user_token() : "";
    UserOperateType op_type = request->op_type();
    bool is_delete = false;
    bool is_invalid = false;
    if (op_type == kCreateUser) {
        if (!operated_user.has_user_name() || !operated_user.has_token()
            || !user_manager_->IsValidForCreate(token, user_name)) {
            is_invalid = true;
        }
    } else if (op_type == kDeleteUser) {
        if (!operated_user.has_user_name()
            || !user_manager_->IsValidForDelete(token, user_name)) {
            is_invalid = true;
        } else {
            is_delete = true;
        }
    } else if (op_type == kChangePwd) {
        if (!operated_user.has_user_name() || !operated_user.has_token()
            || !user_manager_->IsValidForChangepwd(token, user_name)) {
            is_invalid = true;
        } else {
            operated_user = user_manager_->GetUserInfo(user_name);
            operated_user.set_token(request->user_info().token());
        }
    } else if (op_type == kAddToGroup) {
        if (!operated_user.has_user_name() || operated_user.group_name_size() != 1
            || !user_manager_->IsValidForAddToGroup(token, user_name,
                                                     operated_user.group_name(0))) {
            is_invalid = true;
        } else {
            std::string group = operated_user.group_name(0);
            operated_user = user_manager_->GetUserInfo(user_name);
            operated_user.add_group_name(group);
        }
    } else if (op_type == kDeleteFromGroup) {
        if (!operated_user.has_user_name() || operated_user.group_name_size() != 1
            || !user_manager_->IsValidForDeleteFromGroup(token, user_name,
                                                          operated_user.group_name(0))) {
            is_invalid = true;
        } else {
            std::string group = operated_user.group_name(0);
            operated_user = user_manager_->GetUserInfo(user_name);
            user_manager_->DeleteGroupFromUserInfo(operated_user, group);
        }
    } else if (op_type == kShowUser) {
        UserInfo* user_info = response->mutable_user_info();
        *user_info = user_manager_->GetUserInfo(user_name);
        response->set_status(kMasterOk);
        done->Run();
        return;
    } else {
        LOG(ERROR) << "[user-manager] unknown operate type: " << op_type;
        is_invalid = true;
    }
    if (is_invalid) {
        response->set_status(kInvalidArgument);
        done->Run();
        return;
    }
    UserPtr user_ptr(new User(user_name, operated_user));

    std::string key, value;
    user_ptr->ToMetaTableKeyValue(&key, &value);
    MetaWriteRecord record {key, value, is_delete};
    UpdateMetaClosure closure = std::bind(
            &MasterImpl::AddUserInfoToMetaCallback, this, user_ptr, request, response, done, _1);
    MasterEnv().BatchWriteMetaTableAsync(record, closure, FLAGS_tera_master_meta_retry_times);
}

void MasterImpl::SafeModeCmdCtrl(const CmdCtrlRequest* request,
                                 CmdCtrlResponse* response) {
    if (request->arg_list_size() != 1) {
        response->set_status(kInvalidArgument);
        return;
    }

    StatusCode status;
    if (request->arg_list(0) == "enter") {
        if (EnterSafeMode(&status) || status == kMasterIsReadonly) {
            response->set_status(kMasterOk);
        } else {
            response->set_status(status);
        }
    } else if (request->arg_list(0) == "leave") {
        if (LeaveSafeMode(&status) || status == kMasterIsRunning) {
            response->set_status(kMasterOk);
        } else {
            response->set_status(status);
        }
    } else if (request->arg_list(0) == "get") {
        response->set_bool_result(kIsReadonly == GetMasterStatus());
        response->set_status(kMasterOk);
    } else {
        response->set_status(kInvalidArgument);
    }
}

void MasterImpl::ReloadConfig(CmdCtrlResponse* response) {
    if (utils::LoadFlagFile(FLAGS_flagfile)) {
        LOG(INFO) << "[reload config] done";
        response->set_status(kMasterOk);
    } else {
        LOG(ERROR) << "[reload config] config file not found";
        response->set_status(kInvalidArgument);
    }
}

void MasterImpl::TableCmdCtrl(const CmdCtrlRequest* request,
                              CmdCtrlResponse* response) {
    if (request->arg_list_size() < 2) {
        response->set_status(kInvalidArgument);
        return;
    }

    if (request->arg_list(0) == "split") {
        TabletPtr tablet;
        StatusCode status;
        for (int32_t i = 2; i < request->arg_list_size(); i++) {
            if (!tablet_manager_->SearchTablet(request->arg_list(1),
                                               request->arg_list(i),
                                               &tablet, &status)) {
                response->set_status(kInvalidArgument);
                return;
            }
            VLOG(10) << "table split: key " << request->arg_list(i)
                << ", " << tablet;
            //TrySplitTablet(tablet, request->arg_list(i));
        }
        response->set_status(kMasterOk);
    } else {
        response->set_status(kInvalidArgument);
    }
    return;
}

void MasterImpl::TabletCmdCtrl(const CmdCtrlRequest* request,
                               CmdCtrlResponse* response) {
    int32_t request_argc = request->arg_list_size();
    if (request_argc < 2) {
        response->set_status(kInvalidArgument);
        return;
    }
    const std::string& op = request->arg_list(0);
    const std::string& tablet_id = request->arg_list(1);
    TabletPtr tablet;
    bool found = false;
    std::vector<TabletPtr> all_tablet_list;
    tablet_manager_->ShowTable(NULL, &all_tablet_list);
    std::vector<TabletPtr>::iterator it = all_tablet_list.begin();
    for (; it != all_tablet_list.end(); ++it) {
        TabletPtr t = *it;
        if (tablet_id == t->GetPath()) {
            tablet = t;
            found = true;
        }
    }
    if (!found) {
        response->set_status(kInvalidArgument);
        return;
    }

    if (op == "reload" && request_argc == 2) {
        TabletNodePtr current_tablet_node = tablet->GetTabletNode();
        TryMoveTablet(tablet, current_tablet_node);
        response->set_status(kMasterOk);
    } else if (op == "reloadx" && request_argc == 3
               && tablet->SetErrorIgnoredLGs(request->arg_list(2))) {
        TabletNodePtr current_tablet_node = tablet->GetTabletNode();
        TryMoveTablet(tablet, current_tablet_node);
        response->set_status(kMasterOk);
    } else if (op == "move" && request_argc == 3) {
        std::string expect_server_addr = request->arg_list(2);
        TabletNodePtr dest_node;
        if (!expect_server_addr.empty() &&
                !tabletnode_manager_->FindTabletNode(expect_server_addr, &dest_node)) {
            response->set_status(kInvalidArgument);
            return;
        }
        TryMoveTablet(tablet, dest_node);
        response->set_status(kMasterOk);
    } else if (op == "movex" && request_argc == 4
            && tablet->SetErrorIgnoredLGs(request->arg_list(3))) {
        std::string expect_server_addr = request->arg_list(2);
        TabletNodePtr dest_node;
        if (!expect_server_addr.empty() &&
                !tabletnode_manager_->FindTabletNode(expect_server_addr, &dest_node)) {
            response->set_status(kInvalidArgument);
            return;
        }
        TryMoveTablet(tablet, dest_node);
        response->set_status(kMasterOk);
    } else if (op == "split" && (request_argc == 2 || request_argc == 3)) {
        std::string split_key;
        if (request_argc == 3) {
            split_key = request->arg_list(2);
            LOG(INFO) << "User specified split key: " << split_key;
        }
        TrySplitTablet(tablet, split_key);
        response->set_status(kMasterOk);
    } else if (op == "merge" && request_argc == 2) {
        TryMergeTablet(tablet);
        response->set_status(kMasterOk);
    } else {
        response->set_status(kInvalidArgument);
    }
}

void MasterImpl::MetaCmdCtrl(const CmdCtrlRequest* request,
                             CmdCtrlResponse* response) {
    if (request->arg_list_size() != 2) {
        response->set_status(kInvalidArgument);
        return;
    }

    if (request->arg_list(0) == "backup") {
        const std::string& filename = request->arg_list(1);
        StatusCode status = kMasterOk;
        if (tablet_manager_->DumpMetaTableToFile(filename, &status)) {
            response->set_status(kMasterOk);
        } else {
            response->set_status(status);
        }
    } else {
        response->set_status(kInvalidArgument);
    }
}

/////////// common ////////////

bool MasterImpl::SetMasterStatus(const MasterStatus& new_status,
                                 MasterStatus* old_status) {
    MutexLock lock(&status_mutex_);
    if (old_status != NULL) {
        *old_status = status_;
    }
    if (CheckStatusSwitch(status_, new_status)) {
        LOG(INFO) << "master status switch "
            << StatusCodeToString(static_cast<StatusCode>(status_))
            << " to " << StatusCodeToString(static_cast<StatusCode>(new_status));
        status_ = new_status;
        return true;
    }
    return false;
}

MasterImpl::MasterStatus MasterImpl::GetMasterStatus() {
    MutexLock lock(&status_mutex_);
    return status_;
}

bool MasterImpl::CheckStatusSwitch(MasterStatus old_status,
                                   MasterStatus new_status) {
    switch (old_status) {
    case kNotInited:
        if (new_status == kIsSecondary) {
            return true;
        }
        break;
    case kIsSecondary:
        if (new_status == kOnRestore || new_status == kOnWait) {
            return true;
        }
        break;
    case kOnWait:
        if (new_status == kOnRestore) {
            return true;
        }
        break;
    case kOnRestore:
        if (new_status == kIsRunning || new_status == kIsReadonly
            || new_status == kOnWait) {
            return true;
        }
        break;
    case kIsRunning:
        if (new_status == kIsReadonly) {
            return true;
        }
        break;
    case kIsReadonly:
        if (new_status == kIsRunning) {
            return true;
        }
        break;
    default:
        break;
    }

    LOG(ERROR) << "not support master status switch "
        << StatusCodeToString(static_cast<StatusCode>(old_status)) << " to "
        << StatusCodeToString(static_cast<StatusCode>(new_status));
    return false;
}

bool MasterImpl::GetMetaTabletAddr(std::string* addr) {
    return (restored_ && tablet_manager_->GetMetaTabletAddr(addr));
}

/////////// load balance //////////

void MasterImpl::QueryTabletNode() {
    bool gc_query_enable = false;
    {
        MutexLock locker(&mutex_);
        if (!query_enabled_) {
            query_tabletnode_timer_id_ = kInvalidTimerId;
            return;
        }
        if (gc_query_enable_) {
            gc_query_enable_ = false;
            gc_query_enable = true;
        }
    }

    start_query_time_ = get_micros();
    std::vector<TabletNodePtr> tabletnode_array;
    tabletnode_manager_->GetAllTabletNodeInfo(&tabletnode_array);
    LOG(INFO) << "query tabletnodes: " << tabletnode_array.size()
        << ", id " << query_tabletnode_timer_id_;

    if (FLAGS_tera_stat_table_enabled) {
        stat_table_->OpenStatTable();
    }

    CHECK_EQ(query_pending_count_.Get(), 0);
    query_pending_count_.Inc();
    std::vector<TabletNodePtr>::iterator it = tabletnode_array.begin();
    for (; it != tabletnode_array.end(); ++it) {
        TabletNodePtr tabletnode = *it;
        if (tabletnode->state_ != kReady) {
            VLOG(20) << "will not query tabletnode: " << tabletnode->addr_;
            continue;
        }
        query_pending_count_.Inc();
        QueryClosure done =
            std::bind(&MasterImpl::QueryTabletNodeCallback, this, tabletnode->addr_,
                      _1, _2, _3, _4);
        QueryTabletNodeAsync(tabletnode->addr_,
                             FLAGS_tera_master_query_tabletnode_period,
                             gc_query_enable, done);
    }

    if (0 == query_pending_count_.Dec()) {
        {
            MutexLock locker(&mutex_);
            if (query_enabled_) {
                ScheduleQueryTabletNode();
            } else {
                query_tabletnode_timer_id_ = kInvalidTimerId;
            }
        }
        if (gc_query_enable) {
            DoTabletNodeGcPhase2();
        }
    }
}

void MasterImpl::ScheduleQueryTabletNode() {
    mutex_.AssertHeld();
    int schedule_delay = FLAGS_tera_master_query_tabletnode_period;

    LOG(INFO) << "schedule query tabletnodes after " << schedule_delay << "ms.";

    ThreadPool::Task task = std::bind(&MasterImpl::QueryTabletNode, this);
    query_tabletnode_timer_id_ = thread_pool_->DelayTask(schedule_delay, task);
}

void MasterImpl::EnableQueryTabletNodeTimer() {
    MutexLock locker(&mutex_);
    if (query_tabletnode_timer_id_ == kInvalidTimerId) {
        ScheduleQueryTabletNode();
    }
    query_enabled_ = true;
}

void MasterImpl::DisableQueryTabletNodeTimer() {
    MutexLock locker(&mutex_);
    if (query_tabletnode_timer_id_ != kInvalidTimerId) {
        bool non_block = true;
        if (thread_pool_->CancelTask(query_tabletnode_timer_id_, non_block)) {
            query_tabletnode_timer_id_ = kInvalidTimerId;
        }
    }
    query_enabled_ = false;
}

void MasterImpl::ScheduleLoadBalance() {
    {
        MutexLock locker(&mutex_);
        if (!load_balance_enabled_) {
            return;
        }
        if (load_balance_scheduled_) {
            return;
        }
        load_balance_scheduled_ = true;
    }

    ThreadPool::Task task =
        std::bind(static_cast<void(MasterImpl::*)()>(&MasterImpl::LoadBalance), this);
    thread_pool_->AddTask(task);
}

void MasterImpl::EnableLoadBalance() {
    MutexLock locker(&mutex_);
    load_balance_enabled_ = true;
}

void MasterImpl::DisableLoadBalance() {
    MutexLock locker(&mutex_);
    load_balance_enabled_ = false;
}

void MasterImpl::LoadBalance() {
    {
        MutexLock locker(&mutex_);
        if (!load_balance_enabled_) {
            load_balance_scheduled_ = false;
            return;
        }
    }

    LOG(INFO) << "LoadBalance start";
    int64_t start_time = get_micros();

    std::vector<TablePtr> all_table_list;
    std::vector<TabletPtr> all_tablet_list;
    tablet_manager_->ShowTable(&all_table_list, &all_tablet_list);

    std::vector<TabletNodePtr> all_node_list;
    tabletnode_manager_->GetAllTabletNodeInfo(&all_node_list);

    // Make a constant copy of tablet nodes to make sure that the returned value
    // of GetSize, GetQps, ... remain unchanged every time they are called during
    // the load balance process so as not to cause exceptions of std::sort().
    std::vector<TabletNodePtr> all_node_list_copy;
    for (size_t i = 0; i < all_node_list.size(); i++) {
        TabletNodePtr node_copy(new TabletNode(*all_node_list[i]));
        all_node_list_copy.push_back(node_copy);
    }

    uint32_t max_move_num = FLAGS_tera_master_max_move_concurrency;

    // Run qps-based-sheduler first, then size-based-scheduler
    // If read_pending occured, process it first
    max_move_num -= LoadBalance(load_scheduler_.get(), max_move_num, 1,
                                all_node_list_copy, all_tablet_list);

    if (FLAGS_tera_master_load_balance_table_grained) {
        for (size_t i = 0; i < all_table_list.size(); ++i) {
            TablePtr table = all_table_list[i];
            if (table->GetStatus() != kTableEnable) {
                continue;
            }
            if (table->GetTableName() == FLAGS_tera_master_meta_table_name) {
                continue;
            }

            std::vector<TabletPtr> tablet_list;
            table->GetTablet(&tablet_list);
            max_move_num -= LoadBalance(size_scheduler_.get(), max_move_num, 3,
                                        all_node_list_copy, tablet_list, table->GetTableName());
        }
    } else {
        max_move_num -= LoadBalance(size_scheduler_.get(), max_move_num, 3,
                                    all_node_list_copy, all_tablet_list);
    }

    int64_t cost_time = get_micros() - start_time;
    LOG(INFO) << "LoadBalance finish, cost " << cost_time / 1000000.0 << "s";

    {
        MutexLock locker(&mutex_);
        load_balance_scheduled_ = false;
    }
}

uint32_t MasterImpl::LoadBalance(Scheduler* scheduler,
                                 uint32_t max_move_num, uint32_t max_round_num,
                                 std::vector<TabletNodePtr>& tabletnode_list_copy,
                                 std::vector<TabletPtr>& tablet_list,
                                 const std::string& table_name) {
    std::map<std::string, std::vector<TabletPtr> > node_tablet_list;
    std::vector<TabletPtr>::iterator it = tablet_list.begin();
    for (; it != tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        node_tablet_list[tablet->GetServerAddr()].push_back(tablet);
    }

    if (!scheduler->NeedSchedule(tabletnode_list_copy, table_name)) {
        return 0;
    }

    // descending sort the node according to workload,
    // so that the node with heaviest workload will be scheduled first
    scheduler->DescendingSort(tabletnode_list_copy, table_name);

    uint32_t round_count = 0;
    uint32_t total_move_count = 0;
    while (round_count < max_round_num) {
        VLOG(20) << "LoadBalance (" << scheduler->Name() << ") " << table_name
                  << " round " << round_count << " start";

        uint32_t round_move_count = 0;
        std::vector<TabletNodePtr>::iterator node_copy_it = tabletnode_list_copy.begin();
        while (total_move_count < max_move_num && node_copy_it != tabletnode_list_copy.end()) {
            TabletNodePtr node;
            if (tabletnode_manager_->FindTabletNode((*node_copy_it)->GetAddr(), &node)
                && (*node_copy_it)->GetId() == node->GetId()
                && node->GetState() == kReady) {
                const std::vector<TabletPtr>& tablet_list = node_tablet_list[node->GetAddr()];
                if (TabletNodeLoadBalance(node, scheduler, tablet_list, table_name)) {
                    round_move_count++;
                    total_move_count++;
                }
            }
            ++node_copy_it;
        }

        VLOG(20) << "LoadBalance (" << scheduler->Name() << ") " << table_name
                  << " round " << round_count << " move " << round_move_count;

        round_count++;
        if (round_move_count == 0) {
            break;
        }
    }

    if (total_move_count != 0) {
        LOG(INFO) << "LoadBalance (" << scheduler->Name() << ") " << table_name
                  << " total round " << round_count << " total move " << total_move_count;
    }
    return total_move_count;
}

bool MasterImpl::TabletNodeLoadBalance(TabletNodePtr tabletnode, Scheduler* scheduler,
                                       const std::vector<TabletPtr>& tablet_list,
                                       const std::string& table_name) {
    VLOG(7) << "TabletNodeLoadBalance() " << tabletnode->GetAddr() << " "
            << scheduler->Name() << " " << table_name;
    if (tablet_list.size() < 1) {
        return false;
    }

    bool any_tablet_split = false;
    std::vector<TabletPtr> tablet_candidates;

    std::vector<TabletPtr>::const_iterator it;
    for (it = tablet_list.begin(); it != tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->GetStatus() != TabletMeta::kTabletReady
            || tablet->GetTableName() == FLAGS_tera_master_meta_table_name) {
            continue;
        }
        double write_workload = tablet->GetCounter().write_workload();
        int64_t split_size = FLAGS_tera_master_split_tablet_size;
        if (tablet->GetSchema().has_split_size() && tablet->GetSchema().split_size() > 0) {
            split_size = tablet->GetSchema().split_size();
        }
        if (write_workload > FLAGS_tera_master_workload_split_threshold) {
            if (split_size > FLAGS_tera_master_min_split_size) {
                split_size = std::max(FLAGS_tera_master_min_split_size,
                                      static_cast<int64_t>(split_size * FLAGS_tera_master_min_split_ratio));
            }
            VLOG(6) << tablet->GetPath() << ", trigger workload split, write_workload: " << write_workload
                << ", split it by size(M): " << split_size;
        }
        int64_t merge_size = FLAGS_tera_master_merge_tablet_size;
        if (tablet->GetSchema().has_merge_size() && tablet->GetSchema().merge_size() > 0) {
            merge_size = tablet->GetSchema().merge_size();
        }
        if (tablet->GetDataSize() < 0) {
            // tablet size is error, skip it
            continue;
        } else if (tablet->GetDataSize() > (split_size << 20) &&
                   tablet->TestAndSetSplitTimeStamp(get_micros())) {
            TrySplitTablet(tablet);
            any_tablet_split = true;
            continue;
        } else if (tablet->GetDataSize() < (merge_size << 20)) {
            if (!tablet->IsBusy() &&
                write_workload < FLAGS_tera_master_workload_merge_threshold) {
                TryMergeTablet(tablet);
            } else {
                VLOG(6) << "[merge] skip high workload tablet: "
                    << tablet->GetPath() << ", write_workload " << write_workload;
            }
            continue;
        }
        if (tablet->GetStatus() == TabletMeta::kTabletReady) {
            tablet_candidates.push_back(tablet);
        }
    }

    // if any tablet is splitting, no need to move tablet
    if (!FLAGS_tera_master_move_tablet_enabled || any_tablet_split) {
        return false;
    }

    TabletNodePtr dest_tabletnode;
    size_t tablet_index = 0;
    if (scheduler->MayMoveOut(tabletnode, table_name)
            && tabletnode_manager_->ScheduleTabletNode(scheduler, table_name, true, &dest_tabletnode)
            && tabletnode_manager_->ShouldMoveData(scheduler, table_name, tabletnode,
                                                    dest_tabletnode, tablet_candidates,
                                                    &tablet_index)
            && dest_tabletnode->GetState() == kReady) {
        TryMoveTablet(tablet_candidates[tablet_index], dest_tabletnode);
        return true;
    }
    return false;
}

/////////// cache release //////////

void MasterImpl::TryReleaseCache(bool enbaled_debug) {
#if 0
    LOG(INFO) << "TryReleaseCache()";
    size_t free_heap_bytes = 0;
    MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes",
                                                    &free_heap_bytes);
    if (enbaled_debug) {
        LOG(INFO) << "free-heap size: " << free_heap_bytes;
    }

    if (free_heap_bytes == 0) {
        return;
    }

    uint64_t threshold_size =
        FLAGS_tera_master_cache_keep_min * 1024 * 1024;
    if (free_heap_bytes > threshold_size) {
        size_t free_size = free_heap_bytes - threshold_size;
        MallocExtension::instance()->ReleaseToSystem(free_size);
        VLOG(5) << "release cache size: " << free_size;
    }
#endif
}

void MasterImpl::ReleaseCacheWrapper() {
    MutexLock locker(&mutex_);

    TryReleaseCache();

    release_cache_timer_id_ = kInvalidTimerId;
    EnableReleaseCacheTimer();
}

void MasterImpl::EnableReleaseCacheTimer() {
    assert(release_cache_timer_id_ == kInvalidTimerId);
    ThreadPool::Task task =
        std::bind(&MasterImpl::ReleaseCacheWrapper, this);
    int64_t timeout_period = 1000LL *
        FLAGS_tera_master_cache_release_period;
    release_cache_timer_id_ = thread_pool_->DelayTask(
        timeout_period, task);
}

void MasterImpl::DisableReleaseCacheTimer() {
    if (release_cache_timer_id_ != kInvalidTimerId) {
        thread_pool_->CancelTask(release_cache_timer_id_);
        release_cache_timer_id_ = kInvalidTimerId;
    }
}

//////////  ts operation ////////////
void MasterImpl::RefreshTabletNodeList(const std::map<std::string, std::string>& new_ts_list) {
    MutexLock lock(&tabletnode_mutex_);

    std::map<std::string, std::string> del_ts_list;
    std::map<std::string, std::string> add_ts_list;

    std::map<std::string, std::string> old_ts_list;
    tabletnode_manager_->GetAllTabletNodeId(&old_ts_list);

    std::map<std::string, std::string>::const_iterator old_it = old_ts_list.begin();
    std::map<std::string, std::string>::const_iterator new_it = new_ts_list.begin();
    while (old_it != old_ts_list.end() && new_it != new_ts_list.end()) {
        const std::string& old_addr = old_it->first;
        const std::string& new_addr = new_it->first;
        const std::string& old_uuid = old_it->second;
        const std::string& new_uuid = new_it->second;
        int cmp_ret = old_addr.compare(new_addr);
        if (cmp_ret == 0) {
            if (old_uuid != new_uuid) {
                LOG(INFO) << "tabletnode " << old_addr << " restart: "
                    << old_uuid << " -> " << new_uuid;
                del_ts_list[old_addr] = old_uuid;
                add_ts_list[new_addr] = new_uuid;
            }
            ++old_it;
            ++new_it;
        } else if (cmp_ret < 0) {
            del_ts_list[old_addr] = old_uuid;
            ++old_it;
        } else {
            add_ts_list[new_addr] = new_uuid;
            ++new_it;
        }
    }
    for (; old_it != old_ts_list.end(); ++old_it) {
        const std::string& old_addr = old_it->first;
        const std::string& old_uuid = old_it->second;
        del_ts_list[old_addr] = old_uuid;
    }
    for (; new_it != new_ts_list.end(); ++new_it) {
        const std::string& new_addr = new_it->first;
        const std::string& new_uuid = new_it->second;
        add_ts_list[new_addr] = new_uuid;
    }

    std::map<std::string, std::string>::iterator it;
    for (it = del_ts_list.begin(); it != del_ts_list.end(); ++it) {
        const std::string& old_addr = it->first;
        DeleteTabletNode(old_addr, it->second);
    }

    if (add_ts_list.size() > 0 && !restored_) {
        CHECK(GetMasterStatus() == kOnWait);
        Restore(new_ts_list);
        return;
    }

    for (it = add_ts_list.begin(); it != add_ts_list.end(); ++it) {
        const std::string& new_addr = it->first;
        const std::string& new_uuid = it->second;
        AddTabletNode(new_addr, new_uuid);
    }
}

void MasterImpl::AddTabletNode(const std::string& tabletnode_addr,
                               const std::string& tabletnode_uuid) {
    TabletNodePtr node = tabletnode_manager_->AddTabletNode(tabletnode_addr, tabletnode_uuid);
    if (!node) {
        return;
    }
    CHECK(node->GetState() == kReady);
    // update tabletnode info
    timeval update_time;
    gettimeofday(&update_time, NULL);
    TabletNode state;
    state.addr_ = tabletnode_addr;
    state.report_status_ = kTabletNodeReady;
    state.info_.set_addr(tabletnode_addr);
    state.data_size_ = 0;
    state.qps_ = 0;
    state.update_time_ = update_time.tv_sec * 1000 + update_time.tv_usec / 1000;

    tabletnode_manager_->UpdateTabletNode(tabletnode_addr, state);

    // If all tabletnodes restart in one zk callback,
    // master will not enter restore/wait state;
    // meta table must be scheduled to load from here.
    if (meta_tablet_->GetStatus() == TabletMeta::kTabletOffline) {
        TryLoadTablet(meta_tablet_);
    }
    // load offline tablets
    // update tabletnode
    std::vector<TabletPtr> tablet_list;
    tablet_manager_->FindTablet(tabletnode_addr,
                                &tablet_list,
                                false);  // need disabled table/tablets
    std::vector<TabletPtr>::iterator it = tablet_list.begin();
    for (; it != tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->LockTransition()) {
            if (tablet->GetStatus() != TabletMeta::kTabletOffline) {
                tablet->UnlockTransition();
                LOG(WARNING) << "tablet cannot deal TsOffline event, tablet:  " << tablet;
                continue;
            }
            std::shared_ptr<Procedure> load(new LoadTabletProcedure(tablet, node, thread_pool_.get()));
            if (MasterEnv().GetExecutor()->AddProcedure(load) == 0) {
                LOG(WARNING) << "add to procedure_executor fail, may duplicated procid: " << load->ProcId();
                tablet->UnlockTransition();
            }
        }
    }
    TryLeaveSafeMode();
}

void MasterImpl::DeleteTabletNode(const std::string& tabletnode_addr, const std::string& uuid) {
    TabletNodePtr node = tabletnode_manager_->DelTabletNode(tabletnode_addr);
    if (!node) {
        return;
    }
    // possible status: running, readonly, wait.
    if (GetMasterStatus() == kOnWait) {
        return;
    }
    TryEnterSafeMode();

    std::vector<TabletPtr> tablet_list;
    tablet_manager_->FindTablet(tabletnode_addr, &tablet_list, false);
    for (auto it = tablet_list.begin(); it != tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->LockTransition()) {
            // tablet maybe already been updated by another async TabletXxxProcedure (for example a 
            // manual triggered MoveTabletProcedure), leading tablet info got through FindTablet 
            // is stale. skip these kinds of tablets
            if (tablet->GetTabletNode() && tablet->GetTabletNode()->GetId() != uuid) {
                LOG(WARNING) << "stale tablet info, tablet: " << tablet << " has already been resumed @ [" 
                    << tablet->GetTabletNode()->GetAddr() << ", " << tablet->GetTabletNode()->GetId();
                tablet->UnlockTransition();
                continue;
            }
            if (tablet->GetStatus() == TabletMeta::kTabletLoadFail) {
                LOG(INFO) << "try reload tablet in status kTabletLoadFail as ts down, tablet:" << tablet;
                tablet->DoStateTransition(TabletEvent::kTsOffline);
            }
            std::shared_ptr<Procedure> move(new MoveTabletProcedure(tablet, node, thread_pool_.get()));
            if (MasterEnv().GetExecutor()->AddProcedure(move) == 0) {
                LOG(WARNING) << "add to procedure_executor fail, may duplicated procid: " << move->ProcId();
                tablet->UnlockTransition();
            }
        }
    }
}

void MasterImpl::TryEnterSafeMode() {
    if (GetMasterStatus() != kIsRunning) {
        return;
    }
    double tablet_locality_ratio = LiveNodeTabletRatio();
    LOG(INFO) << "tablet locality ratio: " << tablet_locality_ratio;
    if (tablet_locality_ratio < FLAGS_tera_safemode_tablet_locality_ratio) {
        EnterSafeMode();
    }
}

bool MasterImpl::EnterSafeMode(StatusCode* status) {
    MasterStatus old_status;
    if (!SetMasterStatus(kIsReadonly, &old_status)) {
        SetStatusCode(static_cast<StatusCode>(old_status), status);
        return false;
    }

    LOG(WARNING) << kSms << "enter safemode";
    if (!zk_adapter_->MarkSafeMode()) {
        SetStatusCode(kZKError, status);
        return false;
    }

    tablet_manager_->Stop();
    DisableTabletNodeGcTimer();
    DisableLoadBalance();
    DisableGcTrashCleanTimer();
    return true;
}

void MasterImpl::TryLeaveSafeMode() {
    if (GetMasterStatus() != kIsReadonly) {
        return;
    }
    double tablet_locality_ratio = LiveNodeTabletRatio();
    LOG(INFO) << "tablet locality ratio: " << tablet_locality_ratio;
    if (tablet_locality_ratio >= FLAGS_tera_safemode_tablet_locality_ratio) {
        LeaveSafeMode();
    }
}

bool MasterImpl::LeaveSafeMode(StatusCode* status) {
    MasterStatus old_status;
    if (!SetMasterStatus(kIsRunning, &old_status)) {
        SetStatusCode(static_cast<StatusCode>(old_status), status);
        return false;
    }

    LOG(WARNING) << kSms << "leave safemode";
    if (!zk_adapter_->UnmarkSafeMode()) {
        SetStatusCode(kZKError, status);
        return false;
    }

    LoadAllDeadNodeTablets();

    tablet_manager_->Init();
    EnableQueryTabletNodeTimer();
    EnableTabletNodeGcTimer();
    EnableLoadBalance();
    EnableGcTrashCleanTimer();

    std::vector<TabletNodePtr> node_array;
    tabletnode_manager_->GetAllTabletNodeInfo(&node_array);
    for (uint32_t i = 0; i < node_array.size(); i++) {
        TabletNodePtr node = node_array[i];
        if (node->GetState() == kWaitKick) {
            KickTabletNode(node);
        }
    }

    return true;
}

void MasterImpl::LoadAllDeadNodeTablets() {
    std::vector<TabletPtr> all_tablet_list;
    tablet_manager_->ShowTable(NULL, &all_tablet_list);

    std::vector<TabletPtr>::iterator it;
    for (it = all_tablet_list.begin(); it != all_tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->GetStatus() != TabletMeta::kTabletOffline) {
            continue;
        }
        TabletNodePtr node;
        if (tabletnode_manager_->FindTabletNode(tablet->GetServerAddr(), &node)
            && node->GetState() == kReady) {
            continue;
        }
        LOG(INFO) << "try load tablets in dead node, " << tablet;
        TryLoadTablet(tablet);
    }
}
void MasterImpl::TryKickTabletNode(const std::string& tabletnode_addr) {
    if (!FLAGS_tera_master_kick_tabletnode_enabled) {
        LOG(INFO) << "will not kick tabletnode " << tabletnode_addr;
        return;
    }
    LOG(INFO) << "try kick tabletnode " << tabletnode_addr << " ...";

    TabletNodePtr tabletnode;
    if (!tabletnode_manager_->FindTabletNode(tabletnode_addr, &tabletnode)) {
        LOG(WARNING) << "cancel kick tabletnode " << tabletnode_addr << " has been removed";
        return;
    }

    NodeState old_state;
    if (!tabletnode->SetState(kWaitKick, &old_state)) {
        LOG(WARNING) << "cancel kick tabletnode " << tabletnode_addr
            << " state: " << StatusCodeToString(static_cast<StatusCode>(old_state));
        return;
    }
    KickTabletNode(tabletnode);
}

void MasterImpl::KickTabletNode(TabletNodePtr node) {
    // avoid massive kick
    static Mutex mutex;
    {
        MutexLock lock(&mutex);
        MasterStatus status = GetMasterStatus();
        if (status == kIsReadonly) {
            LOG(WARNING) << "cancel kick tabletnode " << node->addr_
                << ", master state: " << StatusCodeToString(static_cast<StatusCode>(status));
            return;
        }
        TryEnterSafeMode();
    }

    NodeState old_state;
    if (!node->SetState(kOnKick, &old_state)) {
        LOG(WARNING) << "cancel kick, tabletnode " << node->addr_
            << " state: " << StatusCodeToString(static_cast<StatusCode>(old_state));
        return;
    }
    if (!zk_adapter_->KickTabletServer(node->addr_, node->uuid_)) {
        LOG(FATAL) << "Unable to kick tabletnode: " << node->addr_;
    }
}

double MasterImpl::LiveNodeTabletRatio() {
    std::vector<TabletPtr> all_tablet_list;
    tablet_manager_->ShowTable(NULL, &all_tablet_list);
    uint64_t tablet_num = all_tablet_list.size();
    if (tablet_num == 0) {
        return 1.0;
    }

    std::map<std::string, std::vector<TabletPtr> > node_tablet_list;
    std::vector<TabletPtr>::iterator it;
    for (it = all_tablet_list.begin(); it != all_tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        node_tablet_list[tablet->GetServerAddr()].push_back(tablet);
    }

    uint64_t live_tablet_num = 0;
    std::vector<TabletNodePtr> all_node_list;
    tabletnode_manager_->GetAllTabletNodeInfo(&all_node_list);
    std::vector<TabletNodePtr>::iterator node_it = all_node_list.begin();
    for (; node_it != all_node_list.end(); ++node_it) {
        TabletNodePtr node = *node_it;
        if (node->GetState() != kReady) {
            continue;
        }
        const std::string& addr = node->GetAddr();
        const std::vector<TabletPtr>& tablet_list = node_tablet_list[addr];
        live_tablet_num += tablet_list.size();
    }
    return (double)live_tablet_num / tablet_num;
}

//////////  table operation ////////////

bool MasterImpl::LoadTabletSync(const TabletMeta& meta,
                                const TableSchema& schema,
                                StatusCode* status) {
    TabletNodePtr node;
    if (!tabletnode_manager_->FindTabletNode(meta.server_addr(), &node)) {
        SetStatusCode(kTabletNodeOffLine, status);
        return false;
    }

    tabletnode::TabletNodeClient node_client(thread_pool_.get(), meta.server_addr(),
                                             FLAGS_tera_master_load_rpc_timeout);

    LoadTabletRequest request;
    LoadTabletResponse response;
    request.set_tablet_name(meta.table_name());
    request.set_sequence_id(this_sequence_id_.Inc());
    request.mutable_key_range()->CopyFrom(meta.key_range());
    request.set_path(meta.path());
    request.mutable_schema()->CopyFrom(schema);
    request.set_session_id(node->uuid_);

    if (node_client.LoadTablet(&request, &response)
        && response.status() == kTabletNodeOk) {
        return true;
    }
    SetStatusCode(response.status(), status);
    return false;
}

bool MasterImpl::UnloadTabletSync(const std::string& table_name,
                                  const std::string& key_start,
                                  const std::string& key_end,
                                  const std::string& server_addr,
                                  StatusCode* status) {
    VLOG(5) << "UnloadTabletSync() for " << table_name << " ["
        << DebugString(key_start) << ", " << DebugString(key_end) << "]";
    tabletnode::TabletNodeClient node_client(thread_pool_.get(), server_addr,
                                                  FLAGS_tera_master_unload_rpc_timeout);

    UnloadTabletRequest request;
    UnloadTabletResponse response;
    request.set_sequence_id(this_sequence_id_.Inc());
    request.set_tablet_name(table_name);
    request.mutable_key_range()->set_key_start(key_start);
    request.mutable_key_range()->set_key_end(key_end);


    if (!node_client.UnloadTablet(&request, &response)
        || response.status() != kTabletNodeOk) {
        SetStatusCode(response.status(), status);
        LOG(ERROR) << "fail to unload table: " << table_name << " ["
            << DebugString(key_start) << ", " << DebugString(key_end) << "]"
            << ", status_: " << StatusCodeToString(response.status());
        return false;
    }
    return true;
}


void MasterImpl::QueryTabletNodeAsync(std::string addr, int32_t timeout,
                                      bool is_gc, QueryClosure done) {
    tabletnode::TabletNodeClient node_client(thread_pool_.get(), addr, timeout);

    QueryRequest* request = new QueryRequest;
    QueryResponse* response = new QueryResponse;
    request->set_sequence_id(this_sequence_id_.Inc());

    if (is_gc) {
        request->set_is_gc_query(true);
    }

    VLOG(20) << "QueryAsync id: " << request->sequence_id() << ", "
        << "server: " << addr;
    node_client.Query(query_thread_pool_.get(), request, response, done);
}

void MasterImpl::QueryTabletNodeCallback(std::string addr, QueryRequest* request,
                                         QueryResponse* response, bool failed,
                                         int error_code) {
    int64_t query_callback_start = get_micros();
    TabletNodePtr node;
    if (!tabletnode_manager_->FindTabletNode(addr, &node)) {
        LOG(WARNING) << "fail to query: server down, id: "
            << request->sequence_id() << ", server: " << addr;
    } else if (failed || response->status() != kTabletNodeOk) {
        if (failed) {
            LOG(WARNING) << "fail to query: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code)
                << ", id: " << request->sequence_id() << ", server: " << addr;
        } else {
            LOG(WARNING) << "fail to query: "
                << StatusCodeToString(response->status())
                << ", id: " << request->sequence_id() << ", server: " << addr;
        }
        int32_t fail_count = node->IncQueryFailCount();
        if (fail_count >= FLAGS_tera_master_kick_tabletnode_query_fail_times) {
            LOG(ERROR) << kSms << "fail to query " << addr
                << " for " << fail_count << " times";
            TryKickTabletNode(addr);
        }
    } else {
        // update tablet meta
        uint32_t meta_num = response->tabletmeta_list().meta_size();
        std::map<tabletnode::TabletRange, int> tablet_map;
        for (uint32_t i = 0; i < meta_num; i++) {
            const TabletMeta& meta = response->tabletmeta_list().meta(i);
            const TabletCounter& counter = response->tabletmeta_list().counter(i);
            const std::string& table_name = meta.table_name();
            const std::string& key_start = meta.key_range().key_start();
            const std::string& key_end = meta.key_range().key_end();

            std::vector<TabletPtr> tablets;
            if (!tablet_manager_->FindOverlappedTablets(table_name, key_start, key_end, &tablets)) {
                LOG(WARNING) << "[query] table not exist, tablet: " << meta.path()
                    << " [" << DebugString(key_start)
                    << ", " << DebugString(key_end)
                    << "] @ " << meta.server_addr()
                    << " status: " << meta.status();
                continue;
            }

            if (tablets.size() > 1) {
                bool any_tablet_load_before_query = false;
                for (uint32_t j = 0; j < tablets.size(); ++j) {
                    if (tablets[j]->ReadyTime() < start_query_time_) {
                        any_tablet_load_before_query = true;
                        break;
                    }
                }
                if (any_tablet_load_before_query) {
                    LOG(ERROR) << "[query] range error tablet: " << meta.path()
                        << " [" << DebugString(key_start)
                        << ", " << DebugString(key_end)
                        << "] @ " << meta.server_addr()
                        << " status: " << meta.status();
                } else {
                    VLOG(20) << "[query] ignore mutable tablet: " << meta.path()
                        << " [" << DebugString(key_start)
                        << ", " << DebugString(key_end)
                        << "] @ " << meta.server_addr()
                        << " status: " << meta.status();
                }
                continue;
            }

            CHECK_EQ(tablets.size(), 1u);
            TabletPtr tablet = tablets[0];
            if (tablet->ReadyTime() >= start_query_time_) {
                VLOG(20) << "[query] ignore mutable tablet: " << meta.path()
                    << " [" << DebugString(key_start)
                    << ", " << DebugString(key_end)
                    << "] @ " << meta.server_addr()
                    << " status: " << meta.status();
            } else if (tablet->GetKeyStart() != key_start || tablet->GetKeyEnd() != key_end) {
                LOG(ERROR) << "[query] range error tablet: " << meta.path()
                    << " [" << DebugString(key_start)
                    << ", " << DebugString(key_end)
                    << "] @ " << meta.server_addr();
            } else if (tablet->GetPath() != meta.path()) {
                LOG(ERROR) << "[query] path error tablet: " << meta.path()
                    << "] @ " << meta.server_addr()
                    << " should be " << tablet->GetPath();
            } else if (TabletMeta::kTabletReady != meta.status()) {
                LOG(ERROR) << "[query] status error tablet: " << meta.path()
                    << "] @ " << meta.server_addr()
                    << " should be kTabletReady";
            } else if (tablet->GetServerAddr() != meta.server_addr()) {
                LOG(ERROR) << "[query] addr error tablet: " << meta.path()
                    << " @ " << meta.server_addr()
                    << " should @ " << tablet->GetServerAddr();
            } else if (tablet->GetTable()->GetStatus() == kTableDisable) {
                LOG(INFO) << "table disabled: " << tablet->GetPath();
            } else {
                VLOG(20) << "[query] OK tablet: " << meta.path()
                    << "] @ " << meta.server_addr();
                tablet->SetUpdateTime(query_callback_start);
                tablet->UpdateSize(meta);
                tablet->SetCounter(counter);
                tablet->SetCompactStatus(meta.compact_status());
            }
        }

        // update tabletnode info
        timeval update_time;
        gettimeofday(&update_time, NULL);
        TabletNode state;
        state.addr_ = addr;
        state.report_status_ = response->tabletnode_info().status_t();
        state.info_ = response->tabletnode_info();
        state.info_.set_addr(addr);
        state.load_ = response->tabletnode_info().load();
        state.data_size_ = 0;
        state.qps_ = 0;
        state.update_time_ = update_time.tv_sec * 1000 + update_time.tv_usec / 1000;
        // calculate data_size of tabletnode
        // count both Ready/OnLoad and OffLine tablet
        std::vector<TabletPtr> tablet_list;
        tablet_manager_->FindTablet(addr,
                                    &tablet_list,
                                    false);  // don't need disabled tables/tablets
        std::vector<TabletPtr>::iterator it;
        for (it = tablet_list.begin(); it != tablet_list.end(); ++it) {
            TabletPtr tablet = *it;
            if (tablet->UpdateTime() != query_callback_start) {
                if (tablet->ReadyTime() < start_query_time_) {
                    LOG(ERROR) << "[query] missed tablet: " << tablet;
                } else {
                    VLOG(20) << "[query] ignore mutable missed tablet: " << tablet;
                }
            }

            TabletMeta::TabletStatus tablet_status = tablet->GetStatus();
            if (tablet_status == TabletMeta::kTabletReady || tablet_status == TabletMeta::kTabletLoading
                || tablet_status == TabletMeta::kTabletOffline) {
                state.data_size_ += tablet->GetDataSize();
                state.qps_ += tablet->GetQps();
                if (state.table_size_.find(tablet->GetTableName()) != state.table_size_.end()) {
                    state.table_size_[tablet->GetTableName()] += tablet->GetDataSize();
                    state.table_qps_[tablet->GetTableName()] += tablet->GetQps();
                } else {
                    state.table_size_[tablet->GetTableName()] = tablet->GetDataSize();
                    state.table_qps_[tablet->GetTableName()] = tablet->GetQps();
                }
            }
        }
        tabletnode_manager_->UpdateTabletNode(addr, state);
        node->ResetQueryFailCount();

        for (int32_t i = 0; i < response->tablet_background_errors_size(); i++) {
            const TabletBackgroundErrorInfo& background_error =
                response->tablet_background_errors(i);
            if (FLAGS_tera_stat_table_enabled) {
                stat_table_->RecordTabletCorrupt(background_error.tablet_name(),
                                                 background_error.detail_info());
            }
        }
        VLOG(20) << "query tabletnode [" << addr << "], status_: "
            << StatusCodeToString(state.report_status_);
    }

    // if this is a gc query, process it
    if (request->is_gc_query()) {
        if (FLAGS_tera_master_gc_strategy == "trackable") {
            for (int32_t i = 0; i < response->tablet_inh_file_infos_size(); i++) {
                const TabletInheritedFileInfo& tablet_inh_info = response->tablet_inh_file_infos(i);
                TablePtr table_ptr;
                if (tablet_manager_->FindTable(tablet_inh_info.table_name(), &table_ptr)) {
                    table_ptr->GarbageCollect(tablet_inh_info);
                }
            }
        } else {
            gc_strategy_->ProcessQueryCallbackForGc(response);
        }
    }

    if (0 == query_pending_count_.Dec()) {
        LOG(INFO) << "query tabletnodes finish, id "
            << query_tabletnode_timer_id_
            << ", cost " << (get_micros() - start_query_time_) / 1000 << "ms." ;
        {
            MutexLock locker(&mutex_);
            if (query_enabled_) {
                ScheduleQueryTabletNode();
            } else {
                query_tabletnode_timer_id_ = kInvalidTimerId;
            }
        }

        ScheduleLoadBalance();

        if (request->is_gc_query()) {
            DoTabletNodeGcPhase2();
        }
    }

    delete request;
    delete response;
    VLOG(20) << "query tabletnode finish " << addr
        << ", id " << query_tabletnode_timer_id_
        << ", callback cost " << (get_micros() - query_callback_start) / 1000 << "ms.";
}

void MasterImpl::CollectTabletInfoCallback(std::string addr,
                                           std::vector<TabletMeta>* tablet_list,
                                           sem_t* finish_counter, Mutex* mutex,
                                           QueryRequest* request,
                                           QueryResponse* response,
                                           bool failed, int error_code) {
    std::unique_ptr<QueryRequest> request_holder(request);
    std::unique_ptr<QueryResponse> response_holder(response);
    TabletNodePtr node;
    if (!tabletnode_manager_->FindTabletNode(addr, &node)) {
        LOG(WARNING) << "fail to query: server down, id: "
            << request->sequence_id() << ", server: " << addr;
    } else if (!failed && response->status() == kTabletNodeOk) {
        mutex->Lock();
        uint32_t meta_num = response->tabletmeta_list().meta_size();
        for (uint32_t i = 0; i < meta_num; i++) {
            const TabletMeta& meta = response->tabletmeta_list().meta(i);
            tablet_list->push_back(meta);
        }
        mutex->Unlock();

        // update tabletnode info
        timeval update_time;
        gettimeofday(&update_time, NULL);
        TabletNode state;
        state.addr_ = addr;
        state.report_status_ = response->tabletnode_info().status_t();
        state.info_ = response->tabletnode_info();
        state.info_.set_addr(addr);
        state.load_ = response->tabletnode_info().load();
        state.data_size_ = 0;
        state.qps_ = 0;
        state.update_time_ = update_time.tv_sec * 1000 + update_time.tv_usec / 1000;
        // calculate data_size of tabletnode
        for (uint32_t i = 0; i < meta_num; i++) {
            const TabletMeta& meta = response->tabletmeta_list().meta(i);
            state.data_size_ += meta.size();
            if (state.table_size_.find(meta.table_name()) != state.table_size_.end()) {
                state.table_size_[meta.table_name()] += meta.size();
            } else {
                state.table_size_[meta.table_name()] = meta.size();
            }
        }
        //NodeState old_state;
        tabletnode_manager_->UpdateTabletNode(addr, state);
        node->ResetQueryFailCount();
        LOG(INFO) << "query tabletnode [" << addr << "], status_: "
            << StatusCodeToString(response->tabletnode_info().status_t());
    } else {
        if (failed) {
            LOG(WARNING) << "fail to query: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code)
                << ", id: " << request->sequence_id() << ", server: " << addr;
        } else {
            LOG(WARNING) << "fail to query: "
                << StatusCodeToString(response->status())
                << ", id: " << request->sequence_id() << ", server: " << addr;
        }
        int32_t fail_count = node->IncQueryFailCount();
        if (fail_count >= FLAGS_tera_master_collect_info_retry_times) {
            LOG(ERROR) << kSms << "fail to query " << addr
                << " for " << fail_count << " times";
            TryKickTabletNode(addr);
        } else {
            ThreadPool::Task task =
                std::bind(&MasterImpl::RetryCollectTabletInfo, this, addr,
                          tablet_list, finish_counter, mutex);
            thread_pool_->DelayTask(FLAGS_tera_master_collect_info_retry_period,
                                     task);
            return;
        }
    }
    sem_post(finish_counter);
}

void MasterImpl::RetryCollectTabletInfo(std::string addr,
                                        std::vector<TabletMeta>* tablet_list,
                                        sem_t* finish_counter, Mutex* mutex) {
    QueryClosure done =
        std::bind(&MasterImpl::CollectTabletInfoCallback, this, addr,
                   tablet_list, finish_counter, mutex, _1, _2, _3, _4);
    QueryTabletNodeAsync(addr, FLAGS_tera_master_collect_info_timeout, false, done);
}


void MasterImpl::AddMetaCallback(std::vector<TabletPtr> tablets,
                                 CreateTableResponse* rpc_response,
                                 google::protobuf::Closure* rpc_done,
                                 bool succ) {
    if (!succ) {
        LOG(INFO) << "create table " << tablets[0]->GetTableName() << " failed";
        rpc_response->set_status(kMetaTabletError);
        rpc_done->Run();
        return;
    }

    rpc_response->set_status(kMasterOk);
    rpc_done->Run();
    LOG(INFO) << "create table " << tablets[0]->GetTableName() << " success";
    for (size_t i = 0; i < tablets.size(); i++) {
        TryLoadTablet(tablets[i]);
    }
}

void MasterImpl::ScheduleTabletNodeGc() {
    mutex_.AssertHeld();
    LOG(INFO) << "[gc] ScheduleTabletNodeGcTimer";
    ThreadPool::Task task =
        std::bind(&MasterImpl::DoTabletNodeGc, this);
    gc_timer_id_ = thread_pool_->DelayTask(
        FLAGS_tera_master_gc_period, task);
}

void MasterImpl::EnableTabletNodeGcTimer() {
    MutexLock lock(&mutex_);
    if (gc_timer_id_ == kInvalidTimerId) {
        ScheduleTabletNodeGc();
    }
    gc_enabled_ = true;
}

void MasterImpl::DoGcTrashClean() {
    {
        MutexLock lock(&mutex_);
        if (!gc_trash_clean_enabled_) {
            gc_trash_clean_timer_id_ = kInvalidTimerId;
            return;
        }
    }

    int64_t start_ts = get_micros();
    io::CleanTrackableGcTrash();
    LOG(INFO) << "[gc] clean trackable gc trash, cost: "
        << (get_micros() - start_ts) / 1000 << " ms";

    MutexLock lock(&mutex_);
    ScheduleGcTrashClean();
}

void MasterImpl::ScheduleGcTrashClean() {
    mutex_.AssertHeld();
    VLOG(10) << "[gc] ScheduleGcTrashClean";
    ThreadPool::Task task =
        std::bind(&MasterImpl::DoGcTrashClean, this);
    gc_timer_id_ = thread_pool_->DelayTask(
        FLAGS_tera_master_gc_trash_clean_period_s * 1000, task);
}

void MasterImpl::EnableGcTrashCleanTimer() {
    if (!FLAGS_tera_master_gc_trash_enabled) {
        return;
    }

    MutexLock lock(&mutex_);
    if (gc_trash_clean_timer_id_ == kInvalidTimerId) {
        ScheduleGcTrashClean();
    }
    gc_trash_clean_enabled_ = true;
}

void MasterImpl::DisableGcTrashCleanTimer() {
    if (!FLAGS_tera_master_gc_trash_enabled) {
        return;
    }

    MutexLock lock(&mutex_);
    if (gc_trash_clean_timer_id_ != kInvalidTimerId) {
        bool non_block = true;
        if (thread_pool_->CancelTask(gc_timer_id_, non_block)) {
            gc_trash_clean_timer_id_ = kInvalidTimerId;
        }
    }
    gc_trash_clean_enabled_ = false;
}

void MasterImpl::DoAvailableCheck() {
    MutexLock lock(&mutex_);
    if (FLAGS_tera_master_availability_check_enabled) {
        tablet_availability_->LogAvailability();
    }
    ScheduleAvailableCheck();
}

void MasterImpl::ScheduleAvailableCheck() {
    mutex_.AssertHeld();
    ThreadPool::Task task =
        std::bind(&MasterImpl::DoAvailableCheck, this);
    thread_pool_->DelayTask(
        FLAGS_tera_master_availability_check_period * 1000, task);
}

void MasterImpl::EnableAvailabilityCheck() {
    MutexLock lock(&mutex_);
    ScheduleAvailableCheck();
}

void MasterImpl::DisableTabletNodeGcTimer() {
    MutexLock lock(&mutex_);
    if (gc_timer_id_ != kInvalidTimerId) {
        bool non_block = true;
        if (thread_pool_->CancelTask(gc_timer_id_, non_block)) {
            gc_timer_id_ = kInvalidTimerId;
        }
    }
    gc_enabled_ = false;
}

void MasterImpl::DoTabletNodeGc() {
    {
        MutexLock lock(&mutex_);
        if (!gc_enabled_) {
            gc_timer_id_ = kInvalidTimerId;
            return;
        }
    }

    bool need_gc = true;
    if (FLAGS_tera_master_gc_strategy == "trackable") {
        std::vector<TablePtr> table_list;
        tablet_manager_->ShowTable(&table_list, NULL);
        for (uint32_t i = 0; i < table_list.size(); ++i) {
            table_list[i]->TryCollectInheritedFile();
        }
    } else {
        need_gc = gc_strategy_->PreQuery();
    }

    MutexLock lock(&mutex_);
    if (!need_gc) {
        if (gc_enabled_) {
            ScheduleTabletNodeGc();
        } else {
            gc_timer_id_ = kInvalidTimerId;
        }
        return;
    }
    gc_query_enable_ = true;
}

void MasterImpl::DoTabletNodeGcPhase2() {
    if (FLAGS_tera_master_gc_strategy == "trackable") {
        std::vector<TablePtr> table_list;
        tablet_manager_->ShowTable(&table_list, NULL);
        for (uint32_t i = 0; i < table_list.size(); ++i) {
            table_list[i]->CleanObsoleteFile();
        }
    } else {
        gc_strategy_->PostQuery();
    }

    LOG(INFO) << "[gc] try clean trash dir.";
    int64_t start = get_micros();
    io::CleanTrashDir();
    int64_t cost = (get_micros() - start) / 1000;
    LOG(INFO) << "[gc] clean trash dir done, cost: " << cost << "ms.";

    MutexLock lock(&mutex_);
    if (gc_enabled_) {
        ScheduleTabletNodeGc();
    } else {
        gc_timer_id_ = kInvalidTimerId;
    }
}

void MasterImpl::RefreshTableCounter() {
    int64_t start = get_micros();
    std::vector<TablePtr> table_list;
    tablet_manager_->ShowTable(&table_list, NULL);
    for (uint32_t i = 0; i < table_list.size(); ++i) {
        table_list[i]->RefreshCounter();
    }

    // Set refresh interval as  query-interval / 2, because each table counter
    // changed after query callback reached.
    ThreadPool::Task task = std::bind(&MasterImpl::RefreshTableCounter, this);
    thread_pool_->DelayTask(FLAGS_tera_master_query_tabletnode_period / 2, task);
    LOG(INFO) << "RefreshTableCounter, cost: "
        << ((get_micros() - start) / 1000) << "ms.";
}

std::string MasterImpl::ProfilingLog() {
    return "[main : " + thread_pool_->ProfilingLog() + "] [query : "
        + query_thread_pool_->ProfilingLog() + "]";
}

bool TryLoadTablet(TabletPtr tablet, TabletNodePtr node) {
    if (!tablet->LockTransition()) {
        LOG(WARNING) << "tablet: " << tablet->GetPath() << "is in transition, giveup this load try";
        return false;
    }
    std::shared_ptr<Procedure> load(new LoadTabletProcedure(tablet, node, MasterEnv().GetThreadPool().get()));
    if (MasterEnv().GetExecutor()->AddProcedure(load) == 0) {
        LOG(WARNING) << "add to procedure_executor fail, may duplicated procid: " << load->ProcId();
        tablet->UnlockTransition();
        return false;
    }
    return true;
}

bool TryUnloadTablet(TabletPtr tablet) {
    if (!tablet->LockTransition()) {
        LOG(WARNING) << "tablet: " << tablet->GetPath() << "is in transition, giveup this unload try";
        return false;
    }
    std::shared_ptr<Procedure> unload(new UnloadTabletProcedure(tablet, MasterEnv().GetThreadPool().get(), false));
    if (MasterEnv().GetExecutor()->AddProcedure(unload) == 0) {
        LOG(WARNING) << "add to procedure_executor fail, may duplicated procid: " << unload->ProcId();
        tablet->UnlockTransition();
        return false;
    }
    return true;
}

bool TryMoveTablet(TabletPtr tablet, TabletNodePtr node) {
    if (!tablet->LockTransition()) {
        LOG(WARNING) << "tablet: " << tablet->GetPath() << "is in transition, giveup this move try";
        return false;
    }
    std::shared_ptr<Procedure> move(new MoveTabletProcedure(tablet, node, MasterEnv().GetThreadPool().get()));
    if (MasterEnv().GetExecutor()->AddProcedure(move) == 0) {
        LOG(WARNING) << "add to procedure_executor fail, may duplicated procid: " << move->ProcId();
        tablet->UnlockTransition();
        return false;
    }
    return true;
}

bool TryMergeTablet(TabletPtr tablet) {
    TabletPtr peer;
    if (!MasterEnv().GetTabletManager()->PickMergeTablet(tablet, &peer)) {
        LOG(WARNING) << "merge abort, cannot get proper merge peer, tablet: " << tablet;
        return false;
    }
    if (!tablet->LockTransition()) {
        LOG(WARNING) << "tablet: " << tablet->GetPath() << "is in transition, giveup this merge try";
        return false;
    }
    if (!peer->LockTransition()) {
        tablet->UnlockTransition();
        LOG(WARNING) << "merge peer is in transition, give up this merge try: " << peer;
        return false;
    }
    std::shared_ptr<Procedure> merge(new MergeTabletProcedure(tablet, peer, MasterEnv().GetThreadPool().get()));
    if(MasterEnv().GetExecutor()->AddProcedure(merge) == 0) {
        LOG(WARNING) << "add to procedure_executor fail, may duplicated procid: " << merge->ProcId();
        tablet->UnlockTransition();
        peer->UnlockTransition();
        return false;
    }
    return true;
}

bool TrySplitTablet(TabletPtr tablet, std::string split_key) {
    if (!tablet->LockTransition()) {
        LOG(WARNING) << "tablet: " << tablet->GetPath() << "is in transition, giveup this split try";
        return false;
    }
    std::shared_ptr<Procedure> split(new SplitTabletProcedure(tablet, split_key, MasterEnv().GetThreadPool().get()));
    if(MasterEnv().GetExecutor()->AddProcedure(split) == 0) {
        LOG(WARNING) << "add to procedure_executor fail, may duplicated procid: " << split->ProcId();
        tablet->UnlockTransition();
        return false;
    }
    return true;
}

} // namespace master
} // namespace tera
