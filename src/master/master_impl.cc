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
#include "utils/config_utils.h"
#include "utils/schema_utils.h"
#include "utils/string_util.h"
#include "utils/timer.h"
#include "utils/utils_cmd.h"

DECLARE_string(tera_master_port);
DECLARE_bool(tera_master_meta_recovery_enabled);
DECLARE_string(tera_master_meta_recovery_file);

DECLARE_bool(tera_master_cache_check_enabled);
DECLARE_int32(tera_master_cache_release_period);
DECLARE_int32(tera_master_cache_keep_min);

DECLARE_int32(tera_master_impl_thread_max_num);
DECLARE_int32(tera_master_impl_query_thread_num);
DECLARE_int32(tera_master_impl_retry_times);

DECLARE_int32(tera_master_common_retry_period);
DECLARE_int32(tera_master_query_tabletnode_period);

DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_master_meta_table_path);
DECLARE_int32(tera_master_meta_retry_times);

DECLARE_bool(tera_zk_enabled);

DECLARE_double(tera_master_workload_split_threshold);
DECLARE_int64(tera_master_split_tablet_size);
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
DECLARE_int32(tera_master_split_rpc_timeout);
DECLARE_int32(tera_master_tabletnode_timeout);
DECLARE_bool(tera_master_move_tablet_enabled);
DECLARE_int32(tera_master_load_slow_retry_times);
DECLARE_int32(tera_master_max_move_concurrency);

DECLARE_int32(tera_max_pre_assign_tablet_num);
DECLARE_int64(tera_tablet_write_block_size);

DECLARE_bool(tera_delete_obsolete_tabledir_enabled);

DECLARE_string(tera_master_stat_table_name);
DECLARE_int64(tera_master_stat_table_ttl);
DECLARE_int64(tera_master_stat_table_interval);
DECLARE_bool(tera_master_stat_table_enabled);
DECLARE_int64(tera_master_stat_table_splitsize);

DECLARE_int32(tera_master_gc_period);

DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_string(tera_leveldb_env_type);

DECLARE_string(tera_zk_root_path);
DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_local_addr);
DECLARE_bool(tera_ins_enabled);

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

using namespace std::placeholders;

namespace tera {
namespace master {

MasterImpl::MasterImpl()
    : status_(kNotInited), restored_(false),
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
      thread_pool_(new ThreadPool(FLAGS_tera_master_impl_thread_max_num)),
      is_stat_table_(false),
      stat_table_(NULL),
      gc_enabled_(false),
      gc_timer_id_(kInvalidTimerId),
      gc_query_enable_(false),
      tablet_availability_(new TabletAvailability(tablet_manager_)) {
    if (FLAGS_tera_master_cache_check_enabled) {
        EnableReleaseCacheTimer();
    }
    if (FLAGS_tera_local_addr == "") {
        local_addr_ = utils::GetLocalHostName()+ ":" + FLAGS_tera_master_port;
    } else {
        local_addr_ = FLAGS_tera_local_addr + ":" + FLAGS_tera_master_port;
    }
    tabletnode::TabletNodeClient::SetThreadPool(thread_pool_.get());

    if (FLAGS_tera_leveldb_env_type != "local") {
        io::InitDfsEnv();
    }

    if (FLAGS_tera_master_gc_strategy == "default") {
        LOG(INFO) << "[gc] gc strategy is BatchGcStrategy";
        gc_strategy_ = std::shared_ptr<GcStrategy>(new BatchGcStrategy(tablet_manager_));
    } else if (FLAGS_tera_master_gc_strategy == "incremental") {
        LOG(INFO) << "[gc] gc strategy is IncrementalGcStrategy";
        gc_strategy_ = std::shared_ptr<GcStrategy>(new IncrementalGcStrategy(tablet_manager_));
    } else {
        LOG(ERROR) << "Unknown gc strategy";
    }
}

MasterImpl::~MasterImpl() {
    LOG(INFO) << "dest impl";
    delete stat_table_;
}

bool MasterImpl::Init() {
    if (FLAGS_tera_zk_enabled) {
        zk_adapter_.reset(new MasterZkAdapter(this, local_addr_));
    } else if (FLAGS_tera_ins_enabled) {
        LOG(INFO) << "ins mode" ;
        zk_adapter_.reset(new InsMasterZkAdapter(this, local_addr_));
    } else {
        LOG(INFO) << "fake zk mode!";
        zk_adapter_.reset(new FakeMasterZkAdapter(this, local_addr_));
    }

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

    std::string meta_tablet_addr;
    if (!RestoreMetaTablet(tablet_list, &meta_tablet_addr)) {
        SetMasterStatus(kOnWait);
        return false;
    }

    SetMasterStatus(kIsReadonly);

    user_manager_->SetupRootUser();
    tablet_manager_->FindTablet(FLAGS_tera_master_meta_table_name, "", &meta_tablet_);
    zk_adapter_->UpdateRootTabletNode(meta_tablet_addr);

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

bool MasterImpl::RestoreMetaTablet(const std::vector<TabletMeta>& tablet_list,
                                   std::string* meta_tablet_addr) {
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

    if (loaded && !loaded_twice) {
        meta_tablet_addr->assign(meta_tablet_meta.server_addr());
    } else if (!LoadMetaTablet(meta_tablet_addr)) {
        return false;
    }

    // meta table has been loaded up by now
    if (FLAGS_tera_master_meta_recovery_enabled) {
        const std::string& filename = FLAGS_tera_master_meta_recovery_file;
        while (!LoadMetaTableFromFile(filename)) {
            LOG(ERROR) << kSms << "fail to recovery meta table from backup";
            ThisThread::Sleep(60 * 1000);
        }
        TabletPtr meta_tablet;
        tablet_manager_->FindTablet(FLAGS_tera_master_meta_table_name, "",
                                     &meta_tablet);
        meta_tablet->SetAddrAndStatus(*meta_tablet_addr, kTableReady);

        while (!tablet_manager_->ClearMetaTable(*meta_tablet_addr)
               || !tablet_manager_->DumpMetaTable(*meta_tablet_addr)) {
            TryKickTabletNode(*meta_tablet_addr);
            if (!LoadMetaTablet(meta_tablet_addr)) {
                return false;
            }
            meta_tablet->SetAddr(*meta_tablet_addr);
        }
        return true;
    }

    StatusCode status = kTabletNodeOk;
    while (!LoadMetaTable(*meta_tablet_addr, &status)) {
        TryKickTabletNode(*meta_tablet_addr);
        if (!LoadMetaTablet(meta_tablet_addr)) {
            return false;
        }
    }
    return true;
}

void MasterImpl::RestoreUserTablet(const std::vector<TabletMeta>& report_meta_list) {
    std::vector<TabletMeta>::const_iterator meta_it = report_meta_list.begin();
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
        CompactStatus compact_status = meta.compact_status();

        TabletPtr tablet;
        if (!tablet_manager_->FindTablet(table_name, key_start, &tablet)
            || !tablet->Verify(table_name, key_start, key_end, path, server_addr)) {
            LOG(INFO) << "unload unexpected table: " << path << ", server: "
                << server_addr;
            TabletMeta unknown_meta = meta;
            unknown_meta.set_status(kTableUnLoading);
            TabletPtr unknown_tablet(new Tablet(unknown_meta));
            UnloadClosure done =
                std::bind(&MasterImpl::UnloadTabletCallback, this, unknown_tablet,
                          FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);
            UnloadTabletAsync(unknown_tablet, done);
        } else {
            tablet->SetStatus(kTableReady);
            tablet->UpdateSize(meta);
            tablet->SetCompactStatus(compact_status);
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
        const std::string& server_addr = tablet->GetServerAddr();
        if (tablet->GetStatus() == kTableReady) {
            VLOG(8) << "READY Tablet, " << tablet;
            continue;
        }
        tablet_availability_->AddNotReadyTablet(tablet->GetPath());
        CHECK(tablet->GetStatus() == kTableNotInit);

        TabletNodePtr node;
        if (server_addr.empty()) {
            tablet->SetStatus(kTableOffLine);
            VLOG(8) << "OFFLINE Tablet with empty addr, " << tablet;
        } else if (!tabletnode_manager_->FindTabletNode(server_addr, &node)) {
            tablet->SetStatus(kTableOffLine);
            VLOG(8) << "OFFLINE Tablet of Dead TS, " << tablet;
        } else if (node->state_ == kReady) {
            tablet->SetStatus(kTableOffLine);
            VLOG(8) << "OFFLINE Tablet of Alive TS, " << tablet;
            TryLoadTablet(tablet, server_addr);
        } else {
            // Ts not response, we count its tablets as Ready and wait for it to be kicked.
            tablet->SetStatus(kTableReady);
            VLOG(8) << "UNKNOWN Tablet of No-Response TS, " << tablet;
        }
    }
}

void MasterImpl::LoadAllOffLineTablet() {
    VLOG(5) << "LoadAllOffLineTablet()";
    std::vector<TabletPtr> all_tablet_list;
    tablet_manager_->ShowTable(NULL, &all_tablet_list);
    std::vector<TabletPtr>::iterator it = all_tablet_list.begin();
    for (; it != all_tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        const std::string& path = tablet->GetPath();
        if (tablet->GetStatus() == kTableOffLine) {
            LOG(INFO) << "load offline tablet: " << path;
            TryLoadTablet(tablet);
        }
    }
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

    while (tabletnode_manager_->ScheduleTabletNode(size_scheduler_.get(), "",
                                                    false, server_addr)) {
        meta.set_server_addr(*server_addr);
        StatusCode status = kTabletNodeOk;
        if (LoadTabletSync(meta, schema, &status)) {
            LOG(INFO) << "load meta tablet on node: " << *server_addr;
            return true;
        }
        LOG(ERROR) << "fail to load meta tablet on node: " << *server_addr
            << ", status: " << StatusCodeToString(status);
        TryKickTabletNode(*server_addr);
        // ThisThread::Sleep(FLAGS_tera_master_common_retry_period);
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

template <typename Request>
bool MasterImpl::HasPermissionOnTable(const Request* request, TablePtr table) {
    if (!FLAGS_tera_acl_enabled
        || IsRootUser(request->user_token())
        || ((table->GetSchema().admin_group() == "") && (table->GetSchema().admin() == ""))
        || (request->has_user_token()
            && CheckUserPermissionOnTable(request->user_token(), table))) {
        return true;

    }
    return false;
}

template <typename Request, typename Response, typename Callback>
bool MasterImpl::HasPermissionOrReturn(const Request* request, Response* response,
                                       Callback* done, TablePtr table, const char* operate) {
    // check permission
    if (HasPermissionOnTable(request, table)) {
        return true;
    } else {
        std::string token = request->has_user_token() ? request->user_token() : "";
        LOG(INFO) << "[acl] " << user_manager_->TokenToUserName(token)
                  << ":" << token << " fail to " << operate;
        response->set_sequence_id(request->sequence_id());
        response->set_status(kNotPermission);
        done->Run();
        return false;
    }
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
    tabletnode::TabletNodeClient meta_node_client(meta_tablet_addr);
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
            TabletPtr meta_tablet;
            TableSchema schema;
            schema.set_kv_only(true);
            LocalityGroupSchema* lg = schema.add_locality_groups();
            schema.set_name(FLAGS_tera_master_meta_table_name);
            lg->set_name("lg_meta");
            lg->set_compress_type(false);
            lg->set_store_type(MemoryStore);
            tablet_manager_->AddTablet(FLAGS_tera_master_meta_table_name, "", "",
                      FLAGS_tera_master_meta_table_path, meta_tablet_addr,
                      schema, kTableNotInit, 0, &meta_tablet);
            meta_tablet->SetStatus(kTableReady);
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
                FillAlias(record.key(), record.value());
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

void MasterImpl::FillAlias(const std::string& key, const std::string& value) {
    TableMeta meta;
    ParseMetaTableKeyValue(key, value, &meta);
    if (!meta.schema().alias().empty()) {
        MutexLock locker(&alias_mutex_);
        alias_[meta.schema().alias()] = meta.schema().name();
        LOG(INFO) << "table alias:" << meta.schema().alias()
                  << " -> " << meta.schema().name();
    }
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
            LOG(INFO) << "load meta table success, " << count << " records";
            TabletPtr meta_tablet;
            TableSchema schema;
            LocalityGroupSchema* lg = schema.add_locality_groups();
            schema.set_name(FLAGS_tera_master_meta_table_name);
            lg->set_name("lg_meta");
            lg->set_compress_type(false);
            lg->set_store_type(MemoryStore);
            tablet_manager_->AddTablet(FLAGS_tera_master_meta_table_name, "", "",
                      FLAGS_tera_master_meta_table_path, "",
                      schema, kTableNotInit, 0, &meta_tablet);
            return true;
        }

        char first_key_char = key[0];
        if (first_key_char == '~') {
            user_manager_->LoadUserMeta(key, value);
        } else if (first_key_char == '@') {
            tablet_manager_->LoadTableMeta(key, value);
            FillAlias(key, value);
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

    {
        TablePtr table;
        if (tablet_manager_->FindTable(request->table_name(), &table)) {
            LOG(ERROR) << "Fail to create table: " << request->table_name()
                << ", table already exist";
            response->set_status(kTableExist);
            done->Run();
            return;
        }
        if (tablet_manager_->FindTable(request->schema().alias(), &table)) {
            LOG(ERROR) << "Fail to create table: " << request->schema().alias()
                << ", table already exist";
            response->set_status(kTableExist);
            done->Run();
            return;
        }
        if (FLAGS_tera_acl_enabled
            && !IsRootUser(request->user_token())
            && FLAGS_tera_only_root_create_table) {
                response->set_sequence_id(request->sequence_id());
                response->set_status(kNotPermission);
                done->Run();
                return;
        }
        if (!request->schema().alias().empty()) {
            bool alias_exist = false;
            {
                MutexLock locker(&alias_mutex_);
                if (alias_.find(request->schema().alias()) != alias_.end()) {
                    alias_exist =  true;
                }
            }
            if (alias_exist) {
                LOG(ERROR) << "Fail to create table: " << request->table_name()
                << ", table already exist, alias:" << request->schema().alias() ;
                response->set_status(kTableExist);
                done->Run();
                return;
            }
        }
    }

    // try clean env, if there is a dir same as table_name, delete it first
    if (!io::MoveEnvDirToTrash(request->table_name())) {
        LOG(ERROR) << "Fail to create table: " << request->table_name()
            << ", cannot move old table dir to trash";
        response->set_status(kTableExist);
        done->Run();
        return;
    }

    int32_t tablet_num = request->delimiters_size() + 1;
    bool delivalid = true;
    for (int32_t i = 1; i < tablet_num - 1; i++) {
        // TODO: Use user defined comparator
        if (request->delimiters(i) <= request->delimiters(i-1)) {
            delivalid = false;
            break;
        }
    }
    if (tablet_num > FLAGS_tera_max_pre_assign_tablet_num || !delivalid
        || request->schema().locality_groups_size() < 1) {
        if (tablet_num > FLAGS_tera_max_pre_assign_tablet_num) {
            LOG(WARNING) << "Too many pre-create tablets " << tablet_num;
        } else if (!delivalid) {
            LOG(WARNING) << "Invalid delimiters for " << request->table_name();
        } else {
            LOG(WARNING) << "No LocalityGroupSchema for " << request->table_name();
        }
        response->set_status(kInvalidArgument);
        done->Run();
        return;
    }

    std::vector<TabletPtr> tablets;
    const std::string& table_name = request->table_name();
    StatusCode status = kMasterOk;
    int32_t add_num = 0;
    tablets.resize(tablet_num);

    for (int32_t i = 1; i <= tablet_num; i++) {
        std::string path = leveldb::GetTabletPathFromNum(request->table_name(), i);
        const std::string& start_key = (i == 1) ? "" : request->delimiters(i-2);
        const std::string& end_key = (i == tablet_num) ? "" : request->delimiters(i-1);

        if (!tablet_manager_->AddTablet(table_name, start_key, end_key, path,
                                         "", request->schema(), kTableNotInit,
                                         FLAGS_tera_tablet_write_block_size * 1024,
                                         &tablets[i-1], &status)) {
            LOG(ERROR) << "Add table fail: " << table_name;
            break;
        }
        add_num++;
    }

    if (add_num != tablet_num) {
        tablet_manager_->DeleteTable(table_name, NULL);
        response->set_status(status);
        done->Run();
        return;
    }
    const LocalityGroupSchema& lg0 = request->schema().locality_groups(0);
    LOG(INFO) << "New table is created: " << request->table_name()
        << ", store_medium: " << lg0.store_type()
        << ", compress: " << lg0.compress_type()
        << ", raw_key: " << request->schema().raw_key()
        << ", has " << tablet_num << " tablets, schema: "
        << request->schema().ShortDebugString();
    // write meta tablet
    TablePtr table = tablets[0]->GetTable();
    std::string table_alias = table->GetSchema().alias();
    if (!table_alias.empty()) {
        MutexLock locker(&alias_mutex_);
        alias_[table_alias] = table_name;
    }
    WriteClosure closure =
        std::bind(&MasterImpl::AddMetaCallback, this, table, tablets,
                   FLAGS_tera_master_meta_retry_times, request, response, done, _1, _2, _3, _4);
    BatchWriteMetaTableAsync(table, tablets, false, closure);
    return;
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
    if (!tablet_manager_->FindTable(request->table_name(), &table)) {
        LOG(ERROR) << "fail to delete table: " << request->table_name()
            << ", table not exist";
        response->set_status(kTableNotFound);
        done->Run();
        return;
    }
    if (!HasPermissionOrReturn(request, response, done, table, "delete table")) {
        return;
    }

    TableStatus old_status;
    if (!table->SetStatus(kTableDeleting, &old_status)) {
        LOG(ERROR) << "fail to delete table: " << request->table_name()
            << ", table status: " << StatusCodeToString(old_status);
        response->set_status(static_cast<StatusCode>(old_status));
        done->Run();
        return;
    }

    std::vector<TabletPtr> tablets;
    table->GetTablet(&tablets);

    // check if all tablet disable
    for (uint32_t i = 0; i < tablets.size(); ++i) {
        TabletPtr tablet = tablets[i];
        if (tablet->GetStatus() != kTabletDisable
            && tablet->GetStatus() != kTableOffLine) {
            CHECK(table->SetStatus(old_status));
            LOG(ERROR) << "fail to delete table: " << request->table_name()
                << ", tablet status: " << StatusCodeToString(tablet->GetStatus());
            response->set_status(kTabletReady);
            done->Run();
            return;
        }
    }

    WriteClosure closure =
        std::bind(&MasterImpl::DeleteTableCallback, this, table, tablets,
                   FLAGS_tera_master_impl_retry_times, response, done, _1, _2, _3, _4);
    BatchWriteMetaTableAsync(table, tablets, true, closure);
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
    if (!tablet_manager_->FindTable(request->table_name(), &table)) {
        LOG(ERROR) << "fail to disable table: " << request->table_name()
            << ", table not exist";
        response->set_status(kTableNotFound);
        done->Run();
        return;
    }
    if (!HasPermissionOrReturn(request, response, done, table, "disable table")) {
        return;
    }

    TableStatus old_status;
    if (!table->SetStatus(kTableDisable, &old_status)) {
        LOG(ERROR) << "fail to disable table: " << request->table_name()
            << ", table status: " << StatusCodeToString(old_status);
        response->set_status(static_cast<StatusCode>(old_status));
        done->Run();
        return;
    }

    WriteClosure closure =
        std::bind(&MasterImpl::UpdateTableRecordForDisableCallback, this, table,
                   FLAGS_tera_master_meta_retry_times, response, done, _1, _2, _3, _4);
    BatchWriteMetaTableAsync(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2),
                             false, closure);
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
    if (!tablet_manager_->FindTable(request->table_name(), &table)) {
        LOG(ERROR) << "fail to enable table: " << request->table_name()
            << ", table not exist";
        response->set_status(kTableNotFound);
        done->Run();
        return;
    }
    if (!HasPermissionOrReturn(request, response, done, table, "enable table")) {
        return;
    }

    TableStatus old_status;
    if (!table->SetStatus(kTableEnable, &old_status)) {
        LOG(ERROR) << "fail to enable table: " << request->table_name()
            << ", table status: " << StatusCodeToString(old_status);
        response->set_status(static_cast<StatusCode>(old_status));
        done->Run();
        return;
    }

    // write meta tablet
    WriteClosure closure =
        std::bind(&MasterImpl::UpdateTableRecordForEnableCallback, this, table,
                   FLAGS_tera_master_meta_retry_times, response, done, _1, _2, _3, _4);
    BatchWriteMetaTableAsync(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2),
                             false, closure);
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
    if (!HasPermissionOrReturn(request, response, done, table, "update-check table")) {
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
    if (!tablet_manager_->FindTable(request->table_name(), &table)) {
        LOG(ERROR) << "Fail to update table: " << request->table_name()
            << ", table not exist";
        response->set_status(kTableNotExist);
        done->Run();
        return;
    }
    if (!HasPermissionOrReturn(request, response, done, table, "update table")) {
        return;
    }

    if (request->schema().locality_groups_size() < 1) {
        LOG(WARNING) << "No LocalityGroupSchema for " << request->table_name();
        response->set_status(kInvalidArgument);
        done->Run();
        return;
    }
    if (!table->PrepareUpdate(request->schema())) {
        // another schema-update is doing...
        LOG(INFO) << "[update] no concurrent schema-update, table:" << table;
        response->set_status(kInvalidArgument);
        done->Run();
        return;
    }

    // write meta tablet
    WriteClosure closure =
        std::bind(&MasterImpl::UpdateTableRecordForUpdateCallback, this, table,
                   FLAGS_tera_master_meta_retry_times, response, done, _1, _2, _3, _4);
    BatchWriteMetaTableAsync(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2),
                             false, closure);
    return;
}

void MasterImpl::CompactTable(const CompactTableRequest* request,
                              CompactTableResponse* response,
                              google::protobuf::Closure* done) {
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
    } else {
        response->set_status(kInvalidArgument);
    }
}

void MasterImpl::AddUserInfoToMetaCallback(UserPtr user_ptr,
                                           int32_t retry_times,
                                           const OperateUserRequest* rpc_request,
                                           OperateUserResponse* rpc_response,
                                           google::protobuf::Closure* rpc_done,
                                           WriteTabletRequest* request,
                                           WriteTabletResponse* response,
                                           bool rpc_failed, int error_code) {
    StatusCode status = response->status();
    delete request;
    delete response;
    if (rpc_failed || status != kTabletNodeOk) {
        if (rpc_failed) {
            LOG(ERROR) << "[user-manager] fail to add to meta tablet: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", "
                << user_ptr->GetUserInfo().user_name() << "...";
        } else {
            LOG(ERROR) << "[user-manager] fail to add to meta tablet: "
                << StatusCodeToString(status) << ", " << user_ptr->GetUserInfo().user_name() << "...";
        }
        if (retry_times <= 0) {
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::AddUserInfoToMetaCallback, this, user_ptr,
                           retry_times - 1, rpc_request, rpc_response, rpc_done, _1, _2, _3, _4);
            SuspendMetaOperation(std::bind(&User::ToMetaTableKeyValue, user_ptr, _1, _2),
                                 rpc_request->op_type() == kDeleteUser, done);
        }
        return;
    }
    rpc_response->set_status(kMasterOk);
    rpc_done->Run();
    LOG(INFO) << "[user-manager] write user info to meta table done: "
        << StatusCodeToString(status) << ", " << user_ptr->GetUserInfo().user_name();
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
    WriteClosure closure =
        std::bind(&MasterImpl::AddUserInfoToMetaCallback, this, user_ptr,
                   FLAGS_tera_master_meta_retry_times, request, response, done, _1, _2, _3, _4);
    BatchWriteMetaTableAsync(std::bind(&User::ToMetaTableKeyValue, user_ptr, _1, _2),
                             is_delete, closure);
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

void MasterImpl::TabletCmdCtrl(const CmdCtrlRequest* request,
                               CmdCtrlResponse* response) {
    if (request->arg_list_size() < 2) {
        response->set_status(kInvalidArgument);
        return;
    }

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

    if (request->arg_list(0) == "reload") {
        std::string current_server_addr = tablet->GetServerAddr();
        TryMoveTablet(tablet,
                      current_server_addr,
                      true);  // force to unload and load tablet even it on the same ts

    } else if (request->arg_list(0) == "move") {
        if (request->arg_list_size() > 3) {
            response->set_status(kInvalidArgument);
            return;
        }
        std::string expect_server_addr;
        if (request->arg_list_size() == 3) {
            expect_server_addr = request->arg_list(2);
        }
        TryMoveTablet(tablet, expect_server_addr);
        response->set_status(kMasterOk);
    } else if (request->arg_list(0) == "split") {
        if (request->arg_list_size() > 3) {
            response->set_status(kInvalidArgument);
            return;
        }
        std::string split_key;
        if (request->arg_list_size() == 3) {
            split_key = request->arg_list(2);
            LOG(INFO) << "User specified split key: " << split_key;
        }
        TrySplitTablet(tablet, split_key);
        response->set_status(kMasterOk);
    } else if (request->arg_list(0) == "merge") {
        if (request->arg_list_size() > 3) {
            response->set_status(kInvalidArgument);
            return;
        }
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
    if (restored_ && meta_tablet_->GetStatus() == kTableReady) {
        *addr = meta_tablet_->GetServerAddr();
        return true;
    }
    return false;
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

    if (FLAGS_tera_master_stat_table_enabled && !is_stat_table_) {
        CreateStatTable();
        ErrorCode err;
        const std::string& tablename = FLAGS_tera_master_stat_table_name;
        stat_table_ = new TableImpl(tablename, thread_pool_.get(), NULL);
        FLAGS_tera_sdk_perf_counter_log_interval = 60;
        if (stat_table_->OpenInternal(&err)) {
            is_stat_table_ = true;
        } else {
            LOG(ERROR) << "fail to open stat_table.";
            delete stat_table_;
            stat_table_ = NULL;
        }
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
        if (tablet->GetStatus() != kTableReady
            || tablet->GetTableName() == FLAGS_tera_master_meta_table_name) {
            continue;
        }
        double write_workload = tablet->GetCounter().write_workload();
        int64_t split_size = FLAGS_tera_master_split_tablet_size;
        if (tablet->GetSchema().has_split_size() && tablet->GetSchema().split_size() > 0) {
            split_size = tablet->GetSchema().split_size();
        }
        if (write_workload > FLAGS_tera_master_workload_split_threshold) {
            split_size /= 2;
            VLOG(6) << tablet->GetPath() << " write_workload too large, split it by size: "
                << split_size;
        }
        int64_t merge_size = FLAGS_tera_master_merge_tablet_size;
        if (tablet->GetSchema().has_merge_size() && tablet->GetSchema().merge_size() > 0) {
            merge_size = tablet->GetSchema().merge_size();
        }
        if (tablet->GetDataSize() < 0) {
            // tablet size is error, skip it
            continue;
        } else if (tablet->GetDataSize() > (split_size << 20)) {
            TrySplitTablet(tablet);
            any_tablet_split = true;
            continue;
        } else if (tablet->GetDataSize() < (merge_size << 20)) {
            if (write_workload < 1) {
                TryMergeTablet(tablet);
            } else {
                VLOG(6) << "[merge] skip high workload tablet: "
                    << tablet->GetPath() << ", write_workload " << write_workload;
            }
            continue;
        }
        if (tablet->GetStatus() == kTableReady) {
            tablet_candidates.push_back(tablet);
        }
    }

    // if any tablet is splitting, no need to move tablet
    if (!FLAGS_tera_master_move_tablet_enabled || any_tablet_split) {
        return false;
    }

    TabletNodePtr dst_tabletnode;
    size_t tablet_index = 0;
    if (scheduler->MayMoveOut(tabletnode, table_name)
            && tabletnode_manager_->ScheduleTabletNode(scheduler, table_name, true, &dst_tabletnode)
            && tabletnode_manager_->ShouldMoveData(scheduler, table_name, tabletnode,
                                                    dst_tabletnode, tablet_candidates,
                                                    &tablet_index)) {
        TryMoveTablet(tablet_candidates[tablet_index], dst_tabletnode->GetAddr());
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
        DeleteTabletNode(old_addr);
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
    if (FLAGS_tera_master_tabletnode_timeout > 0) {
        MutexLock lock(&tabletnode_timer_mutex_);
        std::map<std::string, int64_t>::iterator it =
            tabletnode_timer_id_map_.find(tabletnode_addr);
        if (it != tabletnode_timer_id_map_.end()) {
            uint64_t timer_id = it->second;
            thread_pool_->CancelTask(timer_id);
            tabletnode_timer_id_map_.erase(it);
        }
    }

    TabletNodePtr node = tabletnode_manager_->AddTabletNode(tabletnode_addr, tabletnode_uuid);

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
    NodeState old_state;
    node->SetState(kReady, &old_state);


    // If all tabletnodes restart in one zk callback,
    // master will not enter restore/wait state;
    // meta table must be scheduled to load from here.
    TabletPtr meta_tablet;
    if (tablet_manager_->FindTablet(FLAGS_tera_master_meta_table_name, "",
                                    &meta_tablet)
        && meta_tablet->GetStatus() == kTableOffLine) {
        LOG(INFO) << "try load meta tablet on new ts: " << tabletnode_addr;
        TryLoadTablet(meta_tablet);
    }

    // load offline tablets
    std::vector<TabletPtr> tablet_list;
    tablet_manager_->FindTablet(tabletnode_addr,
                                &tablet_list,
                                true);  // need disabled table/tablets
    std::vector<TabletPtr>::iterator it = tablet_list.begin();
    for (; it != tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->SetStatusIf(kTableOffLine, kTabletPending)) {
            ProcessOffLineTablet(tablet);
        }
        if (tablet->GetStatus() == kTableOffLine) {
            LOG(INFO) << "try load, " << tablet;
            TryLoadTablet(tablet, tabletnode_addr);
        }
    }

    TryLeaveSafeMode();
}

void MasterImpl::DeleteTabletNode(const std::string& tabletnode_addr) {
    tabletnode_manager_->DelTabletNode(tabletnode_addr);
    // possible status: running, readonly, wait.
    if (GetMasterStatus() == kOnWait) {
        return;
    }

    // move all tablets on the deleted tabletnode
    std::vector<TabletPtr> tablet_list;
    tablet_manager_->FindTablet(tabletnode_addr,
                                &tablet_list,
                                true);  // need disabled tables/tablets
    std::vector<TabletPtr>::iterator it;
    for (it = tablet_list.begin(); it != tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        tablet_availability_->AddNotReadyTablet(tablet->GetPath());

        if (FLAGS_tera_master_tabletnode_timeout > 0
            && tablet->GetTableName() != FLAGS_tera_master_meta_table_name) {
            tablet->SetStatusIf(kTabletPending, kTableReady);
        } else if (tablet->SetStatusIf(kTableOffLine, kTableReady)) {
            ProcessOffLineTablet(tablet);
        }

        if (tablet->GetStatus() == kTableUnLoadFail && tablet->GetMergeParam() != NULL) {
            MergeTabletUnloadCallback(tablet);
        }

        if (tablet->SetStatusIf(kTableOffLine, kTableLoadFail)
            || tablet->SetStatusIf(kTableOffLine, kTableUnLoadFail)) {
            ProcessOffLineTablet(tablet);
        }

        if (tablet->SetStatusIf(kTableOnSplit, kTableSplitFail)) {
            ScanClosure done =
                std::bind(&MasterImpl::ScanMetaCallbackForSplit, this, tablet, _1, _2, _3, _4);
            ScanMetaTableAsync(tablet->GetTableName(), tablet->GetKeyStart(),
                               tablet->GetKeyEnd(), done);
        }

        if (tablet->GetTableName() == FLAGS_tera_master_meta_table_name
            && tablet->GetStatus() == kTableOffLine) {
            LOG(INFO) << "try move meta tablet";
            TryLoadTablet(tablet);
        }
    }

    TryEnterSafeMode();

    if (FLAGS_tera_master_tabletnode_timeout > 0) {
        LOG(INFO) << "try move tablet " << FLAGS_tera_master_tabletnode_timeout
            << "(ms) later";
        MutexLock lock(&tabletnode_timer_mutex_);
        ThreadPool::Task task =
            std::bind(&MasterImpl::TryMovePendingTablets, this, tabletnode_addr);
        int64_t timer_id = thread_pool_->DelayTask(
            FLAGS_tera_master_tabletnode_timeout, task);
        tabletnode_timer_id_map_[tabletnode_addr] = timer_id;
    } else if (GetMasterStatus() == kIsRunning) {
        VLOG(5) << "MoveOffLineTablets: " << tabletnode_addr;
        MoveOffLineTablets(tablet_list);
    }
}

void MasterImpl::TryMovePendingTablets(std::string tabletnode_addr) {
    std::vector<TabletPtr> tablet_list;
    tablet_manager_->FindTablet(tabletnode_addr,
                                &tablet_list,
                                true);  // need disabled tables/tablets
    std::vector<TabletPtr>::const_iterator it;
    for (it = tablet_list.begin(); it != tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->SetStatusIf(kTableOffLine, kTabletPending)) {
            ProcessOffLineTablet(tablet);
        }
    }
    if (GetMasterStatus() == kIsRunning) {
        VLOG(5) << "MoveOffLineTablets: " << tabletnode_addr;
        MoveOffLineTablets(tablet_list);
    }
}

void MasterImpl::TryMovePendingTablet(TabletPtr tablet) {
    if (tablet->SetStatusIf(kTableOffLine, kTabletPending)) {
        ProcessOffLineTablet(tablet);
    }
    if (GetMasterStatus() == kIsRunning
        && tablet->GetStatus() == kTableOffLine) {
        LOG(INFO) << "try move pending tablet, " << tablet;
        TryLoadTablet(tablet);
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
    DisableQueryTabletNodeTimer();
    DisableTabletNodeGcTimer();
    DisableLoadBalance();
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

void MasterImpl::LoadAllOffLineTablets() {
    std::vector<TabletPtr> all_tablet_list;
    tablet_manager_->ShowTable(NULL, &all_tablet_list);

    std::vector<TabletPtr>::iterator it;
    for (it = all_tablet_list.begin(); it != all_tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->GetStatus() == kTableOffLine) {
            LOG(INFO) << "try load offline tablet, " << tablet;
            TryLoadTablet(tablet);
        }
    }
}

void MasterImpl::LoadAllDeadNodeTablets() {
    std::vector<TabletPtr> all_tablet_list;
    tablet_manager_->ShowTable(NULL, &all_tablet_list);

    std::vector<TabletPtr>::iterator it;
    for (it = all_tablet_list.begin(); it != all_tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->GetStatus() != kTableOffLine) {
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

void MasterImpl::MoveOffLineTablets(const std::vector<TabletPtr>& tablet_list) {
    std::vector<TabletPtr>::const_iterator it;
    for (it = tablet_list.begin(); it != tablet_list.end(); ++it) {
        TabletPtr tablet = *it;
        if (tablet->GetStatus() == kTableOffLine) {
            LOG(INFO) << "try move offline tablet, " << tablet;
            TryLoadTablet(tablet);
        }
    }
}

void MasterImpl::TryKickTabletNode(const std::string& tabletnode_addr) {
    if (!FLAGS_tera_master_kick_tabletnode_enabled) {
        LOG(INFO) << "will not kick tabletnode " << tabletnode_addr;
        return;
    }
    LOG(INFO) << "try kick tabletnode " << tabletnode_addr << " ...";
//    std::function<void>* callback =
//        std::bind(&MasterImpl::KickTabletNodeAsync, tabletnode_addr);
//    thread_pool_->AddTask(callback);

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

    tabletnode::TabletNodeClient node_client(meta.server_addr(),
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

void MasterImpl::LoadTabletAsync(TabletPtr tablet, LoadClosure done, uint64_t) {
    tabletnode::TabletNodeClient node_client(tablet->GetServerAddr(),
                                            FLAGS_tera_master_load_rpc_timeout);
    LoadTabletRequest* request = new LoadTabletRequest;
    LoadTabletResponse* response = new LoadTabletResponse;
    request->set_tablet_name(tablet->GetTableName());
    request->set_sequence_id(this_sequence_id_.Inc());
    request->mutable_key_range()->set_key_start(tablet->GetKeyStart());
    request->mutable_key_range()->set_key_end(tablet->GetKeyEnd());
    request->set_path(tablet->GetPath());
    request->mutable_schema()->CopyFrom(tablet->GetSchema());
    request->set_session_id(tablet->GetServerId());

    TablePtr table = tablet->GetTable();
    std::vector<uint64_t> snapshot_id;
    std::vector<uint64_t> snapshot_seq;
    table->ListSnapshot(&snapshot_id);
    tablet->ListSnapshot(&snapshot_seq);
    assert(snapshot_id.size() == snapshot_seq.size());
    for (uint32_t i = 0; i < snapshot_id.size(); ++i) {
        request->add_snapshots_id(snapshot_id[i]);
        request->add_snapshots_sequence(snapshot_seq[i]);
    }
    std::vector<std::string> rollback_names;
    std::vector<Rollback> rollbacks;
    table->ListRollback(&rollback_names);
    tablet->ListRollback(&rollbacks);
    assert(rollback_names.size() == rollbacks.size());
    for (uint32_t i = 0; i < rollbacks.size(); ++i) {
        request->add_rollbacks()->CopyFrom(rollbacks[i]);
    }

    TabletMeta meta;
    tablet->ToMeta(&meta);
    CHECK(meta.parent_tablets_size() <= 2)
        << "too many parents tablets: " << meta.parent_tablets_size();
    for (int32_t i = 0; i < meta.parent_tablets_size(); ++i) {
        request->add_parent_tablets(meta.parent_tablets(i));
    }

    LOG(INFO) << "LoadTabletAsync id: " << request->sequence_id() << ", "
        << tablet;
    node_client.LoadTablet(request, response, done);
}

void MasterImpl::LoadTabletCallback(TabletPtr tablet, int32_t retry,
                                    LoadTabletRequest* request,
                                    LoadTabletResponse* response, bool failed,
                                    int error_code) {
    CHECK(tablet->GetStatus() == kTableOnLoad);
    StatusCode status = response->status();
    delete request;
    delete response;
    const std::string& server_addr = tablet->GetServerAddr();

    // server down
    TabletNodePtr node;
    if (!tabletnode_manager_->FindTabletNode(server_addr, &node)) {
        LOG(ERROR) << "fail to load tablet: server down, " << tablet;
        tablet->SetStatusIf(kTableOffLine, kTableOnLoad);
        ProcessOffLineTablet(tablet);
        TryLoadTablet(tablet, server_addr);
        return;
    }

    // server restart
    if (node->uuid_ != tablet->GetServerId()) {
        LOG(ERROR) << "fail to load tablet: server restart, " << tablet;
        tablet->SetStatusIf(kTableOffLine, kTableOnLoad);
        ProcessOffLineTablet(tablet);
        TryLoadTablet(tablet, server_addr);
        return;
    }

    // success
    if (!failed && (status == kTabletNodeOk || status == kTabletReady)) {
        LOG(INFO) << "load tablet success, " << tablet;
        tablet->SetStatusIf(kTableReady, kTableOnLoad);
        tablet_availability_->EraseNotReadyTablet(tablet->GetPath());
        if (tablet->GetTableName() == FLAGS_tera_master_meta_table_name) {
            CHECK(tablet->GetPath() == FLAGS_tera_master_meta_table_path);
            zk_adapter_->UpdateRootTabletNode(server_addr);
            ResumeMetaOperation();
            return;
        }
        ProcessReadyTablet(tablet);

        // load next
        node->FinishLoad(tablet);
        TabletPtr next_tablet;
        while (node->LoadNextWaitTablet(&next_tablet)) {
            if (next_tablet->SetAddrAndStatusIf(server_addr, kTableOnLoad, kTableOffLine)) {
                next_tablet->SetServerId(node->uuid_);
                WriteClosure done =
                    std::bind(&MasterImpl::UpdateMetaForLoadCallback, this, next_tablet,
                              FLAGS_tera_master_meta_retry_times, _1, _2, _3, _4);
                BatchWriteMetaTableAsync(std::bind(&Tablet::ToMetaTableKeyValue, next_tablet, _1, _2),
                                         false, done);
                break;
            }
            node->FinishLoad(next_tablet);
        }
        return;
    }

    // fail
    if (failed) {
        LOG(WARNING) << "fail to load tablet: "
            << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << tablet;
    } else {
        LOG(WARNING) << "fail to load tablet: " << StatusCodeToString(status)
            << ", " << tablet;
    }

    // abort
    if (!failed && status == kTabletOnLoad) {
        // extend max retry times when tablet need repair
        if (retry > FLAGS_tera_master_load_slow_retry_times) {
            LOG(ERROR) << kSms << "abort LoadTablet: try unload, " << tablet;
            UnloadClosure done =
                std::bind(&MasterImpl::UnloadTabletCallback, this, tablet,
                           FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);
            UnloadTabletAsync(tablet, done);
            return;
        }
        if (retry > FLAGS_tera_master_impl_retry_times && retry % 10 == 0) {
            LOG(ERROR) << kSms << "slow load, retry: " << retry << ", " << tablet;
        }
    } else if (retry > FLAGS_tera_master_impl_retry_times) {
        LOG(ERROR) << kSms << "abort LoadTablet: try unload, " << tablet;
        UnloadClosure done =
            std::bind(&MasterImpl::UnloadTabletCallback, this, tablet,
                       FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);
        UnloadTabletAsync(tablet, done);
        return;
    }

    // retry
    ThreadPool::Task task =
        std::bind(&MasterImpl::RetryLoadTablet, this, tablet, retry + 1);
    thread_pool_->DelayTask(
        FLAGS_tera_master_control_tabletnode_retry_period, task);
}

bool MasterImpl::UnloadTabletSync(const std::string& table_name,
                                  const std::string& key_start,
                                  const std::string& key_end,
                                  const std::string& server_addr,
                                  StatusCode* status) {
    VLOG(5) << "UnloadTabletSync() for " << table_name << " ["
        << DebugString(key_start) << ", " << DebugString(key_end) << "]";
    tabletnode::TabletNodeClient node_client(server_addr,
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

void MasterImpl::UnloadTabletAsync(TabletPtr tablet, UnloadClosure done) {
    tabletnode::TabletNodeClient node_client(tablet->GetServerAddr(),
            FLAGS_tera_master_unload_rpc_timeout);
    UnloadTabletRequest* request = new UnloadTabletRequest;
    UnloadTabletResponse* response = new UnloadTabletResponse;
    request->set_sequence_id(this_sequence_id_.Inc());
    request->set_tablet_name(tablet->GetTableName());
    request->mutable_key_range()->set_key_start(tablet->GetKeyStart());
    request->mutable_key_range()->set_key_end(tablet->GetKeyEnd());
    LOG(INFO) << "UnloadTabletAsync id: " << request->sequence_id() << ", "
        << tablet;
    node_client.UnloadTablet(request, response, done);
}

void MasterImpl::UnloadTabletCallback(TabletPtr tablet, int32_t retry,
                                      UnloadTabletRequest* request,
                                      UnloadTabletResponse* response,
                                      bool failed, int error_code) {
    StatusCode status = response->status();
    delete request;
    delete response;

    CHECK(tablet->GetStatus() == kTableUnLoading
          || tablet->GetStatus() == kTableOnLoad
          || tablet->GetStatus() == kTableOnSplit);

    // tablet server addr may change later, so copy one
    std::string server_addr = tablet->GetServerAddr();

    // server down
    TabletNodePtr node;
    if (!tabletnode_manager_->FindTabletNode(server_addr, &node)) {
        LOG(ERROR) << "abort UnloadTablet: server down, " << tablet;
        if (tablet->GetMergeParam() != NULL) {
            CHECK(tablet->GetStatus() == kTableUnLoading);
            MergeTabletUnloadCallback(tablet);
        } else if (tablet->SetAddrAndStatusIf("", kTableOffLine, kTableUnLoading)) {
            ProcessOffLineTablet(tablet);
            TryLoadTablet(tablet);
        } else if (tablet->SetAddrAndStatusIf("", kTableOffLine, kTableOnLoad)) {
            ProcessOffLineTablet(tablet);
            TryLoadTablet(tablet);
        } else {
            CHECK(tablet->GetStatus() == kTableOnSplit);
            ScanClosure done =
                std::bind(&MasterImpl::ScanMetaCallbackForSplit, this, tablet, _1, _2, _3, _4);
            ScanMetaTableAsync(tablet->GetTableName(), tablet->GetKeyStart(),
                               tablet->GetKeyEnd(), done);
        }
        return;
    }

    // server restart (only OnLoad tablet care this)
    if (tablet->GetStatus() == kTableOnLoad && node->uuid_ != tablet->GetServerId()) {
        LOG(ERROR) << "abort UnloadTablet: server restart, " << tablet;
        tablet->SetStatusIf(kTableOffLine, kTableOnLoad);
        ProcessOffLineTablet(tablet);
        TryLoadTablet(tablet, server_addr);
        return;
    }

    // success
    if (!failed && (status == kTabletNodeOk || status == kKeyNotInRange)) {
        LOG(INFO) << "unload tablet success, " << tablet;
        if (tablet->GetMergeParam() != NULL) {
            CHECK(tablet->GetStatus() == kTableUnLoading);
            MergeTabletUnloadCallback(tablet);
        } else if (tablet->SetStatusIf(kTableOffLine, kTableUnLoading)) {
            ProcessOffLineTablet(tablet);
            // unload success, try load
            TryLoadTablet(tablet);
        } else if (tablet->SetStatusIf(kTableOffLine, kTableOnLoad)) {
            ProcessOffLineTablet(tablet);
            // load fail but unload success, try reload
            TryLoadTablet(tablet);

            // load next tablet
            node->FinishLoad(tablet);
            TabletPtr next_tablet;
            while (node->LoadNextWaitTablet(&next_tablet)) {
                if (next_tablet->SetAddrAndStatusIf(server_addr, kTableOnLoad, kTableOffLine)) {
                    next_tablet->SetServerId(node->uuid_);
                    WriteClosure done =
                        std::bind(&MasterImpl::UpdateMetaForLoadCallback, this,
                                   next_tablet, FLAGS_tera_master_meta_retry_times, _1, _2, _3, _4);
                    BatchWriteMetaTableAsync(std::bind(&Tablet::ToMetaTableKeyValue, next_tablet, _1, _2),
                                             false, done);
                    break;
                }
                node->FinishLoad(next_tablet);
            }
        } else {
            CHECK(tablet->GetStatus() == kTableOnSplit);
            // don't know split result, scan meta to determine the result
            ScanClosure done =
                std::bind(&MasterImpl::ScanMetaCallbackForSplit, this, tablet, _1, _2, _3, _4);
            ScanMetaTableAsync(tablet->GetTableName(), tablet->GetKeyStart(),
                               tablet->GetKeyEnd(), done);

            // split next tablet
            TabletNodePtr node;
            if (tabletnode_manager_->FindTabletNode(server_addr, &node)
                && node->uuid_ == tablet->GetServerId()) {
                node->FinishSplit();
                TabletPtr next_tablet;
                std::string split_key;
                while (node->SplitNextWaitTablet(&next_tablet, &split_key)) {
                    if (next_tablet->SetStatusIf(kTableOnSplit, kTableReady)) {
                        next_tablet->SetServerId(node->uuid_);
                        SplitTabletAsync(next_tablet, split_key);
                        break;
                    }
                    node->FinishSplit();
                }
            }
        }
        return;
    }

    // fail
    if (failed) {
        LOG(WARNING) << "fail to unload tablet: "
            << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << tablet;
    } else {
        LOG(WARNING) << "fail to unload tablet: " << StatusCodeToString(status)
            << ", " << tablet;
    }

    // abort
    if (retry <= 0) {
        LOG(ERROR) << "abort UnloadTablet: kick tabletnode, " << tablet;
        tablet->SetStatusIf(kTableUnLoadFail, kTableUnLoading)
            || tablet->SetStatusIf(kTableLoadFail, kTableOnLoad)
            || tablet->SetStatusIf(kTableSplitFail, kTableOnSplit);
        TryKickTabletNode(tablet->GetServerAddr());
        return;
    }

    // retry
    ThreadPool::Task task =
        std::bind(&MasterImpl::RetryUnloadTablet, this, tablet, retry - 1);
    thread_pool_->DelayTask(
        FLAGS_tera_master_control_tabletnode_retry_period, task);
}

void MasterImpl::DelSnapshot(const DelSnapshotRequest* request,
                             DelSnapshotResponse* response,
                             google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(WARNING) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }
    TablePtr table;
    if (!tablet_manager_->FindTable(request->table_name(), &table)) {
        LOG(WARNING) << "fail to delete snapshot: " << request->table_name()
            << ", table not exist";
        response->set_status(kTableNotFound);
        done->Run();
        return;
    }

    std::vector<TabletPtr> tablets;
    table->GetTablet(&tablets);
    assert(tablets.size());

    for (uint32_t i = 0; i < tablets.size(); ++i) {
        if (!tablets[i]->SetStatusIf(kTabletDelSnapshot, kTableReady)) {
            for (uint32_t j = 0; j < i; ++j) {
                tablets[j]->SetStatusIf(kTableReady, kTabletDelSnapshot);
            }
            response->set_status(kTabletNodeOffLine);
            done->Run();
            return;
        }
    }
    uint64_t snapshot = request->snapshot_id();
    int id = table->DelSnapshot(snapshot);
    if (id < 0) {
        LOG(WARNING) << "fail to delete snapshot: " << request->table_name()
            << ", unknown snapshot " << snapshot;
        response->set_status(kTableNotFound);
        for (uint32_t j = 0; j < tablets.size(); ++j) {
            tablets[j]->SetStatusIf(kTableReady, kTabletDelSnapshot);
        }
        done->Run();
        return;
    }

    for (uint32_t i = 0; i < tablets.size(); i++) {
        tablets[i]->DelSnapshot(id);
    }
    WriteClosure closure =
        std::bind(&MasterImpl::DelSnapshotCallback, this,
                table, tablets,
                FLAGS_tera_master_meta_retry_times,
                request, response, done, _1, _2, _3, _4);
    BatchWriteMetaTableAsync(table, tablets, false, closure);
}

void MasterImpl::DelSnapshotCallback(TablePtr table,
                                     std::vector<TabletPtr> tablets,
                                     int32_t retry_times,
                                     const DelSnapshotRequest* rpc_request,
                                     DelSnapshotResponse* rpc_response,
                                     google::protobuf::Closure* rpc_done,
                                     WriteTabletRequest* request,
                                     WriteTabletResponse* response,
                                     bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to del snapshot from meta: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", "
                << tablets[0] << "...";
        } else {
            LOG(ERROR) << "fail to del snapshot from meta: "
                << StatusCodeToString(status) << ", " << tablets[0] << "...";
        }
        if (retry_times <= 0) {
            /// 写meta失败会导致内存与meta不一致, 没处理
            abort();
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::DelSnapshotCallback, this, table,
                           tablets, retry_times - 1, rpc_request, rpc_response,
                           rpc_done, _1, _2, _3, _4);
            SuspendMetaOperation(table, tablets, false, done);
        }
        return;
    } else {
        LOG(INFO) << "DelSnapshot " << rpc_request->table_name()
            << ", write meta " << rpc_request->snapshot_id() << " done";
        rpc_response->set_status(kMasterOk);
        rpc_done->Run();
    }
    for (uint32_t j = 0; j < tablets.size(); ++j) {
        tablets[j]->SetStatusIf(kTableReady, kTabletDelSnapshot);
    }
}

void MasterImpl::GetSnapshot(const GetSnapshotRequest* request,
                             GetSnapshotResponse* response,
                             google::protobuf::Closure* done) {
    LOG(INFO) << "MasterImpl GetSnapshot";
    response->set_sequence_id(request->sequence_id());

    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(WARNING) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    TablePtr table;
    if (!tablet_manager_->FindTable(request->table_name(), &table)) {
        LOG(WARNING) << "fail to create snapshot: " << request->table_name()
            << ", table not exist";
        response->set_status(kTableNotFound);
        done->Run();
        return;
    }

    SnapshotTask* task = new SnapshotTask;
    table->GetTablet(&task->tablets);

    assert(task->tablets.size());

    task->snapshot_id.resize(task->tablets.size());
    task->request = request;
    task->response = response;
    task->done = done;
    task->table = table;
    task->task_num = 0;
    task->finish_num = 0;
    task->aborted = false;
    task->mutex.Lock();
    int64_t snapshot_id = get_micros();
    for (uint32_t i = 0; i < task->tablets.size(); ++i) {
        TabletPtr tablet = task->tablets[i];
        if (!tablet->SetStatusIf(kTabletOnSnapshot, kTableReady)) {
            LOG(INFO) << "will not get snapshot, " << tablet->GetServerAddr()
                << " is not ready";
            task->aborted = true;
            break;
        }
        LOG(INFO) << "Set tablet kTabletOnSnapshot " << tablet->GetPath();
        ++task->task_num;
        SnapshotClosure closure =
            std::bind(&MasterImpl::GetSnapshotCallback, this, static_cast<int32_t>(i), task,
                      _1, _2, _3, _4);
        GetSnapshotAsync(tablet, snapshot_id, 3000, closure);
    }
    if (task->task_num == 0) {
        task->mutex.Unlock();
        delete task;
        LOG(WARNING) << "fail to create snapshot: " << request->table_name()
            << ", all tables kTabletNodeOffLine";
        response->set_status(kTabletNodeOffLine);
        done->Run();
        return;
    }
    task->mutex.Unlock();
}

void MasterImpl::GetSnapshotAsync(TabletPtr tablet, int64_t snapshot_id, int32_t timeout,
                                  SnapshotClosure done) {

    std::string addr = tablet->GetServerAddr();
    tabletnode::TabletNodeClient node_client(addr, timeout);

    SnapshotRequest* request = new SnapshotRequest;
    SnapshotResponse* response = new SnapshotResponse;
    request->set_sequence_id(this_sequence_id_.Inc());
    request->set_table_name(tablet->GetTableName());
    request->set_snapshot_id(snapshot_id);
    request->mutable_key_range()->set_key_start(tablet->GetKeyStart());
    request->mutable_key_range()->set_key_end(tablet->GetKeyEnd());

    LOG(INFO) << "GetSnapshotAsync id: " << request->sequence_id() << ", "
        << "server: " << addr;
    node_client.GetSnapshot(request, response, done);
}

void MasterImpl::GetSnapshotCallback(int32_t tablet_id,
                                     SnapshotTask* task,
                                     SnapshotRequest* master_request,
                                     SnapshotResponse* master_response, bool failed,
                                     int error_code) {
    task->mutex.Lock();
    ++task->finish_num;
    VLOG(6) << "MasterImpl GetSnapshot id= " << tablet_id
        << " finish_num= " << task->finish_num
        << ". Return " << master_response->snapshot_seq();
    if (task->finish_num != task->task_num) {
        if (!failed && master_response->status() == kTabletNodeOk) {
            task->snapshot_id[tablet_id] = master_response->snapshot_seq();
        } else {
            task->aborted = true;
        }
        task->mutex.Unlock();
        return;
    }

    if (failed || task->aborted) {
        LOG(WARNING) << "MasterImpl GetSnapshot fail done";
        for (uint32_t i = 0; i < task->tablets.size(); ++i) {
            VLOG(6) << "Set tablet kTabletOnSnapshot " << task->tablets[i]->GetPath();
            task->tablets[i]->SetStatusIf(kTableReady, kTabletOnSnapshot);
        }
        task->response->set_status(kTabletNodeOffLine);
        task->done->Run();
    } else {
        task->snapshot_id[tablet_id] = master_response->snapshot_seq();
        LOG(INFO) << "MasterImpl GetSnapshot all tablet done";
        int sid = task->table->AddSnapshot(master_request->snapshot_id());
        for (uint32_t i = 0; i < task->tablets.size(); ++i) {
            int tsid = task->tablets[i]->AddSnapshot(task->snapshot_id[i]);
            assert(sid == tsid);
        }
        task->response->set_snapshot_id(master_request->snapshot_id());
        WriteClosure closure =
            std::bind(&MasterImpl::AddSnapshotCallback, this,
                    task->table, task->tablets,
                    FLAGS_tera_master_meta_retry_times,
                    task->request, task->response, task->done, _1, _2, _3, _4);
        BatchWriteMetaTableAsync(task->table, task->tablets, false, closure);
    }
    task->mutex.Unlock();
    delete task;
}

void MasterImpl::AddSnapshotCallback(TablePtr table,
                                     std::vector<TabletPtr> tablets,
                                     int32_t retry_times,
                                     const GetSnapshotRequest* rpc_request,
                                     GetSnapshotResponse* rpc_response,
                                     google::protobuf::Closure* rpc_done,
                                     WriteTabletRequest* request,
                                     WriteTabletResponse* response,
                                     bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    for (uint32_t i = 0; i < tablets.size(); ++i) {
        tablets[i]->SetStatusIf(kTableReady, kTabletOnSnapshot);
    }
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(WARNING) << "fail to write snapshot to meta: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", "
                << tablets[0] << "...";
        } else {
            LOG(WARNING) << "fail to write snapshot to meta: "
                << StatusCodeToString(status) << ", " << tablets[0] << "...";
        }
        if (retry_times <= 0) {
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::AddSnapshotCallback, this, table,
                           tablets, retry_times - 1, rpc_request, rpc_response,
                           rpc_done, _1, _2, _3, _4);
            SuspendMetaOperation(table, tablets, false, done);
        }
        return;
    }
    LOG(INFO) << "Snapshot " << rpc_request->table_name()
        << ", write meta " << rpc_response->snapshot_id() << " done";
    rpc_response->set_status(kMasterOk);
    rpc_done->Run();
}

void MasterImpl::ReleaseSnpashot(TabletPtr tablet, uint64_t snapshot) {
    std::string addr = tablet->GetServerAddr();
    tabletnode::TabletNodeClient node_client(addr, 3000);

    ReleaseSnapshotRequest* request = new ReleaseSnapshotRequest;
    ReleaseSnapshotResponse* response = new ReleaseSnapshotResponse;
    request->set_sequence_id(this_sequence_id_.Inc());
    request->set_table_name(tablet->GetTableName());
    request->set_snapshot_id(snapshot);
    request->mutable_key_range()->set_key_start(tablet->GetKeyStart());
    request->mutable_key_range()->set_key_end(tablet->GetKeyEnd());

    DelSnapshotClosure done =
        std::bind(&MasterImpl::ReleaseSnapshotCallback, this, _1, _2, _3, _4);
    LOG(INFO) << "ClearSnapshot id: " << request->sequence_id()
        << ", server: " << addr;
    node_client.ReleaseSnapshot(request, response, done);
}

void MasterImpl::ReleaseSnapshotCallback(ReleaseSnapshotRequest* request,
                                         ReleaseSnapshotResponse* response,
                                         bool failed, int error_code) {
    /// 删掉删不掉无所谓, 不计较~
}

void MasterImpl::GetRollback(const RollbackRequest* request,
                             RollbackResponse* response,
                             google::protobuf::Closure* done) {
    LOG(INFO) << "MasterImpl Rollback";
    response->set_sequence_id(request->sequence_id());

    MasterStatus master_status = GetMasterStatus();
    if (master_status != kIsRunning) {
        LOG(WARNING) << "master is not ready, status_ = "
            << StatusCodeToString(static_cast<StatusCode>(master_status));
        response->set_status(static_cast<StatusCode>(master_status));
        done->Run();
        return;
    }

    TablePtr table;
    if (!tablet_manager_->FindTable(request->table_name(), &table)) {
        LOG(WARNING) << "fail to rollback to snapshot: " << request->table_name()
            << ", table not exist";
        response->set_status(kTableNotFound);
        done->Run();
        return;
    }

    RollbackTask* task = new RollbackTask;
    table->GetTablet(&task->tablets);

    assert(task->tablets.size());

    task->rollback_points.resize(task->tablets.size());
    task->request = request;
    task->response = response;
    task->done = done;
    task->table = table;
    task->task_num = 0;
    task->finish_num = 0;
    task->aborted = false;
    task->mutex.Lock();
    for (uint32_t i = 0; i < task->tablets.size(); ++i) {
        TabletPtr tablet = task->tablets[i];
        ++task->task_num;
        RollbackClosure closure =
            std::bind(&MasterImpl::RollbackCallback, this, static_cast<int32_t>(i), task,
                      _1, _2, _3, _4);
        RollbackAsync(tablet, request->snapshot_id(), 3000, closure);
    }
    if (task->task_num == 0) {
        task->mutex.Unlock();
        delete task;
        LOG(WARNING) << "fail to rollback to snapshot: " << request->table_name()
            << ", all tables kTabletNodeOffLine";
        response->set_status(kTabletNodeOffLine);
        done->Run();
        return;
    }
    task->mutex.Unlock();
}

void MasterImpl::RollbackAsync(TabletPtr tablet, uint64_t snapshot_id,
                                int32_t timeout, RollbackClosure done) {
    std::string addr = tablet->GetServerAddr();
    tabletnode::TabletNodeClient node_client(addr, timeout);

    SnapshotRollbackRequest* request = new SnapshotRollbackRequest;
    SnapshotRollbackResponse* response = new SnapshotRollbackResponse;
    request->set_sequence_id(this_sequence_id_.Inc());
    request->set_table_name(tablet->GetTableName());
    request->set_snapshot_id(snapshot_id);
    request->mutable_key_range()->set_key_start(tablet->GetKeyStart());
    request->mutable_key_range()->set_key_end(tablet->GetKeyEnd());

    LOG(INFO) << "RollbackAsync id: " << request->sequence_id() << ", "
        << "server: " << addr;
    node_client.Rollback(request, response, done);
}

void MasterImpl::RollbackCallback(int32_t tablet_id, RollbackTask* task,
                                  SnapshotRollbackRequest* master_request,
                                  SnapshotRollbackResponse* master_response,
                                  bool failed, int error_code) {
    MutexLock lock(&task->mutex);
    ++task->finish_num;
    VLOG(6) << "MasterImpl Rollback id= " << tablet_id
        << " finish_num= " << task->finish_num
        << ". Return " << master_response->rollback_point();
    if (task->finish_num != task->task_num) {
        if (!failed && master_response->status() == kTabletNodeOk) {
            task->rollback_points[tablet_id] = master_response->rollback_point();
        } else {
            task->aborted = true;
        }
        return;
    }

    if (failed || task->aborted) {
        LOG(WARNING) << "MasterImpl Rollback fail done";
        task->response->set_status(kTabletNodeOffLine);
        task->done->Run();
    } else {
        task->rollback_points[tablet_id] = master_response->rollback_point();
        LOG(INFO) << "MasterImpl rollback all tablet done";
        int sid = task->table->AddRollback(task->request->rollback_name());
        for (uint32_t i = 0; i < task->tablets.size(); ++i) {
            int tsid = task->tablets[i]->AddRollback(task->request->rollback_name(),
                                                     master_request->snapshot_id(),
                                                     task->rollback_points[i]);
            assert(sid == tsid);
        }
        WriteClosure closure =
            std::bind(&MasterImpl::AddRollbackCallback, this,
                       task->table, task->tablets,
                       FLAGS_tera_master_meta_retry_times,
                       task->request, task->response, task->done, _1, _2, _3, _4);
        BatchWriteMetaTableAsync(task->table, task->tablets, false, closure);
    }
    task->mutex.Unlock();
    delete task;
}

void MasterImpl::AddRollbackCallback(TablePtr table,
                                     std::vector<TabletPtr> tablets,
                                     int32_t retry_times,
                                     const RollbackRequest* rpc_request,
                                     RollbackResponse* rpc_response,
                                     google::protobuf::Closure* rpc_done,
                                     WriteTabletRequest* request,
                                     WriteTabletResponse* response,
                                     bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(WARNING) << "fail to write rollback to meta: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", "
                << tablets[0] << "...";
        } else {
            LOG(WARNING) << "fail to write rollback to meta: "
                << StatusCodeToString(status) << ", " << tablets[0] << "...";
        }
        if (retry_times <= 0) {
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::AddRollbackCallback, this, table,
                           tablets, retry_times - 1, rpc_request, rpc_response,
                           rpc_done, _1, _2, _3, _4);
            SuspendMetaOperation(table, tablets, false, done);
        }
        return;
    }
    LOG(INFO) << "Rollback " << rpc_request->rollback_name() << " to " << rpc_request->table_name()
        << ", write meta " << rpc_request->snapshot_id() << " done";
    rpc_response->set_status(kMasterOk);
    rpc_done->Run();
}

void MasterImpl::ClearUnusedSnapshots(TabletPtr tablet, const TabletMeta& meta) {
    std::vector<uint64_t> snapshots;
    TablePtr table = tablet->GetTable();
    table->ListSnapshot(&snapshots);
#if 0
    std::map<uint64_t, int> snapshot_map;
    for (uint32_t i = 0; i < snapshots.size(); ++i) {
        snapshot_map[snapshots[i]]++;
    }
    for (int32_t i = 0; i < meta.snapshot_list_size(); i++) {
        uint64_t seq = meta.snapshot_list(i);
        if (--snapshot_map[seq] < 1) {
            ClearSnpashot(tablet, seq);
        }
    }
#endif
    std::sort(snapshots.begin(), snapshots.end());
    size_t i = 0;
    for (int j = 0; j < meta.snapshot_list_size(); ++j) {
        uint64_t seq = meta.snapshot_list(j);
        if (i >= snapshots.size() || snapshots[i] != seq) {
            ReleaseSnpashot(tablet, seq);
            continue;
        }
        ++i;
    }
}

void MasterImpl::UpdateSchemaCallback(std::string table_name,
                                      std::string tablet_path,
                                      std::string start_key,
                                      std::string end_key,
                                      int32_t retry_times,
                                      UpdateRequest* request,
                                      UpdateResponse* response,
                                      bool rpc_failed, int status_code) {
    StatusCode status = response->status();
    delete request;
    delete response;
    TabletPtr tablet;
    if (!tablet_manager_->FindTablet(table_name, start_key, &tablet)
        || (tablet->GetKeyEnd() != end_key)) {
        LOG(INFO) << "[update] tablet not found, ignore it:" << tablet_path
            << ", start_key:" << DebugString(start_key)
            << ", end_key:" << DebugString(end_key);
        return;
    }

    // fail
    if (rpc_failed || (status != kTabletNodeOk)) {
        if (rpc_failed) {
            LOG(WARNING) << "[update] fail to update schema: "
                << sofa::pbrpc::RpcErrorCodeToString(status_code)
                << ": " << tablet;
        } else {
            LOG(WARNING) << "[update] fail to update schema: " << StatusCodeToString(status)
                << ": " << tablet;
        }
        if (retry_times > FLAGS_tera_master_schema_update_retry_times) {
            LOG(ERROR) << "[update] retry " << retry_times << " times, kick "
                << tablet->GetServerAddr();
            TryKickTabletNode(tablet->GetServerAddr());
        } else {
            UpdateClosure done =
                std::bind(&MasterImpl::UpdateSchemaCallback, this, tablet->GetTableName(),
                           tablet->GetPath(), tablet->GetKeyStart(), tablet->GetKeyEnd(),
                           retry_times + 1, _1, _2, _3, _4);
            ThreadPool::Task task =
                std::bind(&MasterImpl::NoticeTabletNodeSchemaUpdatedAsync, this, tablet, done);
            thread_pool_->DelayTask(FLAGS_tera_master_schema_update_retry_period * 1000, task);
        }
        return;
    }
    LOG(INFO) << "[update] tablet schema update done. " << tablet;
    TablePtr table = tablet->GetTable();
    if (!table->AddToRange(tablet->GetKeyStart(), tablet->GetKeyEnd())) {
        LOG(ERROR) << "[update] invalid argument:" << tablet;
    }
    if (table->IsCompleteRange()) {
        table->UpdateRpcDone();
        LOG(INFO) << "[update] DONE :" << table;
        // new schema synced to all tablets/ts
        table->SetSchemaIsSyncing(false);
    }
}

void MasterImpl::NoticeTabletNodeSchemaUpdatedAsync(TabletPtr tablet,
                                                    UpdateClosure done) {
    tabletnode::TabletNodeClient node_client(tablet->GetServerAddr(),
                                             FLAGS_tera_master_collect_info_timeout);
    UpdateRequest* request = new UpdateRequest;
    UpdateResponse* response = new UpdateResponse;

    request->set_sequence_id(this_sequence_id_.Inc());
    request->mutable_schema()->CopyFrom(tablet->GetSchema());
    request->set_tablet_name(tablet->GetTableName());
    request->mutable_key_range()->set_key_start(tablet->GetKeyStart());
    request->mutable_key_range()->set_key_end(tablet->GetKeyEnd());

    VLOG(20) << "NoticeTabletNodeSchemaUpdatedAsync id: " << request->sequence_id()
             << ", tablet:" << tablet;
    node_client.Update(request, response, done);
}

void MasterImpl::NoticeTabletNodeSchemaUpdated(TabletPtr tablet) {
    int32_t retry_times = 0;
    UpdateClosure done =
        std::bind(&MasterImpl::UpdateSchemaCallback, this, tablet->GetTableName(),
                   tablet->GetPath(), tablet->GetKeyStart(), tablet->GetKeyEnd(), retry_times,
                   _1, _2, _3, _4);
    NoticeTabletNodeSchemaUpdatedAsync(tablet, done);
}

void MasterImpl::NoticeTabletNodeSchemaUpdated(TablePtr table) {
    std::vector<TabletPtr> tablet_list;
    table->GetTablet(&tablet_list);
    std::vector<TabletPtr>::iterator it;
    for (it = tablet_list.begin(); it != tablet_list.end(); ++it) {
        if ((*it)->GetStatus() != kTableReady) {
            continue;
        }
        NoticeTabletNodeSchemaUpdated(*it);
    }
}

void MasterImpl::QueryTabletNodeAsync(std::string addr, int32_t timeout,
                                      bool is_gc, QueryClosure done) {
    tabletnode::TabletNodeClient node_client(addr, timeout);

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
            } else if (kTableReady != meta.status()) {
                LOG(ERROR) << "[query] status error tablet: " << meta.path()
                    << "] @ " << meta.server_addr()
                    << " should be kTabletReady";
            } else if (tablet->GetServerAddr() != meta.server_addr()) {
                LOG(ERROR) << "[query] addr error tablet: " << meta.path()
                    << " @ " << meta.server_addr()
                    << " should @ " << tablet->GetServerAddr();
            } else if (tablet->GetTable()->GetStatus() == kTableDisable) {
                if (tablet->SetStatusIf(kTableUnLoading, kTableReady)) {
                    UnloadClosure done =
                        std::bind(&MasterImpl::UnloadTabletCallback, this, tablet,
                                   FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);
                    UnloadTabletAsync(tablet, done);
                    LOG(INFO) << "Unload disable tablet: " << tablet->GetPath();
                } else {
                    LOG(INFO) << "Discard disable tablet: " << tablet->GetPath()
                        << ", status: " << tablet->GetStatus();
                }
            } else {
                VLOG(20) << "[query] OK tablet: " << meta.path()
                    << "] @ " << meta.server_addr();
                tablet->SetUpdateTime(query_callback_start);
                tablet->UpdateSize(meta);
                tablet->SetCounter(counter);
                tablet->SetCompactStatus(meta.compact_status());
                ClearUnusedSnapshots(tablet, meta);
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

            TabletStatus tablet_status = tablet->GetStatus();
            if (tablet_status == kTableReady || tablet_status == kTableOnLoad
                || tablet_status == kTableOffLine) {
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
        if (FLAGS_tera_master_stat_table_enabled && stat_table_) {
            DumpStatToTable(state);
        }
        VLOG(20) << "query tabletnode [" << addr << "], status_: "
            << StatusCodeToString(state.report_status_);
    }
    // if this is a gc query, process it
    if (request->is_gc_query()) {
        gc_strategy_->ProcessQueryCallbackForGc(response);
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
        tabletnode_manager_->UpdateTabletNode(addr, state);
        node->ResetQueryFailCount();
        NodeState old_state;
        node->SetState(kReady, &old_state);
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
            delete request;
            delete response;
            return;
        }
    }
    sem_post(finish_counter);
    delete request;
    delete response;
}

void MasterImpl::RetryCollectTabletInfo(std::string addr,
                                        std::vector<TabletMeta>* tablet_list,
                                        sem_t* finish_counter, Mutex* mutex) {
    QueryClosure done =
        std::bind(&MasterImpl::CollectTabletInfoCallback, this, addr,
                   tablet_list, finish_counter, mutex, _1, _2, _3, _4);
    QueryTabletNodeAsync(addr, FLAGS_tera_master_collect_info_timeout, false, done);
}

void MasterImpl::SplitTabletAsync(TabletPtr tablet, const std::string& split_key) {
    const std::string& table_name = tablet->GetTableName();
    const std::string& server_addr = tablet->GetServerAddr();
    const std::string& key_start = tablet->GetKeyStart();
    const std::string& key_end = tablet->GetKeyEnd();

    tabletnode::TabletNodeClient node_client(server_addr,
            FLAGS_tera_master_split_rpc_timeout);

    SplitTabletRequest* request = new SplitTabletRequest;
    SplitTabletResponse* response = new SplitTabletResponse;
    request->set_sequence_id(this_sequence_id_.Inc());
    request->set_tablet_name(table_name);
    request->mutable_key_range()->set_key_start(key_start);
    request->mutable_key_range()->set_key_end(key_end);
    request->add_child_tablets(tablet->GetTable()->GetNextTabletNo());
    request->add_child_tablets(tablet->GetTable()->GetNextTabletNo());
    request->set_split_key(split_key);

    tablet->ToMeta(request->mutable_tablet_meta());
    std::vector<uint64_t> snapshots;
    tablet->GetTable()->ListSnapshot(&snapshots);
    LOG(INFO) << "SplitTabletAsync snapshot num " << snapshots.size();
    SplitClosure done =
        std::bind(&MasterImpl::SplitTabletCallback, this, tablet, _1, _2, _3, _4);

    LOG(INFO) << "SplitTabletAsync id: " << request->sequence_id() << ", "
        << tablet;
    tablet_availability_->AddNotReadyTablet(tablet->GetPath());
    node_client.SplitTablet(request, response, done);
}

void MasterImpl::SplitTabletCallback(TabletPtr tablet,
                                     SplitTabletRequest* request,
                                     SplitTabletResponse* response,
                                     bool failed, int error_code) {
    CHECK(tablet->GetStatus() == kTableOnSplit);
    StatusCode status = response->status();
    delete request;
    delete response;
    const std::string& server_addr = tablet->GetServerAddr();

    // fail
    if (failed || (status != kTabletNodeOk && status != kTableNotSupport
                   && status != kMetaTabletError)) {
        if (failed) {
            LOG(WARNING) << "fail to split: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code)
                << ", " << tablet;
        } else {
            LOG(WARNING) << "fail to split: "
                << StatusCodeToString(status) << ", " << tablet;
        }
        UnloadClosure done =
            std::bind(&MasterImpl::UnloadTabletCallback, this, tablet,
                       FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);
        UnloadTabletAsync(tablet, done);
        return;
    }

    // success
    if (status == kTabletNodeOk) {
        // tabletnode unloaded the tablet
        LOG(INFO) << "RPC SplitTablet success";
    } else if (status == kTableNotSupport) {
        // tabletnode refused to split and didn't unload the tablet
        tablet->SetStatusIf(kTableReady, kTableOnSplit);
        ProcessReadyTablet(tablet);
    } else {
        CHECK(status == kMetaTabletError);
        // meta table is not ok
        LOG(ERROR) << "fail to split: " << StatusCodeToString(status) << ", "
            << tablet;
    }

    TabletNodePtr node;
    if (tabletnode_manager_->FindTabletNode(server_addr, &node)
        && node->uuid_ == tablet->GetServerId()) {
        node->FinishSplit();

        // schedule next split task
        TabletPtr next_tablet;
        std::string split_key;
        while (node->SplitNextWaitTablet(&next_tablet, &split_key)) {
            if (next_tablet->SetStatusIf(kTableOnSplit, kTableReady)) {
                next_tablet->SetServerId(node->uuid_);
                SplitTabletAsync(next_tablet, split_key);
                break;
            }
            node->FinishSplit();
        }
    } else { // server down or restart
        if (tablet->SetStatusIf(kTableOffLine, kTableReady)) {
            ProcessOffLineTablet(tablet);
            TryLoadTablet(tablet, tablet->GetServerAddr());
        }
    }

    if (status == kTableNotSupport) {
        LOG(ERROR) << "ts refused to split tablet: "
            << StatusCodeToString(status) << ", " << tablet
            << ", tablet status " << StatusCodeToString(tablet->GetStatus());
        tablet_availability_->EraseNotReadyTablet(tablet->GetPath());
        return;
    }

    // scan meta tablet
    if (tablet->GetStatus() == kTableOnSplit) {
        ScanClosure done =
            std::bind(&MasterImpl::ScanMetaCallbackForSplit, this, tablet, _1, _2, _3, _4);
        ScanMetaTableAsync(tablet->GetTableName(), tablet->GetKeyStart(),
                           tablet->GetKeyEnd(), done);
    }
}

void MasterImpl::TryLoadTablet(TabletPtr tablet, std::string server_addr) {
    if (!tablet->IsBound()) {
        return;
    }

    const std::string& table_name = tablet->GetTableName();

    if (table_name == FLAGS_tera_master_meta_table_name) {
        CHECK(tablet->GetPath() == FLAGS_tera_master_meta_table_path);
        zk_adapter_->UpdateRootTabletNode("");
    }

    if (tablet->GetTable()->GetStatus() == kTableDisable) {
        VLOG(20) << "LoadTablet skip disable tablet: " << tablet->GetPath();
        return;
    }

    if (!tablet->GetExpectServerAddr().empty()) {
        server_addr = tablet->GetExpectServerAddr();
    }

    TabletNodePtr node;
    if (!server_addr.empty()
        && !tabletnode_manager_->FindTabletNode(server_addr, &node)) {
        tablet->SetExpectServerAddr("");

        if (tablet->GetTableName() == FLAGS_tera_master_meta_table_name) {
            server_addr.clear();
        } else if (FLAGS_tera_master_tabletnode_timeout > 0) {
            tablet->SetAddrAndStatusIf(server_addr, kTabletPending, kTableOffLine);
            LOG(INFO) << "load tablet " << tablet << " on " << server_addr
                << " " << FLAGS_tera_master_tabletnode_timeout << "(ms) later";
            ThreadPool::Task task =
                std::bind(&MasterImpl::TryMovePendingTablet, this, tablet);
            thread_pool_->DelayTask(FLAGS_tera_master_tabletnode_timeout, task);
            return;
        } else if (GetMasterStatus() == kIsRunning) {
            LOG(WARNING) << "give up load " << tablet << " on " << server_addr
                << ": server down, try to pick another server";
            server_addr.clear();
        } else {
            tablet->SetAddrIf(server_addr, kTableOffLine);
            LOG(WARNING) << "give up load " << tablet << " on " << server_addr
                << ": server down, master is in safemode, abort load";
            return;
        }
    }

    while (server_addr.empty()) {
        std::string sche_table_name;
        if (FLAGS_tera_master_load_balance_table_grained
            && table_name != FLAGS_tera_master_meta_table_name) {
            sche_table_name = table_name;
        }

        if (!tabletnode_manager_->ScheduleTabletNode(size_scheduler_.get(), sche_table_name,
                                                      false, &server_addr)) {
            // tablet->SetAddrIf("", kTableOffLine);
            LOG(ERROR) << "no available tabletnode, abort load " << tablet;
            return;
        }
        if (!tabletnode_manager_->FindTabletNode(server_addr, &node)) {
            server_addr.clear();
        }
    }

    // meta table always load immediately
    if (table_name == FLAGS_tera_master_meta_table_name) {
        if (!tablet->GetExpectServerAddr().empty()) {
            node->DoneMoveIn();
            tablet->SetExpectServerAddr("");
        }
        if (tablet->SetAddrAndStatusIf(server_addr, kTableOnLoad, kTableOffLine)) {
            tablet->SetServerId(node->uuid_);
            LoadClosure done = std::bind(&MasterImpl::LoadTabletCallback, this,
                                           tablet, 0, _1, _2, _3, _4);
            LoadTabletAsync(tablet, done);
        }
        return;
    }

    // other tables may wait in a queue
    if (!node->TryLoad(tablet)) {
        tablet->SetAddrIf(server_addr, kTableOffLine);
        LOG(INFO) << "delay load table " << tablet->GetPath()
            << ", too many tablets are loading on server: "
            << server_addr;
        return;
    }
    if (!tablet->GetExpectServerAddr().empty()) {
        node->DoneMoveIn();
        tablet->SetExpectServerAddr("");
    }

    // abort if status switch to offline (server down / disable)
    if (!tablet->SetAddrAndStatusIf(server_addr, kTableOnLoad, kTableOffLine)) {
        LOG(ERROR) << "error state, abort load tablet, " << tablet;
        node->FinishLoad(tablet);
        TabletPtr next_tablet;
        while (node->LoadNextWaitTablet(&next_tablet)) {
            if (next_tablet->SetAddrAndStatusIf(server_addr, kTableOnLoad, kTableOffLine)) {
                if (!next_tablet->GetExpectServerAddr().empty()) {
                    node->DoneMoveIn();
                    next_tablet->SetExpectServerAddr("");
                }
                next_tablet->SetServerId(node->uuid_);
                WriteClosure done =
                    std::bind(&MasterImpl::UpdateMetaForLoadCallback, this,
                               next_tablet, FLAGS_tera_master_meta_retry_times, _1, _2, _3, _4);
                BatchWriteMetaTableAsync(std::bind(&Tablet::ToMetaTableKeyValue, next_tablet, _1, _2),
                                         false, done);
                break;
            }
            node->FinishLoad(next_tablet);
        }
        return;
    }

    // if server down here, let split callback take care of status switch
    tablet->SetServerId(node->uuid_);
    WriteClosure done =
        std::bind(&MasterImpl::UpdateMetaForLoadCallback, this, tablet,
                   FLAGS_tera_master_meta_retry_times, _1, _2, _3, _4);
    BatchWriteMetaTableAsync(std::bind(&Tablet::ToMetaTableKeyValue, tablet, _1, _2),
                             false, done);
    return;
}

void MasterImpl::RetryLoadTablet(TabletPtr tablet, int32_t retry_times) {
    CHECK(tablet->GetStatus() == kTableOnLoad);
    TabletNodePtr node;
    if (!tabletnode_manager_->FindTabletNode(tablet->GetServerAddr(), &node)) {
        LOG(WARNING) << kSms << "abort load on " << tablet->GetServerAddr()
            << ": server down, " << tablet;
        tablet->SetStatus(kTableOffLine);
        ProcessOffLineTablet(tablet);
        TryLoadTablet(tablet, tablet->GetServerAddr());
        return;
    }

    // server restart
    if (node->uuid_ != tablet->GetServerId()) {
        LOG(ERROR) << "retry LoadTablet: server restart, " << tablet;
        tablet->SetStatusIf(kTableOffLine, kTableOnLoad);
        ProcessOffLineTablet(tablet);
        TryLoadTablet(tablet, tablet->GetServerAddr());
        return;
    }

    LoadClosure done = std::bind(&MasterImpl::LoadTabletCallback, this, tablet,
                                   retry_times, _1, _2, _3, _4);
    LoadTabletAsync(tablet, done);
    return;
}

void MasterImpl::RetryUnloadTablet(TabletPtr tablet, int32_t retry_times) {
    // server down
    if (!tabletnode_manager_->FindTabletNode(tablet->GetServerAddr(), NULL)) {
        LOG(ERROR) << "abort UnloadTablet: server down, " << tablet;
        if (tablet->SetAddrAndStatusIf("", kTableOffLine, kTableUnLoading)) {
            ProcessOffLineTablet(tablet);
            TryLoadTablet(tablet);
        } else if (tablet->SetAddrAndStatusIf("", kTableOffLine, kTableOnLoad)) {
            ProcessOffLineTablet(tablet);
            TryLoadTablet(tablet);
        } else {
            CHECK(tablet->GetStatus() == kTableOnSplit);
            ScanClosure done =
                std::bind(&MasterImpl::ScanMetaCallbackForSplit, this, tablet, _1, _2, _3, _4);
            ScanMetaTableAsync(tablet->GetTableName(), tablet->GetKeyStart(),
                               tablet->GetKeyEnd(), done);
        }
        return;
    }

    UnloadClosure done =
        std::bind(&MasterImpl::UnloadTabletCallback, this, tablet, retry_times, _1, _2, _3, _4);
    UnloadTabletAsync(tablet, done);
}

bool MasterImpl::TrySplitTablet(TabletPtr tablet, const std::string& split_key) {
    const std::string& server_addr = tablet->GetServerAddr();

    // abort if server down
    TabletNodePtr node;
    if (!tabletnode_manager_->FindTabletNode(server_addr, &node)) {
        LOG(WARNING) << "abort split on " << server_addr << ": server down";
        return false;
    }

    // delay split
    if (!node->TrySplit(tablet, split_key)) {
        LOG(INFO) << "delay split table " << tablet->GetPath()
            << ", too many tablets are splitting on server: " << server_addr;
        return false;
    }
    // abort if status switch to offline (server down / disable)
    if (!tablet->SetStatusIf(kTableOnSplit, kTableReady)) {
        LOG(ERROR) << "error state, abort split table " << tablet->GetPath();
        return false;
    }

    // if server down here, let split callback take care of status switch
    LOG(INFO) << "begin split table " << tablet->GetPath();
    tablet->SetServerId(node->uuid_);
    SplitTabletAsync(tablet, split_key);
    return true;
}

bool MasterImpl::TryMergeTablet(TabletPtr tablet) {
    MutexLock lock(&tablet_mutex_);
    const std::string& server_addr = tablet->GetServerAddr();

    // abort if server down
    TabletNodePtr node;
    if (!tabletnode_manager_->FindTabletNode(server_addr, &node)) {
        LOG(WARNING) << "[merge] abort merge on " << server_addr << ": server down";
        return false;
    }

    if (tablet->IsBusy()) {
        LOG(INFO) << "[merge] skip merge, tablet is busy: " << tablet->GetPath();
        return false;
    }

    TabletPtr tablet2;
    if (!tablet_manager_->PickMergeTablet(tablet, &tablet2)) {
        VLOG(20) << "[merge] merge failed, no proper tablet for " << tablet;
        return false;
    }

    if (tablet2->GetStatus() != kTableReady ||
        tablet2->IsBusy() ||
        tablet2->GetCounter().write_workload() >= 1) {
        VLOG(20) << "[merge] merge failed, none proper tablet."
            << " status:" << tablet2->GetStatus()
            << " isbusy:" << tablet2->IsBusy()
            << " write workload:" << tablet2->GetCounter().write_workload();
        return false;
    }

    LOG(INFO) << "[merge] begin merge tablet " << tablet->GetPath()
        << " and " << tablet2->GetPath();
    MergeTabletAsync(tablet, tablet2);
    return true;
}

void MasterImpl::MergeTabletAsync(TabletPtr tablet_p1, TabletPtr tablet_p2) {
    bool switch_ok = false;

    // prepare
    switch_ok = tablet_p1->SetStatusIf(kTableUnLoading, kTableReady);
    if (!switch_ok) {
        // why this tablet is not Ready? maybe someone changes it's state
        LOG(WARNING) << "[merge] tablet not ready, merge failed:" << tablet_p1;
        return;
    }
    switch_ok = tablet_p2->SetStatusIf(kTableUnLoading, kTableReady);
    if (!switch_ok) {
        // why this tablet is not Ready? maybe someone changes it's state
        LOG(WARNING) << "[merge] tablet not ready, merge failed:" << tablet_p2;
        // rollback
        CHECK(tablet_p1->SetStatusIf(kTableReady, kTableUnLoading));
        return;
    }

    // commit
    MutexPtr mu(new Mutex());
    MergeParam* param1 = new MergeParam(mu, tablet_p2);
    MergeParam* param2 = new MergeParam(mu, tablet_p1);
    tablet_p1->SetMergeParam(param1);
    tablet_p2->SetMergeParam(param2);
    UnloadClosure done1 =
        std::bind(&MasterImpl::UnloadTabletCallback, this, tablet_p1,
                   FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);
    UnloadClosure done2 =
        std::bind(&MasterImpl::UnloadTabletCallback, this, tablet_p2,
                   FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);

    tablet_availability_->AddNotReadyTablet(tablet_p1->GetPath());
    tablet_availability_->AddNotReadyTablet(tablet_p2->GetPath());
    UnloadTabletAsync(tablet_p1, done1);
    UnloadTabletAsync(tablet_p2, done2);
}

void MasterImpl::MergeTabletAsyncPhase2(TabletPtr tablet_p1, TabletPtr tablet_p2) {
    leveldb::Env* env = io::LeveldbBaseEnv();
    std::vector<std::string> children;
    std::string tablet_path = FLAGS_tera_tabletnode_path_prefix + tablet_p1->GetPath();
    env->GetChildren(tablet_path, &children);
    tablet_path = FLAGS_tera_tabletnode_path_prefix + tablet_p2->GetPath();
    env->GetChildren(tablet_path, &children);
    for (size_t i = 0; i < children.size(); ++i) {
        leveldb::FileType type = leveldb::kUnknown;
        uint64_t number = 0;
        if (ParseFileName(children[i], &number, &type) &&
            type == leveldb::kLogFile) {
            LOG(ERROR) << "[merge] tablet log not clear, merge failed: " << tablet_path;
            MergeTabletFailed(tablet_p1, tablet_p2);
            return;
        }
    }

    std::string meta_addr;
    if (!tablet_manager_->GetMetaTabletAddr(&meta_addr)) {
        LOG(ERROR) << "[merge] meta table not ready.";
        MergeTabletFailed(tablet_p1, tablet_p2);
        return;
    }

    WriteTabletRequest* request = new WriteTabletRequest;
    WriteTabletResponse* response = new WriteTabletResponse;
    request->set_sequence_id(this_sequence_id_.Inc());
    request->set_tablet_name(FLAGS_tera_master_meta_table_name);
    request->set_is_sync(true);
    request->set_is_instant(true);

    // delete the first tablet
    std::string packed_key, packed_value;
    tablet_p1->ToMetaTableKeyValue(&packed_key, &packed_value);
    RowMutationSequence* mu_seq = request->add_row_list();
    mu_seq->set_row_key(packed_key);
    Mutation* mutation = mu_seq->add_mutation_sequence();
    mutation->set_type(kDeleteRow);

    // delete the second tablet
    tablet_p2->ToMetaTableKeyValue(&packed_key, &packed_value);
    mu_seq = request->add_row_list();
    mu_seq->set_row_key(packed_key);
    mutation = mu_seq->add_mutation_sequence();
    mutation->set_type(kDeleteRow);

    // put the new tablet
    TabletMeta new_meta;
    if (tablet_p1->GetKeyStart() == tablet_p2->GetKeyEnd() &&
        tablet_p1->GetKeyStart() != "") {
        tablet_p2->ToMeta(&new_meta);
        new_meta.mutable_key_range()->set_key_end(tablet_p1->GetKeyEnd());
        new_meta.clear_parent_tablets();
        new_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(tablet_p2->GetPath()));
        new_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(tablet_p1->GetPath()));
    } else if (tablet_p1->GetKeyEnd() == tablet_p2->GetKeyStart()) {
        tablet_p1->ToMeta(&new_meta);
        new_meta.mutable_key_range()->set_key_end(tablet_p2->GetKeyEnd());
        new_meta.clear_parent_tablets();
        new_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(tablet_p1->GetPath()));
        new_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(tablet_p2->GetPath()));
    } else {
        LOG(FATAL) << "tablet range error, cannot be merged: "
            << tablet_p1 << ", " << tablet_p2;
    }
    new_meta.set_status(kTableOffLine);
    // load new tablet on server which has larger parent tablet
    new_meta.set_server_addr(
        (tablet_p1->GetDataSize() > tablet_p2->GetDataSize()) ?
        tablet_p1->GetServerAddr() : tablet_p2->GetServerAddr());
    std::string new_path =
        leveldb::GetChildTabletPath(tablet_p1->GetPath(),
                                    tablet_p1->GetTable()->GetNextTabletNo());
    new_meta.set_path(new_path);
    new_meta.set_size(tablet_p1->GetDataSize() + tablet_p2->GetDataSize());

    TabletPtr tablet_c(new Tablet(new_meta, tablet_p1->GetTable()));
    tablet_c->ToMetaTableKeyValue(&packed_key, &packed_value);
    mu_seq = request->add_row_list();
    mu_seq->set_row_key(packed_key);
    mutation = mu_seq->add_mutation_sequence();
    mutation->set_type(kPut);
    mutation->set_value(packed_value);

    WriteClosure done =
        std::bind(&MasterImpl::MergeTabletWriteMetaCallback, this, tablet_c,
                   tablet_p1, tablet_p2, FLAGS_tera_master_meta_retry_times, _1, _2, _3, _4);
    tabletnode::TabletNodeClient meta_node_client(meta_addr);
    meta_node_client.WriteTablet(request, response, done);
}

void MasterImpl::MergeTabletUnloadCallback(TabletPtr tablet) {
    MergeParam* merge_param = (MergeParam*)tablet->GetMergeParam();
    CHECK_NOTNULL(merge_param);
    MutexPtr mutex = merge_param->mutex;
    TabletPtr tablet2 = merge_param->counter_part;
    delete merge_param;
    tablet->SetMergeParam(NULL);

    MutexLock l(mutex.get());
    CHECK(tablet->GetStatus() == kTableUnLoading
          || tablet->GetStatus() == kTableUnLoadFail);

    LOG(INFO) << "[merge] unload tablet finish, " << tablet;
    tablet->SetStatus(kTableOnMerge);

    if (tablet2->GetStatus() == kTableOnMerge) {
        LOG(INFO) << "[merge] tablet2 unload finish, continue merge: " << tablet2;
        MergeTabletAsyncPhase2(tablet, tablet2);
    } else {
        CHECK(tablet2->GetStatus() == kTableUnLoading
              || tablet2->GetStatus() == kTableUnLoadFail);
        LOG(INFO) << "[merge] tablet2 still unloading: " << tablet2;
    }
}

void MasterImpl::MergeTabletWriteMetaCallback(TabletPtr tablet_c,
                                              TabletPtr tablet_p1,
                                              TabletPtr tablet_p2,
                                              int32_t retry_times,
                                              WriteTabletRequest* request,
                                              WriteTabletResponse* response,
                                              bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "[merge] fail to add to meta tablet: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", "
                << tablet_c;
        } else {
            LOG(ERROR) << "[merge] fail to add to meta tablet: "
                << StatusCodeToString(status) << ", "
                << tablet_c;
        }
        if (retry_times <= 0) {
            LOG(ERROR) << "[merge] fail to update meta";
            MergeTabletFailed(tablet_p1, tablet_p2);
        } else {
            std::string meta_addr;
            if (tablet_manager_->GetMetaTabletAddr(&meta_addr)) {
                WriteClosure done =
                    std::bind(&MasterImpl::MergeTabletWriteMetaCallback, this, tablet_c,
                               tablet_p1, tablet_p2, retry_times - 1, _1, _2, _3, _4);
                tabletnode::TabletNodeClient meta_node_client(meta_addr);
                meta_node_client.WriteTablet(request, response, done);
                return;
            } else {
                LOG(WARNING) << "[merge] meta table not ready.";
                MergeTabletFailed(tablet_p1, tablet_p2);
            }
        }
        delete request;
        delete response;
        return;
    }

    TabletMeta new_meta;
    tablet_c->ToMeta(&new_meta);
    if (tablet_p1->GetKeyStart() == tablet_c->GetKeyStart()) {
        DeleteTablet(tablet_p1);
        tablet_manager_->AddTablet(new_meta, TableSchema(), &tablet_c);
        DeleteTablet(tablet_p2);
    } else {
        DeleteTablet(tablet_p2);
        tablet_manager_->AddTablet(new_meta, TableSchema(), &tablet_c);
        DeleteTablet(tablet_p1);
    }
    tablet_availability_->AddNotReadyTablet(tablet_c->GetPath());
    ProcessOffLineTablet(tablet_c);
    TryLoadTablet(tablet_c);
    delete request;
    delete response;
    LOG(INFO) << "[merge] merge tablet finished, from [" << tablet_p1
        << "] and [" << tablet_p2 << "] to [" << tablet_c << "]";
}

void MasterImpl::MergeTabletFailed(TabletPtr tablet_p1, TabletPtr tablet_p2) {
    CHECK(tablet_p1->GetStatus() == kTableOnMerge);
    CHECK(tablet_p2->GetStatus() == kTableOnMerge);
    LOG(INFO) << "[merge] merge failed, tablets unload succ, reload them: "
        << tablet_p1 << ", " << tablet_p2;
    tablet_p1->SetStatusIf(kTableOffLine, kTableOnMerge);
    ProcessOffLineTablet(tablet_p1);
    TryLoadTablet(tablet_p1);
    tablet_p2->SetStatusIf(kTableOffLine, kTableOnMerge);
    ProcessOffLineTablet(tablet_p2);
    TryLoadTablet(tablet_p2);
}

void MasterImpl::BatchWriteMetaTableAsync(ToMetaFunc meta_entry,
                                          bool is_delete, WriteClosure done) {
    std::vector<ToMetaFunc> meta_entries;
    meta_entries.push_back(meta_entry);
    BatchWriteMetaTableAsync(meta_entries, is_delete, done);
}

void MasterImpl::BatchWriteMetaTableAsync(TablePtr table,
                                          const std::vector<TabletPtr>& tablets,
                                          bool is_delete, WriteClosure done) {
    std::vector<ToMetaFunc> meta_entries;
    TablePtr null_ptr;
    if (table != null_ptr) {
        meta_entries.push_back(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2));
    }
    if (tablets.size() != 0) {
        for (size_t i = 0; i < tablets.size(); ++i) {
            meta_entries.push_back(std::bind(&Tablet::ToMetaTableKeyValue, tablets[i], _1, _2));
        }
    }
    BatchWriteMetaTableAsync(meta_entries, is_delete, done);
}

void MasterImpl::BatchWriteMetaTableAsync(std::vector<ToMetaFunc> meta_entries,
                                          bool is_delete, WriteClosure done) {
    VLOG(5) << "WriteMetaTableAsync()";
    std::string meta_addr;
    if (!tablet_manager_->GetMetaTabletAddr(&meta_addr)) {
        SuspendMetaOperation(meta_entries, is_delete, done);
        return;
    }
    WriteTabletRequest* request = new WriteTabletRequest;
    WriteTabletResponse* response = new WriteTabletResponse;
    request->set_sequence_id(this_sequence_id_.Inc());
    request->set_tablet_name(FLAGS_tera_master_meta_table_name);
    request->set_is_sync(true);
    request->set_is_instant(true);
    for (size_t i = 0; i < meta_entries.size(); ++i) {
        std::string packed_key;
        std::string packed_value;
        meta_entries[i](&packed_key, &packed_value);
        RowMutationSequence* mu_seq = request->add_row_list();
        mu_seq->set_row_key(packed_key);
        Mutation* mutation = mu_seq->add_mutation_sequence();
        if (!is_delete) {
            mutation->set_type(kPut);
            mutation->set_value(packed_value);
        } else {
            mutation->set_type(kDeleteRow);
        }
    }
    if (request->row_list_size() == 0) {
        delete request;
        delete response;
        return;
    } else {
        LOG(INFO) << "WriteMetaTableAsync id: " << request->sequence_id();
    }

    tabletnode::TabletNodeClient meta_node_client(meta_addr);
    meta_node_client.WriteTablet(request, response, done);
}

void MasterImpl::AddMetaCallback(TablePtr table,
                                 std::vector<TabletPtr> tablets,
                                 int32_t retry_times,
                                 const CreateTableRequest* rpc_request,
                                 CreateTableResponse* rpc_response,
                                 google::protobuf::Closure* rpc_done,
                                 WriteTabletRequest* request,
                                 WriteTabletResponse* response,
                                 bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to add to meta tablet: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", "
                << tablets[0] << "...";
        } else {
            LOG(ERROR) << "fail to add to meta tablet: "
                << StatusCodeToString(status) << ", " << tablets[0] << "...";
        }
        if (retry_times <= 0) {
            for(size_t i = 0; i < tablets.size(); i++) {
                DeleteTablet(tablets[i]);
            }
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::AddMetaCallback, this, table,
                           tablets, retry_times - 1, rpc_request, rpc_response,
                           rpc_done, _1, _2, _3, _4);
            SuspendMetaOperation(table, tablets, false, done);
        }
        return;
    }

    rpc_response->set_status(kMasterOk);
    rpc_done->Run();
    LOG(INFO) << "create table " << tablets[0]->GetTableName() << " success";

    for (size_t i = 0; i < tablets.size(); i++) {
        if (tablets[i]->SetStatusIf(kTableOffLine, kTableNotInit)) {
            ProcessOffLineTablet(tablets[i]);
            TryLoadTablet(tablets[i]);
        }
    }
}

void MasterImpl::UpdateTableRecordForDisableCallback(TablePtr table, int32_t retry_times,
                                                     DisableTableResponse* rpc_response,
                                                     google::protobuf::Closure* rpc_done,
                                                     WriteTabletRequest* request,
                                                     WriteTabletResponse* response,
                                                     bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to update meta table: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << table;
        } else {
            LOG(ERROR) << "fail to update meta table: "
                << StatusCodeToString(status) << ", " << table;
        }
        if (retry_times <= 0) {
            LOG(ERROR) << kSms << "abort update meta table, " << table;
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::UpdateTableRecordForDisableCallback, this,
                           table, retry_times - 1, rpc_response, rpc_done, _1, _2, _3, _4);
            SuspendMetaOperation(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2),
                                 false, done);
        }
        return;
    }
    LOG(INFO) << "update meta table success, " << table;
    rpc_response->set_status(kMasterOk);
    rpc_done->Run();

    // unload all tablet of this table
    std::vector<TabletPtr> tablet_meta_list;
    table->GetTablet(&tablet_meta_list);
    for (uint32_t i = 0; i < tablet_meta_list.size(); ++i) {
        TabletPtr tablet = tablet_meta_list[i];
        if (tablet->SetStatusIf(kTableUnLoading, kTableReady, kTableDisable)) {
            UnloadClosure done =
                std::bind(&MasterImpl::UnloadTabletCallback, this, tablet,
                           FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);
            UnloadTabletAsync(tablet, done);
        } else {
            tablet->SetStatusIf(kTabletDisable, kTableOffLine, kTableDisable);
        }
    }
}


void MasterImpl::UpdateTableRecordForEnableCallback(TablePtr table, int32_t retry_times,
                                                    EnableTableResponse* rpc_response,
                                                    google::protobuf::Closure* rpc_done,
                                                    WriteTabletRequest* request,
                                                    WriteTabletResponse* response,
                                                    bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to update meta table: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << table;
        } else {
            LOG(ERROR) << "fail to update meta table: "
                << StatusCodeToString(status) << ", " << table;
        }
        if (retry_times <= 0) {
            LOG(ERROR) << kSms << "abort update meta table, " << table;
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::UpdateTableRecordForEnableCallback, this,
                           table, retry_times - 1, rpc_response, rpc_done, _1, _2, _3, _4);
            SuspendMetaOperation(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2),
                                 false, done);
        }
        return;
    }
    LOG(INFO) << "update meta table success, " << table;
    rpc_response->set_status(kMasterOk);
    rpc_done->Run();

    // load all tablet of this table
    std::vector<TabletPtr> tablet_meta_list;
    table->GetTablet(&tablet_meta_list);
    for (uint32_t i = 0; i < tablet_meta_list.size(); ++i) {
        TabletPtr tablet = tablet_meta_list[i];
        if (tablet->SetStatusIf(kTableOffLine, kTabletDisable, kTableEnable)) {
            TryLoadTablet(tablet, tablet->GetServerAddr());
        } else {
            LOG(ERROR) << "fail to load tablet: " << tablet->GetPath()
                << ", tablet status: " << StatusCodeToString(tablet->GetStatus());
        }
    }
}

void MasterImpl::UpdateTableRecordForUpdateCallback(TablePtr table, int32_t retry_times,
                                                    UpdateTableResponse* rpc_response,
                                                    google::protobuf::Closure* rpc_done,
                                                    WriteTabletRequest* request,
                                                    WriteTabletResponse* response,
                                                    bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to update meta table: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << table;
        } else {
            LOG(ERROR) << "fail to update meta table: "
                << StatusCodeToString(status) << ", " << table;
        }
        if (retry_times <= 0) {
            LOG(ERROR) << kSms << "abort update meta table, " << table;
            table->AbortUpdate();
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
            table->SetSchemaIsSyncing(false);
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::UpdateTableRecordForUpdateCallback, this,
                           table, retry_times - 1, rpc_response, rpc_done, _1, _2, _3, _4);
            SuspendMetaOperation(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2),
                                 false, done);
        }
        return;
    }
    bool is_update_cf = IsUpdateCf(table);
    table->CommitUpdate();
    if ((table->GetStatus() == kTableDisable) // no need to sync
        || !FLAGS_tera_online_schema_update_enabled
        || !is_update_cf) {
        LOG(INFO) << "[update] new table schema is updated: " << table->GetSchema().ShortDebugString();
        table->SetSchemaIsSyncing(false); // releases the schema-sync lock
        rpc_response->set_status(kMasterOk);
        rpc_done->Run();
    } else {
        LOG(INFO) << "[update] online-schema-update";
        table->StoreUpdateRpc(rpc_response, rpc_done);
        table->ResetRangeFragment();
        NoticeTabletNodeSchemaUpdated(table);
    }
}

bool MasterImpl::IsUpdateCf(TablePtr table) {
    TableSchema schema;
    if (table->GetOldSchema(&schema)) {
        return IsSchemaCfDiff(table->GetSchema(), schema);
    }
    return true;
}

void MasterImpl::UpdateTableRecordForRenameCallback(TablePtr table, int32_t retry_times,
                                                    RenameTableResponse* rpc_response,
                                                    google::protobuf::Closure* rpc_done,
                                                    std::string old_alias,
                                                    std::string new_alias,
                                                    WriteTabletRequest* request,
                                                    WriteTabletResponse* response,
                                                    bool failed, int error_code
                                                    ) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to update meta table: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << table;
        } else {
            LOG(ERROR) << "fail to update meta table: "
                << StatusCodeToString(status) << ", " << table;
        }
        if (retry_times <= 0) {
            LOG(ERROR) << kSms << "abort update meta table, " << table;
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::UpdateTableRecordForRenameCallback, this,
                           table, retry_times - 1, rpc_response, rpc_done,
                           old_alias, new_alias, _1, _2, _3, _4);
            SuspendMetaOperation(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2),
                                 false, done);
        }
        return;
    }
    {
        MutexLock locker(&alias_mutex_);
        const std::string& internal_table_name = table->GetSchema().name();
        alias_[new_alias] =  internal_table_name;
        alias_.erase(old_alias);
    }
    LOG(INFO) << "Rename done. update meta table success, " << table;
    rpc_response->set_status(kMasterOk);
    rpc_done->Run();
}

void MasterImpl::UpdateTabletRecordCallback(TabletPtr tablet, int32_t retry_times,
                                            WriteTabletRequest* request,
                                            WriteTabletResponse* response,
                                            bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to update meta tablet: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << tablet;
        } else {
            LOG(ERROR) << "fail to update meta tablet: "
                << StatusCodeToString(status) << ", " << tablet;
        }
        if (retry_times <= 0) {
            LOG(ERROR) << kSms << "abort update meta tablet, " << tablet;
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::UpdateTabletRecordCallback, this,
                           tablet, retry_times - 1, _1, _2, _3, _4);
            SuspendMetaOperation(std::bind(&Tablet::ToMetaTableKeyValue, tablet, _1, _2),
                                 false, done);
        }
        return;
    }
    LOG(INFO) << "update meta tablet success, " << tablet;
}

void MasterImpl::UpdateMetaForLoadCallback(TabletPtr tablet, int32_t retry_times,
                                           WriteTabletRequest* request,
                                           WriteTabletResponse* response,
                                           bool failed, int error_code) {
    CHECK(tablet->GetStatus() == kTableOnLoad);
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    std::string server_addr = tablet->GetServerAddr();
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to update meta tablet: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << tablet;
        } else {
            LOG(ERROR) << "fail to update meta tablet: "
                << StatusCodeToString(status) << ", " << tablet;
        }
        if (retry_times <= 0) {
            LOG(ERROR) << kSms << "abort update meta tablet, " << tablet;
            tablet->SetStatusIf(kTableOffLine, kTableOnLoad);
            ProcessOffLineTablet(tablet);
            // load fail, try reload
            TryLoadTablet(tablet, server_addr);

            // load next tablet
            TabletNodePtr node;
            if (tabletnode_manager_->FindTabletNode(server_addr, &node)
                && node->uuid_ == tablet->GetServerId()) {
                node->FinishLoad(tablet);
                TabletPtr next_tablet;
                while (node->LoadNextWaitTablet(&next_tablet)) {
                    if (next_tablet->SetAddrAndStatusIf(server_addr, kTableOnLoad, kTableOffLine)) {
                        next_tablet->SetServerId(node->uuid_);
                        WriteClosure done =
                            std::bind(&MasterImpl::UpdateMetaForLoadCallback, this,
                                       next_tablet, FLAGS_tera_master_meta_retry_times, _1, _2, _3, _4);
                        BatchWriteMetaTableAsync(std::bind(&Tablet::ToMetaTableKeyValue, next_tablet, _1, _2),
                                                 false, done);
                        break;
                    }
                    node->FinishLoad(next_tablet);
                }
            }
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::UpdateMetaForLoadCallback, this,
                           tablet, retry_times - 1, _1, _2, _3, _4);
            SuspendMetaOperation(std::bind(&Tablet::ToMetaTableKeyValue, tablet, _1, _2),
                                 false, done);
        }
        return;
    }
    LOG(INFO) << "update meta tablet success, " << tablet;

    // if server down here, let split callback take care of status switch
    LoadClosure done = std::bind(&MasterImpl::LoadTabletCallback, this,
                                   tablet, 0, _1, _2, _3, _4);
    LoadTabletAsync(tablet, done);
}

void MasterImpl::DeleteTableCallback(TablePtr table,
                                     std::vector<TabletPtr> tablets,
                                     int32_t retry_times,
                                     DeleteTableResponse* rpc_response,
                                     google::protobuf::Closure* rpc_done,
                                     WriteTabletRequest* request,
                                     WriteTabletResponse* response,
                                     bool failed, int error_code) {
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;
    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to delete table meta: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << table;
        } else {
            LOG(ERROR) << "fail to delete table meta: "
                << StatusCodeToString(status) << ", " << table;
        }
        if (retry_times <= 0) {
            LOG(ERROR) << kSms << "abort delete meta table record, " << table;
            rpc_response->set_status(kMetaTabletError);
            rpc_done->Run();
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::DeleteTableCallback, this, table, tablets,
                           retry_times - 1, rpc_response, rpc_done, _1, _2, _3, _4);
            SuspendMetaOperation(table, tablets, true, done);
        }
        return;
    }
    std::string table_alias = table->GetSchema().alias();
    if (!table_alias.empty()) {
        MutexLock locker(&alias_mutex_);
        alias_.erase(table_alias);
    }
    // clean tablet manager
    for (uint32_t i = 0; i < tablets.size(); ++i) {
        TabletPtr tablet = tablets[i];
        DeleteTablet(tablet);
    }
    gc_strategy_->Clear(table->GetTableName());
    LOG(INFO) << "delete meta table record success, " << table;
    rpc_response->set_status(kMasterOk);
    rpc_done->Run();
}

void MasterImpl::DeleteTablet(TabletPtr tablet) {
    tablet_manager_->DeleteTablet(tablet->GetTableName(), tablet->GetKeyStart());
    tablet_availability_->EraseNotReadyTablet(tablet->GetPath());
}

void MasterImpl::ScanMetaTableAsync(const std::string& table_name,
                                    const std::string& tablet_key_start,
                                    const std::string& tablet_key_end,
                                    ScanClosure done) {
    std::string meta_addr;
    if (!tablet_manager_->GetMetaTabletAddr(&meta_addr)) {
        SuspendMetaOperation(table_name, tablet_key_start, tablet_key_end, done);
        return;
    }

    ScanTabletRequest* request = new ScanTabletRequest;
    ScanTabletResponse* response = new ScanTabletResponse;
    request->set_sequence_id(this_sequence_id_.Inc());
    request->set_table_name(FLAGS_tera_master_meta_table_name);
    std::string scan_key_start, scan_key_end;
    MetaTableScanRange(table_name, tablet_key_start, tablet_key_end,
                       &scan_key_start, &scan_key_end);
    request->set_start(scan_key_start);
    request->set_end(scan_key_end);

    LOG(INFO) << "ScanMetaTableAsync id: " << request->sequence_id() << ", "
        << "table: " << table_name << ", range: ["
        << DebugString(tablet_key_start) << ", " << DebugString(tablet_key_end);
    tabletnode::TabletNodeClient meta_node_client(meta_addr);
    meta_node_client.ScanTablet(request, response, done);
}

void MasterImpl::ScanMetaCallbackForSplit(TabletPtr tablet,
                                          ScanTabletRequest* request,
                                          ScanTabletResponse* response,
                                          bool failed, int error_code) {
    CHECK(tablet->GetStatus() == kTableOnSplit);
    delete request;

    if (failed || response->status() != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to scan meta table: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << tablet;
        } else {
            LOG(ERROR) << "fail to scan meta table: "
                << StatusCodeToString(response->status()) << ", " << tablet;
        }
        ScanClosure done =
            std::bind(&MasterImpl::ScanMetaCallbackForSplit, this, tablet, _1, _2, _3, _4);
        SuspendMetaOperation(tablet->GetTableName(), tablet->GetKeyStart(),
                             tablet->GetKeyEnd(), done);
        delete response;
        return;
    }

    uint32_t record_size = response->results().key_values_size();
    VLOG(5) << "scan meta table: " << record_size << " records";
    if (record_size > 2 || record_size == 0) {
        LOG(ERROR) << kSms << "split into " << record_size << " pieces, "
            << tablet;
        // TryKickTabletNode(tablet->GetServerAddr());
        WriteClosure closure =
                std::bind(&MasterImpl::RepairMetaAfterSplitCallback, this,
                           tablet, response, FLAGS_tera_master_meta_retry_times,
                           _1, _2, _3, _4);
        RepairMetaTableAsync(tablet, response, closure);
        return;
    }

    std::string server_addr = tablet->GetServerAddr();
    const std::string& key_start = tablet->GetKeyStart();
    const std::string& key_end = tablet->GetKeyEnd();

    const KeyValuePair& first_record = response->results().key_values(0);
    TabletMeta first_meta;
    ParseMetaTableKeyValue(first_record.key(), first_record.value(),
                           &first_meta);
    const std::string& first_key_start = first_meta.key_range().key_start();
    const std::string& first_key_end = first_meta.key_range().key_end();

    if (record_size == 1) {
        if (tablet->Verify(first_key_start, first_key_end, first_meta.table_name(),
                           first_meta.path(), first_meta.server_addr())) {
            LOG(WARNING) << "split not complete, " << tablet;
            tablet->SetStatus(kTableOffLine);
            ProcessOffLineTablet(tablet);
            TryLoadTablet(tablet);
            delete response;
        } else {
            LOG(ERROR) << kSms << "split into " << record_size << " pieces, "
                << tablet;
            WriteClosure closure =
                    std::bind(&MasterImpl::RepairMetaAfterSplitCallback, this,
                               tablet, response, FLAGS_tera_master_meta_retry_times,
                               _1, _2, _3, _4);
            RepairMetaTableAsync(tablet, response, closure);
        }
        return;
    }

    const KeyValuePair& second_record = response->results().key_values(1);
    TabletMeta second_meta;
    ParseMetaTableKeyValue(second_record.key(), second_record.value(),
                           &second_meta);
    const std::string& second_key_start = second_meta.key_range().key_start();
    const std::string& second_key_end = second_meta.key_range().key_end();

    if (first_key_start != key_start || first_key_end != second_key_start
        || second_key_end != key_end || key_start >= second_key_start
        || (!key_end.empty() && key_end <= second_key_start)
        || (key_end.empty() && second_key_start.empty())) {
        LOG(ERROR) << kSms << "two splits are not successive, " << tablet;
        // TryKickTabletNode(tablet->GetServerAddr());
        WriteClosure closure =
                std::bind(&MasterImpl::RepairMetaAfterSplitCallback, this,
                           tablet, response, FLAGS_tera_master_meta_retry_times,
                           _1, _2, _3, _4);
        RepairMetaTableAsync(tablet, response, closure);
        return;
    }
    TabletPtr first_tablet, second_tablet;
    // update second child tablet meta
    second_meta.set_status(kTableOffLine);
    tablet_manager_->AddTablet(second_meta, TableSchema(), &second_tablet);

    // delete old tablet
    DeleteTablet(tablet);

    // update first child tablet meta
    first_meta.set_status(kTableOffLine);
    tablet_manager_->AddTablet(first_meta, TableSchema(), &first_tablet);

    tablet_availability_->AddNotReadyTablet(first_tablet->GetPath());
    tablet_availability_->AddNotReadyTablet(second_tablet->GetPath());
    LOG(INFO) << "split finish, " << tablet << ", try load child tablets, "
        << "\nfirst: " << first_meta.ShortDebugString()
        << "\nsecond: " << second_meta.ShortDebugString();
    ProcessOffLineTablet(first_tablet);
    TryLoadTablet(first_tablet, server_addr);
    ProcessOffLineTablet(second_tablet);
    TryLoadTablet(second_tablet, server_addr);
    delete response;
}

void MasterImpl::RepairMetaTableAsync(TabletPtr tablet,
                                      ScanTabletResponse* scan_resp,
                                      WriteClosure done) {
    std::string meta_addr;
    if (!tablet_manager_->GetMetaTabletAddr(&meta_addr)) {
        SuspendMetaOperation(tablet, scan_resp, done);
        return;
    }

    WriteTabletRequest* request = new WriteTabletRequest;
    WriteTabletResponse* response = new WriteTabletResponse;
    request->set_sequence_id(this_sequence_id_.Inc());
    request->set_tablet_name(FLAGS_tera_master_meta_table_name);
    request->set_is_sync(true);
    request->set_is_instant(true);
    // first, erase all invalid record
    for (int32_t i = 0; i < scan_resp->results().key_values_size(); i++) {
        const KeyValuePair& record = scan_resp->results().key_values(i);
        RowMutationSequence* mu_seq = request->add_row_list();
        mu_seq->set_row_key(record.key());
        Mutation* mutation = mu_seq->add_mutation_sequence();
        mutation->set_type(kDeleteRow);
    }
    // then, add the correct record
    std::string packed_key;
    std::string packed_value;
    tablet->ToMetaTableKeyValue(&packed_key, &packed_value);
    RowMutationSequence* mu_seq = request->add_row_list();
    mu_seq->set_row_key(packed_key);
    Mutation* mutation = mu_seq->add_mutation_sequence();
    mutation->set_type(kPut);
    mutation->set_value(packed_value);


    LOG(INFO) << "RepairMetaTableAsync id: " << request->sequence_id() << ", "
        << tablet;
    tabletnode::TabletNodeClient meta_node_client(meta_addr);
    meta_node_client.WriteTablet(request, response, done);
}

void MasterImpl::RepairMetaAfterSplitCallback(TabletPtr tablet,
                                              ScanTabletResponse* scan_resp,
                                              int32_t retry_times,
                                              WriteTabletRequest* request,
                                              WriteTabletResponse* response,
                                              bool failed, int error_code) {
    CHECK(tablet->GetStatus() == kTableOnSplit);
    StatusCode status = response->status();
    if (!failed && status == kTabletNodeOk) {
        // all the row status should be the same
        CHECK_GT(response->row_status_list_size(), 0);
        status = response->row_status_list(0);
    }
    delete request;
    delete response;

    if (failed || status != kTabletNodeOk) {
        if (failed) {
            LOG(ERROR) << "fail to repair meta table: "
                << sofa::pbrpc::RpcErrorCodeToString(error_code) << ", " << tablet;
        } else {
            LOG(ERROR) << "fail to repair meta table: "
                << StatusCodeToString(response->status()) << ", " << tablet;
        }
        if (retry_times <= 0) {
            LOG(ERROR) << kSms << "abort repair meta, " << tablet;
            delete scan_resp;
            // we can still repair it at next split
            tablet->SetStatusIf(kTableOffLine, kTableOnSplit);
            ProcessOffLineTablet(tablet);
            TryLoadTablet(tablet);
        } else {
            WriteClosure done =
                std::bind(&MasterImpl::RepairMetaAfterSplitCallback, this,
                           tablet, scan_resp, retry_times - 1, _1, _2, _3, _4);
            SuspendMetaOperation(tablet, scan_resp, done);
        }
        return;
    }
    LOG(INFO) << "repair meta success, " << tablet;

    delete scan_resp;
    tablet->SetStatusIf(kTableOffLine, kTableOnSplit);
    ProcessOffLineTablet(tablet);
    TryLoadTablet(tablet);
}

void MasterImpl::SuspendMetaOperation(TablePtr table, const std::vector<TabletPtr>& tablets,
                                      bool is_delete, WriteClosure done) {
    std::vector<ToMetaFunc> meta_entries;
    TablePtr null_ptr;
    if (table != null_ptr) {
        meta_entries.push_back(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2));
    }
    for (size_t i = 0; i < tablets.size(); ++i) {
        meta_entries.push_back(std::bind(&Tablet::ToMetaTableKeyValue, tablets[i], _1, _2));
    }
    SuspendMetaOperation(meta_entries, is_delete, done);
}

void MasterImpl::SuspendMetaOperation(ToMetaFunc meta_entry,
                                      bool is_delete, WriteClosure done) {
    std::vector<ToMetaFunc> meta_entries;
    meta_entries.push_back(meta_entry);
    SuspendMetaOperation(meta_entries, is_delete, done);
}

void MasterImpl::SuspendMetaOperation(std::vector<ToMetaFunc> meta_entries,
                                      bool is_delete, WriteClosure done) {
    WriteTask* task = new WriteTask;
    task->type_ = kWrite;
    task->done_ = done;
    task->meta_entries_ = meta_entries;
    task->is_delete_ = is_delete;
    PushToMetaPendingQueue((MetaTask*)task);
}

void MasterImpl::SuspendMetaOperation(const std::string& table_name,
                                      const std::string& tablet_key_start,
                                      const std::string& tablet_key_end,
                                      ScanClosure done) {
    ScanTask* task = new ScanTask;
    task->type_ = kScan;
    task->done_ = done;
    task->table_name_ = table_name;
    task->tablet_key_start_ = tablet_key_start;
    task->tablet_key_end_ = tablet_key_end;
    PushToMetaPendingQueue((MetaTask*)task);
}

void MasterImpl::SuspendMetaOperation(TabletPtr tablet,
                                      ScanTabletResponse* scan_resp,
                                      WriteClosure done) {
    RepairTask* task = new RepairTask;
    task->type_ = kRepair;
    task->tablet_ = tablet;
    task->done_ = done;
    task->scan_resp_ = scan_resp;
    PushToMetaPendingQueue((MetaTask*)task);
}

void MasterImpl::PushToMetaPendingQueue(MetaTask* task) {
    bool reload_meta_table = false;
    meta_task_mutex_.Lock();
    if (meta_task_queue_.empty()) {
        reload_meta_table = true;
    }
    meta_task_queue_.push(task);
    meta_task_mutex_.Unlock();
    if (reload_meta_table) {
        TabletPtr meta_tablet;
        tablet_manager_->FindTablet(FLAGS_tera_master_meta_table_name, "",
                                     &meta_tablet);
        TryMoveTablet(meta_tablet);
    }
}

void MasterImpl::ResumeMetaOperation() {
    meta_task_mutex_.Lock();
    while (!meta_task_queue_.empty()) {
        MetaTask* task = meta_task_queue_.front();
        if (task->type_ == kWrite) {
            WriteTask* write_task = (WriteTask*)task;
            BatchWriteMetaTableAsync(write_task->meta_entries_,
                                     write_task->is_delete_, write_task->done_);
            delete write_task;
        } else if (task->type_ == kScan) {
            ScanTask* scan_task = (ScanTask*)task;
            ScanMetaTableAsync(scan_task->table_name_,
                               scan_task->tablet_key_start_,
                               scan_task->tablet_key_end_, scan_task->done_);
            delete scan_task;
        } else if (task->type_ == kRepair) {
            RepairTask* repair_task = (RepairTask*)task;
            RepairMetaTableAsync(repair_task->tablet_, repair_task->scan_resp_,
                                 repair_task->done_);
            delete repair_task;
        }
        meta_task_queue_.pop();
    }
    meta_task_mutex_.Unlock();
}

void MasterImpl::TryMoveTablet(TabletPtr tablet, const std::string& server_addr, bool in_place) {
    if (!in_place && (tablet->GetServerAddr() == server_addr)) {
        return;
    }
    LOG(INFO) << "Move " << tablet << " from " << tablet->GetServerAddr()
        << " to " << server_addr;
    if (tablet->SetStatusIf(kTableUnLoading, kTableReady)) {
        tablet->SetExpectServerAddr(server_addr);
        TabletNodePtr node;
        if (!server_addr.empty() &&
            tabletnode_manager_->FindTabletNode(server_addr, &node)) {
            node->PlanToMoveIn();
        }
        tablet_availability_->AddNotReadyTablet(tablet->GetPath());
        UnloadClosure done =
            std::bind(&MasterImpl::UnloadTabletCallback, this, tablet,
                       FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);
        UnloadTabletAsync(tablet, done);
    }
}

void MasterImpl::ProcessOffLineTablet(TabletPtr tablet) {
    if (!tablet->IsBound()) {
        return;
    }
    tablet->SetStatusIf(kTabletDisable, kTableOffLine, kTableDisable);
}

void MasterImpl::ProcessReadyTablet(TabletPtr tablet) {
    if (tablet->SetStatusIf(kTableUnLoading, kTableReady, kTableDisable)) {
        UnloadClosure done =
            std::bind(&MasterImpl::UnloadTabletCallback, this, tablet,
                       FLAGS_tera_master_impl_retry_times, _1, _2, _3, _4);
        UnloadTabletAsync(tablet, done);
        return;
    }

    if (FLAGS_tera_online_schema_update_enabled
        && tablet->GetTable()->GetSchemaIsSyncing()
        && !tablet->GetTable()->IsSchemaSyncedAtRange(tablet->GetKeyStart(), tablet->GetKeyEnd())) {
        LOG(INFO) << "[update] tablet ready but schema not synced: " << tablet;
        NoticeTabletNodeSchemaUpdated(tablet);
    }
}

bool MasterImpl::CreateStatTable() {
    master::MasterClient master_client(local_addr_);

    CreateTableRequest request;
    CreateTableResponse response;
    request.set_sequence_id(0);
    request.set_table_name(FLAGS_tera_master_stat_table_name);
    request.set_user_token(user_manager_->UserNameToToken("root"));
    TableSchema* schema = request.mutable_schema();

    schema->set_name(FLAGS_tera_master_stat_table_name);
    schema->set_raw_key(Binary);
    schema->set_split_size(FLAGS_tera_master_stat_table_splitsize);

    LocalityGroupSchema* lg = schema->add_locality_groups();
    lg->set_name("lg0");
    lg->set_store_type(FlashStore);
    lg->set_id(0);

    ColumnFamilySchema* cf = schema->add_column_families();
    cf->set_name("tsinfo");
    cf->set_time_to_live(FLAGS_tera_master_stat_table_ttl);
    cf->set_locality_group("lg0");

    master_client.CreateTable(&request, &response);
    switch (response.status()) {
        case kMasterOk:
            return true;
        case kTableExist:
            return true;
        default:
            return false;
    }
}

void MasterImpl::DumpStatCallBack(RowMutation* mutation) {
    VLOG(15) << "dump stat success:" << mutation->RowKey();
    const ErrorCode& error_code = mutation->GetError();
    if (error_code.GetType() != ErrorCode::kOK) {
        VLOG(15) << "exception occured, reason:" << error_code.GetReason();
    }
    delete mutation;
}

void MasterImpl::DumpTabletNodeAddrToTable(const std::string& addr) {
    std::string key = "#" + addr;
    RowMutation* mutation = stat_table_->NewRowMutation(key);
    mutation->Put("tsinfo", "", "");
    mutation->SetCallBack(&DumpStatCallBack);
    stat_table_->ApplyMutation(mutation);
}

void MasterImpl::DumpStatToTable(const TabletNode& stat) {
    int64_t cur_ts = get_micros() & 0x00FFFFFFFFFFFFFF;
    uint64_t inv_ts = (1UL << 56) - cur_ts;
    {
        MutexLock lock(&stat_table_mutex_);
        if (ts_stat_update_time_[stat.addr_] == 0) {
            stat_table_mutex_.Unlock();
            DumpTabletNodeAddrToTable(stat.addr_);
            stat_table_mutex_.Lock();
        }
        if (cur_ts - ts_stat_update_time_[stat.addr_]
            < FLAGS_tera_master_stat_table_interval * 1000000) {
            return;
        }
        ts_stat_update_time_[stat.addr_] = cur_ts;
    }

    char buf[20];
    std::string key, value;
    snprintf(buf, 20, "%16ld", inv_ts);
    key = stat.addr_ + std::string(buf, 16);
    stat.info_.SerializeToString(&value);

    RowMutation* mutation = stat_table_->NewRowMutation(key);
    mutation->Put("tsinfo", "", value);
    mutation->SetCallBack(&DumpStatCallBack);
    stat_table_->ApplyMutation(mutation);
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

    bool need_gc = gc_strategy_->PreQuery();

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
    gc_strategy_->PostQuery();

    LOG(INFO) << "[gc] try clean trash dir.";
    int64_t start = common::timer::get_micros();
    io::CleanTrashDir();
    int64_t cost = (common::timer::get_micros() - start) / 1000;
    LOG(INFO) << "[gc] clean trash dir done, cost: " << cost << "ms.";

    MutexLock lock(&mutex_);
    if (gc_enabled_) {
        ScheduleTabletNodeGc();
    } else {
        gc_timer_id_ = kInvalidTimerId;
    }
}

void MasterImpl::RenameTable(const RenameTableRequest* request,
                             RenameTableResponse* response,
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
    std::string old_alias = request->old_table_name();
    std::string new_alias = request->new_table_name();
    std::string internal_table_name;

    {
        MutexLock locker(&alias_mutex_);
        if (alias_.find(old_alias) == alias_.end()) {
            LOG(ERROR) << "Fail to reanme, " << old_alias << " not exist";
            response->set_status(kTableNotExist);
            done->Run();
            return;
        } else if (alias_.find(new_alias) != alias_.end()) {
            LOG(ERROR) << "Fail to rename, " << new_alias << "already exist";
            response->set_status(kTableExist);
            done->Run();
            return;
        } else if (new_alias.find("@") != std::string::npos) {
            LOG(ERROR) << "Fail to rename, "
                << new_alias << "contains invalid chars: @";
            response->set_status(kInvalidArgument);
            done->Run();
            return;
        } else if (new_alias.empty()) {
            LOG(ERROR) << "Fail to rename, new alias is empty";
            response->set_status(kInvalidArgument);
            done->Run();
            return;
        } else {
            internal_table_name = alias_[old_alias];
        }
    }

    TablePtr table;
    if (!tablet_manager_->FindTable(internal_table_name, &table)) {
        LOG(ERROR) << "Fail to update table: " << internal_table_name
            << ", table not exist";
        response->set_status(kTableNotExist);
        done->Run();
        return;
    }
    TablePtr table2;
    if (tablet_manager_->FindTable(new_alias, &table2)) {
        LOG(ERROR) << "Fail to rename table to: " << new_alias
            << ", table exist";
        response->set_status(kTableExist);
        done->Run();
        return;
    }
    TableSchema schema;
    schema.CopyFrom(table->GetSchema());
    schema.set_alias(new_alias);
    table->SetSchema(schema);
    // write meta tablet
    WriteClosure closure =
        std::bind(&MasterImpl::UpdateTableRecordForRenameCallback, this, table,
                   FLAGS_tera_master_meta_retry_times, response, done,
                   old_alias, new_alias, _1, _2, _3, _4);
    BatchWriteMetaTableAsync(std::bind(&Table::ToMetaTableKeyValue, table, _1, _2),
                             false, closure);
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

} // namespace master
} // namespace tera
