// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tabletnode/tabletnode_impl.h"

#include <set>
#include <vector>

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gperftools/malloc_extension.h>

#include "db/filename.h"
#include "db/table_cache.h"
#include "io/io_utils.h"
#include "io/utils_leveldb.h"
#include "leveldb/cache.h"
#include "leveldb/env_cache.h"
#include "leveldb/env_dfs.h"
#include "leveldb/env_flash.h"
#include "leveldb/env_inmem.h"
#include "leveldb/slog.h"
#include "leveldb/table_utils.h"
#include "proto/kv_helper.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode_client.h"
#include "tabletnode/tablet_manager.h"
#include "tabletnode/tabletnode_zk_adapter.h"
#include "types.h"
#include "utils/counter.h"
#include "utils/string_util.h"
#include "utils/timer.h"
#include "utils/utils_cmd.h"

DECLARE_string(tera_tabletnode_port);
DECLARE_int64(tera_heartbeat_period);
DECLARE_int64(tera_heartbeat_retry_period_factor);
DECLARE_int32(tera_heartbeat_retry_times);

DECLARE_bool(tera_tabletnode_tcm_cache_release_enabled);
DECLARE_int32(bobby_sofa_server_max_pending_buffer_size);
DECLARE_int32(tera_tabletnode_tcm_cache_release_period);

DECLARE_int32(tera_tabletnode_impl_thread_min_num);
DECLARE_int32(tera_tabletnode_impl_thread_max_num);

DECLARE_bool(tera_zk_enabled);

DECLARE_string(tera_master_meta_table_name);
DECLARE_int32(tera_tabletnode_retry_period);
DECLARE_string(tera_leveldb_log_path);

DECLARE_bool(tera_tabletnode_rpc_limit_enabled);
DECLARE_int32(tera_tabletnode_rpc_limit_max_inflow);
DECLARE_int32(tera_tabletnode_rpc_limit_max_outflow);
DECLARE_int32(tera_tabletnode_rpc_max_pending_buffer_size);
DECLARE_int32(tera_tabletnode_rpc_work_thread_num);
DECLARE_int32(tera_tabletnode_scan_pack_max_size);
DECLARE_int32(tera_tabletnode_block_cache_size);
DECLARE_int32(tera_tabletnode_table_cache_size);
DECLARE_int32(tera_tabletnode_compact_thread_num);
DECLARE_string(tera_tabletnode_path_prefix);

// cache-related
DECLARE_bool(tera_tabletnode_cache_enabled);
DECLARE_string(tera_tabletnode_cache_paths);
DECLARE_int32(tera_tabletnode_cache_block_size);
DECLARE_string(tera_tabletnode_cache_name);
DECLARE_int32(tera_tabletnode_cache_mem_size);
DECLARE_int32(tera_tabletnode_cache_disk_size);
DECLARE_int32(tera_tabletnode_cache_disk_filenum);
DECLARE_int32(tera_tabletnode_cache_log_level);
DECLARE_int32(tera_tabletnode_gc_log_level);

DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_local_addr);
DECLARE_bool(tera_ins_enabled);

DECLARE_bool(tera_io_cache_path_vanish_allowed);

extern tera::Counter range_error_counter;
extern tera::Counter rand_read_delay;

static const int GC_LOG_LEVEL = FLAGS_tera_tabletnode_gc_log_level;

namespace tera {
namespace tabletnode {

TabletNodeImpl::TabletNodeImpl(const TabletNodeInfo& tabletnode_info,
                               TabletManager* tablet_manager)
    : m_status(kNotInited),
      m_tablet_manager(tablet_manager),
      m_zk_adapter(NULL),
      m_release_cache_timer_id(kInvalidTimerId),
      m_sysinfo(tabletnode_info),
      m_thread_pool(new ThreadPool(FLAGS_tera_tabletnode_impl_thread_max_num)) {
    if (FLAGS_tera_local_addr == "") {
        m_local_addr = utils::GetLocalHostName()+ ":" + FLAGS_tera_tabletnode_port;
    } else {
        m_local_addr = FLAGS_tera_local_addr + ":" + FLAGS_tera_tabletnode_port;
    }
    TabletNodeClient::SetThreadPool(m_thread_pool.get());

    leveldb::Env::Default()->SetBackgroundThreads(FLAGS_tera_tabletnode_compact_thread_num);
    leveldb::Env::Default()->RenameFile(FLAGS_tera_leveldb_log_path,
                                        FLAGS_tera_leveldb_log_path + ".bak");
    leveldb::Status s =
        leveldb::Env::Default()->NewLogger(FLAGS_tera_leveldb_log_path, &m_ldb_logger);
    leveldb::Env::Default()->SetLogger(m_ldb_logger);

    m_ldb_block_cache =
        leveldb::NewLRUCache(FLAGS_tera_tabletnode_block_cache_size * 1024UL * 1024);
    m_ldb_table_cache =
        new leveldb::TableCache(FLAGS_tera_tabletnode_table_cache_size);
    if (!s.ok()) {
        m_ldb_logger = NULL;
    }

    if (FLAGS_tera_leveldb_env_type != "local") {
        io::InitDfsEnv();
    }

    InitCacheSystem();

    if (m_tablet_manager.get() == NULL) {
        m_tablet_manager.reset(new TabletManager());
    }

    if (FLAGS_tera_tabletnode_tcm_cache_release_enabled) {
        LOG(INFO) << "enable tcmalloc cache release timer";
        EnableReleaseMallocCacheTimer();
    }
}

TabletNodeImpl::~TabletNodeImpl() {
    if (FLAGS_tera_tabletnode_cache_enabled) {
        leveldb::ThreeLevelCacheEnv::RemoveCachePaths();
    }
}

bool TabletNodeImpl::Init() {
    if (FLAGS_tera_zk_enabled) {
        m_zk_adapter.reset(new TabletNodeZkAdapter(this, m_local_addr));
    } else if(FLAGS_tera_ins_enabled) {
        LOG(INFO) << "ins mode!";
        m_zk_adapter.reset(new InsTabletNodeZkAdapter(this, m_local_addr));
    } else {
        LOG(INFO) << "fake zk mode!";
        m_zk_adapter.reset(new FakeTabletNodeZkAdapter(this, m_local_addr));
    }

    SetTabletNodeStatus(kIsIniting);
    m_thread_pool->AddTask(boost::bind(&TabletNodeZkAdapterBase::Init, m_zk_adapter.get()));
    return true;
}

void TabletNodeImpl::InitCacheSystem() {
    if (!FLAGS_tera_tabletnode_cache_enabled) {
        // compitable with legacy FlashEnv
        leveldb::FlashEnv::SetFlashPath(FLAGS_tera_tabletnode_cache_paths,
                                        FLAGS_tera_io_cache_path_vanish_allowed);
        return;
    }

    LOG(INFO) << "activate new cache system";
    // new cache mechanism
    leveldb::ThreeLevelCacheEnv::SetCachePaths(FLAGS_tera_tabletnode_cache_paths);
    leveldb::ThreeLevelCacheEnv::s_mem_cache_size_in_KB_ = FLAGS_tera_tabletnode_cache_mem_size;
    leveldb::ThreeLevelCacheEnv::s_disk_cache_size_in_MB_ = FLAGS_tera_tabletnode_cache_disk_size;
    leveldb::ThreeLevelCacheEnv::s_block_size_ = FLAGS_tera_tabletnode_cache_block_size;
    leveldb::ThreeLevelCacheEnv::s_disk_cache_file_num_ = FLAGS_tera_tabletnode_cache_disk_filenum;
    leveldb::ThreeLevelCacheEnv::s_disk_cache_file_name_ = FLAGS_tera_tabletnode_cache_name;

    if (FLAGS_tera_tabletnode_cache_log_level < 3) {
        LEVELDB_SET_LOG_LEVEL(WARNING);
    } else if (FLAGS_tera_tabletnode_cache_log_level < 4) {
        LEVELDB_SET_LOG_LEVEL(INFO);
    } else {
        LEVELDB_SET_LOG_LEVEL(DEBUG);
    }
}

bool TabletNodeImpl::Exit() {
    m_thread_pool.reset();

    std::vector<TabletMeta*> tablet_meta_list;
    m_tablet_manager->GetAllTabletMeta(&tablet_meta_list);
    std::vector<TabletMeta*>::iterator it = tablet_meta_list.begin();
    for (; it != tablet_meta_list.end(); ++it) {
        TabletMeta*& tablet_meta = *it;
        StatusCode status = kTableOk;
        bool ret = UnloadTablet(tablet_meta->table_name(),
            tablet_meta->key_range().key_start(),
            tablet_meta->key_range().key_end(), &status);
        LOG(INFO) << "unload tablet [" << tablet_meta->path() << "] return " << ret;
        delete tablet_meta;
    }
    return true;
}

void TabletNodeImpl::LoadTablet(const LoadTabletRequest* request,
                                LoadTabletResponse* response,
                                google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    if (!request->has_session_id() || request->session_id() != GetSessionId()) {
        LOG(WARNING) << "load session id not match: "
            << request->session_id() << ", " << GetSessionId();
        response->set_status(kIllegalAccess);
        done->Run();
        return;
    }
    if (request->schema().locality_groups_size() < 1) {
        LOG(WARNING) << "No localitygroups in schema: " << request->tablet_name();
        response->set_status(kIllegalAccess);
        done->Run();
        return;
    }

    const std::string& key_start = request->key_range().key_start();
    const std::string& key_end = request->key_range().key_end();
    const TableSchema& schema = request->schema();
    // to recover snapshots
    assert(request->snapshots_id_size() == request->snapshots_sequence_size());
    std::map<uint64_t, uint64_t> snapshots;
    int32_t num_of_snapshots = request->snapshots_id_size();
    for (int32_t i = 0; i < num_of_snapshots; ++i) {
        snapshots[request->snapshots_id(i)] = request->snapshots_sequence(i);
    }

    // to recover rollbacks
    assert(request->rollback_snapshots_size() == request->rollback_points_size());
    std::map<uint64_t, uint64_t> rollbacks;
    int32_t num_of_rollbacks = request->rollback_snapshots_size();
    for (int32_t i = 0; i < num_of_rollbacks; ++i) {
        rollbacks[request->rollback_snapshots(i)] = request->rollback_points(i);
    }

    LOG(INFO) << "start load tablet, id: " << request->sequence_id()
        << ", table: " << request->tablet_name()
        << ", range: [" << DebugString(key_start)
        << ", " << DebugString(key_end)
        << "], path: " << request->path()
        << ", parent: " << (request->parent_tablets_size() > 0 ? request->parent_tablets(0) : 0)
        << ", schema: " << request->schema().ShortDebugString();

    std::vector<uint64_t> parent_tablets;
    for (int i = 0; i < request->parent_tablets_size(); ++i) {
        CHECK(i < 2) << "parent_tablets should less than 2: " << i;
        parent_tablets.push_back(request->parent_tablets(i));
    }

    io::TabletIO* tablet_io = NULL;
    StatusCode status = kTabletNodeOk;
    if (!m_tablet_manager->AddTablet(request->tablet_name(), request->path(),
                                     key_start, key_end, &tablet_io, &status)) {
        LOG(ERROR) << "fail to add tablet: " << request->path()
            << " [" << DebugString(key_start) << ", "
            << DebugString(key_end) << "], status: "
            << StatusCodeToString(status);
        response->set_status((StatusCode)tablet_io->GetStatus());
        tablet_io->DecRef();
    } else if (!tablet_io->Load(schema, key_start, key_end,
                                request->path(), parent_tablets, snapshots, rollbacks, 
                                m_ldb_logger, m_ldb_block_cache, m_ldb_table_cache, &status)) {
        tablet_io->DecRef();
        LOG(ERROR) << "fail to load tablet: " << request->path()
            << " [" << DebugString(key_start) << ", "
            << DebugString(key_end) << "], status: "
            << StatusCodeToString(status);
        if (!m_tablet_manager->RemoveTablet(request->tablet_name(), key_start,
                                            key_end, &status)) {
            LOG(ERROR) << "fail to remove tablet: " << request->path()
                << " [" << DebugString(key_start) << ", "
                << DebugString(key_end) << "], status: "
                << StatusCodeToString(status);
        }
        response->set_status(kIOError);
    } else {
        tablet_io->DecRef();
        response->set_status(kTabletNodeOk);
    }

    LOG(INFO) << "load tablet: " << request->path() << " ["
        << DebugString(key_start) << ", " << DebugString(key_end) << "]";
    done->Run();
}

bool TabletNodeImpl::UnloadTablet(const std::string& tablet_name,
                                  const std::string& start,
                                  const std::string& end,
                                  StatusCode* status) {
    io::TabletIO* tablet_io = m_tablet_manager->GetTablet(
        tablet_name, start, end, status);
    if (tablet_io == NULL) {
        LOG(WARNING) << "unload fail to get tablet: " << tablet_name
            << " [" << DebugString(start) << ", " << DebugString(end)
            << "], status: " << StatusCodeToString(*status);
        *status = kKeyNotInRange;
        return false;
    }

    if (!tablet_io->Unload(status)) {
        LOG(ERROR) << "fail to unload tablet: " << tablet_io->GetTablePath()
            << " [" << DebugString(start) << ", " << DebugString(end)
            << "], status: " << StatusCodeToString(*status);
        *status = (StatusCode)tablet_io->GetStatus();
        tablet_io->DecRef();
        return false;
    }
    LOG(INFO) << "unload tablet: " << tablet_io->GetTablePath()
        << " [" << DebugString(start) << ", " << DebugString(end) << "]";
    tablet_io->DecRef();

    if (!m_tablet_manager->RemoveTablet(tablet_name, start, end, status)) {
        LOG(ERROR) << "fail to remove tablet: " << tablet_name
            << " [" << DebugString(start) << ", " << DebugString(end)
            << "], status: " << StatusCodeToString(*status);
    }
    *status = kTabletNodeOk;
    return true;
}

void TabletNodeImpl::UnloadTablet(const UnloadTabletRequest* request,
                                  UnloadTabletResponse* response,
                                  google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    StatusCode status = kTabletNodeOk;
    UnloadTablet(request->tablet_name(), request->key_range().key_start(),
                 request->key_range().key_end(), &status);
    response->set_status(status);
    done->Run();
}

void TabletNodeImpl::CompactTablet(const CompactTabletRequest* request,
                                   CompactTabletResponse* response,
                                   google::protobuf::Closure* done)
{
    response->set_sequence_id(request->sequence_id());
    StatusCode status = kTabletNodeOk;
    io::TabletIO* tablet_io = m_tablet_manager->GetTablet(
        request->tablet_name(), request->key_range().key_start(),
        request->key_range().key_end(), &status);
    if (tablet_io == NULL) {
        LOG(WARNING) << "compact fail to get tablet: " << request->tablet_name()
            << " [" << DebugString(request->key_range().key_start())
            << ", " << DebugString(request->key_range().key_end())
            << "], status: " << StatusCodeToString(status);
        response->set_status(kKeyNotInRange);
        done->Run();
        return;
    }
    LOG(INFO) << "start compact tablet: " << tablet_io->GetTablePath()
        << " [" << DebugString(tablet_io->GetStartKey())
        << ", " << DebugString(tablet_io->GetEndKey()) << "]";

    tablet_io->Compact(&status);
    CompactStatus compact_status = tablet_io->GetCompactStatus();
    response->set_status(status);
    response->set_compact_status(compact_status);
    int64_t compact_size = tablet_io->GetDataSize();
    response->set_compact_size(compact_size);
    LOG(INFO) << "compact tablet: " << tablet_io->GetTablePath()
        << " [" << DebugString(tablet_io->GetStartKey())
        << ", " << DebugString(tablet_io->GetEndKey())
        << "], status: " << StatusCodeToString(status)
        << ", compacted size: " << compact_size;
    tablet_io->DecRef();
    done->Run();
}

void TabletNodeImpl::ReadTablet(int64_t start_micros,
                                const ReadTabletRequest* request,
                                ReadTabletResponse* response,
                                google::protobuf::Closure* done,
                                ReadRpcTimer* timer) {
    int32_t row_num = request->row_info_list_size();
    uint64_t snapshot_id = request->snapshot_id() == 0 ? 0 : request->snapshot_id();
    uint32_t read_success_num = 0;

    for (int32_t i = 0; i < row_num; i++) {
        StatusCode row_status = kTabletNodeOk;
        io::TabletIO* tablet_io = m_tablet_manager->GetTablet(
            request->tablet_name(), request->row_info_list(i).key(), &row_status);
        if (tablet_io == NULL) {
            range_error_counter.Inc();
            response->mutable_detail()->add_status(kKeyNotInRange);
        } else {
            if (tablet_io->ReadCells(request->row_info_list(i),
                                     response->mutable_detail()->add_row_result(),
                                     snapshot_id, &row_status)) {
                read_success_num++;
            } else {
                response->mutable_detail()->mutable_row_result()->RemoveLast();
            }
            tablet_io->DecRef();
            response->mutable_detail()->add_status(row_status);
        }
    }

    VLOG(10) << "seq_id: " << request->sequence_id()
        << ", req_row: " << row_num
        << ", read_suc: " << read_success_num;
    response->set_sequence_id(request->sequence_id());
    response->set_success_num(read_success_num);
    response->set_status(kTabletNodeOk);
    done->Run();

    if (NULL != timer) {
        RpcTimerList::Instance()->Erase(timer);
        delete timer;
    }

    int64_t now_ms = get_micros();
    int64_t used_ms =  now_ms - start_micros;
    if (used_ms <= 0) {
        LOG(ERROR) << "now ms: "<< now_ms << " start_ms: "<< start_micros;
    }
    rand_read_delay.Add(used_ms);
}

void TabletNodeImpl::WriteTablet(const WriteTabletRequest* request,
                                 WriteTabletResponse* response,
                                 google::protobuf::Closure* done,
                                 WriteRpcTimer* timer) {
    response->set_sequence_id(request->sequence_id());
    StatusCode status = kTabletNodeOk;

    std::map<io::TabletIO*, std::vector<int32_t>* > req_index_map;
    std::map<io::TabletIO*, std::vector<int32_t>* >::iterator it;

    int32_t row_num = request->row_list_size();
    if (request->row_list_size() > 0) {
        for (int32_t i = 0; i < row_num; i++) {
            io::TabletIO* tablet_io = m_tablet_manager->GetTablet(
                request->tablet_name(), request->row_list(i).row_key(), &status);
            it = req_index_map.find(tablet_io);
            if (it == req_index_map.end()) {
                // keep one ref to tablet_io
                std::vector<int32_t>* index_list = new std::vector<int32_t>;
                req_index_map[tablet_io] = index_list;
                index_list->push_back(i);
            } else {
                if (tablet_io != NULL) {
                    tablet_io->DecRef();
                }
                std::vector<int32_t>* index_list = it->second;
                index_list->push_back(i);
            }
        }
    } else {
        response->set_status(kTabletNodeOk);
        done->Run();
        if (NULL != timer) {
            RpcTimerList::Instance()->Erase(timer);
            delete timer;
        }
        return;
    }

    if (req_index_map.size() == 1 && req_index_map.begin()->first == NULL) {
        range_error_counter.Inc();
        response->set_status(kKeyNotInRange);
        done->Run();
        if (NULL != timer) {
            RpcTimerList::Instance()->Erase(timer);
            delete timer;
        }
        delete req_index_map.begin()->second;
        return;
    }

    // reserve response status list space
    response->set_status(kTabletNodeOk);
    response->mutable_row_status_list()->Reserve(row_num);
    for (int32_t i = 0; i < row_num; i++) {
        response->mutable_row_status_list()->AddAlreadyReserved();
    }

    Counter* done_counter = new Counter;
    for (it = req_index_map.begin(); it != req_index_map.end(); ++it) {
        io::TabletIO* tablet_io = it->first;
        std::vector<int32_t>* index_list = it->second;
        int32_t index_num = index_list->size();
        CHECK(index_num > 0);
        StatusCode status = kTabletNodeOk;
        if (tablet_io == NULL) {
            for (int32_t i = 0; i < index_num; i++) {
                int32_t index = (*index_list)[i];
                response->mutable_row_status_list()->Set(index, kKeyNotInRange);
            }
            delete index_list;
            done_counter->Add(index_num);
            // NULL must be the 1st item in req_index_map, so there is
            // no need to call done->Run().
        } else if (!tablet_io->Write(request, response, done, index_list,
                                     done_counter, timer, &status)) {
            tablet_io->DecRef();
            for (int32_t i = 0; i < index_num; i++) {
                int32_t index = (*index_list)[i];
                response->mutable_row_status_list()->Set(index, status);
            }
            delete index_list;
            if (done_counter->Add(index_num) == row_num) {
                done->Run();
                delete done_counter;
                if (NULL != timer) {
                    RpcTimerList::Instance()->Erase(timer);
                    delete timer;
                }
            }
        } else {
            tablet_io->DecRef();
        }
    }
}

void TabletNodeImpl::GetSnapshot(const SnapshotRequest* request,
                                 SnapshotResponse* response,
                                 google::protobuf::Closure* done) {
    StatusCode status = kTabletNodeOk;
    io::TabletIO* tablet_io = m_tablet_manager->GetTablet(request->table_name(),
                                                          request->key_range().key_start(),
                                                          request->key_range().key_end(),
                                                          &status);
    if (tablet_io == NULL) {
        LOG(WARNING) << "get snapshot fail to get tablet: " << request->table_name()
            << " [" << DebugString(request->key_range().key_start())
            << ", " << DebugString(request->key_range().key_end())
            << "], status: " << StatusCodeToString(status);
        response->set_status(kKeyNotInRange);
        done->Run();
        return;
    }
    uint64_t snapshot = tablet_io->GetSnapshot(request->snapshot_id(), (0x1ull << 56) - 1, &status);
    if (status != kTabletNodeOk) {
        response->set_status(status);
    } else if (snapshot == 0) {
        LOG(ERROR) << "fail to get snapshot: " << tablet_io->GetTablePath()
            << " [" << DebugString(tablet_io->GetStartKey())
            << ", " << DebugString(tablet_io->GetEndKey()) << "]";
        response->set_status(kSnapshotNotExist);
    } else {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kTabletNodeOk);
        response->set_snapshot_seq(snapshot);
        LOG(INFO) << "get snapshot: " << snapshot
            << ", tablet: " << tablet_io->GetTablePath()
            << " [" << DebugString(tablet_io->GetStartKey())
            << ", " << DebugString(tablet_io->GetEndKey()) << "]";
    }
    tablet_io->DecRef();
    done->Run();
}

void TabletNodeImpl::ReleaseSnapshot(const ReleaseSnapshotRequest* request,
                                     ReleaseSnapshotResponse* response,
                                     google::protobuf::Closure* done) {
    StatusCode status = kTabletNodeOk;
    io::TabletIO* tablet_io = m_tablet_manager->GetTablet(request->table_name(),
                                                          request->key_range().key_start(),
                                                          request->key_range().key_end(),
                                                          &status);
    if (tablet_io == NULL) {
        LOG(WARNING) << "release snapshot fail to get tablet: " << request->table_name()
            << " [" << DebugString(request->key_range().key_start())
            << ", " << DebugString(request->key_range().key_end())
            << "], status: " << StatusCodeToString(status);
        response->set_status(kKeyNotInRange);
        done->Run();
        return;
    }
    response->set_sequence_id(request->sequence_id());
    if (tablet_io->ReleaseSnapshot(request->snapshot_id(), &status)) {
        response->set_status(kTabletNodeOk);
        LOG(INFO) << "released snapshot: " << request->snapshot_id()
            << ", tablet: " << tablet_io->GetTablePath()
            << " [" << DebugString(tablet_io->GetStartKey())
            << ", " << DebugString(tablet_io->GetEndKey()) << "]";
    } else {
        response->set_status(status);
        LOG(ERROR) << "release snapshot fail: " << request->snapshot_id()
                << ", tablet: " << tablet_io->GetTablePath()
                << " [" << DebugString(tablet_io->GetStartKey())
                << ", " << DebugString(tablet_io->GetEndKey())
                << "], status: " << StatusCodeToString(status);
    }
    tablet_io->DecRef();
    response->set_snapshot_id(request->snapshot_id());
    done->Run();
}

void TabletNodeImpl::Rollback(const SnapshotRollbackRequest* request, SnapshotRollbackResponse* response,
                              google::protobuf::Closure* done) {
    StatusCode status = kTabletNodeOk;
    io::TabletIO* tablet_io = m_tablet_manager->GetTablet(request->table_name(),
                                                          request->key_range().key_start(),
                                                          request->key_range().key_end(),
                                                          &status);
    if (tablet_io == NULL) {
        LOG(WARNING) << "rollback to snapshot fail to get tablet: " << request->table_name()
            << " [" << DebugString(request->key_range().key_start())
            << ", " << DebugString(request->key_range().key_end())
            << "], status: " << StatusCodeToString(status);
        response->set_status(kKeyNotInRange);
        done->Run();
        return;
    }
    uint64_t rollback_point = tablet_io->Rollback(request->snapshot_id(), &status);
    if (status != kTabletNodeOk) {
        response->set_status(status);
    } else {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kTabletNodeOk);
        response->set_rollback_point(rollback_point);
        LOG(INFO) << "rollback point " << rollback_point << " to snapshot: " << request->snapshot_id()
            << ", tablet: " << tablet_io->GetTablePath()
            << " [" << DebugString(tablet_io->GetStartKey())
            << ", " << DebugString(tablet_io->GetEndKey()) << "]";
    }
    tablet_io->DecRef();
    done->Run();
}

void TabletNodeImpl::Query(const QueryRequest* request,
                           QueryResponse* response,
                           google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    response->set_status(kTabletNodeOk);

    TabletNodeInfo* ts_info = response->mutable_tabletnode_info();
    m_sysinfo.GetTabletNodeInfo(ts_info);
    TabletMetaList* meta_list = response->mutable_tabletmeta_list();
    m_sysinfo.GetTabletMetaList(meta_list);

    if (request->has_is_gc_query() && request->is_gc_query()) {
        std::vector<InheritedLiveFiles> inherited;
        GetInheritedLiveFiles(inherited);
        for (size_t i = 0; i < inherited.size(); ++i) {
            InheritedLiveFiles* files = response->add_inh_live_files();
            *files = inherited[i];
        }
    }
    done->Run();
}

void TabletNodeImpl::RefreshSysInfo() {
    int64_t cur_ts = get_micros();

    m_sysinfo.CollectTabletNodeInfo(m_tablet_manager.get(), m_local_addr);
    m_sysinfo.CollectHardwareInfo();
    m_sysinfo.SetTimeStamp(cur_ts);

    VLOG(15) << "collect sysinfo finished, time used: " << get_micros() - cur_ts << " us.";
}

void TabletNodeImpl::ScanTablet(const ScanTabletRequest* request,
                                ScanTabletResponse* response,
                                google::protobuf::Closure* done) {
    const int64_t PACK_MAX_SIZE =
        static_cast<int64_t>(FLAGS_tera_tabletnode_scan_pack_max_size)<<10;
    //const std::string& start_key = request->key_range().key_start();
    //const std::string& end_key = request->key_range().key_end();
    int64_t buffer_limit = request->buffer_limit();
    if (buffer_limit > PACK_MAX_SIZE) {
        buffer_limit = PACK_MAX_SIZE;
    }
    //VLOG(5) << "ScanTablet() start=[" << start_key
    //    << "], end=[" << end_key << "]";
    if (request->has_sequence_id()) {
        response->set_sequence_id(request->sequence_id());
    }
    StatusCode status = kTabletNodeOk;
    io::TabletIO* tablet_io = NULL;
    tablet_io = m_tablet_manager->GetTablet(request->table_name(),
                                            request->start(), &status);

    if (tablet_io == NULL) {
        range_error_counter.Inc();
        response->set_status(status);
        done->Run();
    } else {
        response->set_end(tablet_io->GetEndKey());
        tablet_io->ScanRows(request, response, done);
        tablet_io->DecRef();
    }
}

void TabletNodeImpl::SplitTablet(const SplitTabletRequest* request,
                                 SplitTabletResponse* response,
                                 google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());

    std::string split_key, path;
    StatusCode status = kTabletNodeOk;
    io::TabletIO* tablet_io = m_tablet_manager->GetTablet(request->tablet_name(),
                                                request->key_range().key_start(),
                                                request->key_range().key_end(),
                                                &status);
    if (tablet_io == NULL) {
        LOG(WARNING) << "split fail to get tablet: " << request->tablet_name()
            << " [" << DebugString(request->key_range().key_start())
            << ", " << DebugString(request->key_range().key_end())
            << "], status: " << StatusCodeToString(status);
        response->set_status(kKeyNotInRange);
        done->Run();
        return;
    }

    if (!tablet_io->Split(&split_key, &status)) {
        LOG(ERROR) << "fail to split tablet: " << tablet_io->GetTablePath()
            << " [" << DebugString(tablet_io->GetStartKey())
            << ", " << DebugString(tablet_io->GetEndKey())
            << "], status: " << StatusCodeToString(status);
        if (status == kTableNotSupport) {
            response->set_status(kTableNotSupport);
        } else {
            response->set_status((StatusCode)tablet_io->GetStatus());
        }
        tablet_io->DecRef();
        done->Run();
        return;
    }
    int64_t first_half_size =
        tablet_io->GetDataSize(request->key_range().key_start(), split_key);
    int64_t second_half_size =
        tablet_io->GetDataSize(split_key, request->key_range().key_end());
    LOG(INFO) << "split tablet: " << tablet_io->GetTablePath()
        << " [" << DebugString(tablet_io->GetStartKey())
        << ", " << DebugString(tablet_io->GetEndKey())
        << "], split key: " << DebugString(split_key);

    if (!tablet_io->Unload(&status)) {
        LOG(ERROR) << "fail to unload tablet: " << tablet_io->GetTablePath()
            << " [" << DebugString(tablet_io->GetStartKey())
            << ", " << DebugString(tablet_io->GetEndKey())
            << "], status: " << StatusCodeToString(status);
        response->set_status((StatusCode)tablet_io->GetStatus());
        tablet_io->DecRef();
        done->Run();
        return;
    }
    TableSchema schema;
    schema.CopyFrom(tablet_io->GetSchema());
    path = tablet_io->GetTablePath();
    LOG(INFO) << "unload tablet: " << tablet_io->GetTablePath()
        << " [" << DebugString(tablet_io->GetStartKey())
        << ", " << DebugString(tablet_io->GetEndKey()) << "]";
    tablet_io->DecRef();

    if (!m_tablet_manager->RemoveTablet(request->tablet_name(),
                                        request->key_range().key_start(),
                                        request->key_range().key_end(),
                                        &status)) {
        LOG(ERROR) << "fail to remove tablet: " << request->tablet_name()
                << " [" << DebugString(request->key_range().key_start())
                << ", " << DebugString(request->key_range().key_end())
                << "], status: " << StatusCodeToString(status);
    }

    UpdateMetaTableAsync(request, response, done, path, split_key, schema,
                         first_half_size, second_half_size, request->tablet_meta());
}

bool TabletNodeImpl::CheckInKeyRange(const KeyList& key_list,
                                     const std::string& key_start,
                                     const std::string& key_end) {
    for (int32_t i = 0; i < key_list.size(); ++i) {
        const std::string& key = key_list.Get(i);
        if (key < key_start || (key_end != "" && key >= key_end)) {
            return false;
        }
    }
    return true;
}

bool TabletNodeImpl::CheckInKeyRange(const KeyValueList& pair_list,
                                     const std::string& key_start,
                                     const std::string& key_end) {
    for (int32_t i = 0; i < pair_list.size(); ++i) {
        const std::string& key = pair_list.Get(i).key();
        if (key < key_start || (key_end != "" && key >= key_end)) {
            return false;
        }
    }
    return true;
}

bool TabletNodeImpl::CheckInKeyRange(const RowReaderList& reader_list,
                                     const std::string& key_start,
                                     const std::string& key_end) {
    for (int32_t i = 0; i < reader_list.size(); ++i) {
        const std::string& key = reader_list.Get(i).key();
        if (key < key_start || (key_end != "" && key >= key_end)) {
            return false;
        }
    }
    return true;
}

bool TabletNodeImpl::CheckInKeyRange(const RowMutationList& row_list,
                                     const std::string& key_start,
                                     const std::string& key_end) {
    for (int32_t i = 0; i < row_list.size(); ++i) {
        const std::string& key = row_list.Get(i).row_key();
        if (key < key_start || (key_end != "" && key >= key_end)) {
            return false;
        }
    }
    return true;
}


/////////// common ////////////

void TabletNodeImpl::EnterSafeMode() {
    SetTabletNodeStatus(kIsReadonly);
}

void TabletNodeImpl::LeaveSafeMode() {
    SetTabletNodeStatus(kIsRunning);
}

void TabletNodeImpl::ExitService() {
    LOG(FATAL) << "master kick me!";
    exit(1);
}

void TabletNodeImpl::SetTabletNodeStatus(const TabletNodeStatus& status) {
    MutexLock lock(&m_status_mutex);
    m_status = status;
}

TabletNodeImpl::TabletNodeStatus TabletNodeImpl::GetTabletNodeStatus() {
    MutexLock lock(&m_status_mutex);
    return m_status;
}

void TabletNodeImpl::SetRootTabletAddr(const std::string& root_tablet_addr) {
    m_root_tablet_addr = root_tablet_addr;
}

void TabletNodeImpl::UpdateMetaTableAsync(const SplitTabletRequest* rpc_request,
         SplitTabletResponse* rpc_response, google::protobuf::Closure* rpc_done,
         const std::string& path, const std::string& key_split,
         const TableSchema& schema, int64_t first_size, int64_t second_size,
         const TabletMeta& meta) {
    WriteTabletRequest* request = new WriteTabletRequest;
    WriteTabletResponse* response = new WriteTabletResponse;
    request->set_sequence_id(m_this_sequence_id++);
    request->set_tablet_name(FLAGS_tera_master_meta_table_name);
    request->set_is_sync(true);
    request->set_is_instant(true);

    TabletMeta tablet_meta;
    tablet_meta.CopyFrom(meta);
    tablet_meta.set_server_addr(m_local_addr);
    tablet_meta.clear_parent_tablets();
    tablet_meta.add_parent_tablets(leveldb::GetTabletNumFromPath(path));

    std::string meta_key, meta_value;
    VLOG(5) << "update meta for split tablet: " << path
        << " [" << DebugString(rpc_request->key_range().key_start())
        << ", " << DebugString(rpc_request->key_range().key_end()) << "]";

    CHECK(2 == rpc_request->child_tablets_size());
    // first write 2nd half
    tablet_meta.set_path(leveldb::GetChildTabletPath(path, rpc_request->child_tablets(0)));
    tablet_meta.set_table_size(second_size);
    tablet_meta.mutable_key_range()->set_key_start(key_split);
    tablet_meta.mutable_key_range()->set_key_end(rpc_request->key_range().key_end());
    MakeMetaTableKeyValue(tablet_meta, &meta_key, &meta_value);
    RowMutationSequence* mu_seq = request->add_row_list();
    mu_seq->set_row_key(meta_key);
    Mutation* mutation = mu_seq->add_mutation_sequence();
    mutation->set_type(kPut);
    mutation->set_value(meta_value);
    VLOG(5) << "write meta: key [" << DebugString(meta_key)
        << "], value_size: " << meta_value.size();

    // then write 1st half
    // update root_tablet_addr in fake zk mode
    if (!FLAGS_tera_zk_enabled) {
        m_zk_adapter->GetRootTableAddr(&m_root_tablet_addr);
    }
    TabletNodeClient meta_tablet_client(m_root_tablet_addr);

    tablet_meta.set_path(leveldb::GetChildTabletPath(path, rpc_request->child_tablets(1)));
    tablet_meta.set_table_size(first_size);
    tablet_meta.mutable_key_range()->set_key_start(rpc_request->key_range().key_start());
    tablet_meta.mutable_key_range()->set_key_end(key_split);
    MakeMetaTableKeyValue(tablet_meta, &meta_key, &meta_value);
    mu_seq = request->add_row_list();
    mu_seq->set_row_key(meta_key);
    mutation = mu_seq->add_mutation_sequence();
    mutation->set_type(kPut);
    mutation->set_value(meta_value);
    VLOG(5) << "write meta: key [" << DebugString(meta_key)
        << "], value_size: " << meta_value.size();

    Closure<void, WriteTabletRequest*, WriteTabletResponse*, bool, int>* done =
        NewClosure(this, &TabletNodeImpl::UpdateMetaTableCallback, rpc_request,
                   rpc_response, rpc_done);
    meta_tablet_client.WriteTablet(request, response, done);
}


void TabletNodeImpl::UpdateMetaTableCallback(const SplitTabletRequest* rpc_request,
         SplitTabletResponse* rpc_response, google::protobuf::Closure* rpc_done,
         WriteTabletRequest* request, WriteTabletResponse* response, bool failed,
         int error_code) {
    if (failed) {
        rpc_response->set_status(kMetaTabletError);
    } else if (response->status() != kTabletNodeOk) {
        LOG(ERROR) << "fail to update meta for tablet: "
            << request->tablet_name() << " ["
            << DebugString(rpc_request->key_range().key_start())
            << ", " << DebugString(rpc_request->key_range().key_end())
            << "], status: " << StatusCodeToString(response->status());
        rpc_response->set_status(kMetaTabletError);
    } else {
        LOG(INFO) << "split tablet success: " << rpc_request->tablet_name()
            << " [" << DebugString(rpc_request->key_range().key_start())
            << ", " << DebugString(rpc_request->key_range().key_end()) << "]";
        rpc_response->set_status(kTabletNodeOk);
    }

    delete request;
    delete response;
    rpc_done->Run();
}

/*
 * all cached tablets/files:
 * ------------------------------------------
 * | active tablets  |   inactive tablets   |
 * |                 |                      |
 * |                 |    all    |    to    |
 * |                 | inherited | *DELETE* |
 * |                 |    files  |          |
 * ------------------------------------------
 */
void TabletNodeImpl::GarbageCollect() {
    if (FLAGS_tera_tabletnode_cache_enabled) {
        return;
    }
    int64_t start_ms = get_micros();
    LOG(INFO) << "[gc] start...";

    // get all inherited sst files
    std::vector<InheritedLiveFiles> table_files;
    GetInheritedLiveFiles(table_files);
    std::set<std::string> inherited_files;
    for (size_t t = 0; t < table_files.size(); ++t) {
        const InheritedLiveFiles& live = table_files[t];
        int lg_num = live.lg_live_files_size();
        for (int lg = 0; lg < lg_num; ++lg) {
            const LgInheritedLiveFiles& lg_live_files = live.lg_live_files(lg);
            for (int f = 0; f < lg_live_files.file_number_size(); ++f) {
                std::string file_path = leveldb::BuildTableFilePath(
                    live.table_name(), lg, lg_live_files.file_number(f));
                inherited_files.insert(file_path);
                // file_path : table-name/tablet-xxx/lg-num/xxx.sst
                VLOG(GC_LOG_LEVEL) << "[gc] inherited live file: " << file_path;
            }
        }
    }

    // get all active tablets
    std::vector<TabletMeta*> tablet_meta_list;
    std::set<std::string> active_tablets;
    m_tablet_manager->GetAllTabletMeta(&tablet_meta_list);
    std::vector<TabletMeta*>::iterator it = tablet_meta_list.begin();
    for (; it != tablet_meta_list.end(); ++it) {
        VLOG(GC_LOG_LEVEL) << "[gc] Active Tablet: " << (*it)->path();
        active_tablets.insert((*it)->path());
        delete (*it);
    }

    // collect flash directories
    const std::vector<std::string>& flash_paths = leveldb::FlashEnv::GetFlashPaths();
    for (size_t d = 0; d < flash_paths.size(); ++d) {
        std::string flash_dir = flash_paths[d] + FLAGS_tera_tabletnode_path_prefix;
        GarbageCollectInPath(flash_dir, leveldb::Env::Default(),
                             inherited_files, active_tablets);
    }

    // collect memory env
    leveldb::Env* mem_env = io::LeveldbMemEnv()->CacheEnv();
    GarbageCollectInPath(FLAGS_tera_tabletnode_path_prefix, mem_env,
                         inherited_files, active_tablets);

    LOG(INFO) << "[gc] finished, time used: " << get_micros() - start_ms << " us.";
}

void TabletNodeImpl::GarbageCollectInPath(const std::string& path, leveldb::Env* env,
                                          const std::set<std::string>& inherited_files,
                                          const std::set<std::string> active_tablets) {
    std::vector<std::string> table_dirs;
    env->GetChildren(path, &table_dirs);
    for (size_t i = 0; i < table_dirs.size(); ++i) {
        std::vector<std::string> cached_tablets;
        env->GetChildren(path + "/" + table_dirs[i], &cached_tablets);
        if (cached_tablets.size() == 0) {
            VLOG(GC_LOG_LEVEL) << "[gc] this directory is empty, delete it: "
                << path + "/" + table_dirs[i];
            env->DeleteDir(path + "/" + table_dirs[i]);
            continue;
        }
        for (size_t j = 0; j < cached_tablets.size(); ++j) {
            std::string tablet_dir = table_dirs[i] + "/" + cached_tablets[j];
            VLOG(GC_LOG_LEVEL) << "[gc] Cached Tablet: " << tablet_dir;
            if (active_tablets.find(tablet_dir) != active_tablets.end()) {
                // active tablets
                continue;
            }
            std::string inactive_tablet_dir = path + "/" + tablet_dir;
            VLOG(GC_LOG_LEVEL) << "[gc] inactive_tablet directory:" << inactive_tablet_dir;
            std::vector<std::string> lgs;
            env->GetChildren(inactive_tablet_dir, &lgs);
            if (lgs.size() == 0) {
                VLOG(GC_LOG_LEVEL) << "[gc] this directory is empty, delete it: " << inactive_tablet_dir;
                env->DeleteDir(inactive_tablet_dir);
                continue;
            }
            for (size_t lg = 0; lg < lgs.size(); ++lg) {
                std::vector<std::string> files;
                env->GetChildren(inactive_tablet_dir + "/" + lgs[lg], &files);
                if (files.size() == 0) {
                    VLOG(GC_LOG_LEVEL) << "[gc] this directory is empty, delete it: "
                        << inactive_tablet_dir + "/" + lgs[lg];
                    env->DeleteDir(inactive_tablet_dir + "/" + lgs[lg]);
                    continue;
                }
                for (size_t f = 0; f < files.size(); ++f) {
                    std::string file = files[f];
                    std::string pathname = inactive_tablet_dir + "/" + lgs[lg] + "/" + file;
                    if (inherited_files.find(tablet_dir + "/" + lgs[lg] + "/" + file) == inherited_files.end()) {
                        VLOG(GC_LOG_LEVEL) << "[gc] delete sst file: " << pathname;
                        env->DeleteFile(pathname);

                    } else {
                        VLOG(GC_LOG_LEVEL) << "[gc] skip inherited file: " << pathname;
                    }
                } // sst file
            } // lg
        } // tablet
    } // table

}

void TabletNodeImpl::SetSessionId(const std::string& session_id) {
    MutexLock lock(&m_status_mutex);
    m_session_id = session_id;
}

std::string TabletNodeImpl::GetSessionId() {
    MutexLock lock(&m_status_mutex);
    return m_session_id;
}

double TabletNodeImpl::GetBlockCacheHitRate() {
    return m_ldb_block_cache->HitRate();
}

TabletNodeSysInfo& TabletNodeImpl::GetSysInfo() {
    return m_sysinfo;
}

void TabletNodeImpl::TryReleaseMallocCache() {
    LOG(INFO) << "TryReleaseMallocCache()";
    size_t free_heap_bytes = 0;
    MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes",
                                                    &free_heap_bytes);
    if (free_heap_bytes == 0) {
        return;
    }

    VLOG(5) << "tcmalloc cache size: " << free_heap_bytes;

    if (free_heap_bytes < 10 * 1024 * 1024) {
        MallocExtension::instance()->ReleaseFreeMemory();
        VLOG(5) << "release tcmalloc cache size: " << free_heap_bytes;
    } else {
        // have workload
        MallocExtension::instance()->ReleaseToSystem(free_heap_bytes / 2);
        VLOG(5) << "release tcmalloc cache size: " << free_heap_bytes / 2;
    }
}

void TabletNodeImpl::ReleaseMallocCache() {
    MutexLock locker(&m_mutex);

    TryReleaseMallocCache();

    m_release_cache_timer_id = kInvalidTimerId;
    EnableReleaseMallocCacheTimer();
}

void TabletNodeImpl::EnableReleaseMallocCacheTimer(int32_t expand_factor) {
    assert(m_release_cache_timer_id == kInvalidTimerId);
    boost::function<void ()> closure =
        boost::bind(&TabletNodeImpl::ReleaseMallocCache, this);
    int64_t timeout_period = expand_factor * 1000 *
        FLAGS_tera_tabletnode_tcm_cache_release_period;
    m_release_cache_timer_id = m_thread_pool->DelayTask(timeout_period, closure);
}

void TabletNodeImpl::DisableReleaseMallocCacheTimer() {
    if (m_release_cache_timer_id != kInvalidTimerId) {
        m_thread_pool->CancelTask(m_release_cache_timer_id);
        m_release_cache_timer_id = kInvalidTimerId;
    }
}

void TabletNodeImpl::GetInheritedLiveFiles(std::vector<InheritedLiveFiles>& inherited) {
    std::set<std::string> not_ready_tables;
    typedef std::vector<std::set<uint64_t> > TableSet;
    std::map<std::string, TableSet> live;

    std::vector<io::TabletIO*> tablet_ios;
    m_tablet_manager->GetAllTablets(&tablet_ios);
    std::vector<io::TabletIO*>::iterator it = tablet_ios.begin();
    for (; it != tablet_ios.end(); ++it) {
        io::TabletIO* tablet_io = *it;
        const std::string& tablename = tablet_io->GetTableName();
        if (not_ready_tables.find(tablename) == not_ready_tables.end()
            && !tablet_io->AddInheritedLiveFiles(&live[tablename])) {
            VLOG(10) << "[gc] " << tablet_io->GetTablePath() << " is not ready, skip it.";
            not_ready_tables.insert(tablename);
            live[tablename].clear();
        }
        tablet_io->DecRef();
    }

    int total = 0;
    std::map<std::string, TableSet>::iterator live_it = live.begin();
    for (; live_it != live.end(); ++live_it) {
        VLOG(10) << "[gc] add inherited file, table " << live_it->first;
        if (not_ready_tables.find(live_it->first) != not_ready_tables.end()) {
            VLOG(10) << "[gc] table: " << live_it->first << " is not ready, skip it.";
            continue;
        }
        InheritedLiveFiles table;
        table.set_table_name(live_it->first);
        for (size_t i = 0; i < live_it->second.size(); ++i) {
            VLOG(10) << "[gc] add inherited file, lg " << i
                << ", " << (live_it->second)[i].size() << " files total";
            LgInheritedLiveFiles* lg_files = table.add_lg_live_files();
            lg_files->set_lg_no(i);
            std::set<uint64_t>::iterator file_it = (live_it->second)[i].begin();
            for (; file_it != (live_it->second)[i].end(); ++file_it) {
                lg_files->add_file_number(*file_it);
                total++;
            }
        }
        inherited.push_back(table);
    }
    LOG(INFO) << "[gc] add inherited file " << total << " total";
}
} // namespace tabletnode
} // namespace tera
