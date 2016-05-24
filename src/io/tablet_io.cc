// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/tablet_io.h"

#include <stdint.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/this_thread.h"
#include "io/coding.h"
#include "io/default_compact_strategy.h"
#include "io/io_utils.h"
#include "io/tablet_writer.h"
#include "io/timekey_comparator.h"
#include "io/ttlkv_compact_strategy.h"
#include "io/utils_leveldb.h"
#include "leveldb/cache.h"
#include "leveldb/compact_strategy.h"
#include "leveldb/env.h"
#include "leveldb/env_cache.h"
#include "leveldb/env_dfs.h"
#include "leveldb/env_flash.h"
#include "leveldb/env_inmem.h"
#include "leveldb/filter_policy.h"
#include "types.h"
#include "utils/counter.h"
#include "utils/scan_filter.h"
#include "utils/string_util.h"
#include "utils/timer.h"
#include "utils/utils_cmd.h"

DECLARE_string(tera_leveldb_env_type);
DECLARE_int64(tera_tablet_log_file_size);
DECLARE_int64(tera_tablet_max_write_buffer_size);
DECLARE_int64(tera_tablet_write_block_size);
DECLARE_int32(tera_tablet_level0_file_limit);
DECLARE_int32(tera_tablet_max_block_log_number);
DECLARE_int64(tera_tablet_write_log_time_out);
DECLARE_bool(tera_log_async_mode);

DECLARE_int64(tera_tablet_living_period);
DECLARE_int32(tera_tablet_flush_log_num);

DECLARE_int32(tera_io_retry_period);
DECLARE_int32(tera_io_retry_max_times);

DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_int32(tera_tabletnode_retry_period);
DECLARE_string(tera_leveldb_compact_strategy);
DECLARE_bool(tera_leveldb_verify_checksums);
DECLARE_bool(tera_leveldb_ignore_corruption_in_compaction);

DECLARE_int32(tera_tabletnode_scan_pack_max_size);
DECLARE_bool(tera_tabletnode_cache_enabled);
DECLARE_int32(tera_leveldb_env_local_seek_latency);
DECLARE_int32(tera_leveldb_env_dfs_seek_latency);
DECLARE_int32(tera_memenv_table_cache_size);
DECLARE_int32(tera_memenv_block_cache_size);

DECLARE_bool(tera_tablet_use_memtable_on_leveldb);
DECLARE_int64(tera_tablet_memtable_ldb_write_buffer_size);
DECLARE_int64(tera_tablet_memtable_ldb_block_size);

extern tera::Counter row_read_delay;

namespace tera {
namespace io {

TabletIO::TabletIO(const std::string& key_start, const std::string& key_end)
    : m_async_writer(NULL),
      m_start_key(key_start),
      m_end_key(key_end),
      m_compact_status(kTableNotCompact),
      m_status(kNotInit),
      m_ref_count(1), m_db_ref_count(0), m_db(NULL),
      m_mem_store_activated(false),
      m_kv_only(false),
      m_key_operator(NULL) {
}

TabletIO::~TabletIO() {
    if (m_status != kNotInit && !Unload()) {
        if (m_async_writer != NULL) {
            m_async_writer->Stop();
            delete m_async_writer;
            m_async_writer = NULL;
        }
        delete m_db;
    }
}

std::string TabletIO::GetTableName() const {
    MutexLock lock(&m_schema_mutex);
    return m_table_schema.name();
}

std::string TabletIO::GetTablePath() const {
    if (!m_tablet_path.empty()) {
        std::string path =
            m_tablet_path.substr(FLAGS_tera_tabletnode_path_prefix.size());
        if (path.at(0) == '/') {
            path = path.substr(1);
        }
        return path;
    } else {
        return m_tablet_path;
    }
}

std::string TabletIO::GetStartKey() const {
    return m_start_key;
}

std::string TabletIO::GetEndKey() const {
    return m_end_key;
}

CompactStatus TabletIO::GetCompactStatus() const {
    return m_compact_status;
}

void TabletIO::SetSchema(const TableSchema& schema) {
    m_table_schema.CopyFrom(schema);
}

TableSchema TabletIO::GetSchema() const {
    MutexLock lock(&m_schema_mutex);
    return m_table_schema;
}

RawKey TabletIO::RawKeyType() const {
    MutexLock lock(&m_schema_mutex);
    return m_table_schema.raw_key();
};

TabletIO::StatCounter& TabletIO::GetCounter() {
    return m_counter;
}

bool TabletIO::Load(const TableSchema& schema,
                    const std::string& path,
                    const std::vector<uint64_t>& parent_tablets,
                    std::map<uint64_t, uint64_t> snapshots,
                    std::map<uint64_t, uint64_t> rollbacks,
                    leveldb::Logger* logger,
                    leveldb::Cache* block_cache,
                    leveldb::TableCache* table_cache,
                    StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status == kReady) {
            return true;
        } else if (m_status != kNotInit) {
            SetStatusCode(m_status, status);
            return false;
        }
        m_status = kOnLoad;
        m_db_ref_count++;
    }

    // any type of table should have at least 1lg+1cf.
    m_table_schema.CopyFrom(schema);
    if (m_table_schema.locality_groups_size() == 0) {
        // only prepare for kv-only mode, no need to set fields of it.
        m_table_schema.add_locality_groups();
    }

    RawKey raw_key = m_table_schema.raw_key();
    if (raw_key == TTLKv || raw_key == GeneralKv) {
        m_kv_only = true;
    } else {
        // for compatible
        if (m_table_schema.column_families_size() == 0) {
            // only prepare for kv-only mode, no need to set fields of it.
            m_table_schema.add_column_families();
            m_kv_only = true;
        } else {
            m_kv_only = m_table_schema.kv_only();
        }
    }

    m_key_operator = GetRawKeyOperatorFromSchema(m_table_schema);
    // [m_raw_start_key, m_raw_end_key)
    m_raw_start_key = m_start_key;
    if (!m_kv_only && !m_start_key.empty()) {
        m_key_operator->EncodeTeraKey(m_start_key, "", "", kLatestTs,
                                      leveldb::TKT_FORSEEK, &m_raw_start_key);
    } else if (m_kv_only && m_table_schema.raw_key() == TTLKv && !m_start_key.empty()) {
        m_key_operator->EncodeTeraKey(m_start_key, "", "", 0, leveldb::TKT_FORSEEK, &m_raw_start_key);
    }
    m_raw_end_key = m_end_key;
    if (!m_kv_only && !m_end_key.empty()) {
        m_key_operator->EncodeTeraKey(m_end_key, "", "", kLatestTs,
                                      leveldb::TKT_FORSEEK, &m_raw_end_key);
    } else if (m_kv_only && m_table_schema.raw_key() == TTLKv && !m_end_key.empty()) {
        m_key_operator->EncodeTeraKey(m_end_key, "", "", 0, leveldb::TKT_FORSEEK, &m_raw_end_key);
    }

    m_ldb_options.key_start = m_raw_start_key;
    m_ldb_options.key_end = m_raw_end_key;
    m_ldb_options.l0_slowdown_writes_trigger = FLAGS_tera_tablet_level0_file_limit;
    m_ldb_options.block_size = FLAGS_tera_tablet_write_block_size * 1024;
    m_ldb_options.max_block_log_number = FLAGS_tera_tablet_max_block_log_number;
    m_ldb_options.write_log_time_out = FLAGS_tera_tablet_write_log_time_out;
    m_ldb_options.log_async_mode = FLAGS_tera_log_async_mode;
    m_ldb_options.info_log = logger;
    m_ldb_options.max_open_files = FLAGS_tera_memenv_table_cache_size;

    m_ldb_options.use_memtable_on_leveldb = FLAGS_tera_tablet_use_memtable_on_leveldb;
    m_ldb_options.memtable_ldb_write_buffer_size =
            FLAGS_tera_tablet_memtable_ldb_write_buffer_size * 1024;
    m_ldb_options.memtable_ldb_block_size = FLAGS_tera_tablet_memtable_ldb_block_size * 1024;
    if (FLAGS_tera_tablet_use_memtable_on_leveldb) {
        LOG(INFO) << "enable mem-ldb for this tablet-server:"
            << " buffer_size:" << m_ldb_options.memtable_ldb_write_buffer_size
            << ", block_size:"  << m_ldb_options.memtable_ldb_block_size;
    }

    if (m_kv_only && m_table_schema.raw_key() == TTLKv) {
        m_ldb_options.filter_policy = leveldb::NewTTLKvBloomFilterPolicy(10);
    } else {
        m_ldb_options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    }
    m_ldb_options.block_cache = block_cache;
    m_ldb_options.table_cache = table_cache;
    m_ldb_options.flush_triggered_log_num = FLAGS_tera_tablet_flush_log_num;
    m_ldb_options.log_file_size = FLAGS_tera_tablet_log_file_size * 1024 * 1024;
    m_ldb_options.parent_tablets = parent_tablets;
    if (m_table_schema.raw_key() == Binary) {
        m_ldb_options.raw_key_format = leveldb::kBinary;
        m_ldb_options.comparator = leveldb::TeraBinaryComparator();
    } else if (m_table_schema.raw_key() == TTLKv) { // KV-Pair-With-TTL
        m_ldb_options.raw_key_format = leveldb::kTTLKv;
        m_ldb_options.comparator = leveldb::TeraTTLKvComparator();
        m_ldb_options.enable_strategy_when_get = true; // active usage of strategy in DB::Get
    } else { // Readable-Table && KV-Pair-Without-TTL
        m_ldb_options.raw_key_format = leveldb::kReadable;
        m_ldb_options.comparator = leveldb::BytewiseComparator();
    }
    m_ldb_options.verify_checksums_in_compaction = FLAGS_tera_leveldb_verify_checksums;
    m_ldb_options.ignore_corruption_in_compaction = FLAGS_tera_leveldb_ignore_corruption_in_compaction;
    m_ldb_options.disable_wal = m_table_schema.disable_wal();
    SetupOptionsForLG();

    std::string path_prefix = FLAGS_tera_tabletnode_path_prefix;
    if (*path_prefix.rbegin() != '/') {
        path_prefix.push_back('/');
    }

    m_tablet_path = path_prefix + path;
    LOG(INFO) << "[Load] Start Open " << m_tablet_path;
    // recover snapshot
    for (std::map<uint64_t, uint64_t>::iterator it = snapshots.begin(); it != snapshots.end(); ++it) {
        id_to_snapshot_num_[it->first] = it->second;
        m_ldb_options.snapshots_sequence.push_back(it->second);
    }
    // recover rollback
    for (std::map<uint64_t, uint64_t>::iterator it = rollbacks.begin(); it != rollbacks.end(); ++it) {
        rollbacks_[id_to_snapshot_num_[it->first]] = it->second;
        m_ldb_options.rollbacks[id_to_snapshot_num_[it->first]] = it->second;
    }

    leveldb::Status db_status = leveldb::DB::Open(m_ldb_options, m_tablet_path, &m_db);

    if (!db_status.ok()) {
        LOG(ERROR) << "fail to open table: " << m_tablet_path
            << ", " << db_status.ToString();
        {
            MutexLock lock(&m_mutex);
            m_status = kNotInit;
            m_db_ref_count--;
        }
        SetStatusCode(db_status, status);
//         delete m_ldb_options.env;
        return false;
    }

    m_async_writer = new TabletWriter(this);
    m_async_writer->Start();

    {
        MutexLock lock(&m_mutex);
        m_status = kReady;
        m_db_ref_count--;
    }

    LOG(INFO) << "[Load] Load " << m_tablet_path << " done";
    return true;
}

bool TabletIO::Unload(StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady && m_status != kSplited) {
            SetStatusCode(m_status, status);
            return false;
        }
        m_status = kUnLoading;
        m_db_ref_count++;
    }

    LOG(INFO) << "[Unload] start shutdown1 " << m_tablet_path;
    leveldb::Status s = m_db->Shutdown1();

    {
        MutexLock lock(&m_mutex);
        m_status = kUnLoading2;
    }

    uint32_t retry = 0;
    while (m_db_ref_count > 1) {
        LOG(ERROR) << "tablet is busy, db ref: " << m_db_ref_count
            << ", try again unload: " << retry++ << " " << m_tablet_path;
        ThisThread::Sleep(FLAGS_tera_io_retry_period);
    }

    LOG(INFO) << "[Unload] stop async writer " << m_tablet_path;
    m_async_writer->Stop();
    delete m_async_writer;
    m_async_writer = NULL;

    if (s.ok()) {
        LOG(INFO) << "[Unload] start shutdown2 " << m_tablet_path;
        m_db->Shutdown2();
    } else {
        LOG(INFO) << "[Unload] shutdown1 failed, keep log " << m_tablet_path;
    }

    delete m_db;
    m_db = NULL;

    delete m_ldb_options.filter_policy;
    if (m_mem_store_activated) {
        delete m_ldb_options.block_cache;
        m_mem_store_activated = false;
    }
    TearDownOptionsForLG();
    LOG(INFO) << "[Unload] done " << m_tablet_path;

    {
        MutexLock lock(&m_mutex);
        m_status = kNotInit;
        m_db_ref_count--;
    }
    return true;
}

// Find average string from input string
// E.g. "abc" & "abe" return "abd"
//      "a" & "b" return "a_"
bool TabletIO::FindAverageKey(const std::string& start, const std::string& end,
                             std::string* res) {
    std::string s = start;
    std::string e = end;
    if (e == "") {
        // make sure end > start
        e.resize(s.size() + 1, '\xFF');
    }
    CHECK(s < e);
    int max_len = s.size() > e.size() ? s.size() : e.size();
    max_len++;  // max_len should be >0
    s.resize(max_len, '\x00');
    e.resize(max_len, '\x00');

    if (s == e) {
        // find failed, e.g. s == "a" && e == "a\0"
        return false;
    }

    std::string ave;
    for (int i = 0; i < max_len; ++i) {
        if (s[i] == e[i]) {
            ave.append(1, s[i]);
            continue;
        } else if ((uint32_t)(unsigned char)s[i] + 1 < (uint32_t)(unsigned char)e[i]) {
            // find success
            char c = (char)(((uint32_t)(unsigned char)s[i]
                             + (uint32_t)(unsigned char)(e[i])) / 2);
            ave.append(1, c);
            break;
        } else {
            // find success
            CHECK((uint32_t)(unsigned char)s[i] == (uint32_t)(unsigned char)e[i] - 1
                  && i < max_len - 1);
            uint32_t c_int = ((uint32_t)(unsigned char)s[i + 1]
                             + (uint32_t)(unsigned char)e[i + 1] + 0x100) / 2;
            if (c_int < 0x100) {
                ave.append(1, s[i]);
            } else {
                ave.append(1, e[i]);
            }
            char c = (char)(c_int);
            ave.append(1, c);
            break;
        }
    }
    CHECK(ave > start && (end == "" || ave < end));
    *res = ave;
    return true;
}

bool TabletIO::ParseRowKey(const std::string& tera_key, std::string* row_key) {
    leveldb::Slice row;
    if ((RawKeyType() == GeneralKv)
        || (m_kv_only && RawKeyType() == Readable)) {
        row = tera_key;
    } else { // Table && TTL-KV
        if (!m_key_operator->ExtractTeraKey(tera_key, &row,
                                            NULL, NULL, NULL, NULL)) {
            VLOG(5) << "fail to extract split key";
            return false;
        }
    }
    *row_key = row.ToString();
    return true;
}

bool TabletIO::Split(std::string* split_key, StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady) {
            SetStatusCode(m_status, status);
            return false;
        }
        if (m_compact_status == kTableOnCompact) {
            SetStatusCode(kTableNotSupport, status);
            return false;
        }
        m_status = kOnSplit;
        m_db_ref_count++;
    }

    std::string raw_split_key;
    split_key->clear();
    if (m_db->FindSplitKey(0.5, &raw_split_key)) {
        ParseRowKey(raw_split_key, split_key);
    }

    if (split_key->empty() || *split_key == m_end_key) {
        // could not find split_key, try calc average key
        std::string smallest_key, largest_key;
        CHECK(m_db->FindKeyRange(&smallest_key, &largest_key));

        std::string srow_key, lrow_key;
        if (!smallest_key.empty()) {
            ParseRowKey(smallest_key, &srow_key);
        } else {
            srow_key = m_start_key;
        }
        if (!largest_key.empty()) {
            ParseRowKey(largest_key, &lrow_key);
        } else {
            lrow_key = m_end_key;
        }
        FindAverageKey(srow_key, lrow_key, split_key);
    }

    VLOG(5) << "start: [" << DebugString(m_start_key)
        << "], end: [" << DebugString(m_end_key)
        << "], split: [" << DebugString(*split_key) << "]";

    MutexLock lock(&m_mutex);
    m_db_ref_count--;
    if (*split_key != ""
        && *split_key != m_start_key
        && *split_key != m_end_key) {
        m_status = kSplited;
        return true;
    } else {
        SetStatusCode(kTableNotSupport, status);
        m_status = kReady;
        return false;
    }
}

bool TabletIO::Compact(int lg_no, StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady) {
            SetStatusCode(m_status, status);
            return false;
        }
        if (m_compact_status == kTableOnCompact) {
            return false;
        }
        m_compact_status = kTableOnCompact;
        m_db_ref_count++;
    }
    CHECK_NOTNULL(m_db);
    m_db->CompactRange(NULL, NULL, lg_no);

    {
        MutexLock lock(&m_mutex);
        m_compact_status = kTableCompacted;
        m_db_ref_count--;
    }
    return true;
}

bool TabletIO::CompactMinor(StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady) {
            SetStatusCode(m_status, status);
            return false;
        }
        m_db_ref_count++;
    }

    CHECK_NOTNULL(m_db);
    m_db->MinorCompact();
    {
        MutexLock lock(&m_mutex);
        m_db_ref_count--;
    }
    return true;
}

bool TabletIO::AddInheritedLiveFiles(std::vector<std::set<uint64_t> >* live) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady) {
            LOG(INFO) << "[gc] tablet not ready, skip it.";
            return false;
        }
        m_db_ref_count++;
    }
    {
        MutexLock lock(&m_schema_mutex);
        if (live->size() == 0) {
            live->resize(m_table_schema.locality_groups_size());
        } else {
            CHECK(live->size() == static_cast<uint64_t>(m_table_schema.locality_groups_size()));
        }
    }
    m_db->AddInheritedLiveFiles(live);
    {
        MutexLock lock(&m_mutex);
        m_db_ref_count--;
    }
    return true;
}

bool TabletIO::IsBusy() {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady) {
            return false;
        }
        m_db_ref_count++;
    }
    bool is_busy = m_db->BusyWrite();
    {
        MutexLock lock(&m_mutex);
        m_db_ref_count--;
    }
    return is_busy;
}

bool TabletIO::Workload(double* write_workload) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady) {
            return false;
        }
        m_db_ref_count++;
    }
    m_db->Workload(write_workload);
    {
        MutexLock lock(&m_mutex);
        m_db_ref_count--;
    }
    return true;
}

bool TabletIO::SnapshotIDToSeq(uint64_t snapshot_id, uint64_t* snapshot_sequence) {
    std::map<uint64_t, uint64_t>::iterator it = id_to_snapshot_num_.find(snapshot_id);
    if (it == id_to_snapshot_num_.end()) {
        return false;
    }
    *snapshot_sequence = it->second;
    return true;
}

bool TabletIO::GetDataSize(uint64_t* size, std::vector<uint64_t>* lgsize,
                           StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady && m_status != kOnSplit
            && m_status != kSplited && m_status != kUnLoading) {
            SetStatusCode(m_status, status);
            return false;
        }
        m_db_ref_count++;
    }

    m_db->GetApproximateSizes(size, lgsize);
    VLOG(10) << "GetDataSize(" << m_tablet_path << ") : " << *size;
    {
        MutexLock lock(&m_mutex);
        m_db_ref_count--;
    }
    if (size && *size == 0) {
        // return reserved buffer size
        *size = FLAGS_tera_tablet_write_block_size * 1024;
    }
    return true;
}

bool TabletIO::Read(const leveldb::Slice& key, std::string* value,
                    uint64_t snapshot_id, StatusCode* status) {
    CHECK_NOTNULL(m_db);
    leveldb::ReadOptions read_option(&m_ldb_options);
    read_option.verify_checksums = FLAGS_tera_leveldb_verify_checksums;
    if (snapshot_id != 0) {
        if (!SnapshotIDToSeq(snapshot_id, &read_option.snapshot)) {
            *status = kSnapshotNotExist;
            return false;
        }
    }
    read_option.rollbacks = rollbacks_;
    leveldb::Status db_status = m_db->Get(read_option, key, value);
    if (!db_status.ok()) {
        // LOG(ERROR) << "fail to read value for key: " << key.data()
        //    << " from tablet: " << m_tablet_path;
        SetStatusCode(db_status, status);
        return false;
    }
    return true;
}

StatusCode TabletIO::InitedScanInterator(const std::string& start_tera_key,
                                         const ScanOptions& scan_options,
                                         leveldb::Iterator** scan_it) {
    leveldb::Slice start_key, start_col, start_qual;
    m_key_operator->ExtractTeraKey(start_tera_key, &start_key, &start_col,
                                   &start_qual, NULL, NULL);

    leveldb::ReadOptions read_option(&m_ldb_options);
    read_option.verify_checksums = FLAGS_tera_leveldb_verify_checksums;
    SetupIteratorOptions(scan_options, &read_option);
    uint64_t snapshot_id = scan_options.snapshot_id;
    if (snapshot_id != 0) {
        if (!SnapshotIDToSeq(snapshot_id, &read_option.snapshot)) {
            TearDownIteratorOptions(&read_option);
            return kSnapshotNotExist;
        }
    }
    read_option.rollbacks = rollbacks_;
    *scan_it = m_db->NewIterator(read_option);
    TearDownIteratorOptions(&read_option);

    VLOG(10) << "ll-scan: " << "startkey=[" << DebugString(start_key.ToString()) << ":"
             << DebugString(start_col.ToString()) << ":" << DebugString(start_qual.ToString());
    std::string start_seek_key;
    m_key_operator->EncodeTeraKey(start_key.ToString(), "", "", kLatestTs,
                                  leveldb::TKT_FORSEEK, &start_seek_key);
    (*scan_it)->Seek(start_seek_key);

    return kTabletNodeOk;
}

bool TabletIO::LowLevelScan(const std::string& start_tera_key,
                            const std::string& end_row_key,
                            const ScanOptions& scan_options,
                            RowResult* value_list,
                            KeyValuePair* next_start_point,
                            uint32_t* read_row_count,
                            uint32_t* read_bytes,
                            bool* is_complete,
                            StatusCode* status) {
    leveldb::Iterator* it = NULL;
    StatusCode ret_code = InitedScanInterator(start_tera_key, scan_options, &it);
    if (ret_code != kTabletNodeOk) {
        SetStatusCode(ret_code, status);
        return false;
    }
    ScanContext scan_context(m_ldb_options.compact_strategy_factory);

    bool ret = LowLevelScan(start_tera_key, end_row_key, scan_options, it, &scan_context,
                            value_list, next_start_point, read_row_count, read_bytes,
                            is_complete, status);
    delete it;
    return ret;
}


bool TabletIO::ScanWithFilter(const ScanOptions& scan_options) {
    return scan_options.filter_list.filter_size() != 0;
}

// 检测`row_buf'中的数据是否为一整行，`row_buf'为空是整行的特例，也返回true
// 从LowLevelScan的for()循环中跳出时，leveldb::Iterator* it 指向第一个不在row_buf中的cell
// 如果这个cell的rowkey和row_buf中的数据rowkey相同，
// 则说明`row_buf'中的数据不是一整行，返回false
// `row_buf'自身的逻辑保证了其中的所有cell必定属于同一行(row)
bool TabletIO::IsCompleteRow(const std::list<KeyValuePair>& row_buf,
                             leveldb::Iterator* it) {
    assert((it != NULL) && (it->Valid()));
    if (row_buf.size() == 0) {
        VLOG(9) << "[filter] row_buf empty";
        return true;
    }
    leveldb::Slice origin_cell = it->key();
    leveldb::Slice cur_cell = it->key();
    for (; it->Valid();) {
        cur_cell = it->key();
        leveldb::Slice row;
        if (!m_key_operator->ExtractTeraKey(cur_cell, &row,
                                            NULL, NULL, NULL, NULL)) {
            LOG(ERROR) << "[filter] invalid tera key: " << DebugString(cur_cell.ToString());
            it->Next();
            continue;
        }
        if (cur_cell.compare(origin_cell) != 0) {
            it->Seek(origin_cell);
        }
        bool res = row.compare(row_buf.begin()->key()) == 0;
        VLOG(9) << "[filter] " << ( res ? "NOT " : "") << "complete row";
        return !res;
    }
    if (cur_cell.compare(origin_cell) != 0) {
        it->Seek(origin_cell);
    }
    VLOG(9) << "[filter] reach the end, row_buf is complete row";
    return true;
}

// 检测是否应该过滤掉`row_buf'中cell所在的一整行(row)
// 用户指定了一定数量的filter，针对某些特定列的值对row进行过滤，
// 返回false表示不过滤这一行，这一行数据被返回给用户
bool TabletIO::ShouldFilterRow(const ScanOptions& scan_options,
                               const std::list<KeyValuePair>& row_buf,
                               leveldb::Iterator* it) {
    assert((it != NULL) && it->Valid());
    if (row_buf.size() == 0) {
        VLOG(9) << "[filter] row_buf empty";
        return false;
    }
    std::string origin_row = row_buf.begin()->key();

    leveldb::Slice origin_cell = it->key();

    int filter_num = scan_options.filter_list.filter_size();

    // TODO(taocipian)
    // 0). some target cf maybe already in row_buf
    // 1). collects all target cf and sorts them,
    //     then Seek() to the 1st cf, Next() to the rest
    for (int i = 0; i < filter_num; ++i) {
        const Filter& filter = scan_options.filter_list.filter(i);
        // 针对用户指定了过滤条件的每一列，seek过去看看是否符合
        std::string target_cf = filter.content();
        VLOG(9) << "[filter] " << i << " of " << filter_num
                << " , target cf:" << target_cf;
        std::string seek_key;
        m_key_operator->EncodeTeraKey(origin_row, target_cf, "", kLatestTs,
                                      leveldb::TKT_FORSEEK, &seek_key);
        it->Seek(seek_key);
        for (; it->Valid();) {
            leveldb::Slice row, cf, qu;
            int64_t ts;
            if (!m_key_operator->ExtractTeraKey(it->key(), &row, &cf, &qu, &ts, NULL)) {
                LOG(ERROR) << "[filter] invalid tera key: " << DebugString(it->key().ToString());
                it->Next();
                continue;
            }
            if ((row.ToString() != origin_row)
                || (cf.ToString() != target_cf)
                || (qu.ToString() != "")) {
                // 用户试图过滤不存在的一列，忽略这个过滤条件
                VLOG(9) << "[filter] target cf not found:" << target_cf;
                break;
            }
            KeyValuePair pair;
            leveldb::Slice value = it->value();
            MakeKvPair(row, cf, qu, ts, value, &pair);
            if (!CheckCell(pair, filter)) {
                it->Seek(origin_cell);
                VLOG(9) << "[filter] check failed at target cf:" << target_cf;
                return true;
            }
            VLOG(9) << "[filter] target cf check passed";
            break;
        }
    }
    it->Seek(origin_cell);
    VLOG(9) << "[filter] this row check passed";
    return false;
}

// seek到`row_buf'中cell所在行(row)的下一行，
// 调用者需要检查此函数返回以后迭代器的状态是否有效，
// 因为可能已经到了数据库的最后
void TabletIO::GotoNextRow(const std::list<KeyValuePair>& row_buf,
                           leveldb::Iterator* it,
                           KeyValuePair* next) {
    assert(it != NULL);
    if (!it->Valid() || row_buf.size() == 0) {
        return;
    }
    std::string row = row_buf.begin()->key();
    std::string next_row = row + '\0';
    std::string seek_key;
    m_key_operator->EncodeTeraKey(next_row, "", "", kLatestTs,
                                  leveldb::TKT_FORSEEK, &seek_key);
    it->Seek(seek_key);
    MakeKvPair(leveldb::Slice(next_row), "", "", kLatestTs, "", next);
    VLOG(9) << "[filter] goto next row:" << next_row << ":" << next_row.size();
}

inline bool TabletIO::LowLevelScan(const std::string& start_tera_key,
                                   const std::string& end_row_key,
                                   const ScanOptions& scan_options,
                                   leveldb::Iterator* it,
                                   ScanContext* scan_context,
                                   RowResult* value_list,
                                   KeyValuePair* next_start_point,
                                   uint32_t* read_row_count,
                                   uint32_t* read_bytes,
                                   bool* is_complete,
                                   StatusCode* status) {
    leveldb::CompactStrategy* compact_strategy = scan_context->compact_strategy;
    std::string& last_key = scan_context->last_key;
    std::string& last_col = scan_context->last_col;
    std::string& last_qual = scan_context->last_qual;
    uint32_t& version_num = scan_context->version_num;

    std::list<KeyValuePair> row_buf;
    uint32_t buffer_size = 0;
    int64_t number_limit = 0;
    value_list->clear_key_values();
    *read_row_count = 0;
    *read_bytes = 0;
    int64_t now_time = GetTimeStampInMs();
    int64_t time_out = now_time + scan_options.timeout;
    KeyValuePair next_start_kv_pair;
    VLOG(9) << "ll-scan timeout set to be " << scan_options.timeout;

    for (; it->Valid();) {
        bool has_merged = false;
        std::string merged_value;
        m_counter.low_read_cell.Inc();
        *read_bytes += it->key().size() + it->value().size();
        now_time = GetTimeStampInMs();

        leveldb::Slice tera_key = it->key();
        leveldb::Slice value = it->value();
        leveldb::Slice key, col, qual;
        int64_t ts = 0;
        leveldb::TeraKeyType type;
        if (!m_key_operator->ExtractTeraKey(tera_key, &key, &col, &qual, &ts, &type)) {
            LOG(WARNING) << "invalid tera key: " << DebugString(tera_key.ToString());
            it->Next();
            continue;
        }

        VLOG(10) << "ll-scan: " << "tablet=[" << m_tablet_path
            << "] key=[" << DebugString(key.ToString())
            << "] column=[" << DebugString(col.ToString())
            << ":" << DebugString(qual.ToString())
            << "] ts=[" << ts << "] type=[" << type << "]";

        if (end_row_key.size() && key.compare(end_row_key) >= 0) {
            // scan finished
            break;
        }

        if (now_time > time_out) {
            VLOG(9) << "ll-scan timeout. Mark next start key: " << DebugString(tera_key.ToString());
            MakeKvPair(key, col, qual, ts, "", &next_start_kv_pair);
            break;
        }

        const std::set<std::string>& cf_set = scan_options.iter_cf_set;
        if (cf_set.size() > 0 &&
            cf_set.find(col.ToString()) == cf_set.end() &&
            type != leveldb::TKT_DEL) {
            // donot need this column, skip row deleting tag
            it->Next();
            continue;
        }

        if (compact_strategy->ScanDrop(it->key(), 0)) {
            // skip drop record
            it->Next();
            continue;
        }

        // only use for sync scan, not available for stream scan
        if (m_key_operator->Compare(it->key(), start_tera_key) < 0) {
            // skip out-of-range records
            // keep record of version info to prevent dirty data
            if (key.compare(last_key) == 0 &&
                col.compare(last_col) == 0 &&
                qual.compare(last_qual) == 0) {
                ++version_num;
            } else {
                last_key.assign(key.data(), key.size());
                last_col.assign(col.data(), col.size());
                last_qual.assign(qual.data(), qual.size());
                version_num = 1;
            }
            it->Next();
            continue;
        }

        // begin to scan next row
        if (key.compare(last_key) != 0) {
            *read_row_count += 1;
            ProcessRowBuffer(row_buf, scan_options, value_list, &buffer_size, &number_limit);
            row_buf.clear();
        }

        // max version filter
        if (key.compare(last_key) == 0 &&
            col.compare(last_col) == 0 &&
            qual.compare(last_qual) == 0) {
            if (++version_num > scan_options.max_versions) {
                it->Next();
                continue;
            }
        } else {
            last_key.assign(key.data(), key.size());
            last_col.assign(col.data(), col.size());
            last_qual.assign(qual.data(), qual.size());
            version_num = 1;
            int64_t merged_num;
            has_merged =
                compact_strategy->ScanMergedValue(it, &merged_value, &merged_num);
            VLOG(10) << "has_merged:" << has_merged;
            if (has_merged) {
                m_counter.low_read_cell.Add(merged_num);
                value = merged_value;
                key = last_key;
                col = last_col;
                qual = last_qual;
                VLOG(10) << "ll-scan: merge: " << std::string(key.data())
                    << ":" << std::string(col.data())
                    << ":" << std::string(qual.data());
            }
        }

        KeyValuePair kv;
        (const_cast<ScanOptions&>(scan_options)).version_num = version_num;
        MakeKvPair(key, col, qual, ts, value, &kv);
        row_buf.push_back(kv);

        // check scan buffer
        if (buffer_size >= scan_options.max_size || number_limit >= scan_options.number_limit) {
            VLOG(10) << "stream scan, break scan context, version_num " << version_num
                << ", key " << DebugString(key.ToString()) << ", col " << DebugString(col.ToString())
                << ", qual " << DebugString(qual.ToString());
            it->Next();
            break;
        }

        if (!has_merged) {
            it->Next();
        }
    }

    if (ScanWithFilter(scan_options)
        && it->Valid()
        && !IsCompleteRow(row_buf, it)
        && ShouldFilterRow(scan_options, row_buf, it)) {
        GotoNextRow(row_buf, it, &next_start_kv_pair);
    } else {
        // process the last row of tablet
        ProcessRowBuffer(row_buf, scan_options, value_list, &buffer_size, &number_limit);
    }

    leveldb::Status it_status;
    if (!it->Valid()) {
        it_status = it->status();
    }

    if (!it_status.ok()) {
        SetStatusCode(it_status, status);
        VLOG(10) << "ll-seek fail: " << "tablet=[" << m_tablet_path <<
            "] status=[" << StatusCodeToString(*status);
        return false;
    }

    // check if scan finished
    SetStatusCode(kTableOk, status);
    if ((buffer_size < scan_options.max_size) &&
        (number_limit < scan_options.number_limit) &&
        (now_time <= time_out)) {
        *is_complete = true;
    } else {
        if (now_time > time_out && next_start_point) {
            next_start_point->CopyFrom(next_start_kv_pair);
        }
        *is_complete = false;
    }
    return true;
}

void TabletIO::MakeKvPair(leveldb::Slice key, leveldb::Slice col, leveldb::Slice qual,
                          int64_t ts, leveldb::Slice value, KeyValuePair* kv) {
    kv->set_key(key.data(), key.size());
    kv->set_column_family(col.data(), col.size());
    kv->set_qualifier(qual.data(), qual.size());
    kv->set_timestamp(ts);
    kv->set_value(value.data(), value.size());
}

bool TabletIO::LowLevelSeek(const std::string& row_key,
                            const ScanOptions& scan_options,
                            RowResult* value_list,
                            StatusCode* status) {
    StatusCode s;
    SetStatusCode(kTableOk, &s);
    value_list->clear_key_values();

    // create tera iterator
    leveldb::ReadOptions read_option(&m_ldb_options);
    read_option.verify_checksums = FLAGS_tera_leveldb_verify_checksums;
    SetupIteratorOptions(scan_options, &read_option);
    uint64_t snapshot_id = scan_options.snapshot_id;
    if (snapshot_id != 0) {
        if (!SnapshotIDToSeq(snapshot_id, &read_option.snapshot)) {
            TearDownIteratorOptions(&read_option);
            SetStatusCode(kSnapshotNotExist, status);
            return false;
        }
    }
    read_option.rollbacks = rollbacks_;
    leveldb::Iterator* it_data = m_db->NewIterator(read_option);
    TearDownIteratorOptions(&read_option);

    // init compact strategy
    leveldb::CompactStrategy* compact_strategy =
        m_ldb_options.compact_strategy_factory->NewInstance();

    // seek to the row start & process row delete mark
    std::string row_seek_key;
    m_key_operator->EncodeTeraKey(row_key, "", "", kLatestTs,
                                  leveldb::TKT_FORSEEK, &row_seek_key);
    it_data->Seek(row_seek_key);
    m_counter.low_read_cell.Inc();
    if (it_data->Valid()) {
        VLOG(10) << "ll-seek: " << "tablet=[" << m_tablet_path
            << "] row_key=[" << row_key << "]";
        leveldb::Slice cur_row_key;
        m_key_operator->ExtractTeraKey(it_data->key(), &cur_row_key,
                                       NULL, NULL, NULL, NULL);
        if (cur_row_key.compare(row_key) > 0) {
            SetStatusCode(kKeyNotExist, &s);
        } else {
            compact_strategy->ScanDrop(it_data->key(), 0);
        }
    } else {
        SetStatusCode(kKeyNotExist, &s);
    }
    if (s != kTableOk) {
        delete compact_strategy;
        delete it_data;
        SetStatusCode(s, status);
        return false;
    }

    ColumnFamilyMap::const_iterator it_cf =
        scan_options.column_family_list.begin();
    for (; it_cf != scan_options.column_family_list.end(); ++it_cf) {
        const string& cf_name = it_cf->first;
        const std::set<std::string>& qu_set = it_cf->second;

        // seek to the cf start & process cf delete mark
        std::string cf_seek_key;
        m_key_operator->EncodeTeraKey(row_key, cf_name, "", kLatestTs,
                                      leveldb::TKT_FORSEEK, &cf_seek_key);
        it_data->Seek(cf_seek_key);
        m_counter.low_read_cell.Inc();
        if (it_data->Valid()) {
            VLOG(10) << "ll-seek: " << "tablet=[" << m_tablet_path
                << "] row_key=[" << row_key
                << "] cf=[" << cf_name << "]";
            leveldb::Slice cur_row, cur_cf;
            m_key_operator->ExtractTeraKey(it_data->key(), &cur_row, &cur_cf,
                                           NULL, NULL, NULL);
            if (cur_row.compare(row_key) > 0 || cur_cf.compare(cf_name) > 0) {
                continue;
            } else {
                compact_strategy->ScanDrop(it_data->key(), 0);
            }
        } else {
            VLOG(10) << "ll-seek fail, not found.";
            continue;
        }

        if (qu_set.empty()) {
            LOG(FATAL) << "low level seek only support qualifier read.";
        }
        std::set<std::string>::iterator it_qu = qu_set.begin();
        for (; it_qu != qu_set.end(); ++it_qu) {
            const string& qu_name = *it_qu;
            VLOG(10) << "ll-seek: try find " << "tablet=[" << m_tablet_path
                << "] row_key=[" << row_key << "] cf=[" << cf_name
                << "] qu=[" << qu_name << "]";

            // seek to the cf start & process cf delete mark
            std::string qu_seek_key;
            m_key_operator->EncodeTeraKey(row_key, cf_name, qu_name, kLatestTs,
                                          leveldb::TKT_FORSEEK, &qu_seek_key);
            it_data->Seek(qu_seek_key);
            uint32_t version_num = 0;
            for (; it_data->Valid();) {
                m_counter.low_read_cell.Inc();
                VLOG(10) << "ll-seek: " << "tablet=[" << m_tablet_path
                    << "] row_key=[" << row_key << "] cf=[" << cf_name
                    << "] qu=[" << qu_name << "]";
                leveldb::Slice cur_row, cur_cf, cur_qu;
                int64_t timestamp;
                m_key_operator->ExtractTeraKey(it_data->key(), &cur_row, &cur_cf,
                                               &cur_qu, &timestamp, NULL);
                if (cur_row.compare(row_key) > 0 || cur_cf.compare(cf_name) > 0 ||
                    cur_qu.compare(qu_name) > 0) {
                    break;
                }

                // skip qu delete mark
                if (compact_strategy->ScanDrop(it_data->key(), 0)) {
                    VLOG(10) << "ll-seek: scan drop " << "tablet=[" << m_tablet_path
                        << "] row_key=[" << row_key << "] cf=[" << cf_name
                        << "] qu=[" << qu_name << "]";
                    it_data->Next();
                    continue;
                }

                // version filter
                if (++version_num > scan_options.max_versions) {
                    break;
                }

                KeyValuePair* kv = value_list->add_key_values();
                kv->set_key(row_key);
                kv->set_column_family(cf_name);
                kv->set_qualifier(qu_name);
                kv->set_timestamp(timestamp);

                int64_t merged_num;
                std::string merged_value;
                bool has_merged =
                    compact_strategy->ScanMergedValue(it_data, &merged_value, &merged_num);
                if (has_merged) {
                    m_counter.low_read_cell.Add(merged_num);
                    kv->set_value(merged_value);
                } else {
                    leveldb::Slice value = it_data->value();
                    kv->set_value(value.data(), value.size());
                    it_data->Next();
                }
            }
        }
    }
    delete compact_strategy;
    delete it_data;

    SetStatusCode(s, status);
    if (s == kTableOk) {
        return true;
    } else {
        return false;
    }
}

bool TabletIO::ReadCells(const RowReaderInfo& row_reader, RowResult* value_list,
                         uint64_t snapshot_id, StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady && m_status != kOnSplit
            && m_status != kSplited && m_status != kUnLoading) {
            if (m_status == kUnLoading2) {
                // keep compatable for old sdk protocol
                // we can remove this in the future.
                SetStatusCode(kUnLoading, status);
            } else {
                SetStatusCode(m_status, status);
            }
            return false;
        }
        m_db_ref_count++;
    }

    int64_t read_ms = get_micros();

    if (m_kv_only) {
        std::string key(row_reader.key());
        std::string value;
        if (RawKeyType() == TTLKv) {
            key.append(8, '\0');
        }
        if (!Read(key, &value, snapshot_id, status)) {
            m_counter.read_rows.Inc();
            row_read_delay.Add(get_micros() - read_ms);
            {
                MutexLock lock(&m_mutex);
                m_db_ref_count--;
            }
            return false;
        }
        KeyValuePair* result = value_list->add_key_values();
        result->set_key(row_reader.key());
        result->set_value(value);
        m_counter.read_rows.Inc();
        m_counter.read_size.Add(result->ByteSize());
        row_read_delay.Add(get_micros() - read_ms);
        {
            MutexLock lock(&m_mutex);
            m_db_ref_count--;
        }
        return true;
    }

    ScanOptions scan_options;
    bool ll_seek_available = true;
    for (int32_t i = 0; i < row_reader.cf_list_size(); ++i) {
        const ColumnFamily& column_family = row_reader.cf_list(i);
        const std::string& column_family_name = column_family.family_name();
        std::set<std::string>& qualifier_list =
            scan_options.column_family_list[column_family_name];
        qualifier_list.clear();
        for (int32_t j = 0; j < column_family.qualifier_list_size(); ++j) {
            qualifier_list.insert(column_family.qualifier_list(j));
        }
        if (qualifier_list.empty()) {
            ll_seek_available = false;
        }
        scan_options.iter_cf_set.insert(column_family_name);
    }
    if (scan_options.column_family_list.empty()) {
        ll_seek_available = false;
    }

    if (row_reader.has_max_version()) {
        scan_options.max_versions = row_reader.max_version();
    }
    if (row_reader.has_time_range()) {
        scan_options.ts_start = row_reader.time_range().ts_start();
        scan_options.ts_end = row_reader.time_range().ts_end();
    }

    scan_options.snapshot_id = snapshot_id;

    VLOG(10) << "ReadCells: " << "key=[" << DebugString(row_reader.key()) << "]";

    bool ret = false;
    // if read all columns, use LowLevelScan
    if (ll_seek_available) {
        ret = LowLevelSeek(row_reader.key(), scan_options, value_list, status);
    } else {
        std::string start_tera_key;
        m_key_operator->EncodeTeraKey(row_reader.key(), "", "", kLatestTs,
                                        leveldb::TKT_VALUE, &start_tera_key);
        std::string end_row_key = row_reader.key() + '\0';
        uint32_t read_row_count = 0;
        uint32_t read_bytes = 0;
        bool is_complete = false;
        ret = LowLevelScan(start_tera_key, end_row_key, scan_options,
                           value_list, NULL, &read_row_count, &read_bytes,
                           &is_complete, status);
    }
    m_counter.read_rows.Inc();
    row_read_delay.Add(get_micros() - read_ms);
    {
        MutexLock lock(&m_mutex);
        m_db_ref_count--;
    }
    if (!ret) {
        return false;
    } else {
        m_counter.read_size.Add(value_list->ByteSize());
    }

    if (value_list->key_values_size() == 0) {
        SetStatusCode(kKeyNotExist, status);
        return false;
    }
    return true;
}

bool TabletIO::WriteBatch(leveldb::WriteBatch* batch, bool disable_wal, bool sync,
                          StatusCode* status) {
    leveldb::WriteOptions options;
    options.disable_wal = disable_wal;
    options.sync = sync;

    CHECK_NOTNULL(m_db);

    m_counter.write_size.Add(batch->DataSize());
    leveldb::Status db_status = m_db->Write(options, batch);
    if (!db_status.ok()) {
        LOG(ERROR) << "fail to batch write to tablet: " << m_tablet_path
            << ", " << db_status.ToString();
        SetStatusCode(kIOError, status);
        return false;
    }
    SetStatusCode(kTableOk, status);
    return true;
}

bool TabletIO::WriteOne(const std::string& key, const std::string& value,
                        bool sync, StatusCode* status) {
    leveldb::WriteBatch batch;
    batch.Put(key, value);
    return WriteBatch(&batch, false, sync, status);
}

bool TabletIO::Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
                     std::vector<StatusCode>* status_vec, bool is_instant,
                     WriteCallback callback, StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady && m_status != kOnSplit
            && m_status != kSplited && m_status != kUnLoading) {
            if (m_status == kUnLoading2) {
                // keep compatable for old sdk protocol
                // we can remove this in the future.
                SetStatusCode(kUnLoading, status);
            } else {
                SetStatusCode(m_status, status);
            }
            return false;
        }
        m_db_ref_count++;
    }
    bool ret = m_async_writer->Write(row_mutation_vec, status_vec, is_instant,
                                     callback, status);
    {
        MutexLock lock(&m_mutex);
        m_db_ref_count--;
    }
    return ret;
}

bool TabletIO::ScanRows(const ScanTabletRequest* request,
                        ScanTabletResponse* response,
                        google::protobuf::Closure* done) {
    StatusCode status = kTabletNodeOk;
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady && m_status != kOnSplit
            && m_status != kSplited && m_status != kUnLoading) {
            if (m_status == kUnLoading2) {
                // keep compatable for old sdk protocol
                // we can remove this in the future.
                SetStatusCode(kUnLoading, &status);
            } else {
                SetStatusCode(m_status, &status);
            }
            response->set_status(status);
            done->Run();
            return false;
        }
        m_db_ref_count++;
    }

    bool success = false;
    if (m_kv_only) {
        ScanOption scan_option;
        scan_option.set_snapshot_id(request->snapshot_id());
        scan_option.mutable_key_range()->set_key_start(request->start());
        scan_option.mutable_key_range()->set_key_end(request->end());
        if (request->has_buffer_limit()) {
            scan_option.set_size_limit(request->buffer_limit());
        } else {
            scan_option.set_size_limit(FLAGS_tera_tabletnode_scan_pack_max_size << 10);
        }
        scan_option.set_round_down(request->round_down());
        bool complete = false;
        success = Scan(scan_option, response->mutable_results()->mutable_key_values(),
                       &complete, &status);
        if (success) {
            response->set_complete(complete);
        }
        response->set_status(status);
        done->Run();
    } else if (request->has_session_id() && request->session_id() > 0) {
        success = ScanRowsStreaming(request, response, done);
    } else {
        success = ScanRowsRestricted(request, response, done);
    }
    {
        MutexLock lock(&m_mutex);
        m_db_ref_count--;
    }
    return success;
}

bool TabletIO::ScanRowsRestricted(const ScanTabletRequest* request,
                                  ScanTabletResponse* response,
                                  google::protobuf::Closure* done) {
    std::string start_tera_key;
    std::string end_row_key;
    SetupScanInternalTeraKey(request, &start_tera_key, &end_row_key);

    ScanOptions scan_options;
    SetupScanRowOptions(request, &scan_options);

    uint32_t read_row_count = 0;
    uint32_t read_bytes = 0;
    bool is_complete = false;

    StatusCode status = kTabletNodeOk;
    bool ret = false;
    if (LowLevelScan(start_tera_key, end_row_key, scan_options,
                     response->mutable_results(), response->mutable_next_start_point(),
                     &read_row_count, &read_bytes, &is_complete, &status)) {
        response->set_complete(is_complete);
        m_counter.scan_rows.Add(read_row_count);
        m_counter.scan_size.Add(read_bytes);
        ret = true;
    }

    response->set_status(status);
    done->Run();
    return ret;
}

bool TabletIO::ScanRowsStreaming(const ScanTabletRequest* request,
                                 ScanTabletResponse* response,
                                 google::protobuf::Closure* done) {
    bool is_first_scan = false;
    std::string table_name = request->table_name();
    m_stream_scan.PushTask(request, response, done, &is_first_scan);
    if (!is_first_scan) {
        LOG(INFO) << "not first rpc to call scan: " << table_name;
        return true;
    }

    std::string start_tera_key;
    std::string end_row_key;
    SetupScanInternalTeraKey(request, &start_tera_key, &end_row_key);

    ScanOptions scan_options;
    SetupScanRowOptions(request, &scan_options);

    uint32_t read_row_count = 0;
    uint32_t read_bytes = 0;
    bool is_complete = false;

    uint64_t session_id = request->session_id();
    StreamScan* scan_stream = m_stream_scan.GetStream(session_id);
    leveldb::Iterator* it = NULL;
    StatusCode ret_code = InitedScanInterator(start_tera_key, scan_options, &it);
    if (ret_code != kTabletNodeOk) {
        scan_stream->SetStatusCode(ret_code);
        m_stream_scan.RemoveSession(session_id);
        return false;
    }
    ScanContext scan_context(m_ldb_options.compact_strategy_factory);

    StatusCode status = kTabletNodeOk;
    uint64_t data_id = 0;
    while (status == kTabletNodeOk) {
        RowResult value_list;
        if (LowLevelScan(start_tera_key, end_row_key, scan_options, it, &scan_context,
                         &value_list, NULL, &read_row_count, &read_bytes,
                         &is_complete, &status)) {
            m_counter.scan_rows.Add(read_row_count);
            m_counter.scan_size.Add(read_bytes);

            scan_stream->SetCompleted(is_complete);
            if (!scan_stream->PushData(data_id, value_list)) {
                break;
            }
            data_id++;
        }
        scan_stream->SetStatusCode(status);
        if (is_complete) {
            break;
        }
    }

    delete it;
    m_stream_scan.RemoveSession(session_id);
    return true;
}

bool TabletIO::Scan(const ScanOption& option, KeyValueList* kv_list,
                    bool* complete, StatusCode* status) {
    std::string start = option.key_range().key_start();
    std::string end = option.key_range().key_end();
    if (start < m_start_key) {
        start = m_start_key;
    }
    if (end.empty() || (!m_end_key.empty() && end > m_end_key)) {
        end = m_end_key;
    }

    bool noexist_end = false;
    if (end.empty()) {
        noexist_end = true;
    }

    int64_t pack_size = 0;
    uint64_t snapshot_id = option.snapshot_id();
    leveldb::ReadOptions read_option(&m_ldb_options);
    read_option.verify_checksums = FLAGS_tera_leveldb_verify_checksums;
    if (snapshot_id != 0) {
        if (!SnapshotIDToSeq(snapshot_id, &read_option.snapshot)) {
            *status = kSnapshotNotExist;
            return false;
        }
    }
    read_option.rollbacks = rollbacks_;
    // TTL-KV : m_key_operator::Compare会解RawKey([row_key | expire_timestamp])
    // 因此传递给Leveldb的Key一定要保证以expire_timestamp结尾.
    leveldb::CompactStrategy* strategy = NULL;
    if (RawKeyType() == TTLKv) {
        if (!start.empty()) {
            std::string start_key;
            m_key_operator->EncodeTeraKey(start, "", "", 0, leveldb::TKT_FORSEEK, &start_key);
            start.swap(start_key);
        }
        if (!end.empty()) {
            std::string end_key;
            m_key_operator->EncodeTeraKey(end, "", "", 0, leveldb::TKT_FORSEEK, &end_key);
            end.swap(end_key);
        }
        strategy = m_ldb_options.compact_strategy_factory->NewInstance();
    }

    leveldb::Iterator* it = m_db->NewIterator(read_option);
    it->Seek(start);
    if (option.round_down()) {
        if (it->Valid() && m_key_operator->Compare(it->key(), start) > 0) {
            it->Prev();
            if (!it->Valid()) {
                it->SeekToFirst();
            }
        } else if (!it->Valid()) {
            it->SeekToLast();
        }
    }
    for (; it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        leveldb::Slice value = it->value();
        *complete = (!noexist_end && m_key_operator->Compare(key, end) >= 0);
        if (*complete || (option.size_limit() > 0 && pack_size > option.size_limit())) {
            break;
        } else {
            if (!(strategy && strategy->ScanDrop(key, 0))) {
                KeyValuePair* pair = kv_list->Add();
                if (RawKeyType() == TTLKv) {
                    pair->set_key(key.data(), key.size() - sizeof(int64_t));
                } else {
                    pair->set_key(key.data(), key.size());
                }
                pair->set_value(value.data(), value.size());
                pack_size += pair->key().size() + pair->value().size();
            } else {
                VLOG(10) << "[KV-Scan] key:[" << key.ToString() << "] Dropped.";
            }
        }
    }
    if (!it->Valid()) {
        *complete = true;
    }
    m_counter.scan_rows.Add(kv_list->size());
    m_counter.scan_size.Add(pack_size);
    delete it;
    delete strategy;

    return true;
}

void TabletIO::SetupScanInternalTeraKey(const ScanTabletRequest* request,
                                        std::string* start_tera_key,
                                        std::string* end_row_key) {
    bool has_cf = request->has_start_family();
    bool has_qualifier = (has_cf && request->has_start_qualifier());
    // bool has_ts = (has_qualifier && request->has_start_timestamp());
    bool has_ts = request->has_start_timestamp();

    std::string start_key = request->start();
    *end_row_key = request->end();
    if (start_key == "" || start_key < m_start_key) {
        start_key = m_start_key;
    }
    if (*end_row_key == "" || (m_end_key != "" && *end_row_key > m_end_key)) {
        *end_row_key = m_end_key;
    }

    m_key_operator->EncodeTeraKey(start_key,
                                  has_cf ? request->start_family() : "",
                                  has_qualifier ? request->start_qualifier() : "",
                                  has_ts ? request->start_timestamp() : kLatestTs,
                                  leveldb::TKT_VALUE,
                                  start_tera_key);
}

void TabletIO::SetupScanRowOptions(const ScanTabletRequest* request,
                                   ScanOptions* scan_options) {
    scan_options->max_size = 65536;
    for (int32_t i = 0; i < request->cf_list_size(); ++i) {
        const ColumnFamily& column_family = request->cf_list(i);
        const std::string& column_family_name = column_family.family_name();
        std::set<std::string>& qualifier_list =
            scan_options->column_family_list[column_family_name];
        qualifier_list.clear();
        for (int32_t j = 0; j < column_family.qualifier_list_size(); ++j) {
            qualifier_list.insert(column_family.qualifier_list(j));
        }
        scan_options->iter_cf_set.insert(column_family_name);
    }

    if (request->has_filter_list() &&
        request->filter_list().filter_size() > 0) {
        scan_options->filter_list.CopyFrom(request->filter_list());
    }
    if (scan_options->iter_cf_set.size() > 0 &&
        scan_options->filter_list.filter_size() > 0) {
        ScanFilter scan_filter(scan_options->filter_list);
        scan_filter.GetAllCfs(&scan_options->iter_cf_set);
    }
    if (request->has_max_version()) {
        scan_options->max_versions = request->max_version();
    }
    if (request->has_timerange()) {
        scan_options->ts_start = request->timerange().ts_start();
        scan_options->ts_end = request->timerange().ts_end();
    }
    if (request->has_buffer_limit()) {
        scan_options->max_size = request->buffer_limit();
    }
    if (request->has_number_limit() && (request->number_limit() > 0)) {
        scan_options->number_limit = request->number_limit();
    }
    if (request->timeout()) {
        scan_options->timeout = request->timeout();
    }
    scan_options->version_num = 0;
    scan_options->snapshot_id = request->snapshot_id();
}

// no concurrent, so no lock on m_schema_mutex
void TabletIO::SetupOptionsForLG() {
    if (m_kv_only) {
        if (RawKeyType() == TTLKv) {
            m_ldb_options.compact_strategy_factory =
                new KvCompactStrategyFactory(m_table_schema);
        } else {
            m_ldb_options.compact_strategy_factory =
                new leveldb::DummyCompactStrategyFactory();
        }
    } else if (FLAGS_tera_leveldb_compact_strategy == "default") {
        // default strategy
        m_ldb_options.compact_strategy_factory =
            new DefaultCompactStrategyFactory(m_table_schema);
    } else {
        m_ldb_options.compact_strategy_factory =
            new leveldb::DummyCompactStrategyFactory();
    }

    std::set<uint32_t>* exist_lg_list = new std::set<uint32_t>;
    std::map<uint32_t, leveldb::LG_info*>* lg_info_list =
        new std::map<uint32_t, leveldb::LG_info*>;

    int64_t triggered_log_size = 0;
    for (int32_t lg_i = 0; lg_i < m_table_schema.locality_groups_size();
         ++lg_i) {
        if (m_table_schema.locality_groups(lg_i).is_del()) {
            continue;
        }
        const LocalityGroupSchema& lg_schema =
            m_table_schema.locality_groups(lg_i);
        bool compress = lg_schema.compress_type();
        StoreMedium store = lg_schema.store_type();

        leveldb::LG_info* lg_info = new leveldb::LG_info(lg_schema.id());

        if (store == MemoryStore) {
            m_ldb_options.env = lg_info->env = LeveldbMemEnv();
            m_ldb_options.seek_latency = 0;
            m_ldb_options.block_cache =
                leveldb::NewLRUCache(FLAGS_tera_memenv_block_cache_size * 1024 * 1024);
            m_mem_store_activated = true;
        } else if (store == FlashStore) {
            if (!FLAGS_tera_tabletnode_cache_enabled) {
                m_ldb_options.env = lg_info->env = LeveldbFlashEnv();
            } else {
                LOG(INFO) << "activate block-level Cache store";
                m_ldb_options.env = lg_info->env = leveldb::EnvThreeLevelCache();
            }
            m_ldb_options.seek_latency = FLAGS_tera_leveldb_env_local_seek_latency;
        } else {
            m_ldb_options.env = lg_info->env = LeveldbBaseEnv();
            m_ldb_options.seek_latency = FLAGS_tera_leveldb_env_dfs_seek_latency;
        }

        if (compress) {
            lg_info->compression = leveldb::kSnappyCompression;
        }

        lg_info->block_size = lg_schema.block_size() * 1024;
        if (lg_schema.use_memtable_on_leveldb()) {
            lg_info->use_memtable_on_leveldb = true;
            lg_info->memtable_ldb_write_buffer_size =
                lg_schema.memtable_ldb_write_buffer_size() * 1024;
            lg_info->memtable_ldb_block_size =
                lg_schema.memtable_ldb_block_size() * 1024;
            LOG(INFO) << "enable mem-ldb for LG:" << lg_schema.name().c_str()
                << ", buffer_size:" << lg_info->memtable_ldb_write_buffer_size
                << ", block_size:"  << lg_info->memtable_ldb_block_size;
        }
        lg_info->sst_size = lg_schema.sst_size();
        m_ldb_options.sst_size = lg_schema.sst_size();
        // FLAGS_tera_tablet_write_buffer_size is the max buffer size
        int64_t max_size = FLAGS_tera_tablet_max_write_buffer_size * 1024 * 1024;
        if (lg_schema.sst_size() * 4 < max_size) {
            lg_info->write_buffer_size = lg_schema.sst_size() * 4;
        } else {
            lg_info->write_buffer_size = max_size;
        }
        triggered_log_size += lg_info->write_buffer_size;
        exist_lg_list->insert(lg_i);
        (*lg_info_list)[lg_i] = lg_info;
    }

    if (exist_lg_list->size() == 0) {
        delete exist_lg_list;
    } else {
        m_ldb_options.exist_lg_list = exist_lg_list;
        m_ldb_options.flush_triggered_log_size = triggered_log_size * 2;
    }
    if (lg_info_list->size() == 0) {
        delete lg_info_list;
    } else {
        m_ldb_options.lg_info_list = lg_info_list;
    }

    IndexingCfToLG();
}

void TabletIO::TearDownOptionsForLG() {
    if (m_ldb_options.compact_strategy_factory) {
        delete m_ldb_options.compact_strategy_factory;
        m_ldb_options.compact_strategy_factory = NULL;
    }

    if (m_ldb_options.exist_lg_list) {
        m_ldb_options.exist_lg_list->clear();
        delete m_ldb_options.exist_lg_list;
        m_ldb_options.exist_lg_list = NULL;
    }

    if (m_ldb_options.lg_info_list) {
        std::map<uint32_t, leveldb::LG_info*>::iterator it =
            m_ldb_options.lg_info_list->begin();
        for (; it != m_ldb_options.lg_info_list->end(); ++it) {
            delete it->second;
        }
        delete m_ldb_options.lg_info_list;
        m_ldb_options.lg_info_list = NULL;
    }
}

void TabletIO::IndexingCfToLG() {
    for (int32_t i = 0; i < m_table_schema.locality_groups_size(); ++i) {
        const LocalityGroupSchema& lg_schema =
            m_table_schema.locality_groups(i);
        m_lg_id_map[lg_schema.name()] = i; // lg_schema.id();
    }
    for (int32_t i = 0; i < m_table_schema.column_families_size(); ++i) {
        const ColumnFamilySchema& cf_schema =
            m_table_schema.column_families(i);

        std::map<std::string, uint32_t>::iterator it =
            m_lg_id_map.find(cf_schema.locality_group());
        if (it == m_lg_id_map.end()) {
            // using default lg for not-defined descor
            m_cf_lg_map[cf_schema.name()] = 0;
        } else {
            m_cf_lg_map[cf_schema.name()] = it->second;
        }
    }
}

void TabletIO::SetupIteratorOptions(const ScanOptions& scan_options,
                                    leveldb::ReadOptions* leveldb_opts) {
    MutexLock lock(&m_schema_mutex);
    std::set<uint32_t> target_lgs;
    std::set<std::string>::const_iterator cf_it = scan_options.iter_cf_set.begin();
    for (; cf_it != scan_options.iter_cf_set.end(); ++cf_it) {
        std::map<std::string, uint32_t>::iterator map_it =
            m_cf_lg_map.find(*cf_it);
        if (map_it != m_cf_lg_map.end()) {
            target_lgs.insert(map_it->second);
        }
    }
    if (target_lgs.size() > 0) {
        leveldb_opts->target_lgs = new std::set<uint32_t>(target_lgs);
    }
}

void TabletIO::TearDownIteratorOptions(leveldb::ReadOptions* opts) {
    if (opts->target_lgs) {
        delete opts->target_lgs;
        opts->target_lgs = NULL;
    }
}

bool TabletIO::ShouldFilterRowBuffer(std::list<KeyValuePair>& row_buf,
                                     const ScanOptions& scan_options) {
    if (row_buf.size() <= 0) {
        return true;
    }
    std::list<KeyValuePair>::iterator it;
    int filter_num = scan_options.filter_list.filter_size();

    VLOG(10) << "Filter check: kv_num: " << row_buf.size()
        << ", filter_num: " << filter_num;

    for (int i = 0; i < filter_num; ++i) {
        const Filter& filter = scan_options.filter_list.filter(i);
        for (it = row_buf.begin(); it != row_buf.end(); ++it) {
            if (it->column_family() != filter.content()) {
                continue;
            }
            if (filter.value_type() != kINT64) {
                LOG(ERROR) << "only support int64 value.";
                return true;
            }
            if (!CheckCell(*it, filter)) {
                return true;
            }
        }
    }
    return false;
}

void TabletIO::ProcessRowBuffer(std::list<KeyValuePair>& row_buf,
                                const ScanOptions& scan_options,
                                RowResult* value_list,
                                uint32_t* buffer_size,
                                int64_t* number_limit) {
    if (row_buf.size() <= 0) {
        return;
    }
    if (ShouldFilterRowBuffer(row_buf, scan_options)) {
        return;
    }

    std::list<KeyValuePair>::iterator it;
    for (it = row_buf.begin(); it != row_buf.end(); ++it) {
        const std::string& key = it->key();
        const std::string& col = it->column_family();
        const std::string& qual = it->qualifier();
        const std::string& value = it->value();
        int64_t ts = it->timestamp();

        // skip unnecessary columns and qualifiers
        if (scan_options.column_family_list.size() > 0) {
            ColumnFamilyMap::const_iterator it =
                scan_options.column_family_list.find(col);
            if (it != scan_options.column_family_list.end()) {
                const std::set<std::string>& qual_list = it->second;
                if (qual_list.size() > 0 && qual_list.end() == qual_list.find(qual)) {
                    continue;
                }
            } else {
                continue;
            }
        }
        // time range filter
        if (ts < scan_options.ts_start || ts > scan_options.ts_end) {
            continue;
        }

        value_list->add_key_values()->CopyFrom(*it);
        (*number_limit)++;
        *buffer_size += key.size() + col.size() + qual.size()
            + sizeof(ts) + value.size();
    }
}

uint64_t TabletIO::GetSnapshot(uint64_t id, uint64_t snapshot_sequence,
                               StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady) {
            SetStatusCode(m_status, status);
            return 0;
        }
        m_db_ref_count++;
    }
    uint64_t snapshot = m_db->GetSnapshot(snapshot_sequence);
    MutexLock lock(&m_mutex);
    id_to_snapshot_num_[id] = snapshot;
    m_db_ref_count--;
    return snapshot;
}

bool TabletIO::ReleaseSnapshot(uint64_t snapshot_id, StatusCode* status) {
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady) {
            SetStatusCode(m_status, status);
            return false;
        }
        if (id_to_snapshot_num_.find(snapshot_id) == id_to_snapshot_num_.end()) {
            SetStatusCode(kSnapshotNotExist, status);
            return false;
        }
        m_db_ref_count++;
    }
    m_db->ReleaseSnapshot(id_to_snapshot_num_[snapshot_id]);
    MutexLock lock(&m_mutex);
    id_to_snapshot_num_.erase(snapshot_id);
    m_db_ref_count--;
    return true;
}

void TabletIO::ListSnapshot(std::vector<uint64_t>* snapshot_id) {
    MutexLock lock(&m_mutex);
    if (m_status != kReady) {
        return;
    }
    for (std::map<uint64_t, uint64_t>::iterator it = id_to_snapshot_num_.begin();
         it != id_to_snapshot_num_.end(); ++it) {
        snapshot_id->push_back(it->first);
        VLOG(7) << m_tablet_path << " ListSnapshot: " << it->first << " - " << it->second;
    }
}

uint64_t TabletIO::Rollback(uint64_t snapshot_id, StatusCode* status) {
    uint64_t sequence;
    {
        MutexLock lock(&m_mutex);
        if (m_status != kReady) {
            SetStatusCode(m_status, status);
            return false;
        }
        std::map<uint64_t, uint64_t>::iterator it = id_to_snapshot_num_.find(snapshot_id);
        if (it == id_to_snapshot_num_.end()) {
            SetStatusCode(kSnapshotNotExist, status);
            return false;
        } else {
            sequence = it->second;
        }
        m_db_ref_count++;
    }
    uint64_t rollback_point = m_db->Rollback(sequence);
    MutexLock lock(&m_mutex);
    rollbacks_[sequence] = rollback_point;
    m_db_ref_count--;
    return rollback_point;
}

uint32_t TabletIO::GetLGidByCFName(const std::string& cfname) {
    MutexLock lock(&m_schema_mutex);
    std::map<std::string, uint32_t>::iterator it = m_cf_lg_map.find(cfname);
    if (it != m_cf_lg_map.end()) {
        return it->second;
    }
    return 0;
}

void TabletIO::SetStatus(TabletStatus status) {
    MutexLock lock(&m_mutex);
    m_status = status;
}

TabletIO::TabletStatus TabletIO::GetStatus() {
    MutexLock lock(&m_mutex);
    return m_status;
}

const leveldb::RawKeyOperator* TabletIO::GetRawKeyOperator() {
    return m_key_operator;
}

void TabletIO::GetAndClearCounter(TabletCounter* counter) {
    counter->set_low_read_cell(m_counter.low_read_cell.Clear());
    counter->set_scan_rows(m_counter.scan_rows.Clear());
    counter->set_scan_kvs(m_counter.scan_kvs.Clear());
    counter->set_scan_size(m_counter.scan_size.Clear());
    counter->set_read_rows(m_counter.read_rows.Clear());
    counter->set_read_kvs(m_counter.read_kvs.Clear());
    counter->set_read_size(m_counter.read_size.Clear());
    counter->set_write_rows(m_counter.write_rows.Clear());
    counter->set_write_kvs(m_counter.write_kvs.Clear());
    counter->set_write_size(m_counter.write_size.Clear());
    counter->set_is_on_busy(IsBusy());
    double write_workload = 0;
    Workload(&write_workload);
    counter->set_write_workload(write_workload);
}

int32_t TabletIO::AddRef() {
    MutexLock lock(&m_mutex);
    ++m_ref_count;
    return m_ref_count;
}

int32_t TabletIO::DecRef() {
    int32_t ref = 0;
    {
        MutexLock lock(&m_mutex);
        ref = (--m_ref_count);
    }
    if (ref == 0) {
        delete this;
    }
    return ref;
}

int32_t TabletIO::GetRef() const {
    return m_ref_count;
}

void TabletIO::ApplySchema(const TableSchema& schema) {
    MutexLock lock(&m_schema_mutex);
    SetSchema(schema);
    IndexingCfToLG();
    m_ldb_options.compact_strategy_factory->SetArg(&schema);
}

} // namespace io
} // namespace tera
