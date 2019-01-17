// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/tablet_io.h"

#include <stdint.h>
#include <memory>

#include "common/counter.h"
#include "common/metric/prometheus_subscriber.h"
#include "common/metric/ratio_subscriber.h"
#include "common/this_thread.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "leveldb/cache.h"
#include "leveldb/compact_strategy.h"
#include "leveldb/env.h"
#include "leveldb/env_dfs.h"
#include "leveldb/env_flash.h"
#include "leveldb/env_inmem.h"
#include "leveldb/env_mock.h"
#include "leveldb/filter_policy.h"
#include "leveldb/raw_key_operator.h"
#include "io/coding.h"
#include "io/default_compact_strategy.h"
#include "io/io_utils.h"
#include "io/tablet_writer.h"
#include "io/timekey_comparator.h"
#include "io/ttlkv_compact_strategy.h"
#include "io/utils_leveldb.h"
#include "tabletnode/tabletnode_metric_name.h"
#include "types.h"
#include "utils/string_util.h"
#include "utils/utils_cmd.h"
#include "io_utils.h"
#include "sdk/filter_utils.h"

DECLARE_string(tera_leveldb_env_type);
DECLARE_int64(tera_tablet_log_file_size);
DECLARE_int64(tera_tablet_max_write_buffer_size);
DECLARE_int64(tera_tablet_write_block_size);
DECLARE_int32(tera_tablet_level0_file_limit);
DECLARE_int32(tera_tablet_ttl_percentage);
DECLARE_int32(tera_tablet_del_percentage);
DECLARE_int32(tera_tablet_max_block_log_number);
DECLARE_int64(tera_tablet_write_log_time_out);
DECLARE_bool(tera_log_async_mode);

DECLARE_int64(tera_tablet_living_period);
DECLARE_int32(tera_tablet_flush_log_num);

DECLARE_int32(tera_io_retry_period);
DECLARE_int32(tera_io_retry_max_times);

DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_uint64(tera_leveldb_manifest_switch_size_MB);
DECLARE_string(tera_leveldb_compact_strategy);
DECLARE_bool(tera_leveldb_verify_checksums);
DECLARE_bool(tera_leveldb_ignore_corruption_in_compaction);
DECLARE_bool(tera_leveldb_use_file_lock);

DECLARE_int32(tera_tabletnode_scan_pack_max_size);
DECLARE_uint64(tera_tabletnode_prefetch_scan_size);
DECLARE_int32(tera_leveldb_env_local_seek_latency);
DECLARE_int32(tera_leveldb_env_dfs_seek_latency);
DECLARE_int32(tera_memenv_table_cache_size);
DECLARE_bool(tera_use_flash_for_memenv);

DECLARE_bool(tera_tablet_use_memtable_on_leveldb);
DECLARE_int64(tera_tablet_memtable_ldb_write_buffer_size);
DECLARE_int64(tera_tablet_memtable_ldb_block_size);

DECLARE_bool(tera_leveldb_ignore_corruption_in_open);
DECLARE_int32(tera_leveldb_slow_down_level0_score_limit);
DECLARE_int32(tera_leveldb_max_background_compactions);
DECLARE_int32(tera_tablet_max_sub_parallel_compaction);
DECLARE_int32(tera_tablet_unload_count_limit);

DECLARE_bool(debug_tera_tablet_unload);

DECLARE_bool(tera_leveldb_use_direct_io_read);
DECLARE_bool(tera_leveldb_use_direct_io_write);
DECLARE_uint64(tera_leveldb_posix_write_buffer_size);
DECLARE_uint64(tera_leveldb_table_builder_write_batch_size);
DECLARE_int32(tera_leveldb_memtable_shard_num);

DECLARE_bool(tera_enable_persistent_cache);
DECLARE_bool(enable_dfs_read_thread_limiter);

namespace tera {
namespace io {

using tera::tabletnode::kRowDelayMetric;
using tera::tabletnode::kRowCountMetric;
using tera::tabletnode::kRowThroughPutMetric;

using tera::tabletnode::kApiLabelRead;
using tera::tabletnode::kApiLabelScan;
using tera::tabletnode::kApiLabelWrite;

using tera::tabletnode::kLowLevelReadMetric;
using tera::tabletnode::kScanDropCountMetric;
using tera::tabletnode::kScanFilterCountMetric;
using tera::tabletnode::kBatchScanCountMetric;
using tera::tabletnode::kSyncScanCountMetric;

tera::MetricCounter low_level_read_count(kLowLevelReadMetric, {SubscriberType::QPS});
tera::MetricCounter scan_drop_count(kScanDropCountMetric, {SubscriberType::QPS});
tera::MetricCounter scan_filter_count(kScanFilterCountMetric, {SubscriberType::QPS});
tera::MetricCounter batch_scan_count(kBatchScanCountMetric, {SubscriberType::QPS});
tera::MetricCounter sync_scan_count(kSyncScanCountMetric, {SubscriberType::QPS});

tera::MetricCounter row_read_delay(kRowDelayMetric, kApiLabelRead, {});
tera::MetricCounter row_read_count(kRowCountMetric, kApiLabelRead, {SubscriberType::QPS});
tera::MetricCounter row_read_bytes(kRowThroughPutMetric, kApiLabelRead,
                                   {SubscriberType::THROUGHPUT});

tera::MetricCounter row_scan_delay(kRowDelayMetric, kApiLabelScan, {});
tera::MetricCounter row_scan_count(kRowCountMetric, kApiLabelScan, {SubscriberType::QPS});
tera::MetricCounter row_scan_bytes(kRowThroughPutMetric, kApiLabelScan,
                                   {SubscriberType::THROUGHPUT});

tera::MetricCounter row_write_bytes(kRowThroughPutMetric, kApiLabelWrite,
                                    {SubscriberType::THROUGHPUT});

tera::AutoSubscriberRegister row_read_delay_per_row(std::unique_ptr<Subscriber>(
    new tera::RatioSubscriber(MetricId("tera_ts_row_read_delay_us_per_row"),
                              std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
                                  MetricId(kRowDelayMetric, kApiLabelRead), SubscriberType::SUM)),
                              std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
                                  MetricId(kRowCountMetric, kApiLabelRead),
                                  SubscriberType::SUM)))));

tera::AutoSubscriberRegister row_scan_delay_per_row(std::unique_ptr<Subscriber>(
    new tera::RatioSubscriber(MetricId("tera_ts_row_scan_delay_us_per_row"),
                              std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
                                  MetricId(kRowDelayMetric, kApiLabelScan), SubscriberType::SUM)),
                              std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
                                  MetricId(kRowCountMetric, kApiLabelScan),
                                  SubscriberType::SUM)))));

std::ostream& operator<<(std::ostream& o, const TabletIO& tablet_io) {
  o << tablet_io.short_path_ << " [" << DebugString(tablet_io.start_key_) << ", "
    << DebugString(tablet_io.end_key_) << "]";
  return o;
}

std::string MetricLabelToString(const std::string& tablet_path) {
  size_t sep_pos = tablet_path.find_last_of("/");
  if (sep_pos == std::string::npos) {
    // meta tablet
    return LabelStringBuilder()
        .Append("table", tablet_path)
        .Append("tablet", tablet_path)
        .ToString();
  } else {
    std::string table_name = tablet_path.substr(0, sep_pos);
    return LabelStringBuilder()
        .Append("table", table_name)
        .Append("tablet", tablet_path)
        .ToString();
  }
}
TabletIO::TabletIO(const std::string& key_start, const std::string& key_end,
                   const std::string& path)
    : TabletIO(key_start, key_end, path, 0, 0) {}

TabletIO::TabletIO(const std::string& key_start, const std::string& key_end,
                   const std::string& path, int64_t ctime, uint64_t version)
    : async_writer_(NULL),
      scan_context_manager_(NULL),
      start_key_(key_start),
      end_key_(key_end),
      ctime_(ctime),
      version_(version),
      short_path_(path),
      compact_status_(kTableNotCompact),
      status_(kNotInit),
      tablet_status_(static_cast<tera::TabletMeta::TabletStatus>(kTabletReady)),
      last_err_msg_(""),
      ref_count_(1),
      db_ref_count_(0),
      db_(NULL),
      m_memory_cache(NULL),
      kv_only_(false),
      key_operator_(NULL),
      try_unload_count_(0),
      counter_(short_path_),
      mock_env_(NULL) {}

TabletIO::~TabletIO() {
  if (status_ != kNotInit && !Unload()) {
    if (async_writer_ != NULL) {
      async_writer_->Stop();
      delete async_writer_;
      async_writer_ = NULL;
    }
    delete db_;
  }
}

void TabletIO::SetMockEnv(leveldb::Env* e) { mock_env_ = e; }

std::string TabletIO::GetTableName() const {
  MutexLock lock(&schema_mutex_);
  return table_schema_.name();
}

std::string TabletIO::GetTablePath() const {
  if (!tablet_path_.empty()) {
    std::string path = tablet_path_.substr(FLAGS_tera_tabletnode_path_prefix.size());
    if (path.at(0) == '/') {
      path = path.substr(1);
    }
    return path;
  } else {
    return tablet_path_;
  }
}

std::string TabletIO::GetStartKey() const { return start_key_; }

std::string TabletIO::GetEndKey() const { return end_key_; }

const std::string& TabletIO::GetMetricLabel() const { return counter_.label; }

CompactStatus TabletIO::GetCompactStatus() const { return compact_status_; }

void TabletIO::SetSchema(const TableSchema& schema) { table_schema_.CopyFrom(schema); }

TableSchema TabletIO::GetSchema() const {
  MutexLock lock(&schema_mutex_);
  return table_schema_;
}

RawKey TabletIO::RawKeyType() const {
  MutexLock lock(&schema_mutex_);
  return table_schema_.raw_key();
};

TabletIO::StatCounter& TabletIO::GetCounter() { return counter_; }

void TabletIO::SetMemoryCache(leveldb::Cache* cache) { m_memory_cache = cache; }

bool TabletIO::Load(const TableSchema& schema, const std::string& path,
                    const std::vector<uint64_t>& parent_tablets,
                    const std::set<std::string>& ignore_err_lgs, leveldb::Logger* logger,
                    leveldb::Cache* block_cache, leveldb::TableCache* table_cache,
                    StatusCode* status) {
  {
    MutexLock lock(&mutex_);
    if (status_ == kReady) {
      return true;
    } else if (status_ != kNotInit) {
      SetStatusCode(status_, status);
      return false;
    }
    status_ = kOnLoad;
    db_ref_count_++;
  }

  // any type of table should have at least 1lg+1cf.
  table_schema_.CopyFrom(schema);
  if (table_schema_.locality_groups_size() == 0) {
    // only prepare for kv-only mode, no need to set fields of it.
    table_schema_.add_locality_groups();
  }

  RawKey raw_key = table_schema_.raw_key();
  if (raw_key == TTLKv || raw_key == GeneralKv) {
    kv_only_ = true;
  } else {
    // for compatible
    if (table_schema_.column_families_size() == 0) {
      // only prepare for kv-only mode, no need to set fields of it.
      table_schema_.add_column_families();
      kv_only_ = true;
    } else {
      kv_only_ = table_schema_.kv_only();
    }
  }

  if (kv_only_) {
    ldb_options_.memtable_shard_num = 0;
  } else {
    ldb_options_.memtable_shard_num = FLAGS_tera_leveldb_memtable_shard_num;
  }

  key_operator_ = GetRawKeyOperatorFromSchema(table_schema_);
  // [raw_start_key_, raw_end_key_)
  raw_start_key_ = start_key_;
  if (!kv_only_ && !start_key_.empty()) {
    key_operator_->EncodeTeraKey(start_key_, "", "", kLatestTs, leveldb::TKT_FORSEEK,
                                 &raw_start_key_);
  } else if (kv_only_ && table_schema_.raw_key() == TTLKv && !start_key_.empty()) {
    key_operator_->EncodeTeraKey(start_key_, "", "", 0, leveldb::TKT_FORSEEK, &raw_start_key_);
  }
  raw_end_key_ = end_key_;
  if (!kv_only_ && !end_key_.empty()) {
    key_operator_->EncodeTeraKey(end_key_, "", "", kLatestTs, leveldb::TKT_FORSEEK, &raw_end_key_);
  } else if (kv_only_ && table_schema_.raw_key() == TTLKv && !end_key_.empty()) {
    key_operator_->EncodeTeraKey(end_key_, "", "", 0, leveldb::TKT_FORSEEK, &raw_end_key_);
  }

  ldb_options_.key_start = raw_start_key_;
  ldb_options_.key_end = raw_end_key_;
  ldb_options_.l0_slowdown_writes_trigger = FLAGS_tera_tablet_level0_file_limit;
  ldb_options_.max_sub_parallel_compaction = FLAGS_tera_tablet_max_sub_parallel_compaction;
  ldb_options_.ttl_percentage = FLAGS_tera_tablet_ttl_percentage;
  ldb_options_.del_percentage = FLAGS_tera_tablet_del_percentage;
  ldb_options_.block_size = FLAGS_tera_tablet_write_block_size * 1024;
  ldb_options_.max_block_log_number = FLAGS_tera_tablet_max_block_log_number;
  ldb_options_.write_log_time_out = FLAGS_tera_tablet_write_log_time_out;
  ldb_options_.log_async_mode = FLAGS_tera_log_async_mode;
  ldb_options_.info_log = logger;
  ldb_options_.max_open_files = FLAGS_tera_memenv_table_cache_size;
  ldb_options_.manifest_switch_size = FLAGS_tera_leveldb_manifest_switch_size_MB;
  ldb_options_.max_background_compactions = FLAGS_tera_leveldb_max_background_compactions;
  ldb_options_.slow_down_level0_score_limit = FLAGS_tera_leveldb_slow_down_level0_score_limit;
  ldb_options_.ignore_corruption_in_open = FLAGS_tera_leveldb_ignore_corruption_in_open;

  ldb_options_.use_memtable_on_leveldb = FLAGS_tera_tablet_use_memtable_on_leveldb;
  ldb_options_.memtable_ldb_write_buffer_size =
      FLAGS_tera_tablet_memtable_ldb_write_buffer_size * 1024;
  ldb_options_.memtable_ldb_block_size = FLAGS_tera_tablet_memtable_ldb_block_size * 1024;
  if (FLAGS_tera_tablet_use_memtable_on_leveldb) {
    LOG(INFO) << "enable mem-ldb for this tablet-server:"
              << " buffer_size:" << ldb_options_.memtable_ldb_write_buffer_size
              << ", block_size:" << ldb_options_.memtable_ldb_block_size;
  }

  uint32_t bloom_filter_bits_per_key = table_schema_.has_bloom_filter_bits_per_key()
                                           ? table_schema_.bloom_filter_bits_per_key()
                                           : 10;

  LOG(INFO) << "Use " << bloom_filter_bits_per_key << " bits per key for bloom filter.";

  if (kv_only_ && table_schema_.raw_key() == TTLKv) {
    ldb_options_.filter_policy = leveldb::NewTTLKvBloomFilterPolicy(bloom_filter_bits_per_key);
  } else if (kv_only_) {
    ldb_options_.filter_policy = leveldb::NewBloomFilterPolicy(bloom_filter_bits_per_key);
  } else if (table_schema_.raw_key() == Readable) {
    ldb_options_.filter_policy = leveldb::NewRowKeyBloomFilterPolicy(
        bloom_filter_bits_per_key, leveldb::ReadableRawKeyOperator());
  } else {
    CHECK_EQ(table_schema_.raw_key(), Binary);
    ldb_options_.filter_policy = leveldb::NewRowKeyBloomFilterPolicy(
        bloom_filter_bits_per_key, leveldb::BinaryRawKeyOperator());
  }
  ldb_options_.block_cache = block_cache;
  ldb_options_.table_cache = table_cache;
  ldb_options_.flush_triggered_log_num = FLAGS_tera_tablet_flush_log_num;
  ldb_options_.log_file_size = FLAGS_tera_tablet_log_file_size * 1024 * 1024;
  ldb_options_.parent_tablets = parent_tablets;
  if (table_schema_.raw_key() == Binary) {
    ldb_options_.raw_key_format = leveldb::kBinary;
    ldb_options_.comparator = leveldb::TeraBinaryComparator();
  } else if (table_schema_.raw_key() == TTLKv) {  // KV-Pair-With-TTL
    ldb_options_.raw_key_format = leveldb::kTTLKv;
    ldb_options_.comparator = leveldb::TeraTTLKvComparator();
    ldb_options_.enable_strategy_when_get = true;  // active usage of strategy in DB::Get
  } else {                                         // Readable-Table && KV-Pair-Without-TTL
    ldb_options_.raw_key_format = leveldb::kReadable;
    ldb_options_.comparator = leveldb::BytewiseComparator();
  }
  ldb_options_.verify_checksums_in_compaction = FLAGS_tera_leveldb_verify_checksums;
  ldb_options_.ignore_corruption_in_compaction = FLAGS_tera_leveldb_ignore_corruption_in_compaction;
  ldb_options_.use_file_lock = FLAGS_tera_leveldb_use_file_lock;
  ldb_options_.disable_wal = table_schema_.disable_wal();
  SetupOptionsForLG(ignore_err_lgs);

  std::string path_prefix = FLAGS_tera_tabletnode_path_prefix;
  if (*path_prefix.rbegin() != '/') {
    path_prefix.push_back('/');
  }

  tablet_path_ = path_prefix + path;
  ldb_options_.dfs_storage_path_prefix = path_prefix;
  LOG(INFO) << "[Load] Start Open " << tablet_path_ << ", kv_only " << kv_only_
            << ", raw_key_operator " << key_operator_->Name();

  leveldb::Status db_status = leveldb::DB::Open(ldb_options_, tablet_path_, &db_);

  if (!db_status.ok()) {
    LOG(ERROR) << "fail to open table: " << tablet_path_ << ", " << db_status.ToString();
    {
      MutexLock lock(&mutex_);
      status_ = kNotInit;
      last_err_msg_ = db_status.ToString();
      db_ref_count_--;
    }
    SetStatusCode(db_status, status);
    //         delete ldb_options_.env;
    return false;
  }

  async_writer_ = new TabletWriter(this);
  async_writer_->Start();

  scan_context_manager_ = new ScanContextManager;

  {
    MutexLock lock(&mutex_);
    status_ = kReady;
    // reset try unload count to 0 for ready
    try_unload_count_ = 0;
    db_ref_count_--;
  }

  LOG(INFO) << "[Load] Load " << tablet_path_ << " done";
  return true;
}

bool TabletIO::ShouldForceUnloadOnError() {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      return false;
    }
    db_ref_count_++;
  }
  // If TabletIO is Ready but has encountered some fatal errors
  bool ret = db_->ShouldForceUnloadOnError();
  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
  return ret;
}

bool TabletIO::Unload(StatusCode* status) {
  {
    MutexLock lock(&mutex_);
    // inc try unload times
    ++try_unload_count_;
    LOG(INFO) << "tablet " << tablet_path_ << " unload try times:" << try_unload_count_;
    if (status_ != kReady) {
      SetStatusCode(status_, status);
      return false;
    }
    status_ = kUnloading;
    db_ref_count_++;
  }

  LOG(INFO) << "[Unload] start shutdown1 " << tablet_path_;
  leveldb::Status s = db_->Shutdown1();
  {
    MutexLock lock(&mutex_);
    status_ = kUnloading2;
  }

  uint32_t retry = 0;
  while (db_ref_count_ > 1) {
    LOG(ERROR) << "tablet is busy, db ref: " << db_ref_count_ << ", try again unload: " << retry++
               << " " << tablet_path_;
    ThisThread::Sleep(FLAGS_tera_io_retry_period);
  }

  LOG(INFO) << "[Unload] stop async writer " << tablet_path_;
  async_writer_->Stop();
  delete async_writer_;
  async_writer_ = NULL;

  if (s.ok()) {
    LOG(INFO) << "[Unload] start shutdown2 " << tablet_path_;
    db_->Shutdown2();
  } else {
    LOG(INFO) << "[Unload] shutdown1 failed, keep log " << tablet_path_;
  }

  delete scan_context_manager_;
  delete db_;
  db_ = NULL;

  delete ldb_options_.filter_policy;
  TearDownOptionsForLG();
  LOG(INFO) << "[Unload] done " << tablet_path_;

  {
    MutexLock lock(&mutex_);
    status_ = kNotInit;
    db_ref_count_--;
  }
  return true;
}

// Find average string from input string
// E.g. "abc" & "abe" return "abd"
//      "a" & "b" return "a_"
bool TabletIO::FindAverageKey(const std::string& start, const std::string& end, std::string* res) {
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

  // algorithm: use big number ADD and division
  unsigned int carry[max_len + 1];
  unsigned int sum[max_len];
  carry[max_len] = 0;
  for (int i = max_len - 1; i >= 0; --i) {
    sum[i] = (unsigned char)s[i] + (unsigned char)e[i] + carry[i + 1];
    carry[i] = sum[i] / 256;
    sum[i] %= 256;
  }
  memset((char*)carry + sizeof(int), '\0', (max_len) * sizeof(int));
  for (int i = 0; i < max_len; ++i) {
    carry[i + 1] = (sum[i] + carry[i] * 256) % 2;
    sum[i] = (sum[i] + carry[i] * 256) / 2;
  }
  std::string ave_key;
  for (int i = 0; i < max_len; ++i) {
    ave_key.append(1, char(sum[i]));
    if (ave_key > start && (end == "" || ave_key < end)) {
      break;
    }
  }
  CHECK(ave_key > start && (end == "" || ave_key < end));
  *res = ave_key;
  return true;
}

bool TabletIO::ParseRowKey(const std::string& tera_key, std::string* row_key) {
  leveldb::Slice row;
  if ((RawKeyType() == GeneralKv) || (kv_only_ && RawKeyType() == Readable)) {
    row = tera_key;
  } else {  // Table && TTL-KV
    if (!key_operator_->ExtractTeraKey(tera_key, &row, NULL, NULL, NULL, NULL)) {
      VLOG(5) << "fail to extract split key";
      return false;
    }
  }
  *row_key = row.ToString();
  return true;
}

bool TabletIO::Split(std::string* split_key, StatusCode* status) {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      SetStatusCode(status_, status);
      return false;
    }
    if (compact_status_ == kTableOnCompact) {
      SetStatusCode(kTableNotSupport, status);
      return false;
    }
    db_ref_count_++;
  }

  if (split_key->empty()) {
    std::string raw_split_key;
    if (db_->FindSplitKey(0.5, &raw_split_key)) {
      ParseRowKey(raw_split_key, split_key);
    }

    if (split_key->empty() || *split_key == end_key_) {
      // could not find split_key, try calc average key
      std::string smallest_key, largest_key;
      CHECK(db_->FindKeyRange(&smallest_key, &largest_key));

      std::string srow_key, lrow_key;
      if (!smallest_key.empty()) {
        ParseRowKey(smallest_key, &srow_key);
      } else {
        srow_key = start_key_;
      }
      if (!largest_key.empty()) {
        ParseRowKey(largest_key, &lrow_key);
      } else {
        lrow_key = end_key_;
      }
      FindAverageKey(srow_key, lrow_key, split_key);
    }
  }
  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }

  VLOG(5) << "start: [" << DebugString(start_key_) << "], end: [" << DebugString(end_key_)
          << "], split: [" << DebugString(*split_key) << "]";

  if (*split_key != "" && *split_key > start_key_ && (end_key_ == "" || *split_key < end_key_)) {
    return true;
  } else {
    SetStatusCode(kTableNotSupport, status);
    return false;
  }
}

bool TabletIO::Compact(int lg_no, StatusCode* status, CompactionType type) {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      SetStatusCode(status_, status);
      return false;
    }
    if (compact_status_ == kTableOnCompact) {
      return false;
    }
    compact_status_ = kTableOnCompact;
    db_ref_count_++;
  }
  CHECK_NOTNULL(db_);
  if (type == kManualCompaction) {
    db_->CompactRange(NULL, NULL, lg_no);
  } else if (type == kMinorCompaction) {
    db_->MinorCompact();
  }

  {
    MutexLock lock(&mutex_);
    compact_status_ = kTableCompacted;
    db_ref_count_--;
  }
  return true;
}

bool TabletIO::AddInheritedLiveFiles(std::vector<std::set<uint64_t> >* live) {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      LOG(INFO) << "[gc] tablet not ready, skip it.";
      return false;
    }
    db_ref_count_++;
  }
  {
    MutexLock lock(&schema_mutex_);
    if (live->size() == 0) {
      live->resize(table_schema_.locality_groups_size());
    } else {
      CHECK(live->size() == static_cast<uint64_t>(table_schema_.locality_groups_size()));
    }
  }
  db_->AddInheritedLiveFiles(live);
  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
  return true;
}

bool TabletIO::IsBusy() {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      return false;
    }
    db_ref_count_++;
  }
  bool is_busy = db_->BusyWrite();
  is_busy = is_busy ? true : async_writer_->IsBusy();
  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
  return is_busy;
}
bool TabletIO::Workload(double* write_workload) {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      return false;
    }
    db_ref_count_++;
  }

  // if busy cause by write log, set workload score more than 10, because level
  // 0
  // limits to 20 sst files by default, which score is 10.
  db_->Workload(write_workload);
  if (*write_workload < 10.618 && async_writer_->IsBusy()) {
    *write_workload = 10.618;
  }

  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
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

bool TabletIO::GetDataSize(uint64_t* size, std::vector<uint64_t>* lgsize, uint64_t* mem_table_size,
                           StatusCode* status) {
  {
    MutexLock lock(&mutex_);
    if ((status_ != kReady && status_ != kUnloading) || IsUrgentUnload()) {
      SetStatusCode(status_, status);
      return false;
    }
    db_ref_count_++;
  }

  db_->GetApproximateSizes(size, lgsize, mem_table_size);
  VLOG(10) << "GetDataSize(" << tablet_path_ << ") : " << *size;
  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
  if (*size == 0) {
    // return reserved buffer size
    *size = FLAGS_tera_tablet_write_block_size * 1024;
  }
  return true;
}

bool TabletIO::Read(const leveldb::Slice& key, std::string* value, uint64_t snapshot_id,
                    StatusCode* status) {
  CHECK_NOTNULL(db_);
  leveldb::ReadOptions read_option(&ldb_options_);
  read_option.enable_dfs_read_thread_limiter = FLAGS_enable_dfs_read_thread_limiter;
  read_option.verify_checksums = FLAGS_tera_leveldb_verify_checksums;
  if (snapshot_id != 0) {
    if (!SnapshotIDToSeq(snapshot_id, &read_option.snapshot)) {
      *status = kSnapshotNotExist;
      return false;
    }
  }
  read_option.rollbacks = rollbacks_;
  leveldb::Status db_status = db_->Get(read_option, key, value);
  if (!db_status.ok()) {
    // LOG(ERROR) << "fail to read value for key: " << key.data()
    //    << " from tablet: " << tablet_path_;
    SetStatusCode(db_status, status);
    return false;
  }
  return true;
}

StatusCode TabletIO::InitScanIterator(const std::string& start_tera_key,
                                      const std::string& end_row_key,
                                      const ScanOptions& scan_options,
                                      leveldb::Iterator** scan_it) {
  leveldb::ReadOptions read_option(&ldb_options_);
  read_option.verify_checksums = FLAGS_tera_leveldb_verify_checksums;
  SetupIteratorOptions(scan_options, &read_option);
  uint64_t snapshot_id = scan_options.snapshot_id;
  if (snapshot_id != 0) {
    if (!SnapshotIDToSeq(snapshot_id, &read_option.snapshot)) {
      TearDownIteratorOptions(&read_option);
      return kSnapshotNotExist;
    }
  }

  leveldb::Slice start_key, start_col, start_qual;
  if (!kv_only_) {
    key_operator_->ExtractTeraKey(start_tera_key, &start_key, &start_col, &start_qual, NULL, NULL);
    // single row scan optimization for table
    // Readcell branch: end_row_key just check here, not used for scan
    if (start_key.ToString() + '\0' == end_row_key) {
      SetupSingleRowIteratorOptions(start_key.ToString(), &read_option);
    }
  }
  read_option.rollbacks = rollbacks_;
  // just batchscan need prefetch
  if (scan_options.is_batch_scan) {
    read_option.prefetch_scan = true;
    read_option.prefetch_scan_size = FLAGS_tera_tabletnode_prefetch_scan_size;
  }

  *scan_it = db_->NewIterator(read_option);
  TearDownIteratorOptions(&read_option);

  if ((*scan_it)->status().IsShutdownInProgress()) {
    TABLET_UNLOAD_LOG << "on waiting_for_shutdown2_ new a ErrorIterator, and "
                         "return kKeyNotInRange";
    return kKeyNotInRange;
  }

  std::string start_seek_key;
  if (kv_only_) {
    start_seek_key = start_tera_key; // start_tera_key is seek key trans by SetupScanKey()
  } else {
    key_operator_->EncodeTeraKey(start_key.ToString(), "", "", kLatestTs, leveldb::TKT_FORSEEK,
                                 &start_seek_key);
  }
  (*scan_it)->Seek(start_seek_key);

  return kTabletNodeOk;
}

bool TabletIO::LowLevelScan(const std::string& start_tera_key, const std::string& end_row_key,
                            const ScanOptions& scan_options, RowResult* values,
                            KeyValuePair* next_start_point, uint32_t* read_row_count,
                            uint32_t* read_cell_count, uint32_t* read_bytes, bool* complete,
                            StatusCode* status) {
  leveldb::Iterator* it = NULL;
  StatusCode ret_code = InitScanIterator(start_tera_key, end_row_key, scan_options, &it);
  if (ret_code != kTabletNodeOk) {
    SetStatusCode(ret_code, status);
    return false;
  }

  ScanContext* context = new ScanContext;
  context->compact_strategy = ldb_options_.compact_strategy_factory->NewInstance();
  context->version_num = 1;
  context->qu_num = 1;
  bool ret =
      LowLevelScan(start_tera_key, end_row_key, scan_options, it, context, values, next_start_point,
                   read_row_count, read_cell_count, read_bytes, complete, status);
  delete it;
  delete context->compact_strategy;
  delete context;
  return ret;
}

bool TabletIO::ScanWithFilter(const ScanOptions& scan_options) {
  return scan_options.filter != nullptr;
}

// 检测`row_buf'中的数据是否为一整行，`row_buf'为空是整行的特例，也返回true
// 从LowLevelScan的for()循环中跳出时，leveldb::Iterator* it
// 指向第一个不在row_buf中的cell
// 如果这个cell的rowkey和row_buf中的数据rowkey相同，
// 则说明`row_buf'中的数据不是一整行，返回false
// `row_buf'自身的逻辑保证了其中的所有cell必定属于同一行(row)
bool TabletIO::IsCompleteRow(const SingleRowBuffer& row_buf, leveldb::Iterator* it) {
  assert((it != NULL) && (it->Valid()));
  if (row_buf.Size() == 0) {
    VLOG(9) << "[filter] key value list empty";
    return true;
  }
  leveldb::Slice origin_cell = it->key();
  leveldb::Slice cur_cell = it->key();
  for (; it->Valid();) {
    cur_cell = it->key();
    leveldb::Slice row;
    if (!key_operator_->ExtractTeraKey(cur_cell, &row, NULL, NULL, NULL, NULL)) {
      LOG(ERROR) << "[filter] invalid tera key: " << DebugString(cur_cell.ToString());
      it->Next();
      continue;
    }
    if (cur_cell.compare(origin_cell) != 0) {
      it->Seek(origin_cell);
    }
    bool res = row.compare(row_buf.RowKey(0)) == 0;
    VLOG(9) << "[filter] " << (res ? "NOT " : "") << "complete row";
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
bool TabletIO::ShouldFilterRow(const ScanOptions& scan_options, const SingleRowBuffer& row_buf,
                               leveldb::Iterator* it) {
  assert((it != NULL) && it->Valid());
  if (row_buf.Size() == 0) {
    VLOG(9) << "[filter] row_buf empty";
    return false;
  }
  const std::string& origin_row = row_buf.RowKey(0);
  leveldb::Slice origin_cell = it->key();
  scan_options.filter->Reset();
  filter::Filter::ReturnCode rc = filter::Filter::kIncludeCurCell;

  // TODO(taocipian)
  // 0). some target cf maybe already in row_buf
  // 1). collects all target cf and sorts them,
  //     then Seek() to the 1st cf, Next() to the rest
  for (auto filter_column_it = scan_options.filter_column_set.begin();
       filter_column_it != scan_options.filter_column_set.end(); filter_column_it++) {
    // 针对用户指定了过滤条件的每一列，seek过去看看是否符合
    std::string seek_key;
    key_operator_->EncodeTeraKey(origin_row, filter_column_it->first, filter_column_it->second,
                                 kLatestTs, leveldb::TKT_FORSEEK, &seek_key);
    it->Seek(seek_key);
    rc = filter::Filter::kIncludeCurCell;
    for (; it->Valid();) {
      leveldb::Slice row, cf, qu;
      int64_t ts;
      if (!key_operator_->ExtractTeraKey(it->key(), &row, &cf, &qu, &ts, NULL)) {
        LOG(ERROR) << "[filter] invalid tera key: " << DebugString(it->key().ToString());
        it->Next();
        continue;
      }
      if (row.ToString() != origin_row) {
        // 用户试图过滤不存在的一列，忽略这个过滤条件
        break;
      }
      rc = scan_options.filter->FilterCell(cf.ToString(), qu.ToString(), it->value().ToString());
      break;
    }
    if (rc == filter::Filter::kNotIncludeCurAndLeftCellOfRow) {
      break;
    }
  }
  it->Seek(origin_cell);
  return scan_options.filter->FilterRow();
}

// seek到`row_buf'中cell所在行(row)的下一行，
// 调用者需要检查此函数返回以后迭代器的状态是否有效，
// 因为可能已经到了数据库的最后
void TabletIO::GotoNextRow(const SingleRowBuffer& row_buf, leveldb::Iterator* it,
                           KeyValuePair* next) {
  assert(it != NULL);
  if (!it->Valid() || row_buf.Size() == 0) {
    return;
  }
  std::string row = row_buf.RowKey(0);
  std::string next_row = row + '\0';
  std::string seek_key;
  key_operator_->EncodeTeraKey(next_row, "", "", kLatestTs, leveldb::TKT_FORSEEK, &seek_key);
  it->Seek(seek_key);
  MakeKvPair(leveldb::Slice(next_row), "", "", kLatestTs, "", next);
  VLOG(9) << "[filter] goto next row:" << next_row << ":" << next_row.size();
}

bool TabletIO::LowLevelScan(const std::string& start_tera_key, const std::string& end_row_key,
                            const ScanOptions& scan_options, leveldb::Iterator* it,
                            ScanContext* scan_context, RowResult* values,
                            KeyValuePair* next_start_point, uint32_t* read_row_count,
                            uint32_t* read_cell_count, uint32_t* read_bytes, bool* complete,
                            StatusCode* status) {
  leveldb::CompactStrategy* compact_strategy = scan_context->compact_strategy;
  std::string& last_key = scan_context->last_key;
  std::string& last_col = scan_context->last_col;
  std::string& last_qual = scan_context->last_qual;
  uint32_t& version_num = scan_context->version_num;
  uint64_t& qu_num = scan_context->qu_num;

  SingleRowBuffer row_buf;
  uint32_t buffer_size = 0;
  int64_t number_limit = 0;
  values->clear_key_values();
  *read_row_count = 0;
  *read_cell_count = 0;
  *read_bytes = 0;
  int64_t now_time = GetTimeStampInMs();
  int64_t time_out = now_time + scan_options.timeout;
  KeyValuePair next_start_kv_pair;
  VLOG(9) << "ll-scan timeout set to be " << scan_options.timeout << ", start_tera_key "
          << DebugString(start_tera_key) << ", end_row_key " << DebugString(end_row_key)
          << ", max_size " << scan_options.max_size << ", number_limit "
          << scan_options.number_limit << ", max_versions " << scan_options.max_versions
          << ", max_qualifiers " << scan_options.max_qualifiers;

  *complete = false;
  for (; it->Valid();) {
    bool has_merged = false;
    std::string merged_value;
    counter_.low_read_cell.Inc();
    low_level_read_count.Inc();
    *read_bytes += it->key().size() + it->value().size();
    ++*read_cell_count;
    now_time = GetTimeStampInMs();

    leveldb::Slice tera_key = it->key();
    leveldb::Slice value = it->value();
    leveldb::Slice key, col, qual;
    int64_t ts = 0;
    leveldb::TeraKeyType type;
    if (!key_operator_->ExtractTeraKey(tera_key, &key, &col, &qual, &ts, &type)) {
      LOG(WARNING) << "invalid tera key: " << DebugString(tera_key.ToString());
      it->Next();
      continue;
    }

    VLOG(10) << "ll-scan: "
             << "tablet=[" << tablet_path_ << "] key=[" << DebugString(key.ToString())
             << "] column=[" << DebugString(col.ToString()) << ":" << DebugString(qual.ToString())
             << "] ts=[" << ts << "] type=[" << type << "] buffer_size=[" << buffer_size
             << "] row_count=[" << *read_row_count << "] kv_count=[" << *read_cell_count
             << "] number_limit=[" << number_limit << "]"
             << "] read_bytes=[" << *read_bytes << "] qu_num=[" << qu_num << "]";

    if (now_time > time_out) {
      VLOG(9) << "ll-scan timeout, now_time: " << now_time << ", time_out: " << time_out;
      if (next_start_point != NULL) {
        VLOG(9) << "Mark next start key: " << DebugString(tera_key.ToString());
        MakeKvPair(key, col, qual, ts, "", next_start_point);
      }
      SetStatusCode(kRPCTimeout, status);
      break;
    }
    if (db_->IsShutdown1Finished()) {
      TABLET_UNLOAD_LOG << "break lowlevelscan before iterator next";
      SetStatusCode(kKeyNotInRange, status);
      break;
    }

    if (end_row_key.size() && key.compare(end_row_key) >= 0) {
      // scan finished
      *complete = true;
      break;
    }

    const std::set<std::string>& cf_set = scan_options.iter_cf_set;
    if (cf_set.size() > 0 && cf_set.find(col.ToString()) == cf_set.end() &&
        type != leveldb::TKT_DEL) {
      // donot need this column, skip row deleting tag
      it->Next();
      continue;
    }

    if (compact_strategy->ScanDrop(it->key(), 0)) {
      // skip drop record
      scan_drop_count.Inc();
      it->Next();
      continue;
    }

    // only use for sync scan, not available for stream scan
    if (key_operator_->Compare(it->key(), start_tera_key) < 0) {
      // skip out-of-range records
      // keep record of version info to prevent dirty data
      if (key.compare(last_key) == 0 && col.compare(last_col) == 0 &&
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
      ProcessRowBuffer(row_buf, scan_options, values, &buffer_size, &number_limit);
      row_buf.Clear();
    }

    if (key.compare(last_key) == 0 && col.compare(last_col) == 0 && qual.compare(last_qual) == 0) {
      if (++version_num > scan_options.max_versions) {
        it->Next();
        continue;
      }
    } else {
      if (key.compare(last_key) == 0 && col.compare(last_col) == 0) {
        if (++qu_num > scan_options.max_qualifiers) {
          VLOG(10) << "max_qualifiers triggered, max_qualifiers: " << scan_options.max_qualifiers;
          it->Next();
          continue;
        }
      } else {
        qu_num = 1;
      }

      last_key.assign(key.data(), key.size());
      last_col.assign(col.data(), col.size());
      last_qual.assign(qual.data(), qual.size());
      version_num = 1;
      int64_t merged_num = 0;
      has_merged = compact_strategy->ScanMergedValue(it, &merged_value, &merged_num);
      if (has_merged) {
        counter_.low_read_cell.Add(merged_num - 1);
        low_level_read_count.Add(merged_num - 1);
        value = merged_value;
        key = last_key;
        col = last_col;
        qual = last_qual;

        VLOG(10) << "ll-scan merge: "
                 << "key=[" << DebugString(key.ToString()) << "] column=["
                 << DebugString(col.ToString()) << ":" << DebugString(qual.ToString()) << "] ts=["
                 << ts << "] type=[" << type << "]"
                 << " value=[" << DebugString(value.ToString()) << "] merged=" << merged_num;
      }
    }

    row_buf.Add(key, col, qual, value, ts);

    // ScanMergedValue may have set it->Next()
    // Must make sure has_merged == false before it->Next()
    // Couldn't put this part in if (has_merged) else { it->Next() }
    if (!has_merged) {
      it->Next();
    }

    // check scan buffer
    if (buffer_size >= scan_options.max_size || number_limit >= scan_options.number_limit) {
      VLOG(10) << "stream scan, break scan context"
               << ", buffer_size " << buffer_size << ", number_limit " << number_limit << ", key "
               << DebugString(key.ToString()) << ", col " << DebugString(col.ToString())
               << ", qual " << DebugString(qual.ToString());
      break;
    }
  }
  *complete = !it->Valid() ? true : *complete;

  if (ScanWithFilter(scan_options) && it->Valid() && !IsCompleteRow(row_buf, it) &&
      ShouldFilterRow(scan_options, row_buf, it)) {
    GotoNextRow(row_buf, it, &next_start_kv_pair);
  } else {
    // process the last row of tablet
    ProcessRowBuffer(row_buf, scan_options, values, &buffer_size, &number_limit);
  }

  if (*status == kRPCTimeout || *status == kKeyNotInRange) {
    return false;
  }

  if (!it->Valid() && !(it->status().ok())) {
    SetStatusCode(it->status(), status);
    VLOG(10) << "ll-scan fail: "
             << "tablet=[" << tablet_path_ << "], "
             << "status=[" << StatusCodeToString(*status) << "]";
    return false;
  }

  SetStatusCode(kTabletNodeOk, status);
  return true;
}

void TabletIO::MakeKvPair(leveldb::Slice key, leveldb::Slice col, leveldb::Slice qual, int64_t ts,
                          leveldb::Slice value, KeyValuePair* kv) {
  kv->set_key(key.data(), key.size());
  kv->set_column_family(col.data(), col.size());
  kv->set_qualifier(qual.data(), qual.size());
  kv->set_timestamp(ts);
  kv->set_value(value.data(), value.size());
}

bool TabletIO::LowLevelSeek(const std::string& row_key, const ScanOptions& scan_options,
                            RowResult* values, StatusCode* status) {
  StatusCode s;
  SetStatusCode(kTabletNodeOk, &s);
  values->clear_key_values();

  // create tera iterator
  leveldb::ReadOptions read_option(&ldb_options_);
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
  SetupSingleRowIteratorOptions(row_key, &read_option);
  std::unique_ptr<leveldb::Iterator> it_data(db_->NewIterator(read_option));
  TearDownIteratorOptions(&read_option);
  if (it_data->status().IsShutdownInProgress()) {
    TABLET_UNLOAD_LOG << "on waiting_for_shutdown2_ new a ErrorIterator, and return early";
    SetStatusCode(kKeyNotInRange, status);
    return false;
  }

  // init compact strategy
  leveldb::CompactStrategy* compact_strategy = ldb_options_.compact_strategy_factory->NewInstance();

  // seek to the row start & process row delete mark
  std::string row_seek_key;
  key_operator_->EncodeTeraKey(row_key, "", "", kLatestTs, leveldb::TKT_FORSEEK, &row_seek_key);
  it_data->Seek(row_seek_key);
  counter_.low_read_cell.Inc();
  low_level_read_count.Inc();
  if (it_data->Valid()) {
    VLOG(10) << "ll-seek: "
             << "tablet=[" << tablet_path_ << "] row_key=[" << row_key << "]";
    leveldb::Slice cur_row_key;
    key_operator_->ExtractTeraKey(it_data->key(), &cur_row_key, NULL, NULL, NULL, NULL);
    if (cur_row_key.compare(row_key) > 0) {
      SetStatusCode(kKeyNotExist, &s);
    } else {
      compact_strategy->ScanDrop(it_data->key(), 0);
    }
  } else if (it_data->status().ok()) {
    SetStatusCode(kKeyNotExist, &s);
  } else {
    SetStatusCode(it_data->status(), &s);
  }

  if (s != kTabletNodeOk) {
    delete compact_strategy;
    SetStatusCode(s, status);
    return false;
  }

  ColumnFamilyMap::const_iterator it_cf = scan_options.column_family_list.begin();
  for (; it_cf != scan_options.column_family_list.end(); ++it_cf) {
    const std::string& cf_name = it_cf->first;
    const std::set<std::string>& qu_set = it_cf->second;

    // seek to the cf start & process cf delete mark
    std::string cf_seek_key;
    key_operator_->EncodeTeraKey(row_key, cf_name, "", kLatestTs, leveldb::TKT_FORSEEK,
                                 &cf_seek_key);
    it_data->Seek(cf_seek_key);
    counter_.low_read_cell.Inc();
    low_level_read_count.Inc();
    if (it_data->Valid()) {
      VLOG(10) << "ll-seek: "
               << "tablet=[" << tablet_path_ << "] row_key=[" << row_key << "] cf=[" << cf_name
               << "]";
      leveldb::Slice cur_row, cur_cf;
      key_operator_->ExtractTeraKey(it_data->key(), &cur_row, &cur_cf, NULL, NULL, NULL);
      if (cur_row.compare(row_key) > 0 || cur_cf.compare(cf_name) > 0) {
        continue;
      } else {
        compact_strategy->ScanDrop(it_data->key(), 0);
      }
    } else if (it_data->status().ok()) {
      VLOG(10) << "ll-seek fail, not found.";
      continue;
    } else {
      SetStatusCode(it_data->status(), status);
      return false;
    }

    if (qu_set.empty()) {
      LOG(FATAL) << "low level seek only support qualifier read.";
    }
    std::set<std::string>::iterator it_qu = qu_set.begin();
    for (; it_qu != qu_set.end(); ++it_qu) {
      const std::string& qu_name = *it_qu;
      VLOG(10) << "ll-seek: try find "
               << "tablet=[" << tablet_path_ << "] row_key=[" << row_key << "] cf=[" << cf_name
               << "] qu=[" << qu_name << "]";

      // seek to the cf start & process cf delete mark
      std::string qu_seek_key;
      key_operator_->EncodeTeraKey(row_key, cf_name, qu_name, kLatestTs, leveldb::TKT_FORSEEK,
                                   &qu_seek_key);
      it_data->Seek(qu_seek_key);
      uint32_t version_num = 0;
      for (; it_data->Valid();) {
        if (db_->IsShutdown1Finished()) {
          // break early on waiting_for_shutdown2_
          // igrone haven't scan versions of this qualifier
          TABLET_UNLOAD_LOG << "break lowlevelscan before iterator next";
          SetStatusCode(kKeyNotInRange, &s);
          break;
        }
        counter_.low_read_cell.Inc();
        low_level_read_count.Inc();
        VLOG(10) << "ll-seek: "
                 << "tablet=[" << tablet_path_ << "] row_key=[" << row_key << "] cf=[" << cf_name
                 << "] qu=[" << qu_name << "]";
        leveldb::Slice cur_row, cur_cf, cur_qu;
        int64_t timestamp;
        key_operator_->ExtractTeraKey(it_data->key(), &cur_row, &cur_cf, &cur_qu, &timestamp, NULL);
        if (cur_row.compare(row_key) > 0 || cur_cf.compare(cf_name) > 0 ||
            cur_qu.compare(qu_name) > 0) {
          break;
        }

        // skip qu delete mark and out-of-range version
        if (compact_strategy->ScanDrop(it_data->key(), 0)) {
          VLOG(10) << "ll-seek: scan drop "
                   << "tablet=[" << tablet_path_ << "] row_key=[" << row_key << "] cf=[" << cf_name
                   << "] qu=[" << qu_name << "]";
          scan_drop_count.Inc();
          // skip to next qualifier
          break;
        }

        if (scan_options.ts_start > timestamp) {
          break;
        }
        if (scan_options.ts_end < timestamp) {
          it_data->Next();
          continue;
        }

        // version filter
        if (++version_num > scan_options.max_versions) {
          break;
        }

        KeyValuePair* kv = values->add_key_values();
        kv->set_key(row_key);
        kv->set_column_family(cf_name);
        kv->set_qualifier(qu_name);
        kv->set_timestamp(timestamp);

        int64_t merged_num;
        std::string merged_value;
        bool has_merged =
            compact_strategy->ScanMergedValue(it_data.get(), &merged_value, &merged_num);
        if (has_merged) {
          counter_.low_read_cell.Add(merged_num - 1);
          low_level_read_count.Add(merged_num - 1);
          kv->set_value(merged_value);
          VLOG(10) << "ll-seek merge: "
                   << "key=[" << DebugString(row_key) << "] column=[" << DebugString(cf_name) << ":"
                   << DebugString(qu_name) << "] ts=[" << timestamp << "] "
                   << " value_v=[" << io::DecodeBigEndain(merged_value.data()) << "] "
                   << " value=[" << DebugString(merged_value) << "] merged=" << merged_num;
        } else {
          leveldb::Slice value = it_data->value();
          kv->set_value(value.data(), value.size());
          it_data->Next();
        }
      }
      if (!it_data->status().ok()) {
        SetStatusCode(it_data->status(), status);
        return false;
      }
      if (s == kKeyNotInRange) {
        // only on waiting_for_shutdown2_ != NULL will break with kKeyNotInRange
        // igrone haven't scan qualifiers
        break;
      }
    }
    if (s == kKeyNotInRange) {
      // only on waiting_for_shutdown2_ != NULL will break with kKeyNotInRange
      // igrone haven't scan column_families
      break;
    }
  }
  delete compact_strategy;

  SetStatusCode(s, status);
  return kTabletNodeOk == s;
}

bool TabletIO::ReadCells(const RowReaderInfo& row_reader, RowResult* values, uint64_t snapshot_id,
                         StatusCode* status, int64_t timeout_ms) {
  {
    MutexLock lock(&mutex_);
    if ((status_ != kReady && status_ != kUnloading) || IsUrgentUnload()) {
      if (status_ == kUnloading2) {
        // keep compatable for old sdk protocol
        // we can remove this in the future.
        SetStatusCode(kUnloading, status);
      } else {
        SetStatusCode(status_, status);
      }
      return false;
    }
    db_ref_count_++;
  }

  int64_t start_read_us = get_micros();

  if (kv_only_) {
    std::string key(row_reader.key());
    std::string value;
    if (RawKeyType() == TTLKv) {
      key.append(8, '\0');
    }
    if (!Read(key, &value, snapshot_id, status)) {
      counter_.read_rows.Inc();
      row_read_count.Inc();
      row_read_delay.Add(get_micros() - start_read_us);
      {
        MutexLock lock(&mutex_);
        db_ref_count_--;
      }
      return false;
    }
    KeyValuePair* result = values->add_key_values();
    result->set_key(row_reader.key());
    result->set_value(value);
    counter_.read_rows.Inc();
    row_read_count.Inc();
    counter_.read_size.Add(result->ByteSize());
    row_read_bytes.Add(result->ByteSize());
    row_read_delay.Add(get_micros() - start_read_us);
    {
      MutexLock lock(&mutex_);
      db_ref_count_--;
    }
    return true;
  }

  ScanOptions scan_options;
  scan_options.enable_dfs_read_thread_limiter = FLAGS_enable_dfs_read_thread_limiter;
  bool ll_seek_available = true;
  for (int32_t i = 0; i < row_reader.cf_list_size(); ++i) {
    const ColumnFamily& column_family = row_reader.cf_list(i);
    const std::string& column_family_name = column_family.family_name();
    std::set<std::string>& qualifier_list = scan_options.column_family_list[column_family_name];
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

  if (row_reader.has_max_qualifiers()) {
    scan_options.max_qualifiers = row_reader.max_qualifiers();
  } else {
    scan_options.max_qualifiers = std::numeric_limits<uint64_t>::max();
  }

  if (row_reader.has_time_range()) {
    scan_options.ts_start = row_reader.time_range().ts_start();
    scan_options.ts_end = row_reader.time_range().ts_end();
    VLOG(10) << "ReadCells: "
             << "timerange=[" << scan_options.ts_start << "," << scan_options.ts_end << "]";
  }

  scan_options.snapshot_id = snapshot_id;
  scan_options.timeout = timeout_ms;

  VLOG(10) << "ReadCells: "
           << "key=[" << DebugString(row_reader.key()) << "]";

  bool ret = false;
  // if read all columns, use LowLevelScan
  if (ll_seek_available) {
    ret = LowLevelSeek(row_reader.key(), scan_options, values, status);
  } else {
    std::string start_tera_key;
    key_operator_->EncodeTeraKey(row_reader.key(), "", "", kLatestTs, leveldb::TKT_VALUE,
                                 &start_tera_key);
    std::string end_row_key = row_reader.key() + '\0';
    uint32_t read_row_count = 0;
    uint32_t read_cell_count = 0;
    uint32_t read_bytes = 0;
    bool complete = false;
    ret = LowLevelScan(start_tera_key, end_row_key, scan_options, values, NULL, &read_row_count,
                       &read_cell_count, &read_bytes, &complete, status);
  }
  counter_.read_rows.Inc();
  row_read_count.Inc();
  row_read_delay.Add(get_micros() - start_read_us);
  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
  if (!ret) {
    return false;
  } else {
    counter_.read_size.Add(values->ByteSize());
    row_read_bytes.Add(values->ByteSize());
  }

  if (values->key_values_size() == 0) {
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

  CHECK_NOTNULL(db_);

  leveldb::Status db_status = db_->Write(options, batch);
  if (!db_status.ok()) {
    LOG(ERROR) << "fail to batch write to tablet: " << tablet_path_ << ", " << db_status.ToString();
    SetStatusCode(kIOError, status);
    return false;
  }
  counter_.write_size.Add(batch->DataSize());
  row_write_bytes.Add(batch->DataSize());
  SetStatusCode(kTabletNodeOk, status);
  return true;
}

bool TabletIO::WriteOne(const std::string& key, const std::string& value, bool sync,
                        StatusCode* status) {
  leveldb::WriteBatch batch;
  batch.Put(key, value);
  return WriteBatch(&batch, false, sync, status);
}

bool TabletIO::Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
                     std::vector<StatusCode>* status_vec, bool is_instant, WriteCallback callback,
                     StatusCode* status) {
  {
    MutexLock lock(&mutex_);
    if ((status_ != kReady && status_ != kUnloading) || IsUrgentUnload()) {
      if (status_ == kUnloading2) {
        // keep compatable for old sdk protocol
        // we can remove this in the future.
        SetStatusCode(kUnloading, status);
      } else {
        SetStatusCode(status_, status);
      }
      return false;
    }
    db_ref_count_++;
  }
  bool ret = async_writer_->Write(row_mutation_vec, status_vec, is_instant, callback, status);
  if (!ret) {
    counter_.write_reject_rows.Add(row_mutation_vec->size());
  }

  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
  return ret;
}

bool TabletIO::ScanRows(const ScanTabletRequest* request, ScanTabletResponse* response,
                        google::protobuf::Closure* done) {
  StatusCode status = kTabletNodeOk;
  {
    MutexLock lock(&mutex_);
    if ((status_ != kReady && status_ != kUnloading) || IsUrgentUnload()) {
      if (status_ == kUnloading2) {
        // keep compatable for old sdk protocol
        // we can remove this in the future.
        SetStatusCode(kUnloading, &status);
      } else {
        SetStatusCode(status_, &status);
      }
      response->set_status(status);
      done->Run();
      return false;
    }
    db_ref_count_++;
  }

  bool success = false;
  // slide window of batchscan use unique rpc session
  // so, has_session_id means batchscan
  if (kv_only_ && !request->has_session_id()) {
    success = ScanKvsRestricted(request, response, done);
  } else if (request->has_session_id() && request->session_id() > 0) {
    batch_scan_count.Inc();
    success = HandleScan(request, response, done);
  } else {
    sync_scan_count.Inc();
    success = ScanRowsRestricted(request, response, done);
  }
  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
  return success;
}

bool TabletIO::ScanKvsRestricted(const ScanTabletRequest* request, ScanTabletResponse* response,
                                 google::protobuf::Closure* done) {
  bool ret = false;
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
  uint32_t read_row_count = 0;
  uint32_t read_bytes = 0;
  int64_t start_scan_us = get_micros();

  StatusCode status = kTabletNodeOk;
  if (Scan(scan_option, response->mutable_results()->mutable_key_values(), &read_row_count,
           &read_bytes, &complete, &status)) {
    response->set_complete(complete);
    ret = true;
  }

  counter_.scan_rows.Add(read_row_count);
  counter_.scan_kvs.Add(read_row_count);
  counter_.scan_size.Add(read_bytes);
  row_scan_count.Add(read_row_count);
  row_scan_bytes.Add(read_bytes);
  row_scan_delay.Add(get_micros() - start_scan_us);

  response->set_data_size(read_bytes);
  response->set_row_count(read_row_count);
  response->set_cell_count(read_row_count);

  response->set_status(status);
  done->Run();
  return ret;
}

bool TabletIO::ScanRowsRestricted(const ScanTabletRequest* request, ScanTabletResponse* response,
                                  google::protobuf::Closure* done) {
  std::string start_tera_key;
  std::string end_row_key;
  SetupScanKey(request, &start_tera_key, &end_row_key);

  ScanOptions scan_options;
  SetupScanRowOptions(request, &scan_options);

  uint32_t read_row_count = 0;
  uint32_t read_cell_count = 0;
  uint32_t read_bytes = 0;
  bool complete = false;

  StatusCode status = kTabletNodeOk;
  bool ret = false;

  int64_t start_scan_us = get_micros();

  if (LowLevelScan(start_tera_key, end_row_key, scan_options, response->mutable_results(),
                   response->mutable_next_start_point(), &read_row_count, &read_cell_count,
                   &read_bytes, &complete, &status)) {
    response->set_complete(complete);
    ret = true;
  }

  counter_.scan_rows.Add(read_row_count);
  counter_.scan_kvs.Add(read_cell_count);
  counter_.scan_size.Add(read_bytes);
  row_scan_count.Add(read_row_count);
  row_scan_bytes.Add(read_bytes);
  row_scan_delay.Add(get_micros() - start_scan_us);

  response->set_data_size(read_bytes);
  response->set_row_count(read_row_count);
  response->set_cell_count(read_cell_count);

  response->set_status(status);
  done->Run();
  return ret;
}

bool TabletIO::HandleScan(const ScanTabletRequest* request, ScanTabletResponse* response,
                          google::protobuf::Closure* done) {
  // concurrency control, ensure only one scanner step init leveldb::Iterator
  ScanContext* context = scan_context_manager_->GetScanContext(this, request, response, done);
  if (context == NULL) {
    return true;
  }

  // first rpc init iterator and scan parameter
  if (context->it == NULL) {
    SetupScanRowOptions(request, &(context->scan_options));
    context->scan_options.is_batch_scan = true;
    // context->complete set false in GetScanContext()
    SetupScanKey(request, &(context->start_tera_key), &(context->end_row_key));
    context->ret_code = InitScanIterator(context->start_tera_key, context->end_row_key,
                                         context->scan_options, &(context->it));
    if (!kv_only_ || RawKeyType() == TTLKv) {
      context->compact_strategy = ldb_options_.compact_strategy_factory->NewInstance();
    }
  }
  // schedule scan context
  return scan_context_manager_->ScheduleScanContext(context);
}

void TabletIO::ProcessScan(ScanContext* context) {
  uint32_t rows_scan_num = 0;
  uint32_t cells_scan_num = 0;
  uint32_t size_scan_bytes = 0;
  int64_t start_scan_us = get_micros();

  if (kv_only_) {
    KvTableScan(context, &rows_scan_num, &size_scan_bytes);
    context->data_size = size_scan_bytes;
    context->cell_count = rows_scan_num;  // if kv table, cell_count equal row_count
    context->row_count = rows_scan_num;
    counter_.scan_kvs.Add(rows_scan_num);
  } else {
    LowLevelScan(context->start_tera_key, context->end_row_key, context->scan_options, context->it,
                 context, context->result, NULL, &rows_scan_num, &cells_scan_num, &size_scan_bytes,
                 &context->complete, &context->ret_code);
    context->data_size = size_scan_bytes;
    context->cell_count = cells_scan_num;
    context->row_count = rows_scan_num;
    counter_.scan_kvs.Add(cells_scan_num);
  }
  counter_.scan_rows.Add(rows_scan_num);
  counter_.scan_size.Add(size_scan_bytes);
  row_scan_count.Add(rows_scan_num);
  row_scan_bytes.Add(size_scan_bytes);
  row_scan_delay.Add(get_micros() - start_scan_us);
}

bool TabletIO::KvTableScan(ScanContext* scan_context, uint32_t* read_row_count,
                           uint32_t* read_bytes) {
  std::string& start = scan_context->start_tera_key;
  std::string& end = scan_context->end_row_key;
  ScanOptions& scan_options = scan_context->scan_options;
  bool& complete = scan_context->complete;
  RowResult* values = scan_context->result;
  StatusCode* status = &scan_context->ret_code;
  leveldb::CompactStrategy* compact_strategy = scan_context->compact_strategy;

  values->clear_key_values();
  *read_row_count = 0;
  *read_bytes = 0;
  int64_t now_time = GetTimeStampInMs();
  int64_t time_out = now_time + scan_options.timeout;
  VLOG(9) << "kv-scan timeout " << scan_options.timeout << ", max_size " << scan_options.max_size
          << ", number_limit " << scan_options.number_limit << ", start_key " << DebugString(start)
          << ", end_key " << DebugString(end);

  auto it = scan_context->it;
  for (; it->Valid(); it->Next()) {
    leveldb::Slice key = it->key();
    leveldb::Slice value = it->value();
    ++*read_row_count;

    VLOG(10) << "kv-scan: tablet=[" << tablet_path_ << "] key=[" << DebugString(key.ToString())
             << "] scan_row_count=[" << *read_row_count << "]"
             << " read_bytes=[" << *read_bytes << "]";

    if (RawKeyType() == TTLKv) {
      complete = (!end.empty() && key_operator_->Compare(key, end) >= 0);
    } else {
      complete = (!end.empty() && key.compare(end) >= 0);
    }

    now_time = GetTimeStampInMs();
    // 4 conditions: complete, timeout, max size, max row number
    if (complete || now_time >= time_out ||
        (scan_options.max_size > 0 && *read_bytes >= scan_options.max_size) ||
        *read_row_count > scan_options.number_limit) {
      break;
    }

    if (compact_strategy && compact_strategy->ScanDrop(key, 0)) {
      VLOG(10) << "[KV-Scan] key:[" << key.ToString() << "] Dropped.";
    } else {
      KeyValuePair* pair = values->add_key_values();
      if (RawKeyType() == TTLKv) {
        pair->set_key(key.data(), key.size() - sizeof(int64_t));
      } else {
        pair->set_key(key.data(), key.size());
      }
      pair->set_value(value.data(), value.size());
      *read_bytes += pair->key().size() + pair->value().size();
    }

    if (db_->IsShutdown1Finished()) {
      // return early on waiting_for_shutdown2_, igrone rows haven't scan
      TABLET_UNLOAD_LOG << "break scan kv table before iterator next";
      *status = kKeyNotInRange;
      return false;
    }
  }

  if (!it->Valid()) {
    complete = true;
  }
  if (!it->Valid() && !(it->status().ok())) {
    SetStatusCode(it->status(), status);
    VLOG(10) << "kv-scan fail: "
             << "tablet=[" << tablet_path_ << "], "
             << "status=[" << StatusCodeToString(*status) << "]";
    return false;
  }
  SetStatusCode(kTabletNodeOk, status);
  return true;
}

bool TabletIO::Scan(const ScanOption& option, KeyValueList* kv_list, uint32_t* read_row_count,
                    uint32_t* read_bytes, bool* complete, StatusCode* status) {
  std::string start = option.key_range().key_start();
  std::string end = option.key_range().key_end();
  if (start < start_key_) {
    start = start_key_;
  }
  if (end.empty() || (!end_key_.empty() && end > end_key_)) {
    end = end_key_;
  }

  // TTL-KV : key_operator_::Compare会解RawKey([row_key | expire_timestamp])
  // 因此传递给Leveldb的Key一定要保证以expire_timestamp结尾.
  std::unique_ptr<leveldb::CompactStrategy> strategy(nullptr);
  if (RawKeyType() == TTLKv) {
    if (!start.empty()) {
      std::string start_key;
      key_operator_->EncodeTeraKey(start, "", "", 0, leveldb::TKT_FORSEEK, &start_key);
      start.swap(start_key);
    }
    if (!end.empty()) {
      std::string end_key;
      key_operator_->EncodeTeraKey(end, "", "", 0, leveldb::TKT_FORSEEK, &end_key);
      end.swap(end_key);
    }
    strategy.reset(ldb_options_.compact_strategy_factory->NewInstance());
  }

  *read_row_count = 0;
  *read_bytes = 0;

  uint64_t snapshot_id = option.snapshot_id();
  leveldb::ReadOptions read_option(&ldb_options_);
  read_option.verify_checksums = FLAGS_tera_leveldb_verify_checksums;
  if (snapshot_id != 0 && !SnapshotIDToSeq(snapshot_id, &read_option.snapshot)) {
    *status = kSnapshotNotExist;
    return false;
  }
  read_option.rollbacks = rollbacks_;

  std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(read_option));
  if (it->status().IsShutdownInProgress()) {
    TABLET_UNLOAD_LOG << "on waiting_for_shutdown2_ new a ErrorIterator, and "
                         "return kKeyNotInRange";
    *status = kKeyNotInRange;
    return false;
  }

  it->Seek(start);
  // round down is just for internal meta scan
  if (option.round_down()) {
    if (it->Valid() && key_operator_->Compare(it->key(), start) > 0) {
      it->Prev();
      if (!it->Valid()) {
        it->SeekToFirst();
      }
    } else if (!it->Valid()) {
      it->SeekToLast();
    }
  }

  int64_t pack_size = 0;
  for (; it->Valid(); it->Next()) {
    leveldb::Slice key = it->key();
    leveldb::Slice value = it->value();
    *read_bytes += it->key().size() + it->value().size();
    *read_row_count += 1;
    if (RawKeyType() == TTLKv) {  // only compare row key
      *complete = (!end.empty() && key_operator_->Compare(key, end) >= 0);
    } else {
      *complete = (!end.empty() && key.compare(end) >= 0);
    }

    if (*complete || (option.size_limit() > 0 && pack_size > option.size_limit())) {
      break;
    }

    if (strategy && strategy->ScanDrop(key, 0)) {
      VLOG(10) << "[KV-Scan] key:[" << key.ToString() << "] Dropped.";
    } else {
      KeyValuePair* pair = kv_list->Add();
      if (RawKeyType() == TTLKv) {
        pair->set_key(key.data(), key.size() - sizeof(int64_t));
      } else {
        pair->set_key(key.data(), key.size());
      }
      pair->set_value(value.data(), value.size());
      pack_size += pair->key().size() + pair->value().size();
    }

    if (db_->IsShutdown1Finished()) {
      // return early on waiting_for_shutdown2_, igrone rows haven't scan
      TABLET_UNLOAD_LOG << "break scan kv before iterator next";
      *status = kKeyNotInRange;
      return false;
    }
  }
  if (!it->Valid()) {
    *complete = true;
  }

  return true;
}

void TabletIO::SetupScanKey(const ScanTabletRequest* request, std::string* start_tera_key,
                            std::string* end_row_key) {
  std::string start_key = request->start();
  if (start_key.empty() || start_key < start_key_) {
    start_key = start_key_;
  }
  *end_row_key = request->end();
  if (end_row_key->empty() || (!end_key_.empty() && *end_row_key > end_key_)) {
    *end_row_key = end_key_;
  }

  if (kv_only_) {
    // TTL-KV : key_operator_::Compare会解RawKey([row_key | expire_timestamp])
    // 因此传递给Leveldb的Key一定要保证以expire_timestamp结尾.
    if (RawKeyType() == TTLKv) {
      if (!start_key.empty()) {
        std::string start_ttlkv_key;
        key_operator_->EncodeTeraKey(start_key, "", "", 0, leveldb::TKT_FORSEEK, &start_ttlkv_key);
        start_key.swap(start_ttlkv_key);
      }
      if (!end_row_key->empty()) {
        std::string end_ttlkv_key;
        key_operator_->EncodeTeraKey(*end_row_key, "", "", 0, leveldb::TKT_FORSEEK, &end_ttlkv_key);
        end_row_key->swap(end_ttlkv_key);
      }
    }
    *start_tera_key = start_key;
  } else {
    bool has_cf = request->has_start_family();
    bool has_qualifier = (has_cf && request->has_start_qualifier());
    // bool has_ts = (has_qualifier && request->has_start_timestamp());
    bool has_ts = request->has_start_timestamp();
    key_operator_->EncodeTeraKey(start_key, has_cf ? request->start_family() : "",
                                 has_qualifier ? request->start_qualifier() : "",
                                 has_ts ? request->start_timestamp() : kLatestTs,
                                 leveldb::TKT_VALUE, start_tera_key);
  }
}

void TabletIO::AddFilterCfs(filter::ColumnSet& filter_column_set, std::set<std::string>* cf_set) {
  for (auto& filter_column : filter_column_set) {
    cf_set->insert(filter_column.first);
  }
}

bool TabletIO::IsValidOldFilter(const Filter& old_filter_desc) {
  if (!old_filter_desc.has_type() || old_filter_desc.type() != BinComp) {
    LOG(ERROR) << "only support compare.";
    return false;
  }
  if (!old_filter_desc.has_field() || old_filter_desc.field() != ValueFilter) {
    LOG(ERROR) << "only support value-compare.";
    return false;
  }
  if (!old_filter_desc.has_value_type()) {
    LOG(ERROR) << "only support int64 value.";
    return false;
  }
  if (!old_filter_desc.has_content()) {
    return false;
  }
  if (!old_filter_desc.has_ref_value()) {
    return false;
  }
  if (!old_filter_desc.has_bin_comp_op()) {
    return false;
  }
  return true;
}

bool TabletIO::TransFilter(const FilterList& old_filter_list_desc, ScanOptions* scan_options) {
  filter::FilterListPtr new_filter_list =
      std::make_shared<filter::FilterList>(filter::FilterList::kAnd);
  for (int i = 0; i < old_filter_list_desc.filter_size(); ++i) {
    const Filter& old_filter_desc = old_filter_list_desc.filter(i);
    if (!IsValidOldFilter(old_filter_desc)) {
      return false;
    }
    int64_t ref_value = *(int64_t*)(const_cast<char*>(old_filter_desc.ref_value().c_str()));
    filter::IntegerComparatorPtr comparator =
        std::make_shared<filter::IntegerComparator>(filter::IntegerValueType::kInt64, ref_value);
    filter::CompareOperator op;
    if (!TransBinCompOp(old_filter_desc.bin_comp_op(), &op)) {
      return false;
    }
    filter::ValueFilterPtr value_filter = std::make_shared<filter::ValueFilter>(op, comparator);
    value_filter->SetColumnFamily(old_filter_desc.content());
    new_filter_list->AddFilter(value_filter);
  }
  scan_options->filter = new_filter_list;
  return true;
}

bool TabletIO::SetupFilter(const filter::FilterDesc& filter_desc, ScanOptions* scan_options) {
  filter::FilterDesc::FilterType filter_type = filter_desc.type();
  switch (filter_type) {
    case filter::FilterDesc::kFilterList:
      scan_options->filter = std::make_shared<filter::FilterList>();
      break;
    case filter::FilterDesc::kValueFilter:
      scan_options->filter = std::make_shared<filter::ValueFilter>();
      break;
    default:
      scan_options->filter.reset();
      LOG(WARNING) << "not support filter type";
      break;
  }
  if (scan_options->filter) {
    int ret = scan_options->filter->ParseFrom(filter_desc.serialized_filter());
    if (ret) {
      return true;
    } else {
      LOG(WARNING) << "parse filter failed";
      scan_options->filter.reset();
    }
  }
  return false;
}

void TabletIO::SetupScanRowOptions(const ScanTabletRequest* request, ScanOptions* scan_options) {
  scan_options->max_size = 65536;
  for (int32_t i = 0; i < request->cf_list_size(); ++i) {
    const ColumnFamily& column_family = request->cf_list(i);
    const std::string& column_family_name = column_family.family_name();
    std::set<std::string>& qualifier_list = scan_options->column_family_list[column_family_name];
    qualifier_list.clear();
    for (int32_t j = 0; j < column_family.qualifier_list_size(); ++j) {
      qualifier_list.insert(column_family.qualifier_list(j));
    }
    scan_options->iter_cf_set.insert(column_family_name);
  }

  int ret = 0;
  if (request->has_filter_list() && request->filter_list().filter_size() > 0) {
    ret = TransFilter(request->filter_list(), scan_options);
  } else if (request->has_filter()) {
    ret = SetupFilter(request->filter(), scan_options);
  }
  if (scan_options->iter_cf_set.size() > 0 && ret) {
    scan_options->filter->GetAllColumn(&scan_options->filter_column_set);
    AddFilterCfs(scan_options->filter_column_set, &scan_options->iter_cf_set);
  }
  if (request->has_max_version()) {
    scan_options->max_versions = request->max_version();
  }
  if (request->has_max_qualifiers()) {
    scan_options->max_qualifiers = request->max_qualifiers();
  } else {
    scan_options->max_qualifiers = std::numeric_limits<uint64_t>::max();
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
  scan_options->snapshot_id = request->snapshot_id();
}

// no concurrent, so no lock on schema_mutex_
void TabletIO::SetupOptionsForLG(const std::set<std::string>& ignore_err_lgs) {
  if (kv_only_) {
    if (RawKeyType() == TTLKv) {
      ldb_options_.compact_strategy_factory = new KvCompactStrategyFactory(table_schema_);
    } else {
      ldb_options_.compact_strategy_factory = new leveldb::DummyCompactStrategyFactory();
    }
  } else if (FLAGS_tera_leveldb_compact_strategy == "default") {
    // default strategy
    ldb_options_.compact_strategy_factory = new DefaultCompactStrategyFactory(table_schema_);
  } else {
    ldb_options_.compact_strategy_factory = new leveldb::DummyCompactStrategyFactory();
  }

  std::set<uint32_t>* exist_lg_list = new std::set<uint32_t>;
  std::map<uint32_t, leveldb::LG_info*>* lg_info_list = new std::map<uint32_t, leveldb::LG_info*>;
  std::set<uint32_t> ignore_corruption_in_open_lg_list;

  int64_t triggered_log_size = 0;
  for (int32_t lg_i = 0; lg_i < table_schema_.locality_groups_size(); ++lg_i) {
    if (table_schema_.locality_groups(lg_i).is_del()) {
      continue;
    }
    const LocalityGroupSchema& lg_schema = table_schema_.locality_groups(lg_i);
    bool compress = lg_schema.compress_type();
    StoreMedium store = lg_schema.store_type();

    leveldb::LG_info* lg_info = new leveldb::LG_info(lg_schema.id());
    lg_info->memtable_shard_num = ldb_options_.memtable_shard_num;

    if (mock_env_ != NULL) {
      // for testing
      LOG(INFO) << "mock env used";
      lg_info->env = LeveldbMockEnv();
    } else if (store == MemoryStore) {
      if (FLAGS_tera_use_flash_for_memenv) {
        if (!GetCachePaths().empty()) {
          if (FLAGS_tera_enable_persistent_cache) {
            lg_info->env = LeveldbBaseEnv();
            auto s = GetPersistentCache(&lg_info->persistent_cache);
            assert(s.ok());
          } else {
            lg_info->env = LeveldbFlashEnv();
            if (!lg_info->env) {
              lg_info->env = LeveldbBaseEnv();
            }
          }
        }
      } else {
        lg_info->env = LeveldbMemEnv();
      }
      lg_info->seek_latency = 0;
      lg_info->block_cache = m_memory_cache;
    } else if (store == FlashStore && !GetCachePaths().empty()) {
      if (FLAGS_tera_enable_persistent_cache) {
        lg_info->env = LeveldbBaseEnv();
        auto s = GetPersistentCache(&lg_info->persistent_cache);
        assert(s.ok());
      } else {
        lg_info->env = LeveldbFlashEnv();
        if (!lg_info->env) {
          lg_info->env = LeveldbBaseEnv();
        } else {
          lg_info->use_direct_io_read = FLAGS_tera_leveldb_use_direct_io_read;
          lg_info->use_direct_io_write = FLAGS_tera_leveldb_use_direct_io_write;
          lg_info->posix_write_buffer_size = FLAGS_tera_leveldb_posix_write_buffer_size;
        }
      }
      if (lg_info->persistent_cache || lg_info->env == LeveldbFlashEnv()) {
        lg_info->seek_latency = FLAGS_tera_leveldb_env_local_seek_latency;
      }
    } else {
      lg_info->env = LeveldbBaseEnv();
      lg_info->seek_latency = FLAGS_tera_leveldb_env_dfs_seek_latency;
    }

    if (compress) {
      lg_info->compression = leveldb::kSnappyCompression;
    }

    lg_info->block_size = lg_schema.block_size() * 1024;
    if (lg_schema.use_memtable_on_leveldb()) {
      lg_info->use_memtable_on_leveldb = true;
      lg_info->memtable_ldb_write_buffer_size = lg_schema.memtable_ldb_write_buffer_size() * 1024;
      lg_info->memtable_ldb_block_size = lg_schema.memtable_ldb_block_size() * 1024;
      LOG(INFO) << "enable mem-ldb for LG:" << lg_schema.name().c_str()
                << ", buffer_size:" << lg_info->memtable_ldb_write_buffer_size
                << ", block_size:" << lg_info->memtable_ldb_block_size;
    }
    lg_info->sst_size = lg_schema.sst_size();
    // FLAGS_tera_tablet_write_buffer_size is the max buffer size
    int64_t max_size = FLAGS_tera_tablet_max_write_buffer_size * 1024 * 1024;
    if (lg_schema.sst_size() * 4 < max_size) {
      lg_info->write_buffer_size = lg_schema.sst_size() * 4;
    } else {
      lg_info->write_buffer_size = max_size;
    }
    triggered_log_size += lg_info->write_buffer_size;
    lg_info->table_builder_batch_write = (FLAGS_tera_leveldb_table_builder_write_batch_size > 0);
    lg_info->table_builder_batch_size = FLAGS_tera_leveldb_table_builder_write_batch_size;
    exist_lg_list->insert(lg_i);
    (*lg_info_list)[lg_i] = lg_info;
    if (ignore_err_lgs.find(lg_schema.name()) != ignore_err_lgs.end()) {
      ignore_corruption_in_open_lg_list.insert(lg_i);
    }
  }
  if (mock_env_ != NULL) {
    ldb_options_.env = LeveldbMockEnv();
  } else {
    ldb_options_.env = LeveldbBaseEnv();
  }

  if (exist_lg_list->size() == 0) {
    delete exist_lg_list;
  } else {
    ldb_options_.exist_lg_list = exist_lg_list;
    ldb_options_.flush_triggered_log_size = triggered_log_size * 2;
  }
  if (lg_info_list->size() == 0) {
    delete lg_info_list;
  } else {
    ldb_options_.lg_info_list = lg_info_list;
    ldb_options_.ignore_corruption_in_open_lg_list = ignore_corruption_in_open_lg_list;
  }

  IndexingCfToLG();
}

void TabletIO::TearDownOptionsForLG() {
  if (ldb_options_.compact_strategy_factory) {
    delete ldb_options_.compact_strategy_factory;
    ldb_options_.compact_strategy_factory = NULL;
  }

  if (ldb_options_.exist_lg_list) {
    ldb_options_.exist_lg_list->clear();
    delete ldb_options_.exist_lg_list;
    ldb_options_.exist_lg_list = NULL;
  }

  if (ldb_options_.lg_info_list) {
    std::map<uint32_t, leveldb::LG_info*>::iterator it = ldb_options_.lg_info_list->begin();
    for (; it != ldb_options_.lg_info_list->end(); ++it) {
      delete it->second;
    }
    delete ldb_options_.lg_info_list;
    ldb_options_.lg_info_list = NULL;
  }
}

void TabletIO::IndexingCfToLG() {
  for (int32_t i = 0; i < table_schema_.locality_groups_size(); ++i) {
    const LocalityGroupSchema& lg_schema = table_schema_.locality_groups(i);
    lg_id_map_[lg_schema.name()] = i;  // lg_schema.id();
  }
  for (int32_t i = 0; i < table_schema_.column_families_size(); ++i) {
    const ColumnFamilySchema& cf_schema = table_schema_.column_families(i);

    std::map<std::string, uint32_t>::iterator it = lg_id_map_.find(cf_schema.locality_group());
    if (it == lg_id_map_.end()) {
      // using default lg for not-defined descor
      cf_lg_map_[cf_schema.name()] = 0;
    } else {
      cf_lg_map_[cf_schema.name()] = it->second;
    }
  }
}

void TabletIO::SetupIteratorOptions(const ScanOptions& scan_options,
                                    leveldb::ReadOptions* leveldb_opts) {
  MutexLock lock(&schema_mutex_);
  std::set<uint32_t> target_lgs;
  std::set<std::string>::const_iterator cf_it = scan_options.iter_cf_set.begin();
  for (; cf_it != scan_options.iter_cf_set.end(); ++cf_it) {
    std::map<std::string, uint32_t>::iterator map_it = cf_lg_map_.find(*cf_it);
    if (map_it != cf_lg_map_.end()) {
      target_lgs.insert(map_it->second);
    }
  }
  if (target_lgs.size() > 0) {
    leveldb_opts->target_lgs = new std::set<uint32_t>(target_lgs);
  }
  leveldb_opts->enable_dfs_read_thread_limiter = scan_options.enable_dfs_read_thread_limiter;
}

void TabletIO::SetupSingleRowIteratorOptions(const std::string& row_key,
                                             leveldb::ReadOptions* opts) {
  std::string row_start_key, row_end_key;
  key_operator_->EncodeTeraKey(row_key, "", "", kLatestTs, leveldb::TKT_FORSEEK, &row_start_key);

  if (RawKeyType() == Readable) {
    key_operator_->EncodeTeraKey(row_key + '\1', "", "", kLatestTs, leveldb::TKT_FORSEEK,
                                 &row_end_key);
  } else {
    key_operator_->EncodeTeraKey(row_key + '\0', "", "", kLatestTs, leveldb::TKT_FORSEEK,
                                 &row_end_key);
  }

  opts->read_single_row = true;
  opts->row_start_key = row_start_key;
  opts->row_end_key = row_end_key;
}

void TabletIO::TearDownIteratorOptions(leveldb::ReadOptions* opts) {
  if (opts->target_lgs) {
    delete opts->target_lgs;
    opts->target_lgs = NULL;
  }
}

bool TabletIO::ShouldFilterRowBuffer(const SingleRowBuffer& row_buf,
                                     const ScanOptions& scan_options) {
  if (row_buf.Size() <= 0) {
    return true;
  }
  if (!scan_options.filter) {
    return false;
  }
  scan_options.filter->Reset();
  for (size_t kv_index = 0; kv_index != row_buf.Size(); ++kv_index) {
    filter::Filter::ReturnCode rc = scan_options.filter->FilterCell(
        row_buf.ColumnFamily(kv_index), row_buf.Qualifier(kv_index), row_buf.Value(kv_index));
    if (rc == filter::Filter::kNotIncludeCurAndLeftCellOfRow) {
      break;
    }
  }
  if (scan_options.filter->FilterRow()) {
    return true;
  } else {
    return false;
  }
}

void TabletIO::ProcessRowBuffer(const SingleRowBuffer& row_buf, const ScanOptions& scan_options,
                                RowResult* values, uint32_t* buffer_size, int64_t* number_limit) {
  if (row_buf.Size() <= 0) {
    return;
  }
  if (ShouldFilterRowBuffer(row_buf, scan_options)) {
    scan_filter_count.Add(row_buf.Size());
    return;
  }

  for (size_t kv_index = 0; kv_index != row_buf.Size(); ++kv_index) {
    const std::string& key = row_buf.RowKey(kv_index);
    const std::string& col = row_buf.ColumnFamily(kv_index);
    const std::string& qual = row_buf.Qualifier(kv_index);
    const std::string& value = row_buf.Value(kv_index);
    int64_t ts = row_buf.TimeStamp(kv_index);

    // skip unnecessary columns and qualifiers
    if (scan_options.column_family_list.size() > 0) {
      ColumnFamilyMap::const_iterator it = scan_options.column_family_list.find(col);
      if (it != scan_options.column_family_list.end()) {
        const std::set<std::string>& qual_list = it->second;
        if (qual_list.size() > 0 && qual_list.end() == qual_list.find(qual)) {
          scan_filter_count.Inc();
          continue;
        }
      } else {
        scan_filter_count.Inc();
        continue;
      }
    }
    // time range filter
    if (ts < scan_options.ts_start || ts > scan_options.ts_end) {
      scan_filter_count.Inc();
      continue;
    }

    (*number_limit)++;
    *buffer_size += key.size() + col.size() + qual.size() + sizeof(ts) + value.size();

    row_buf.Serialize(kv_index, values->add_key_values());
  }
}

uint64_t TabletIO::GetSnapshot(uint64_t id, uint64_t snapshot_sequence, StatusCode* status) {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      SetStatusCode(status_, status);
      return 0;
    }
    db_ref_count_++;
  }
  uint64_t snapshot = db_->GetSnapshot(snapshot_sequence);
  MutexLock lock(&mutex_);
  id_to_snapshot_num_[id] = snapshot;
  db_ref_count_--;
  return snapshot;
}

bool TabletIO::ReleaseSnapshot(uint64_t snapshot_id, StatusCode* status) {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      SetStatusCode(status_, status);
      return false;
    }
    if (id_to_snapshot_num_.find(snapshot_id) == id_to_snapshot_num_.end()) {
      SetStatusCode(kSnapshotNotExist, status);
      return false;
    }
    db_ref_count_++;
  }
  db_->ReleaseSnapshot(id_to_snapshot_num_[snapshot_id]);
  MutexLock lock(&mutex_);
  id_to_snapshot_num_.erase(snapshot_id);
  db_ref_count_--;
  return true;
}

void TabletIO::ListSnapshot(std::vector<uint64_t>* snapshot_id) {
  MutexLock lock(&mutex_);
  if (status_ != kReady) {
    return;
  }
  for (std::map<uint64_t, uint64_t>::iterator it = id_to_snapshot_num_.begin();
       it != id_to_snapshot_num_.end(); ++it) {
    snapshot_id->push_back(it->first);
    VLOG(7) << tablet_path_ << " ListSnapshot: " << it->first << " - " << it->second;
  }
}

uint64_t TabletIO::Rollback(uint64_t snapshot_id, StatusCode* status) {
  uint64_t sequence;
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      SetStatusCode(status_, status);
      return false;
    }
    std::map<uint64_t, uint64_t>::iterator it = id_to_snapshot_num_.find(snapshot_id);
    if (it == id_to_snapshot_num_.end()) {
      SetStatusCode(kSnapshotNotExist, status);
      return false;
    } else {
      sequence = it->second;
    }
    db_ref_count_++;
  }
  uint64_t rollback_point = db_->Rollback(sequence);
  MutexLock lock(&mutex_);
  rollbacks_[sequence] = rollback_point;
  db_ref_count_--;
  return rollback_point;
}

uint32_t TabletIO::GetLGidByCFName(const std::string& cfname) {
  MutexLock lock(&schema_mutex_);
  std::map<std::string, uint32_t>::iterator it = cf_lg_map_.find(cfname);
  if (it != cf_lg_map_.end()) {
    return it->second;
  }
  return 0;
}

void TabletIO::SetStatus(TabletStatus status) {
  MutexLock lock(&mutex_);
  status_ = status;
}

TabletIO::TabletStatus TabletIO::GetStatus() {
  MutexLock lock(&mutex_);
  return status_;
}

std::string TabletIO::GetLastErrorMessage() {
  MutexLock lock(&mutex_);
  return last_err_msg_;
}

const leveldb::RawKeyOperator* TabletIO::GetRawKeyOperator() { return key_operator_; }

int32_t TabletIO::AddRef() {
  MutexLock lock(&mutex_);
  ++ref_count_;
  return ref_count_;
}

int32_t TabletIO::DecRef() {
  int32_t ref = 0;
  {
    MutexLock lock(&mutex_);
    ref = (--ref_count_);
  }
  if (ref == 0) {
    delete this;
  }
  return ref;
}

int32_t TabletIO::GetRef() const { return ref_count_; }

void TabletIO::ApplySchema(const TableSchema& schema) {
  MutexLock lock(&schema_mutex_);
  SetSchema(schema);
  IndexingCfToLG();
  ldb_options_.compact_strategy_factory->SetArg(&schema);
}

bool TabletIO::SingleRowTxnCheck(const std::string& row_key,
                                 const SingleRowTxnReadInfo& txn_read_info, StatusCode* status) {
  // init scan_options
  ScanOptions scan_options;
  for (int32_t i = 0; i < txn_read_info.read_column_list_size(); ++i) {
    const ColumnFamily& column_info = txn_read_info.read_column_list(i);
    std::set<std::string>& qualifier_list =
        scan_options.column_family_list[column_info.family_name()];
    for (int32_t j = 0; j < column_info.qualifier_list_size(); ++j) {
      qualifier_list.insert(column_info.qualifier_list(j));
    }
    scan_options.iter_cf_set.insert(column_info.family_name());
  }
  scan_options.max_versions = txn_read_info.max_versions();
  if (txn_read_info.has_start_timestamp()) {
    scan_options.ts_start = txn_read_info.start_timestamp();
  }
  if (txn_read_info.has_end_timestamp()) {
    scan_options.ts_end = txn_read_info.end_timestamp();
  }

  // read the row
  std::string start_tera_key;
  key_operator_->EncodeTeraKey(row_key, "", "", kLatestTs, leveldb::TKT_VALUE, &start_tera_key);
  std::string end_row_key = row_key + '\0';
  RowResult row_result;
  uint32_t read_row_count = 0;
  uint32_t read_cell_count = 0;
  uint32_t read_bytes = 0;
  bool complete = false;
  if (!LowLevelScan(start_tera_key, end_row_key, scan_options, &row_result, NULL, &read_row_count,
                    &read_cell_count, &read_bytes, &complete, status)) {
    return false;
  }

  // verify values against txn_read_info
  if (row_result.key_values_size() != txn_read_info.read_result().key_values_size()) {
    LOG(WARNING) << "[stxn] [kTxnFail] " << row_result.key_values_size() << ":"
                 << txn_read_info.read_result().key_values_size();
    SetStatusCode(kTxnFail, status);
    return false;
  }
  // older sdk's write request has no start_timestamp/end_timestamp/value
  bool has_timestamp = txn_read_info.has_start_timestamp() || txn_read_info.has_end_timestamp();
  for (int32_t i = 0; i < row_result.key_values_size(); ++i) {
    const KeyValuePair& new_kv = row_result.key_values(i);
    const KeyValuePair& old_kv = txn_read_info.read_result().key_values(i);
    if (new_kv.column_family() != old_kv.column_family() ||
        new_kv.qualifier() != old_kv.qualifier() || new_kv.timestamp() != old_kv.timestamp() ||
        (has_timestamp && new_kv.value() != old_kv.value())) {
      LOG(WARNING) << "[stxn] [kTxnFail] ColumnFamily=" << new_kv.column_family() << ":"
                   << old_kv.column_family() << " Qualifier=" << new_kv.qualifier() << ":"
                   << old_kv.qualifier() << " Timestamp=" << new_kv.timestamp() << ":"
                   << old_kv.timestamp() << " " << has_timestamp
                   << "Value=" << (new_kv.value() != old_kv.value());
      SetStatusCode(kTxnFail, status);
      return false;
    }
  }
  return true;
}

bool TabletIO::RefreshDBStatus() {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      return false;
    }
    db_ref_count_++;
  }

  std::string db_property_key = "leveldb.verify-db-integrity";
  std::string db_property_val;
  if (db_->GetProperty(db_property_key, &db_property_val)) {
    MutexLock lock(&mutex_);
    if (db_property_val.find("verify_fail") != std::string::npos) {
      tablet_status_ = TabletMeta::kTabletCorruption;
      LOG(WARNING) << "db status: " << db_property_val;
    } else if (db_property_val.find("manifest_error") != std::string::npos) {
      tablet_status_ = TabletMeta::kTabletManifestError;
      LOG(WARNING) << "db status: " << db_property_val;
    } else {
      tablet_status_ = static_cast<tera::TabletMeta::TabletStatus>(kTabletReady);
    }
  }

  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
  return true;
}

void TabletIO::GetDBStatus(tera::TabletMeta::TabletStatus* tablet_status) {
  *tablet_status = static_cast<tera::TabletMeta::TabletStatus>(kTabletReady);
  MutexLock lock(&mutex_);
  if (status_ == kReady) {
    *tablet_status = tablet_status_;
  }
}

void TabletIO::CheckBackgroundError(std::string* error_msg) {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      return;
    }
    db_ref_count_++;
  }

  std::string db_property_key = "leveldb.compaction_error";
  std::string db_property_val;
  if (db_->GetProperty(db_property_key, &db_property_val) &&
      db_property_val.find("Corruption: ") != std::string::npos) {
    if (db_property_val.length() > kReportErrorSize) {
      LOG(ERROR) << "Find compaction error too much for report, " << db_property_val;
      db_property_val = "Too much error for report, check ts log.";
    }
    *error_msg = db_property_val;
  }
  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
}

bool TabletIO::IsUrgentUnload() const {
  return try_unload_count_ >= FLAGS_tera_tablet_unload_count_limit;
}

bool TabletIO::GetDBLevelSize(std::vector<int64_t>* result) {
  {
    MutexLock lock(&mutex_);
    if (status_ != kReady) {
      return false;
    }
    db_ref_count_++;
  }
  db_->GetCurrentLevelSize(result);
  {
    MutexLock lock(&mutex_);
    db_ref_count_--;
  }
  return true;
}
}  // namespace io
}  // namespace tera
