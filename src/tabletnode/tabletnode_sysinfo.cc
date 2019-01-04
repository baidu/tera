// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#include <cmath>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "common/base/string_number.h"
#include "common/metric/collector_report_publisher.h"
#include "common/metric/ratio_subscriber.h"
#include "common/metric/prometheus_subscriber.h"
#include "common/timer.h"
#include "common/this_thread.h"
#include "tabletnode/tabletnode_sysinfo.h"
#include "proto/proto_helper.h"
#include "quota/ts_write_flow_controller.h"
#include "tabletnode/tabletnode_metric_name.h"
#include "utils/tprinter.h"
#include "utils/utils_cmd.h"
#include "io/utils_leveldb.h"
#include "leveldb/persistent_cache.h"

DECLARE_bool(tera_tabletnode_dump_running_info);
DECLARE_bool(tera_tabletnode_dump_level_size_info_enabled);
DECLARE_string(tera_tabletnode_running_info_dump_file);
DECLARE_int64(tera_tabletnode_sysinfo_check_interval);
DECLARE_bool(tera_enable_persistent_cache);

namespace leveldb {
extern tera::Counter rawkey_compare_counter;

extern tera::Counter dfs_read_size_counter;
extern tera::Counter dfs_write_size_counter;
extern tera::Counter posix_read_size_counter;
extern tera::Counter posix_write_size_counter;

extern tera::Counter posix_read_counter;
extern tera::Counter posix_write_counter;
extern tera::Counter posix_sync_counter;
extern tera::Counter posix_list_counter;
extern tera::Counter posix_exists_counter;
extern tera::Counter posix_open_counter;
extern tera::Counter posix_close_counter;
extern tera::Counter posix_delete_counter;
extern tera::Counter posix_tell_counter;
extern tera::Counter posix_seek_counter;
extern tera::Counter posix_info_counter;
extern tera::Counter posix_other_counter;

extern tera::Counter dfs_read_delay_counter;
extern tera::Counter dfs_write_delay_counter;
extern tera::Counter dfs_sync_delay_counter;

extern tera::Counter dfs_read_counter;
extern tera::Counter dfs_write_counter;
extern tera::Counter dfs_sync_counter;
extern tera::Counter dfs_flush_counter;
extern tera::Counter dfs_list_counter;
extern tera::Counter dfs_exists_counter;
extern tera::Counter dfs_open_counter;
extern tera::Counter dfs_close_counter;
extern tera::Counter dfs_delete_counter;
extern tera::Counter dfs_tell_counter;
extern tera::Counter dfs_info_counter;
extern tera::Counter dfs_other_counter;

extern tera::Counter dfs_read_error_counter;
extern tera::Counter dfs_write_error_counter;
extern tera::Counter dfs_sync_error_counter;
extern tera::Counter dfs_flush_error_counter;
extern tera::Counter dfs_list_error_counter;
extern tera::Counter dfs_exists_error_counter;
extern tera::Counter dfs_open_error_counter;
extern tera::Counter dfs_close_error_counter;
extern tera::Counter dfs_delete_error_counter;
extern tera::Counter dfs_tell_error_counter;
extern tera::Counter dfs_info_error_counter;
extern tera::Counter dfs_other_error_counter;

extern tera::Counter dfs_read_hang_counter;
extern tera::Counter dfs_write_hang_counter;
extern tera::Counter dfs_sync_hang_counter;
extern tera::Counter dfs_flush_hang_counter;
extern tera::Counter dfs_list_hang_counter;
extern tera::Counter dfs_exists_hang_counter;
extern tera::Counter dfs_open_hang_counter;
extern tera::Counter dfs_close_hang_counter;
extern tera::Counter dfs_delete_hang_counter;
extern tera::Counter dfs_tell_hang_counter;
extern tera::Counter dfs_info_hang_counter;
extern tera::Counter dfs_other_hang_counter;

extern tera::Counter dfs_opened_read_files_counter;
extern tera::Counter dfs_opened_write_files_counter;

extern tera::Counter ssd_read_counter;
extern tera::Counter ssd_read_size_counter;
extern tera::Counter ssd_write_counter;
extern tera::Counter ssd_write_size_counter;
}

namespace tera {
namespace tabletnode {
// dfs metrics
tera::AutoCollectorRegister dfs_read_size_metric(
    kDfsReadBytesThroughPut,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_read_size_counter, true)),
    {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister dfs_write_size_metric(
    kDfsWriteBytesThroughPut,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_write_size_counter, true)),
    {SubscriberType::THROUGHPUT});

tera::AutoCollectorRegister dfs_read_metric(
    kDfsRequestMetric, kDfsReadLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_read_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_write_metric(
    kDfsRequestMetric, kDfsWriteLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_write_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_sync_metric(
    kDfsRequestMetric, kDfsSyncLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_sync_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_flush_metric(
    kDfsRequestMetric, kDfsFlushLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_flush_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_list_metric(
    kDfsRequestMetric, kDfsListLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_list_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_exists_metric(
    kDfsRequestMetric, kDfsExistsLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_exists_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_open_metric(
    kDfsRequestMetric, kDfsOpenLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_open_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_close_metric(
    kDfsRequestMetric, kDfsCloseLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_close_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_delete_metric(
    kDfsRequestMetric, kDfsDeleteLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_delete_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_tell_metric(
    kDfsRequestMetric, kDfsTellLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_tell_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_info_metric(
    kDfsRequestMetric, kDfsInfoLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_info_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_other_metric(
    kDfsRequestMetric, kDfsOtherLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_other_counter)),
    {SubscriberType::QPS});

tera::AutoCollectorRegister dfs_read_error_counter(
    kDfsErrorMetric, kDfsReadLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_read_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_write_error_counter(
    kDfsErrorMetric, kDfsWriteLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_write_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_sync_error_counter(
    kDfsErrorMetric, kDfsSyncLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_sync_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_flush_error_counter(
    kDfsErrorMetric, kDfsFlushLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_flush_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_list_error_counter(
    kDfsErrorMetric, kDfsListLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_list_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_exists_error_counter(
    kDfsErrorMetric, kDfsExistsLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_exists_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_open_error_counter(
    kDfsErrorMetric, kDfsOpenLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_open_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_close_error_counter(
    kDfsErrorMetric, kDfsCloseLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_close_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_delete_error_counter(
    kDfsErrorMetric, kDfsDeleteLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_delete_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_tell_error_counter(
    kDfsErrorMetric, kDfsTellLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_tell_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_info_error_counter(
    kDfsErrorMetric, kDfsInfoLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_info_error_counter)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_other_error_counter(
    kDfsErrorMetric, kDfsOtherLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_other_error_counter)),
    {SubscriberType::QPS});

tera::AutoCollectorRegister dfs_read_delay_metric(
    kDfsReadDelayMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_read_delay_counter, true)), {});
tera::AutoCollectorRegister dfs_write_delay_metric(
    kDfsWriteDelayMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_write_delay_counter, true)), {});
tera::AutoCollectorRegister dfs_sync_delay_metric(
    kDfsSyncDelayMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_sync_delay_counter, true)), {});

tera::AutoSubscriberRegister dfs_read_delay_avg_subscriber(std::unique_ptr<Subscriber>(
    new RatioSubscriber(MetricId(kDfsReadDelayPerRequestMetric),
                        std::unique_ptr<Subscriber>(new PrometheusSubscriber(
                            MetricId(kDfsReadDelayMetric), SubscriberType::SUM)),
                        std::unique_ptr<Subscriber>(new PrometheusSubscriber(
                            MetricId(kDfsRequestMetric, kDfsReadLabel), SubscriberType::SUM)))));

tera::AutoSubscriberRegister dfs_write_delay_avg_subscriber(std::unique_ptr<Subscriber>(
    new RatioSubscriber(MetricId(kDfsWriteDelayPerRequestMetric),
                        std::unique_ptr<Subscriber>(new PrometheusSubscriber(
                            MetricId(kDfsWriteDelayMetric), SubscriberType::SUM)),
                        std::unique_ptr<Subscriber>(new PrometheusSubscriber(
                            MetricId(kDfsRequestMetric, kDfsWriteLabel), SubscriberType::SUM)))));

tera::AutoSubscriberRegister dfs_sync_delay_avg_subscriber(std::unique_ptr<Subscriber>(
    new RatioSubscriber(MetricId(kDfsSyncDelayPerRequestMetric),
                        std::unique_ptr<Subscriber>(new PrometheusSubscriber(
                            MetricId(kDfsSyncDelayMetric), SubscriberType::SUM)),
                        std::unique_ptr<Subscriber>(new PrometheusSubscriber(
                            MetricId(kDfsRequestMetric, kDfsSyncLabel), SubscriberType::SUM)))));

tera::AutoCollectorRegister dfs_read_hang_metric(
    kDfsHangMetric, kDfsReadLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_read_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_write_hang_metric(
    kDfsHangMetric, kDfsWriteLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_write_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_sync_hang_metric(
    kDfsHangMetric, kDfsSyncLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_sync_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_flush_hang_metric(
    kDfsHangMetric, kDfsFlushLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_flush_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_list_hang_metric(
    kDfsHangMetric, kDfsListLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_list_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_exists_hang_metric(
    kDfsHangMetric, kDfsExistsLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_exists_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_open_hang_metric(
    kDfsHangMetric, kDfsOpenLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_open_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_close_hang_metric(
    kDfsHangMetric, kDfsCloseLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_close_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_delete_hang_metric(
    kDfsHangMetric, kDfsDeleteLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_delete_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_tell_hang_metric(
    kDfsHangMetric, kDfsTellLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_tell_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_info_hang_metric(
    kDfsHangMetric, kDfsInfoLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_info_hang_counter)),
    {SubscriberType::SUM});
tera::AutoCollectorRegister dfs_other_hang_metric(
    kDfsHangMetric, kDfsOtherLabel,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_other_hang_counter)),
    {SubscriberType::SUM});

tera::AutoCollectorRegister dfs_opened_read_files_metric(
    kDfsOpenedReadFilesCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_opened_read_files_counter,
                                                    false)));
tera::AutoCollectorRegister dfs_opened_write_files_metric(
    kDfsOpenedWriteFilesCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_opened_write_files_counter,
                                                    false)));
// ssd metrics
tera::AutoCollectorRegister ssd_read_through_put_metric(
    kSsdReadThroughPutMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::ssd_read_size_counter, true)),
    {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister ssd_write_through_put_metric(
    kSsdWriteThroughPutMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::ssd_write_size_counter, true)),
    {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister ssd_read_metric(
    kSsdReadCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::ssd_read_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister ssd_write_metric(
    kSsdWriteCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::ssd_write_counter, true)),
    {SubscriberType::QPS});
// local metrics
tera::AutoCollectorRegister posix_read_size_metric(
    kPosixReadThroughPutMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_read_size_counter, true)),
    {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister posix_write_size_metric(
    kPosixWriteThroughPutMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_write_size_counter, true)),
    {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister posix_read_metric(
    kPosixReadCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_read_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_write_metric(
    kPosixWriteCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_write_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_sync_metric(
    kPosixSyncCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_sync_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_list_metric(
    kPosixListCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_list_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_exists_metric(
    kPosixExistsCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_exists_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_open_metric(
    kPosixOpenCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_open_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_close_metric(
    kPosixCloseCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_close_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_delete_metric(
    kPosixDeleteCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_delete_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_tell_metric(
    kPosixTellCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_tell_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_seek_metric(
    kPosixSeekCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_seek_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_info_metric(
    kPosixInfoCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_info_counter, true)),
    {SubscriberType::QPS});
tera::AutoCollectorRegister posix_other_metric(
    kPosixOtherCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_other_counter, true)),
    {SubscriberType::QPS});

tera::AutoCollectorRegister rawkey_compare_metric(
    kRawkeyCompareCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::rawkey_compare_counter, true)),
    {SubscriberType::QPS});

tera::MetricCounter not_ready_counter(kNotReadyCountMetric, {SubscriberType::LATEST}, false);
tera::MetricCounter ts_tablet_size_counter(kTabletSizeCounter, {SubscriberType::LATEST}, false);
tera::MetricCounter ts_tablet_num_counter(kTabletNumCounter, {SubscriberType::LATEST}, false);
tera::MetricCounter mem_table_size(kMemTableSize, {SubscriberType::LATEST}, false);

class TabletNodeSysInfoDumper {
 public:
  explicit TabletNodeSysInfoDumper(const std::string& filename)
      : filename_(filename),
        fp_(NULL, [](FILE* f) {
          if (f) {
            fclose(f);
          }
        }) {
    std::string dirname = filename_.substr(0, filename_.rfind('/'));
    int ret = mkdir(dirname.c_str(), 0755);
    if (ret != 0 && errno != EEXIST) {
      LOG(ERROR) << "fail to make dump dir " << dirname;
    }
    auto tmp_file = fopen(filename_.c_str(), "w");
    if (!tmp_file) {
      LOG(ERROR) << "fail to open dump file " << filename_;
    }
    fp_.reset(tmp_file);
  }
  ~TabletNodeSysInfoDumper() = default;

  template <typename T>
  bool DumpData(const std::string& item_name, T data) const {
    std::stringstream ss;
    ss << item_name << " : " << data;
    fprintf(fp_.get(), "%s\r\n", ss.str().c_str());
    return true;
  }

 private:
  std::string filename_;
  std::unique_ptr<FILE, std::function<void(FILE*)>> fp_;
};

TabletNodeSysInfo::TabletNodeSysInfo()
    : info_{new TabletNodeInfo}, tablet_list_{new TabletMetaList} {
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpSysInfo);
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpHardWareInfo);
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpIoInfo);
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpCacheInfo);
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpRequestInfo);
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpDfsInfo);
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpPosixInfo);
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpLevelSizeInfo);
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpPersistentCacheInfo);
  RegisterDumpInfoFunction(&TabletNodeSysInfo::DumpOtherInfo);
}

TabletNodeSysInfo::~TabletNodeSysInfo() {}

void TabletNodeSysInfo::AddExtraInfo(const std::string& name, int64_t value) {
  MutexLock lock(&mutex_);
  if (!info_.unique()) {
    SwitchInfo();
  }
  assert(info_.unique());
  ExtraTsInfo* e_info = info_->add_extra_info();
  e_info->set_name(name);
  e_info->set_value(value);
}

void TabletNodeSysInfo::SetProcessStartTime(int64_t ts) {
  MutexLock lock(&mutex_);
  if (!info_.unique()) {
    SwitchInfo();
  }
  assert(info_.unique());
  info_->set_process_start_time(ts);
}

void TabletNodeSysInfo::SetTimeStamp(int64_t ts) {
  MutexLock lock(&mutex_);
  if (!info_.unique()) {
    SwitchInfo();
  }
  assert(info_.unique());
  info_->set_timestamp(ts);
}

struct DBSize {
  uint64_t size;
  std::vector<uint64_t> lg_size;
};

void TabletNodeSysInfo::RefreshTabletsStatus(TabletManager* tablet_manager) {
  std::vector<io::TabletIO*> tablet_ios;
  tablet_manager->GetAllTablets(&tablet_ios);
  std::vector<io::TabletIO*>::iterator it = tablet_ios.begin();
  while (it != tablet_ios.end()) {
    io::TabletIO* tablet_io = *it;
    if (tablet_io->ShouldForceUnloadOnError()) {
      LOG(WARNING) << *tablet_io << ", has internal error triggered unload";
      StatusCode status;
      if (!tablet_io->Unload(&status)) {
        LOG(ERROR) << *tablet_io
                   << ", Unload tablet failed, status: " << StatusCodeToString(status);
      }
      if (!tablet_manager->RemoveTablet(tablet_io->GetTableName(), tablet_io->GetStartKey(),
                                        tablet_io->GetEndKey(), &status)) {
        LOG(ERROR) << *tablet_io
                   << ", remove from TabletManager failed, status: " << StatusCodeToString(status);
      }
      tablet_io->DecRef();
      it = tablet_ios.erase(it);
      continue;
    }

    // sham random sleep (0-999 ms) to relieve nfs pressure
    ThisThread::Sleep(get_millis() % 1000);
    // refresh db status whether is corruption
    tablet_io->RefreshDBStatus();
    tablet_io->DecRef();
    ++it;
  }
}

void TabletNodeSysInfo::CollectTabletNodeInfo(TabletManager* tablet_manager,
                                              const string& server_addr) {
  std::vector<io::TabletIO*> tablet_ios;
  std::vector<TabletMeta::TabletStatus> db_status_vec;
  std::vector<DBSize> db_size_vec;

  tablet_manager->GetAllTablets(&tablet_ios);
  ts_tablet_num_counter.Set(tablet_ios.size());

  std::vector<io::TabletIO*>::iterator it = tablet_ios.begin();
  while (it != tablet_ios.end()) {
    io::TabletIO* tablet_io = *it;
    if (tablet_io->ShouldForceUnloadOnError()) {
      LOG(WARNING) << *tablet_io << ", has internal error triggered unload";
      StatusCode status;
      if (!tablet_io->Unload(&status)) {
        LOG(ERROR) << *tablet_io
                   << ", Unload tablet failed, status: " << StatusCodeToString(status);
      }
      if (!tablet_manager->RemoveTablet(tablet_io->GetTableName(), tablet_io->GetStartKey(),
                                        tablet_io->GetEndKey(), &status)) {
        LOG(ERROR) << *tablet_io
                   << ", remove from TabletManager failed, status: " << StatusCodeToString(status);
      }
      tablet_io->DecRef();
      it = tablet_ios.erase(it);
      continue;
    }

    // check db status whether is corruption
    TabletMeta::TabletStatus tablet_status = static_cast<TabletMeta::TabletStatus>(kTabletReady);
    tablet_io->GetDBStatus(&tablet_status);
    db_status_vec.push_back(tablet_status);

    DBSize db_size;
    uint64_t tmp_mem_table_size{0};
    tablet_io->GetDataSize(&db_size.size, &db_size.lg_size, &tmp_mem_table_size);
    mem_table_size.Set((int64_t)tmp_mem_table_size);
    db_size_vec.push_back(db_size);

    ++it;
  }

  MutexLock lock(&mutex_);
  if (!info_.unique()) {
    SwitchInfo();
  }
  assert(info_.unique());
  std::shared_ptr<CollectorReport> latest_report =
      CollectorReportPublisher::GetInstance().GetCollectorReport();
  int64_t interval = latest_report->interval_ms;
  if (interval <= 0) {
    // maybe happen at first report, the metric values must be 0
    // set to any non-zero value to avoid div 0
    VLOG(16) << "Metric Report interval is 0";
    interval = 1000;
  }

  tablet_list_->Clear();
  int64_t total_size = 0;
  int64_t scan_kvs = 0;
  int64_t read_kvs = 0;
  int64_t write_kvs = 0;
  int64_t busy_cnt = 0;
  int64_t not_ready = 0;
  int64_t db_corruption_cnt = 0;

  for (uint32_t i = 0; i < tablet_ios.size(); i++) {
    io::TabletIO* tablet_io = tablet_ios[i];
    TabletMeta::TabletStatus tablet_status = db_status_vec[i];
    DBSize db_size = db_size_vec[i];

    TabletMeta* tablet_meta = tablet_list_->add_meta();
    tablet_meta->set_status(TabletMeta::TabletStatus(tablet_io->GetStatus()));
    if (tablet_meta->status() != TabletMeta::kTabletReady) {
      ++not_ready;
    }
    tablet_meta->set_server_addr(server_addr);
    tablet_meta->set_table_name(tablet_io->GetTableName());
    tablet_meta->set_path(tablet_io->GetTablePath());
    tablet_meta->mutable_key_range()->set_key_start(tablet_io->GetStartKey());
    tablet_meta->mutable_key_range()->set_key_end(tablet_io->GetEndKey());
    tablet_meta->set_create_time(tablet_io->CreateTime());
    tablet_meta->set_version(tablet_io->Version());
    tablet_meta->set_size(db_size.size);
    for (size_t i = 0; i < db_size.lg_size.size(); ++i) {
      tablet_meta->add_lg_size(db_size.lg_size[i]);
    }
    tablet_meta->set_compact_status(tablet_io->GetCompactStatus());
    total_size += tablet_meta->size();

    TabletCounter* counter = tablet_list_->add_counter();
    const std::string& label_str = tablet_io->GetMetricLabel();
    counter->set_low_read_cell(latest_report->FindMetricValue(kLowReadCellMetricName, label_str));
    counter->set_scan_rows(latest_report->FindMetricValue(kScanRowsMetricName, label_str));
    counter->set_scan_kvs(latest_report->FindMetricValue(kScanKvsMetricName, label_str));
    counter->set_scan_size(latest_report->FindMetricValue(kScanThroughPutMetricName, label_str));
    counter->set_read_rows(latest_report->FindMetricValue(kReadRowsMetricName, label_str));
    counter->set_read_kvs(latest_report->FindMetricValue(kReadKvsMetricName, label_str));
    counter->set_read_size(latest_report->FindMetricValue(kReadThroughPutMetricName, label_str));
    counter->set_write_rows(latest_report->FindMetricValue(kWriteRowsMetricName, label_str));
    counter->set_write_kvs(latest_report->FindMetricValue(kWriteKvsMetricName, label_str));
    counter->set_write_size(latest_report->FindMetricValue(kWriteThroughPutMetricName, label_str));
    counter->set_is_on_busy(tablet_io->IsBusy());
    double write_workload = 0;
    tablet_io->Workload(&write_workload);
    counter->set_write_workload(write_workload);
    counter->set_db_status(tablet_status);  // set runtime counter

    scan_kvs += counter->scan_kvs();
    read_kvs += counter->read_kvs();
    write_kvs += counter->write_kvs();

    if (counter->is_on_busy()) {
      busy_cnt++;
    }
    if (counter->db_status() == TabletMeta::kTabletCorruption) {
      db_corruption_cnt++;
    }
    tablet_io->DecRef();
  }
  not_ready_counter.Set(not_ready);
  ts_tablet_size_counter.Set(total_size);

  int64_t low_read_cell = latest_report->FindMetricValue(kLowLevelReadMetric);
  int64_t read_rows = latest_report->FindMetricValue(kRowCountMetric, kApiLabelRead);
  int64_t read_size = latest_report->FindMetricValue(kRowThroughPutMetric, kApiLabelRead);
  int64_t write_rows = latest_report->FindMetricValue(kRowCountMetric, kApiLabelWrite);
  int64_t write_size = latest_report->FindMetricValue(kRowThroughPutMetric, kApiLabelWrite);
  int64_t scan_rows = latest_report->FindMetricValue(kRowCountMetric, kApiLabelScan);
  int64_t scan_size = latest_report->FindMetricValue(kRowThroughPutMetric, kApiLabelScan);

  info_->set_low_read_cell(low_read_cell * 1000 / interval);
  info_->set_scan_rows(scan_rows * 1000 / interval);
  info_->set_scan_kvs(scan_kvs * 1000 / interval);
  info_->set_scan_size(scan_size * 1000 / interval);
  info_->set_read_rows(read_rows * 1000 / interval);
  info_->set_read_kvs(read_kvs * 1000 / interval);
  info_->set_read_size(read_size * 1000 / interval);
  info_->set_write_rows(write_rows * 1000 / interval);
  info_->set_write_kvs(write_kvs * 1000 / interval);
  info_->set_write_size(write_size * 1000 / interval);
  info_->set_tablet_onbusy(busy_cnt);
  info_->set_tablet_corruption(db_corruption_cnt);
  // refresh tabletnodeinfo
  info_->set_load(total_size);
  info_->set_tablet_total(tablet_ios.size());

  int64_t tmp;
  tmp = latest_report->FindMetricValue(kDfsReadBytesThroughPut) * 1000 / interval;
  info_->set_dfs_io_r(tmp);
  tmp = latest_report->FindMetricValue(kDfsWriteBytesThroughPut) * 1000 / interval;
  info_->set_dfs_io_w(tmp);
  tmp = latest_report->FindMetricValue(kPosixReadThroughPutMetric) * 1000 / interval;
  info_->set_local_io_r(tmp);
  tmp = latest_report->FindMetricValue(kPosixWriteThroughPutMetric) * 1000 / interval;
  info_->set_local_io_w(tmp);
  // Requests need to go through dfs's master
  tmp = latest_report->FindMetricValue(kDfsRequestMetric, kDfsOpenLabel) +
        latest_report->FindMetricValue(kDfsRequestMetric, kDfsCloseLabel) +
        latest_report->FindMetricValue(kDfsRequestMetric, kDfsDeleteLabel);
  info_->set_dfs_master_qps(tmp * 1000 / interval);

  int64_t read_pending = latest_report->FindMetricValue(kPendingCountMetric, kApiLabelRead);
  int64_t write_pending = latest_report->FindMetricValue(kPendingCountMetric, kApiLabelWrite);
  int64_t scan_pending = latest_report->FindMetricValue(kPendingCountMetric, kApiLabelScan);
  int64_t compact_pending = latest_report->FindMetricValue(kPendingCountMetric, kApiLabelCompact);

  info_->set_read_pending(read_pending);
  info_->set_write_pending(write_pending);
  info_->set_scan_pending(scan_pending);

  // collect extra infos
  info_->clear_extra_info();
  ExtraTsInfo* einfo = info_->add_extra_info();

  int64_t range_error_sum = latest_report->FindMetricValue(kRangeErrorMetric, kApiLabelRead) +
                            latest_report->FindMetricValue(kRangeErrorMetric, kApiLabelWrite) +
                            latest_report->FindMetricValue(kRangeErrorMetric, kApiLabelScan);

  tmp = range_error_sum * 1000 / interval;
  einfo->set_name("range_error");
  einfo->set_value(tmp);

  einfo = info_->add_extra_info();
  einfo->set_name("read_pending");
  einfo->set_value(read_pending);

  einfo = info_->add_extra_info();
  einfo->set_name("write_pending");
  einfo->set_value(write_pending);

  einfo = info_->add_extra_info();
  einfo->set_name("scan_pending");
  einfo->set_value(scan_pending);

  einfo = info_->add_extra_info();
  einfo->set_name("compact_pending");
  einfo->set_value(compact_pending);

  einfo = info_->add_extra_info();
  tmp = latest_report->FindMetricValue(kRejectCountMetric, kApiLabelRead) * 1000 / interval;
  einfo->set_name("read_reject");
  einfo->set_value(tmp);

  einfo = info_->add_extra_info();
  tmp = latest_report->FindMetricValue(kRejectCountMetric, kApiLabelWrite) * 1000 / interval;
  einfo->set_name("write_reject");
  einfo->set_value(tmp);

  einfo = info_->add_extra_info();
  tmp = latest_report->FindMetricValue(kRejectCountMetric, kApiLabelScan) * 1000 / interval;
  einfo->set_name("scan_reject");
  einfo->set_value(tmp);

  einfo = info_->add_extra_info();
  tmp = latest_report->FindMetricValue(kRequestCountMetric, kApiLabelRead) * 1000 / interval;
  einfo->set_name("read_request");
  einfo->set_value(tmp);

  einfo = info_->add_extra_info();
  tmp = latest_report->FindMetricValue(kRequestCountMetric, kApiLabelWrite) * 1000 / interval;
  einfo->set_name("write_request");
  einfo->set_value(tmp);

  einfo = info_->add_extra_info();
  tmp = latest_report->FindMetricValue(kRequestCountMetric, kApiLabelScan) * 1000 / interval;
  einfo->set_name("scan_request");
  einfo->set_value(tmp);

  einfo = info_->add_extra_info();
  tmp = latest_report->FindMetricValue(kErrorCountMetric, kApiLabelRead) * 1000 / interval;
  einfo->set_name("read_error");
  einfo->set_value(tmp);

  einfo = info_->add_extra_info();
  tmp = latest_report->FindMetricValue(kErrorCountMetric, kApiLabelWrite) * 1000 / interval;
  einfo->set_name("write_error");
  einfo->set_value(tmp);

  einfo = info_->add_extra_info();
  tmp = latest_report->FindMetricValue(kErrorCountMetric, kApiLabelScan) * 1000 / interval;
  einfo->set_name("scan_error");
  einfo->set_value(tmp);
}

void TabletNodeSysInfo::CollectHardwareInfo() {
  MutexLock lock(&mutex_);
  if (!info_.unique()) {
    SwitchInfo();
  }
  assert(info_.unique());
  std::shared_ptr<CollectorReport> latest_report =
      CollectorReportPublisher::GetInstance().GetCollectorReport();

  int64_t cpu_usage = latest_report->FindMetricValue(kInstCpuMetricName);
  info_->set_cpu_usage(static_cast<float>(cpu_usage));

  int64_t mem_usage = latest_report->FindMetricValue(kInstMemMetricName);
  info_->set_mem_used(mem_usage);

  int64_t net_rx_usage = latest_report->FindMetricValue(kInstNetRXMetricName);
  info_->set_net_rx(net_rx_usage);

  int64_t net_tx_usage = latest_report->FindMetricValue(kInstNetTXMetricName);
  info_->set_net_tx(net_tx_usage);
}

void TabletNodeSysInfo::GetTabletNodeInfo(TabletNodeInfo* info) {
  MutexLock lock(&mutex_);
  info->CopyFrom(*info_);
}

void TabletNodeSysInfo::GetTabletMetaList(TabletMetaList* meta_list) {
  MutexLock lock(&mutex_);
  meta_list->CopyFrom(*tablet_list_);
}

void TabletNodeSysInfo::SetServerAddr(const std::string& addr) {
  MutexLock lock(&mutex_);
  if (!info_.unique()) {
    SwitchInfo();
  }
  assert(info_.unique());
  info_->set_addr(addr);
}

void TabletNodeSysInfo::SetPersistentCacheSize(uint64_t size) {
  MutexLock lock(&mutex_);
  if (!info_.unique()) {
    SwitchInfo();
  }
  assert(info_.unique());
  info_->set_persistent_cache_size(size);
}

void TabletNodeSysInfo::SetStatus(StatusCode status) {
  MutexLock lock(&mutex_);
  if (!info_.unique()) {
    SwitchInfo();
  }
  assert(info_.unique());
  info_->set_status_t(status);
}

void TabletNodeSysInfo::DumpSysInfo(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                    const std::shared_ptr<CollectorReport>& latest_report,
                                    const TabletNodeSysInfoDumper& dumper) {
  double snappy_ratio = latest_report->FindMetricValue(kSnappyCompressionRatioMetric);
  if (snappy_ratio > 0) {
    snappy_ratio /= 100.0;
  }

  int64_t rawkey_compare_count = latest_report->FindMetricValue(kRawkeyCompareCountMetric);

  if (FLAGS_tera_tabletnode_dump_running_info) {
    dumper.DumpData("low_level", info_ptr->low_read_cell());
    dumper.DumpData("read", info_ptr->read_rows());
    dumper.DumpData("rspeed", info_ptr->read_size());
    dumper.DumpData("write", info_ptr->write_rows());
    dumper.DumpData("wspeed", info_ptr->write_size());
    dumper.DumpData("scan", info_ptr->scan_rows());
    dumper.DumpData("sspeed", info_ptr->scan_size());
    dumper.DumpData("snappy", snappy_ratio);
    dumper.DumpData("rowcomp", rawkey_compare_count);
  }

  LOG(INFO) << "[SysInfo]"
            << " low_level " << info_ptr->low_read_cell() << " read " << info_ptr->read_rows()
            << " rspeed " << utils::ConvertByteToString(info_ptr->read_size()) << " write "
            << info_ptr->write_rows() << " wspeed "
            << utils::ConvertByteToString(info_ptr->write_size()) << " scan "
            << info_ptr->scan_rows() << " sspeed "
            << utils::ConvertByteToString(info_ptr->scan_size()) << " snappy " << snappy_ratio
            << " rawcomp " << rawkey_compare_count;
}

void TabletNodeSysInfo::DumpHardWareInfo(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                         const std::shared_ptr<CollectorReport>& latest_report,
                                         const TabletNodeSysInfoDumper& dumper) {
  // hardware info
  if (FLAGS_tera_tabletnode_dump_running_info) {
    dumper.DumpData("mem_used", info_ptr->mem_used());
    dumper.DumpData("net_tx", info_ptr->net_tx());
    dumper.DumpData("net_rx", info_ptr->net_rx());
    dumper.DumpData("cpu_usage", info_ptr->cpu_usage());
  }

  LOG(INFO) << "[HardWare Info] "
            << " mem_used " << info_ptr->mem_used() << " "
            << utils::ConvertByteToString(info_ptr->mem_used()) << " net_tx " << info_ptr->net_tx()
            << " " << utils::ConvertByteToString(info_ptr->net_tx()) << " net_rx "
            << info_ptr->net_rx() << " " << utils::ConvertByteToString(info_ptr->net_rx())
            << " cpu_usage " << info_ptr->cpu_usage() << "%";
}

void TabletNodeSysInfo::DumpIoInfo(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                   const std::shared_ptr<CollectorReport>& latest_report,
                                   const TabletNodeSysInfoDumper& dumper) {
  int64_t ssd_read_count = latest_report->FindMetricValue(kSsdReadCountMetric);
  int64_t ssd_read_size = latest_report->FindMetricValue(kSsdReadThroughPutMetric);
  int64_t ssd_write_count = latest_report->FindMetricValue(kSsdWriteCountMetric);
  int64_t ssd_write_size = latest_report->FindMetricValue(kSsdWriteThroughPutMetric);

  if (FLAGS_tera_tabletnode_dump_running_info) {
    dumper.DumpData("dfs_r", info_ptr->dfs_io_r());
    dumper.DumpData("dfs_w", info_ptr->dfs_io_w());
    dumper.DumpData("local_r", info_ptr->local_io_r());
    dumper.DumpData("local_w", info_ptr->local_io_w());
    dumper.DumpData("ssd_r_counter", ssd_read_count);
    dumper.DumpData("ssd_r_size", ssd_read_size);
    dumper.DumpData("ssd_w_counter", ssd_write_count);
    dumper.DumpData("ssd_w_size", ssd_write_size);
  }

  LOG(INFO) << "[IO]"
            << " dfs_r " << info_ptr->dfs_io_r() << " "
            << utils::ConvertByteToString(info_ptr->dfs_io_r()) << " dfs_w " << info_ptr->dfs_io_w()
            << " " << utils::ConvertByteToString(info_ptr->dfs_io_w()) << " local_r "
            << info_ptr->local_io_r() << " " << utils::ConvertByteToString(info_ptr->local_io_r())
            << " local_w " << info_ptr->local_io_w() << " "
            << utils::ConvertByteToString(info_ptr->local_io_w()) << " ssd_r " << ssd_read_count
            << " " << utils::ConvertByteToString(ssd_read_size) << " ssd_w " << ssd_write_count
            << " " << utils::ConvertByteToString(ssd_write_size);
}

void TabletNodeSysInfo::DumpCacheInfo(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                      const std::shared_ptr<CollectorReport>& latest_report,
                                      const TabletNodeSysInfoDumper& dumper) {
  double block_cache_hitrate =
      static_cast<double>(latest_report->FindMetricValue(kBlockCacheHitRateMetric)) / 100.0;
  if (block_cache_hitrate < 0.0) {
    block_cache_hitrate = NAN;
  }
  int64_t block_cache_entries = latest_report->FindMetricValue(kBlockCacheEntriesMetric);
  int64_t block_cache_charge = latest_report->FindMetricValue(kBlockCacheChargeMetric);
  double table_cache_hitrate =
      static_cast<double>(latest_report->FindMetricValue(kTableCacheHitRateMetric)) / 100.0;
  if (table_cache_hitrate < 0.0) {
    table_cache_hitrate = NAN;
  }
  int64_t table_cache_entries = latest_report->FindMetricValue(kTableCacheEntriesMetric);
  int64_t table_cache_charge = latest_report->FindMetricValue(kTableCacheChargeMetric);
  if (FLAGS_tera_tabletnode_dump_running_info) {
    dumper.DumpData("block_cache_hitrate", block_cache_hitrate);
    dumper.DumpData("block_cache_entry", block_cache_entries);
    dumper.DumpData("block_cache_bytes", block_cache_charge);
    dumper.DumpData("table_cache_hitrate", table_cache_hitrate);
    dumper.DumpData("table_cache_entry", table_cache_entries);
    dumper.DumpData("table_cache_bytes", table_cache_charge);
  }
  LOG(INFO) << "[Cache HitRate/Cnt/Size] table_cache " << table_cache_hitrate << " "
            << table_cache_entries << " " << table_cache_charge << ", block_cache "
            << block_cache_hitrate << " " << block_cache_entries << " " << block_cache_charge;
}

void TabletNodeSysInfo::DumpRequestInfo(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                        const std::shared_ptr<CollectorReport>& latest_report,
                                        const TabletNodeSysInfoDumper& dumper) {
  auto interval = latest_report->interval_ms;

  int64_t finished_read_request =
      latest_report->FindMetricValue(kFinishedRequestCountMetric, kApiLabelRead);
  int64_t finished_write_request =
      latest_report->FindMetricValue(kFinishedRequestCountMetric, kApiLabelWrite);
  int64_t finished_scan_request =
      latest_report->FindMetricValue(kFinishedRequestCountMetric, kApiLabelScan);
  LOG(INFO) << "[Finished Requests] "
            << "read: " << finished_read_request * 1000 / interval
            << ", write: " << finished_write_request * 1000 / interval
            << ", scan: " << finished_scan_request * 1000 / interval;

  int64_t read_request_delay_avg =
      finished_read_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayMetric,
                                                                      kApiLabelRead) /
                                           finished_read_request;
  int64_t write_request_delay_avg =
      finished_write_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayMetric,
                                                                       kApiLabelWrite) /
                                            finished_write_request;
  int64_t scan_request_delay_avg =
      finished_scan_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayMetric,
                                                                      kApiLabelScan) /
                                           finished_scan_request;

  int64_t read_delay_percentile_95 =
      finished_read_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayPercentileMetric,
                                                                      kReadLabelPercentile95);
  int64_t read_delay_percentile_99 =
      finished_read_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayPercentileMetric,
                                                                      kReadLabelPercentile99);
  int64_t write_delay_percentile_95 =
      finished_write_request == 0 ? 0 : latest_report->FindMetricValue(
                                            kRequestDelayPercentileMetric, kWriteLabelPercentile95);
  int64_t write_delay_percentile_99 =
      finished_write_request == 0 ? 0 : latest_report->FindMetricValue(
                                            kRequestDelayPercentileMetric, kWriteLabelPercentile99);
  int64_t scan_delay_percentile_95 =
      finished_scan_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayPercentileMetric,
                                                                      kScanLabelPercentile95);
  int64_t scan_delay_percentile_99 =
      finished_scan_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayPercentileMetric,
                                                                      kScanLabelPercentile99);

  if (FLAGS_tera_tabletnode_dump_running_info) {
    dumper.DumpData("read_delay_avg", read_request_delay_avg);
    dumper.DumpData("read_delay_95", read_delay_percentile_95);
    dumper.DumpData("read_delay_99", read_delay_percentile_99);
    dumper.DumpData("write_delay_avg", write_request_delay_avg);
    dumper.DumpData("write_delay_95", write_delay_percentile_95);
    dumper.DumpData("write_delay_99", write_delay_percentile_99);
    dumper.DumpData("scan_delay_avg", scan_request_delay_avg);
    dumper.DumpData("scan_delay_95", scan_delay_percentile_95);
    dumper.DumpData("scan_delay_99", scan_delay_percentile_99);
  }

  LOG(INFO) << "[Requests Delay In Us] "
            << "Read [Avg: " << read_request_delay_avg
            << ", Percentile 95: " << read_delay_percentile_95
            << ", Percentile 99: " << read_delay_percentile_99
            << "]; Write [Avg: " << write_request_delay_avg
            << ", Percentile 95: " << write_delay_percentile_95
            << ", Percentile 99: " << write_delay_percentile_99
            << "]; Scan [Avg: " << scan_request_delay_avg
            << ", Percentile 95: " << scan_delay_percentile_95
            << ", Percentile 99: " << scan_delay_percentile_99 << "]";

  int64_t read_rows = latest_report->FindMetricValue(kRowCountMetric, kApiLabelRead);
  int64_t write_rows = latest_report->FindMetricValue(kRowCountMetric, kApiLabelWrite);
  int64_t scan_rows = latest_report->FindMetricValue(kRowCountMetric, kApiLabelScan);
  int64_t row_read_delay =
      (read_rows == 0 ? 0
                      : latest_report->FindMetricValue(kRowDelayMetric, kApiLabelRead) / read_rows);
  int64_t row_write_delay =
      (write_rows == 0 ? 0 : latest_report->FindMetricValue(kRowDelayMetric, kApiLabelWrite) /
                                 write_rows);
  int64_t row_scan_delay =
      (scan_rows == 0 ? 0
                      : latest_report->FindMetricValue(kRowDelayMetric, kApiLabelScan) / scan_rows);
  LOG(INFO) << "[Row Delay In Ms] "
            << "row_read_delay: " << row_read_delay / 1000.0
            << ", row_write_delay: " << row_write_delay / 1000.0
            << ", row_scan_delay: " << row_scan_delay / 1000.0;

  // extra info
  std::ostringstream ss;
  int cols = info_ptr->extra_info_size();
  ss << "[Pending] ";
  for (int i = 0; i < cols; ++i) {
    ss << info_ptr->extra_info(i).name() << " " << info_ptr->extra_info(i).value() << " ";
    if (FLAGS_tera_tabletnode_dump_running_info) {
      dumper.DumpData(info_ptr->extra_info(i).name(), info_ptr->extra_info(i).value());
    }
  }
  LOG(INFO) << ss.str();
}

void TabletNodeSysInfo::DumpDfsInfo(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                    const std::shared_ptr<CollectorReport>& latest_report,
                                    const TabletNodeSysInfoDumper& dumper) {
  int64_t dfs_read_delay = latest_report->FindMetricValue(kDfsReadDelayMetric);
  int64_t dfs_write_delay = latest_report->FindMetricValue(kDfsWriteDelayMetric);
  int64_t dfs_sync_delay = latest_report->FindMetricValue(kDfsSyncDelayMetric);
  int64_t dfs_read_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsReadLabel);
  int64_t dfs_write_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsWriteLabel);
  int64_t dfs_sync_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsSyncLabel);
  int64_t dfs_flush_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsFlushLabel);
  int64_t dfs_list_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsListLabel);
  int64_t dfs_other_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsOtherLabel);
  int64_t dfs_exists_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsExistsLabel);
  int64_t dfs_open_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsOpenLabel);
  int64_t dfs_close_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsCloseLabel);
  int64_t dfs_delete_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsDeleteLabel);
  int64_t dfs_tell_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsTellLabel);
  int64_t dfs_info_count = latest_report->FindMetricValue(kDfsRequestMetric, kDfsInfoLabel);
  int64_t dfs_read_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsReadLabel);
  int64_t dfs_write_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsWriteLabel);
  int64_t dfs_sync_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsSyncLabel);
  int64_t dfs_flush_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsFlushLabel);
  int64_t dfs_list_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsListLabel);
  int64_t dfs_other_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsOtherLabel);
  int64_t dfs_exists_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsExistsLabel);
  int64_t dfs_open_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsOpenLabel);
  int64_t dfs_close_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsCloseLabel);
  int64_t dfs_delete_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsDeleteLabel);
  int64_t dfs_tell_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsTellLabel);
  int64_t dfs_info_hang = latest_report->FindMetricValue(kDfsHangMetric, kDfsInfoLabel);
  double rdelay =
      dfs_read_count ? static_cast<double>(dfs_read_delay) / 1000.0 / dfs_read_count : 0;
  double wdelay =
      dfs_write_count ? static_cast<double>(dfs_write_delay) / 1000.0 / dfs_write_count : 0;
  double sdelay =
      dfs_sync_count ? static_cast<double>(dfs_sync_delay) / 1000.0 / dfs_sync_count : 0;

  if (FLAGS_tera_tabletnode_dump_running_info) {
    dumper.DumpData("dfs_read", dfs_read_count);
    dumper.DumpData("dfs_read_hang", dfs_read_hang);
    dumper.DumpData("dfs_rdealy", rdelay);
    dumper.DumpData("dfs_write", dfs_write_count);
    dumper.DumpData("dfs_write_hang", dfs_write_hang);
    dumper.DumpData("dfs_wdelay", wdelay);
    dumper.DumpData("dfs_sync", dfs_sync_count);
    dumper.DumpData("dfs_sync_hang", dfs_sync_hang);
    dumper.DumpData("dfs_sdelay", sdelay);
    dumper.DumpData("dfs_flush", dfs_flush_count);
    dumper.DumpData("dfs_flush_hang", dfs_flush_hang);
    dumper.DumpData("dfs_list", dfs_list_count);
    dumper.DumpData("dfs_list_hang", dfs_list_hang);
    dumper.DumpData("dfs_info", dfs_info_count);
    dumper.DumpData("dfs_info_hang", dfs_info_hang);
    dumper.DumpData("dfs_exists", dfs_exists_count);
    dumper.DumpData("dfs_exists_hang", dfs_exists_hang);
    dumper.DumpData("dfs_open", dfs_open_count);
    dumper.DumpData("dfs_open_hang", dfs_open_hang);
    dumper.DumpData("dfs_close", dfs_close_count);
    dumper.DumpData("dfs_close_hang", dfs_close_hang);
    dumper.DumpData("dfs_delete", dfs_delete_count);
    dumper.DumpData("dfs_delete_hang", dfs_delete_hang);
    dumper.DumpData("dfs_tell", dfs_tell_count);
    dumper.DumpData("dfs_tell_hang", dfs_tell_hang);
    dumper.DumpData("dfs_other", dfs_other_count);
    dumper.DumpData("dfs_other_hang", dfs_other_hang);
  }

  int64_t dfs_opened_read_files = latest_report->FindMetricValue(kDfsOpenedReadFilesCountMetric);
  int64_t dfs_opened_write_files = latest_report->FindMetricValue(kDfsOpenedWriteFilesCountMetric);

  if (FLAGS_tera_tabletnode_dump_running_info) {
    dumper.DumpData("dfs_opened_read_files_count", dfs_opened_read_files);
    dumper.DumpData("dfs_opened_write_files_count", dfs_opened_write_files);
  }

  LOG(INFO) << "[Dfs] read " << dfs_read_count << " " << dfs_read_hang << " "
            << "rdelay " << rdelay << " "
            << "rdelay_total " << dfs_read_delay << " "
            << "write " << dfs_write_count << " " << dfs_write_hang << " "
            << "wdelay " << wdelay << " "
            << "wdelay_total " << dfs_write_delay << " "
            << "sync " << dfs_sync_count << " " << dfs_sync_hang << " "
            << "sdelay " << sdelay << " "
            << "sdelay_total " << dfs_sync_delay << " "
            << "flush " << dfs_flush_count << " " << dfs_flush_hang << " "
            << "list " << dfs_list_count << " " << dfs_list_hang << " "
            << "info " << dfs_info_count << " " << dfs_info_hang << " "
            << "exists " << dfs_exists_count << " " << dfs_exists_hang << " "
            << "open " << dfs_open_count << " " << dfs_open_hang << " "
            << "close " << dfs_close_count << " " << dfs_close_hang << " "
            << "delete " << dfs_delete_count << " " << dfs_delete_hang << " "
            << "tell " << dfs_tell_count << " " << dfs_tell_hang << " "
            << "other " << dfs_other_count << " " << dfs_other_hang << " "
            << "opened: read " << dfs_opened_read_files << " "
            << "write " << dfs_opened_write_files;
}

void TabletNodeSysInfo::DumpPosixInfo(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                      const std::shared_ptr<CollectorReport>& latest_report,
                                      const TabletNodeSysInfoDumper& dumper) {
  int64_t posix_read_count = latest_report->FindMetricValue(kPosixReadCountMetric);
  int64_t posix_write_count = latest_report->FindMetricValue(kPosixWriteCountMetric);
  int64_t posix_sync_count = latest_report->FindMetricValue(kPosixSyncCountMetric);
  int64_t posix_list_count = latest_report->FindMetricValue(kPosixListCountMetric);
  int64_t posix_info_count = latest_report->FindMetricValue(kPosixInfoCountMetric);
  int64_t posix_exists_count = latest_report->FindMetricValue(kPosixExistsCountMetric);
  int64_t posix_open_count = latest_report->FindMetricValue(kPosixOpenCountMetric);
  int64_t posix_close_count = latest_report->FindMetricValue(kPosixCloseCountMetric);
  int64_t posix_delete_count = latest_report->FindMetricValue(kPosixDeleteCountMetric);
  int64_t posix_tell_count = latest_report->FindMetricValue(kPosixTellCountMetric);
  int64_t posix_seek_count = latest_report->FindMetricValue(kPosixSeekCountMetric);
  int64_t posix_other_count = latest_report->FindMetricValue(kPosixOtherCountMetric);

  if (FLAGS_tera_tabletnode_dump_running_info) {
    dumper.DumpData("local_read", posix_read_count);
    dumper.DumpData("local_write", posix_write_count);
    dumper.DumpData("local_sync", posix_sync_count);
    dumper.DumpData("local_list", posix_list_count);
    dumper.DumpData("local_info", posix_info_count);
    dumper.DumpData("local_exists", posix_exists_count);
    dumper.DumpData("local_open", posix_open_count);
    dumper.DumpData("local_close", posix_close_count);
    dumper.DumpData("local_delete", posix_delete_count);
    dumper.DumpData("local_tell", posix_tell_count);
    dumper.DumpData("local_seek", posix_seek_count);
    dumper.DumpData("local_other", posix_other_count);
  }

  LOG(INFO) << "[Local] read " << posix_read_count << " "
            << "write " << posix_write_count << " "
            << "sync " << posix_sync_count << " "
            << "list " << posix_list_count << " "
            << "info " << posix_info_count << " "
            << "exists " << posix_exists_count << " "
            << "open " << posix_open_count << " "
            << "close " << posix_close_count << " "
            << "delete " << posix_delete_count << " "
            << "tell " << posix_tell_count << " "
            << "seek " << posix_seek_count << " "
            << "other " << posix_other_count;
}

void TabletNodeSysInfo::DumpLevelSizeInfo(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                          const std::shared_ptr<CollectorReport>& latest_report,
                                          const TabletNodeSysInfoDumper& dumper) {
  if (FLAGS_tera_tabletnode_dump_level_size_info_enabled) {
    int64_t level0_size = latest_report->FindMetricValue(kLevelSize, "level:0");
    int64_t level1_size = latest_report->FindMetricValue(kLevelSize, "level:1");
    int64_t level2_size = latest_report->FindMetricValue(kLevelSize, "level:2");
    int64_t level3_size = latest_report->FindMetricValue(kLevelSize, "level:3");
    int64_t level4_size = latest_report->FindMetricValue(kLevelSize, "level:4");
    int64_t level5_size = latest_report->FindMetricValue(kLevelSize, "level:5");
    int64_t level6_size = latest_report->FindMetricValue(kLevelSize, "level:6");

    if (FLAGS_tera_tabletnode_dump_running_info) {
      dumper.DumpData("level0_size", level0_size);
      dumper.DumpData("level1_size", level1_size);
      dumper.DumpData("level2_size", level2_size);
      dumper.DumpData("level3_size", level3_size);
      dumper.DumpData("level4_size", level4_size);
      dumper.DumpData("level5_size", level5_size);
      dumper.DumpData("level6_size", level6_size);
    }

    LOG(INFO) << "[Level Size] L0: " << utils::ConvertByteToString(level0_size)
              << " , L1: " << utils::ConvertByteToString(level1_size)
              << " , L2: " << utils::ConvertByteToString(level2_size)
              << " , L3: " << utils::ConvertByteToString(level3_size)
              << " , L4: " << utils::ConvertByteToString(level4_size)
              << " , L5: " << utils::ConvertByteToString(level5_size)
              << " , L6: " << utils::ConvertByteToString(level6_size);
  }
}

void TabletNodeSysInfo::DumpPersistentCacheInfo(
    const std::shared_ptr<TabletNodeInfo>& info_ptr,
    const std::shared_ptr<CollectorReport>& latest_report, const TabletNodeSysInfoDumper& dumper) {
  if (FLAGS_tera_enable_persistent_cache && !tera::io::GetCachePaths().empty()) {
    auto& persistent_cache_paths = tera::io::GetPersistentCachePaths();
    int64_t write_count =
        latest_report->FindMetricValue(leveldb::PersistentCacheMetricNames::kWriteCount);
    int64_t write_throughput =
        latest_report->FindMetricValue(leveldb::PersistentCacheMetricNames::kWriteThroughput);
    int64_t read_throughput =
        latest_report->FindMetricValue(leveldb::PersistentCacheMetricNames::kReadThroughput);
    int64_t cache_hits =
        latest_report->FindMetricValue(leveldb::PersistentCacheMetricNames::kCacheHits);
    int64_t cache_misses =
        latest_report->FindMetricValue(leveldb::PersistentCacheMetricNames::kCacheMisses);
    int64_t cache_errors =
        latest_report->FindMetricValue(leveldb::PersistentCacheMetricNames::kCacheErrors);
    int64_t file_entries =
        latest_report->FindMetricValue(leveldb::PersistentCacheMetricNames::kFileEntries);
    int64_t cache_capacity{0};
    int64_t cache_size{0};
    int64_t meta_size{0};
    for (const auto& path : persistent_cache_paths) {
      cache_capacity += latest_report->FindMetricValue(
          leveldb::PersistentCacheMetricNames::kCacheCapacity, "path:" + path);
      cache_size += latest_report->FindMetricValue(leveldb::PersistentCacheMetricNames::kCacheSize,
                                                   "path:" + path);
      meta_size += latest_report->FindMetricValue(
          leveldb::PersistentCacheMetricNames::kMetaDataSize, "path:" + path);
    }
    double hit_pct = 0;
    double miss_pct = 0;
    if (cache_hits || cache_misses) {
      hit_pct = (double)cache_hits / (cache_hits + cache_misses);
      miss_pct = (double)cache_misses / (cache_hits + cache_misses);
    }
    LOG(INFO) << "[Persistent Cache] write_size: " << utils::ConvertByteToString(write_throughput)
              << ", write_count: " << write_count
              << ", read_size: " << utils::ConvertByteToString(read_throughput)
              << ", cache_hits: " << cache_hits << ", cache_misses: " << cache_misses
              << ", hit_percent: " << hit_pct * 100 << ", miss_percent: " << miss_pct * 100
              << ", cache_errors: " << cache_errors << ", file_entries: " << file_entries
              << ", cache_capacity: " << utils::ConvertByteToString(cache_capacity)
              << ", cache_size: " << utils::ConvertByteToString(cache_size)
              << ", metadata_size: " << utils::ConvertByteToString(meta_size);
  }
}

void TabletNodeSysInfo::DumpOtherInfo(const std::shared_ptr<TabletNodeInfo>& info_ptr,
                                      const std::shared_ptr<CollectorReport>& latest_report,
                                      const TabletNodeSysInfoDumper& dumper) {
  auto mem_table_size = latest_report->FindMetricValue(kMemTableSize);
  auto bloom_filter_size = latest_report->FindMetricValue("tera_filter_block_size");

  LOG(INFO) << "[Others] mem_table_size: " << utils::ConvertByteToString(mem_table_size)
            << ", bloom_filter_size: " << utils::ConvertByteToString(bloom_filter_size);

  if (FLAGS_tera_tabletnode_dump_running_info) {
    dumper.DumpData("mem_table_size", mem_table_size);
    dumper.DumpData("bloom_filter_size", bloom_filter_size);
  }
}

void TabletNodeSysInfo::DumpLog() {
  decltype(info_) info_ptr;
  {
    MutexLock lock(&mutex_);
    info_ptr = info_;
  }
  std::shared_ptr<CollectorReport> latest_report =
      CollectorReportPublisher::GetInstance().GetCollectorReport();
  TabletNodeSysInfoDumper dumper(FLAGS_tera_tabletnode_running_info_dump_file);

  std::for_each(std::begin(dump_info_functions_), std::end(dump_info_functions_),
                [&latest_report, &dumper, &info_ptr](std::function<DumpInfoFunction>& f) {
                  f(info_ptr, latest_report, dumper);
                });
}

void TabletNodeSysInfo::UpdateWriteFlowController() {
  MutexLock lock(&mutex_);
  TsWriteFlowController::Instance().Append(info_->timestamp() / 1000, info_->write_size());
}

}  // namespace tabletnode
}  // namespace tera
