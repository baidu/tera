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

#include "tabletnode/tabletnode_sysinfo.h"
#include "common/base/string_number.h"
#include "proto/proto_helper.h"
#include "tabletnode/tabletnode_metric_name.h"
#include "common/timer.h"
#include "utils/tprinter.h"
#include "utils/utils_cmd.h"
#include "common/metric/collector_report_publisher.h"
#include "common/metric/ratio_subscriber.h"
#include "common/metric/prometheus_subscriber.h"

DECLARE_bool(tera_tabletnode_dump_running_info);
DECLARE_string(tera_tabletnode_running_info_dump_file);
DECLARE_int64(tera_tabletnode_sysinfo_check_interval);

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

extern tera::Counter dfs_read_counter;
extern tera::Counter dfs_write_counter;
extern tera::Counter dfs_read_delay_counter;
extern tera::Counter dfs_write_delay_counter;
extern tera::Counter dfs_sync_delay_counter;
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

extern tera::Counter ssd_read_counter;
extern tera::Counter ssd_read_size_counter;
extern tera::Counter ssd_write_counter;
extern tera::Counter ssd_write_size_counter;
}


namespace tera {
namespace tabletnode {

// dfs metrics
tera::AutoCollectorRegister dfs_read_size_metric(kDfsReadBytesThroughPut,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_read_size_counter, true)), {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister dfs_write_size_metric(kDfsWriteBytesThroughPut,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_write_size_counter, true)), {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister dfs_read_delay_metric(kDfsReadDelayMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_read_delay_counter, true)), {});
tera::AutoCollectorRegister dfs_write_delay_metric(kDfsWriteDelayMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_write_delay_counter, true)), {});
tera::AutoCollectorRegister dfs_sync_delay_metric(kDfsSyncDelayMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_sync_delay_counter, true)), {});
tera::AutoCollectorRegister dfs_read_metric(kDfsReadCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_read_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_write_metric(kDfsWriteCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_write_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_sync_metric(kDfsSyncCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_sync_counter, true)), {SubscriberType::QPS});

tera::AutoSubscriberRegister dfs_read_delay_avg_subscriber (std::unique_ptr<Subscriber>(new RatioSubscriber(
    MetricId(kDfsReadDelayPerRequestMetric),
    std::unique_ptr<Subscriber>(new PrometheusSubscriber(MetricId(kDfsReadDelayMetric), SubscriberType::SUM)),
    std::unique_ptr<Subscriber>(new PrometheusSubscriber(MetricId(kDfsReadCountMetric), SubscriberType::SUM)))));

tera::AutoSubscriberRegister dfs_write_delay_avg_subscriber (std::unique_ptr<Subscriber>(new RatioSubscriber(
    MetricId(kDfsWriteDelayPerRequestMetric),
    std::unique_ptr<Subscriber>(new PrometheusSubscriber(MetricId(kDfsWriteDelayMetric), SubscriberType::SUM)),
    std::unique_ptr<Subscriber>(new PrometheusSubscriber(MetricId(kDfsWriteCountMetric), SubscriberType::SUM)))));

tera::AutoSubscriberRegister dfs_sync_delay_avg_subscriber (std::unique_ptr<Subscriber>(new RatioSubscriber(
    MetricId(kDfsSyncDelayPerRequestMetric),
    std::unique_ptr<Subscriber>(new PrometheusSubscriber(MetricId(kDfsSyncDelayMetric), SubscriberType::SUM)),
    std::unique_ptr<Subscriber>(new PrometheusSubscriber(MetricId(kDfsSyncCountMetric), SubscriberType::SUM)))));

tera::AutoCollectorRegister dfs_flush_metric(kDfsFlushCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_flush_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_list_metric(kDfsListCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_list_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_exists_metric(kDfsExistsCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_exists_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_open_metric(kDfsOpenCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_open_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_close_metric(kDfsCloseCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_close_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_delete_metric(kDfsDeleteCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_delete_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_tell_metric(kDfsTellCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_tell_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_info_metric(kDfsInfoCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_info_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_other_metric(kDfsOtherCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_other_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister dfs_read_hang_metric(kDfsReadHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_read_hang_counter, false)));
tera::AutoCollectorRegister dfs_write_hang_metric(kDfsWriteHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_write_hang_counter, false)));
tera::AutoCollectorRegister dfs_sync_hang_metric(kDfsSyncHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_sync_hang_counter, false)));
tera::AutoCollectorRegister dfs_flush_hang_metric(kDfsFlushHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_flush_hang_counter, false)));
tera::AutoCollectorRegister dfs_list_hang_metric(kDfsListHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_list_hang_counter, false)));
tera::AutoCollectorRegister dfs_exists_hang_metric(kDfsExistsHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_exists_hang_counter, false)));
tera::AutoCollectorRegister dfs_open_hang_metric(kDfsOpenHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_open_hang_counter, false)));
tera::AutoCollectorRegister dfs_close_hang_metric(kDfsCloseHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_close_hang_counter, false)));
tera::AutoCollectorRegister dfs_delete_hang_metric(kDfsDeleteHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_delete_hang_counter, false)));
tera::AutoCollectorRegister dfs_tell_hang_metric(kDfsTellHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_tell_hang_counter, false)));
tera::AutoCollectorRegister dfs_info_hang_metric(kDfsInfoHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_info_hang_counter, false)));
tera::AutoCollectorRegister dfs_other_hang_metric(kDfsOtherHangMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::dfs_other_hang_counter, false)));
// ssd metrics
tera::AutoCollectorRegister ssd_read_through_put_metric(kSsdReadThroughPutMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::ssd_read_size_counter, true)), {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister ssd_write_through_put_metric(kSsdWriteThroughPutMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::ssd_write_size_counter, true)), {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister ssd_read_metric(kSsdReadCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::ssd_read_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister ssd_write_metric(kSsdWriteCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::ssd_write_counter, true)), {SubscriberType::QPS});
// local metrics
tera::AutoCollectorRegister posix_read_size_metric(kPosixReadThroughPutMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_read_size_counter, true)), {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister posix_write_size_metric(kPosixWriteThroughPutMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_write_size_counter, true)), {SubscriberType::THROUGHPUT});
tera::AutoCollectorRegister posix_read_metric(kPosixReadCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_read_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_write_metric(kPosixWriteCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_write_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_sync_metric(kPosixSyncCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_sync_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_list_metric(kPosixListCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_list_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_exists_metric(kPosixExistsCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_exists_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_open_metric(kPosixOpenCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_open_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_close_metric(kPosixCloseCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_close_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_delete_metric(kPosixDeleteCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_delete_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_tell_metric(kPosixTellCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_tell_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_seek_metric(kPosixSeekCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_seek_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_info_metric(kPosixInfoCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_info_counter, true)), {SubscriberType::QPS});
tera::AutoCollectorRegister posix_other_metric(kPosixOtherCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::posix_other_counter, true)), {SubscriberType::QPS});

tera::AutoCollectorRegister rawkey_compare_metric(kRawkeyCompareCountMetric,
    std::unique_ptr<Collector>(new CounterCollector(&leveldb::rawkey_compare_counter, true)), {SubscriberType::QPS});

class TabletNodeSysInfoDumper {
public:
    TabletNodeSysInfoDumper(const std::string& filename) :
        filename_(filename), fp_(NULL) {

    }
    ~TabletNodeSysInfoDumper() {
        if (fp_) {
            fclose(fp_);
            fp_ = NULL;
        }
    }
    template<typename T>
    bool DumpData(const std::string& item_name, T data) {
        if (!fp_) {
            std::string dirname = filename_.substr(0, filename_.rfind('/'));
            int ret = mkdir(dirname.c_str(), 0755);
            if (ret != 0 && errno != EEXIST) {
                LOG(ERROR) << "fail to make dump dir " << dirname;
                return false;
            }
            fp_ = fopen(filename_.c_str(), "w");
            if (!fp_) {
                LOG(ERROR) << "fail to open dump file " << filename_;
                return false;
            }
        }
        std::stringstream ss;
        ss << item_name << " : " << data;
        fprintf(fp_, "%s\r\n", ss.str().c_str());
        return true;
    }
private:
    std::string filename_;
    FILE* fp_;
};

TabletNodeSysInfo::TabletNodeSysInfo() {
    last_check_ts_ = get_micros();
}

TabletNodeSysInfo::TabletNodeSysInfo(const TabletNodeInfo& info)
    : info_(info) {
    last_check_ts_ = get_micros();
}

TabletNodeSysInfo::~TabletNodeSysInfo() {}

void TabletNodeSysInfo::AddExtraInfo(const std::string& name, int64_t value) {
    MutexLock lock(&mutex_);
    ExtraTsInfo* e_info = info_.add_extra_info();
    e_info->set_name(name);
    e_info->set_value(value);
}

void TabletNodeSysInfo::SetProcessStartTime(int64_t ts) {
    MutexLock lock(&mutex_);
    info_.set_process_start_time(ts);
}

void TabletNodeSysInfo::SetTimeStamp(int64_t ts) {
    MutexLock lock(&mutex_);
    info_.set_timestamp(ts);
}

struct DBSize {
    uint64_t size;
    std::vector<uint64_t> lg_size;
};

void TabletNodeSysInfo::CollectTabletNodeInfo(TabletManager* tablet_manager,
                                              const string& server_addr) {
    std::vector<io::TabletIO*> tablet_ios;
    std::vector<TabletStatus> db_status_vec;
    std::vector<DBSize> db_size_vec;

    int64_t ts = get_micros();
    bool need_check = false;
    if (ts - last_check_ts_ > FLAGS_tera_tabletnode_sysinfo_check_interval) {
        last_check_ts_ = ts;
        need_check = true;
    }
    tablet_manager->GetAllTablets(&tablet_ios);
    std::vector<io::TabletIO*>::iterator it = tablet_ios.begin();
    while (it != tablet_ios.end()) {
        io::TabletIO* tablet_io = *it;
        if (tablet_io->ShouldForceUnloadOnError()) {
            LOG(WARNING) << *tablet_io << ", has internal error triggered unload";
            StatusCode status;
            if (!tablet_io->Unload(&status)) {
                LOG(ERROR) << *tablet_io << ", Unload tablet failed, status: "
                    << StatusCodeToString(status);
            }
            if (!tablet_manager->RemoveTablet(tablet_io->GetTableName(),
                        tablet_io->GetStartKey(), tablet_io->GetEndKey(), &status)) {
                LOG(ERROR) << *tablet_io << ", remove from TabletManager failed, status: "
                    << StatusCodeToString(status);
            }
            tablet_io->DecRef();
            it = tablet_ios.erase(it);
            continue;
        }

        // check db status whether is corruption
        TabletStatus tablet_status = static_cast<TabletStatus>(kTabletReady);
        tablet_io->GetDBStatus(&tablet_status, need_check);
        db_status_vec.push_back(tablet_status);

        DBSize db_size;
        tablet_io->GetDataSize(&db_size.size, &db_size.lg_size);
        db_size_vec.push_back(db_size);

        ++it;
    }

    MutexLock lock(&mutex_);
    std::shared_ptr<CollectorReport> latest_report = CollectorReportPublisher::GetInstance().GetCollectorReport();
    int64_t interval = latest_report->interval_ms;
    if (interval <= 0) {
        // maybe happen at first report, the metric values must be 0
        // set to any non-zero value to avoid div 0
        VLOG(16) << "Metric Report interval is 0";
        interval = 1000;
    }

    tablet_list_.Clear();
    int64_t total_size = 0;
    int64_t scan_kvs = 0;
    int64_t read_kvs = 0;
    int64_t write_kvs = 0;
    int64_t busy_cnt = 0;
    int64_t db_corruption_cnt = 0;

    for (uint32_t i = 0; i < tablet_ios.size(); i++) {
        io::TabletIO* tablet_io = tablet_ios[i];
        TabletStatus tablet_status = db_status_vec[i];
        DBSize db_size = db_size_vec[i];

        TabletMeta* tablet_meta = tablet_list_.add_meta();
        tablet_meta->set_status(TabletStatus(tablet_io->GetStatus()));
        tablet_meta->set_server_addr(server_addr);
        tablet_meta->set_table_name(tablet_io->GetTableName());
        tablet_meta->set_path(tablet_io->GetTablePath());
        tablet_meta->mutable_key_range()->set_key_start(tablet_io->GetStartKey());
        tablet_meta->mutable_key_range()->set_key_end(tablet_io->GetEndKey());

        tablet_meta->set_size(db_size.size);
        for (size_t i = 0; i < db_size.lg_size.size(); ++i) {
            tablet_meta->add_lg_size(db_size.lg_size[i]);
        }
        tablet_meta->set_compact_status(tablet_io->GetCompactStatus());
        total_size += tablet_meta->size();

        TabletCounter* counter = tablet_list_.add_counter();
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
        counter->set_db_status(tablet_status); // set runtime counter

        scan_kvs += counter->scan_kvs();
        read_kvs += counter->read_kvs();
        write_kvs += counter->write_kvs();

        if (counter->is_on_busy()) {
            busy_cnt++;
        }
        if (counter->db_status() == kTabletCorruption) {
            db_corruption_cnt++;
        }
        tablet_io->DecRef();
    }

    int64_t low_read_cell =
        latest_report->FindMetricValue(kLowLevelReadMetric);
    int64_t read_rows =
        latest_report->FindMetricValue(kRowCountMetric, kApiLabelRead);
    int64_t read_size =
        latest_report->FindMetricValue(kRowThroughPutMetric, kApiLabelRead);
    int64_t write_rows =
        latest_report->FindMetricValue(kRowCountMetric, kApiLabelWrite);
    int64_t write_size =
        latest_report->FindMetricValue(kRowThroughPutMetric, kApiLabelWrite);
    int64_t scan_rows =
        latest_report->FindMetricValue(kRowCountMetric, kApiLabelScan);
    int64_t scan_size =
        latest_report->FindMetricValue(kRowThroughPutMetric, kApiLabelScan);

    info_.set_low_read_cell(low_read_cell * 1000 / interval);
    info_.set_scan_rows(scan_rows * 1000 / interval);
    info_.set_scan_kvs(scan_kvs * 1000 / interval);
    info_.set_scan_size(scan_size * 1000 / interval);
    info_.set_read_rows(read_rows * 1000 / interval);
    info_.set_read_kvs(read_kvs * 1000 / interval);
    info_.set_read_size(read_size * 1000 / interval);
    info_.set_write_rows(write_rows * 1000 / interval);
    info_.set_write_kvs(write_kvs * 1000 / interval);
    info_.set_write_size(write_size * 1000 / interval);
    info_.set_tablet_onbusy(busy_cnt);
    info_.set_tablet_corruption(db_corruption_cnt);

    // refresh tabletnodeinfo
    info_.set_load(total_size);
    info_.set_tablet_total(tablet_ios.size());

    int64_t tmp;
    tmp = latest_report->FindMetricValue(kDfsReadBytesThroughPut) * 1000 / interval;
    info_.set_dfs_io_r(tmp);
    tmp = latest_report->FindMetricValue(kDfsWriteBytesThroughPut) * 1000 / interval;
    info_.set_dfs_io_w(tmp);
    tmp = latest_report->FindMetricValue(kPosixReadThroughPutMetric) * 1000 / interval;
    info_.set_local_io_r(tmp);
    tmp = latest_report->FindMetricValue(kPosixWriteThroughPutMetric) * 1000 / interval;
    info_.set_local_io_w(tmp);

    int64_t read_pending = latest_report->FindMetricValue(kPendingCountMetric, kApiLabelRead);
    int64_t write_pending = latest_report->FindMetricValue(kPendingCountMetric, kApiLabelWrite);
    int64_t scan_pending = latest_report->FindMetricValue(kPendingCountMetric, kApiLabelScan);
    int64_t compact_pending = latest_report->FindMetricValue(kPendingCountMetric, kApiLabelCompact);

    info_.set_read_pending(read_pending);
    info_.set_write_pending(write_pending);
    info_.set_scan_pending(scan_pending);

    // collect extra infos
    info_.clear_extra_info();
    ExtraTsInfo* einfo = info_.add_extra_info();

    int64_t range_error_sum =
            latest_report->FindMetricValue(kRangeErrorMetric, kApiLabelRead) +
            latest_report->FindMetricValue(kRangeErrorMetric, kApiLabelWrite) +
            latest_report->FindMetricValue(kRangeErrorMetric, kApiLabelScan);

    tmp = range_error_sum * 1000 / interval;
    einfo->set_name("range_error");
    einfo->set_value(tmp);

    einfo = info_.add_extra_info();
    einfo->set_name("read_pending");
    einfo->set_value(read_pending);

    einfo = info_.add_extra_info();
    einfo->set_name("write_pending");
    einfo->set_value(write_pending);

    einfo = info_.add_extra_info();
    einfo->set_name("scan_pending");
    einfo->set_value(scan_pending);

    einfo = info_.add_extra_info();
    einfo->set_name("compact_pending");
    einfo->set_value(compact_pending);

    einfo = info_.add_extra_info();
    tmp = latest_report->FindMetricValue(kRejectCountMetric, kApiLabelRead) * 1000 / interval;
    einfo->set_name("read_reject");
    einfo->set_value(tmp);

    einfo = info_.add_extra_info();
    tmp = latest_report->FindMetricValue(kRejectCountMetric, kApiLabelWrite) * 1000 / interval;
    einfo->set_name("write_reject");
    einfo->set_value(tmp);

    einfo = info_.add_extra_info();
    tmp = latest_report->FindMetricValue(kRejectCountMetric, kApiLabelScan) * 1000 / interval;
    einfo->set_name("scan_reject");
    einfo->set_value(tmp);

    einfo = info_.add_extra_info();
    tmp = latest_report->FindMetricValue(kRequestCountMetric, kApiLabelRead) * 1000 / interval;
    einfo->set_name("read_request");
    einfo->set_value(tmp);

    einfo = info_.add_extra_info();
    tmp = latest_report->FindMetricValue(kRequestCountMetric, kApiLabelWrite) * 1000 / interval;
    einfo->set_name("write_request");
    einfo->set_value(tmp);

    einfo = info_.add_extra_info();
    tmp = latest_report->FindMetricValue(kRequestCountMetric, kApiLabelScan) * 1000 / interval;
    einfo->set_name("scan_request");
    einfo->set_value(tmp);

    einfo = info_.add_extra_info();
    tmp = latest_report->FindMetricValue(kErrorCountMetric, kApiLabelRead) * 1000 / interval;
    einfo->set_name("read_error");
    einfo->set_value(tmp);

    einfo = info_.add_extra_info();
    tmp = latest_report->FindMetricValue(kErrorCountMetric, kApiLabelWrite) * 1000 / interval;
    einfo->set_name("write_error");
    einfo->set_value(tmp);

    einfo = info_.add_extra_info();
    tmp = latest_report->FindMetricValue(kErrorCountMetric, kApiLabelScan) * 1000 / interval;
    einfo->set_name("scan_error");
    einfo->set_value(tmp);
}

void TabletNodeSysInfo::CollectHardwareInfo() {
    MutexLock lock(&mutex_);
    std::shared_ptr<CollectorReport> latest_report = CollectorReportPublisher::GetInstance().GetCollectorReport();

    int64_t cpu_usage = latest_report->FindMetricValue(kInstCpuMetricName);
    info_.set_cpu_usage(static_cast<float>(cpu_usage));

    int64_t mem_usage = latest_report->FindMetricValue(kInstMemMetricName);
    info_.set_mem_used(mem_usage);

    int64_t net_rx_usage = latest_report->FindMetricValue(kInstNetRXMetricName);
    info_.set_net_rx(net_rx_usage);

    int64_t net_tx_usage = latest_report->FindMetricValue(kInstNetTXMetricName);
    info_.set_net_tx(net_tx_usage);
}

void TabletNodeSysInfo::GetTabletNodeInfo(TabletNodeInfo* info) {
    MutexLock lock(&mutex_);
    info->CopyFrom(info_);
}

void TabletNodeSysInfo::GetTabletMetaList(TabletMetaList* meta_list) {
    MutexLock lock(&mutex_);
    meta_list->CopyFrom(tablet_list_);
}

void TabletNodeSysInfo::SetServerAddr(const std::string& addr) {
    MutexLock lock(&mutex_);
    info_.set_addr(addr);
}

void TabletNodeSysInfo::SetStatus(StatusCode status) {
    MutexLock lock(&mutex_);
    info_.set_status_t(status);
}

void TabletNodeSysInfo::DumpLog() {
    MutexLock lock(&mutex_);
    std::shared_ptr<CollectorReport> latest_report = CollectorReportPublisher::GetInstance().GetCollectorReport();
    int64_t interval = latest_report->interval_ms;
    
    TabletNodeSysInfoDumper dumper(FLAGS_tera_tabletnode_running_info_dump_file);

    double snappy_ratio = latest_report->FindMetricValue(kSnappyCompressionRatioMetric);
    if (snappy_ratio > 0) {
        snappy_ratio /= 100.0;
    }

    int64_t rawkey_compare_count = latest_report->FindMetricValue(kRawkeyCompareCountMetric);

    if (FLAGS_tera_tabletnode_dump_running_info) {
        dumper.DumpData("low_level", info_.low_read_cell());
        dumper.DumpData("read", info_.read_rows());
        dumper.DumpData("rspeed", info_.read_size());
        dumper.DumpData("write", info_.write_rows());
        dumper.DumpData("wspeed", info_.write_size());
        dumper.DumpData("scan", info_.scan_rows());
        dumper.DumpData("sspeed", info_.scan_size());
        dumper.DumpData("snappy", snappy_ratio);
        dumper.DumpData("rowcomp", rawkey_compare_count);
    }

    LOG(INFO) << "[SysInfo]"
        << " low_level " << info_.low_read_cell()
        << " read " << info_.read_rows()
        << " rspeed " << utils::ConvertByteToString(info_.read_size())
        << " write " << info_.write_rows()
        << " wspeed " << utils::ConvertByteToString(info_.write_size())
        << " scan " << info_.scan_rows()
        << " sspeed " << utils::ConvertByteToString(info_.scan_size())
        << " snappy " << snappy_ratio
        << " rawcomp " << rawkey_compare_count;

    // hardware info
    if (FLAGS_tera_tabletnode_dump_running_info) {
        dumper.DumpData("mem_used", info_.mem_used());
        dumper.DumpData("net_tx", info_.net_tx());
        dumper.DumpData("net_rx", info_.net_rx());
        dumper.DumpData("cpu_usage", info_.cpu_usage());
    }

    LOG(INFO) << "[HardWare Info] "
        << " mem_used " << info_.mem_used() << " "
        << utils::ConvertByteToString(info_.mem_used())
        << " net_tx " << info_.net_tx() << " "
        << utils::ConvertByteToString(info_.net_tx())
        << " net_rx " << info_.net_rx() << " "
        << utils::ConvertByteToString(info_.net_rx())
        << " cpu_usage " << info_.cpu_usage() << "%";

    // net and io info
    int64_t ssd_read_count = latest_report->FindMetricValue(kSsdReadCountMetric);
    int64_t ssd_read_size = latest_report->FindMetricValue(kSsdReadThroughPutMetric);
    int64_t ssd_write_count = latest_report->FindMetricValue(kSsdWriteCountMetric);
    int64_t ssd_write_size = latest_report->FindMetricValue(kSsdWriteThroughPutMetric);
    if (FLAGS_tera_tabletnode_dump_running_info) {
        dumper.DumpData("dfs_r", info_.dfs_io_r());
        dumper.DumpData("dfs_w", info_.dfs_io_w());
        dumper.DumpData("local_r", info_.local_io_r());
        dumper.DumpData("local_w", info_.local_io_w());
        dumper.DumpData("ssd_r_counter", ssd_read_count);
        dumper.DumpData("ssd_r_size", ssd_read_size);
        dumper.DumpData("ssd_w_counter", ssd_write_count);
        dumper.DumpData("ssd_w_size", ssd_write_size);
    }

    LOG(INFO) << "[IO]"
        << " dfs_r " << info_.dfs_io_r() << " "
        << utils::ConvertByteToString(info_.dfs_io_r())
        << " dfs_w " << info_.dfs_io_w() << " "
        << utils::ConvertByteToString(info_.dfs_io_w())
        << " local_r " << info_.local_io_r() << " "
        << utils::ConvertByteToString(info_.local_io_r())
        << " local_w " << info_.local_io_w() << " "
        << utils::ConvertByteToString(info_.local_io_w())
        << " ssd_r " << ssd_read_count << " "
        << utils::ConvertByteToString(ssd_read_size)
        << " ssd_w " << ssd_write_count << " "
        << utils::ConvertByteToString(ssd_write_size);

    // cache info
    double block_cache_hitrate = static_cast<double>(latest_report->FindMetricValue(kBlockCacheHitRateMetric)) / 100.0;
    if (block_cache_hitrate < 0.0) {
        block_cache_hitrate = NAN;
    }
    int64_t block_cache_entries = latest_report->FindMetricValue(kBlockCacheEntriesMetric);
    int64_t block_cache_charge = latest_report->FindMetricValue(kBlockCacheChargeMetric);
    double table_cache_hitrate = static_cast<double>(latest_report->FindMetricValue(kTableCacheHitRateMetric)) / 100.0;
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
    LOG(INFO) << "[Cache HitRate/Cnt/Size] table_cache "
              << table_cache_hitrate << " "
              << table_cache_entries << " "
              << table_cache_charge
              << ", block_cache "
              << block_cache_hitrate << " "
              << block_cache_entries << " "
              << block_cache_charge;
    
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

    int64_t read_request_delay =
        (finished_read_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayMetric, kApiLabelRead) / finished_read_request);
    int64_t write_request_delay =
        (finished_write_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayMetric, kApiLabelWrite) / finished_write_request);
    int64_t scan_request_delay =
        (finished_scan_request == 0 ? 0 : latest_report->FindMetricValue(kRequestDelayMetric, kApiLabelScan) / finished_scan_request);
    LOG(INFO) << "[Requests Delay In Ms] "
              << "read: " << read_request_delay / 1000.0
              << ", write: " << write_request_delay / 1000.0
              << ", scan: " << scan_request_delay / 1000.0;

    int64_t read_rows =
        latest_report->FindMetricValue(kRowCountMetric, kApiLabelRead);
    int64_t write_rows =
        latest_report->FindMetricValue(kRowCountMetric, kApiLabelWrite);
    int64_t scan_rows =
        latest_report->FindMetricValue(kRowCountMetric, kApiLabelScan);
    int64_t row_read_delay = 
        (read_rows == 0 ? 0 : latest_report->FindMetricValue(kRowDelayMetric, kApiLabelRead) / read_rows);
    int64_t row_write_delay = 
        (write_rows == 0 ? 0 : latest_report->FindMetricValue(kRowDelayMetric, kApiLabelWrite) / write_rows);
    int64_t row_scan_delay = 
        (scan_rows == 0 ? 0 : latest_report->FindMetricValue(kRowDelayMetric, kApiLabelScan) / scan_rows);
    LOG(INFO) << "[Row Delay In Ms] "
              << "row_read_delay: " << row_read_delay / 1000.0
              << ", row_write_delay: " << row_write_delay / 1000.0
              << ", row_scan_delay: " << row_scan_delay / 1000.0;

    // extra info
    std::ostringstream ss;
    int cols = info_.extra_info_size();
    ss << "[Pending] ";
    for (int i = 0; i < cols; ++i) {
        ss << info_.extra_info(i).name() << " " << info_.extra_info(i).value() << " ";
        if (FLAGS_tera_tabletnode_dump_running_info) {
            dumper.DumpData(info_.extra_info(i).name(), info_.extra_info(i).value());
        }
    }
    LOG(INFO) << ss.str();

    // DFS info
    int64_t dfs_read_delay = latest_report->FindMetricValue(kDfsReadDelayMetric);
    int64_t dfs_write_delay = latest_report->FindMetricValue(kDfsWriteDelayMetric);
    int64_t dfs_sync_delay = latest_report->FindMetricValue(kDfsSyncDelayMetric);
    int64_t dfs_read_count = latest_report->FindMetricValue(kDfsReadCountMetric);
    int64_t dfs_write_count = latest_report->FindMetricValue(kDfsWriteCountMetric);
    int64_t dfs_sync_count = latest_report->FindMetricValue(kDfsSyncCountMetric);
    int64_t dfs_flush_count = latest_report->FindMetricValue(kDfsFlushCountMetric);
    int64_t dfs_list_count = latest_report->FindMetricValue(kDfsListCountMetric);
    int64_t dfs_other_count = latest_report->FindMetricValue(kDfsOtherCountMetric);
    int64_t dfs_exists_count = latest_report->FindMetricValue(kDfsExistsCountMetric);
    int64_t dfs_open_count = latest_report->FindMetricValue(kDfsOpenCountMetric);
    int64_t dfs_close_count = latest_report->FindMetricValue(kDfsCloseCountMetric);
    int64_t dfs_delete_count = latest_report->FindMetricValue(kDfsDeleteCountMetric);
    int64_t dfs_tell_count = latest_report->FindMetricValue(kDfsTellCountMetric);
    int64_t dfs_info_count = latest_report->FindMetricValue(kDfsInfoCountMetric);
    int64_t dfs_read_hang = latest_report->FindMetricValue(kDfsReadHangMetric);
    int64_t dfs_write_hang = latest_report->FindMetricValue(kDfsWriteHangMetric);
    int64_t dfs_sync_hang = latest_report->FindMetricValue(kDfsSyncHangMetric);
    int64_t dfs_flush_hang = latest_report->FindMetricValue(kDfsFlushHangMetric);
    int64_t dfs_list_hang = latest_report->FindMetricValue(kDfsListHangMetric);
    int64_t dfs_other_hang = latest_report->FindMetricValue(kDfsOtherHangMetric);
    int64_t dfs_exists_hang = latest_report->FindMetricValue(kDfsExistsHangMetric);
    int64_t dfs_open_hang = latest_report->FindMetricValue(kDfsOpenHangMetric);
    int64_t dfs_close_hang = latest_report->FindMetricValue(kDfsCloseHangMetric);
    int64_t dfs_delete_hang = latest_report->FindMetricValue(kDfsDeleteHangMetric);
    int64_t dfs_tell_hang = latest_report->FindMetricValue(kDfsTellHangMetric);
    int64_t dfs_info_hang = latest_report->FindMetricValue(kDfsInfoHangMetric);
    double rdelay = dfs_read_count ? static_cast<double>(dfs_read_delay) / 1000.0 / dfs_read_count : 0;
    double wdelay = dfs_write_count ? static_cast<double>(dfs_write_delay) / 1000.0 / dfs_write_count : 0;
    double sdelay = dfs_sync_count ? static_cast<double>(dfs_sync_delay) / 1000.0 / dfs_sync_count : 0;

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

    LOG(INFO) << "[Dfs] read " << dfs_read_count << " "
        << dfs_read_hang << " "
        << "rdelay " << rdelay << " "
        << "rdelay_total " << dfs_read_delay << " "
        << "write " << dfs_write_count << " "
        << dfs_write_hang << " "
        << "wdelay " << wdelay << " "
        << "wdelay_total " << dfs_write_delay << " "
        << "sync " << dfs_sync_count << " "
        << dfs_sync_hang << " "
        << "sdelay " << sdelay << " "
        << "sdelay_total " << dfs_sync_delay << " "
        << "flush " << dfs_flush_count << " "
        << dfs_flush_hang << " "
        << "list " << dfs_list_count << " "
        << dfs_list_hang << " "
        << "info " << dfs_info_count << " "
        << dfs_info_hang << " "
        << "exists " << dfs_exists_count << " "
        << dfs_exists_hang << " "
        << "open " << dfs_open_count << " "
        << dfs_open_hang << " "
        << "close " << dfs_close_count << " "
        << dfs_close_hang << " "
        << "delete " << dfs_delete_count << " "
        << dfs_delete_hang << " "
        << "tell " << dfs_tell_count << " "
        << dfs_tell_hang << " "
        << "other " << dfs_other_count << " "
        << dfs_other_hang;

    // local info
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

} // namespace tabletnode
} // namespace tera
