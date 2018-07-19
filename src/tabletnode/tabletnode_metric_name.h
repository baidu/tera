// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLETNODE_METRIC_NAME_H_
#define TERA_TABLETNODE_TABLETNODE_METRIC_NAME_H_

#include <string>

#include "common/metric/hardware_collectors.h"

namespace tera {
namespace tabletnode {

// api labels
const char* const kApiLabelRead = "api:read";
const char* const kApiLabelWrite = "api:write";
const char* const kApiLabelScan = "api:scan";
const char* const kApiLabelCompact = "api:compact";

// env lables
const char* const kEnvLabelDfs = "env:dfs";
const char* const kEnvLabelSsd = "env:ssd";
const char* const kEnvLabelPosix = "env:posix";
const char* const kEnvLabelOther = "env:other";

// metric names
const char* const kRequestCountMetric = "tera_ts_request_count";
const char* const kPendingCountMetric = "tera_ts_pending_count";
const char* const kRejectCountMetric = "tera_ts_reject_count";
const char* const kErrorCountMetric = "tera_ts_error_count";
const char* const kRangeErrorMetric = "tera_ts_range_error_count";

const char* const kRowDelayMetric = "tera_ts_row_delay_us_total";
const char* const kRowCountMetric = "tera_ts_row_count";
const char* const kRowThroughPutMetric = "tera_ts_row_through_put";
const char* const kLowLevelReadMetric = "tera_ts_low_level_read";
const char* const kScanDropCountMetric = "tera_ts_scan_drop_count";

const char* const kRequestDelayMetric = "tera_ts_request_delay_us_total";
const char* const kFinishedRequestCountMetric = "tera_ts_finished_request_count";

const char* const kLevelSize = "tera_ts_level_size_counter";
const char* const kBatchScanCountMetric = "tera_ts_batch_scan_count";
const char* const kSyncScanCountMetric = "tera_ts_sync_scan_count";

const char* const kFlushToDiskDelayMetric = "tera_ts_flush_to_disk_delay";
const char* const kFlushCheck = "flush:check";
const char* const kFlushBatch = "flush:batch";
const char* const kFlushWrite = "flush:write";
const char* const kFlushFinish = "flush:finish";

const char* const kRequestDelayAvgMetric = "tera_ts_request_delay_us_avg";
const char* const kRequestDelayPercentileMetric = "tera_ts_request_delay_percentile";

const char* const kWriteLabelPercentile95 = "api:write,percentile:95";
const char* const kWriteLabelPercentile99 = "api:write,percentile:99";
const char* const kReadLabelPercentile95 = "api:read,percentile:95";
const char* const kReadLabelPercentile99 = "api:read,percentile:99";
const char* const kScanLabelPercentile95 = "api:scan,percentile:95";
const char* const kScanLabelPercentile99 = "api:scan,percentile:99";

// cache metric names
const char* const kBlockCacheHitRateMetric = "tera_ts_block_cache_hit_percentage";
const char* const kBlockCacheEntriesMetric = "tera_ts_block_cache_entry_count";
const char* const kBlockCacheChargeMetric = "tera_ts_block_cache_charge_bytes";

const char* const kTableCacheHitRateMetric = "tera_ts_table_cache_hit_percentage";
const char* const kTableCacheEntriesMetric = "tera_ts_table_cache_entry_count";
const char* const kTableCacheChargeMetric = "tera_ts_table_cache_charge_bytes";

// env metric names
const char* const kDfsReadBytesThroughPut = "tera_ts_dfs_read_bytes_through_put";
const char* const kDfsWriteBytesThroughPut = "tera_ts_dfs_write_bytes_through_put";

const char* const kDfsReadDelayMetric = "tera_ts_dfs_read_delay_us_total";
const char* const kDfsWriteDelayMetric = "tera_ts_dfs_write_delay_us_total";
const char* const kDfsSyncDelayMetric = "tera_ts_dfs_sync_delay_us_total";

const char* const kDfsReadDelayPerRequestMetric = "tera_ts_dfs_read_delay_us_per_request";
const char* const kDfsWriteDelayPerRequestMetric = "tera_ts_dfs_write_delay_us_per_request";
const char* const kDfsSyncDelayPerRequestMetric = "tera_ts_dfs_sync_delay_us_per_request";

const char* const kDfsReadLabel = "operate:read";
const char* const kDfsWriteLabel = "operate:write";
const char* const kDfsSyncLabel = "operate:sync";
const char* const kDfsFlushLabel = "operate:flush";
const char* const kDfsListLabel = "operate:list";
const char* const kDfsOtherLabel = "operate:other";
const char* const kDfsExistsLabel = "operate:exists";
const char* const kDfsOpenLabel = "operate:open";
const char* const kDfsCloseLabel = "operate:close";
const char* const kDfsDeleteLabel = "operate:delete";
const char* const kDfsTellLabel = "operate:tell";
const char* const kDfsInfoLabel = "operate:info";

const char* const kDfsHangMetric = "tera_ts_dfs_hang_count";
const char* const kDfsRequestMetric = "tera_ts_dfs_request_count";
const char* const kDfsErrorMetric = "tera_ts_dfs_error_count";

const char* const kDfsOpenedReadFilesCountMetric = "tera_ts_dfs_opened_read_files";
const char* const kDfsOpenedWriteFilesCountMetric = "tera_ts_dfs_opened_write_files";

const char* const kSsdReadCountMetric = "tera_ts_ssd_read_count";
const char* const kSsdReadThroughPutMetric = "tera_ts_ssd_read_through_put";
const char* const kSsdWriteCountMetric = "tera_ts_ssd_write_count";
const char* const kSsdWriteThroughPutMetric = "tera_ts_ssd_write_through_put";

const char* const kPosixReadThroughPutMetric = "tera_ts_posix_read_through_put";
const char* const kPosixWriteThroughPutMetric = "tera_ts_posix_write_through_put";
const char* const kPosixReadCountMetric = "tera_ts_posix_read_count";
const char* const kPosixWriteCountMetric = "tera_ts_posix_write_count";
const char* const kPosixSyncCountMetric = "tera_ts_posix_sync_count";
const char* const kPosixListCountMetric = "tera_ts_posix_list_count";
const char* const kPosixExistsCountMetric = "tera_ts_posix_exists_count";
const char* const kPosixOpenCountMetric = "tera_ts_posix_open_count";
const char* const kPosixCloseCountMetric = "tera_ts_posix_close_count";
const char* const kPosixDeleteCountMetric = "tera_ts_posix_delete_count";
const char* const kPosixTellCountMetric = "tera_ts_posix_tell_count";
const char* const kPosixSeekCountMetric = "tera_ts_posix_seek_count";
const char* const kPosixInfoCountMetric = "tera_ts_posix_info_count";
const char* const kPosixOtherCountMetric = "tera_ts_posix_other_count";

const char* const kRawkeyCompareCountMetric = "tera_ts_rawkey_compare_count";
const char* const kSnappyCompressionRatioMetric = "tera_ts_snappy_compression_percentage";

const char* const kNotReadyCountMetric = "tera_ts_not_ready_count";
const char* const kTabletSizeCounter = "tera_ts_tablet_size_count";
const char* const kTabletNumCounter = "tera_ts_tablet_num_count";
} // end namespace tabletnode
} // end namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_METRIC_NAME_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

