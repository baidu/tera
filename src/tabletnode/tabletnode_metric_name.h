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

const char* const kRequestDelayMetric = "tera_ts_request_delay_us_total";
const char* const kFinishedRequestCountMetric = "tera_ts_finished_request_count";

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
const char* const kDfsReadCountMetric = "tera_ts_dfs_read_count";
const char* const kDfsWriteCountMetric = "tera_ts_dfs_write_count";
const char* const kDfsSyncCountMetric = "tera_ts_dfs_sync_count";
const char* const kDfsReadDelayPerRequestMetric = "tera_ts_dfs_read_delay_us_per_request";
const char* const kDfsWriteDelayPerRequestMetric = "tera_ts_dfs_write_delay_us_per_request";
const char* const kDfsSyncDelayPerRequestMetric = "tera_ts_dfs_sync_delay_us_per_request";
const char* const kDfsFlushCountMetric = "tera_ts_dfs_flush_count";
const char* const kDfsListCountMetric = "tera_ts_dfs_list_count";
const char* const kDfsOtherCountMetric = "tera_ts_dfs_other_count";
const char* const kDfsExistsCountMetric = "tera_ts_dfs_exists_count";
const char* const kDfsOpenCountMetric = "tera_ts_dfs_open_count";
const char* const kDfsCloseCountMetric = "tera_ts_dfs_close_count";
const char* const kDfsDeleteCountMetric = "tera_ts_dfs_delete_count";
const char* const kDfsTellCountMetric = "tera_ts_dfs_tell_count";
const char* const kDfsInfoCountMetric = "tera_ts_dfs_info_count";
const char* const kDfsReadHangMetric = "tera_ts_dfs_read_hang_total";
const char* const kDfsWriteHangMetric = "tera_ts_dfs_write_hang_total";
const char* const kDfsSyncHangMetric = "tera_ts_dfs_sync_hang_total";
const char* const kDfsFlushHangMetric = "tera_ts_dfs_flush_hang_total";
const char* const kDfsListHangMetric = "tera_ts_dfs_list_hang_total";
const char* const kDfsOtherHangMetric = "tera_ts_dfs_other_hang_total";
const char* const kDfsExistsHangMetric = "tera_ts_dfs_exists_hang_total";
const char* const kDfsOpenHangMetric = "tera_ts_dfs_open_hang_total";
const char* const kDfsCloseHangMetric = "tera_ts_dfs_close_hang_total";
const char* const kDfsDeleteHangMetric = "tera_ts_dfs_delete_hang_total";
const char* const kDfsTellHangMetric = "tera_ts_dfs_tell_hang_total";
const char* const kDfsInfoHangMetric = "tera_ts_dfs_info_hang_total";

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
} // end namespace tabletnode
} // end namespace tera 
 
#endif // TERA_TABLETNODE_TABLETNODE_METRIC_NAME_H_
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

