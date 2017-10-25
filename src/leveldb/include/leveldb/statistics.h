// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
#define STORAGE_LEVELDB_INCLUDE_STATISTICS_H_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <vector>

namespace leveldb {

/**
 * Keep adding ticker's here.
 *  1. Any ticker should be added before TICKER_ENUM_MAX.
 *  2. Add a readable string in TickersNameMap below for the newly added ticker.
 */
enum Tickers : uint32_t {
  TICKER_ENUM_MAX
};

// The order of items listed in  Tickers should be the same as
// the order listed in TickersNameMap
const std::vector<std::pair<Tickers, std::string> > TickersNameMap = {
};

/**
 * Keep adding histogram's here.
 * Any histogram whould have value less than HISTOGRAM_ENUM_MAX
 * Add a new Histogram by assigning it the current value of HISTOGRAM_ENUM_MAX
 * Add a string representation in HistogramsNameMap below
 * And increment HISTOGRAM_ENUM_MAX
 */
enum Histograms : uint32_t {
  // tera block cache spec
  TERA_BLOCK_CACHE_PREAD_QUEUE = 0,
  TERA_BLOCK_CACHE_PREAD_SSD_READ,
  TERA_BLOCK_CACHE_PREAD_FILL_USER_DATA,
  TERA_BLOCK_CACHE_PREAD_RELEASE_BLOCK,
  TERA_BLOCK_CACHE_LOCKMAP_DS_RELOAD_NR,
  TERA_BLOCK_CACHE_PREAD_GET_BLOCK,
  TERA_BLOCK_CACHE_PREAD_BLOCK_NR,
  TERA_BLOCK_CACHE_GET_DATA_SET,
  TERA_BLOCK_CACHE_DS_LRU_LOOKUP,
  TERA_BLOCK_CACHE_PREAD_WAIT_UNLOCK,
  TERA_BLOCK_CACHE_ALLOC_FID,
  TERA_BLOCK_CACHE_GET_FID,
  TERA_BLOCK_CACHE_EVICT_NR,
  TERA_BLOCK_CACHE_PREAD_DFS_READ,
  TERA_BLOCK_CACHE_PREAD_SSD_WRITE,
  HISTOGRAM_ENUM_MAX,  // TODO(ldemailly): enforce HistogramsNameMap match
};

const std::vector<std::pair<Histograms, std::string> > HistogramsNameMap = {
    {TERA_BLOCK_CACHE_PREAD_QUEUE, "tera.block_cache.pread_queue"},
    {TERA_BLOCK_CACHE_PREAD_SSD_READ, "tera.block_cache.pread_ssd_read"},
    {TERA_BLOCK_CACHE_PREAD_FILL_USER_DATA, "tera.block_cache.pread_fill_user_data"},
    {TERA_BLOCK_CACHE_PREAD_RELEASE_BLOCK, "tera.block_cache.pread_release_block"},
    {TERA_BLOCK_CACHE_LOCKMAP_DS_RELOAD_NR, "tera.block_cache.lockmap_ds_reload_nr"},
    {TERA_BLOCK_CACHE_PREAD_GET_BLOCK, "tera.block_cache.pread_get_block"},
    {TERA_BLOCK_CACHE_PREAD_BLOCK_NR, "tera.block_cache.pread_block_nr"},
    {TERA_BLOCK_CACHE_GET_DATA_SET, "tera.block_cache.get_data_set"},
    {TERA_BLOCK_CACHE_DS_LRU_LOOKUP, "tera.block_cache.ds_lru_lookup"},
    {TERA_BLOCK_CACHE_PREAD_WAIT_UNLOCK, "tera.block_cache.pread_wait_unlock"},
    {TERA_BLOCK_CACHE_ALLOC_FID, "tera.block_cache.alloc_fid"},
    {TERA_BLOCK_CACHE_GET_FID, "tera.block_cache.get_fid"},
    {TERA_BLOCK_CACHE_EVICT_NR, "tera.block_cache.evict_nr"},
    {TERA_BLOCK_CACHE_PREAD_DFS_READ, "tera.block_cache.pread_dfs_read"},
    {TERA_BLOCK_CACHE_PREAD_SSD_WRITE, "tera.block_cache.pread_ssd_write"},
};

struct HistogramData {
  double median; // 中值
  double percentile95;
  double percentile99; // 99分为点
  double average;
  double standard_deviation;
};

// Analyze the performance of a db
class Statistics {
 public:
  virtual ~Statistics() {}

  virtual int64_t GetTickerCount(uint32_t ticker_type) = 0;
  virtual void RecordTick(uint32_t ticker_type, uint64_t count = 0) = 0;
  virtual void SetTickerCount(uint32_t ticker_type, uint64_t count) = 0;

  virtual void GetHistogramData(uint32_t type,
                                HistogramData* const data) = 0;
  virtual std::string GetBriefHistogramString(uint32_t type) { return ""; }
  virtual std::string GetHistogramString(uint32_t type) const { return ""; }
  virtual void MeasureTime(uint32_t histogram_type, uint64_t time) = 0;
  virtual void ClearHistogram(uint32_t type) = 0;

  // String representation of the statistic object.
  virtual std::string ToString() {
    // Do nothing by default
    return std::string("ToString(): not implemented");
  }
  virtual void ClearAll() = 0;
};

// Create a concrete DBStatistics object
Statistics* CreateDBStatistics();

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
