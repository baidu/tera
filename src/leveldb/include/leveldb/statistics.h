// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

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
  // tera flash block cache spec
  FLASH_BLOCK_CACHE_PREAD_QUEUE = 0,
  FLASH_BLOCK_CACHE_PREAD_SSD_READ,
  FLASH_BLOCK_CACHE_PREAD_FILL_USER_DATA,
  FLASH_BLOCK_CACHE_PREAD_RELEASE_BLOCK,
  FLASH_BLOCK_CACHE_LOCKMAP_BS_RELOAD_NR,
  FLASH_BLOCK_CACHE_PREAD_GET_BLOCK,
  FLASH_BLOCK_CACHE_PREAD_BLOCK_NR,
  FLASH_BLOCK_CACHE_GET_BLOCK_SET,
  FLASH_BLOCK_CACHE_BS_LRU_LOOKUP,
  FLASH_BLOCK_CACHE_PREAD_WAIT_UNLOCK,
  FLASH_BLOCK_CACHE_ALLOC_FID,
  FLASH_BLOCK_CACHE_GET_FID,
  FLASH_BLOCK_CACHE_EVICT_NR,
  FLASH_BLOCK_CACHE_PREAD_DFS_READ,
  FLASH_BLOCK_CACHE_PREAD_SSD_WRITE,
  HISTOGRAM_ENUM_MAX,  // TODO(ldemailly): enforce HistogramsNameMap match
};

const std::vector<std::pair<Histograms, std::string> > HistogramsNameMap = {
    {FLASH_BLOCK_CACHE_PREAD_QUEUE, "flash_block_cache.pread_queue"},
    {FLASH_BLOCK_CACHE_PREAD_SSD_READ, "flash_block_cache.pread_ssd_read"},
    {FLASH_BLOCK_CACHE_PREAD_FILL_USER_DATA, "flash_block_cache.pread_fill_user_data"},
    {FLASH_BLOCK_CACHE_PREAD_RELEASE_BLOCK, "flash_block_cache.pread_release_block"},
    {FLASH_BLOCK_CACHE_LOCKMAP_BS_RELOAD_NR, "flash_block_cache.lockmap_bs_reload_nr"},
    {FLASH_BLOCK_CACHE_PREAD_GET_BLOCK, "flash_block_cache.pread_get_block"},
    {FLASH_BLOCK_CACHE_PREAD_BLOCK_NR, "flash_block_cache.pread_block_nr"},
    {FLASH_BLOCK_CACHE_GET_BLOCK_SET, "flash_block_cache.get_block_set"},
    {FLASH_BLOCK_CACHE_BS_LRU_LOOKUP, "flash_block_cache.bs_lru_lookup"},
    {FLASH_BLOCK_CACHE_PREAD_WAIT_UNLOCK, "flash_block_cache.pread_wait_unlock"},
    {FLASH_BLOCK_CACHE_ALLOC_FID, "flash_block_cache.alloc_fid"},
    {FLASH_BLOCK_CACHE_GET_FID, "flash_block_cache.get_fid"},
    {FLASH_BLOCK_CACHE_EVICT_NR, "flash_block_cache.evict_nr"},
    {FLASH_BLOCK_CACHE_PREAD_DFS_READ, "flash_block_cache.pread_dfs_read"},
    {FLASH_BLOCK_CACHE_PREAD_SSD_WRITE, "flash_block_cache.pread_ssd_write"},
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

