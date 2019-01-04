// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <vector>

#include "env.h"
#include "table/format.h"
#include "status.h"

namespace leveldb {
class Env;
class WriteableCacheFile;

// persistent cache's metric name for prometheus.
namespace PersistentCacheMetricNames {
const char* const kWriteThroughput = "persistent_cache_write_through_put";
const char* const kWriteCount = "persistent_cache_write_cnt";
const char* const kReadThroughput = "persistent_cache_read_through_put";
const char* const kCacheHits = "persistent_cache_hits";
const char* const kCacheMisses = "persistent_cache_misses";
const char* const kCacheErrors = "persistent_cache_errors";
const char* const kCacheCapacity = "persistent_cache_capacity";
const char* const kFileEntries = "persistent_cache_file_entries";
const char* const kCacheSize = "persistent_cache_size";
const char* const kMetaDataSize = "persistent_cache_metadata_size";
};  // PersistentCacheMetricNames

// Persistent Cache Config
//
// This struct captures all the options that are used to configure persistent
// cache. Some of the terminologies used in naming the options are
//
// cache size :
// This is the logical maximum for the cache size
//
struct PersistentCacheConfig {
  PersistentCacheConfig(leveldb::Env* _env, const std::string& _path, const uint64_t _cache_size)
      : env(_env), path(_path), cache_size(_cache_size) {
    if (path.back() != '/') {
      path.append("/");
    }
  }

  //
  // Validate the settings. Our intentions are to catch erroneous settings ahead
  // of time instead going violating invariants or causing dead locks.
  //
  leveldb::Status ValidateSettings() const {
    if (!env || path.empty()) {
      return leveldb::Status::InvalidArgument("empty or null args");
    }

    if (cache_size <= 0) {
      return leveldb::Status::InvalidArgument("cache size <= 0");
    }

    return leveldb::Status::OK();
  }

  //
  // Env abstraction to use for system level operations
  //
  leveldb::Env* env;

  //
  // Path for the block cache where blocks are persisted
  //
  std::string path;

  //
  // Logical cache size
  //
  int64_t cache_size = std::numeric_limits<int64_t>::max();

  //
  // Retry times when reserve space failed
  //
  uint64_t write_retry_times = 5;

  //
  // Transfer existing flash_env files to persistent cache
  //
  bool transfer_flash_env_files = false;

  std::string ToString() const;

  void SetEnvOptions(const EnvOptions& opt) { env_opt = opt; }

  EnvOptions env_opt;
};

// PersistentCache
//
// Persistent cache interface for caching IO pages on a persistent medium. The
// cache interface is specifically designed for persistent read cache.
class PersistentCache {
 public:
  using StatsType = std::map<std::string, double>;

  virtual ~PersistentCache() = default;

  // Read page cache by page identifier.
  //
  // key        user cache key for target cache file.
  // offset     offset to read in the file.
  // length     length to read in the file.
  // content    user should use data in content.
  // data       buffer where the data should be copied.
  virtual Status Read(const Slice& key, size_t offset, size_t length, Slice* content,
                      SstDataScratch* scratch) = 0;

  // Force evict a file from the cache.
  virtual void ForceEvict(const Slice& key) = 0;

  // Generate a new cache file for write.
  // After write done, user should call two finish functions:
  // 1. file->Close(key):
  //    This function will insert the cache file to persistent
  //    cache system automatically.
  // 2. file->Abandon():
  //    This funtion will abandon this file,
  //    and this file will never be read by user.
  virtual Status NewWriteableCacheFile(const std::string& path, WriteableCacheFile**) = 0;

  // Return total capacity of persistent cache in bytes, including used and unused space.
  virtual size_t GetCapacity() const = 0;

  // Return used bytes of persistent cache.
  virtual size_t GetUsage() const = 0;

  virtual Status Open() = 0;

  virtual std::vector<std::string> GetAllKeys() = 0;
  virtual void GarbageCollect() = 0;
};

Status NewPersistentCache(const PersistentCacheConfig&, std::shared_ptr<PersistentCache>*);

Status NewShardedPersistentCache(const std::vector<PersistentCacheConfig>&,
                                 std::shared_ptr<PersistentCache>*);
}  // leveldb
