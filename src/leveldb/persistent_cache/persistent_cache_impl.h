// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include <memory>
#include <mutex>
#include <sstream>
#include <string>

#include "common/metric/metric_counter.h"
#include "leveldb/persistent_cache.h"
#include "persistent_cache_metadata.h"

namespace leveldb {

class PersistentCacheImpl : public PersistentCache {
  friend class WriteableCacheFile;
  friend class PersistentCacheMetaData;

 public:
  struct Statistics {
    Statistics();
    tera::MetricCounter write_throughput;
    tera::MetricCounter write_count;
    tera::MetricCounter read_throughput;
    tera::MetricCounter cache_hits;
    tera::MetricCounter cache_misses;
    tera::MetricCounter cache_errors;
    tera::MetricCounter file_entries;
  };

  PersistentCacheImpl(const PersistentCacheConfig &opt, const std::shared_ptr<Statistics> &stats);

  // Interface Impl
  ~PersistentCacheImpl() override { metadata_.Clear(); }
  Status Open() override;
  Status Read(const Slice &key, size_t offset, size_t length, Slice *content,
              SstDataScratch *scratch) override;
  void ForceEvict(const Slice &key) override;
  Status NewWriteableCacheFile(const std::string &path, WriteableCacheFile **file) override;

  size_t GetCapacity() const override { return static_cast<size_t>(opt_.cache_size); }
  size_t GetUsage() const override { return static_cast<size_t>(size_.Get()); }
  std::vector<std::string> GetAllKeys() override;
  void GarbageCollect() override;

 private:
  Status MakeRoomForWrite(int64_t size);
  void ForceEvict(CacheFile *file);
  bool Insert(const Slice &key, CacheFile *file);

  // These two methods is used for moving env_flash's file to Persistent Cache with out refill
  // cache.
  // Maybe deprecated in future version.
  void AddExistingFile(const Slice &key, const std::string &file_name);
  void PullEnvFlashFiles();

  // Get cache directory path
  std::string GetCachePath() const { return opt_.path; }

  // Get cache metadata directory path
  std::string GetMetaPath() const { return opt_.path + "/persistent_cache_meta"; }

  std::shared_ptr<Statistics> GetStats() { return stats_; }

  bool IsCacheFile(const std::string &file);

  Status DeleteFileAndReleaseCache(CacheFile *file);

  void CleanupCacheFolder(const std::string &folder);
  void DoCleanupCacheFolder(const std::string &folder);

 private:
  std::mutex lock_;                  // Synchronization
  const PersistentCacheConfig opt_;  // BlockCache options
  PersistentCacheMetaData metadata_;
  uint64_t writer_cache_id_ = 0;  // Current cache file identifier
  tera::MetricCounter size_;      // Size of the cache
  tera::MetricCounter capacity_;  // Capacity of the cache
  tera::MetricCounter metadata_size_;
  std::shared_ptr<Statistics> stats_;  // Statistics
};

}  // namespace leveldb
