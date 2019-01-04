// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include <algorithm>
#include <memory>
#include <vector>
#include <unordered_map>

#include "common/rwmutex.h"
#include "common/this_thread.h"
#include "persistent_cache_impl.h"
#include "util/hash.h"
#include "util/random.h"

namespace leveldb {
class CacheFile;

class ShardedPersistentCacheImpl : public PersistentCache {
  // This private label is used for unit test flag -Dprivate=public;
 private:
  using PersistentCachePtr = std::unique_ptr<PersistentCacheImpl>;

 public:
  Status NewWriteableCacheFile(const std::string& path, WriteableCacheFile** file) override {
    uint64_t index{PickPersistentCacheIndex()};
    auto status = persistent_caches_[index]->NewWriteableCacheFile(path, file);

    if (status.ok()) {
      assert(*file);
      assert(index < persistent_caches_.size());
      (*file)->SetInsertCallback(std::bind(&ShardedPersistentCacheImpl::InsertCallback, this,
                                           std::placeholders::_1, index));
    }
    return status;
  }

  explicit ShardedPersistentCacheImpl(const std::vector<PersistentCacheConfig>& opts)
      : stats_{new PersistentCacheImpl::Statistics} {
    for (const auto& opt : opts) {
      persistent_caches_.emplace_back(PersistentCachePtr{new PersistentCacheImpl{opt, stats_}});
    }
  }

  ~ShardedPersistentCacheImpl() override = default;

  Status Read(const Slice& key, size_t offset, size_t length, Slice* content,
              SstDataScratch* scratch) override {
    uint64_t index;
    auto key_str = key.ToString();
    {
      ReadLock _(&index_rw_lock_);
      if (cache_index_.find(key_str) == cache_index_.end()) {
        stats_->cache_misses.Inc();
        return Status::NotFound("persistent cache: index not found.");
      }
      index = cache_index_[key_str];
    }
    return persistent_caches_[index]->Read(key, offset, length, content, scratch);
  }

  // Only can be called in single thread once before use persistent cache,
  // so it doesn't need any lock mechanism.
  Status Open() override {
    for (auto& persistent_cache : persistent_caches_) {
      auto s = persistent_cache->Open();
      if (!s.ok()) {
        return s;
      }
    }

    for (size_t i = 0; i != persistent_caches_.size(); ++i) {
      auto& cache = persistent_caches_[i];
      auto keys = cache->GetAllKeys();
      for (auto& key : keys) {
        // Same key in more than one persistent cache impl, remove the latter one.
        if (cache_index_.find(key) != cache_index_.end()) {
          cache->ForceEvict(key);
        } else {
          cache_index_.emplace(std::move(key), i);
        }
      }
    }

    return Status::OK();
  }

  void ForceEvict(const Slice& key) override {
    uint64_t index;
    auto key_str = key.ToString();
    {
      WriteLock _{&index_rw_lock_};
      if (cache_index_.find(key_str) == cache_index_.end()) {
        return;
      }
      index = cache_index_[key_str];
      cache_index_.erase(key_str);
    }
    return persistent_caches_[index]->ForceEvict(key);
  }

  size_t GetCapacity() const override {
    size_t size = 0;
    for (auto& persistent_cache : persistent_caches_) {
      size += persistent_cache->GetCapacity();
    }
    return size;
  }

  size_t GetUsage() const override {
    size_t size = 0;
    for (auto& persistent_cache : persistent_caches_) {
      size += persistent_cache->GetUsage();
    }
    return size;
  }

  std::vector<std::string> GetAllKeys() override {
    std::vector<std::string> result;
    std::vector<std::vector<std::string>> sub_results;
    size_t total_size{0};
    for (auto& persistent_cache : persistent_caches_) {
      sub_results.emplace_back(persistent_cache->GetAllKeys());
      total_size += sub_results.back().size();
    }
    result.reserve(total_size);

    for (auto& sub_result : sub_results) {
      result.insert(result.end(), std::make_move_iterator(sub_result.begin()),
                    std::make_move_iterator(sub_result.end()));
    }

    return std::move(result);
  }

  void GarbageCollect() override {
    for (auto& persistent_cache : persistent_caches_) {
      persistent_cache->GarbageCollect();
    }
  }

 private:
  void InsertCallback(const Slice& key, uint64_t index) {
    auto key_str = key.ToString();
    WriteLock _{&index_rw_lock_};
    LEVELDB_LOG("Insert to sharded persistent cache, key %s, index %lu.\n", key_str.c_str(), index);
    cache_index_[key_str] = index;
  }

  size_t FreeSpace(const PersistentCachePtr& p) { return p->GetCapacity() - p->GetUsage(); }

  uint64_t PickPersistentCacheIndex() {
    // Read doc/persistent_cache.md for Pick strategy's detail.
    std::vector<size_t> indexes_for_random_pick;
    for (size_t i = 0; i != persistent_caches_.size(); ++i) {
      const auto& p_cache = persistent_caches_[i];
      if ((double)p_cache->GetUsage() / p_cache->GetCapacity() < 0.9) {
        indexes_for_random_pick.push_back(i);
      }
    }

    if (indexes_for_random_pick.empty()) {
      // Pick persistent cache index by free space size for using every disk's space as much as
      // possible. This strategy will be enabled when all persistent_cache_impl_'s usage percent
      // is larger than 90%.
      auto iter =
          std::max_element(persistent_caches_.begin(), persistent_caches_.end(),
                           [this](const PersistentCachePtr& x, const PersistentCachePtr& y) {
                             return FreeSpace(x) < FreeSpace(y);
                           });

      return iter - persistent_caches_.begin();
    } else {
      // Pick persistent cache index by random for best performance.
      auto idx = ThisThread::GetRandomValue<size_t>(0, indexes_for_random_pick.size() - 1);
      return indexes_for_random_pick[idx];
    }
  }

 private:
  // After Open() method called, this vector is only used for read, so it's lock-free.
  std::vector<PersistentCachePtr> persistent_caches_;

  // This class handles serval persistent-caches, and cache_index_ managers each key's index as
  // follow:
  // user key=>index of persistent_caches_;
  std::unordered_map<std::string, uint64_t> cache_index_;
  std::shared_ptr<PersistentCacheImpl::Statistics> stats_;
  RWMutex index_rw_lock_;
};
}  // namespace leveldb
