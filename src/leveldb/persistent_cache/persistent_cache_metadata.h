// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include <algorithm>
#include <functional>
#include <unordered_set>
#include <string>
#include <unordered_map>

#include "hash_table.h"
#include "hash_table_evictable.h"
#include "leveldb/persistent_cache.h"
#include "leveldb/slice.h"
#include "lrulist.h"
#include "persistent_cache_file.h"

namespace leveldb {

struct FileInfo {
  explicit FileInfo(const Slice& key, uint64_t cache_id)
      : key_(key.ToString()), cache_id_(cache_id) {}

  std::string key_;
  uint64_t cache_id_;
};

//
// Persistent Cache Metadata
//
// The Persistent Cache Metadata holds all the metadata
// associated with persistent cache.
// It fundamentally contains 2 indexes and an LRU.
//
// Cache Id Index
//
// This is a forward index that maps a given fname to a cache id
//
// Cache File Index
//
// This is a forward index that maps a given cache-id to a cache file object.
class PersistentCacheImpl;

// Thread safe. But Init() method can only be called once.
class PersistentCacheMetaData {
  using CacheFileWrapper = std::unique_ptr<CacheFile, std::function<void(CacheFile*)>>;

 public:
  explicit PersistentCacheMetaData(const PersistentCacheConfig& config,
                                   const uint32_t cache_file_capacity = 128 * 1024)
      : config_(config),
        cache_file_index_(cache_file_capacity),
        cache_id_index_(cache_file_capacity),
        db_(nullptr) {}

  virtual ~PersistentCacheMetaData() = default;

  // Restore Persistent cache meta data
  Status Init(uint64_t* recovered_cache_id, PersistentCacheImpl* cache);

  bool AddCacheFile(CacheFile* file);

  // Read cache file based on cache_id
  CacheFileWrapper Lookup(uint64_t cache_id);

  FileInfo* Insert(const Slice& key, CacheFile* cache_file);

  bool Lookup(const Slice& key, uint64_t* cache_id);

  FileInfo* Remove(const Slice& key);

  // Find and evict a cache file using LRU policy
  CacheFile* Evict();

  virtual void Clear();

  CacheFile* ForceEvict(const Slice& key);
  void ForceEvict(CacheFile* file);

  uint64_t GetDBSize() {
    uint64_t db_size(0);
    db_->GetApproximateSizes(&db_size);
    return db_size;
  }

  std::vector<std::string> GetAllKeys() {
    std::vector<std::string> res;
    {
      std::lock_guard<std::mutex> _{keys_lock_};
      res.reserve(keys_.size());
      res.insert(res.end(), keys_.begin(), keys_.end());
    }
    return std::move(res);
  }

  std::vector<std::unique_ptr<CacheFile>> CollectEvictableFiles();

 protected:
  virtual void RemoveFileInfo(CacheFile* file);

 private:
  FileInfo* InsertWithoutPutDb(const Slice& key, uint64_t cache_id);
  // Cache file index definition
  //
  // cache-id => CacheFile
  struct CacheFileHash {
    uint64_t operator()(const CacheFile* rec) { return std::hash<uint64_t>()(rec->cacheid()); }
  };

  struct CacheFileEqual {
    bool operator()(const CacheFile* lhs, const CacheFile* rhs) {
      return lhs->cacheid() == rhs->cacheid();
    }
  };

  typedef EvictableHashTable<CacheFile, CacheFileHash, CacheFileEqual> CacheFileIndexType;

  // cache_id Index
  //
  // key => cache_id
  struct Hash {
    size_t operator()(FileInfo* node) const { return std::hash<std::string>()(node->key_); }
  };

  struct Equal {
    bool operator()(FileInfo* lhs, FileInfo* rhs) const { return lhs->key_ == rhs->key_; }
  };

  typedef HashTable<FileInfo*, Hash, Equal> CacheIdIndexType;

  const PersistentCacheConfig config_;
  CacheFileIndexType cache_file_index_;
  CacheIdIndexType cache_id_index_;
  std::unique_ptr<leveldb::DB> db_;
  std::unordered_set<CacheFile*> evictable_files_;
  std::mutex evictable_files_lock_;
  // all keys in meta data
  std::unordered_set<std::string> keys_;
  std::mutex keys_lock_;
};
}  // namespace leveldb
