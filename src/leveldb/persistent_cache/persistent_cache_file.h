// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <common/metric/metric_counter.h>

#include "common/mutex.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "lrulist.h"
#include "table/format.h"

// The file level operations are encapsulated in the following abstractions
//
// CacheFile
//       ^
//       |
//       |
// RandomAccessCacheFile (For reading)
//       ^
//       |
//       |
// WriteableCacheFile (For writing)
//
// Write IO code path :
//

namespace leveldb {

class PersistentCacheImpl;
class FileInfo;

// class CacheFile
//
// Generic interface to support building file specialized for read/writing
class CacheFile : public LRUElement<CacheFile> {
 public:
  explicit CacheFile(const uint64_t cache_id) : cache_id_(cache_id) {}

  CacheFile(uint64_t cache_id, Env* env, const EnvOptions& opt, const std::string& path,
            PersistentCacheImpl* cache)
      : LRUElement<CacheFile>(),
        cache_id_(cache_id),
        env_(env),
        env_opt_(opt),
        path_(path),
        cache_(cache) {}

  virtual ~CacheFile() = default;

  // append key/value to file and return LBA locator to user
  virtual Status Append(const Slice& /*val*/) {
    assert(!"not implemented");
    return Status::InvalidArgument("Not Implemented");
  }

  // read from the record locator (LBA) and return key, value and status
  virtual Status Read(size_t offset, size_t length, Slice* /*block*/,
                      std::unique_ptr<char, std::function<void(char*)>>* /*scratch*/) {
    assert(!"not implemented");
    return Status::InvalidArgument("Not Implemented");
  }

  // get file path
  std::string Path() const { return path_; }
  // get cache ID
  uint64_t cacheid() const { return cache_id_; }

  Status Delete(uint64_t* size);

  void SetInfo(FileInfo* info) { info_ = info; }

  FileInfo* Info() { return info_; }

 protected:
  const uint64_t cache_id_;   // Cache id for the file
  Env* const env_ = nullptr;  // Env for IO
  EnvOptions env_opt_;        // Env options for env (dio, write buffer...)
  std::string path_;          // Directory name
  PersistentCacheImpl* cache_ = nullptr;
  FileInfo* info_ = nullptr;  // Related file info
};

// class RandomAccessFile
//
// Thread safe implementation for reading random data from file
class RandomAccessCacheFile : public CacheFile {
 public:
  RandomAccessCacheFile(uint64_t cache_id, Env* env, const EnvOptions& opt, const std::string& path,
                        PersistentCacheImpl* cache = nullptr)
      : CacheFile(cache_id, env, opt, path, cache) {}

  ~RandomAccessCacheFile() override = default;

  // open file for reading
  Status Open();

  // read data from the disk
  Status Read(size_t offset, size_t length, Slice* block, SstDataScratch* scratch) override;

 private:
  std::unique_ptr<RandomAccessFile> file_ = nullptr;

 protected:
  Status OpenImpl();
};

// class WriteableCacheFile
//
// All writes to the files are cached in buffers. The buffers are flushed to
// disk as they get filled up. When file size reaches a certain size, a new file
// will be created provided there is free space
class WriteableCacheFile : public RandomAccessCacheFile {
  static constexpr uint32_t kRetryIntervalUs = 1000000;  // sleep 1s when reserve failed.
 public:
  explicit WriteableCacheFile(uint64_t cache_id, Env* env, const EnvOptions& opt,
                              const std::string& path, uint64_t write_retry_times,
                              PersistentCacheImpl* cache)
      : RandomAccessCacheFile(cache_id, env, opt, path, cache),
        write_retry_times_(write_retry_times) {}

  ~WriteableCacheFile() override = default;

  // create file on disk
  Status Create();

  // append data to end of file
  Status Append(const Slice&) override;

  // Called when user successfully finish cache file's writing, and want to insert this file to
  // persistent cache with specified cache key.
  // This method will close file and open it for reading, and insert it to persistent cache's
  // metadata.
  // After called this method, user can read this cache file with the specified cache key.
  // (If no error occured when insert to metadata)
  void Close(const Slice& key);

  // Called when user write cache file failed, and want to directly remove it.
  // This method will close file without inserting to persistent cache's metadata.
  // And it will be removed in next GC procedure.
  void Abandon();

  void SetInsertCallback(std::function<void(const Slice& key)> cb) { InsertCallback = cb; }

 private:
  // Leveldb Env file abstraction
  std::unique_ptr<WritableFile> file_ = nullptr;
  // This call back will be set by ShardedPersistentCache for updating its cache_index_.
  // And it will be called after successfully close and insert.
  std::function<void(const Slice& key)> InsertCallback;
  const uint64_t write_retry_times_ = 5;
};
}  // namespace leveldb
