// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/persistent_cache.h"
#include "leveldb/persistent_cache/persistent_cache_file.h"
#include "persistent_cache_helper.h"

namespace leveldb {

std::mutex PersistentCacheHelper::updating_files_mutex_;
common::RWMutex PersistentCacheHelper::closing_file_rw_mutex_;
common::ThreadPool PersistentCacheHelper::copy_to_local_thread_pool_{kThreadNum};

std::unordered_set<std::string> PersistentCacheHelper::updating_files_;
std::atomic<uint32_t> PersistentCacheHelper::pending_num_{0};
const std::uint64_t PersistentCacheHelper::max_pending_num_{kMaxPendingNum};

void PersistentCacheHelper::ScheduleCopyToLocal(Env *env, const std::string &fname, uint64_t fsize,
                                                const std::string &key,
                                                const std::shared_ptr<PersistentCache> &p_cache) {
  if (pending_num_.load() >= max_pending_num_ || !p_cache) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(updating_files_mutex_);
    if (updating_files_.find(key) != updating_files_.end()) {
      return;
    }
    updating_files_.emplace(key);
  }
  ++pending_num_;

  copy_to_local_thread_pool_.AddTask([=](int64_t) {
    DoCopyToLocal(env, fname, fsize, key, p_cache);
    --pending_num_;
    std::lock_guard<std::mutex> lock(updating_files_mutex_);
    assert(updating_files_.find(key) != updating_files_.end());
    updating_files_.erase(key);
  });
}

void PersistentCacheHelper::DoCopyToLocal(Env *env, const std::string &fname, uint64_t fsize,
                                          const std::string &key,
                                          std::shared_ptr<PersistentCache> p_cache) {
  LEVELDB_LOG("Schedule Copy To Local: %s, Pending Num: %u\n", fname.c_str(), pending_num_.load());
  uint64_t time_s = env->NowMicros();

  std::unique_ptr<SequentialFile> dfs_file;
  SequentialFile *tmp_file;
  auto s = env->NewSequentialFile(fname, &tmp_file);

  if (!s.ok()) {
    LEVELDB_LOG("Copy To Local Failed %s : %s\n", fname.c_str(), s.ToString().c_str());
    return;
  }
  dfs_file.reset(tmp_file);

  WriteableCacheFile *cache_file;
  s = p_cache->NewWriteableCacheFile(key, &cache_file);
  if (!s.ok()) {
    LEVELDB_LOG("Copy To Local Failed %s : %s\n", fname.c_str(), s.ToString().c_str());
    return;
  }
  assert(cache_file->refs_);

  std::unique_ptr<char[]> buf(new char[1048576]);  // Read 1M data each time
  Slice result;
  size_t local_size = 0;

  while (dfs_file->Read(1048576, &result, buf.get()).ok() && result.size() > 0 &&
         cache_file->Append(result).ok()) {
    local_size += result.size();
  }

  if (local_size == fsize) {
    {
      WriteLock _(&closing_file_rw_mutex_);
      cache_file->Close(key);
    }
    uint64_t time_used = env->NowMicros() - time_s;
    LEVELDB_LOG("copy %s to local success in %llu ms.\n", fname.c_str(),
                static_cast<unsigned long long>(time_used) / 1000);
  } else {
    cache_file->Abandon();
    uint64_t dfs_file_size = 0;
    s = env->GetFileSize(fname, &dfs_file_size);
    if (!s.ok()) {
      LEVELDB_LOG("dfs GetFileSize fail %s : %s\n", fname.c_str(), s.ToString().c_str());
    } else {
      LEVELDB_LOG(
          "copy %s to local fail, size %lu, dfs size %lu, local "
          "size %lu\n",
          fname.c_str(), fsize, dfs_file_size, local_size);
    }
  }
}

Status PersistentCacheHelper::TryReadFromPersistentCache(
    const std::shared_ptr<PersistentCache> &p_cache, const Slice &key, uint64_t offset,
    uint64_t length, Slice *contents, SstDataScratch *val) {
  ReadLock _(&closing_file_rw_mutex_);
  return p_cache->Read(key, offset, length, contents, val);
}
}  // leveldb
