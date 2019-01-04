// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "common/thread_pool.h"
#include "common/rwmutex.h"
#include "format.h"
#include "leveldb/env.h"
#include "leveldb/status.h"

#pragma once
namespace leveldb {
class PersistentCache;

class PersistentCacheHelper {
  static constexpr uint32_t kThreadNum = 10;
  static constexpr uint32_t kMaxPendingNum = 10;

 public:
  static void ScheduleCopyToLocal(Env *env, const std::string &fname, uint64_t fsize,
                                  const std::string &key,
                                  const std::shared_ptr<PersistentCache> &p_cache);

  static void DoCopyToLocal(Env *env, const std::string &fname, uint64_t fsize,
                            const std::string &key, std::shared_ptr<PersistentCache> p_cache);

  static Status TryReadFromPersistentCache(const std::shared_ptr<PersistentCache> &p_cache,
                                           const Slice &key, uint64_t offset, uint64_t length,
                                           Slice *contents, SstDataScratch *val);

 private:
  static std::mutex updating_files_mutex_;
  static common::RWMutex closing_file_rw_mutex_;
  static std::unordered_set<std::string> updating_files_;
  static common::ThreadPool copy_to_local_thread_pool_;
  static std::atomic<uint32_t> pending_num_;
  static const uint64_t max_pending_num_;
};
}  // leveldb
