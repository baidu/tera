// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <assert.h>

#include <iostream>
#include <mutex>
#include <sstream>
#include <type_traits>
#include <unordered_map>

#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_int32(master_merge_procedure_limit);
DECLARE_int32(master_split_procedure_limit);
DECLARE_int32(master_move_procedure_limit);
DECLARE_int32(master_load_procedure_limit);
DECLARE_int32(master_unload_procedure_limit);

namespace tera {
namespace master {

class ProcedureLimiter final {
 public:
  enum class LockType { kNoLimit = 0, kMerge, kSplit, kMove, kLoad, kUnload };

  friend std::ostream& operator<<(std::ostream& os, const ProcedureLimiter::LockType& type) {
    static const std::vector<std::string> msg = {"kNoLimit", "kMerge", "kSplit",
                                                 "kMove",    "kLoad",  "kUnload"};
    size_t index = static_cast<std::underlying_type<ProcedureLimiter::LockType>::type>(type);
    assert(index < msg.size());
    os << msg[index];
    return os;
  }

 public:
  static ProcedureLimiter& Instance() {
    static ProcedureLimiter instance;
    return instance;
  }

  bool GetLock(const LockType& type) {
    if (type == LockType::kNoLimit) {
      VLOG(20) << "[ProcedureLimiter] get lock for type:" << type << " success";
      return true;
    }
    assert(limit_.find(type) != limit_.end());
    assert(in_use_.find(type) != in_use_.end());

    std::lock_guard<std::mutex> guard(mutex_);
    if (in_use_[type] >= limit_[type]) {
      VLOG(20) << "[ProcedureLimiter] get lock for type:" << type
               << " fail, reason: lock exhaust, lock limit:" << limit_[type]
               << ", in use:" << in_use_[type];
      return false;
    }
    ++in_use_[type];
    VLOG(20) << "[ProcedureLimiter] get lock for type:" << type
             << " success, lock limit:" << limit_[type] << ", in use:" << in_use_[type];
    return true;
  }

  void ReleaseLock(const LockType& type) {
    if (type == LockType::kNoLimit) {
      VLOG(20) << "[ProcedureLimiter] release lock for type:" << type << " success";
      return;
    }
    assert(limit_.find(type) != limit_.end());
    assert(in_use_.find(type) != in_use_.end());

    std::lock_guard<std::mutex> guard(mutex_);
    assert(in_use_[type] > 0);
    --in_use_[type];
    VLOG(20) << "[ProcedureLimiter] release lock for type:" << type
             << " success, lock limit:" << limit_[type] << ", in use:" << in_use_[type];
  }

  void SetLockLimit(const LockType& type, uint32_t num) {
    std::lock_guard<std::mutex> guard(mutex_);
    limit_[type] = num;
    VLOG(20) << "[ProcedureLimiter] set lock type:" << type << " with lock limit:" << num;
  }

  uint32_t GetLockLimit(const LockType& type) const {
    assert(limit_.find(type) != limit_.end());
    std::lock_guard<std::mutex> guard(mutex_);
    return limit_[type];
  }

  uint32_t GetLockInUse(const LockType& type) const {
    assert(in_use_.find(type) != in_use_.end());
    std::lock_guard<std::mutex> guard(mutex_);
    return in_use_[type];
  }

  std::string GetSummary() const {
    std::ostringstream res_ss;
    res_ss << "[kMerge, limit:" << GetLockLimit(LockType::kMerge)
           << ", in_use:" << GetLockInUse(LockType::kMerge)
           << "]\n[kSplit, limit:" << GetLockLimit(LockType::kSplit)
           << ", in_use:" << GetLockInUse(LockType::kSplit)
           << "]\n[kMove, limit:" << GetLockLimit(LockType::kMove)
           << ", in_use:" << GetLockInUse(LockType::kMove)
           << "]\n[kLoad, limit:" << GetLockLimit(LockType::kLoad)
           << ", in_use:" << GetLockInUse(LockType::kLoad)
           << "]\n[kUnload, limit:" << GetLockLimit(LockType::kUnload)
           << ", in_use:" << GetLockInUse(LockType::kUnload) << "]";
    return res_ss.str();
  }

 private:
  struct LockTypeHash {
    template <typename T>
    std::size_t operator()(const T& t) const {
      return static_cast<std::size_t>(t);
    }
  };
  mutable std::unordered_map<LockType, uint32_t, LockTypeHash> limit_;
  mutable std::unordered_map<LockType, uint32_t, LockTypeHash> in_use_;
  mutable std::mutex mutex_;

 private:
  ProcedureLimiter() {
    SetLockLimit(LockType::kMerge, static_cast<uint32_t>(FLAGS_master_merge_procedure_limit));
    SetLockLimit(LockType::kSplit, static_cast<uint32_t>(FLAGS_master_split_procedure_limit));
    SetLockLimit(LockType::kMove, static_cast<uint32_t>(FLAGS_master_move_procedure_limit));
    SetLockLimit(LockType::kLoad, static_cast<uint32_t>(FLAGS_master_load_procedure_limit));
    SetLockLimit(LockType::kUnload, static_cast<uint32_t>(FLAGS_master_unload_procedure_limit));
    {
      std::lock_guard<std::mutex> guard(mutex_);
      in_use_[LockType::kMerge] = 0;
      in_use_[LockType::kSplit] = 0;
      in_use_[LockType::kMove] = 0;
      in_use_[LockType::kLoad] = 0;
      in_use_[LockType::kUnload] = 0;
    }
  }

  ~ProcedureLimiter() = default;

  ProcedureLimiter(const ProcedureLimiter&) = delete;
  ProcedureLimiter& operator=(const ProcedureLimiter&) = delete;
  ProcedureLimiter(ProcedureLimiter&&) = delete;
  ProcedureLimiter& operator=(ProcedureLimiter&&) = delete;
};

}  // namespace master
}  // namespace tera
