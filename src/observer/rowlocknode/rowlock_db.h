// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_ROWLOCKNODE_ROWLOCK_DB_H_
#define TERA_OBSERVER_ROWLOCKNODE_ROWLOCK_DB_H_

#include <map>
#include <memory>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/scoped_ptr.h"
#include "common/mutex.h"
#include "common/thread_pool.h"
#include "common/timer.h"

DECLARE_int32(rowlock_db_sharding_number);
DECLARE_int32(rowlock_db_ttl);
DECLARE_int32(rowlock_timing_wheel_patch_num);

namespace tera {
namespace observer {

class RowlockDB {
public:
    RowlockDB() 
        : timing_wheel_pos_(0),
          timing_wheel_patch_num_(FLAGS_rowlock_timing_wheel_patch_num) {
        timing_wheel_.resize(timing_wheel_patch_num_);
    }

    ~RowlockDB() {}

    bool TryLock(uint64_t row) {
        MutexLock locker(&mutex_);
        if (locks_.find(row) == locks_.end()) {
            locks_[row].reset(new uint64_t(row));
            std::weak_ptr<uint64_t> ptr = locks_[row];
            timing_wheel_[timing_wheel_pos_].push_back(ptr);
            return true;
        } else {
            return false;
        }
    }

    void UnLock(uint64_t row) {
        MutexLock locker(&mutex_);
        locks_.erase(row);
    }

    // call this function ever timeout period
    // 1. pointer of timing wheel move forward by one step
    // 2. clear all the rowlock keys and remove them from locks_
    // 3. the next 60 seconds all new rowlock keys will be put into this wheel patch
    void ClearTimeout() { 
        // pointer forward
        mutex_.Lock();
        timing_wheel_pos_ = (timing_wheel_pos_ + 1) % timing_wheel_patch_num_;
        std::vector<std::weak_ptr<uint64_t>> buffer;

        // release memory
        buffer.swap(timing_wheel_[timing_wheel_pos_]);
        mutex_.Unlock();

        // remove key from locks_
        for (uint32_t i = 0; i < buffer.size(); ++i) {
            if (!buffer[i].expired()) {
                mutex_.Lock();
                auto it = buffer[i].lock();
                locks_.erase(*it);
                mutex_.Unlock();
            }           
        }
    }

    size_t Size() const {
        MutexLock locker(&mutex_);
        return locks_.size();
    }

private:
    mutable Mutex mutex_;

    std::unordered_map<uint64_t, std::shared_ptr<uint64_t>> locks_;

    // timing wheel
    uint32_t timing_wheel_pos_;
    uint32_t timing_wheel_patch_num_;
    std::vector<std::vector<std::weak_ptr<uint64_t>>> timing_wheel_;
};

class ShardedRowlockDB {
public:
    ShardedRowlockDB() : thread_pool_(new ThreadPool(1)) {
        lock_map_.resize(FLAGS_rowlock_db_sharding_number);

        for (int32_t i = 0; i < FLAGS_rowlock_db_sharding_number; ++i) {
            std::unique_ptr<RowlockDB> db(new RowlockDB()); 
            lock_map_[i].reset(db.release());
        }
        ScheduleClearTimeout();
    }

    ~ShardedRowlockDB() {}

    bool TryLock(uint64_t row) {
        std::unique_ptr<RowlockDB>& db_node = lock_map_[row % FLAGS_rowlock_db_sharding_number];

        if (db_node->TryLock(row) == true) {
            return true;
        } else {
            return false;
        }
    }

    void UnLock(uint64_t row) {
        std::unique_ptr<RowlockDB>& db_node = lock_map_[row % FLAGS_rowlock_db_sharding_number];
        db_node->UnLock(row);
    }

    size_t Size() const {
        size_t size = 0;
        for (uint32_t i = 0; i < lock_map_.size(); ++i) {
            size += lock_map_[i]->Size();
        }
        return size;
    }

private:
    void ScheduleClearTimeout() {
        ClearTimeout();

        ThreadPool::Task task = std::bind(&ShardedRowlockDB::ScheduleClearTimeout, this);
        // everytime timing wheel move forward one step, every patch_num steps data will be cleared
        thread_pool_->DelayTask(FLAGS_rowlock_db_ttl / FLAGS_rowlock_timing_wheel_patch_num, task);
    }

    void ClearTimeout() {
        for (int32_t i = 0; i < FLAGS_rowlock_db_sharding_number; ++i) {
            lock_map_[i]->ClearTimeout();
        }
    }

private:
    std::vector<std::unique_ptr<RowlockDB>> lock_map_;
    scoped_ptr<ThreadPool> thread_pool_;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_ROWLOCKNODE_ROWLOCK_DB_H_
