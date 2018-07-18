// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. 

#include <iostream>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "observer/rowlocknode/rowlock_db.h"
#include "common/counter.h"

DECLARE_int32(rowlock_timing_wheel_patch_num);

namespace tera {
namespace observer {

class LockTest {
public:
    void Lock(tera::observer::ShardedRowlockDB* db, Counter* succeed) {
        for (uint32_t i = 0; i < 10; ++i) {
            uint64_t key = 1;

            if (db->TryLock(key) == true) {
                succeed->Inc();
            }
        }
    }
};

TEST(ShardedRowlockDB, LockTest) {
    ShardedRowlockDB db;

    // test for lock
    EXPECT_EQ(0, db.Size());

    // different keys
    EXPECT_TRUE(db.TryLock(0));
    EXPECT_TRUE(db.TryLock(1));
    EXPECT_TRUE(db.TryLock(2));

    // same key that has been locked
    EXPECT_FALSE(db.TryLock(0));
    EXPECT_FALSE(db.TryLock(1));
    EXPECT_FALSE(db.TryLock(2));

    // test for unlock
    db.UnLock(0);
    EXPECT_TRUE(db.TryLock(0));

    // unlock for other locked keys
    EXPECT_FALSE(db.TryLock(1));
    EXPECT_FALSE(db.TryLock(2));

    // double unlock
    db.UnLock(0);
    db.UnLock(0);
    EXPECT_TRUE(db.TryLock(0));

    // unlock size
    EXPECT_EQ(3, db.Size());
    db.UnLock(0);
    EXPECT_EQ(2, db.Size());
    db.UnLock(0);
    EXPECT_EQ(2, db.Size());
    db.UnLock(1);
    EXPECT_EQ(1, db.Size());
    db.UnLock(2);
    EXPECT_EQ(0, db.Size());

    // test for ClearTimeout
    for (int32_t i = 0; i < FLAGS_rowlock_timing_wheel_patch_num; ++i) {
        // all keys will not be unlocked until timeing wheel works
        EXPECT_TRUE(db.TryLock(i));
        EXPECT_EQ(i + 1, db.Size());
        db.ClearTimeout();
    }

    // timing wheel has run a circle, oldest key will be unlocked
    EXPECT_EQ(FLAGS_rowlock_timing_wheel_patch_num - 1, db.Size());

    // unlock the second oldest key
    db.ClearTimeout();
    EXPECT_EQ(FLAGS_rowlock_timing_wheel_patch_num - 2, db.Size());

    // test for ClearTimeout multi keys
    for (int32_t i = 0; i < FLAGS_rowlock_timing_wheel_patch_num; ++i) {
        // all keys will not be unlocked until timeing wheel works
        EXPECT_TRUE(db.TryLock(i * 10 + 1000000));
        EXPECT_TRUE(db.TryLock(i * 10 + 1000001));
        EXPECT_TRUE(db.TryLock(i * 10 + 1000002));
        db.ClearTimeout();
    }

    // timing wheel has run a circle, oldest 3 keys will be unlocked
    EXPECT_EQ(FLAGS_rowlock_timing_wheel_patch_num * 3 - 3, db.Size());

    // unlock the oldest 3 keys
    db.ClearTimeout();
    EXPECT_EQ(FLAGS_rowlock_timing_wheel_patch_num * 3 - 6, db.Size());
}

TEST(RowlockDB, LockTest) {
    RowlockDB db;

    // test for lock
    EXPECT_EQ(0, db.Size());

    // different keys
    EXPECT_TRUE(db.TryLock(0));
    EXPECT_TRUE(db.TryLock(1));
    EXPECT_TRUE(db.TryLock(2));

    // same key that has been locked
    EXPECT_FALSE(db.TryLock(0));
    EXPECT_FALSE(db.TryLock(1));
    EXPECT_FALSE(db.TryLock(2));

    // test for unlock
    db.UnLock(0);
    EXPECT_TRUE(db.TryLock(0));

    // unlock for other locked keys
    EXPECT_FALSE(db.TryLock(1));
    EXPECT_FALSE(db.TryLock(2));

    // double unlock
    db.UnLock(0);
    db.UnLock(0);
    EXPECT_TRUE(db.TryLock(0));

    // unlock size
    EXPECT_EQ(3, db.Size());
    db.UnLock(0);
    EXPECT_EQ(2, db.Size());
    db.UnLock(0);
    EXPECT_EQ(2, db.Size());
    db.UnLock(1);
    EXPECT_EQ(1, db.Size());
    db.UnLock(2);
    EXPECT_EQ(0, db.Size());

    // test for ClearTimeout
    for (int32_t i = 0; i < FLAGS_rowlock_timing_wheel_patch_num; ++i) {
        // all keys will not be unlocked until timeing wheel works
        EXPECT_TRUE(db.TryLock(i));
        EXPECT_EQ(i + 1, db.Size());
        db.ClearTimeout();
    }

    // timing wheel has run a circle, oldest key will be unlocked
    EXPECT_EQ(FLAGS_rowlock_timing_wheel_patch_num - 1, db.Size());

    // unlock the second oldest key
    db.ClearTimeout();
    EXPECT_EQ(FLAGS_rowlock_timing_wheel_patch_num - 2, db.Size());
}

TEST(ShardedRowlockDB, ParaTest) {
    Counter counter;
    ShardedRowlockDB db;
    LockTest test;

    // 10 threads to lock the same key
    ThreadPool thread_pool(10);
    for (uint32_t i = 0; i < 10; ++i) {
        ThreadPool::Task task = std::bind(&LockTest::Lock, &test, &db, &counter);
        thread_pool.AddTask(task);
    }
    sleep(1);
    EXPECT_EQ(1, db.Size());
    EXPECT_EQ(1, counter.Get());

    for (int32_t i = 0; i < FLAGS_rowlock_timing_wheel_patch_num; ++i) {
        db.ClearTimeout();
    }
    EXPECT_EQ(0, db.Size());
}

} // namespace observer
} // namespace tera
