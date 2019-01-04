// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include "master/procedure_limiter.h"

namespace tera {
namespace master {
namespace test {

class ProcedureLimiterTest : public ::testing::Test {
 public:
  ProcedureLimiterTest() {}
  virtual ~ProcedureLimiterTest() {}

  virtual void SetUp() {}
  virtual void TearDown() {}

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
};

class Worker {
 public:
  Worker(const ProcedureLimiter::LockType& type, size_t i)
      : type_(type), index_(i), finished_(false) {}

  Worker(const Worker& w) : type_(w.type_), index_(w.index_), finished_(w.finished_) {}

  Worker(Worker&& w) : type_(w.type_), index_(w.index_), finished_(w.finished_) {}

  void Work() {
    {
      std::unique_lock<std::mutex> lk(mutex_);
      cv_.wait(lk, [this] { return this->finished_ == true; });
    }
    ProcedureLimiter::Instance().ReleaseLock(type_);
  }

  void Start() { ProcedureLimiter::Instance().GetLock(type_); }

  void Finish() {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      finished_ = true;
    }
    cv_.notify_one();
  }

 private:
  ProcedureLimiter::LockType type_;
  std::condition_variable cv_;
  std::mutex mutex_;
  bool finished_;
  size_t index_;
};

TEST_F(ProcedureLimiterTest, NoLimitTypeTest) {
  for (size_t i = 0; i < 100; ++i) {
    ASSERT_TRUE(ProcedureLimiter::Instance().GetLock(ProcedureLimiter::LockType::kNoLimit));
  }
}

TEST_F(ProcedureLimiterTest, LargeConcurrencyTest) {
  const ProcedureLimiter::LockType type = ProcedureLimiter::LockType::kMerge;
  const uint32_t lock_num = 20;
  ProcedureLimiter::Instance().SetLockLimit(type, lock_num);
  ASSERT_EQ(lock_num, ProcedureLimiter::Instance().GetLockLimit(type));

  // The reserve() is very important, vector may call class constructor when copacity grow, it may
  // have side effect!!!
  std::vector<Worker> workers;
  workers.reserve(lock_num);
  std::vector<std::thread> threads;
  threads.reserve(lock_num);

  for (size_t i = 0; i < lock_num; ++i) {
    workers.emplace_back(type, i);
    workers[i].Start();
    threads.emplace_back(&Worker::Work, &workers[i]);
  }

  // wait for the cv_ to enter waiting state
  std::this_thread::sleep_for(std::chrono::seconds(1));

  ASSERT_EQ(lock_num, ProcedureLimiter::Instance().GetLockInUse(type));
  ASSERT_FALSE(ProcedureLimiter::Instance().GetLock(type));

  for (size_t i = 0; i < lock_num; ++i) {
    workers[i].Finish();
    threads[i].join();
  }

  ASSERT_EQ(0, ProcedureLimiter::Instance().GetLockInUse(type));
}

TEST_F(ProcedureLimiterTest, MultiTypeTest) {
  ProcedureLimiter::Instance().SetLockLimit(ProcedureLimiter::LockType::kMerge, 10);
  ProcedureLimiter::Instance().SetLockLimit(ProcedureLimiter::LockType::kSplit, 10);

  Worker worker1(ProcedureLimiter::LockType::kMerge, 1);
  worker1.Start();
  std::thread work_thread_1(&Worker::Work, &worker1);

  Worker worker2(ProcedureLimiter::LockType::kMerge, 2);
  worker2.Start();
  std::thread work_thread_2(&Worker::Work, &worker2);

  Worker worker3(ProcedureLimiter::LockType::kSplit, 3);
  worker3.Start();
  std::thread work_thread_3(&Worker::Work, &worker3);

  // wait for the cv_ to enter waiting state
  std::this_thread::sleep_for(std::chrono::seconds(1));

  ASSERT_EQ(2, ProcedureLimiter::Instance().GetLockInUse(ProcedureLimiter::LockType::kMerge));
  ASSERT_EQ(1, ProcedureLimiter::Instance().GetLockInUse(ProcedureLimiter::LockType::kSplit));

  worker1.Finish();
  work_thread_1.join();
  ASSERT_EQ(1, ProcedureLimiter::Instance().GetLockInUse(ProcedureLimiter::LockType::kMerge));

  worker2.Finish();
  work_thread_2.join();
  ASSERT_EQ(0, ProcedureLimiter::Instance().GetLockInUse(ProcedureLimiter::LockType::kMerge));

  worker3.Finish();
  work_thread_3.join();
  ASSERT_EQ(0, ProcedureLimiter::Instance().GetLockInUse(ProcedureLimiter::LockType::kSplit));
}

TEST_F(ProcedureLimiterTest, IncLimitTest) {
  ProcedureLimiter::LockType type = ProcedureLimiter::LockType::kMerge;
  ProcedureLimiter::Instance().SetLockLimit(type, 1);

  Worker worker1(type, 1);
  worker1.Start();
  std::thread work_thread_1(&Worker::Work, &worker1);

  ASSERT_EQ(1, ProcedureLimiter::Instance().GetLockInUse(type));
  ASSERT_FALSE(ProcedureLimiter::Instance().GetLock(type));

  ProcedureLimiter::Instance().SetLockLimit(type, 2);

  Worker worker2(type, 2);
  worker2.Start();
  std::thread work_thread_2(&Worker::Work, &worker2);

  ASSERT_EQ(2, ProcedureLimiter::Instance().GetLockInUse(type));
  ASSERT_FALSE(ProcedureLimiter::Instance().GetLock(type));

  // wait for the cv_ to enter waiting state
  std::this_thread::sleep_for(std::chrono::seconds(1));

  worker1.Finish();
  work_thread_1.join();
  ASSERT_EQ(1, ProcedureLimiter::Instance().GetLockInUse(type));

  worker2.Finish();
  work_thread_2.join();
  ASSERT_EQ(0, ProcedureLimiter::Instance().GetLockInUse(type));
}

TEST_F(ProcedureLimiterTest, DecLimitTest) {
  ProcedureLimiter::LockType type = ProcedureLimiter::LockType::kMerge;
  ProcedureLimiter::Instance().SetLockLimit(type, 2);

  Worker worker1(type, 1);
  worker1.Start();
  std::thread work_thread_1(&Worker::Work, &worker1);
  ASSERT_EQ(1, ProcedureLimiter::Instance().GetLockInUse(type));

  Worker worker2(type, 2);
  worker2.Start();
  std::thread work_thread_2(&Worker::Work, &worker2);
  ASSERT_EQ(2, ProcedureLimiter::Instance().GetLockInUse(type));

  ASSERT_FALSE(ProcedureLimiter::Instance().GetLock(type));

  ProcedureLimiter::Instance().SetLockLimit(type, 1);

  // wait for the cv_ to enter waiting state
  std::this_thread::sleep_for(std::chrono::seconds(1));

  worker1.Finish();
  work_thread_1.join();
  ASSERT_EQ(1, ProcedureLimiter::Instance().GetLockInUse(type));

  ASSERT_FALSE(ProcedureLimiter::Instance().GetLock(type));

  worker2.Finish();
  work_thread_2.join();
  ASSERT_EQ(0, ProcedureLimiter::Instance().GetLockInUse(type));

  ASSERT_TRUE(ProcedureLimiter::Instance().GetLock(type));
  ASSERT_EQ(1, ProcedureLimiter::Instance().GetLockInUse(type));
  ProcedureLimiter::Instance().ReleaseLock(type);
  ASSERT_EQ(0, ProcedureLimiter::Instance().GetLockInUse(type));
}

}  // namespace test
}  // namespace master
}  // namespace tera
