// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <thread>
#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include "sdk_task.h"
#include "common/counter.h"
#include "common/timer.h"

using std::string;
using namespace std::placeholders;
DEFINE_int32(thread_num, 10, "thread number for TimeoutManager ThreadPool");
DEFINE_int32(perf_test_thead_num, 10, "thread number of put/pop");
DEFINE_int32(perf_test_duration, 2, "seconds for performance test");
namespace tera {

#define YELLOW "\033[33m" /* Yellow */

static Counter callback_called_times = Counter();
static Counter task_counter = Counter();

class TestTask : public SdkTask {
 public:
  std::string dummy_key;

  TestTask() : SdkTask(SdkTask::READ) {}
  virtual ~TestTask() {}

  bool IsAsync() { return false; }
  uint32_t Size() { return 0; }
  void SetTimeOut(int64_t timeout) {}
  int64_t TimeOut() { return 0; }
  void Wait() {}
  void SetError(ErrorCode::ErrorCodeType err, const std::string& reason) {}
  std::string InternalRowKey() { return dummy_key; }
  int64_t GetCommitTimes() { return 0; }
  void RunCallback() { abort(); }
};

class SdkTimeoutManagerTest : public ::testing::Test {
 public:
  SdkTimeoutManagerTest() : thread_pool_(FLAGS_thread_num), timeout_manager_(NULL) {}

  virtual void SetUp() {
    timeout_manager_ = new SdkTimeoutManager(&thread_pool_);
    ASSERT_TRUE(timeout_manager_ != NULL);
    callback_called_times.Clear();
    task_counter.Clear();
  }
  virtual void TearDown() { delete timeout_manager_; }

 private:
  common::ThreadPool thread_pool_;
  SdkTimeoutManager* timeout_manager_ = NULL;
};

static void TimeoutFunc(SdkTask* task) { callback_called_times.Add(1); }

static SdkTask::TimeoutFunc timeout_func = std::bind(TimeoutFunc, _1);

TEST_F(SdkTimeoutManagerTest, PutTaskPopTaskTest) {
  const int32_t LOOP_CNT = 10000;
  int64_t put_start_time = get_micros();
  bool succ = true;
  for (int32_t i = 0; i < LOOP_CNT; ++i) {
    TestTask* sdk_task = new TestTask();
    sdk_task->SetId(LOOP_CNT - i);
    succ &= timeout_manager_->PutTask(sdk_task, 5000, timeout_func);
  }
  EXPECT_TRUE(succ);
  int64_t put_done_time = get_micros();

  uint32_t task_cnt = 0;
  for (uint32_t i = 0; i < SdkTimeoutManager::kShardNum; ++i) {
    uint32_t shard_due_cnt = timeout_manager_->map_shard_[i].due_time_map.size();
    EXPECT_EQ(shard_due_cnt, timeout_manager_->map_shard_[i].id_hash_map.size());
    task_cnt += shard_due_cnt;
  }
  EXPECT_EQ(task_cnt, LOOP_CNT);

  int64_t pop_start_time = get_micros();
  for (uint32_t shard_idx = 0; shard_idx < SdkTimeoutManager::kShardNum; ++shard_idx) {
    SdkTimeoutManager::DueTimeMap& due_time_map =
        timeout_manager_->map_shard_[shard_idx].due_time_map;
    uint32_t shard_task_cnt = due_time_map.size();
    uint32_t shard_pop_cnt = 0;
    while (!due_time_map.empty()) {
      SdkTask* task = timeout_manager_->PopTask((*due_time_map.begin())->GetId());
      EXPECT_TRUE(task != NULL);
      shard_pop_cnt += 1;
      delete static_cast<TestTask*>(task);
    }
    EXPECT_EQ(shard_pop_cnt, shard_task_cnt);
  }
  int64_t pop_done_time = get_micros();

  std::cout << YELLOW << "SdkTimeoutManager performance(single thread): "
            << "\n\t\tPutTask: "
            << int(LOOP_CNT / ((put_done_time - put_start_time + 1) / 1000000.0))
            << "\n\t\tPopTask: "
            << int(LOOP_CNT / ((pop_done_time - pop_start_time + 1) / 1000000.0)) << std::endl;
}

TEST_F(SdkTimeoutManagerTest, CheckTimeout) {
  const int32_t LOOP_CNT = 10000;
  std::vector<TestTask*> tasks;
  tasks.reserve(LOOP_CNT);
  bool succ = true;
  for (int32_t i = 0; i < LOOP_CNT; ++i) {
    TestTask* sdk_task = new TestTask();
    sdk_task->SetId(i + 1);
    succ &= timeout_manager_->PutTask(sdk_task, 500, timeout_func);
    tasks.push_back(sdk_task);
  }
  EXPECT_TRUE(true);
  // waiting until all SdkTasks have been check timeout and their TimeoutFunc
  // been put to thread pool to execute
  for (uint32_t shard = 0; shard < SdkTimeoutManager::kShardNum; ++shard) {
    while (!timeout_manager_->map_shard_[shard].due_time_map.empty()) {
      usleep(timeout_manager_->timeout_precision_);
    }
  }
  // waiting another 100ms until all TimeoutFunc in thread_pool have been done
  usleep(250000);
  EXPECT_EQ(callback_called_times.Get(), LOOP_CNT);

  TestTask* sdk_task = new TestTask();
  sdk_task->SetId(100);
  EXPECT_TRUE(timeout_manager_->PutTask(sdk_task, 500, timeout_func));
  tasks.push_back(sdk_task);
  EXPECT_FALSE(timeout_manager_->PutTask(sdk_task, 500, timeout_func));

  sdk_task = new TestTask();
  sdk_task->SetId(100);
  EXPECT_FALSE(timeout_manager_->PutTask(sdk_task, 500, timeout_func));
  tasks.push_back(sdk_task);

  usleep(1000);
  sdk_task = new TestTask();
  sdk_task->SetId(100);
  EXPECT_FALSE(timeout_manager_->PutTask(sdk_task, 500, timeout_func));
  tasks.push_back(sdk_task);
  // waiting until all SdkTasks have been check timeout and their TimeoutFunc
  // been put to thread pool to execute
  for (uint32_t shard = 0; shard < SdkTimeoutManager::kShardNum; ++shard) {
    while (!timeout_manager_->map_shard_[shard].due_time_map.empty()) {
      usleep(timeout_manager_->timeout_precision_);
    }
  }
  // waiting another 100ms until all TimeoutFunc in thread_pool have been done
  usleep(250000);
  EXPECT_EQ(callback_called_times.Get(), 1 + LOOP_CNT);
  for (std::size_t i = 0; i < tasks.size(); ++i) {
    delete tasks[i];
  }
}

static bool add_task_run = true;
static void AddTaskFunc(SdkTimeoutManager* mgr, int64_t timeout) {
  while (add_task_run) {
    SdkTask* task = new TestTask();
    task->SetId(task_counter.Add(1));
    mgr->PutTask(task, timeout, timeout_func);
  }
}

static void PopTaskFunc(SdkTimeoutManager* mgr) {
  int64_t task_id;
  while ((task_id = task_counter.Sub(1) + 1) > 0) {
    SdkTask* task = mgr->PopTask(task_id);
    delete static_cast<TestTask*>(task);
  }
}

TEST_F(SdkTimeoutManagerTest, PutPopPerformance) {
  std::vector<std::thread> threads;
  threads.reserve(FLAGS_perf_test_thead_num);
  add_task_run = true;
  int64_t timeout = FLAGS_perf_test_duration * 1000 + 1000;
  for (int32_t i = 0; i < FLAGS_perf_test_thead_num; ++i) {
    threads.emplace_back(std::thread(std::bind(&AddTaskFunc, timeout_manager_, timeout)));
  }
  sleep(FLAGS_perf_test_duration);
  add_task_run = false;
  for (std::size_t i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }
  threads.clear();
  int64_t task_cnt = task_counter.Get();

  int64_t pop_start_time = get_micros();
  for (int i = 0; i < FLAGS_perf_test_thead_num; ++i) {
    threads.emplace_back(std::thread(std::bind(PopTaskFunc, timeout_manager_)));
  }
  for (std::size_t i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }
  int64_t pop_end_time = get_micros();
  std::cout << YELLOW << "SdkTimeoutManager performance(" << FLAGS_perf_test_thead_num
            << " put/pop threads): "
            << "\n\t\tPutTask: " << task_cnt / FLAGS_perf_test_duration
            << "\n\t\tPopTask: " << int(task_cnt / ((pop_end_time - pop_start_time) / 1000000.0))
            << std::endl;
}

TEST_F(SdkTimeoutManagerTest, CheckTimeoutPerformance) {
  common::ThreadPool thread_pool(FLAGS_thread_num);
  SdkTimeoutManager* timeout_mgr = new SdkTimeoutManager(&thread_pool);

  std::vector<std::thread> threads;
  threads.reserve(FLAGS_perf_test_thead_num);
  add_task_run = true;
  // timeout set to 1us
  int64_t timeout = 1;
  int64_t start_time = get_micros();
  for (int32_t i = 0; i < FLAGS_perf_test_thead_num; ++i) {
    threads.emplace_back(std::thread(std::bind(&AddTaskFunc, timeout_mgr, timeout)));
  }
  sleep(FLAGS_perf_test_duration);
  add_task_run = false;
  int64_t end_time = get_micros();
  for (std::size_t i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }
  threads.clear();
  int64_t callback_run_cnt = callback_called_times.Get();
  int64_t pending_cnt = task_counter.Get() - callback_run_cnt;
  delete timeout_mgr;

  std::cout << YELLOW << "SdkTimeoutManager performance@CheckTimeout(" << FLAGS_perf_test_thead_num
            << " put threads, " << FLAGS_thread_num << "TimeoutFunc run threads): "
            << "\n\t\tPutTask: " << task_counter.Get() / FLAGS_perf_test_duration
            << "\n\t\tPending: " << pending_cnt / FLAGS_perf_test_duration
            << "\n\t\tCheckTimeout: " << callback_run_cnt / FLAGS_perf_test_duration << ","
            << int(task_counter.Get() / ((end_time - start_time) / 1000000.0)) << std::endl;
}

}  // namespace tera
