// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include <functional>
#include <iostream>

#include "gtest/gtest.h"

#include "common/mutex.h"
#include "common/thread_pool.h"
#include "common/timer.h"

namespace tera {

TEST(TimerTest, Basic) {
    struct timespec ts1, ts2, ts3;
    struct timeval tv;

    clock_gettime(CLOCK_MONOTONIC, &ts1);
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts3);
    clock_gettime(CLOCK_REALTIME, &ts2);
    gettimeofday(&tv, NULL);

    std::cout << "ts1.tv_sec " << ts1.tv_sec
        << ", ts1.tv_nsec " << ts1.tv_nsec
        << std::endl;
    std::cout << "ts2.tv_sec " << ts2.tv_sec
        << ", ts2.tv_nsec " << ts2.tv_nsec
        << std::endl;
    std::cout << "ts3.tv_sec " << ts3.tv_sec
        << ", ts3.tv_nsec " << ts3.tv_nsec
        << std::endl;
    std::cout << "tv.tv_sec " << tv.tv_sec
        << ", tv.tv_usec " << tv.tv_usec
        << std::endl;

    int delta = 0;
    delta = ts2.tv_sec - tv.tv_sec;
    ASSERT_TRUE(-1 <= delta && delta <= 1);
    ASSERT_TRUE(ts1.tv_sec < ts2.tv_sec);
    ASSERT_TRUE(ts1.tv_sec < tv.tv_sec);
}

TEST(TimerTest, test1) {
    struct timespec ts1;
    struct timeval tv;

    clock_gettime(CLOCK_REALTIME, &ts1);
    gettimeofday(&tv, NULL);
    int64_t ts = common::timer::get_micros();

    int delta = 0;
    delta = ts1.tv_sec - tv.tv_sec;
    ASSERT_TRUE(-1 <= delta && delta <= 1);
    delta = ts / 1000000 - tv.tv_sec;
    ASSERT_TRUE(-1 <= delta && delta <= 1);
}

common::Mutex mu;
common::CondVar cv(&mu);

void DelayTask_issue1(int32_t time, int32_t time_ms) {
    struct timespec ts1;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    int delta = ts1.tv_sec - (time + time_ms / 1000);
    ASSERT_TRUE(-1 <= delta && delta <= 1);
    cv.Signal();
    return;
}

TEST(ThreadPoolTest, Basic) {
    mu.Lock();
    common::ThreadPool* pool = new common::ThreadPool(1000);
    struct timespec ts1;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    ThreadPool::Task task =
        std::bind(&DelayTask_issue1, ts1.tv_sec, 5000);
    pool->DelayTask(5000, task);

    cv.Wait();
    mu.Unlock();
    delete pool;
}

} // namespace tera
