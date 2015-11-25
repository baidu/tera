// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdint.h>
#include <stdio.h>

#include <boost/bind.hpp>

#include "gtest/gtest.h"

#include "common/mutex.h"
#include "common/thread_pool.h"
#include "counter.h"

namespace tera {

Mutex mutex;
int ref = 0;
int loop_num = 100000;
int thread_num = 1000;

void callback_add(Counter* counter) {
    for (int i = 0; i < loop_num; ++i) {
        counter->Add(100000);
    }
    MutexLock lock(&mutex);
//    std::cout << "add: " << counter->Get() << std::endl;
    ref--;
}

void callback_sub(Counter* counter) {
    for (int i = 0; i < loop_num; ++i) {
        counter->Sub(100000);
    }
    MutexLock lock(&mutex);
//    std::cout << "sub: " << counter->Get() << std::endl;
    ref--;
}

void callback_inc(Counter* counter) {
    for (int i = 0; i < loop_num; ++i) {
        counter->Inc();
    }
    MutexLock lock(&mutex);
//    std::cout << "inc: " << counter->Get() << std::endl;
    ref--;
}

void callback_dec(Counter* counter) {
    for (int i = 0; i < loop_num; ++i) {
        counter->Dec();
    }
    MutexLock lock(&mutex);
//    std::cout << "dec: " << counter->Get() << std::endl;
    ref--;
}

void callback_clear(Counter* counter) {
    for (int i = 0; i < loop_num / 300; ++i) {
        ASSERT_GE(counter->Clear(), 0);
    }
    MutexLock lock(&mutex);
//    std::cout << "clear: " << counter->Get() << std::endl;
    ref--;
}

TEST(CounterTest, Basic) {
    Counter counter;
    ThreadPool* pool = new ThreadPool(thread_num);
    for (int i = 0; i < thread_num / 4; ++i) {
        boost::function<void ()> callback =
            boost::bind(&callback_add, &counter);
        pool->AddTask(callback);

        callback = boost::bind(&callback_sub, &counter);
        pool->AddTask(callback);

        callback = boost::bind(&callback_inc, &counter);
        pool->AddTask(callback);

        callback = boost::bind(&callback_dec, &counter);
        pool->AddTask(callback);

        MutexLock locker(&mutex);
        ref += 4;
    }
    while (1) {
        MutexLock locker(&mutex);
        if (ref == 0) {
            break;
        }
    }
    ASSERT_EQ(counter.Get(), 0);
    delete pool;
}

TEST(CounterTest, Clear) {
    Counter counter;
    ThreadPool* pool = new ThreadPool(thread_num);
    for (int i = 0; i < thread_num / 3; ++i) {
        boost::function<void ()> callback =
            boost::bind(&callback_add, &counter);
        pool->AddTask(callback);

        callback = boost::bind(&callback_inc, &counter);
        pool->AddTask(callback);

        callback = boost::bind(&callback_clear, &counter);
        pool->AddTask(callback);

        MutexLock lock(&mutex);
        ref += 3;
    }
    while (1) {
        MutexLock lock(&mutex);
        if (ref == 0) {
            break;
        }
    }
    ASSERT_GE(counter.Clear(), 0);
    ASSERT_EQ(counter.Get(), 0);
    delete pool;
}

} // namespace tera
