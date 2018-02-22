// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. 

#include <string>

#include <gperftools/profiler.h>
#include <gperftools/heap-profiler.h>

#include "gtest/gtest.h"
 
#include "common/cpu_profiler.h" 
#include "common/heap_profiler.h" 
#include "common/this_thread.h" 
 
namespace tera { 
 
class ProfilerTest : public ::testing::Test {
public:
    virtual void SetUp() {}
    
    virtual void TearDown() {}

private:
    CpuProfiler cpu_profiler_;
    HeapProfiler heap_profiler_;
};

TEST_F(ProfilerTest, SetEnableTest) {
    ProfilerState ps;
    EXPECT_FALSE(cpu_profiler_.enable_);
    EXPECT_FALSE(heap_profiler_.enable_);
    ProfilerGetCurrentState(&ps);
    EXPECT_FALSE(ps.enabled);
    EXPECT_FALSE(IsHeapProfilerRunning());

    cpu_profiler_.SetProfilerFile("Cpu")
                 .SetEnable(true);

    heap_profiler_.SetProfilerFile("Heap")
                  .SetEnable(true);

    EXPECT_TRUE(cpu_profiler_.enable_);
    EXPECT_TRUE(heap_profiler_.enable_);

    ThisThread::Sleep(2000);
    ProfilerGetCurrentState(&ps);
    EXPECT_TRUE(ps.enabled);
    EXPECT_TRUE(IsHeapProfilerRunning());

    cpu_profiler_.SetEnable(false);
    heap_profiler_.SetEnable(false);
    
    EXPECT_FALSE(cpu_profiler_.enable_);
    EXPECT_FALSE(heap_profiler_.enable_);

    ThisThread::Sleep(2000);
    ProfilerGetCurrentState(&ps);
    EXPECT_FALSE(ps.enabled);
    EXPECT_FALSE(IsHeapProfilerRunning());
}

TEST_F(ProfilerTest, SetInvervalTest) {
    EXPECT_EQ(cpu_profiler_.interval_, std::chrono::seconds(10));
    EXPECT_EQ(heap_profiler_.interval_, std::chrono::seconds(10));
    cpu_profiler_.SetInterval(1000);
    heap_profiler_.SetInterval(2000);
    EXPECT_EQ(cpu_profiler_.interval_, std::chrono::seconds(1000));
    EXPECT_EQ(heap_profiler_.interval_, std::chrono::seconds(2000));
}

TEST_F(ProfilerTest, SetProfilerFileTest) {
    EXPECT_EQ(cpu_profiler_.profiler_file_, std::string(""));
    EXPECT_EQ(heap_profiler_.profiler_file_, std::string(""));
    cpu_profiler_.SetProfilerFile("Good");
    heap_profiler_.SetProfilerFile("Bad");
    EXPECT_EQ(cpu_profiler_.profiler_file_, std::string("Good"));
    EXPECT_EQ(heap_profiler_.profiler_file_, std::string("Bad"));
}
} // end namespace tera 
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

