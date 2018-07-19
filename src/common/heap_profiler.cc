// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <chrono>
#include <ctime>
#include <string.h>
#include <gperftools/heap-profiler.h>

#include "common/heap_profiler.h"

DEFINE_int64(heap_profile_allocation_interval, 1073741824, "Env variable for heap profiler's allocation interval");
DEFINE_int64(heap_profile_inuse_interval, 1073741824, "Env variable for heap profiler's inuse interval");


namespace tera {

HeapProfiler::HeapProfiler(const std::string& profiler_file):
    exit_(false),
    profiler_file_(profiler_file),
    thread_(&HeapProfiler::run, this) {}

HeapProfiler::~HeapProfiler() {
    exit_ = true;
    cv_.notify_one();
    thread_.join();
    if (IsHeapProfilerRunning()) {
        HeapProfilerStop();
    }
}

void HeapProfiler::run() {
    while (!exit_.load()) {
        bool enable;
        {
            std::unique_lock<std::mutex> lock(lock_);
            enable = enable_;
        }
        if (enable) {
            // "reason" is time
            std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            char ts[128];
            ctime_r(&t, ts);
            ts[strlen(ts) - 1] = '\0'; // erase \n
            
            if (IsHeapProfilerRunning() == 0) {
                HeapProfilerStart(profiler_file_.c_str());
            }
            HeapProfilerDump(ts);
            LOG(INFO) << "[Heap Profiler] Heap Profiler Dumped";
        } else {
            if (IsHeapProfilerRunning()) {
                HeapProfilerStop();
            }
        }
        std::unique_lock<std::mutex> lock(lock_);
        cv_.wait_for(lock, interval_);
    }
}

HeapProfiler& HeapProfiler::SetEnable(bool enable) {
    if (enable) {
        setenv("HEAP_PROFILE_ALLOCATION_INTERVAL",
                std::to_string(FLAGS_heap_profile_allocation_interval).c_str(),
                1);

        setenv("HEAP_PROFILE_INUSE_INTERVAL",
                std::to_string(FLAGS_heap_profile_inuse_interval).c_str(),
                1);

        LOG(INFO) << "[Heap Profiler] HEAP_PROFILE_ALLOCATION_INTERVAL: "
                    << getenv("HEAP_PROFILE_ALLOCATION_INTERVAL");
        LOG(INFO) << "[Heap Profiler] HEAP_PROFILE_INUSE_INTERVAL: "
                    << getenv("HEAP_PROFILE_INUSE_INTERVAL");
        LOG(INFO) << "[Heap Profiler] Heap Profiler Enabled";
    } else {
        unsetenv("HEAP_PROFILE_ALLOCATION_INTERVAL");
        unsetenv("HEAP_PROFILE_INUSE_INTERVAL");
        LOG(INFO) << "[Heap Profiler] Heap Profiler Disabled";
    }

    {
        std::unique_lock<std::mutex> lock(lock_);
        enable_ = enable;
    }

    cv_.notify_one();
    return *this;
}

} // namespace tera