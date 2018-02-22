// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <chrono>
#include <ctime>
#include <string.h>
#include <gperftools/heap-profiler.h>

#include "common/heap_profiler.h"

namespace tera {

HeapProfiler::HeapProfiler() 
    : exit_(false),
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
        if (enable_) {
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

} // namespace tera