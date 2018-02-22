// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <chrono>
#include <gperftools/profiler.h>

#include "common/cpu_profiler.h"

namespace tera {

CpuProfiler::CpuProfiler() 
    : exit_(false), 
    thread_(&CpuProfiler::run, this) {}

CpuProfiler::~CpuProfiler() {
    exit_ = true;
    cv_.notify_one();
    thread_.join();
    ProfilerState ps;
    ProfilerGetCurrentState(&ps);
    if (ps.enabled) {
        ProfilerStop();
    }
}

void CpuProfiler::run() {
    while (!exit_.load()) {
        if (enable_) {
            ProfilerState ps;
            ProfilerGetCurrentState(&ps);
            if (ps.enabled == 0) {
                ProfilerStart(profiler_file_.c_str());
            }

            ProfilerFlush();
            LOG(INFO) << "[Cpu Profiler] Cpu Profiler Dumped";
        } else {
            ProfilerState ps;
            ProfilerGetCurrentState(&ps);
            if (ps.enabled) {
                ProfilerStop();
            }
        }
        std::unique_lock<std::mutex> lock(lock_);
        cv_.wait_for(lock, interval_);
    }
}

} // namespace tera