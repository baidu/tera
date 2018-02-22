// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_HEAP_PROFILER_H
#define TERA_HEAP_PROFILER_H

#include <atomic>
#include <thread>
#include <mutex>
#include <string>
#include <condition_variable>
#include <cstdlib>

#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_int64(heap_profile_allocation_interval);
DECLARE_int64(heap_profile_inuse_interval);

namespace tera {

class HeapProfiler {
public:

    /**
     * @brief Init HeapProfiler and the detect thread will start
    **/
    HeapProfiler();
    /**
     * @brief: the heap profiler will stop after descontrucor called 
     *
    **/
    ~HeapProfiler();

    HeapProfiler& SetEnable(bool enable) {
        enable_ = enable;

        if (enable_) {
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
        cv_.notify_one();
        return *this;
    }
    
    HeapProfiler& SetInterval(int second) {
        interval_ = std::chrono::seconds(second);
        cv_.notify_one();
        return *this;
    }

    HeapProfiler& SetProfilerFile(const std::string& file) {
        profiler_file_ = file;
        cv_.notify_one();
        return *this;
    }

private:
    void run();
private:
    std::atomic<bool> exit_;
    bool enable_{false};
    std::chrono::seconds interval_{10};
    std::string profiler_file_;
    std::thread thread_;
    std::mutex lock_;
    std::condition_variable cv_;
};

} // namespace tera

#endif  //TERA_HEAP_PROFILER

/* vim: set ts=4 sw=4 sts=4 tw=100 */