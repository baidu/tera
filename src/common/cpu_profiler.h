// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_CPU_PROFILER_H
#define TERA_CPU_PROFILER_H

#include <atomic>
#include <thread>
#include <string>
#include <mutex>
#include <condition_variable>

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace tera {

class CpuProfiler {
public:
    /**
     * @brief Init CpuProfiler and the detect thread will start
    **/
    CpuProfiler();

    ~CpuProfiler();

    CpuProfiler& SetEnable(bool enable) {
        enable_ = enable;
        if (enable_) {
            LOG(INFO) << "[Cpu Profiler] Cpu Profiler Enabled";
        } else {
            LOG(INFO) << "[Cpu Profiler] Cpu Profiler Disabled";
        }
        cv_.notify_one();
        return *this;
    }

    CpuProfiler& SetInterval(int second) {
        interval_ = std::chrono::seconds(second);
        cv_.notify_one();
        return *this;
    }

    CpuProfiler& SetProfilerFile(const std::string& file) {
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

#endif  //TERA_CPU_PROFILER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
