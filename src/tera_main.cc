// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <signal.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/scoped_ptr.h"
#include "common/log/log_cleaner.h"
#include "common/heap_profiler.h"
#include "common/cpu_profiler.h"
#include "tera_entry.h"
#include "utils/utils_cmd.h"
#include "version.h"

DECLARE_bool(cpu_profiler_enabled);
DECLARE_bool(heap_profiler_enabled);
DECLARE_int32(cpu_profiler_dump_interval);
DECLARE_int32(heap_profiler_dump_interval);
DECLARE_string(tera_log_prefix);
DECLARE_string(tera_local_addr);
DECLARE_bool(tera_info_log_clean_enable);

extern std::string GetTeraEntryName();
extern tera::TeraEntry* GetTeraEntry();

volatile sig_atomic_t g_quit = 0;

static void SignalIntHandler(int sig) {
    g_quit = 1;
}

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);


    if (FLAGS_tera_log_prefix.empty()) {
        FLAGS_tera_log_prefix = GetTeraEntryName();
        if (FLAGS_tera_log_prefix.empty()) {
            FLAGS_tera_log_prefix = "tera";
        }
    }
    tera::utils::SetupLog(FLAGS_tera_log_prefix);

    tera::CpuProfiler cpu_profiler;
    cpu_profiler.SetEnable(FLAGS_cpu_profiler_enabled)
                .SetInterval(FLAGS_cpu_profiler_dump_interval)
                .SetProfilerFile("Cpu");

    tera::HeapProfiler heap_profiler;
    heap_profiler.SetEnable(FLAGS_heap_profiler_enabled)
                 .SetInterval(FLAGS_heap_profiler_dump_interval)
                 .SetProfilerFile("Heap");

    if (argc > 1) {
        std::string ext_cmd = argv[1];
        if (ext_cmd == "version") {
            PrintSystemVersion();
            return 0;
        }
    }

    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);

    scoped_ptr<tera::TeraEntry> entry(GetTeraEntry());
    if (entry.get() == NULL) {
        return -1;
    }

    if (!entry->Start()) {
        return -1;
    }
    
    // start log cleaner
    if (FLAGS_tera_info_log_clean_enable) {
        common::LogCleaner::StartCleaner();
        LOG(INFO) << "start log cleaner";
    } else {
        LOG(INFO) << "log cleaner is disable";
    }

    while (!g_quit) {
        if (!entry->Run()) {
            LOG(ERROR) << "Server run error ,and then exit now ";
            break;
        }
    }
    if (g_quit) {
        LOG(INFO) << "received interrupt signal from user, will stop";
    }

    common::LogCleaner::StopCleaner();

    if (!entry->Shutdown()) {
        return -1;
    }

    return 0;
}
