// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <signal.h>
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gperftools/malloc_extension.h>
#include <fstream>

#include "common/base/scoped_ptr.h"
#include "common/log/log_cleaner.h"
#include "common/heap_profiler.h"
#include "common/cpu_profiler.h"
#include "tera/tera_entry.h"
#include "utils/utils_cmd.h"
#include "version.h"

DECLARE_string(tera_log_prefix);
DECLARE_string(tera_local_addr);
DECLARE_bool(tera_info_log_clean_enable);

DEFINE_bool(cpu_profiler_enabled, false, "enable cpu profiler");
DEFINE_bool(heap_profiler_enabled, false, "enable heap profiler");
DEFINE_int32(cpu_profiler_dump_interval, 120, "cpu profiler dump interval");
DEFINE_int32(heap_profiler_dump_interval, 120, "heap profiler dump interval");

extern std::string GetTeraEntryName();
extern tera::TeraEntry* GetTeraEntry();

volatile sig_atomic_t g_quit = 0;

static void DumpStringToFile(const std::string& s, const std::string& filename) {
  std::fstream file;
  file.open(filename, std::ios::out);
  if (!file.is_open()) {
    LOG(ERROR) << "Open file " << filename << " failed";
    return;
  }
  file << s;
  file.close();
}

static void SignalIntHandler(int sig) { g_quit = 1; }

// Dump a memory profile after receive SIRUSR1.
// NOTE: by default, tcmalloc does not do any heap sampling, and this
//       function will always return an empty sample.  To get useful
//       data from it, you must also set the environment
//       variable TCMALLOC_SAMPLE_PARAMETER to a value such as 524288(bytes).
static void SignalUsr1Handler(int sig) {
  static std::atomic<int64_t> idx{0};
  std::string str;
  MallocExtension::instance()->GetHeapSample(&str);
  DumpStringToFile(str, std::to_string(getpid()) + "." +
                            (std::to_string(idx.fetch_add(1)) + ".sample.heap").c_str());
}

// Dump current detail memory usage to file.
static void SignalUsr2Handler(int sig) {
  static std::atomic<int64_t> idx{0};
  char buffer[1024000];
  MallocExtension::instance()->GetStats(buffer, sizeof(buffer));
  DumpStringToFile(buffer, std::to_string(getpid()) + "." +
                               (std::to_string(idx.fetch_add(1)) + ".mem.detail").c_str());
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
  cpu_profiler.SetEnable(FLAGS_cpu_profiler_enabled).SetInterval(FLAGS_cpu_profiler_dump_interval);

  tera::HeapProfiler heap_profiler;
  heap_profiler.SetEnable(FLAGS_heap_profiler_enabled)
      .SetInterval(FLAGS_heap_profiler_dump_interval);

  if (argc > 1) {
    std::string ext_cmd = argv[1];
    if (ext_cmd == "version") {
      PrintSystemVersion();
      return 0;
    }
  }

  signal(SIGINT, SignalIntHandler);
  signal(SIGTERM, SignalIntHandler);
  signal(SIGUSR1, SignalUsr1Handler);
  signal(SIGUSR2, SignalUsr2Handler);

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
