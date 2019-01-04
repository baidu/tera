// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gperftools/malloc_extension.h>

#include "common/metric/tcmalloc_collector.h"
#include "common/timer.h"
#include "gflags/gflags.h"

DEFINE_int64(tera_tcmalloc_collect_period_second, 60,
             "tcmalloc metrics checking period (in second)");

namespace tera {
int64_t TcmallocCollector::Collect() {
  auto current_time_ms = get_millis();
  if (current_time_ms - last_check_ms_ >= FLAGS_tera_tcmalloc_collect_period_second * 1000) {
    size_t ret = 0;
    switch (type_) {
      case TcmallocMetricType::kInUse: {
        MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &ret);
        break;
      }
      case TcmallocMetricType::kCentralCacheFreeList: {
        MallocExtension::instance()->GetNumericProperty("tcmalloc.central_cache_free_bytes", &ret);
        break;
      }
      case TcmallocMetricType::kThreadCacheFreeList: {
        MallocExtension::instance()->GetNumericProperty("tcmalloc.thread_cache_free_bytes", &ret);
        break;
      }
    }
    val_ = static_cast<int64_t>(ret);
    last_check_ms_ = current_time_ms;
  }
  return val_;
}
}  // end namespace tera
