// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_METRIC_COUNTER_COLLECTOR_H_
#define TERA_COMMON_METRIC_COUNTER_COLLECTOR_H_

#include "common/metric/collector.h"
#include "common/counter.h"
 
namespace tera { 

class CounterCollector : public Collector {
public:
    /// if is_periodic is true, the counter will be cleared when collect
    /// this parameter is usually true, but it's false with some instantaneous value
    /// Eg: read_pending_count, scan_pending_count, which can't be clear during collect.
    explicit CounterCollector(Counter* counter, 
                              bool is_periodic = true):
        counter_(counter), 
        is_periodic_(is_periodic) {}

    ~CounterCollector() override {}

    int64_t Collect() override {
        if (counter_ == NULL) {
            return -1;
        } else {
            return is_periodic_ ? counter_->Clear() : counter_->Get();
        }
    }
private:
    Counter* const counter_;
    const bool is_periodic_;
};
} // end namespace tera
 
#endif // TERA_COMMON_METRIC_COUNTER_COLLECTOR_H_
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */