// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMOM_METRIC_RATIO_COLLECTOR_H_
#define TERA_COMMOM_METRIC_RATIO_COLLECTOR_H_
 
#include <cmath> 
#include "common/metric/collector_report_publisher.h"
 
namespace tera { 

class RatioCollector : public Collector {
public:
    explicit RatioCollector(Counter* first_counter, 
                            Counter* second_counter, 
                            bool is_periodic = true):
        first_counter_(first_counter),
        second_counter_(second_counter),
        is_periodic_(is_periodic) {}

    int64_t Collect() override {
        if (NULL == first_counter_ || NULL == second_counter_) {
            return 0;
        } else {
            double ratio = (double)first_counter_->Get() / second_counter_->Get();
            if (is_periodic_) {
                first_counter_->Clear();
                second_counter_->Clear();
            }
            return isnan(ratio) ? -1 : static_cast<int64_t>(ratio * 100);
        }
    }
private:
    Counter* const first_counter_;
    Counter* const second_counter_;
    const bool is_periodic_;
};

} // end namespace tera 
 
#endif // TERA_COMMOM_METRIC_RATIO_COLLECTOR_H_
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

