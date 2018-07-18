// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TIMEORACLE_TIMEORACLE_H_
#define TERA_TIMEORACLE_TIMEORACLE_H_

#include <atomic>
#include <iostream>
#include <time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

namespace tera {
namespace timeoracle {

constexpr int64_t kTimestampPerMilliSecond = 10000ULL;
constexpr int64_t kTimestampPerSecond = kTimestampPerMilliSecond * 1000ULL;
constexpr int64_t kBaseTimestampMilliSecond = 1483200000000ULL; // 20170101 00:00

inline int64_t clock_realtime_ms() {
    struct timespec tp;
    ::clock_gettime(CLOCK_REALTIME, &tp);
    return tp.tv_sec * 1000ULL + tp.tv_nsec / 1000000ULL - kBaseTimestampMilliSecond;
}

class Timeoracle {
public:
    Timeoracle(int64_t start_timestamp) : start_timestamp_(start_timestamp),
        limit_timestamp_(0) {
    }

    // if num == 0, see next timstamp
    // if return 0, allocate timestamp failed
    int64_t GetTimestamp(int64_t num) {
        int64_t start_timestamp = start_timestamp_.fetch_add(num);

        if ((start_timestamp + num) >= limit_timestamp_) {
            return 0;
        }

        return start_timestamp;
    }

    int64_t UpdateLimitTimestamp(int64_t limit_timestamp) {
        if (limit_timestamp > limit_timestamp_) {
            limit_timestamp_ = limit_timestamp;
        } else {
            LOG(ERROR) << "update limit timestamp failed, limit_timestamp_=" << limit_timestamp_
                << ",update to " << limit_timestamp;
            return 0;
        }
        return limit_timestamp;
    }

    int64_t UpdateStartTimestamp() {
        const int64_t cur_timestamp = CurrentTimestamp();

        int64_t start_timestamp = 0;
        while (1) {
            start_timestamp = start_timestamp_;
            if (start_timestamp < cur_timestamp) {
                if (start_timestamp_.compare_exchange_strong(start_timestamp, cur_timestamp)) {
                    return cur_timestamp;
                }
                continue;
            }

            int64_t limit_timestamp = limit_timestamp_;
            if (start_timestamp > limit_timestamp) {
                if (start_timestamp_.compare_exchange_strong(start_timestamp, limit_timestamp)) {
                    LOG(WARNING) << "adjust start timestamp to limit timestamp " << limit_timestamp;
                    return limit_timestamp;
                }
                continue;
            }

            break;
        }

        LOG(INFO) << "ignore to adjust start timestamp, current timestamp is " << cur_timestamp;
        return start_timestamp;
    }

    int64_t GetStartTimestamp() const {
        return start_timestamp_;
    }

    int64_t GetLimitTimestamp() const {
        return limit_timestamp_;
    }

private:
    std::atomic<int64_t>  start_timestamp_;
    std::atomic<int64_t>  limit_timestamp_;

public:
    static int64_t UniqueTimestampMs() {
        while (true) {
            int64_t ts = clock_realtime_ms();
            int64_t last_timestamp_ms = s_last_timestamp_ms;

            if (ts <= last_timestamp_ms) {
                return s_last_timestamp_ms.fetch_add(1) + 1;
            }

            if (s_last_timestamp_ms.compare_exchange_strong(last_timestamp_ms, ts)) {
                return ts;
            }
        }
    }

    static int64_t CurrentTimestamp() {
        return UniqueTimestampMs() * kTimestampPerMilliSecond;
    }

private:
    static std::atomic<int64_t>    s_last_timestamp_ms;
};

} // namespace timeoracle
} // namespace tera

#endif // TERA_TIMEORACLE_TIMEORACLE_H_
