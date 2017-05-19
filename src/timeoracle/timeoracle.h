// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TIMEORACLE_TIMEORACLE_H_
#define TERA_TIMEORACLE_TIMEORACLE_H_

#include <atomic>
#include <time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

namespace tera {
namespace timeoracle {

constexpr uint64_t kNanoPerSecond = 1000000000ULL;

inline uint64_t clock_realtime_ns() {
    struct timespec tp;
    ::clock_gettime(CLOCK_REALTIME, &tp);
    return tp.tv_sec * kNanoPerSecond + tp.tv_nsec;
}

class Timeoracle {
public:
    Timeoracle(uint64_t start_timestamp) : start_timestamp_(start_timestamp),
        limit_timestamp_(0) {
    }

    // if num == 0, see next timstamp
    uint64_t GetTimestamp(uint64_t num) {
        uint64_t start_timestamp = start_timestamp_.fetch_add(num);

        if ((start_timestamp + num) >= limit_timestamp_) {
            return 0;
        }

        return start_timestamp;
    }

    uint64_t UpdateLimitTimestamp(uint64_t limit_timestamp) {
        if (limit_timestamp > limit_timestamp_) {
            limit_timestamp_ = limit_timestamp;
        } else {
            LOG(ERROR) << "update limit timestamp failed, limit_timestamp_=" << limit_timestamp_
                << ",update to " << limit_timestamp;
            return 0;
        }
        return limit_timestamp;
    }

    void UpdateStartTimestamp() {
        const uint64_t cur_timestamp_ns = UniqueTimestampNs();
        /*
        if (cur_timestamp_ns >= limit_timestamp_) {
            return ;
        }
        */

        while (1) {
            uint64_t start_timestamp = start_timestamp_;
            if (start_timestamp < cur_timestamp_ns) {
                if (start_timestamp_.compare_exchange_strong(start_timestamp, cur_timestamp_ns)) {
                    return ;
                }
                continue;
            }
            uint64_t limit_timestamp = limit_timestamp_;
            if (start_timestamp > limit_timestamp) {
                if (start_timestamp_.compare_exchange_strong(start_timestamp, limit_timestamp)) {
                    return ;
                }
                continue;
            }
            break;
        }
    }

    uint64_t GetStartTimestamp() const {
        return start_timestamp_;
    }

    uint64_t GetLimitTimestamp() const {
        return limit_timestamp_;
    }

private:
    std::atomic<uint64_t>  start_timestamp_;
    std::atomic<uint64_t>  limit_timestamp_;

public:
    static uint64_t UniqueTimestampNs() {
        while (true) {
            uint64_t ts = clock_realtime_ns();
            uint64_t last_timestamp_ns = s_last_timestamp_ns;

            if (ts <= last_timestamp_ns) {
                return s_last_timestamp_ns.fetch_add(1) + 1 ;
            }

            if (s_last_timestamp_ns.compare_exchange_strong(last_timestamp_ns, ts)) {
                return ts;
            }
        }
    }

private:
    static std::atomic<uint64_t>    s_last_timestamp_ns;
};

} // namespace timeoracle
} // namespace tera

#endif // TERA_TIMEORACLE_TIMEORACLE_H_
