// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_RANDOM_H_
#define TERA_LOAD_BALANCER_RANDOM_H_

#include <assert.h>

#include <ctime>
#include <random>

#include "common/timer.h"

namespace tera {
namespace load_balancer {

class Random {
public:
    // random from [a, b)
    //  a < b should be ensured
    // can generate negative number
    // avg time cost: 25us
    static int RandStd(int a, int b) {
        assert(a < b);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(a, b - 1);

        return dis(gen);
    }

    // random from [a, b)
    // a < b should be ensured
    // can not generate negative number
    // avg time cost: 150ns
    static uint32_t RandTime(uint32_t a, uint32_t b) {
        assert(a < b);

        int64_t time_us = get_micros();
        return time_us % (b - a) + a;
    }

    // random from [a, b)
    // a < b should be ensured
    // can not generate negative number
    // avg time cost: 15ns
    static uint32_t Rand(uint32_t a, uint32_t b) {
        assert(a < b);

        uint32_t rand = xorshift32();
        return rand % (b - a) + a;
    }

private:
    /* The state word must be initialized to non-zero */
    static uint32_t xorshift32() {
        /* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
        static uint32_t state = time(NULL);
        uint32_t x = state;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        state = x;
        return x;
    }
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_RANDOM_H_
