// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "benchmark/tpcc/random_generator.h"

#include <assert.h>

namespace tera {
namespace tpcc {

RandomGenerator::RandomGenerator():c_({0,0,0}) {
    InitRandomState();
}

void RandomGenerator::InitRandomState() {
    memset(&rand_state_, 0, sizeof(rand_state_));
    int ret = initstate_r(static_cast<unsigned int>(time(NULL)),
                          rand_state_buf_,
                          sizeof(rand_state_buf_),
                          &rand_state_);
    assert(ret == 0);
}

NURandConstant RandomGenerator::GetRandomConstant() const {
    return c_;
}

void RandomGenerator::SetRandomConstant() {
    c_.c_last = GetRandom(0, 255);
    c_.c_id = GetRandom(0, 1023);
    c_.ol_i_id = GetRandom(0, 8191);
}

inline bool VarfiyConstantAvailableForRun(int run_last, int load_last) {
    int delta = run_last - load_last;
    delta = delta > 0 ? delta : -1 * delta;
    return 65 <=delta && delta <= 119 && delta != 96 && delta != 112;
}

void RandomGenerator::SetRandomConstant(const NURandConstant& constant_for_load) {
    c_.c_last = GetRandom(0, 255);
    c_.c_id = GetRandom(0, 1023);
    c_.ol_i_id = GetRandom(0, 8191);
    while (!VarfiyConstantAvailableForRun(c_.c_last, constant_for_load.c_last)) {
        c_.c_last = GetRandom(0, 255);
    }
}

int RandomGenerator::GetRandom(int lower, int upper) {
    int ret = 0;
    int err = random_r(&rand_state_, &ret);
    assert(err == 0);
    return lower <= upper ? (ret % (upper - lower + 1) + lower) : (ret % (lower - upper + 1) + upper);
}

int RandomGenerator::GetRandom(int lower, int upper, int exclude) {
    if (exclude > upper || exclude < lower) {
        return GetRandom(lower, upper);
    } else {
        int rand = GetRandom(lower, upper - 1);
        if (rand >= exclude) {
            ++rand;
        }
        return rand;
    }
}

std::string RandomGenerator::MakeAString(int lower_len, int upper_len) {
    int len = GetRandom(lower_len, upper_len); 
    std::string ret;
    for (int i = 0; i < len; ++i) {
        ret += (char)('a' + GetRandom(0, 25));
    }
    return ret;
}

std::string RandomGenerator::MakeNString(int lower_len, int upper_len) {
    int len = GetRandom(lower_len, upper_len); 
    std::string ret;
    for (int i = 0; i < len; ++i) {
        ret += (char)('0' + GetRandom(0, 9));
    }
    return ret;
}

float RandomGenerator::MakeFloat(float lower, float upper, int digits) {
	float num = 1.0;
    for (int i = 0; i < digits; ++i) {
        num *= 10;
	}
    return GetRandom(int(lower * num + 0.5), int(upper * num + 0.5)) / num;
}

std::vector<int> RandomGenerator::MakeDisOrderList(int lower, int upper) {
    std::vector<int> ret(upper - lower + 1, -1);
    for (int i = 0; i < upper - lower + 1; ++i) {
        int rand_pos = GetRandom(0, upper - lower);
        while (true) {
            if (ret[rand_pos] == -1) {
                ret[rand_pos] = lower + i;
                break;
            }
            rand_pos = GetRandom(0, upper - lower);
        }
    }
    return ret;
}

int RandomGenerator::NURand(int A, int x, int y) {
    int C = 0;
    switch(A) {
        case 255:
            C = c_.c_last;
            break;
        case 1023:
            C = c_.c_id;
            break;
        case 8191:
            C = c_.ol_i_id;
            break;
        default:
            LOG(ERROR) << "NURand: A = " << A << " not available";
            abort();
    }
    return (((GetRandom(0, A) | GetRandom(x, y)) + C) % (y - x + 1)) + x;
}

} // namespace tpcc
} // namespace tera
