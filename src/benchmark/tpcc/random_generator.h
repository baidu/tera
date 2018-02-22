// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef TERA_BENCHMARK_TPCC_RANDOM_GENERATOR_H
#define TERA_BENCHMARK_TPCC_RANDOM_GENERATOR_H

#include <stdlib.h>
#include <string>
#include <vector>

#include "benchmark/tpcc/tpcc_types.h"

namespace tera {
namespace tpcc {

struct NURandConstant {
    int c_last;
    int c_id;
    int ol_i_id;
};

class RandomGenerator {
public:
    RandomGenerator();
    virtual ~RandomGenerator(){}

    NURandConstant GetRandomConstant() const;
    void SetRandomConstant();
    void SetRandomConstant(const NURandConstant& constant_for_load);

    // make a string A len=rand[lower_len, upper_len] A[x] = set(a..z)
    std::string MakeAString(int lower_len, int upper_len);

    // make a string N len=rand[lower_len, upper_len] N[x] = set(0..9)
    std::string MakeNString(int lower_len, int upper_len);

	float MakeFloat(float lower, float upper, int digits);

    std::vector<int> MakeDisOrderList(int lower, int upper);

    int NURand(int A, int lower, int upper);

    // get rand int from [lower, upper]
    int GetRandom(int lower, int upper); 

    int GetRandom(int lower, int upper, int exclude);
private:
    void InitRandomState();
private:
    // for system call random_r and initstate_r
    char rand_state_buf_[kRandomStateSize];
    struct random_data rand_state_;
    
    // for NURand, need a constant
    NURandConstant c_;
};

} // namespace tpcc
} // namespace tera

#endif /* TERA_BENCHMARK_TPCC_RANDOM_GENERATOR_H */
