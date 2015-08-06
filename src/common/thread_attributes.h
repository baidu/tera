// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_THREAD_ATTRIBUTES_H_
#define TERA_COMMON_THREAD_ATTRIBUTES_H_

#ifndef __USE_GNU
#define __USE_GNU
#endif

#include <sched.h>

#include "common/base/stdint.h"

class ThreadAttributes {
public:
    ThreadAttributes() {
        cpu_num_ = sysconf(_SC_NPROCESSORS_CONF);
        mask_ = GetCpuAffinity();
    }
    ~ThreadAttributes() {}

    int32_t GetCpuNum() {
        return cpu_num_;
    }

    cpu_set_t GetCpuAffinity() {
        ResetCpuMask();
        if (sched_getaffinity(0, sizeof(mask_), &mask_) == -1) {
            ResetCpuMask();
        }
        return mask_;
    }
    bool SetCpuAffinity() {
        if (sched_setaffinity(0, sizeof(mask_), &mask_) == -1) {
            return false;
        }
        return true;
    }

    bool SetCpuMask(int32_t cpu_id) {
        if (cpu_id < 0 || cpu_id > cpu_num_) {
            return false;
        }

        if (CPU_ISSET(cpu_id, &mask_)) {
            return true;
        }
        CPU_SET(cpu_id, &mask_);
        return true;
    }
    void ResetCpuMask() {
        CPU_ZERO(&mask_);
    }
    void MarkCurMask() {
        CPU_ZERO(&last_mask_);
        last_mask_ = mask_;
    }
    bool RevertCpuAffinity() {
        ResetCpuMask();
        mask_ = last_mask_;
        return SetCpuAffinity();
    }

private:
    int32_t cpu_num_;
    cpu_set_t mask_;
    cpu_set_t last_mask_;
};

#endif // TERA_COMMON_THREAD_THREAD_ATTRIBUTES_H_
