// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tabletnode/tabletnode_entry.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "gtest/gtest.h"

DECLARE_bool(tera_tabletnode_cpu_affinity_enabled);
DECLARE_string(tera_tabletnode_cpu_affinity_set);

namespace tera {
namespace tabletnode {

class TabletNodeEntryTest : public ::testing::Test {
public:
    static void *CpuBusyCallback(void*) {
        while (1);
        return NULL;
    }

    void MakeCpuBusy() {
        int thread_num = 50;
        pthread_t thread[thread_num];

        for (int i = 0; i < thread_num; ++i) {
            if (pthread_create(&thread[i], NULL, &CpuBusyCallback, NULL) != 0) {
                LOG(ERROR) << "fail to create thread";
                return;
            }
        }
    }

    int GetCoreNum() {
        int core_num = 0;
        FILE* pf;
        pf = popen("more /proc/cpuinfo | "
                   "grep \"physical id\" | "
                   "uniq | wc -l", "r");
        fscanf(pf, "%d", &core_num);
        return core_num;
    }

    void GetCpuInfo(std::vector<double>* cpu_info) {
        int core_num;
        core_num = GetCoreNum();
        char s[100];

        FILE* pf;
        pf = popen("mpstat -A 1 2 | "
                   "grep \"Average\" |"
                   "awk '{print $3}'", "r");
        // skip useless 2 line from start
        fscanf(pf, "%s", s);
        fscanf(pf, "%s", s);
        for (int i = 0; i < core_num; ++i) {
            double info;
            fscanf(pf, "%s", s);
            info = strtod(s, NULL);
            (*cpu_info).push_back(info);
        }
    }

    void TestBody() {}

    TabletNodeEntry ts_entry;
};

TEST_F(TabletNodeEntryTest, ProcessorAffinity) {
    TabletNodeEntryTest test_instance;

    FLAGS_tera_tabletnode_cpu_affinity_set = "4,5";
    FLAGS_tera_tabletnode_cpu_affinity_enabled = true;
    test_instance.ts_entry.SetProcessorAffinity();
    test_instance.MakeCpuBusy();

    std::vector<double> cpu_info;
    test_instance.GetCpuInfo(&cpu_info);

    for (int i = 0; i < cpu_info.size(); ++i) {
        if (i == 4 || i == 5) {
            EXPECT_GT(cpu_info[i], 50.0) << "core num: " << i;
        } else {
            EXPECT_LT(cpu_info[i], 50.0) << "core num: " << i;
        }
    }
}
} // namespace tabletnode
} // namespace tera

