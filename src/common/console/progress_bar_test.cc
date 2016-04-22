// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: xupeilin@baidu.com

#include "progress_bar.h"

#include <stdio.h>
#include <stdlib.h>
#include <gtest/gtest.h>

using common::ProgressBar;

TEST(ProgressBarTest, Test) {
    int cur_size = 0;
    int total_size = 100000000;

    ProgressBar progress_bar(ProgressBar::ENHANCED, total_size, 100, "B");

    srand((uint32_t)time(NULL));
    timespec interval = {0, 1000};
    while (cur_size < total_size) {
        cur_size += rand() % 10000;
        progress_bar.Refresh(cur_size);
        nanosleep(&interval, &interval);
    }
    progress_bar.Done();
}
