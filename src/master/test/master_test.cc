// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "utils/utils_cmd.h"

DECLARE_string(tera_master_port);
DECLARE_string(log_dir);
DECLARE_bool(tera_zk_enabled);
DECLARE_string(tera_leveldb_env_type);
DECLARE_string(tera_fake_zk_path_prefix);

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);

    FLAGS_tera_zk_enabled = false;
    FLAGS_tera_leveldb_env_type = "local";

    tera::utils::SetupLog("master_test");
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}

