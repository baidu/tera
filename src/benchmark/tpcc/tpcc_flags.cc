// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "gflags/gflags.h"

DEFINE_int64(transactions_count, 200, "the count of transactions");
DEFINE_int32(warehouses_count, 2, "the count of warsehouses");
DEFINE_int32(tpcc_thread_pool_size, 20, "size of tpcc thread pool");
DEFINE_int32(tpcc_run_gtxn_thread_pool_size, 20,
             "size of tpcc run global transactions thread pool");
DEFINE_string(db_type, "tera", "test db type");
DEFINE_string(tera_client_flagfile, "./tera.flag", "the flag file path of tera client");
DEFINE_string(tera_table_schema_dir, "./tpcc_schemas/", "table schema directory");
DEFINE_int32(generate_data_wait_times, 3600000, "(ms) generate data wait times, default 1h");
DEFINE_int32(driver_wait_times, 3600000, "(ms) driver wait times, default 1h");
