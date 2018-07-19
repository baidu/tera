// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "benchmark/tpcc/data_generator.h"
#include "benchmark/tpcc/driver.h"
#include "benchmark/tpcc/random_generator.h"
#include "benchmark/tpcc/tpccdb.h"
#include "benchmark/tpcc/tpcc_types.h"
#include "types.h"
#include "common/timer.h"
#include "version.h"

DECLARE_int64(transactions_count);
DECLARE_int32(warehouses_count);
DECLARE_string(db_type);

int main(int argc, char *argv[]) {
    // load conf from flags
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc > 1 && strcmp(argv[1], "version") == 0) {
        PrintSystemVersion();
        return 0;
    }
    if (FLAGS_warehouses_count > tera::tpcc::kMaxWarehouseId
        && FLAGS_warehouses_count <= 0) {
        LOG(ERROR) << "--warehouses_count=" << FLAGS_warehouses_count << " is not availability";
        return -1;
    }

    tera::tpcc::RandomGenerator random_gen;
    random_gen.SetRandomConstant();

    tera::tpcc::TpccDb* db = tera::tpcc::TpccDb::NewTpccDb(FLAGS_db_type);
    // do clean tables
    if (argc == 2 && strcmp(argv[1], "clean") == 0) {
        if(!db->CleanTables()) {
            LOG(ERROR) << "clean tables failed, exit";
            _Exit(EXIT_FAILURE);
        }
        delete db;
        return 0;
    }
    
    if (!db->CreateTables()) {
        LOG(ERROR) << "create tables failed, exit";
        _Exit(EXIT_FAILURE);
    }

    tera::tpcc::DataGenerator data_gen(&random_gen, db);
    int64_t beg_ts = tera::get_micros();
    data_gen.GenItems();
    data_gen.GenWarehouses();
    data_gen.Join();
    int64_t cost_t = tera::get_micros() - beg_ts;
    LOG(INFO) << "Generate Tables Cost:" << cost_t << "us";
    
    // init driver
    tera::tpcc::NURandConstant constant = random_gen.GetRandomConstant();
    random_gen.SetRandomConstant(constant);
    tera::tpcc::Driver driver(&random_gen, db);
    // run test
    int64_t beg_txn_ts = tera::get_micros();
    driver.RunTransactions();
    driver.Join();
    int64_t cost_txn_t = tera::get_micros() - beg_txn_ts;
    LOG(INFO) << "RunTransactions Cost:" << cost_txn_t << "us";
    delete db;
    return 0;
}
