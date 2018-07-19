// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include <iostream>

#include "benchmark/tpcc/data_generator.h"
#include "benchmark/tpcc/mock_tpccdb.h"
#include "benchmark/tpcc/random_generator.h"
#include "benchmark/tpcc/tpccdb.h"

#include "gflags/gflags.h"
#include "gtest/gtest.h"

DECLARE_int32(warehouses_count);

namespace tera {
namespace tpcc {

class DataGeneratorTest : public ::testing::Test {
public:
    DataGeneratorTest() {
        random_gen_.SetRandomConstant();
        TpccDb* db_ = (TpccDb*)(&mdb_);
        data_gen_ = new DataGenerator(&random_gen_, db_);
	}

    void CleanStateCounter(int table_enum_num = -1) {
        if (table_enum_num == -1) {
            for (int i = 0; i < kTpccTableCnt; ++i) {
                data_gen_->states_[i].first.Set(0);
                data_gen_->states_[i].second.Set(0);
            }
        } else if (table_enum_num > -1 && table_enum_num < kTpccTableCnt) {
            data_gen_->states_[table_enum_num].first.Set(0);
            data_gen_->states_[table_enum_num].second.Set(0);
        }
    }

	~DataGeneratorTest() {
        delete data_gen_;
    }
private:
    RandomGenerator random_gen_;
    TpccDb* db_;
    MockTpccDb mdb_;
    DataGenerator* data_gen_;

};

TEST_F(DataGeneratorTest, GenItem) {
    CleanStateCounter();
    mdb_.flag_ = true;
    data_gen_->GenItem(1, false);
    EXPECT_TRUE(data_gen_->states_[kItemTable].first.Get() == 1);
    data_gen_->GenItem(1, false);
    EXPECT_TRUE(data_gen_->states_[kItemTable].first.Get() == 2);
    mdb_.flag_ = false;
    data_gen_->GenItem(1, false);
    EXPECT_TRUE(data_gen_->states_[kItemTable].second.Get() == 1);
}

TEST_F(DataGeneratorTest, GenStock) {
    CleanStateCounter();
    mdb_.flag_ = true;
    data_gen_->GenStock(1, 2, false);
    EXPECT_TRUE(data_gen_->states_[kStockTable].first.Get() == 1);
    data_gen_->GenStock(1, 2, false);
    EXPECT_TRUE(data_gen_->states_[kStockTable].first.Get() == 2);
    mdb_.flag_ = false;
    data_gen_->GenStock(1, 3, false);
    EXPECT_TRUE(data_gen_->states_[kStockTable].second.Get() == 1);
}

TEST_F(DataGeneratorTest, GenStocks) {
    CleanStateCounter();
    mdb_.flag_ = true;
    for (int i = 1; i <=FLAGS_warehouses_count; ++i) {
        data_gen_->GenStocks(i);
    }
    data_gen_->Join();
    EXPECT_TRUE(data_gen_->states_[kStockTable].first.Get() == FLAGS_warehouses_count * kItemCount);
}

} // namespace tpcc
} // namespace tera
