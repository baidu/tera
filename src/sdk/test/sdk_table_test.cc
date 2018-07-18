// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: lidatong@baidu.com

#include <atomic>
#include <iostream>
#include <string>
#include <thread>
#include <memory>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "sdk/scan_impl.h"
#include "sdk/read_impl.h"
#include "sdk/table_impl.h"
#include "sdk/sdk_zk.h"
#include "sdk/test/mock_table.h"
#include "tera.h"

DECLARE_string(tera_coord_type);

namespace tera {

class SdkTableTest :  public ::testing::Test {
public:
    SdkTableTest() : thread_pool_(2) {}
    ~SdkTableTest() {}

    std::shared_ptr<MockTable> OpenTable(const std::string& tablename) {
        FLAGS_tera_coord_type = "mock_zk";
        std::shared_ptr<MockTable> table_(new MockTable(tablename, &thread_pool_));
        return table_;
    }

private:
    common::ThreadPool thread_pool_;
};

TEST_F(SdkTableTest, UnnormalLifeCycleReadTableMetaCallBack) {
    std::shared_ptr<MockTable> table_ = OpenTable("t1");
    ErrorCode ret_err;
    ReadTabletRequest request;
    ReadTabletResponse response;
    ThreadPool::Task task_ = std::bind(&TableImpl::ReadTableMetaCallBackWrapper,
        std::weak_ptr<TableImpl>(std::static_pointer_cast<TableImpl>(table_)),
        &ret_err, 0, &request, &response, false, 0);
    table_->AddDelayTask(1 * 1000/*ms*/, task_);
    table_.reset();
    sleep(2);
}

TEST_F(SdkTableTest, UnnormalLifeCycleScanMetaTableCallBack) {
    std::shared_ptr<MockTable> table_ = OpenTable("t1");
    ScanTabletRequest request;
    ScanTabletResponse response;
    std::string empty_str("");
    int64_t start_time = 0;
    ThreadPool::Task task_ = std::bind(&TableImpl::ScanMetaTableCallBackWrapper,
        std::weak_ptr<TableImpl>(std::static_pointer_cast<TableImpl>(table_)),
        empty_str, empty_str, empty_str, start_time, &request, &response, false, 0);
    table_->AddDelayTask(1 * 1000/*ms*/, task_);
    table_.reset();
    sleep(2);
}

TEST_F(SdkTableTest, UnnormalLifeCycleReaderCallBack) {
    std::shared_ptr<MockTable> table_ = OpenTable("t1");
    std::vector<int64_t> vec;
    ReadTabletRequest request;
    ReadTabletResponse response;
    ThreadPool::Task task_ = std::bind(&TableImpl::ReaderCallBackWrapper,
        std::weak_ptr<TableImpl>(std::static_pointer_cast<TableImpl>(table_)),
        &vec, &request, &response, false, 0);
    table_->AddDelayTask(1 * 1000/*ms*/, task_);
    table_.reset();
    sleep(2);
}

TEST_F(SdkTableTest, UnnormalLifeCycleMutateCallBack) {
    std::shared_ptr<MockTable> table_ = OpenTable("t1");
    std::vector<int64_t> vec;
    WriteTabletRequest request;
    WriteTabletResponse response;
    ThreadPool::Task task_ = std::bind(&TableImpl::MutateCallBackWrapper,
        std::weak_ptr<TableImpl>(std::static_pointer_cast<TableImpl>(table_)),
        &vec, &request, &response, false, 0);
    table_->AddDelayTask(1 * 1000/*ms*/, task_);
    table_.reset();
    sleep(2);
}

TEST_F(SdkTableTest, UnnormalLifeCycleScanCallBack) {
    std::shared_ptr<MockTable> table_ = OpenTable("t1");
    ScanTask scan_task;
    ScanTabletRequest request;
    ScanTabletResponse response;
    ThreadPool::Task task_ = std::bind(&TableImpl::ScanCallBackWrapper,
        std::weak_ptr<TableImpl>(std::static_pointer_cast<TableImpl>(table_)),
        &scan_task, &request, &response, false, 0);
    table_->AddDelayTask(1 * 1000/*ms*/, task_);
    table_.reset();
    sleep(2);
}

}