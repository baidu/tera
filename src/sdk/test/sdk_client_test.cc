// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: lidatong@baidu.com

#include <memory>
#include <iostream>

#include "common/thread_pool.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "sdk/client_impl.h"
#include "sdk/table_impl.h"
#include "sdk/global_txn.h"
#include "tera.h"

DECLARE_string(tera_coord_type);
DECLARE_bool(tera_sdk_mock_enable);

namespace tera {

class SdkClientTest :  public ::testing::Test {
public:
    SdkClientTest() {
        FLAGS_tera_coord_type = "mock_zk";
        FLAGS_tera_sdk_mock_enable = true;
    }
    ~SdkClientTest() {}
};

TEST_F(SdkClientTest, MultiNewClient) {
    Client* client1 = Client::NewClient();
    EXPECT_TRUE(NULL != client1);
    Client* client2 = Client::NewClient();
    EXPECT_TRUE(NULL != client2);
    EXPECT_TRUE((static_cast<ClientWrapper*>(client1))->GetClientImpl()
        == (static_cast<ClientWrapper*>(client2))->GetClientImpl());
    delete client1;
    delete client2;
}

TEST_F(SdkClientTest, SingleClientSingleTable) {
    Client* client = Client::NewClient();
    EXPECT_TRUE(NULL != client); 
    ErrorCode err;
    Table* table1 = client->OpenTable("t1", &err);
    EXPECT_TRUE(err.GetType() == ErrorCode::kOK);
    delete table1;
    delete client;
}

TEST_F(SdkClientTest, SingleClientMutiTable) {
    Client* client = Client::NewClient();
    EXPECT_TRUE(NULL != client); 
    ErrorCode err;
    Table* table1 = client->OpenTable("t1", &err);
    EXPECT_TRUE(err.GetType() == ErrorCode::kOK);
    Table* table2 = client->OpenTable("t1", &err);
    EXPECT_TRUE(err.GetType() == ErrorCode::kOK);

    delete table1;
    delete client;
    delete table2;
}

TEST_F(SdkClientTest, MultiClientMutiTable) {
    Client* client1 = Client::NewClient();
    EXPECT_TRUE(NULL != client1);
    Client* client2 = Client::NewClient(); 
    EXPECT_TRUE(NULL != client2);
    ErrorCode err;
    Table* table1 = client1->OpenTable("t1", &err);
    EXPECT_TRUE(err.GetType() == ErrorCode::kOK);
    Table* table2 = client2->OpenTable("t2", &err);
    EXPECT_TRUE(err.GetType() == ErrorCode::kOK);

    delete table1;
    delete client1;
    delete table2;
    delete client2;
}

static void MultiThreadTable(Client* client) {
    ErrorCode err;
    Table* table1 = client->OpenTable("t1", &err);
    EXPECT_TRUE(err.GetType() == ErrorCode::kOK); 
    Table* table2 = client->OpenTable("t2", &err);
    EXPECT_TRUE(err.GetType() == ErrorCode::kOK); 
    delete table1;
    delete table2;
}

TEST_F(SdkClientTest, MultiClientMutiTableMultiThread) {
    common::ThreadPool thread_pool(5);
    Client* client = Client::NewClient();
    EXPECT_TRUE(NULL != client);
    ThreadPool::Task task_ = std::bind(MultiThreadTable, client);
    int cnt = 10;
    while (cnt--) {
        thread_pool.AddTask(task_);
    }
    thread_pool.Stop(true); 
    delete client;  // delete client* won't let multi-thread know,
                    // so delete at last in case client is used after it's deleted.
}

TEST_F(SdkClientTest, MultiClientMutiTableMultiThreadDelayTask) {
    common::ThreadPool thread_pool(5);
    Client* client = Client::NewClient();
    EXPECT_TRUE(NULL != client);
    ThreadPool::Task task_ = std::bind(MultiThreadTable, client);
    int cnt = 10;
    while (cnt--) {
        thread_pool.DelayTask(cnt * 100 /*ms*/, task_);
    }
    thread_pool.Stop(true); 
    delete client;
}

TEST_F(SdkClientTest, SingleClientTwoGlobalTransaction) {
    Client* client = Client::NewClient();
    EXPECT_TRUE(NULL != client); 
    Transaction* global_transaction1 = client->NewGlobalTransaction();
    EXPECT_TRUE(NULL != global_transaction1); 
    Transaction* global_transaction2 = client->NewGlobalTransaction();
    EXPECT_TRUE(NULL != global_transaction2); 
    delete global_transaction1;
    delete client;
    delete global_transaction2;
}

TEST_F(SdkClientTest, SingleClientNULLGlobalTransaction) {
    Transaction* global_transaction = GlobalTxn::NewGlobalTxn(std::shared_ptr<ClientImpl>(), NULL, NULL);
    EXPECT_TRUE(NULL == global_transaction);
}

TEST_F(SdkClientTest, SingleClientRowTransaction) {
    Client* client = Client::NewClient();
    EXPECT_TRUE(NULL != client); 
    ErrorCode err;
    Table* table = client->OpenTable("t", &err);
    EXPECT_TRUE(err.GetType() == ErrorCode::kOK);
    std::string row_key("test1");
    Transaction* row_transaction1 = table->StartRowTransaction(row_key);
    EXPECT_TRUE(NULL != row_transaction1); 
    row_key = "test2";
    Transaction* row_transaction2 = table->StartRowTransaction(row_key);
    EXPECT_TRUE(NULL != row_transaction2); 
    delete row_transaction1;
    delete table;
    delete client;
    delete row_transaction2;
}

}