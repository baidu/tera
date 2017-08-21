// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <atomic>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "observer/executor.h"
#include "observer/observer.h"
#include "utils/utils_cmd.h"
#include "common/thread.h"

namespace observer {

class TestWorker : public Observer {
public:
    TestWorker(const std::string& observer_name, ColumnList& observed_columns): 
        Observer(observer_name, observed_columns), counter_(0), notified_(false) {}
    virtual ~TestWorker() {}
    virtual bool OnNotify(tera::Transaction* t, 
            tera::Table* table, 
            const std::string& row, 
            const Column& column, 
            const std::string& value, 
            int64_t timestamp) {
        row_ = row;
        column_ = column;
        value_ = value;
        timestamp_ = timestamp;
        notified_ = true;
        
        ++counter_;
        
        observer::ColumnList ack_columns;
        observer::Column c = {"observer_test_table", "Data", "Page"};
        ack_columns.push_back(c);
        Ack(ack_columns, row, -1);

        return true;
    }
    std::string row_;
    Column column_;
    std::string value_;
    int64_t timestamp_;
    std::atomic<int> counter_;
    std::atomic<bool> notified_;
};

class ObserverImplTest : public ::testing::Test {
public:
    void OnNotifyTest() {
        tera::ErrorCode err;
        tera::Client* client = tera::Client::NewClient("./tera.flag", &err);
        if (tera::ErrorCode::kOK != err.GetType()) {
            LOG(ERROR) << "new client failed";
            return;
        }
        tera::Table* table = client->OpenTable("observer_test_table", &err);
        if (tera::ErrorCode::kOK != err.GetType()) {
            LOG(ERROR) << "open table failed"; 
            return;
        }
        tera::Transaction* t = tera::NewTransaction();
        assert(t != NULL);
        tera::RowMutation* mu0 = table->NewRowMutation("www.baidu.com");
        mu0->Put("Data", "Page", "hello world");
        t->ApplyMutation(mu0);
        t->Commit();
        tera::RowMutation* mu1 = table->NewRowMutation("www.baidu.com");
        // mu1->Put("Notify", "Data+Page", "1", t->GetStartTimestamp());
        mu1->Put("Notify", "Data+Page", "1", -1);
        table->ApplyMutation(mu1);
        delete t;
        delete mu0;
        delete mu1;
        
        ColumnList columns;
        Column c = {"observer_test_table", "Data", "Page"};
        columns.push_back(c);
        TestWorker worker("TestWorker", columns);
        
        Executor* executor = Executor::NewExecutor();
        executor->RegisterObserver(&worker);
        run_thread_.Start(std::bind(&ObserverImplTest::DoRun, this, executor));
        while (!worker.notified_) {
            sleep(1);
        }
        executor->Quit();
        EXPECT_TRUE(("www.baidu.com" == worker.row_) &&
                ("observer_test_table" == worker.column_.table_name) &&
                ("Data" == worker.column_.family) &&
                ("Page" == worker.column_.qualifier) &&
                ("hello world" == worker.value_)); 
    }
private:
    void DoRun(Executor* executor) {
        executor->Run();
    }
    common::Thread run_thread_;
};

TEST_F(ObserverImplTest, OnNotifyTest) {
    OnNotifyTest();
}

} // namespace observer

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
