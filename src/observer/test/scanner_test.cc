// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <atomic>
#include <iostream>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "observer/executor/random_key_selector.h"
#include "observer/executor/scanner_impl.h"
#include "observer/observer_demo/demo_observer.h"
#include "sdk/client_impl.h"
#include "sdk/global_txn.h"
#include "sdk/mutate_impl.h"
#include "sdk/read_impl.h"
#include "sdk/table_impl.h"
#include "sdk/sdk_utils.h"
#include "tera.h"

DECLARE_bool(tera_sdk_client_for_gtxn);
DECLARE_bool(tera_sdk_tso_client_enabled);
DECLARE_string(tera_coord_type);
DECLARE_bool(rowlock_test);

namespace tera {
namespace observer {


class TestRowReader : public RowReaderImpl {
public:
    TestRowReader(TableImpl* table, const std::string& row_key)
        : RowReaderImpl(table, row_key), seq_(0) {
            if (row_key == "empty") {
                // empty case
            } else if (row_key == "900") {
                value_.push_back("900");
                value_.push_back("900");
                value_.push_back("901");
                value_.push_back("920");
            } else if (row_key == "1100") {
                value_.push_back("1000");
                value_.push_back("1000");
                value_.push_back("1100");
                value_.push_back("1100");
            } else if (row_key == "1hour") {
                value_.push_back("810");
                value_.push_back("820");
                value_.push_back("830");
                value_.push_back("840");
            } else if (row_key == "collision_mix") {
                value_.push_back("100");
                value_.push_back("1000");
                value_.push_back("4700");
                value_.push_back("1100");
            } else if (row_key == "error_ts") {
                value_.push_back("100:sffaeeew");
            } else if (row_key == "some_error_ts") {
                value_.push_back("wrong_string");
                value_.push_back("900");
                value_.push_back("900");
                value_.push_back("900");
            } else {
                value_.push_back("1010");
                value_.push_back("1012");
                value_.push_back("1013");
                value_.push_back("1014");
                value_.push_back("1015");
                value_.push_back("1016");
                value_.push_back("1017");
            }
    }
    virtual std::string Value() {
        return value_[seq_];

    }
    virtual int64_t Timestamp() {
        return 9999999;
    }
    virtual void AddColumn(const std::string& family, const std::string& qualifier) {}
    virtual bool Done() {
        return seq_ == value_.size();
    }
    virtual void Next() {
        seq_++;
    }
private:
    std::vector<std::string> value_;
    uint32_t seq_;
};

class TestTransaction : public GlobalTxn {
public:
    TestTransaction(int64_t start_ts, common::ThreadPool* thread_pool, bool error = false)
        : GlobalTxn(NULL, thread_pool, NULL),
        start_timestamp_(1000), error_(error) {}

    virtual ~TestTransaction() {}
    virtual ErrorCode Get(RowReader* row_reader) {
        ErrorCode err;
        return err;
    }
    virtual int64_t GetStartTimestamp() {
        return start_timestamp_;
    }
    virtual const ErrorCode& GetError() {
        if (error_ == true) {
            err_.SetFailed(ErrorCode::kSystem, "");
        }
        return err_;
    }
private:
    int64_t start_timestamp_;
    ErrorCode err_;
    bool error_;
};

class TestRowMutationImpl : public RowMutationImpl {
public:
    TestRowMutationImpl(Table* table, const std::string& row_key)
        : RowMutationImpl(table, row_key) {}
    virtual void Put(const std::string& value, int32_t ttl = -1) {}
    virtual void ApplyMutation(RowMutation* row_mu) {}
};

class TestTable : public TableImpl {
public:
    TestTable(const std::string& table_name,
              ThreadPool* thread_pool,
              sdk::ClusterFinder* cluster) 
                : TableImpl(table_name, thread_pool, cluster), 
                  global_txn_(true),
                  thread_pool_(thread_pool) {} 
    virtual RowReader* NewRowReader(const std::string& row_key) {
        return new TestRowReader(this, row_key);
    }
    virtual Transaction* StartRowTransaction(const std::string& row_key) {
        return new TestTransaction(1, thread_pool_);
    }
    virtual RowMutation* NewRowMutation(const std::string& row_key) {
        return new TestRowMutationImpl(this, row_key);
    }
    virtual void CommitRowTransaction(Transaction* transaction) {}
    virtual bool GetDescriptor(TableDescriptor* schema, ErrorCode* err) {
        schema->AddLocalityGroup("lg0");
        tera::ColumnFamilyDescriptor* cfd1 = schema->AddColumnFamily("cf1");
        cfd1->EnableNotify();
        ExtendNotifyLgToDescriptor(schema);
        if (!global_txn_) {
            cfd1->DisableGlobalTransaction();
        }
        return true;
    }
private:
    bool global_txn_;
    common::ThreadPool* thread_pool_;
};

class TestResultStream : public tera::ResultStream{
public:
    virtual bool Done(ErrorCode* err) {
        if (next_number_ < row_name_.size()) {
            return false;
        } else {
            return true;
        }
    }
    virtual void Next() {
        next_number_++;
    }

    virtual std::string RowName() const {
        return row_name_[next_number_];
    }
    virtual std::string Qualifier() const {
        return qualifier_[next_number_];
    }


    virtual std::string Family() const {
        return "";
    }

    virtual int64_t Timestamp() const {
        return 0;
    }
    virtual std::string Value() const {
        return "";
    }

    virtual int64_t ValueInt64() const {
        return 0;
    }

    virtual bool LookUp(const std::string& row_key) {
        return true;
    }

    virtual std::string ColumnName() const {
        return "";
    }
private:
    uint32_t next_number_;
    std::vector<string> row_name_;
    std::vector<string> qualifier_;
    bool done_;
};

class TestObserver : public tera::observer::Observer {
public:
    TestObserver() : count_(0) {}
    virtual ~TestObserver() {}
    virtual ErrorCode OnNotify(tera::Transaction* t,
                              tera::Client* client,
                              const std::string& table_name,
                              const std::string& family,
                              const std::string& qualifier,
                              const std::string& row,
                              const std::string& value,
                              int64_t timestamp,
                              Notification* notification) {
        LOG(INFO) << "[Notify TestObserver] table:family:qualifer=" <<
        table_name << ":" << family << ":" <<
        qualifier << " row=" << row <<
        " value=" << value << " timestamp=" << timestamp;

        count_++;

        tera::ErrorCode err;
        // do nothing
        return err;
    }
    virtual std::string GetObserverName() const {
        return "TestObserver";
    }

    virtual TransactionType GetTransactionType() const {
        return kGlobalTransaction;
    }
private:
    std::atomic<uint32_t> count_;
};

class TestClient : public ClientImpl {
public:
    TestClient() : ClientImpl("", "") {}
    ~TestClient() {}
    virtual Table* OpenTable(const std::string& table_name, ErrorCode* err) {
        return  static_cast<tera::Table*>(new TestTable(table_name, &thread_pool_, NULL)); 
    }
};

class TestKeySelector : public RandomKeySelector {
public:
    TestKeySelector() {}
    virtual ErrorCode Observe(const std::string& table_name) {
        tera::ErrorCode err;
        return err;
    }
};

TEST(ScannerImpl, ParseNotifyQualifier) {
    FLAGS_tera_sdk_client_for_gtxn = true;
    FLAGS_tera_coord_type = "mock_zk";
    ScannerImpl scanner;

    std::string data_family;
    std::string data_qualfier;

    EXPECT_TRUE(scanner.ParseNotifyQualifier("C:url", &data_family, &data_qualfier));
    EXPECT_EQ(data_family, "C");
    EXPECT_EQ(data_qualfier, "url");

    EXPECT_TRUE(scanner.ParseNotifyQualifier("cf:page", &data_family, &data_qualfier));
    EXPECT_EQ(data_family, "cf");
    EXPECT_EQ(data_qualfier, "page");

    EXPECT_TRUE(scanner.ParseNotifyQualifier("cf::::::", &data_family, &data_qualfier));
    EXPECT_EQ(data_family, "cf");
    EXPECT_EQ(data_qualfier, ":::::");

    EXPECT_TRUE(scanner.ParseNotifyQualifier("cf:___", &data_family, &data_qualfier));
    EXPECT_EQ(data_family, "cf");
    EXPECT_EQ(data_qualfier, "___");

    EXPECT_FALSE(scanner.ParseNotifyQualifier("Curl", &data_family, &data_qualfier));
    EXPECT_FALSE(scanner.ParseNotifyQualifier("C_url", &data_family, &data_qualfier));
    EXPECT_FALSE(scanner.ParseNotifyQualifier("C.urlN_", &data_family, &data_qualfier));
    EXPECT_FALSE(scanner.ParseNotifyQualifier("++page", &data_family, &data_qualfier));

}

TEST(ScannerImpl, DoReadValue) {
    FLAGS_tera_sdk_client_for_gtxn = true;
    FLAGS_mock_rowlock_enable = true;
    FLAGS_tera_coord_type = "mock_zk";
    common::ThreadPool thread_pool(2);
    ScannerImpl scanner;
    TestTable table("test_table", &thread_pool, NULL);

    std::shared_ptr<NotifyCell> notify_cell(new NotifyCell(new TestTransaction(1, &thread_pool)));
    Column column = {"test_table", "family", "qualifier"};

    notify_cell->row = "row";
    notify_cell->value = "value";
    notify_cell->timestamp = 999999999;
    notify_cell->observed_column = column;
    notify_cell->table = &table;

    // no table name
    EXPECT_FALSE(scanner.DoReadValue(notify_cell));
    // no column
    ScannerImpl::TableObserveInfo cell;
    (*scanner.table_observe_info_)["test_table"] = cell;
    EXPECT_FALSE(scanner.DoReadValue(notify_cell));
    // size 0
    (*scanner.table_observe_info_)["test_table"].observe_columns[column].clear();
    EXPECT_FALSE(scanner.DoReadValue(notify_cell));

    Observer* observer = new TestObserver();
    // normal
    (*scanner.table_observe_info_)["test_table"].observe_columns[column].insert(observer);
    EXPECT_TRUE(scanner.DoReadValue(notify_cell));

    // multi observer
    Observer* parse = new TestObserver();
    (*scanner.table_observe_info_)["test_table"].observe_columns[column].insert(parse);
    EXPECT_TRUE(scanner.DoReadValue(notify_cell));
}

TEST(ScannerImpl, MultiThreadDoReadValue) {
    FLAGS_tera_sdk_client_for_gtxn = true;
    FLAGS_mock_rowlock_enable = true;
    FLAGS_tera_coord_type = "mock_zk";
    common::ThreadPool thread_pool(2);
    ScannerImpl scanner;
    TestTable table("test_table", &thread_pool, NULL);

    std::shared_ptr<NotifyCell> notify_cell(new NotifyCell(new TestTransaction(1, &thread_pool)));
    Column column = {"test_table", "family", "qualifier"};

    notify_cell->row = "row";
    notify_cell->value = "value";
    notify_cell->timestamp = 100;
    notify_cell->observed_column = column;
    notify_cell->table = &table;

    Observer* observer = new TestObserver();
    (*scanner.table_observe_info_)["test_table"].observe_columns[column].insert(observer);

    common::ThreadPool worker_thread(10);
    for (uint32_t i = 0; i < 10; ++i) {
        worker_thread.AddTask(std::bind(&ScannerImpl::DoReadValue, &scanner, notify_cell));
    }
    worker_thread.Stop(true);
    scanner.transaction_threads_->Stop(true);
    EXPECT_EQ(((TestObserver*)observer)->count_, 10);
}

TEST(ScannerImpl, NextRow) {
    FLAGS_tera_sdk_client_for_gtxn = true;
    FLAGS_tera_coord_type = "mock_zk";
    std::unique_ptr<tera::ResultStream> result_stream(new TestResultStream());
    ScannerImpl scanner;
    std::set<Column> columns;
    bool finished = false;
    std::string vec_rowkey;
    std::vector<Column> vec_col;

    // stream done
    EXPECT_FALSE(scanner.NextRow(columns, result_stream.get(), "table_name", &finished, &vec_rowkey, &vec_col));
    EXPECT_EQ(true, finished);

    finished = false;
    static_cast<TestResultStream*>(result_stream.get())->row_name_.push_back("row1");
    static_cast<TestResultStream*>(result_stream.get())->qualifier_.push_back("cf:page1");
    static_cast<TestResultStream*>(result_stream.get())->row_name_.push_back("row1");
    static_cast<TestResultStream*>(result_stream.get())->qualifier_.push_back("cf:page2");
    static_cast<TestResultStream*>(result_stream.get())->row_name_.push_back("row2");
    static_cast<TestResultStream*>(result_stream.get())->qualifier_.push_back("cf:page3");
    static_cast<TestResultStream*>(result_stream.get())->row_name_.push_back("row2");
    static_cast<TestResultStream*>(result_stream.get())->qualifier_.push_back("cf:page4");

    Column colum_1 = {"table_name", "cf", "page1"};
    Column colum_2 = {"table_name", "cf", "page2"};
    Column colum_3 = {"table_name", "cf", "page3"};
    Column colum_4 = {"table_name", "cf", "page4"};
    columns.insert(colum_1);
    columns.insert(colum_2);
    columns.insert(colum_3);
    columns.insert(colum_4);

    // row 1
    EXPECT_TRUE(scanner.NextRow(columns, result_stream.get(), "table_name", &finished, &vec_rowkey, &vec_col));
    EXPECT_FALSE(finished);

    // row 1 data
    EXPECT_EQ(vec_col.size(), 2);
    EXPECT_EQ(vec_rowkey, "row1");
    EXPECT_EQ(vec_col[0].qualifier, "page1");
    EXPECT_EQ(vec_col[1].qualifier, "page2");

    // row 2
    EXPECT_TRUE(scanner.NextRow(columns, result_stream.get(), "table_name", &finished, &vec_rowkey, &vec_col));
    EXPECT_FALSE(finished);

    // row 2 data
    EXPECT_EQ(vec_col.size(), 2);
    EXPECT_EQ(vec_rowkey, "row2");
    EXPECT_EQ(vec_col[0].qualifier, "page3");
    EXPECT_EQ(vec_col[1].qualifier, "page4");

    // scan finish
    EXPECT_FALSE(scanner.NextRow(columns, result_stream.get(), "table_name", &finished, &vec_rowkey, &vec_col));
    EXPECT_TRUE(finished);
}



TEST(ScannerImpl, CheckConflictOnAckColumn) {
    FLAGS_tera_sdk_client_for_gtxn = true;
    FLAGS_tera_coord_type = "mock_zk";
    common::ThreadPool thread_pool(2);
    ScannerImpl scanner;
    TestTable table("test_table", &thread_pool, NULL);

    std::shared_ptr<NotifyCell> notify_cell(new NotifyCell(new TestTransaction(1, &thread_pool)));
    Column column = {"test_table", "family", "qualifier"};

    notify_cell->row = "row";
    notify_cell->value = "value";
    notify_cell->timestamp = 1000;
    notify_cell->observed_column = column;
    notify_cell->table = &table;

    std::set<Observer*> observers;

    TestObserver observer;
    observers.insert(&observer);

    // empty case
    notify_cell->row = "empty";
    EXPECT_TRUE(scanner.CheckConflictOnAckColumn(notify_cell, observers));

    // row reader ts < transaction(notify) ts
    notify_cell->row = "900";
    EXPECT_TRUE(scanner.CheckConflictOnAckColumn(notify_cell, observers));

    // row reader ts > transaction(notify) ts
    notify_cell->row = "1100";
    EXPECT_FALSE(scanner.CheckConflictOnAckColumn(notify_cell, observers));

    // transaction ts - row reader ts < 600
    notify_cell->timestamp = 700;
    notify_cell->row = "1hour";
    EXPECT_FALSE(scanner.CheckConflictOnAckColumn(notify_cell, observers));

    // collision_mix: some legal, some illegal
    notify_cell->row = "collision_mix";
    EXPECT_FALSE(scanner.CheckConflictOnAckColumn(notify_cell, observers));

    // ack parse fail
    notify_cell->timestamp = 1000;
    notify_cell->row = "error_ts";
    EXPECT_FALSE(scanner.CheckConflictOnAckColumn(notify_cell, observers));

    // some ack parse fail
    notify_cell->row = "some_error_ts";
    EXPECT_FALSE(scanner.CheckConflictOnAckColumn(notify_cell, observers));

    // mutation fail
    std::shared_ptr<NotifyCell> notify_cell_fail(new NotifyCell(new TestTransaction(1, &thread_pool, true)));

    notify_cell_fail->row = "row";
    notify_cell_fail->value = "value";
    notify_cell_fail->timestamp = 1000;
    notify_cell_fail->observed_column = column;
    notify_cell_fail->table = &table;

    // empty case
    notify_cell->row = "empty";
    EXPECT_FALSE(scanner.CheckConflictOnAckColumn(notify_cell_fail, observers));

    // row reader ts < transaction(notify) ts
    notify_cell->row = "900";
    EXPECT_FALSE(scanner.CheckConflictOnAckColumn(notify_cell_fail, observers));
}

} // namespace observer
} // namespace tera

