// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <atomic>
#include <iostream>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <memory>

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
DECLARE_bool(tera_gtxn_test_opened);
DECLARE_bool(mock_rowlock_enable);

namespace tera {
namespace observer {

class TestRowReader : public RowReaderImpl {
 public:
  TestRowReader(TableImpl* table, const std::string& row_key)
      : RowReaderImpl(table, row_key), seq_(0) {
    if (row_key == "empty" || row_key == "empty_fail") {
      // empty case
    } else if (row_key == "900" || row_key == "900_fail") {
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
    } else if (row_key == "one_version") {
      value_.push_back("901");
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
  virtual std::string Value() { return value_[seq_]; }
  virtual int64_t Timestamp() { return 9999999; }
  virtual void AddColumn(const std::string& family, const std::string& qualifier) {}
  virtual bool Done() { return seq_ >= value_.size(); }
  virtual void Next() {
    if (seq_ < value_.size()) {
      seq_++;
    }
  }

 private:
  std::vector<std::string> value_;
  uint32_t seq_;
  // void* user_context_;
};

class TestTransaction : public GlobalTxn {
 public:
  TestTransaction(int64_t start_ts, common::ThreadPool* thread_pool, bool error = false)
      : GlobalTxn(NULL, thread_pool, NULL), start_timestamp_(1000), error_(error) {}

  virtual ~TestTransaction() {}
  virtual ErrorCode Get(RowReader* row_reader) {
    ErrorCode err;
    return err;
  }
  virtual int64_t GetStartTimestamp() { return start_timestamp_; }
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
  TestRowMutationImpl(TableImpl* table, const std::string& row_key)
      : RowMutationImpl(table, row_key) {}
  virtual void Put(const std::vector<std::string>& value, int32_t ttl = -1) {}
  virtual void ApplyMutation(RowMutation* row_mu) {}
};

class TestTable : public TableImpl {
 public:
  TestTable(const std::string& table_name, ThreadPool* thread_pool)
      : TableImpl(table_name, thread_pool, std::shared_ptr<ClientImpl>()),
        global_txn_(true),
        thread_pool_(thread_pool) {}
  virtual RowReader* NewRowReader(const std::string& row_key) {
    return new TestRowReader(this, row_key);
  }
  virtual Transaction* StartRowTransaction(const std::string& row_key) {
    if (row_key == "empty_fail" || row_key == "900_fail") {
      return new TestTransaction(1, thread_pool_, true);
    }
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
  virtual void Get(RowReader* row_reader) {}

 private:
  bool global_txn_;
  common::ThreadPool* thread_pool_;
};

class TestResultStream : public tera::ResultStream {
 public:
  virtual bool Done(ErrorCode* err) {
    if (next_number_ < row_name_.size()) {
      return false;
    } else {
      return true;
    }
  }
  virtual void Next() { next_number_++; }

  virtual std::string GetLastKey() const { return ""; }

  virtual uint64_t GetDataSize() const { return 0; }

  virtual uint64_t GetRowCount() const { return 0; }

  virtual void Cancel() { return; }

  virtual std::string RowName() const { return row_name_[next_number_]; }
  virtual std::string Qualifier() const { return qualifier_[next_number_]; }

  virtual std::string Family() const { return ""; }

  virtual int64_t Timestamp() const { return 0; }
  virtual std::string Value() const { return ""; }

  virtual int64_t ValueInt64() const { return 0; }

  virtual bool LookUp(const std::string& row_key) { return true; }

  virtual std::string ColumnName() const { return ""; }

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
  virtual void OnNotify(tera::Transaction* t, tera::Client* client, const std::string& table_name,
                        const std::string& family, const std::string& qualifier,
                        const std::string& row, const std::string& value, int64_t timestamp,
                        Notification* notification) {
    LOG(INFO) << "[Notify TestObserver] table:family:qualifer=" << table_name << ":" << family
              << ":" << qualifier << " row=" << row << " value=" << value
              << " timestamps[0]=" << value;

    count_++;
    // do nothing
  }
  virtual std::string GetObserverName() const { return "TestObserver"; }

  virtual TransactionType GetTransactionType() const { return kGlobalTransaction; }

 private:
  std::atomic<uint32_t> count_;
};

class TestClient : public ClientImpl {
 public:
  TestClient() : ClientImpl(ClientOptions(), NULL, NULL), thread_pool_(5) {}
  ~TestClient() {}
  virtual Table* OpenTable(const std::string& table_name, ErrorCode* err) {
    return static_cast<tera::Table*>(new TestTable(table_name, &thread_pool_));
  }
  virtual Transaction* NewGlobalTransaction() { return new TestTransaction(1, &thread_pool_); }

 private:
  common::ThreadPool thread_pool_;
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

TEST(ScannerImpl, NextRow) {
  FLAGS_tera_sdk_client_for_gtxn = true;
  FLAGS_tera_coord_type = "mock_zk";
  std::unique_ptr<tera::ResultStream> result_stream(new TestResultStream());
  ScannerImpl scanner;
  bool finished = false;
  std::string vec_rowkey;
  std::vector<Column> vec_col;

  // stream done
  EXPECT_FALSE(
      scanner.NextRow(result_stream.get(), "table_name", &finished, &vec_rowkey, &vec_col));
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

  // row 1
  EXPECT_TRUE(scanner.NextRow(result_stream.get(), "table_name", &finished, &vec_rowkey, &vec_col));
  EXPECT_FALSE(finished);

  // row 1 data
  EXPECT_EQ(vec_col.size(), 2);
  EXPECT_EQ(vec_rowkey, "row1");
  EXPECT_EQ(vec_col[0].qualifier, "page1");
  EXPECT_EQ(vec_col[1].qualifier, "page2");

  // row 2
  EXPECT_TRUE(scanner.NextRow(result_stream.get(), "table_name", &finished, &vec_rowkey, &vec_col));
  EXPECT_FALSE(finished);

  // row 2 data
  EXPECT_EQ(vec_col.size(), 2);
  EXPECT_EQ(vec_rowkey, "row2");
  EXPECT_EQ(vec_col[0].qualifier, "page3");
  EXPECT_EQ(vec_col[1].qualifier, "page4");

  // scan finish
  EXPECT_FALSE(
      scanner.NextRow(result_stream.get(), "table_name", &finished, &vec_rowkey, &vec_col));
  EXPECT_TRUE(finished);
}

TEST(ScannerImpl, CheckTransactionTypeLegalForTable) {
  ScannerImpl scanner;
  EXPECT_EQ(scanner.CheckTransactionTypeLegalForTable(kGlobalTransaction, kGlobalTransaction),
            true);
  EXPECT_EQ(scanner.CheckTransactionTypeLegalForTable(kSingleRowTransaction, kSingleRowTransaction),
            true);
  EXPECT_EQ(scanner.CheckTransactionTypeLegalForTable(kNoneTransaction, kNoneTransaction), true);

  EXPECT_EQ(scanner.CheckTransactionTypeLegalForTable(kNoneTransaction, kGlobalTransaction), false);
  EXPECT_EQ(scanner.CheckTransactionTypeLegalForTable(kNoneTransaction, kSingleRowTransaction),
            true);

  EXPECT_EQ(scanner.CheckTransactionTypeLegalForTable(kSingleRowTransaction, kNoneTransaction),
            false);
  EXPECT_EQ(scanner.CheckTransactionTypeLegalForTable(kSingleRowTransaction, kGlobalTransaction),
            false);

  EXPECT_EQ(scanner.CheckTransactionTypeLegalForTable(kGlobalTransaction, kNoneTransaction), false);
  EXPECT_EQ(scanner.CheckTransactionTypeLegalForTable(kGlobalTransaction, kSingleRowTransaction),
            false);
}

TEST(ScannerImpl, PrepareNotifyCell) {
  FLAGS_tera_coord_type = "mock_zk";
  FLAGS_mock_rowlock_enable = true;
  ScannerImpl scanner;
  scanner.tera_client_.reset(new TestClient());
  ErrorCode err;

  std::vector<Column> notify_columns;
  std::vector<std::shared_ptr<NotifyCell>> notify_cells;
  std::set<Column> observe_columns;
  std::shared_ptr<AutoRowUnlocker> unlocker(new AutoRowUnlocker("test_table", "row"));
  Observer* observer = new TestObserver();

  Column column = {"test_table", "row", "qualifier"};
  observe_columns.insert(column);
  tera::Table* table = scanner.tera_client_->OpenTable("test_table", &err);
  (*(scanner.table_observe_info_))["test_table"].table = table;
  (*(scanner.table_observe_info_))["test_table"].type = kGlobalTransaction;
  (*(scanner.table_observe_info_))["test_table"].observe_columns[column].insert(observer);

  scanner.PrepareNotifyCell(table, "row", observe_columns, notify_columns, unlocker, &notify_cells);
  EXPECT_EQ(notify_cells.size(), 0);

  notify_columns.push_back(column);
  scanner.PrepareNotifyCell(table, "row", observe_columns, notify_columns, unlocker, &notify_cells);
  EXPECT_EQ(notify_cells.size(), 1);
  EXPECT_EQ(notify_cells[0]->table, table);
  EXPECT_EQ(notify_cells[0]->row, "row");
  EXPECT_EQ(notify_cells[0]->observed_column, column);
  EXPECT_EQ(notify_cells[0]->observer, observer);
}

TEST(ScannerImpl, GetAckQualifierPrefix) {
  ScannerImpl scanner;

  EXPECT_EQ(scanner.GetAckQualifierPrefix("family", "qualifier"), "family:qualifier");
  EXPECT_EQ(scanner.GetAckQualifierPrefix("a", "b"), "a:b");
  EXPECT_EQ(scanner.GetAckQualifierPrefix("a:", "b"), "a::b");
  EXPECT_EQ(scanner.GetAckQualifierPrefix("a", ":b"), "a::b");
}

TEST(ScannerImpl, GetAckQualifier) {
  ScannerImpl scanner;

  EXPECT_EQ(scanner.GetAckQualifier("prefix", "observer_name"), "prefix+ack_observer_name");
  EXPECT_EQ(scanner.GetAckQualifier("a", "b"), "a+ack_b");
  EXPECT_EQ(scanner.GetAckQualifier("a+", "b"), "a++ack_b");
  EXPECT_EQ(scanner.GetAckQualifier("a", "_b"), "a+ack__b");
  EXPECT_EQ(scanner.GetAckQualifier("a+", "_b"), "a++ack__b");
}

}  // namespace observer
}  // namespace tera
