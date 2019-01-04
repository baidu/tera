// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <atomic>
#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/thread_pool.h"
#include "common/semaphore.h"
#include "observer/observer.h"
#include "observer/executor/random_key_selector.h"
#include "observer/scanner.h"
#include "observer/executor/scanner_impl.h"
#include "observer/executor/notification_impl.h"
#include "sdk/client_impl.h"
#include "sdk/sdk_utils.h"
#include "sdk/single_row_txn.h"
#include "tera.h"
#include "types.h"
#include "sdk/table_impl.h"

DECLARE_bool(tera_gtxn_test_opened);
DECLARE_int64(start_ts);
DECLARE_int64(begin_commit_ts);
DECLARE_int64(begin_prewrite_ts);
DECLARE_int64(end_prewrite_ts);
DECLARE_int64(commit_ts);
DECLARE_string(flagfile);
DECLARE_string(tera_coord_type);
DECLARE_bool(tera_sdk_client_for_gtxn);
DECLARE_bool(mock_rowlock_enable);
DECLARE_bool(tera_sdk_tso_client_enabled);
DECLARE_int32(observer_scanner_thread_num);

namespace tera {
namespace observer {

class TestWorker : public Observer {
 public:
  TestWorker() : counter_(0), notified_(false) {}
  virtual ~TestWorker() {}
  virtual void OnNotify(tera::Transaction* t, tera::Client* client, const std::string& table_name,
                        const std::string& family, const std::string& qualifier,
                        const std::string& row, const std::string& value, int64_t timestamp,
                        Notification* notification) {
    LOG(INFO) << "[Notify DemoObserver] table:family:qualifer=" << table_name << ":" << family
              << ":" << qualifier << " row=" << row << " value=" << value
              << " timestamps=" << timestamp;

    table_name_ = table_name;
    family_ = family;
    qualifier_ = qualifier;
    row_ = row;
    value_ = value;

    tera::ErrorCode err;
    notified_ = true;
    ++counter_;

    std::unique_ptr<Table> table(client->OpenTable(table_name, &err));
    notification->Ack(table.get(), row, family, qualifier);
  }

  virtual std::string GetObserverName() const { return "DemoObserver"; }

  virtual TransactionType GetTransactionType() const { return kGlobalTransaction; }

 private:
  std::atomic<int> counter_;
  std::atomic<bool> notified_;

  std::string table_name_;
  std::string family_;
  std::string qualifier_;
  std::string row_;
  std::string value_;
};

class TestWorkerGTX : public Observer {
 public:
  TestWorkerGTX() : counter_(0), notified_(false) {}
  virtual ~TestWorkerGTX() {}
  virtual void OnNotify(tera::Transaction* t, tera::Client* client, const std::string& table_name,
                        const std::string& family, const std::string& qualifier,
                        const std::string& row, const std::string& value, int64_t timestamp,
                        Notification* notification) {
    LOG(INFO) << "[Notify TestWorkerGTX] table:family:qualifer=" << table_name << ":" << family
              << ":" << qualifier << " row=" << row << " value=" << value
              << " timestamps=" << timestamp;

    table_name_ = table_name;
    family_ = family;
    qualifier_ = qualifier;
    row_ = row;
    value_ = value;

    tera::ErrorCode err;
    notified_ = true;
    ++counter_;

    std::unique_ptr<Table> table(client->OpenTable(table_name, &err));

    // write ForwordIndex column
    tera::RowMutation* mutation = table->NewRowMutation(row);
    mutation->Put(family, qualifier + "_test", row + "_");
    t->ApplyMutation(mutation);

    tera::ErrorCode error;
    t->Ack(table.get(), row, family, qualifier);
    t->Commit();
    delete mutation;
  }

  virtual std::string GetObserverName() const { return "DemoObserver"; }

  virtual TransactionType GetTransactionType() const { return kSingleRowTransaction; }

 private:
  std::atomic<int> counter_;
  std::atomic<bool> notified_;

  std::string table_name_;
  std::string family_;
  std::string qualifier_;
  std::string row_;
  std::string value_;
};

class DemoObserver : public tera::observer::Observer {
 public:
  DemoObserver() {}
  virtual ~DemoObserver() {}
  virtual void OnNotify(tera::Transaction* t, tera::Client* client, const std::string& table_name,
                        const std::string& family, const std::string& qualifier,
                        const std::string& row, const std::string& value, int64_t timestamp,
                        Notification* notification) {
    LOG(INFO) << "[Notify ParseObserver] table:family:qualifer=" << table_name << ":" << family
              << ":" << qualifier << " row=" << row << " value=" << value
              << " timestamps=" << timestamp;
    // do nothing
  }
  virtual std::string GetObserverName() const { return "DemoObserver"; }
  virtual TransactionType GetTransactionType() const { return kGlobalTransaction; }
};

class TestWorkerNTX : public Observer {
 public:
  TestWorkerNTX() : counter_(0), notified_(false) {}
  virtual ~TestWorkerNTX() {}
  virtual void OnNotify(tera::Transaction* t, tera::Client* client, const std::string& table_name,
                        const std::string& family, const std::string& qualifier,
                        const std::string& row, const std::string& value, int64_t timestamp,
                        Notification* notification) {
    LOG(INFO) << "[Notify TestWorkerNTX] table:family:qualifer=" << table_name << ":" << family
              << ":" << qualifier << " row=" << row << " value=" << value
              << " timestamps=" << timestamp;

    table_name_ = table_name;
    family_ = family;
    qualifier_ = qualifier;
    row_ = row;
    value_ = value;

    notified_ = true;
    ++counter_;

    // do something without transaction
  }

  virtual std::string GetObserverName() const { return "DemoObserver"; }

  virtual TransactionType GetTransactionType() const { return kNoneTransaction; }

 private:
  std::atomic<int> counter_;
  std::atomic<bool> notified_;

  std::string table_name_;
  std::string family_;
  std::string qualifier_;
  std::string row_;
  std::string value_;
};

class TestTxn : public SingleRowTxn {
 public:
  TestTxn(Table* table, const std::string& row_key, common::ThreadPool* thread_pool,
          int64_t start_ts = 0)
      : SingleRowTxn(static_cast<TableWrapper*>(table)->GetTableImpl(), row_key, thread_pool),
        start_timestamp_(start_ts) {}
  ~TestTxn() {}

  virtual int64_t GetStartTimestamp() { return 10; }

 private:
  int64_t start_timestamp_;
};

class ObserverImplTest : public ::testing::Test {
 public:
  void OnNotifyTest() {
    tera::ErrorCode err;
    LOG(ERROR) << "FALG FILE: " << FLAGS_flagfile;
    tera::Client* client = tera::Client::NewClient(FLAGS_flagfile, &err);
    // for ut test
    EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
    // for no core
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "new client failed";
      return;
    }

    // create table
    tera::TableDescriptor table_desc("observer_test_table");
    table_desc.EnableTxn();

    table_desc.AddLocalityGroup("lg1");
    tera::ColumnFamilyDescriptor* cf1 = table_desc.AddColumnFamily("cf", "lg1");
    cf1->EnableGlobalTransaction();
    cf1->EnableNotify();
    ExtendNotifyLgToDescriptor(&table_desc);

    client->CreateTable(table_desc, &err);
    if (err.GetType() != tera::ErrorCode::kOK) {
      LOG(ERROR) << "Create table fail";
      return;
    }

    std::unique_ptr<Table> table(client->OpenTable("observer_test_table", &err));
    EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "open table failed";
      return;
    }

    sleep(1);
    std::unique_ptr<tera::Transaction> t(table->StartRowTransaction("www.baidu.com"));

    assert(t != NULL);
    std::unique_ptr<tera::RowMutation> mu0(table->NewRowMutation("www.baidu.com"));
    mu0->Put("_N_", "cf:Page", "I am not important");
    t->ApplyMutation(mu0.get());
    t->Commit();

    std::unique_ptr<tera::Transaction> g_txn(client->NewGlobalTransaction());
    assert(g_txn != NULL);
    std::unique_ptr<tera::RowMutation> mu1(table->NewRowMutation("www.baidu.com"));

    mu1->Put("cf", "Page", "hello world", -1);
    g_txn->ApplyMutation(mu1.get());
    g_txn->Commit();

    if (g_txn->GetError().GetType() != tera::ErrorCode::kOK) {
      std::cout << g_txn->GetError().ToString() << std::endl;
    } else {
      std::cout << "commit success" << std::endl;
    }

    // varibles for fake timeoracle
    FLAGS_start_ts = 10;
    FLAGS_begin_commit_ts = 1;
    FLAGS_begin_prewrite_ts = 1;
    FLAGS_end_prewrite_ts = 1;
    FLAGS_commit_ts = 13;

    Observer* observer = new TestWorker();
    Observer* demo = new DemoObserver();

    Scanner* scanner = new ScannerImpl();
    bool ret = scanner->Init();
    EXPECT_EQ(true, ret);
    if (!ret) {
      LOG(ERROR) << "fail to init scanner_impl";
      return;
    }

    err = scanner->Observe("observer_test_table", "cf", "Page", observer);
    EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

    err = scanner->Observe("observer_test_table", "cf", "Page", demo);
    EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

    if (!scanner->Start()) {
      LOG(ERROR) << "fail to start scanner_impl";
      return;
    }

    while (!static_cast<TestWorker*>(observer)->notified_) {
      sleep(1);
    }

    EXPECT_EQ("www.baidu.com", static_cast<TestWorker*>(observer)->row_);
    EXPECT_EQ("observer_test_table", static_cast<TestWorker*>(observer)->table_name_);
    EXPECT_EQ("cf", static_cast<TestWorker*>(observer)->family_);
    EXPECT_EQ("Page", static_cast<TestWorker*>(observer)->qualifier_);
    EXPECT_EQ("hello world", static_cast<TestWorker*>(observer)->value_);

    scanner->Exit();
    delete scanner;
  }

  void SingleRowTransactionTest() {
    tera::ErrorCode err;
    tera::Client* client = tera::Client::NewClient(FLAGS_flagfile, &err);
    // for ut test
    EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
    // for no core
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "new client failed";
      return;
    }

    // create table
    tera::TableDescriptor table_desc("observer_table_gtx");
    table_desc.EnableTxn();

    table_desc.AddLocalityGroup("lg1");
    tera::ColumnFamilyDescriptor* cf1 = table_desc.AddColumnFamily("cf", "lg1");
    cf1->EnableNotify();
    ExtendNotifyLgToDescriptor(&table_desc);

    client->CreateTable(table_desc, &err);
    if (err.GetType() != tera::ErrorCode::kOK) {
      LOG(ERROR) << "Create table fail";
      return;
    }

    std::unique_ptr<Table> table(client->OpenTable("observer_table_gtx", &err));
    EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "open table failed";
      return;
    }

    std::unique_ptr<tera::Transaction> t(table->StartRowTransaction("www.baidu.com"));

    assert(t != NULL);
    std::unique_ptr<tera::RowMutation> mu0(table->NewRowMutation("www.baidu.com"));
    mu0->Put("_N_", "cf:Page", "I am not important");
    mu0->Put("cf", "Page", "hello world", -1);
    t->ApplyMutation(mu0.get());
    t->Commit();

    if (t->GetError().GetType() != tera::ErrorCode::kOK) {
      std::cout << t->GetError().ToString() << std::endl;
    } else {
      std::cout << "commit success" << std::endl;
    }

    Observer* observer = new TestWorkerGTX();

    Scanner* scanner = new ScannerImpl();
    bool ret = scanner->Init();

    EXPECT_EQ(true, ret);
    if (!ret) {
      LOG(ERROR) << "fail to init scanner_impl";
      return;
    }

    err = scanner->Observe("observer_table_gtx", "cf", "Page", observer);
    EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

    if (!scanner->Start()) {
      LOG(ERROR) << "fail to start scanner_impl";
      return;
    }

    while (!static_cast<TestWorkerGTX*>(observer)->notified_) {
      sleep(1);
    }

    EXPECT_EQ("www.baidu.com", static_cast<TestWorkerGTX*>(observer)->row_);
    EXPECT_EQ("observer_table_gtx", static_cast<TestWorkerGTX*>(observer)->table_name_);
    EXPECT_EQ("cf", static_cast<TestWorkerGTX*>(observer)->family_);
    EXPECT_EQ("Page", static_cast<TestWorkerGTX*>(observer)->qualifier_);
    EXPECT_EQ("hello world", static_cast<TestWorkerGTX*>(observer)->value_);
    LOG(ERROR) << "Finish";
    scanner->Exit();
    delete scanner;
  }

  void NonTransactionTest() {
    tera::ErrorCode err;
    tera::Client* client = tera::Client::NewClient(FLAGS_flagfile, &err);
    // for ut test
    EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
    // for no core
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "new client failed";
      return;
    }

    // create table
    tera::TableDescriptor table_desc("observer_table_ntx");

    table_desc.AddLocalityGroup("lg1");
    tera::ColumnFamilyDescriptor* cf1 = table_desc.AddColumnFamily("cf", "lg1");
    cf1->EnableNotify();
    ExtendNotifyLgToDescriptor(&table_desc);

    client->CreateTable(table_desc, &err);
    if (err.GetType() != tera::ErrorCode::kOK) {
      LOG(ERROR) << "Create table fail";
      return;
    }

    std::unique_ptr<Table> table(client->OpenTable("observer_table_ntx", &err));
    EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "open table failed";
      return;
    }

    table->Put("www.baidu.com", "_N_", "cf:Page", "I am not important", &err);
    if (err.GetType() != tera::ErrorCode::kOK) {
      LOG(ERROR) << "put _N_ error";
      return;
    }
    table->Put("www.baidu.com", "cf", "Page", "hello world", -1, &err);
    if (err.GetType() != tera::ErrorCode::kOK) {
      LOG(ERROR) << "put cf error";
      return;
    }

    Observer* observer = new TestWorkerNTX();

    Scanner* scanner = new ScannerImpl();
    bool ret = scanner->Init();

    EXPECT_EQ(true, ret);
    if (!ret) {
      LOG(ERROR) << "fail to init scanner_impl";
      return;
    }

    err = scanner->Observe("observer_table_ntx", "cf", "Page", observer);
    EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

    if (!scanner->Start()) {
      LOG(ERROR) << "fail to start scanner_impl";
      return;
    }

    while (!static_cast<TestWorkerNTX*>(observer)->notified_) {
      sleep(1);
    }

    EXPECT_EQ("www.baidu.com", static_cast<TestWorkerNTX*>(observer)->row_);
    EXPECT_EQ("observer_table_ntx", static_cast<TestWorkerNTX*>(observer)->table_name_);
    EXPECT_EQ("cf", static_cast<TestWorkerNTX*>(observer)->family_);
    EXPECT_EQ("Page", static_cast<TestWorkerNTX*>(observer)->qualifier_);
    EXPECT_EQ("hello world", static_cast<TestWorkerNTX*>(observer)->value_);
    scanner->Exit();
    delete scanner;
  }

  void ObserveTest() {
    tera::ErrorCode err;
    tera::Client* client = tera::Client::NewClient(FLAGS_flagfile, &err);
    // for ut test
    EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
    // for no core
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "new client failed";
      return;
    }

    // create table
    tera::TableDescriptor table_desc("observer_table");
    table_desc.EnableTxn();
    table_desc.AddLocalityGroup("notify");
    tera::ColumnFamilyDescriptor* cf_t = table_desc.AddColumnFamily(kNotifyColumnFamily, "notify");
    cf_t->EnableGlobalTransaction();

    table_desc.AddLocalityGroup("lg1");
    tera::ColumnFamilyDescriptor* cf1 = table_desc.AddColumnFamily("cf", "lg1");
    cf1->EnableGlobalTransaction();
    cf1->EnableNotify();
    tera::ColumnFamilyDescriptor* cf2 = table_desc.AddColumnFamily("cf_1", "lg1");
    cf2->EnableGlobalTransaction();
    cf2->EnableNotify();

    ExtendNotifyLgToDescriptor(&table_desc);

    client->CreateTable(table_desc, &err);
    if (err.GetType() != tera::ErrorCode::kOK) {
      LOG(ERROR) << "Create table fail";
    }

    FLAGS_tera_sdk_client_for_gtxn = true;
    FLAGS_tera_coord_type = "ins";
    common::ThreadPool thread_pool(5);
    ScannerImpl* scanner = new ScannerImpl();
    Observer* observer = new DemoObserver();
    scanner->key_selector_.reset(new RandomKeySelector());

    // single thread

    err = scanner->Observe("observer_table", "cf", "qualifier", observer);
    EXPECT_TRUE(err.GetType() != tera::ErrorCode::kOK);

    scanner->tera_client_.reset(tera::Client::NewClient(FLAGS_flagfile, &err));
    EXPECT_EQ(scanner->table_observe_info_->size(), 0);

    err = scanner->Observe("observer_table", "cf", "qualifier", observer);
    EXPECT_TRUE(err.GetType() == tera::ErrorCode::kOK);

    err = scanner->Observe("observer_table", "cf", "qualifier", observer);
    EXPECT_FALSE(err.GetType() == tera::ErrorCode::kOK);

    err = scanner->Observe("observer_table", "cf_1", "qualifier", observer);
    EXPECT_TRUE(err.GetType() == tera::ErrorCode::kOK);

    // multi thread
    std::string qualifier;

    for (uint32_t i = 0; i < 10; ++i) {
      qualifier += 'a';
      thread_pool.AddTask(
          std::bind(&ScannerImpl::Observe, scanner, "observer_table", "cf", qualifier, observer));
    }
    thread_pool.Stop(true);
    EXPECT_EQ(1, scanner->observers_.size());
    EXPECT_EQ(10 + 2, (*(scanner->table_observe_info_))["observer_table"].observe_columns.size());
    scanner->Exit();
    delete scanner;
  }

  void ValidateCellValueTest() {
    tera::ErrorCode err;
    std::unique_ptr<tera::Client> client(tera::Client::NewClient(FLAGS_flagfile, &err));
    // for ut test
    EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
    // for no core
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "new client failed";
      return;
    }

    common::ThreadPool thread_pool(5);
    ScannerImpl* scanner = new ScannerImpl();
    scanner->key_selector_.reset(new RandomKeySelector());
    std::unique_ptr<Table> table(client->OpenTable("observer_test_table", &err));

    Observer* observer = new TestWorker();
    bool ret = scanner->Init();
    EXPECT_EQ(true, ret);
    if (!ret) {
      LOG(ERROR) << "fail to init scanner_impl";
      return;
    }
    err = scanner->Observe("observer_test_table", "Data", "Page", observer);
    EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

    ScannerImpl::NotificationContext* context = new ScannerImpl::NotificationContext();
    common::Semaphore s(1);
    s.Acquire();
    std::shared_ptr<NotifyCell> notify_cell(new NotifyCell(s));
    context->notify_cell = notify_cell;
    Column column = {"observer_test_table", "Data", "qu"};
    notify_cell->row = "row";
    notify_cell->observed_column = column;
    notify_cell->table = table.get();

    // no value
    RowReader* row_reader(table->NewRowReader("no_row"));
    row_reader->SetContext(context);
    row_reader->AddColumn("Data", "qu");
    table->Get(row_reader);
    scanner->ValidateCellValue(row_reader);
    sleep(1);
    EXPECT_FALSE(static_cast<TestWorker*>(observer)->notified_);

    // no table
    table->Put("row1", "Data", "qu", "value", &err);
    if (err.GetType() != tera::ErrorCode::kOK) {
      LOG(ERROR) << "put error: " << err.GetReason();
      return;
    }
    row_reader = table->NewRowReader("row1");
    context = new ScannerImpl::NotificationContext();
    context->notify_cell = notify_cell;
    notify_cell->row = "row 1";

    row_reader->SetContext(context);
    table->Get(row_reader);
    scanner->ValidateCellValue(row_reader);
    sleep(1);
    EXPECT_FALSE(static_cast<TestWorker*>(observer)->notified_);

    // no column
    column = {"observer_test_table", "Data", "qu"};
    row_reader = table->NewRowReader("row1");
    context = new ScannerImpl::NotificationContext();
    context->notify_cell = notify_cell;
    notify_cell->row = "row 1";
    row_reader->SetContext(context);

    table->Get(row_reader);
    scanner->ValidateCellValue(row_reader);
    sleep(1);
    EXPECT_FALSE(static_cast<TestWorker*>(observer)->notified_);

    scanner->Exit();
    delete scanner;
  }

  void ValidateAckConfilictTest() {
    tera::ErrorCode err;
    std::unique_ptr<tera::Client> client(tera::Client::NewClient(FLAGS_flagfile, &err));
    // for ut test
    EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
    // for no core
    if (tera::ErrorCode::kOK != err.GetType()) {
      LOG(ERROR) << "new client failed";
      return;
    }

    common::ThreadPool thread_pool(5);
    ScannerImpl* scanner = new ScannerImpl();
    scanner->key_selector_.reset(new RandomKeySelector());
    std::unique_ptr<Table> table(client->OpenTable("observer_test_table", &err));

    Observer* observer = new TestWorker();
    bool ret = scanner->Init();
    EXPECT_EQ(true, ret);
    if (!ret) {
      LOG(ERROR) << "fail to init scanner_impl";
      return;
    }
    err = scanner->Observe("observer_test_table", "cf", "qu", observer);
    EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

    ScannerImpl::NotificationContext* context = new ScannerImpl::NotificationContext();
    common::Semaphore s(1);
    s.Acquire();
    std::shared_ptr<NotifyCell> notify_cell(new NotifyCell(s));
    context->notify_cell = notify_cell;
    Column column = {"observer_test_table", "cf", "qu"};
    notify_cell->row = "row1";
    notify_cell->observed_column = column;
    notify_cell->timestamp = 1;

    // no value
    RowReader* row_reader = table->NewRowReader("row1");
    row_reader->AddColumn("cf", "qu");
    row_reader->SetContext(context);

    // wrong value, context deleted
    table->Put("row1", "cf", "qu", "!#%E^E%&$&$%&$^", &err);
    table->Get(row_reader);
    scanner->ValidateAckConfict(row_reader);
    sleep(1);
    EXPECT_FALSE(static_cast<TestWorker*>(observer)->notified_);

    // wrong ts
    table->Put("row2", "cf", "qu", "10", &err);
    context = new ScannerImpl::NotificationContext();
    row_reader = table->NewRowReader("row2");
    row_reader->AddColumn("cf", "qu");
    row_reader->SetContext(context);
    notify_cell->row = "row2";
    context->notify_cell = notify_cell;
    notify_cell->notify_transaction.reset(new TestTxn(table.get(), "row2", &thread_pool, 10));

    table->Get(row_reader);
    scanner->ValidateAckConfict(row_reader);
    sleep(1);
    EXPECT_FALSE(static_cast<TestWorker*>(observer)->notified_);

    scanner->Exit();
    delete scanner;
  }
};

TEST_F(ObserverImplTest, OnNotifyTest) { OnNotifyTest(); }

TEST_F(ObserverImplTest, SingleRowTransactionTest) { SingleRowTransactionTest(); }

TEST_F(ObserverImplTest, NoneTransactionTest) { NonTransactionTest(); }

TEST_F(ObserverImplTest, ObserveTest) { ObserveTest(); }

TEST_F(ObserverImplTest, ValidateCellValue) { ValidateCellValueTest(); }

TEST_F(ObserverImplTest, ValidateAckConfilict) { ValidateAckConfilictTest(); }

}  // namespace observer
}  // namespace tera

int main(int argc, char** argv) {
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  ::testing::InitGoogleTest(&argc, argv);

  FLAGS_mock_rowlock_enable = true;
  FLAGS_tera_sdk_client_for_gtxn = true;
  FLAGS_tera_gtxn_test_opened = false;
  FLAGS_tera_sdk_tso_client_enabled = false;
  FLAGS_observer_scanner_thread_num = 1;
  int ret = RUN_ALL_TESTS();
  exit(EXIT_FAILURE);
  return 0;
}
