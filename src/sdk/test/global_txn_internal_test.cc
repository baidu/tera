// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include <iostream>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "sdk/global_txn_internal.h"
#include "sdk/read_impl.h"
#include "sdk/sdk_zk.h"
#include "sdk/sdk_utils.h"
#include "sdk/table_impl.h"
#include "sdk/test/mock_table.h"
#include "tera.h"

DECLARE_string(tera_coord_type);
DECLARE_int32(tera_sdk_read_timeout);
DECLARE_int32(tera_gtxn_all_puts_size_limit);

namespace tera {

class GlobalTxnInternalTest : public ::testing::Test {
 public:
  GlobalTxnInternalTest()
      : start_ts_(100), thread_pool_(2), gtxn_internal_(std::shared_ptr<ClientImpl>()) {
    gtxn_internal_.SetStartTimestamp(start_ts_);
  }

  ~GlobalTxnInternalTest() {}

  std::shared_ptr<Table> OpenTable(const std::string& tablename) {
    FLAGS_tera_coord_type = "fake_zk";
    std::shared_ptr<MockTable> table_(new MockTable(tablename, &thread_pool_));
    return table_;
  }

  void MakeKvPair(const std::string& row, const std::string& cf, const std::string& qu, int64_t ts,
                  const std::string& val, RowResult* value_list) {
    value_list->clear_key_values();
    KeyValuePair* kv = value_list->add_key_values();
    kv->set_key(row);
    kv->set_column_family(cf);
    kv->set_qualifier(qu);
    kv->set_timestamp(ts);
    kv->set_value(val);
  }

  void SetSchema(Table* table, const TableSchema& table_schema) {
    TableImpl* table_impl = static_cast<tera::TableImpl*>(table);
    table_impl->table_schema_ = table_schema;
  }

  void BuildResult(RowReaderImpl* reader_impl, const RowResult& value_list, RowReader::TRow* row) {
    reader_impl->result_.clear_key_values();
    reader_impl->SetResult(value_list);
    row->clear();
    reader_impl->ToMap(row);
  }

 private:
  int64_t start_ts_;
  common::ThreadPool thread_pool_;
  GlobalTxnInternal gtxn_internal_;
};

TEST_F(GlobalTxnInternalTest, CheckTable) {
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  std::shared_ptr<Table> t2 = OpenTable("t2");
  std::shared_ptr<Table> t3 = OpenTable("t3");
  std::shared_ptr<Table> t4 = OpenTable("t4");
  EXPECT_FALSE(t1.get() == NULL);
  EXPECT_FALSE(t2.get() == NULL);
  EXPECT_FALSE(t3.get() == NULL);
  EXPECT_FALSE(t4.get() == NULL);
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();
  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));
  // table<txn=true> and not exist cf<gtxn=true>
  TableDescriptor desc1("t1");
  desc1.EnableTxn();
  desc1.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd11 = desc1.AddColumnFamily("cf1");
  cfd11->DisableGlobalTransaction();
  TableSchema schema1;
  TableDescToSchema(desc1, &schema1);
  SetSchema(t2.get(), schema1);
  EXPECT_FALSE(gtxn_internal_.CheckTable(t2.get(), &status));

  // table<txn=false> and exist cf<gtxn=true>
  TableDescriptor desc2("t1");
  desc2.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd12 = desc2.AddColumnFamily("cf1");
  cfd12->EnableGlobalTransaction();
  TableSchema schema2;
  TableDescToSchema(desc2, &schema2);
  SetSchema(t3.get(), schema2);
  EXPECT_FALSE(gtxn_internal_.CheckTable(t3.get(), &status));

  // table<txn=false> and not exist cf<gtxn=true>
  TableDescriptor desc3("t1");
  desc3.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd13 = desc3.AddColumnFamily("cf1");
  cfd13->DisableGlobalTransaction();
  TableSchema schema3;
  TableDescToSchema(desc3, &schema3);
  SetSchema(t4.get(), schema3);
  EXPECT_FALSE(gtxn_internal_.CheckTable(t4.get(), &status));
}

TEST_F(GlobalTxnInternalTest, IsLockedByOthers) {
  std::shared_ptr<Table> t1 = OpenTable("t1");

  Cell cell1(t1.get(), "row1", "cf1", "qu1", start_ts_, "val");

  RowReader* reader = t1->NewRowReader("row1");
  RowReaderImpl* reader_impl = (RowReaderImpl*)reader;
  RowResult value_list;
  // exist lock col && ts < start_ts_
  // 12 < 100 less than start_ts
  MakeKvPair("row1", "cf1", PackLockName("qu1"), 12, "", &value_list);
  RowReader::TRow row;
  BuildResult(reader_impl, value_list, &row);
  EXPECT_TRUE(gtxn_internal_.IsLockedByOthers(row, cell1));

  // not exist lock col
  value_list.clear_key_values();
  MakeKvPair("row1", "cf1", "qu1", 120, "", &value_list);
  BuildResult(reader_impl, value_list, &row);
  EXPECT_FALSE(gtxn_internal_.IsLockedByOthers(row, cell1));

  // exist lock col && ts > start_ts_
  value_list.clear_key_values();
  // 120 > 100
  MakeKvPair("row1", "cf1", PackLockName("qu1"), 120, "", &value_list);
  BuildResult(reader_impl, value_list, &row);

  EXPECT_FALSE(gtxn_internal_.IsLockedByOthers(row, cell1));
}

TEST_F(GlobalTxnInternalTest, IsPrimary) {
  std::shared_ptr<Table> t1 = OpenTable("t1");
  EXPECT_FALSE(t1.get() == NULL);
  Cell cell1(t1.get(), "row1", "cf1", "qu1", start_ts_, "val");
  Cell cell2(t1.get(), "row1", "cf2", "qu1", start_ts_, "val");

  PrimaryInfo info2;
  info2.set_table_name("t1");
  info2.set_row_key("row1");
  info2.set_column_family("cf1");
  info2.set_qualifier("qu1");
  info2.set_gtxn_start_ts(200);

  EXPECT_TRUE(gtxn_internal_.IsPrimary(cell1, info2));
  EXPECT_FALSE(gtxn_internal_.IsPrimary(cell2, info2));
}

TEST_F(GlobalTxnInternalTest, FindTable) {
  std::shared_ptr<Table> t1 = OpenTable("t1");
  EXPECT_FALSE(t1.get() == NULL);

  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd = desc.AddColumnFamily("cf2");
  cfd->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  // call CheckTable(t1.get())
  ErrorCode status;
  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  // t1 in tables_
  Table* t11 = gtxn_internal_.FindTable("t1");
  EXPECT_TRUE(t11->GetName() == t1->GetName());
}

TEST_F(GlobalTxnInternalTest, ConflictWithOtherWrite) {
  std::shared_ptr<Table> t1 = OpenTable("t1");
  RowReader* r = t1->NewRowReader("row1");
  RowReaderImpl* reader_impl = (RowReaderImpl*)r;
  RowResult value_list;
  // 12 < 100 less than start_ts
  MakeKvPair("row1", "cf1", "qu1", 12, "", &value_list);
  reader_impl->SetResult(value_list);
  ErrorCode status;
  std::vector<Write> ws;
  // ws is empty
  std::unique_ptr<RowReaderImpl> reader(reader_impl);
  EXPECT_FALSE(gtxn_internal_.ConflictWithOtherWrite(&ws, reader, &status));

  // different row writes
  for (int i = 0; i < 3; ++i) {
    Cell cell(t1.get(), "row2", "cf" + std::to_string(i), "qu" + std::to_string(i), start_ts_,
              "val");
    Write w(cell);
    ws.push_back(w);
  }
  EXPECT_FALSE(gtxn_internal_.ConflictWithOtherWrite(&ws, reader, &status));

  // same row, but not exist target cf
  ws.clear();
  for (int i = 0; i < 3; ++i) {
    Cell cell(t1.get(), "row1", "cf0", "qu" + std::to_string(i), start_ts_, "val");
    Write w(cell);
    ws.push_back(w);
  }
  EXPECT_FALSE(gtxn_internal_.ConflictWithOtherWrite(&ws, reader, &status));

  // same row,cf, but not exist write_col, lock_col
  ws.clear();
  for (int i = 0; i < 3; ++i) {
    Cell cell(t1.get(), "row1", "cf1", "qu" + std::to_string(i), start_ts_, "val");
    Write w(cell);
    ws.push_back(w);
  }
  EXPECT_FALSE(gtxn_internal_.ConflictWithOtherWrite(&ws, reader, &status));

  // same row, cf && exist write_col(latest_ts >= start_ts_)
  value_list.clear_key_values();
  // 120 > 100
  MakeKvPair("row1", "cf1", PackWriteName("qu1"), 120, "", &value_list);
  reader_impl->result_.clear_key_values();
  reader_impl->SetResult(value_list);

  EXPECT_TRUE(gtxn_internal_.ConflictWithOtherWrite(&ws, reader, &status));
  EXPECT_TRUE(status.GetType() == ErrorCode::kGTxnWriteConflict);

  // same row, cf && exist write_col(latest_ts < start_ts_)
  // not exist lock_col
  value_list.clear_key_values();
  // 20 < 100 less than start_ts
  MakeKvPair("row1", "cf1", PackWriteName("qu1"), 20, "", &value_list);
  reader_impl->result_.clear_key_values();
  reader_impl->SetResult(value_list);

  EXPECT_FALSE(gtxn_internal_.ConflictWithOtherWrite(&ws, reader, &status));

  // same row, cf && exist write_col(latest_ts < start_ts_)
  // not exist lock_col
  value_list.clear_key_values();
  // 20 < 100 less than start_ts
  MakeKvPair("row1", "cf1", PackWriteName("qu1"), 20, "", &value_list);
  MakeKvPair("row1", "cf1", PackLockName("qu1"), 20, "", &value_list);
  reader_impl->result_.clear_key_values();
  reader_impl->SetResult(value_list);

  EXPECT_TRUE(gtxn_internal_.ConflictWithOtherWrite(&ws, reader, &status));
  EXPECT_TRUE(status.GetType() == ErrorCode::kGTxnLockConflict);
}

TEST_F(GlobalTxnInternalTest, IsGTxnColumnFamily) {
  const std::string cf1 = "cf1", cf2 = "cf2";

  std::shared_ptr<Table> t1 = OpenTable("t1");
  EXPECT_FALSE(t1.get() == NULL);

  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd = desc.AddColumnFamily(cf1);
  cfd->DisableGlobalTransaction();
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily(cf2);
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  // IsGTxnColumnFamily(t1, xxx) must be call after CheckTable(t1.get())
  EXPECT_FALSE(gtxn_internal_.IsGTxnColumnFamily("t1", cf1));
  EXPECT_FALSE(gtxn_internal_.IsGTxnColumnFamily("t1", cf2));
  EXPECT_FALSE(gtxn_internal_.IsGTxnColumnFamily("t2", cf1));
  // call CheckTable(t1.get())
  ErrorCode status;
  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  // call IsGTxnColumnFamily(t1, xxx) cf1 is gtxn=false
  EXPECT_FALSE(gtxn_internal_.IsGTxnColumnFamily("t1", cf1));

  // call IsGTxnColumnFamily(t1, xxx) cf2 is gtxn=true
  EXPECT_TRUE(gtxn_internal_.IsGTxnColumnFamily("t1", cf2));

  // call IsGTxnColumnFamily(t2, xxx)
  EXPECT_FALSE(gtxn_internal_.IsGTxnColumnFamily("t2", cf1));
}

TEST_F(GlobalTxnInternalTest, SetInternalSdkTaskTimeout) {
  std::shared_ptr<Table> t1 = OpenTable("t1");
  RowReader* reader = t1->NewRowReader("row1");
  RowReaderImpl* reader_impl = (RowReaderImpl*)reader;

  EXPECT_TRUE(gtxn_internal_.terminal_time_ == 0);
  gtxn_internal_.SetCommitDuration(1000);
  EXPECT_TRUE(gtxn_internal_.terminal_time_ > 1000);

  gtxn_internal_.SetInternalSdkTaskTimeout(reader);
  EXPECT_TRUE(reader_impl->TimeOut() == 1000);

  sleep(2);
  gtxn_internal_.SetInternalSdkTaskTimeout(reader);
  EXPECT_TRUE(reader_impl->TimeOut() == 1);
  EXPECT_TRUE(gtxn_internal_.IsTimeOut() == true);

  gtxn_internal_.is_timeout_ = false;
  EXPECT_FALSE(gtxn_internal_.terminal_time_ == 0);
  gtxn_internal_.SetCommitDuration(1000000);
  EXPECT_TRUE(gtxn_internal_.terminal_time_ > 1000000);

  gtxn_internal_.SetInternalSdkTaskTimeout(reader);
  EXPECT_TRUE(reader_impl->TimeOut() == FLAGS_tera_sdk_read_timeout);
  EXPECT_TRUE(gtxn_internal_.IsTimeOut() == false);
}

TEST_F(GlobalTxnInternalTest, VerifyWritesSize0) {
  std::shared_ptr<Table> t1 = OpenTable("t1");
  RowMutation* mu = t1->NewRowMutation("r1");
  int64_t writes_size = 0;
  bool ret = gtxn_internal_.VerifyWritesSize(mu, &writes_size);
  EXPECT_TRUE(writes_size == 0);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kBadParam);
  delete mu;
}

TEST_F(GlobalTxnInternalTest, VerifyWritesSize1) {
  std::shared_ptr<Table> t1 = OpenTable("t1");
  RowMutation* mu = t1->NewRowMutation("r1");
  mu->Put("cf0", "qu1", "value", (int64_t)(5));
  mu->Put("cf0", "qu2", "value", (int64_t)(5));
  mu->Put("cf0", "qu3", "value", (int64_t)(5));
  mu->Put("cf0", "qu4", "value", (int64_t)(5));
  mu->DeleteColumns("cf1", "qu5", (int64_t)(5));
  mu->DeleteColumns("cf1", "qu6", (int64_t)(5));
  mu->DeleteColumns("cf1", "qu7", (int64_t)(5));

  int64_t writes_size = 0;
  FLAGS_tera_gtxn_all_puts_size_limit = 10;
  bool ret = gtxn_internal_.VerifyWritesSize(mu, &writes_size);
  RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(mu);
  EXPECT_TRUE(row_mu_impl->Size() == writes_size);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kGTxnDataTooLarge);
  delete mu;
}

TEST_F(GlobalTxnInternalTest, VerifyWritesSize2) {
  std::shared_ptr<Table> t1 = OpenTable("t1");
  RowMutation* mu = t1->NewRowMutation("r1");
  mu->Put("cf0", "qu1", "value", (int64_t)(5));

  int64_t writes_size = 0;
  FLAGS_tera_gtxn_all_puts_size_limit = 100000;
  bool ret = gtxn_internal_.VerifyWritesSize(mu, &writes_size);
  RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(mu);
  EXPECT_TRUE(row_mu_impl->Size() == writes_size);
  EXPECT_TRUE(ret);
  EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kOK);
  delete mu;
}

TEST_F(GlobalTxnInternalTest, BadQualifier) {
  bool ret = BadQualifier("");
  EXPECT_FALSE(ret);
  ret = BadQualifier("aaaaaaaaaaaaaaa");
  EXPECT_FALSE(ret);
  ret = BadQualifier("!*_");
  EXPECT_TRUE(ret);
  ret = BadQualifier("!!!!!!!*_");
  EXPECT_TRUE(ret);
  ret = BadQualifier("!!!!!");
  EXPECT_TRUE(ret);
  ret = BadQualifier("A!");
  EXPECT_FALSE(ret);
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowMutation0) {
  std::shared_ptr<Table> t1 = OpenTable("t1");
  RowMutation* mu = t1->NewRowMutation("r1");
  bool ret = gtxn_internal_.VerifyUserRowMutation(mu);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kBadParam);
  delete mu;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowMutation1) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowMutation* mu = t1->NewRowMutation("r1");
  mu->Put("cf1", "qu1", "value", (int64_t)(5));
  mu->Put("cf1", "!Nqu1", "value", (int64_t)(5));
  mu->Put("cf1", "qu2", "value", (int64_t)(5));
  bool ret = gtxn_internal_.VerifyUserRowMutation(mu);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kBadParam);
  delete mu;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowMutation2) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowMutation* mu = t1->NewRowMutation("r1");
  mu->Put("cf0", "qu1", "value", (int64_t)(5));
  mu->Put("cf1", "qu1_N_", "value", (int64_t)(5));
  mu->Put("cf1", "qu2", "value", (int64_t)(5));
  bool ret = gtxn_internal_.VerifyUserRowMutation(mu);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kBadParam);
  delete mu;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowMutation3) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowMutation* mu = t1->NewRowMutation("r1");
  mu->Put("cf1", "qu1", "value", (int64_t)(5));
  mu->DeleteColumns("cf1", "qu1", (int64_t)(5));
  mu->DeleteColumn("cf1", "qu2", (int64_t)(5));
  mu->DeleteFamily("cf1", (int64_t)(5));
  bool ret = gtxn_internal_.VerifyUserRowMutation(mu);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kGTxnNotSupport);
  delete mu;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowMutation4) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowMutation* mu = t1->NewRowMutation("r1");
  mu->Put("cf1", "qu1", "value", (int64_t)(5));
  mu->DeleteColumns("cf1", "qu1", (int64_t)(5));
  mu->DeleteColumn("cf1", "qu2", (int64_t)(5));
  bool ret = gtxn_internal_.VerifyUserRowMutation(mu);
  EXPECT_TRUE(ret);
  EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kOK);
  delete mu;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowReader0) {
  std::shared_ptr<Table> t1 = OpenTable("t1");
  RowReader* r = t1->NewRowReader("r1");
  bool ret = gtxn_internal_.VerifyUserRowReader(r);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kBadParam);
  delete r;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowReader1) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  // cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_FALSE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowReader* r = t1->NewRowReader("r1");
  r->AddColumn("cf1", "qu");
  bool ret = gtxn_internal_.VerifyUserRowReader(r);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(r->GetError().GetType() == status.GetType());
  delete r;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowReader2) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowReader* r = t1->NewRowReader("r1");
  r->AddColumn("cf1", "qu");
  r->SetSnapshot(10);
  bool ret = gtxn_internal_.VerifyUserRowReader(r);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kBadParam);
  delete r;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowReader3) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowReader* r = t1->NewRowReader("r1");
  r->AddColumnFamily("cf1");
  bool ret = gtxn_internal_.VerifyUserRowReader(r);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kBadParam);
  delete r;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowReader4) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowReader* r = t1->NewRowReader("r1");
  r->AddColumn("cf0", "qu");
  bool ret = gtxn_internal_.VerifyUserRowReader(r);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kBadParam);
  delete r;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowReader5) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowReader* r = t1->NewRowReader("r1");
  r->AddColumn("cf1", "!qu");
  bool ret = gtxn_internal_.VerifyUserRowReader(r);
  EXPECT_FALSE(ret);
  EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kBadParam);
  delete r;
}

TEST_F(GlobalTxnInternalTest, VerifyUserRowReader6) {
  // set a table to tables_
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  RowReader* r = t1->NewRowReader("r1");
  r->AddColumn("cf1", "qu");
  r->AddColumn("cf1", "q1");
  r->AddColumn("cf1", "q2");
  bool ret = gtxn_internal_.VerifyUserRowReader(r);
  EXPECT_TRUE(ret);
  EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kOK);
  delete r;
}

TEST_F(GlobalTxnInternalTest, PrimaryIsLocked1) {
  // bad case b. read primary lock failed
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  tera::PrimaryInfo info2;
  std::string info2_str;
  info2.set_table_name("t1");
  info2.set_row_key("row1");
  info2.set_column_family("cf1");
  info2.set_qualifier("qu1");
  info2.set_gtxn_start_ts(100);
  info2.SerializeToString(&info2_str);
  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  ErrorCode mock_status;
  mock_status.SetFailed(ErrorCode::kSystem, "");
  std::vector<ErrorCode> reader_errs;
  reader_errs.push_back(mock_status);
  (static_cast<MockTable*>(t1.get()))->AddReaderErrors(reader_errs);

  EXPECT_FALSE(gtxn_internal_.PrimaryIsLocked(info2, 12, &status));
  EXPECT_TRUE(status.GetType() == ErrorCode::kSystem);
}

TEST_F(GlobalTxnInternalTest, PrimaryIsLocked2) {
  // bad case a. read primary lock notfound
  ErrorCode status;
  std::shared_ptr<Table> t1 = OpenTable("t1");
  // table<txn=true> and exist cf<gtxn=true>
  TableDescriptor desc("t1");
  desc.EnableTxn();
  desc.AddLocalityGroup("lg0");
  ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
  cfd1->EnableGlobalTransaction();

  TableSchema schema;
  TableDescToSchema(desc, &schema);
  SetSchema(t1.get(), schema);

  tera::PrimaryInfo info2;
  std::string info2_str;
  info2.set_table_name("t1");
  info2.set_row_key("row1");
  info2.set_column_family("cf1");
  info2.set_qualifier("qu1");
  info2.set_gtxn_start_ts(100);
  info2.SerializeToString(&info2_str);
  EXPECT_TRUE(gtxn_internal_.CheckTable(t1.get(), &status));

  ErrorCode mock_status;
  mock_status.SetFailed(ErrorCode::kNotFound, "");
  std::vector<ErrorCode> reader_errs;
  reader_errs.push_back(mock_status);
  (static_cast<MockTable*>(t1.get()))->AddReaderErrors(reader_errs);

  EXPECT_FALSE(gtxn_internal_.PrimaryIsLocked(info2, 12, &status));
}

}  // namespace tera
