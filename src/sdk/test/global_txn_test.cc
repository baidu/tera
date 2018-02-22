// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include <atomic>
#include <iostream>
#include <string>
#include <thread>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "sdk/global_txn.h"
#include "sdk/global_txn_internal.h"
#include "sdk/read_impl.h"
#include "sdk/table_impl.h"
#include "sdk/sdk_zk.h"
#include "sdk/test/mock_table.h"
#include "tera.h"

DECLARE_string(tera_coord_type);

namespace tera {

class GlobalTxnTest : public ::testing::Test {
public:
    GlobalTxnTest() : 
        thread_pool_(2), 
        gtxn_(Client::NewClient(), &thread_pool_, (new sdk::MockTimeoracleClusterFinder(""))) {
        gtxn_.status_.SetFailed(ErrorCode::kOK);
        gtxn_.status_returned_ = false;
    }
    
    ~GlobalTxnTest() {}
    
    void SetSchema(Table* table, const TableSchema& table_schema) {
        TableImpl* table_impl = static_cast<tera::TableImpl*>(table);
        table_impl->table_schema_ = table_schema;    
    }
    
    Table* OpenTable(const std::string& tablename) {
        FLAGS_tera_coord_type = "fake_zk";
        return static_cast<tera::Table*>(new MockTable(tablename, &thread_pool_));
    }
    
private:
    common::ThreadPool thread_pool_;
    GlobalTxn gtxn_;
};

TEST_F(GlobalTxnTest, Commit) {

    // sync commit ut
    gtxn_.user_commit_callback_ = NULL;
    // mutation haven't apply
    gtxn_.finish_ = false;
    gtxn_.status_returned_ = false;
    gtxn_.put_fail_cnt_.Set(10);
    gtxn_.has_commited_ = false;
    EXPECT_TRUE(gtxn_.Commit().GetType() == ErrorCode::kGTxnOpAfterCommit);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnOpAfterCommit);
    EXPECT_TRUE(gtxn_.finish_ == true);
    EXPECT_TRUE(gtxn_.has_commited_ == false);

    // have commited
    gtxn_.finish_ = false;
    gtxn_.status_returned_ = false;
    gtxn_.put_fail_cnt_.Set(0);
    gtxn_.has_commited_ = true;
    EXPECT_TRUE(gtxn_.Commit().GetType() == ErrorCode::kGTxnOpAfterCommit);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnOpAfterCommit);
    EXPECT_TRUE(gtxn_.finish_ == true);
    EXPECT_TRUE(gtxn_.has_commited_ == true);

    // run commit in the legal state
    gtxn_.finish_ = false;
    gtxn_.status_returned_ = false;
    gtxn_.writes_.clear();
    gtxn_.put_fail_cnt_.Set(0);
    gtxn_.has_commited_ = false;
    EXPECT_TRUE(gtxn_.Commit().GetType() == ErrorCode::kOK);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kOK);
    EXPECT_TRUE(gtxn_.finish_ == true);
    EXPECT_TRUE(gtxn_.has_commited_ == true);
}

TEST_F(GlobalTxnTest, DoVerifyPrimaryLockedCallback) {
    RowReaderImpl* reader_impl = new RowReaderImpl(NULL, "rowkey");
    SingleRowTxn* txn = new SingleRowTxn(NULL, "rowkey", NULL);
    reader_impl->txn_ = txn;

    // not found primary
    reader_impl->error_code_.SetFailed(ErrorCode::kNotFound, "");

    RowReader* reader = static_cast<RowReader*>(reader_impl);
    gtxn_.DoVerifyPrimaryLockedCallback(reader);    
    EXPECT_TRUE(gtxn_.finish_ == true);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnPrimaryLost);
}

TEST_F(GlobalTxnTest, DoVerifyPrimaryLockedCallback1) {
    RowReaderImpl* reader_impl = new RowReaderImpl(NULL, "rowkey");
    SingleRowTxn* txn = new SingleRowTxn(NULL, "rowkey", NULL);
    reader_impl->txn_ = txn;

    // reader timeout
    reader_impl->error_code_.SetFailed(ErrorCode::kTimeout, "");
    RowReader* reader = static_cast<RowReader*>(reader_impl);
    gtxn_.DoVerifyPrimaryLockedCallback(reader);    
    EXPECT_TRUE(gtxn_.finish_ == true);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnPrimaryCommitTimeout);
}

TEST_F(GlobalTxnTest, DoVerifyPrimaryLockedCallback2) {
    RowReaderImpl* reader_impl = new RowReaderImpl(NULL, "rowkey");
    SingleRowTxn* txn = new SingleRowTxn(NULL, "rowkey", NULL);
    reader_impl->txn_ = txn;
    // reader other error
    reader_impl->error_code_.SetFailed(ErrorCode::kSystem, "");
    RowReader* reader = static_cast<RowReader*>(reader_impl);
    gtxn_.DoVerifyPrimaryLockedCallback(reader);    
    EXPECT_TRUE(gtxn_.finish_ == true);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kSystem);
}

TEST_F(GlobalTxnTest, CheckPrimaryStatusAndCommmitSecondaries) {
    SingleRowTxn* txn = new SingleRowTxn(NULL, "rowkey", NULL);
    
    // primary commit timeout
    gtxn_.finish_ = false;
    gtxn_.status_returned_ = false;
    txn->mutation_buffer_.SetError(ErrorCode::kTimeout,"");
    gtxn_.CheckPrimaryStatusAndCommmitSecondaries(txn);
    EXPECT_TRUE(gtxn_.finish_ == true);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnPrimaryCommitTimeout);

    // primary commit other error
    gtxn_.finish_ = false;
    gtxn_.status_returned_ = false;
    txn = new SingleRowTxn(NULL, "rowkey", NULL);
    txn->mutation_buffer_.SetError(ErrorCode::kSystem, "");
    gtxn_.CheckPrimaryStatusAndCommmitSecondaries(txn);

    EXPECT_TRUE(gtxn_.finish_ == true);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kSystem);

    // primary done run next step
    gtxn_.finish_ = false;
    gtxn_.status_returned_ = false;
    txn = new SingleRowTxn(NULL, "rowkey", NULL);
    txn->mutation_buffer_.SetError(ErrorCode::kOK, "");
    gtxn_.writes_.clear();
    const std::string tablename = "test_t";
    Table* t = OpenTable(tablename);
    Cell cell(t, "r1", "cf", "qu", 1, "val");
    Write w(cell);
    // insert a 'Write'
    gtxn_.SaveWrite(tablename, "r1", w);

    gtxn_.acks_.clear();
    gtxn_.notifies_.clear();
    gtxn_.CheckPrimaryStatusAndCommmitSecondaries(txn);

    EXPECT_TRUE(gtxn_.finish_ == true);
    EXPECT_TRUE(gtxn_.status_returned_ == true);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kOK);
}

TEST_F(GlobalTxnTest, SaveWrite) {
    const std::string tablename = "test_t", tablename4 = "test_t4";
    Table* t = OpenTable(tablename);
    const std::string row_key = "r1", row_key4 = "r2";
    Cell cell(t, row_key, "cf", "qu", 1, "val");
    Write w(cell);
    gtxn_.writes_.clear();
    // insert a 'Write'
    gtxn_.SaveWrite(tablename, row_key, w);
    GlobalTxn::TableWithRowkey twr(tablename, row_key);
    auto w1 = gtxn_.writes_.find(twr);
    EXPECT_TRUE(w1 != gtxn_.writes_.end());
    
    // insert a same 'Write'
    gtxn_.SaveWrite(tablename, row_key, w);
    EXPECT_TRUE(gtxn_.writes_.size() == 1);

    // insert a delete type 'Write' at same Cell
    Cell cell2(t, row_key, "cf", "qu", 1);
    Write w2(cell2);
    gtxn_.SaveWrite(tablename, row_key, w2);
    EXPECT_TRUE(gtxn_.writes_.size() == 1);
    
    delete t;
}

TEST_F(GlobalTxnTest, DoAckCallback) {
    const std::string tablename = "test_t1", tablename5 = "test_t5";
    Table* t1 = OpenTable(tablename);
    Table* t5 = OpenTable(tablename5);
    
    // test acks cnt = 2 && not notify
    RowMutation* mu1 = t1->NewRowMutation("r1");
    RowMutation* mu5 = t5->NewRowMutation("r1");
    gtxn_.finish_ = false;
    gtxn_.ack_done_cnt_.Set(0);
    gtxn_.acks_cnt_.Set(2);
    gtxn_.notifies_cnt_.Set(0);
    gtxn_.DoAckCallback(mu1);
    EXPECT_TRUE(gtxn_.finish_ == false);
    gtxn_.DoAckCallback(mu5);
    EXPECT_TRUE(gtxn_.finish_ == true);
    
    // test acks cnt = 2 && notify cnt > 0
    RowMutation* mu11 = t1->NewRowMutation("r1");
    RowMutation* mu55 = t5->NewRowMutation("r1");
    gtxn_.finish_ = false;
    gtxn_.ack_done_cnt_.Set(0);
    gtxn_.acks_cnt_.Set(2);
    gtxn_.notifies_cnt_.Set(1);

    gtxn_.DoAckCallback(mu11);
    EXPECT_TRUE(gtxn_.finish_ == false);
    gtxn_.DoAckCallback(mu55);
    EXPECT_TRUE(gtxn_.finish_ == false);

    delete t1;
    delete t5;
}

TEST_F(GlobalTxnTest, DoNotifyCallback) {
    const std::string tablename = "test_t11", tablename5 = "test_t55";
    Table* t11 = OpenTable(tablename);
    Table* t55 = OpenTable(tablename5);
    
    // test notifies cnt = 2
    RowMutation* mu1 = t11->NewRowMutation("r1");
    RowMutation* mu5 = t55->NewRowMutation("r1");
    gtxn_.finish_ = false;
    gtxn_.notify_done_cnt_.Set(0);
    gtxn_.notifies_cnt_.Set(2);
    gtxn_.all_task_pushed_ = true;
    gtxn_.DoNotifyCallback(mu1);
    EXPECT_TRUE(gtxn_.finish_ == false);
    gtxn_.DoNotifyCallback(mu5);
    EXPECT_TRUE(gtxn_.finish_ == true);
    delete t11;
    delete t55;
}

void NotifyWarpper(GlobalTxn* gtxn, 
                          Table* t,
                          const std::string& row_key,
                          const std::string& column_family,
                          const std::string& qualifier) {
    gtxn->Notify(t, row_key, column_family, qualifier);
}

TEST_F(GlobalTxnTest, Notify) {
    size_t notify_thread_cnt = 30;
    std::vector<std::thread> threads;
    // all Table* is NULL
    gtxn_.notifies_.clear();
    gtxn_.notifies_cnt_.Set(0);
    EXPECT_TRUE(0 == gtxn_.notifies_.size());
    EXPECT_TRUE(gtxn_.notifies_cnt_.Get() == 0);
    threads.reserve(notify_thread_cnt);
    Table* t0 = NULL;
    for (int i = 0; i < notify_thread_cnt; ++i) {
        threads.emplace_back(std::thread(NotifyWarpper, &gtxn_, t0, "", "", ""));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(0 == gtxn_.notifies_.size());
    EXPECT_TRUE(gtxn_.notifies_cnt_.Get() == 0);

    // same table and same row
    gtxn_.notifies_.clear();
    gtxn_.notifies_cnt_.Set(0);
    EXPECT_TRUE(0 == gtxn_.notifies_.size());
    EXPECT_TRUE(gtxn_.notifies_cnt_.Get() == 0);
    Table* t1 = OpenTable("t1");
    threads.reserve(30);
    for (int i = 0; i < notify_thread_cnt; ++i) {
        threads.emplace_back(std::thread(NotifyWarpper, &gtxn_, t1, "r1", "", ""));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(1 == gtxn_.notifies_.size());
    EXPECT_TRUE(gtxn_.notifies_cnt_.Get() == 1);
    GlobalTxn::TableWithRowkey twr("t1", "r1");
    EXPECT_TRUE(gtxn_.notifies_[twr].size() == notify_thread_cnt);

    // same table and diff row
    gtxn_.notifies_.clear();
    gtxn_.notifies_cnt_.Set(0);
    EXPECT_TRUE(0 == gtxn_.notifies_.size());
    EXPECT_TRUE(gtxn_.notifies_cnt_.Get() == 0);
    for (int i = 0; i < notify_thread_cnt; ++i) {
        threads.emplace_back(std::thread(NotifyWarpper, &gtxn_, t1, "r" + std::to_string(i), "", ""));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(notify_thread_cnt == gtxn_.notifies_.size());
    EXPECT_TRUE(gtxn_.notifies_cnt_.Get() == notify_thread_cnt);

    for (int i = 0; i < notify_thread_cnt; ++i) {
        GlobalTxn::TableWithRowkey twr1("t1", "r" + std::to_string(i));
        EXPECT_TRUE(gtxn_.notifies_[twr1].size() == 1);
    }
}

void AckWarpper(GlobalTxn* gtxn, Table* t,
                const std::string& row_key,
                const std::string& column_family,
                const std::string& qualifier) {
    gtxn->Ack(t, row_key, column_family, qualifier);
}

TEST_F(GlobalTxnTest, Ack) {
    size_t ack_thread_cnt = 30;
    std::vector<std::thread> threads;
    // all Table* is NULL
    gtxn_.acks_.clear();
    gtxn_.acks_cnt_.Set(0);
    EXPECT_TRUE(0 == gtxn_.acks_.size());
    EXPECT_TRUE(gtxn_.acks_cnt_.Get() == 0);
    threads.reserve(ack_thread_cnt);
    Table* t0 = NULL;
    for (int i = 0; i < ack_thread_cnt; ++i) {
        threads.emplace_back(std::thread(AckWarpper, &gtxn_, t0, "", "", ""));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(0 == gtxn_.acks_.size());
    EXPECT_TRUE(gtxn_.acks_cnt_.Get() == 0);

    // same table and same row
    gtxn_.acks_.clear();
    gtxn_.acks_cnt_.Set(0);
    EXPECT_TRUE(0 == gtxn_.acks_.size());
    EXPECT_TRUE(gtxn_.acks_cnt_.Get() == 0);
    Table* t1 = OpenTable("t1");
    threads.reserve(30);
    for (int i = 0; i < ack_thread_cnt; ++i) {
        threads.emplace_back(std::thread(AckWarpper, &gtxn_, t1, "r1", "", ""));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(1 == gtxn_.acks_.size());
    EXPECT_TRUE(gtxn_.acks_cnt_.Get() == 1);
    GlobalTxn::TableWithRowkey twr("t1", "r1");
    EXPECT_TRUE(gtxn_.acks_[twr].size() == ack_thread_cnt);

    // same table and diff row
    gtxn_.acks_.clear();
    gtxn_.acks_cnt_.Set(0);
    EXPECT_TRUE(0 == gtxn_.acks_.size());
    EXPECT_TRUE(gtxn_.acks_cnt_.Get() == 0);
    for (int i = 0; i < ack_thread_cnt; ++i) {
        threads.emplace_back(std::thread(AckWarpper, &gtxn_, t1, "r" + std::to_string(i), "", ""));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(ack_thread_cnt == gtxn_.acks_.size());
    EXPECT_TRUE(gtxn_.acks_cnt_.Get() == ack_thread_cnt);

    for (int i = 0; i < ack_thread_cnt; ++i) {
        GlobalTxn::TableWithRowkey twr1("t1", "r" + std::to_string(i));
        EXPECT_TRUE(gtxn_.acks_[twr1].size() == 1);
    }
}

TEST_F(GlobalTxnTest, DoCommitSecondariesCallback0) {
    // mutation error is kOK will finish
    std::vector<std::thread> threads;
    size_t secondaries_thread_cnt = 10;
    gtxn_.all_task_pushed_ = true;
    gtxn_.status_.SetFailed(ErrorCode::kOK);
    gtxn_.acks_cnt_.Set(0);
    gtxn_.ack_done_cnt_.Set(0);
    gtxn_.notifies_cnt_.Set(0);
    gtxn_.notify_done_cnt_.Set(0);
    gtxn_.writes_cnt_.Set(secondaries_thread_cnt);
    for (int i = 0; i < secondaries_thread_cnt; ++i) {
        RowMutationImpl* mu_impl = new RowMutationImpl(NULL, "rowkey");
        mu_impl->error_code_.SetFailed(ErrorCode::kOK, "");
        RowMutation* mu = static_cast<RowMutation*>(mu_impl);
        auto func = std::bind(&GlobalTxn::DoCommitSecondariesCallback, &gtxn_, mu);
        threads.emplace_back(std::thread(func));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(gtxn_.finish_ == true);
}

TEST_F(GlobalTxnTest, DoCommitSecondariesCallback1) {
    // mutation error is kOK not last one
    size_t secondaries_thread_cnt = 50;
    std::vector<std::thread> threads;
    threads.reserve(secondaries_thread_cnt);
    gtxn_.status_.SetFailed(ErrorCode::kOK);
    gtxn_.acks_cnt_.Set(0);
    gtxn_.ack_done_cnt_.Set(0);
    gtxn_.notifies_cnt_.Set(0);
    gtxn_.notify_done_cnt_.Set(0);
    gtxn_.writes_cnt_.Set(secondaries_thread_cnt + 1);
    for (int i = 0; i < secondaries_thread_cnt; ++i) {
        RowMutationImpl* mu_impl = new RowMutationImpl(NULL, "rowkey");
        mu_impl->error_code_.SetFailed(ErrorCode::kOK, "");
        RowMutation* mu = static_cast<RowMutation*>(mu_impl);
        auto func = std::bind(&GlobalTxn::DoCommitSecondariesCallback, &gtxn_, mu);
        threads.emplace_back(std::thread(func));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(gtxn_.finish_ == false);
}

TEST_F(GlobalTxnTest, DoCommitSecondariesCallback2) {
    // mutation error is not kOK but status_ is not changed
    size_t secondaries_thread_cnt = 10;
    std::vector<std::thread> threads;
    threads.reserve(secondaries_thread_cnt);
    gtxn_.all_task_pushed_ = true;
    gtxn_.status_.SetFailed(ErrorCode::kOK);
    gtxn_.acks_cnt_.Set(0);
    gtxn_.ack_done_cnt_.Set(0);
    gtxn_.notifies_cnt_.Set(0);
    gtxn_.notify_done_cnt_.Set(0);
    gtxn_.writes_cnt_.Set(secondaries_thread_cnt);
    for (int i = 0; i < secondaries_thread_cnt; ++i) {
        RowMutationImpl* mu_impl = new RowMutationImpl(NULL, "rowkey");
        mu_impl->error_code_.SetFailed(ErrorCode::kSystem, "");
        RowMutation* mu = static_cast<RowMutation*>(mu_impl);
        auto func = std::bind(&GlobalTxn::DoCommitSecondariesCallback, &gtxn_, mu);
        threads.emplace_back(std::thread(func));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kOK);
    EXPECT_TRUE(gtxn_.finish_ == true);
}

TEST_F(GlobalTxnTest, DoVerifyPrimaryLockedCallback3) {
    // mutation error is not kOK but status_ is not changed
    size_t secondaries_thread_cnt = 30;
    std::vector<std::thread> threads;

    threads.reserve(secondaries_thread_cnt);
    gtxn_.status_.SetFailed(ErrorCode::kOK);
    gtxn_.acks_cnt_.Set(10);
    gtxn_.ack_done_cnt_.Set(9);
    gtxn_.notifies_cnt_.Set(10);
    gtxn_.notify_done_cnt_.Set(10);
    gtxn_.writes_cnt_.Set(secondaries_thread_cnt);
    for (int i = 0; i < secondaries_thread_cnt; ++i) {
        RowMutationImpl* mu_impl = new RowMutationImpl(NULL, "rowkey");
        mu_impl->error_code_.SetFailed(ErrorCode::kOK, "");
        RowMutation* mu = static_cast<RowMutation*>(mu_impl);
        auto func = std::bind(&GlobalTxn::DoCommitSecondariesCallback, &gtxn_, mu);
        threads.emplace_back(std::thread(func));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }
    threads.clear();
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kOK);
    EXPECT_TRUE(gtxn_.finish_ == false);

}

std::atomic<int> g_callback_run_cnt(0);

static void EmptyMutationCallback(RowMutation* mu) {
    LOG(INFO) << "run empty mutation callback";
    ++g_callback_run_cnt;
} 

// has_commited == true && status_returned_ == false && set mutation callback
TEST_F(GlobalTxnTest, ApplyMutation0) {
    g_callback_run_cnt = 0;
    gtxn_.has_commited_ = true;
    gtxn_.status_returned_ = false;
    
    RowMutationImpl* mu_impl = new RowMutationImpl(NULL, "rowkey");
    RowMutation* mu = static_cast<RowMutation*>(mu_impl);
    mu->SetCallBack(EmptyMutationCallback);
    gtxn_.ApplyMutation(mu);
    thread_pool_.Stop(true);
    EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kGTxnOpAfterCommit);
    EXPECT_TRUE(gtxn_.status_returned_ == true);
    EXPECT_TRUE(gtxn_.put_fail_cnt_.Get() == 0);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnOpAfterCommit);
    EXPECT_TRUE(g_callback_run_cnt == 1);
}

// has_commited == true && status_returned_ == false && don't set mutation callback
TEST_F(GlobalTxnTest, ApplyMutation1) {
    g_callback_run_cnt = 0;
    gtxn_.has_commited_ = true;
    gtxn_.status_returned_ = false;
    
    RowMutationImpl* mu_impl = new RowMutationImpl(NULL, "rowkey");
    RowMutation* mu = static_cast<RowMutation*>(mu_impl);
    gtxn_.ApplyMutation(mu);
    thread_pool_.Stop(true);
    EXPECT_TRUE(mu->GetError().GetType() == ErrorCode::kGTxnOpAfterCommit);
    EXPECT_TRUE(gtxn_.status_returned_ == true);
    EXPECT_TRUE(gtxn_.put_fail_cnt_.Get() == 0);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnOpAfterCommit);
    EXPECT_TRUE(g_callback_run_cnt == 0);
}

TEST_F(GlobalTxnTest, SetReaderStatusAndRunCallback0) {
    RowReaderImpl* reader_impl = new RowReaderImpl(NULL, "rowkey");
    ErrorCode status;
    status.SetFailed(ErrorCode::kSystem, "");
    gtxn_.SetReaderStatusAndRunCallback(reader_impl,&status);
    RowReader* r = static_cast<RowReader*>(reader_impl);
    thread_pool_.Stop(true);
    EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kSystem);
    EXPECT_TRUE(r->IsFinished());
    delete r;
}

TEST_F(GlobalTxnTest, SetReaderStatusAndRunCallback1) {
    RowReaderImpl* reader_impl = new RowReaderImpl(NULL, "rowkey");
    reader_impl->SetCallBack([](RowReader* r) {
        EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kSystem);
        delete r;        
    });
    ErrorCode status;
    status.SetFailed(ErrorCode::kSystem, "");
    gtxn_.SetReaderStatusAndRunCallback(reader_impl,&status);
    thread_pool_.Stop(true);
}

TEST_F(GlobalTxnTest, Get0) {
    gtxn_.has_commited_ = true;
    RowReaderImpl* reader_impl = new RowReaderImpl(NULL, "rowkey");
    RowReader* r = static_cast<RowReader*>(reader_impl);
    EXPECT_TRUE(gtxn_.Get(r).GetType() == ErrorCode::kGTxnOpAfterCommit);
    thread_pool_.Stop(true);
    EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kGTxnOpAfterCommit);
    EXPECT_TRUE(r->IsFinished());
    delete r;
}

TEST_F(GlobalTxnTest, Get1) {
    // set a table to tables_
    ErrorCode status;
    Table* t1 = OpenTable("t1");
    // table<txn=true> and exist cf<gtxn=true>
    TableDescriptor desc("t1");
    desc.EnableTxn(); 
    desc.AddLocalityGroup("lg0");
    ColumnFamilyDescriptor* cfd1 = desc.AddColumnFamily("cf1");
    cfd1->EnableGlobalTransaction();

    TableSchema schema;
    TableDescToSchema(desc, &schema);
    SetSchema(t1, schema);
  
    EXPECT_TRUE(gtxn_.gtxn_internal_->CheckTable(t1, &status));

    RowReader* r = t1->NewRowReader("r1");
    bool ret = gtxn_.gtxn_internal_->VerifyUserRowReader(r);
    EXPECT_FALSE(ret);

    gtxn_.has_commited_ = false;
    EXPECT_TRUE(gtxn_.Get(r).GetType() == ErrorCode::kBadParam);
    thread_pool_.Stop(true);
    EXPECT_TRUE(r->GetError().GetType() == ErrorCode::kBadParam);
    EXPECT_TRUE(r->IsFinished());
    delete r;
    delete t1;
}

TEST_F(GlobalTxnTest, DoGetCellReaderCallback0) {
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);
    InternalReaderContext* ctx = new InternalReaderContext(2, r_impl, &gtxn_); 
    r->SetContext(ctx);
    std::vector<Cell*> cells;
    cells.push_back(new Cell(t1, "r1", "cf1", "qu"));
    cells.push_back(new Cell(t1, "r1", "cf2", "qu"));
    for(auto& cell : cells) {
        ctx->cell_map[cell] = 0;
    }
    RowReader* inter_r = t1->NewRowReader("r1");
    inter_r->SetContext(new CellReaderContext(cells[0], ctx));
    RowReaderImpl* inter_r_impl = static_cast<RowReaderImpl*>(inter_r);
    inter_r_impl->error_code_.SetFailed(ErrorCode::kNotFound, "");
    gtxn_.DoGetCellReaderCallback(inter_r);
    EXPECT_TRUE(ctx->not_found_cnt == 1);
    EXPECT_TRUE(ctx->fail_cell_cnt == 0);
    EXPECT_TRUE(ctx->active_cell_cnt == 1);
    thread_pool_.Stop(true);
    EXPECT_FALSE(r_impl->IsFinished());
}

TEST_F(GlobalTxnTest, DoGetCellReaderCallback1) {
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);
    InternalReaderContext* ctx = new InternalReaderContext(2, r_impl, &gtxn_); 
    r->SetContext(ctx);
    std::vector<Cell*> cells;
    cells.push_back(new Cell(t1, "r1", "cf1", "qu"));
    cells.push_back(new Cell(t1, "r1", "cf2", "qu"));
    for(auto& cell : cells) {
        ctx->cell_map[cell] = 0;
    }
    RowReader* inter_r = t1->NewRowReader("r1");
    inter_r->SetContext(new CellReaderContext(cells[0], ctx));
    RowReaderImpl* inter_r_impl = static_cast<RowReaderImpl*>(inter_r);
    inter_r_impl->error_code_.SetFailed(ErrorCode::kOK, "");
    gtxn_.DoGetCellReaderCallback(inter_r);
    EXPECT_TRUE(ctx->fail_cell_cnt == 0);
    EXPECT_TRUE(ctx->not_found_cnt == 1);
    EXPECT_TRUE(ctx->active_cell_cnt == 1);
    thread_pool_.Stop(true);
    EXPECT_FALSE(r_impl->IsFinished());
}

TEST_F(GlobalTxnTest, DoGetCellReaderCallback2) {
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);
    InternalReaderContext* ctx = new InternalReaderContext(2, r_impl, &gtxn_); 
    r->SetContext(ctx);
    std::vector<Cell*> cells;
    cells.push_back(new Cell(t1, "r1", "cf1", "qu"));
    cells.push_back(new Cell(t1, "r1", "cf2", "qu"));
    for(auto& cell : cells) {
        ctx->cell_map[cell] = 0;
    }
    RowReader* inter_r = t1->NewRowReader("r1");
    inter_r->SetContext(new CellReaderContext(cells[0], ctx));
    RowReaderImpl* inter_r_impl = static_cast<RowReaderImpl*>(inter_r);
    inter_r_impl->error_code_.SetFailed(ErrorCode::kSystem, "");
    gtxn_.DoGetCellReaderCallback(inter_r);
    EXPECT_TRUE(ctx->fail_cell_cnt == 1);
    EXPECT_TRUE(ctx->not_found_cnt == 0);
    EXPECT_TRUE(ctx->active_cell_cnt == 1);
    thread_pool_.Stop(true);
    EXPECT_FALSE(r_impl->IsFinished());
}

TEST_F(GlobalTxnTest, DoGetCellReaderCallback3) {
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);
    InternalReaderContext* ctx = new InternalReaderContext(1, r_impl, &gtxn_); 
    r->SetContext(ctx);
    std::vector<Cell*> cells;
    cells.push_back(new Cell(t1, "r1", "cf1", "qu"));
    for(auto& cell : cells) {
        ctx->cell_map[cell] = 0;
    }
    RowReader* inter_r = t1->NewRowReader("r1");
    inter_r->SetContext(new CellReaderContext(cells[0], ctx));
    RowReaderImpl* inter_r_impl = static_cast<RowReaderImpl*>(inter_r);
    inter_r_impl->error_code_.SetFailed(ErrorCode::kSystem, "");
    gtxn_.DoGetCellReaderCallback(inter_r);
    thread_pool_.Stop(true);
    EXPECT_TRUE(r_impl->IsFinished());
}

TEST_F(GlobalTxnTest, MergeCellToRow) {
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);
    InternalReaderContext* ctx = new InternalReaderContext(1, r_impl, &gtxn_); 
    r->SetContext(ctx);
    std::vector<Cell*> cells;
    cells.push_back(new Cell(t1, "r1", "cf1", "qu"));
    for(auto& cell : cells) {
        ctx->cell_map[cell] = 0;
    }
    RowReader* inter_r = t1->NewRowReader("r1");
    inter_r->SetContext(new CellReaderContext(cells[0], ctx));
    ErrorCode status;
    status.SetFailed(ErrorCode::kSystem, "");
    gtxn_.MergeCellToRow(inter_r, status);
    thread_pool_.Stop(true);
    EXPECT_TRUE(r_impl->IsFinished());
}

TEST_F(GlobalTxnTest, GetCellCallback) {
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);
    InternalReaderContext* ctx = new InternalReaderContext(1, r_impl, &gtxn_); 
    r->SetContext(ctx);
    std::vector<Cell*> cells;
    cells.push_back(new Cell(t1, "r1", "cf1", "qu"));
    for(auto& cell : cells) {
        ctx->cell_map[cell] = 0;
    }
    RowReader* inter_r = t1->NewRowReader("r1");
    inter_r->SetContext(new CellReaderContext(cells[0], ctx));
    RowReaderImpl* inter_r_impl = static_cast<RowReaderImpl*>(inter_r);
    inter_r_impl->error_code_.SetFailed(ErrorCode::kSystem, "");
    gtxn_.GetCellCallback((CellReaderContext*)inter_r->GetContext());
    thread_pool_.Stop(true);
    EXPECT_TRUE(r_impl->IsFinished());
}

TEST_F(GlobalTxnTest, RollForward) {
    // can't find primary write cell 
    Table* t1 = OpenTable("t1");
    Cell cell(t1, "r1", "cf1", "qu");
    tera::PrimaryInfo primary;
    primary.set_table_name("t1");
    primary.set_row_key("r1");
    primary.set_column_family("cf1");
    primary.set_qualifier("qu");
    primary.set_gtxn_start_ts(12);
    ErrorCode status;

    std::set<std::string> gtxn_cfs;
    gtxn_.gtxn_internal_->tables_["t1"] =
        std::pair<Table*, std::set<std::string>>(t1, gtxn_cfs);
    ErrorCode mock_status;
    mock_status.SetFailed(ErrorCode::kNotFound,"");
    std::vector<ErrorCode> reader_errs;
    reader_errs.push_back(mock_status);
    (static_cast<MockTable*>(t1))->AddReaderErrors(reader_errs);
    gtxn_.RollForward(cell, primary, 0, &status);
    EXPECT_TRUE(ErrorCode::kGTxnPrimaryLost == status.GetType());
}

TEST_F(GlobalTxnTest, CleanLock0) {
    // cell same as primary
    Table* t1 = OpenTable("t1");
    Cell cell(t1, "r1", "cf1", "qu");
    tera::PrimaryInfo primary;
    primary.set_table_name("t1");
    primary.set_row_key("r1");
    primary.set_column_family("cf1");
    primary.set_qualifier("qu");
    primary.set_gtxn_start_ts(12);
    // init status is OK
    ErrorCode status;
    status.SetFailed(ErrorCode::kOK);
    std::set<std::string> gtxn_cfs;
    gtxn_.gtxn_internal_->tables_["t1"] = 
        std::pair<Table*, std::set<std::string>>(t1, gtxn_cfs);
    // only this cell will call mutation 
    ErrorCode mock_status1;
    mock_status1.SetFailed(ErrorCode::kSystem,"");
    std::vector<ErrorCode> mu_errs;
    mu_errs.push_back(mock_status1);
    (static_cast<MockTable*>(t1))->AddMutationErrors(mu_errs);
    // run test
    gtxn_.CleanLock(cell, primary, &status);
    EXPECT_TRUE(mock_status1.GetType() == status.GetType());
}

TEST_F(GlobalTxnTest, CleanLock1) {
    // cell diff with primary
    Table* t1 = OpenTable("t1");
    Cell cell(t1, "r1", "cf1", "qu");
    tera::PrimaryInfo primary;
    primary.set_table_name("t1");
    primary.set_row_key("r2"); // diff row
    primary.set_column_family("cf1");
    primary.set_qualifier("qu");
    primary.set_gtxn_start_ts(12);
    // init status is OK
    ErrorCode status;
    status.SetFailed(ErrorCode::kOK);
    std::set<std::string> gtxn_cfs;
    gtxn_.gtxn_internal_->tables_["t1"] = 
        std::pair<Table*, std::set<std::string>>(t1, gtxn_cfs);
    // mock primary return kSystem but cell kOK
    // will get kSystem
    ErrorCode mock_status1;
    ErrorCode mock_status2;
    mock_status1.SetFailed(ErrorCode::kSystem,"");
    mock_status2.SetFailed(ErrorCode::kOK,"");
    std::vector<ErrorCode> mu_errs;
    mu_errs.push_back(mock_status1);
    mu_errs.push_back(mock_status2);
    (static_cast<MockTable*>(t1))->AddMutationErrors(mu_errs);
    // run test
    gtxn_.CleanLock(cell, primary, &status);
    EXPECT_TRUE(mock_status1.GetType() == status.GetType());
    EXPECT_TRUE(mock_status2.GetType() != status.GetType());
}

TEST_F(GlobalTxnTest, CleanLock2) {
    // cell diff with primary
    Table* t1 = OpenTable("t1");
    Cell cell(t1, "r1", "cf1", "qu");
    tera::PrimaryInfo primary;
    primary.set_table_name("t1");
    primary.set_row_key("r2"); // diff row
    primary.set_column_family("cf1");
    primary.set_qualifier("qu");
    primary.set_gtxn_start_ts(12);
    // init status is OK
    ErrorCode status;
    status.SetFailed(ErrorCode::kOK);
    std::set<std::string> gtxn_cfs;
    gtxn_.gtxn_internal_->tables_["t1"] = 
        std::pair<Table*, std::set<std::string>>(t1, gtxn_cfs);
    // mock primary return kOk but cell kSystem
    // will get kSystem
    ErrorCode mock_status1;
    ErrorCode mock_status2;
    mock_status1.SetFailed(ErrorCode::kOK,"");
    mock_status2.SetFailed(ErrorCode::kSystem,"");
    std::vector<ErrorCode> mu_errs;
    mu_errs.push_back(mock_status1);
    mu_errs.push_back(mock_status2);
    (static_cast<MockTable*>(t1))->AddMutationErrors(mu_errs);
    // run test
    gtxn_.CleanLock(cell, primary, &status);
    EXPECT_TRUE(mock_status1.GetType() != status.GetType());
    EXPECT_TRUE(mock_status2.GetType() == status.GetType());
}

TEST_F(GlobalTxnTest, CleanLock3) {
    // cell diff with primary
    Table* t1 = OpenTable("t1");
    Cell cell(t1, "r1", "cf1", "qu");
    tera::PrimaryInfo primary;
    primary.set_table_name("t1");
    primary.set_row_key("r2"); // diff row
    primary.set_column_family("cf1");
    primary.set_qualifier("qu");
    primary.set_gtxn_start_ts(12);
    // init status is OK
    ErrorCode status;
    status.SetFailed(ErrorCode::kOK);
    std::set<std::string> gtxn_cfs;
    gtxn_.gtxn_internal_->tables_["t1"] = 
        std::pair<Table*, std::set<std::string>>(t1, gtxn_cfs);
    // mock primary return kTimeout but cell kSystem
    // will get kSystem, the latest error will return
    ErrorCode mock_status1;
    ErrorCode mock_status2;
    mock_status1.SetFailed(ErrorCode::kTimeout,"");
    mock_status2.SetFailed(ErrorCode::kSystem,"");
    std::vector<ErrorCode> mu_errs;
    mu_errs.push_back(mock_status1);
    mu_errs.push_back(mock_status2);
    (static_cast<MockTable*>(t1))->AddMutationErrors(mu_errs);
    // run test
    gtxn_.CleanLock(cell, primary, &status);
    EXPECT_TRUE(mock_status1.GetType() != status.GetType());
    EXPECT_TRUE(mock_status2.GetType() == status.GetType());
}

void AddKeyValueToResult(const std::string& key, const std::string& cf,
        const std::string& qu, int64_t timestamp,
        const std::string& value, RowResult* result) {
    KeyValuePair* kv = result->add_key_values();
    kv->set_key(key);
    kv->set_column_family(cf);
    kv->set_qualifier(qu);
    kv->set_timestamp(timestamp);
    kv->set_value(value);
}

TEST_F(GlobalTxnTest, EncodeWriteValue) {
    std::string ret = EncodeWriteValue(1, 100);
    int type;
    int64_t ts;
    DecodeWriteValue(ret, &type, &ts);

    EXPECT_TRUE(type == 1);
    EXPECT_TRUE(ts == 100);
}

TEST_F(GlobalTxnTest, DecodeWriteValue) {
    // a int bigger than mutaion type
    std::string ret = EncodeWriteValue(99, 1000000);
    int type;
    int64_t ts;
    DecodeWriteValue(ret, &type, &ts);

    EXPECT_TRUE(type == 99);
    EXPECT_TRUE(ts == 1000000);
}

TEST_F(GlobalTxnTest, FindValueFromResultRow0) {
    // the success case
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);

    // build RowReader::TRow
    // cf must exist before call FindValueFromResultRow
    RowResult result;
    gtxn_.start_ts_ = 14;
    AddKeyValueToResult("r1", "cf1", "qu1", 9, "v1", &result);
    AddKeyValueToResult("r1", "cf1", "qu1", 13, "v2", &result);
    
    AddKeyValueToResult("r1", "cf1", "qu1_W_", 15, EncodeWriteValue(0, 13), &result);
    AddKeyValueToResult("r1", "cf1", "qu1_W_", 12, EncodeWriteValue(0, 9), &result);
    r_impl->SetResult(result);
    RowReader::TRow row;
    r->ToMap(&row);

    for (auto& cf : row) {
        std::cout << cf.first << "\n";
        for (auto& qu : cf.second) {
            std::cout << "\t" << qu.first << "\n";
            for (auto& v : qu.second) {
                std::cout << "\t\tts=" << v.first << ",v=" << v.second << "\n";
            }
        }
    }

    // build target_cell
    Cell target_cell(t1, "r1", "cf1", "qu1");
    
    // run test
    EXPECT_TRUE(gtxn_.FindValueFromResultRow(row, &target_cell));
    EXPECT_TRUE(target_cell.Timestamp() == 9);
    EXPECT_TRUE(target_cell.Value() == "v1");

    delete t1;
    delete r;
}

TEST_F(GlobalTxnTest, FindValueFromResultRow1) {
    // the not found
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);

    // build RowReader::TRow
    // cf must exist before call FindValueFromResultRow
    RowResult result;
    r_impl->SetResult(result);
    gtxn_.start_ts_ = 11;
    RowReader::TRow row;
    r->ToMap(&row);

    // build target_cell
    Cell target_cell(t1, "r1", "cf1", "qu1");
    
    // run test
    EXPECT_FALSE(gtxn_.FindValueFromResultRow(row, &target_cell));

    delete t1;
    delete r;
}

TEST_F(GlobalTxnTest, FindValueFromResultRow2) {
    // the not found write col
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);

    // build RowReader::TRow
    // cf must exist before call FindValueFromResultRow
    RowResult result;
    gtxn_.start_ts_ = 11;

    AddKeyValueToResult("r1", "cf1", "qu1", 9, "v1", &result);
    AddKeyValueToResult("r1", "cf1", "qu1", 13, "v2", &result);
    r_impl->SetResult(result);
    
    RowReader::TRow row;
    r->ToMap(&row);

    // build target_cell
    Cell target_cell(t1, "r1", "cf1", "qu1");
    
    // run test
    EXPECT_FALSE(gtxn_.FindValueFromResultRow(row, &target_cell));

    delete t1;
    delete r;
}

TEST_F(GlobalTxnTest, FindValueFromResultRow3) {
    // the not found rigth version
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);

    // build RowReader::TRow
    // cf must exist before call FindValueFromResultRow
    RowResult result;
    gtxn_.start_ts_ = 11;

    AddKeyValueToResult("r1", "cf1", "qu1", 9, "v1", &result);
    AddKeyValueToResult("r1", "cf1", "qu1", 13, "v2", &result);
    
    AddKeyValueToResult("r1", "cf1", "qu1_W_", 15, EncodeWriteValue(0, 13), &result);
    // make ts = 9 v1 is deleted before this function called
    AddKeyValueToResult("r1", "cf1", "qu1_W_", 12, EncodeWriteValue(1, 9), &result);
    r_impl->SetResult(result);
    RowReader::TRow row;
    r->ToMap(&row);

    // build target_cell
    Cell target_cell(t1, "r1", "cf1", "qu1");
    // run test
    EXPECT_FALSE(gtxn_.FindValueFromResultRow(row, &target_cell));

    delete t1;
    delete r;
}

TEST_F(GlobalTxnTest, FindValueFromResultRow4) {
    // the not found rigth version
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);

    // build RowReader::TRow
    // cf must exist before call FindValueFromResultRow
    RowResult result;
    gtxn_.start_ts_ = 11;

    AddKeyValueToResult("r1", "cf1", "qu1", 9, "v1", &result);
    AddKeyValueToResult("r1", "cf1", "qu1", 13, "v2", &result);
    
    // maybe other older version clean by gc, before this function called
    AddKeyValueToResult("r1", "cf1", "qu1_W_", 15, EncodeWriteValue(0, 13), &result);
    r_impl->SetResult(result);
    RowReader::TRow row;
    r->ToMap(&row);

    // build target_cell
    Cell target_cell(t1, "r1", "cf1", "qu1");
    // run test
    EXPECT_FALSE(gtxn_.FindValueFromResultRow(row, &target_cell));

    delete t1;
    delete r;
}

TEST_F(GlobalTxnTest, FindValueFromResultRow5) {
    // the not found rigth version
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);

    // build RowReader::TRow
    // cf must exist before call FindValueFromResultRow
    RowResult result;
    gtxn_.start_ts_ = 11;

    // maybe version 1 was clean by gc, before this function called
    AddKeyValueToResult("r1", "cf1", "qu1", 13, "v2", &result);
    
    AddKeyValueToResult("r1", "cf1", "qu1_W_", 15, EncodeWriteValue(0, 13), &result);
    AddKeyValueToResult("r1", "cf1", "qu1_W_", 12, EncodeWriteValue(0, 9), &result);
    r_impl->SetResult(result);
    RowReader::TRow row;
    r->ToMap(&row);

    // build target_cell
    Cell target_cell(t1, "r1", "cf1", "qu1");
    // run test
    EXPECT_FALSE(gtxn_.FindValueFromResultRow(row, &target_cell));

    delete t1;
    delete r;
}

TEST_F(GlobalTxnTest, SetLastStatus) {
    ErrorCode status;
    status.SetFailed(ErrorCode::kOK, "");
    gtxn_.status_returned_ = false;
    gtxn_.SetLastStatus(&status);
    EXPECT_TRUE(gtxn_.status_returned_);
    EXPECT_TRUE(gtxn_.status_.GetType() == status.GetType());

    status.SetFailed(ErrorCode::kTimeout, "");
    gtxn_.status_returned_ = true;
    gtxn_.SetLastStatus(&status);
    EXPECT_TRUE(gtxn_.status_returned_);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kOK);
}

static bool g_callback_run_flag = false;

TEST_F(GlobalTxnTest, RunUserCallback0) {
    g_callback_run_flag = false;
    gtxn_.SetCommitCallback([](Transaction* t) {g_callback_run_flag = true;});
    gtxn_.RunUserCallback();
    EXPECT_TRUE(g_callback_run_flag);
}

static void WaitWapper(GlobalTxn* gtxn) {
    gtxn->WaitForComplete();
    g_callback_run_flag = true;
}

TEST_F(GlobalTxnTest, RunUserCallback1) {
    g_callback_run_flag = false;
    thread_pool_.AddTask(std::bind(&WaitWapper, &gtxn_));
    gtxn_.RunUserCallback();
    EXPECT_TRUE(gtxn_.finish_);
    thread_pool_.Stop(true);
    EXPECT_TRUE(g_callback_run_flag);
}

TEST_F(GlobalTxnTest, BackoffAndMaybeCleanupLock0) {
    bool try_clean = false;
    ErrorCode status;
    // make sure have lock_ts < start_ts
    // can't found primary
    Table* t1 = OpenTable("t1");
    RowReader* r = t1->NewRowReader("r1");
    RowReaderImpl* r_impl = static_cast<RowReaderImpl*>(r);

    // build RowReader::TRow
    // cf must exist before call FindValueFromResultRow
    RowResult result;
    gtxn_.start_ts_ = 11;

    // start_ts > lock ts and primary info is bad for parse
    AddKeyValueToResult("r1", "cf1", "qu1_L_", 9, "primary info", &result);
    r_impl->SetResult(result);
    RowReader::TRow row;
    r->ToMap(&row);

    // build target_cell
    Cell target_cell(t1, "r1", "cf1", "qu1");
    // run test
    gtxn_.BackoffAndMaybeCleanupLock(row, target_cell, try_clean, &status);
    EXPECT_TRUE(status.GetType() == ErrorCode::kGTxnPrimaryLost);
    delete t1;
    delete r;
}

TEST_F(GlobalTxnTest, RunAfterPrewriteFailed0) {
    Table* t = OpenTable("t1");
    Cell cell(t, "r1", "cf", "qu", 1, "val");
    Write w(cell);
    std::vector<Write> ws;
    ws.push_back(w);
    PrewriteContext* ctx = new PrewriteContext(&ws, &gtxn_, w.TableName(), w.RowKey());
    ctx->status.SetFailed(ErrorCode::kOK, "");
    gtxn_.RunAfterPrewriteFailed(ctx);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kOK);
}

TEST_F(GlobalTxnTest, RunAfterPrewriteFailed1) {
    Table* t = OpenTable("t1");
    Cell cell(t, "r1", "cf", "qu", 1, "val");
    Write w(cell);
    std::vector<Write> ws;
    ws.push_back(w);
    PrewriteContext* ctx = new PrewriteContext(&ws, &gtxn_, w.TableName(), w.RowKey());
    ctx->status.SetFailed(ErrorCode::kTimeout, "");
    gtxn_.RunAfterPrewriteFailed(ctx);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnPrewriteTimeout);
}

TEST_F(GlobalTxnTest, RunAfterPrewriteFailed2) {
    Table* t = OpenTable("t1");
    Cell cell(t, "r1", "cf", "qu", 1, "val");
    Write w(cell);
    std::vector<Write> ws;
    ws.push_back(w);
    PrewriteContext* ctx = new PrewriteContext(&ws, &gtxn_, w.TableName(), w.RowKey());
    gtxn_.gtxn_internal_->is_timeout_ = true;
    gtxn_.RunAfterPrewriteFailed(ctx);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnPrewriteTimeout);
    delete t;
}

TEST_F(GlobalTxnTest, DoPrewriteCallback0) {
    // case a. global timeout
    Table* t = OpenTable("t1");
    Transaction* txn = t->StartRowTransaction("r1");
    SingleRowTxn* stxn = static_cast<SingleRowTxn*>(txn);
    Cell cell(t, "r1", "cf", "qu", 1, "val");
    Write w(cell);
    std::vector<Write> ws;
    ws.push_back(w);
    PrewriteContext* ctx = new PrewriteContext(&ws, &gtxn_, w.TableName(), w.RowKey());
    stxn->SetContext(ctx);
    gtxn_.gtxn_internal_->is_timeout_ = true;
    gtxn_.DoPrewriteCallback(stxn);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnPrewriteTimeout);
    delete t;
}

TEST_F(GlobalTxnTest, DoPrewriteCallback1) {
    // case b. this operator timeout
    Table* t = OpenTable("t1");
    Transaction* txn = t->StartRowTransaction("r1");
    SingleRowTxn* stxn = static_cast<SingleRowTxn*>(txn);
    Cell cell(t, "r1", "cf", "qu", 1, "val");
    Write w(cell);
    std::vector<Write> ws;
    ws.push_back(w);
    PrewriteContext* ctx = new PrewriteContext(&ws, &gtxn_, w.TableName(), w.RowKey());
    stxn->SetContext(ctx);
    stxn->mutation_buffer_.SetError(ErrorCode::kTimeout,"");
    gtxn_.gtxn_internal_->is_timeout_ = false;
    gtxn_.DoPrewriteCallback(stxn);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnPrewriteTimeout);
    delete t;
}

TEST_F(GlobalTxnTest, DoPrewriteCallback2) {
    // case b. this operator error
    Table* t = OpenTable("t1");
    Transaction* txn = t->StartRowTransaction("r1");
    SingleRowTxn* stxn = static_cast<SingleRowTxn*>(txn);
    Cell cell(t, "r1", "cf", "qu", 1, "val");
    Write w(cell);
    std::vector<Write> ws;
    ws.push_back(w);
    PrewriteContext* ctx = new PrewriteContext(&ws, &gtxn_, w.TableName(), w.RowKey());
    stxn->SetContext(ctx);
    stxn->mutation_buffer_.SetError(ErrorCode::kSystem,"");
    gtxn_.gtxn_internal_->is_timeout_ = false;
    gtxn_.DoPrewriteCallback(stxn);
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kSystem);
    delete t;
}

TEST_F(GlobalTxnTest, VerifyPrimaryLocked) {
    Table* t = OpenTable("t1");
    Cell cell(t, "r1", "cf", "qu", 1, "val");
    Write w(cell);
    gtxn_.primary_write_ = &w;

    ErrorCode mock_status;
    mock_status.SetFailed(ErrorCode::kNotFound,"");
    std::vector<ErrorCode> reader_errs;
    reader_errs.push_back(mock_status);
    (static_cast<MockTable*>(t))->AddReaderErrors(reader_errs);

    gtxn_.VerifyPrimaryLocked();
    EXPECT_TRUE(gtxn_.status_.GetType() == ErrorCode::kGTxnPrimaryLost);
}


} // namespace tera
