// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <atomic>
#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/thread_pool.h"
#include "observer/executor/observer.h"
#include "observer/executor/random_key_selector.h"
#include "observer/executor/scanner.h"
#include "observer/executor/scanner_impl.h"
#include "observer/executor/notification_impl.h"
#include "sdk/client_impl.h"
#include "sdk/sdk_utils.h"
#include "tera.h"
#include "types.h"

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

namespace tera {
namespace observer {

class TestWorker : public Observer {
public:
    TestWorker(): counter_(0), notified_(false) {}
    virtual ~TestWorker() {}
    virtual ErrorCode  OnNotify(tera::Transaction* t,
	                            tera::Client* client,
	                            const std::string& table_name,
	                            const std::string& family,
	                            const std::string& qualifier,
	                          	const std::string& row,
	                          	const std::string& value,
	                          	int64_t timestamp,
                                Notification* notification) {
    	LOG(INFO) << "[Notify DemoObserver] table:family:qualifer=" <<
	        table_name << ":" << family << ":" <<
	        qualifier << " row=" << row <<
	        " value=" << value << " timestamp=" << timestamp;

	    table_name_ = table_name;
	  	family_ = family;
	  	qualifier_ = qualifier;
	  	row_ = row;
	  	value_ = value;

		tera::ErrorCode err;
        notified_ = true;    
        ++counter_;

        tera::Table* table = client->OpenTable(table_name, &err);
        notification->Ack(table, row, family, qualifier);

        return err;
    }

    virtual std::string GetObserverName() const {
        return "DemoObserver";
    }

    virtual TransactionType GetTransactionType() const {
        return kGlobalTransaction;
    }
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
    TestWorkerGTX(): counter_(0), notified_(false) {}
    virtual ~TestWorkerGTX() {}
    virtual ErrorCode  OnNotify(tera::Transaction* t,
                                tera::Client* client,
                                const std::string& table_name,
                                const std::string& family,
                                const std::string& qualifier,
                                const std::string& row,
                                const std::string& value,
                                int64_t timestamp,
                                Notification* notification) {
        LOG(INFO) << "[Notify TestWorkerGTX] table:family:qualifer=" <<
            table_name << ":" << family << ":" <<
            qualifier << " row=" << row <<
            " value=" << value << " timestamp=" << timestamp;

        table_name_ = table_name;
        family_ = family;
        qualifier_ = qualifier;
        row_ = row;
        value_ = value;

        tera::ErrorCode err;
        notified_ = true;    
        ++counter_;

        tera::Table* table = client->OpenTable(table_name, &err);

        // write ForwordIndex column
        tera::RowMutation* mutation = table->NewRowMutation(row);
        mutation->Put(family, qualifier + "_test", row + "_");
        t->ApplyMutation(mutation);

        tera::ErrorCode error;
        t->Ack(table, row, family, qualifier);
        table->CommitRowTransaction(t);
        delete mutation;
        return error;

        return err;
    }

    virtual std::string GetObserverName() const {
        return "DemoObserver";
    }

    virtual TransactionType GetTransactionType() const {
        return kSingleRowTransaction;
    }
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
    virtual ErrorCode OnNotify(tera::Transaction* t,
                              tera::Client* client,
                              const std::string& table_name,
                              const std::string& family,
                              const std::string& qualifier,
                              const std::string& row,
                              const std::string& value,
                              int64_t timestamp,
                              Notification* notification) {
	    LOG(INFO) << "[Notify ParseObserver] table:family:qualifer=" <<
	        table_name << ":" << family << ":" <<
	        qualifier << " row=" << row <<
	        " value=" << value << " timestamp=" << timestamp;

	    tera::ErrorCode err;
	    // do nothing
	    return err;
    }
    virtual std::string GetObserverName() const {
    	return "DemoObserver";
    }
    virtual TransactionType GetTransactionType() const {
        return kGlobalTransaction;
    }
};

class TestWorkerNTX : public Observer {
public:
    TestWorkerNTX(): counter_(0), notified_(false) {}
    virtual ~TestWorkerNTX() {}
    virtual ErrorCode  OnNotify(tera::Transaction* t,
                                tera::Client* client,
                                const std::string& table_name,
                                const std::string& family,
                                const std::string& qualifier,
                                const std::string& row,
                                const std::string& value,
                                int64_t timestamp,
                                Notification* notification) {
        LOG(INFO) << "[Notify TestWorkerNTX] table:family:qualifer=" <<
            table_name << ":" << family << ":" <<
            qualifier << " row=" << row <<
            " value=" << value << " timestamp=" << timestamp;

        table_name_ = table_name;
        family_ = family;
        qualifier_ = qualifier;
        row_ = row;
        value_ = value;

        tera::ErrorCode err;
        notified_ = true;    
        ++counter_;

        // do something without transaction

        return err;
    }

    virtual std::string GetObserverName() const {
        return "DemoObserver";
    }

    virtual TransactionType GetTransactionType() const {
        return kNoneTransaction;
    }
private:
    std::atomic<int> counter_;
    std::atomic<bool> notified_;

    std::string table_name_;
    std::string family_;
    std::string qualifier_;
    std::string row_;
    std::string value_;
};

class ObserverImplTest : public ::testing::Test {
public:
    void OnNotifyTest() {
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
        }

        tera::Table* table = client->OpenTable("observer_test_table", &err);
        EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
        if (tera::ErrorCode::kOK != err.GetType()) {
            LOG(ERROR) << "open table failed"; 
            return;
        }

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
	    if(!ret) {
			LOG(ERROR) << "fail to init scanner_impl";
	        return;
		}

        err = scanner->Observe("observer_test_table", "cf", "Page", observer);
        EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

        err = scanner->Observe("observer_test_table", "cf", "Page", demo);
        EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

		if(!scanner->Start()) {
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
        }

        tera::Table* table = client->OpenTable("observer_table_gtx", &err);
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
        if(!ret) {
            LOG(ERROR) << "fail to init scanner_impl";
            return;
        }

        err = scanner->Observe("observer_table_gtx", "cf", "Page", observer);
        EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

        if(!scanner->Start()) {
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
        }

        tera::Table* table = client->OpenTable("observer_table_ntx", &err);
        EXPECT_EQ(tera::ErrorCode::kOK, err.GetType());
        if (tera::ErrorCode::kOK != err.GetType()) {
            LOG(ERROR) << "open table failed"; 
            return;
        }

        table->Put("www.baidu.com", "_N_", "cf:Page", "I am not important", &err);
        table->Put("www.baidu.com", "cf", "Page", "hello world", -1, &err);

        Observer* observer = new TestWorkerNTX();
        
        Scanner* scanner = new ScannerImpl();
        bool ret = scanner->Init();

        EXPECT_EQ(true, ret);
        if(!ret) {
            LOG(ERROR) << "fail to init scanner_impl";
            return;
        }

        err = scanner->Observe("observer_table_ntx", "cf", "Page", observer);
        EXPECT_EQ(err.GetType(), tera::ErrorCode::kOK);

        if(!scanner->Start()) {
            LOG(ERROR) << "fail to start scanner_impl";
            return;
        }

        while (!static_cast<TestWorkerGTX*>(observer)->notified_) {
            sleep(1);
        }

        EXPECT_EQ("www.baidu.com", static_cast<TestWorkerGTX*>(observer)->row_);
        EXPECT_EQ("observer_table_ntx", static_cast<TestWorkerGTX*>(observer)->table_name_);
        EXPECT_EQ("cf", static_cast<TestWorkerGTX*>(observer)->family_);
        EXPECT_EQ("Page", static_cast<TestWorkerGTX*>(observer)->qualifier_);
        EXPECT_EQ("hello world", static_cast<TestWorkerGTX*>(observer)->value_);
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
        Observer* observer =  new DemoObserver();
        scanner->key_selector_.reset(new RandomKeySelector());

        // single thread

        err = scanner->Observe("observer_table", "cf", "qualifier", observer);
        EXPECT_TRUE(err.GetType() != tera::ErrorCode::kOK);

        scanner->tera_client_ = tera::Client::NewClient(FLAGS_flagfile, &err);
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
            thread_pool.AddTask(std::bind(&ScannerImpl::Observe, scanner, "observer_table", "cf", qualifier, observer));
        }
        thread_pool.Stop(true);
        EXPECT_EQ(1, scanner->observers_.size());
        EXPECT_EQ(10 + 2, (*(scanner->table_observe_info_))["observer_table"].observe_columns.size());   
        scanner->Exit();
        delete scanner;
    }
};

TEST_F(ObserverImplTest, OnNotifyTest) {
	FLAGS_tera_gtxn_test_opened = true;
    FLAGS_tera_coord_type = "ins";
    FLAGS_mock_rowlock_enable = true;
    OnNotifyTest();
}

TEST_F(ObserverImplTest, SingleRowTransactionTest) {
    FLAGS_tera_gtxn_test_opened = true;
    FLAGS_tera_coord_type = "ins";
    FLAGS_mock_rowlock_enable = true;
    SingleRowTransactionTest();
}

TEST_F(ObserverImplTest, NoneTransactionTest) {
    FLAGS_tera_gtxn_test_opened = true;
    FLAGS_tera_coord_type = "ins";
    FLAGS_mock_rowlock_enable = true;
    NonTransactionTest();
}

TEST_F(ObserverImplTest, ObserveTest) {
    FLAGS_tera_gtxn_test_opened = true;
    FLAGS_tera_coord_type = "ins";
    FLAGS_mock_rowlock_enable = true;
    ObserveTest();
}

} // namespace observer
} // namespace tera

int main(int argc, char** argv) {
    FLAGS_tera_sdk_client_for_gtxn = true;
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

