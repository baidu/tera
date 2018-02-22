// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/observer_demo/demo_observer.h"

#include <glog/logging.h>

namespace tera {
namespace observer {

ErrorCode DemoObserver::OnNotify(tera::Transaction* t,
		                         tera::Client* client,
		                         const std::string& table_name,
		                         const std::string& family,
		                         const std::string& qualifier,
		                         const std::string& row,
		                         const std::string& value,
		                         int64_t timestamp,
                                 Notification* notification) {
    VLOG(12) <<"[time] OnNotify start. [row] " << row;
    LOG(INFO) << "[Notify DemoObserver] table:family:qualifer=" <<
        table_name << ":" << family << ":" <<
        qualifier << " row=" << row <<
        " value=" << value << " timestamp=" << timestamp;

    tera::ErrorCode err;
    tera::Table* table = client->OpenTable(table_name, &err);

    // write ForwordIndex column
    tera::RowMutation* mutation = table->NewRowMutation(row);
    mutation->Put("Data", "ForwordIndex", "FIValue_" + row);
    t->ApplyMutation(mutation);

    tera::ErrorCode error;
    notification->Ack(table, row, family, qualifier);
    error = t->Commit();
    delete mutation;
    VLOG(12) <<"[time] OnNotify finish. [row] " << row;
    return error;
}

std::string DemoObserver::GetObserverName() const {
	return "DemoObserver";
}

TransactionType DemoObserver::GetTransactionType() const {
    return kGlobalTransaction;
}

ErrorCode ParseObserver::OnNotify(tera::Transaction* t,
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
    tera::Table* table = client->OpenTable(table_name, &err);
    notification->Ack(table, row, family, qualifier);
    err = t->Commit();
    return err;
}

std::string ParseObserver::GetObserverName() const {
    return "ParseObserver";
}

TransactionType ParseObserver::GetTransactionType() const {
    return kGlobalTransaction;
}

ErrorCode SingleRowObserver::OnNotify(tera::Transaction* t,
                                 tera::Client* client,
                                 const std::string& table_name,
                                 const std::string& family,
                                 const std::string& qualifier,
                                 const std::string& row,
                                 const std::string& value,
                                 int64_t timestamp,
                                 Notification* notification) {
    LOG(INFO) << "[Notify SingleRowObserver] table:family:qualifer=" <<
        table_name << ":" << family << ":" <<
        qualifier << " row=" << row <<
        " value=" << value << " timestamp=" << timestamp;

    tera::ErrorCode err;
    tera::Table* table = client->OpenTable(table_name, &err);

    // single row txn
    tera::RowMutation* mutation = table->NewRowMutation(row);
    mutation->Put(family, "another_qu", "value");
    t->ApplyMutation(mutation);

    tera::ErrorCode error;
    notification->Ack(table, row, family, qualifier);
    tera::Table* another_table = client->OpenTable("another_table", &err);
    notification->Ack(another_table, "somerow", "family", "qualifier");
    error = t->Commit();
    delete mutation;
    return error;
}

std::string SingleRowObserver::GetObserverName() const {
    return "SingleRowObserver";
}

TransactionType SingleRowObserver::GetTransactionType() const {
    return kSingleRowTransaction;
}

ErrorCode NoneTransactionObserver::OnNotify(tera::Transaction* t,
                                 tera::Client* client,
                                 const std::string& table_name,
                                 const std::string& family,
                                 const std::string& qualifier,
                                 const std::string& row,
                                 const std::string& value,
                                 int64_t timestamp,
                                 Notification* notification) {
    LOG(INFO) << "[Notify NoneTransactionObserver] table:family:qualifer=" <<
        table_name << ":" << family << ":" <<
        qualifier << " row=" << row <<
        " value=" << value << " timestamp=" << timestamp;

    tera::ErrorCode err;
    tera::Table* table = client->OpenTable(table_name, &err);

    // do something
    // kNoneTransaction notify
    notification->Ack(table, row, family, qualifier);

    // kNoneTransaction ack 
    tera::Table* notify_table = client->OpenTable("notify_table", &err);
    notification->Notify(notify_table, "notify_row", "family", "qualifier");
    return err;
}

std::string NoneTransactionObserver::GetObserverName() const {
    return "NoneTransactionObserver";
}

TransactionType NoneTransactionObserver::GetTransactionType() const {
    return kNoneTransaction;
}

} // namespace observer
} // namespace tera