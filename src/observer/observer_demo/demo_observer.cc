// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/observer_demo/demo_observer.h"
#include "common/timer.h"
#include <atomic>
#include <memory>
#include <glog/logging.h>

namespace tera {
namespace observer {

void CommitCallBack(tera::Transaction* txn) {
    IntegrationObserver::TxnContext* ctx 
        = (IntegrationObserver::TxnContext*)(txn->GetContext());
    if (txn->GetError().GetType() != tera::ErrorCode::kOK) {
        LOG(ERROR) << txn->GetError().ToString() << " fail_cnt:" << ++(((IntegrationObserver*)(ctx->observer))->fail_cnt_);
    }
    LOG(INFO) <<"[time] OnNotify finish. [row] " << ctx->row << " time:" << get_micros() - ctx->begin_time << " done_cnt:"
             << ++(((IntegrationObserver*)(ctx->observer))->done_cnt_);
    ctx->notification->Done();
    delete ctx;
}

void ReadRowCallBack(tera::RowReader* reader) {
    std::unique_ptr<tera::RowReader> reader_ptr(reader);
    IntegrationObserver::TxnContext* ctx 
        = (IntegrationObserver::TxnContext*)(reader_ptr->GetContext());
    if (reader_ptr->GetError().GetType() != tera::ErrorCode::kOK) {
    	LOG(ERROR) << "row:" << ctx->row << " " << reader_ptr->GetError().ToString() 
                   << " fail_cnt:" << ++(((IntegrationObserver*)(ctx->observer))->fail_cnt_);
        ctx->notification->Done();
        delete ctx;
        return;
    }
    std::string s1;
    std::string s2;
    while (!reader_ptr->Done()) {
        if (reader_ptr->ColumnName() == "Data:qu0") {
            s1 = reader_ptr->Value();
        }
        else if (reader_ptr->ColumnName() == "Data:qu1") {
            s2 = reader_ptr->Value();
        }
        reader_ptr->Next();
    }
    std::unique_ptr<tera::RowMutation> mutation(ctx->output_table->NewRowMutation(ctx->row));
    mutation->Put(ctx->family, "qu3", s1 + s2);
    ctx->txn->ApplyMutation(mutation.get());

    ctx->notification->Ack(ctx->input_table, ctx->row, ctx->family, ctx->qualifier);
    ctx->txn->SetContext(ctx);
    ctx->txn->SetCommitCallback(CommitCallBack);
    ctx->txn->Commit();
    // CommitCallBack(ctx->txn);
}

void IntegrationObserver::OnNotify(tera::Transaction* t,
                                   tera::Client* client,
                                   const std::string& table_name,
                                   const std::string& family,
                                   const std::string& qualifier,
                                   const std::string& row,
                                   const std::string& value,
                                   int64_t timestamp,
                                   Notification* notification) {
    LOG(INFO) << "[OnNotify start] table:row:cf:qu="
              << table_name << ":" << row << ":" << family << ":" 
              << qualifier << ":" << timestamp
              << " begin count:" << ++notify_cnt_;
    TxnContext* ctx = new TxnContext();
    ctx->observer = this;
    ctx->txn = t;
    ctx->notification = notification;
    ctx->row = row;
    ctx->family = family;
    ctx->qualifier = qualifier;
    ctx->begin_time = get_micros();

    tera::ErrorCode err;
    ctx->input_table = client->OpenTable(table_name, &err);
    ctx->output_table = client->OpenTable("second_table", &err);

    tera::RowReader* reader = ctx->input_table->NewRowReader(row);
    reader->AddColumn(family, "qu0");
    reader->AddColumn(family, "qu1");
    reader->SetContext(ctx);
    reader->SetCallBack(ReadRowCallBack);
    t->Get(reader);
}

std::string IntegrationObserver::GetObserverName() const {
    return "IntegrationObserver";
}

TransactionType IntegrationObserver::GetTransactionType() const {
    return kGlobalTransaction;
}

void DemoObserver::OnNotify(tera::Transaction* t,
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
        " value=" << value << " timestamps=" << timestamp;

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
    notification->Done();
    VLOG(12) <<"[time] OnNotify finish. [row] " << row;
}

std::string DemoObserver::GetObserverName() const {
    return "DemoObserver";
}

TransactionType DemoObserver::GetTransactionType() const {
    return kGlobalTransaction;
}

void TranscationCallback(Transaction* transaction) {
    ParseObserver::TransactionContext* context = (ParseObserver::TransactionContext*)(transaction->GetContext());
    LOG(INFO) << "table: " << context->table_name << " row: " << context->row;
}

void ParseObserver::OnNotify(tera::Transaction* t,
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
        " value=" << value << " timestamps=" << timestamp;

    tera::ErrorCode err;
    TransactionContext* context = new TransactionContext();
    context->table_name = table_name;
    context->row = row;
    t->SetContext(context);
    t->SetCommitCallback(TranscationCallback);
    // do nothing
    tera::Table* table = client->OpenTable(table_name, &err);

    tera::RowMutation* mutation = table->NewRowMutation(row);
    mutation->Put(family, qualifier, "value");
    t->ApplyMutation(mutation);
    notification->Ack(table, row, family, qualifier);
    err = t->Commit();
    notification->Done();
}

std::string ParseObserver::GetObserverName() const {
    return "ParseObserver";
}

TransactionType ParseObserver::GetTransactionType() const {
    return kGlobalTransaction;
}

void SingleRowObserver::OnNotify(tera::Transaction* t,
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
        " value=" << value << " timestamps=" << timestamp;

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
    notification->Done();
}

std::string SingleRowObserver::GetObserverName() const {
    return "SingleRowObserver";
}

TransactionType SingleRowObserver::GetTransactionType() const {
    return kSingleRowTransaction;
}

void NoneTransactionObserver::OnNotify(tera::Transaction* t,
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
        " value=" << value << " timestamps=" << timestamp;

    tera::ErrorCode err;
    tera::Table* table = client->OpenTable(table_name, &err);

    // do something
    // kNoneTransaction notify
    notification->Ack(table, row, family, qualifier);

    // kNoneTransaction ack
    tera::Table* notify_table = client->OpenTable("notify_table", &err);
    notification->Notify(notify_table, "notify_row", "family", "qualifier");
    notification->Done();
}

std::string NoneTransactionObserver::GetObserverName() const {
    return "NoneTransactionObserver";
}

TransactionType NoneTransactionObserver::GetTransactionType() const {
    return kNoneTransaction;
}

} // namespace observer
} // namespace tera

