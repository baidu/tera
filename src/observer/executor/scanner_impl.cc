// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/executor/scanner_impl.h"

#include <assert.h>
#include <signal.h>
#include <sys/time.h>

#include <functional>

#include "gflags/gflags.h"

#include "common/base/string_number.h"
#include "common/this_thread.h"
#include "observer/executor/random_key_selector.h"
#include "observer/executor/notification.h"
#include "observer/executor/notification_impl.h"
#include "observer/rowlocknode/fake_rowlock_client.h"
#include "sdk/table_impl.h"
#include "sdk/sdk_utils.h"
#include "tera.h"
#include "types.h"

DECLARE_int32(observer_proc_thread_num);
DECLARE_int32(observer_scanner_thread_num);
DECLARE_int32(observer_ack_conflict_timeout);
DECLARE_int64(observer_max_pending_limit);
DECLARE_int64(observer_ack_timeout_time);
DECLARE_string(flagfile);
DECLARE_string(rowlock_server_ip);
DECLARE_string(rowlock_server_port);
DECLARE_int32(observer_rowlock_client_thread_num);
DECLARE_int32(observer_random_access_thread_num);
DECLARE_bool(mock_rowlock_enable);

using namespace std::placeholders;

namespace tera {
namespace observer {

ScannerImpl* ScannerImpl::scanner_instance_ = new ScannerImpl();
Scanner* Scanner::GetScanner() {
    return ScannerImpl::GetInstance();
}

ScannerImpl* ScannerImpl::GetInstance() {
    return scanner_instance_;
}

ScannerImpl::ScannerImpl()
    : table_observe_info_(new std::map<std::string, TableObserveInfo>),
      scan_table_threads_(new common::ThreadPool(FLAGS_observer_scanner_thread_num)),
      observer_threads_(new common::ThreadPool(FLAGS_observer_proc_thread_num)),
      transaction_callback_threads_(new common::ThreadPool(FLAGS_observer_random_access_thread_num)),
      quit_(false),
      semaphore_(FLAGS_observer_max_pending_limit) {
    profiling_thread_.Start(std::bind(&ScannerImpl::Profiling, this));
}

ScannerImpl::~ScannerImpl() {
    Exit();

    scan_table_threads_->Stop(true);
    transaction_callback_threads_->Stop(false);
    observer_threads_->Stop(true);

    profiling_thread_.Join();

    MutexLock locker(&table_mutex_);
    // close table
    for (auto it = table_observe_info_->begin(); it != table_observe_info_->end(); ++it) {
        if (it->second.table) {
            delete it->second.table;
        }
    }

    for (auto it = observers_.begin(); it != observers_.end(); ++it) {
    	delete *it;
    }
}

ErrorCode ScannerImpl::Observe(const std::string& table_name,
                               const std::string& column_family,
                               const std::string& qualifier,
                               Observer* observer) {
    // Observe before init
    tera::ErrorCode err;
    if (!tera_client_) {
        LOG(ERROR) << "Init scanner first!";
        err.SetFailed(ErrorCode::kSystem, "observe before scanner init");
        return err;
    }

    Column column = {table_name, column_family, qualifier};

    {
    
        MutexLock locker(&table_mutex_);
        if (!table_observe_info_.unique()) {
            // Shared_ptr construct a new copy from the original one.
            // Former requests still reading the original shared_ptr
            // Write operation executed on the new copy, so as the later requests
            table_observe_info_.reset(new std::map<std::string, TableObserveInfo>(*table_observe_info_));
        }

        if (!(*table_observe_info_)[table_name].table) {
            // init table
            tera::Table* table = tera_client_->OpenTable(table_name, &err);
            if (tera::ErrorCode::kOK != err.GetType()) {
                LOG(ERROR) << "open tera table [" << table_name
                           << "] failed, " << err.ToString();
                return err;
            }
            LOG(INFO) << "open tera table [" << table_name << "] succ";

            // build map<table_name, table>
            (*table_observe_info_)[table_name].table = table;
            (*table_observe_info_)[table_name].type = GetTableTransactionType(table);
        }

        if (!CheckTransactionTypeLegalForTable(observer->GetTransactionType(),
                (*table_observe_info_)[table_name].type)) {

            LOG(ERROR) << "Transaction type does not match table. table_name: "
                       << table_name << " type: "
                       << (*table_observe_info_)[table_name].type
                       << "  , observer name: "
                       << observer->GetObserverName() << " type: "
                       << observer->GetTransactionType();

            err.SetFailed(ErrorCode::kSystem, "Transaction type does not match table");
            return err;
        }

        auto it = (*table_observe_info_)[table_name].observe_columns[column].insert(observer);
        if (!it.second) {
            LOG(ERROR) << "Observer " << observer->GetObserverName()
                       << " observe " << table_name << ":"
                       << column_family << ":" << qualifier
                       << " more than once!";
            err.SetFailed(ErrorCode::kSystem,
                "the same observer observe the same column more than once");
            return err;
        }
        observers_.insert(observer);
    }

    err = key_selector_->Observe(table_name);
    LOG(INFO) << "Observer start. table: " << table_name
              << "  cf:qu " << column_family << ":"
              << qualifier << "  observer: "
              << observer->GetObserverName();

    return err;
}

bool ScannerImpl::Init() {
    tera::ErrorCode err;
    if (!tera_client_) {
        tera_client_.reset(tera::Client::NewClient(FLAGS_flagfile, &err));

        if (tera::ErrorCode::kOK != err.GetType()) {
            LOG(ERROR) << "init tera client [" << FLAGS_flagfile
                       << "] failed, " << err.ToString();
            return false;
        }
    }

    // init key_selector_
    // different selector started by different flags
    key_selector_.reset(new RandomKeySelector());

    return true;
}

bool ScannerImpl::Start() {
    for (int32_t idx = 0; idx < FLAGS_observer_scanner_thread_num; ++idx) {
        scan_table_threads_->AddTask(std::bind(&ScannerImpl::ScanTable, this));
    }
    return true;
}

void ScannerImpl::Exit() {
    quit_ = true;
}

tera::Client* ScannerImpl::GetTeraClient() const {
    return tera_client_.get();
}

void ScannerImpl::ScanTable() {
    std::string start_key;
    std::string table_name;
    std::set<Column> observe_columns;
    tera::Table* table = NULL;

    // table and start key will be refreshed.
    while (!quit_) {
        if (key_selector_->SelectStart(&table_name, &start_key)) {
            GetObserveColumns(table_name, &observe_columns);
            table = GetTable(table_name);
            if (DoScanTable(table, observe_columns, start_key, "")) {
                DoScanTable(table, observe_columns, "", start_key);
            }
        }
    }
}

bool ScannerImpl::DoScanTable(tera::Table* table,
                              const std::set<Column>& observe_columns,
                              const std::string& start_key,
                              const std::string& end_key) {
    if (table == NULL) {
        return false;
    }
    LOG(INFO) << "Start scan table. Table name: [" << table->GetName()
        << "]. Start key: [" << start_key << "]";

    tera::ScanDescriptor desc(start_key);
    desc.SetEnd(end_key);

    // Notify stores in single lg
    desc.AddColumnFamily(kNotifyColumnFamily);
    tera::ErrorCode err;
    std::unique_ptr<tera::ResultStream> result_stream(table->Scan(desc, &err));
    if (tera::ErrorCode::kOK != err.GetType()) {
        LOG(ERROR) << "table scan failed, " << err.ToString();
        return false;
    }

    if (result_stream->Done(&err)) {
        return !quit_;
    }

    bool finished = false;
    while (true) {
        std::string rowkey;
        std::vector<Column> notify_columns;
        if (!NextRow(result_stream.get(), table->GetName(), &finished, &rowkey, &notify_columns)) {
            return finished;
        }

        if (!TryLockRow(table->GetName(), rowkey)) {
            // collision
            LOG(INFO) <<"[rowlock failed] table=" << table->GetName() << " row=" << rowkey;
            return false;
        }
        VLOG(12) <<"[time] read value start. [row] " << rowkey;

        std::shared_ptr<AutoRowUnlocker> unlocker(new AutoRowUnlocker(table->GetName(), rowkey));
        std::vector<std::shared_ptr<NotifyCell>> notify_cells;
        PrepareNotifyCell(table, rowkey, observe_columns, notify_columns, unlocker, &notify_cells);

        for (uint32_t i = 0; i < notify_cells.size(); ++i) {
            AsyncReadCell(notify_cells[i]);
        }
    }
    return true;
}
void ScannerImpl::PrepareNotifyCell(tera::Table* table,
                                    const std::string& rowkey,
                                    const std::set<Column>& observe_columns,
                                    const std::vector<Column>& notify_columns, 
                                    std::shared_ptr<AutoRowUnlocker> unlocker,
                                    std::vector<std::shared_ptr<NotifyCell>>* notify_cells) {
    std::shared_ptr<std::map<std::string, TableObserveInfo>> table_observe_info_read_copy;
    {   
        MutexLock locker(&table_mutex_);
        // shared_ptr ref +1
        table_observe_info_read_copy = table_observe_info_;
    }

    for (auto notify_column = notify_columns.begin(); notify_column != notify_columns.end(); ++notify_column) {
        if (observe_columns.find(*notify_column) == observe_columns.end()) {
            LOG(WARNING) << "miss observed column, table_name" << table->GetName() 
                << " cf=" << notify_column->family << " qu=" << notify_column->qualifier;
            continue;
        }
        std::map<Column, std::set<Observer*>>& observe_columns = 
            (*table_observe_info_read_copy)[table->GetName()].observe_columns;

        TransactionType type = (*table_observe_info_read_copy)[table->GetName()].type;

        for (auto observer = observe_columns[*notify_column].begin(); 
            observer != observe_columns[*notify_column].end(); ++observer) {             
            semaphore_.Acquire();
            std::shared_ptr<NotifyCell> notify_cell(new NotifyCell(semaphore_));
            switch (type) {
                case kGlobalTransaction:
                    notify_cell->notify_transaction.reset(tera_client_->NewGlobalTransaction());
                    if (!notify_cell->notify_transaction) {
                        LOG(ERROR) << "NewGlobalTransaction failed. Notify cell ignored. table: "
                        << table->GetName() << " row: " << rowkey
                        << " family: " << notify_column->family
                        << " qualifier: " << notify_column->qualifier;
                        continue;
                    }
                    break;
                case kSingleRowTransaction:
                    notify_cell->notify_transaction.reset(table->StartRowTransaction(rowkey));
                    if (!notify_cell->notify_transaction) {
                        LOG(ERROR) << "StartRowTransaction failed. Notify cell ignored. table: "
                        << table->GetName() << " row: "
                        << rowkey << " family: " << notify_column->family
                        << " qualifier: " << notify_column->qualifier;
                        continue;
                    }
                    break;
                default:
                    break;
            }
  
            notify_cell->table = table;
            notify_cell->row = rowkey;
            notify_cell->observed_column = *notify_column;
            notify_cell->unlocker = unlocker;
            notify_cell->observer = *observer;
            notify_cells->push_back(notify_cell);
        }
    }                                 
}

bool ScannerImpl::NextRow(tera::ResultStream* result_stream,
                          const std::string& table_name, bool* finished,
                          std::string* row, std::vector<Column>* notify_columns) {
    tera::ErrorCode err;

    // check finish
    if (result_stream->Done(&err)) {
        *finished = true;
        return false;
    }

    if (tera::ErrorCode::kOK != err.GetType()) {
        LOG(ERROR) << "scanning failed" << err.ToString();
        return false;
    }

    notify_columns->clear();
    *row = result_stream->RowName();

    // scan cell
    while (!result_stream->Done(&err) && result_stream->RowName() == *row) {
        std::string observe_cf;
        std::string observe_qu;
        if (quit_) {
            return false;
        }

        if (!ParseNotifyQualifier(result_stream->Qualifier(), &observe_cf, &observe_qu)) {
            LOG(WARNING) << "parse notify qualifier failed: " << result_stream->Qualifier();
            result_stream->Next();
            continue;
        }  

        Column notify_column = {table_name, observe_cf, observe_qu};

        notify_columns->push_back(notify_column);
        result_stream->Next();
    }
    return true;
}

// example qualifier: C:url
// C: cf; column: url;
bool ScannerImpl::ParseNotifyQualifier(const std::string& notify_qualifier,
                                       std::string* data_family,
                                       std::string* data_qualifier) {

    std::vector<std::string> frags;
    std::size_t pos = std::string::npos;
    std::size_t start_pos = 0;
    std::string frag;

    // parse cf
    pos = notify_qualifier.find_first_of(':', start_pos);
    if (pos == std::string::npos) {
        LOG(ERROR) << "Parse notify qualifier error: " << notify_qualifier;
        return false;
    }
    frag = notify_qualifier.substr(start_pos, pos - start_pos);
    frags.push_back(frag);
    start_pos = pos + 1;

    pos = notify_qualifier.size();
    frag = notify_qualifier.substr(start_pos, pos - start_pos);
    frags.push_back(frag);
    if (2 != frags.size()) {
        return false;
    }
    if (frags[0] == "" || frags[1] == "") {
        return false;
    }
    *data_family = frags[0];
    *data_qualifier = frags[1];

    return true;
}

void ScannerImpl::AsyncReadCell(std::shared_ptr<NotifyCell> notify_cell) {
    VLOG(12) <<"[time] do read value start. [row] " 
             << notify_cell->row << " cf:qu " << notify_cell->observed_column.family 
             << ":" << notify_cell->observed_column.qualifier;
    tera::RowReader* value_reader = 
        notify_cell->table->NewRowReader(notify_cell->row);
    assert(value_reader != NULL);
    value_reader->AddColumn(notify_cell->observed_column.family,
                          notify_cell->observed_column.qualifier);
    // transaction read
    NotificationContext* context = new NotificationContext();
    context->notify_cell = notify_cell;
    context->scanner_impl = this;

    value_reader->SetContext(context);
    value_reader->SetCallBack([] (RowReader* value_reader) {
        NotificationContext* context = (NotificationContext*)(value_reader->GetContext());
        if (!context->scanner_impl->quit_) {
            context->scanner_impl->transaction_callback_threads_->
                AddTask(std::bind(&ScannerImpl::ValidateCellValue,
                                  context->scanner_impl,
                                  value_reader));
        } else {
            // call auto unlocker
            delete context;
            context = NULL;
            delete value_reader;
        }
    });
    if (notify_cell->notify_transaction.get()) {
        notify_cell->notify_transaction->Get(value_reader);
    } else {
        notify_cell->table->Get(value_reader);
    }
}

void ScannerImpl::GetObserveColumns(const std::string& table_name,
                                    std::set<Column>* observe_columns) {
    observe_columns->clear();

    std::shared_ptr<std::map<std::string, TableObserveInfo>> table_observe_info_read_copy;
    {
    
        MutexLock locker(&table_mutex_);
        // shared_ptr ref +1
        table_observe_info_read_copy = table_observe_info_;
    }

    for (auto it : (*table_observe_info_read_copy)[table_name].observe_columns) {
        observe_columns->insert(it.first);
    }
}

tera::Table* ScannerImpl::GetTable(const std::string table_name) {
    std::shared_ptr<std::map<std::string, TableObserveInfo>> table_observe_info_read_copy;
    {
    
        MutexLock locker(&table_mutex_);
        table_observe_info_read_copy = table_observe_info_;
    }
    return (*table_observe_info_read_copy)[table_name].table;
}

void ScannerImpl::Profiling() {
    while (!quit_) {
        LOG(INFO) << "[Observer Profiling Info]  total: "
            << total_counter_.Get() << " failed: "
            << fail_counter_.Get()
            << "  transaction pending: "
            << observer_threads_->PendingNum();
        ThisThread::Sleep(1000);
        total_counter_.Clear();
        fail_counter_.Clear();
    }
}

void ScannerImpl::AsyncReadAck(std::shared_ptr<NotifyCell> notify_cell) {
    VLOG(12) <<"[time] Check ACK start. [cf:qu] "
        << notify_cell->observed_column.family
        << notify_cell->observed_column.qualifier;

    const std::string& ack_qualifier_prefix =
        GetAckQualifierPrefix(notify_cell->observed_column.family,
                              notify_cell->observed_column.qualifier);

    // use transaction to read column Ack
    std::shared_ptr<tera::Transaction> row_transaction(
        notify_cell->table->StartRowTransaction(notify_cell->row));
    NotificationContext* context = new NotificationContext();

    // read Acks
    tera::RowReader* row_reader = notify_cell->table->NewRowReader(notify_cell->row);

    const std::string& ack_qualifier = GetAckQualifier(ack_qualifier_prefix,
                                                notify_cell->observer->GetObserverName());
    context->ack_qualifier = ack_qualifier;

    row_reader->AddColumn(notify_cell->observed_column.family, ack_qualifier);

    context->notify_cell = notify_cell;
    context->scanner_impl = this;
    context->ack_transaction = row_transaction;
    row_reader->SetContext(context);
    row_reader->SetCallBack([] (RowReader* ack_reader) {
        NotificationContext* context = (NotificationContext*)(ack_reader->GetContext());
        if (!context->scanner_impl->quit_) {
            context->scanner_impl->transaction_callback_threads_->AddTask(
                    std::bind(&ScannerImpl::ValidateAckConfict, 
                              context->scanner_impl,
                              ack_reader));
        } else {
            // call auto unlocker
            delete context;
            context = NULL;
            delete ack_reader;
        }
    });

    row_transaction->Get(row_reader);
}

std::string ScannerImpl::GetAckQualifierPrefix(
    const std::string& family,
    const std::string& qualifier) const {
    return family + ":" + qualifier;
}

std::string ScannerImpl::GetAckQualifier(const std::string& prefix,
                                         const std::string& observer_name) const {
    return prefix + "+ack_" + observer_name;
}

bool ScannerImpl::TryLockRow(const std::string& table_name,
                             const std::string& row) const {
    VLOG(12) << "[time] trylock wait " << table_name << " " << row;
    
    RowlockRequest request;
    RowlockResponse response;

    std::shared_ptr<RowlockClient> rowlock_client;

    if (FLAGS_mock_rowlock_enable == true) {
        rowlock_client.reset(new FakeRowlockClient());
    } else {
        rowlock_client.reset(new RowlockClient());
    }

    request.set_table_name(table_name);
    request.set_row(row);

    VLOG(12) << "[time] trylock " << table_name << " " << row;
    if (!rowlock_client->TryLock(&request, &response)) {
        LOG(ERROR) << "TryLock rpc fail, row: " << row;
        return false;
    }

    if (response.lock_status() != kLockSucc) {
        LOG(INFO) << "Lock row fail, row: " << request.row();
        return false;
    }
    VLOG(12) << "[time] lock success " << request.table_name()
             << " " << request.row();

    return true;
}

bool ScannerImpl::CheckTransactionTypeLegalForTable(TransactionType transaction_type,
                                                    TransactionType table_type) {
    if (transaction_type == table_type) {
        return true;
    }

    if (transaction_type == kNoneTransaction && table_type ==  kSingleRowTransaction) {
        return true;
    }

    return false;
}

TransactionType ScannerImpl::GetTableTransactionType(tera::Table* table) {
    tera::ErrorCode err;
    std::shared_ptr<Table> table_ptr;
    table_ptr.reset(tera_client_->OpenTable(table->GetName(), &err));
    std::shared_ptr<TableImpl> table_impl(static_cast<TableWrapper*>(table_ptr.get())->GetTableImpl());
    TableSchema schema = table_impl->GetTableSchema();

    if (IsTransactionTable(schema)) {
        std::set<std::string> gtxn_cfs;
        FindGlobalTransactionCfs(schema, &gtxn_cfs);
        if (gtxn_cfs.size() > 0) {
            return kGlobalTransaction;
        }
        return kSingleRowTransaction;
    }
    return kNoneTransaction;
}

void ScannerImpl::ValidateCellValue(RowReader* value_reader) {
    std::unique_ptr<NotificationContext> context((NotificationContext*)(value_reader->GetContext()));
    std::shared_ptr<NotifyCell> notify_cell = context->notify_cell;
    VLOG(12) <<"[time] do read value finish. [row] " << notify_cell->row;

    std::unique_ptr<RowReader> cell_reader(value_reader);

    if (cell_reader->Done()) {
        LOG(WARNING) << "No read value, row: " << notify_cell->row;
        return;
    }

    if (tera::ErrorCode::kOK == cell_reader->GetError().GetType()) {
        notify_cell->value = cell_reader->Value();
        notify_cell->timestamp = cell_reader->Timestamp();    

        std::shared_ptr<std::map<std::string, TableObserveInfo>> table_observe_info_read_copy;
        {    
            MutexLock locker(&table_mutex_);
            table_observe_info_read_copy = table_observe_info_;
        }

        auto it = table_observe_info_read_copy->find(notify_cell->observed_column.table_name);
        if (it == table_observe_info_read_copy->end()) {
            LOG(WARNING) << "table not found: " << notify_cell->observed_column.table_name;
            return;
        }

        if (it->second.observe_columns.find(notify_cell->observed_column)
            == it->second.observe_columns.end()) {
            LOG(WARNING) << "column not found. cf: "
                << notify_cell->observed_column.family
                << "  qu: " << notify_cell->observed_column.qualifier;
            return;
        }

        if (it->second.observe_columns[notify_cell->observed_column].size() == 0) {
            LOG(WARNING) << "no match observers, table="
                << notify_cell->observed_column.table_name
                <<" cf=" << notify_cell->observed_column.family
                << " qu=" << notify_cell->observed_column.qualifier;
            return;
        }

        if (notify_cell->observer->GetTransactionType() != kGlobalTransaction) {
            ObserveCell(notify_cell);
        } else {
            AsyncReadAck(notify_cell);
        }
    } else {
        LOG(WARNING) << "[read failed] table=" << notify_cell->table->GetName()
            << " cf=" << notify_cell->observed_column.family
            << " qu=" << notify_cell->observed_column.qualifier
            << " row=" << notify_cell->row << " err="
            << cell_reader->GetError().GetType()
            << cell_reader->GetError().GetReason();
        return;
    }
}

void ScannerImpl::ObserveCell(std::shared_ptr<NotifyCell> notify_cell) {
    observer_threads_->AddTask( [=] (int64_t) {
        Notification* notification = GetNotification(notify_cell);
        notify_cell->observer->OnNotify(notify_cell->notify_transaction.get(), tera_client_.get(),
                                        notify_cell->observed_column.table_name,
                                        notify_cell->observed_column.family,
                                        notify_cell->observed_column.qualifier,
                                        notify_cell->row, notify_cell->value,
                                        notify_cell->timestamp, notification);
        total_counter_.Inc();
    });
}


void ScannerImpl::ValidateAckConfict(RowReader* ack_reader) {
    NotificationContext* context = (NotificationContext*)(ack_reader->GetContext());
    std::shared_ptr<NotifyCell> notify_cell = context->notify_cell;
    std::unique_ptr<RowReader> ack_row_reader(ack_reader);

    bool is_collision = false;

    if (tera::ErrorCode::kOK == ack_row_reader->GetError().GetType()) {
        while (!ack_reader->Done()) {
            int64_t latest_observer_start_ts = 0;
            if (!StringToNumber(ack_row_reader->Value(), &latest_observer_start_ts)) {
                LOG(INFO) << "Convert string to timestamp failed! String: "
                           << ack_row_reader->Value() << " row="
                           << notify_cell->row << " cf="
                           << notify_cell->observed_column.family << " qu="
                           << notify_cell->observed_column.qualifier;
                is_collision = true;
                break;
            }

            // collision check ack ts later than notify ts
            if (latest_observer_start_ts >= notify_cell->timestamp &&
                notify_cell->notify_transaction->GetStartTimestamp() - latest_observer_start_ts
                < FLAGS_observer_ack_conflict_timeout) {
                // time too short, collisision, ignore
                is_collision = true;
                LOG(INFO) << "own collision. row=" << notify_cell->row
                          << " cf=" << notify_cell->observed_column.family
                          << " qu=" << notify_cell->observed_column.qualifier
                          << ", latest observer start_ts="
                          << latest_observer_start_ts
                          << ", observer start_ts="
                          << notify_cell->notify_transaction->GetStartTimestamp()
                          << ", data commit_ts=" << notify_cell->timestamp;
                break;

            }
            ack_row_reader->Next();
        }
    } else {
        LOG(INFO) << "read Acks failed, err="
                  << ack_row_reader->GetError().GetReason() << " row="
                  << notify_cell->row << " cf="
                  << notify_cell->observed_column.family << " qu="
                  << notify_cell->observed_column.qualifier;
    }

    if (!is_collision) {
        context->scanner_impl->SetAckVersion(context);
    } else {
        delete context;
        context = NULL;
    }
}

void ScannerImpl::SetAckVersion(NotificationContext* ack_context) {
    std::shared_ptr<Transaction> row_transaction = ack_context->ack_transaction;
    std::shared_ptr<NotifyCell> notify_cell = ack_context->notify_cell;
    // set Acks
    std::unique_ptr<tera::RowMutation> set_ack_version(notify_cell->table->NewRowMutation(notify_cell->row));

    set_ack_version->Put(notify_cell->observed_column.family, ack_context->ack_qualifier,
                         std::to_string(notify_cell->notify_transaction->GetStartTimestamp()), 
                         FLAGS_observer_ack_conflict_timeout);

    row_transaction->SetContext(ack_context);
    row_transaction->SetCommitCallback([] (Transaction* ack_transaction) {
        NotificationContext* ack_context = (NotificationContext*)(ack_transaction->GetContext());
        if (!ack_context->scanner_impl->quit_) {
            ack_context->scanner_impl->transaction_callback_threads_->AddTask(
                    std::bind(&ScannerImpl::SetAckVersionCallBack, 
                              ack_context->scanner_impl,
                              ack_transaction));
        } else {
            delete ack_context;
            ack_context = NULL;
        }       
    });
    row_transaction->ApplyMutation(set_ack_version.get());
    row_transaction->Commit();
}

void ScannerImpl::SetAckVersionCallBack(Transaction* ack_transaction) {
    std::unique_ptr<NotificationContext> ack_context((NotificationContext*)(ack_transaction->GetContext()));
    std::shared_ptr<NotifyCell> notify_cell = ack_context->notify_cell;

    if (ack_transaction->GetError().GetType() != tera::ErrorCode::kOK) {
        LOG(INFO) << "write Ack failed, row=" << notify_cell->row
                  << " err=" << ack_transaction->GetError().GetReason()
                  << " cf=" << notify_cell->observed_column.family
                  << " qu=" << notify_cell->observed_column.qualifier;
        return;
    }
    VLOG(12) <<"[time] ACK mutation finish. [cf:qu] "
             << notify_cell->observed_column.family
             << notify_cell->observed_column.qualifier;

    ObserveCell(notify_cell);
}

} // namespace observer
} // namespace tera

