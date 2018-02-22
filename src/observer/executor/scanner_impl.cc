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
DECLARE_int64(observer_max_pending_task);
DECLARE_int64(observer_ack_timeout_time);
DECLARE_string(flagfile);
DECLARE_string(rowlock_server_ip);
DECLARE_string(rowlock_server_port);
DECLARE_int32(observer_rowlock_client_thread_num);
DECLARE_bool(mock_rowlock_enable);

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
    : tera_client_(NULL),
      table_observe_info_(new std::map<std::string, TableObserveInfo>),
      scan_table_threads_(new common::ThreadPool(FLAGS_observer_scanner_thread_num)),
      transaction_threads_(new common::ThreadPool(FLAGS_observer_proc_thread_num)),
      quit_(false),
      cond_(&quit_mutex_) {
    profiling_thread_.Start(std::bind(&ScannerImpl::Profiling, this));
}

ScannerImpl::~ScannerImpl() {
    Exit();

    scan_table_threads_->Stop(true);
    transaction_threads_->Stop(true);
    profiling_thread_.Join();

    MutexLock locker(&table_mutex_);
    // close table
    for (auto it = table_observe_info_->begin(); it != table_observe_info_->end(); ++it) {
        if (it->second.table != NULL) {
            delete it->second.table;
        }
    }

    if (tera_client_ != NULL) {
        delete tera_client_;
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
    if (NULL == tera_client_) {
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

        if ((*table_observe_info_)[table_name].table == NULL) {
            // init table
            tera::Table* table = tera_client_->OpenTable(table_name, &err);
            if (tera::ErrorCode::kOK != err.GetType()) {
                LOG(ERROR) << "open tera table [" << table_name << "] failed, " << err.ToString();
                return err;
            }
            LOG(INFO) << "open tera table [" << table_name << "] succ";

            // build map<table_name, table>
            (*table_observe_info_)[table_name].table = table;
            (*table_observe_info_)[table_name].type = GetTableTransactionType(table);
        }

        if (!CheckTransactionTypeLegalForTable(observer->GetTransactionType(),
                (*table_observe_info_)[table_name].type)) {
            LOG(ERROR) << "Transaction type does not match table. table_name: " << table_name
                << " type: " << (*table_observe_info_)[table_name].type << "  , observer name: " <<
                observer->GetObserverName() << " type: " << observer->GetTransactionType();
            err.SetFailed(ErrorCode::kSystem, "Transaction type does not match table");
            return err;
        }

        auto it = (*table_observe_info_)[table_name].observe_columns[column].insert(observer);
        if (!it.second) {
            LOG(ERROR) << "Observer " << observer->GetObserverName() << " observe " << table_name
                << ":" << column_family << ":" << qualifier << " more than once!";
            err.SetFailed(ErrorCode::kSystem, "the same observer observe the same column more than once");
            return err;
        }
        observers_.insert(observer);
    }

    err = key_selector_->Observe(table_name);
    LOG(INFO) << "Observer start. table: " << table_name << "  cf:qu " << column_family << ":" <<
        qualifier << "  observer: " << observer->GetObserverName();

    return err;
}

bool ScannerImpl::Init() {
    tera::ErrorCode err;
    if (NULL == tera_client_) {
        tera_client_ = tera::Client::NewClient(FLAGS_flagfile, &err);

        if (tera::ErrorCode::kOK != err.GetType()) {
            LOG(ERROR) << "init tera client [" << FLAGS_flagfile << "] failed, " << err.ToString();
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
    // the scope of quit_mutex only covers cond_ broadcast
    MutexLock locker(&quit_mutex_);
    quit_ = true;
    cond_.Broadcast();
}

tera::Client* ScannerImpl::GetTeraClient() const {
    return tera_client_;
}

void ScannerImpl::ScanTable() {
    std::string start_key;
    std::string table_name;
    std::set<Column> columns;
    tera::Table* table = NULL;

    // table and start key will be refreshed.
    while (true) {
        {
            MutexLock locker(&quit_mutex_);
            if (quit_) {
	            break;
	        }
            cond_.TimeWaitInUs(kObserverWaitTime);
        }

        if (key_selector_->SelectStart(&table_name, &start_key)) {
            GetObserveColumns(table_name, &columns);
        } else {
            continue;
        }

        table = GetTable(table_name);
        if (DoScanTable(table, columns, start_key, "")) {
            DoScanTable(table, columns, "", start_key);
        }
    }
}

bool ScannerImpl::DoScanTable(tera::Table* table,
                              const std::set<Column>& columns,
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
        LOG(ERROR) << " ERR " << err.GetReason();
    }

    bool finished = false;
    std::string rowkey;
    std::vector<Column> vec_col;
    while (NextRow(columns, result_stream.get(), table->GetName(), &finished, &rowkey, &vec_col)) {
        // lock row
        if (!TryLockRow(table->GetName(), rowkey)) {
            // collision
            LOG(INFO) <<"[rowlock failed] table=" << table->GetName() << " row=" << rowkey;
            return false;
        }
        VLOG(12) <<"[time] Transaction start. [row] " << rowkey;

        // automatic unlock
        std::shared_ptr<AutoRowUnlocker> unlocker(
            new AutoRowUnlocker(table->GetName(), rowkey));

        for (uint32_t i = 0; i < vec_col.size(); ++i ) {
            tera::Transaction* t = NULL;
            TransactionType type;
            {
                MutexLock locker(&table_mutex_);
                type = (*table_observe_info_)[table->GetName()].type;
            }

            switch (type) {
                case kGlobalTransaction:
                    t = tera_client_->NewGlobalTransaction();
                    if (t == NULL) {
                        LOG(ERROR) << "NewGlobalTransaction failed. Notify cell ignored. table: " << table->GetName()
                        << " row: " << rowkey << " family: " << vec_col[i].family
                        << " qualifier: " << vec_col[i].qualifier;
                        continue;
                    }
                    break;
                case kSingleRowTransaction:
                    t = table->StartRowTransaction(rowkey);
                    if (t == NULL) {
                        LOG(ERROR) << "StartRowTransaction failed. Notify cell ignored. table: " << table->GetName()
                        << " row: " << rowkey << " family: " << vec_col[i].family
                        << " qualifier: " << vec_col[i].qualifier;
                        continue;
                    }
                    break;
                default:
                    break;
            }
            std::shared_ptr<NotifyCell> notify_cell(new NotifyCell(t));
            notify_cell->table = table;
            notify_cell->row = rowkey;
            notify_cell->observed_column = vec_col[i];
            notify_cell->unlocker = unlocker;

            DoReadValue(notify_cell);
        }

        MutexLock locker(&quit_mutex_);
        if (quit_) {
            return false;
        }
    }
    if (finished) {
        return true;
    } else {
        return false;
    }

}

bool ScannerImpl::NextRow(const std::set<Column>& columns, tera::ResultStream* result_stream,
                          const std::string& table_name, bool* finished,
                          std::string* row, std::vector<Column>* vec_col) {
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

    vec_col->clear();
    *row = result_stream->RowName();

    // scan cell
    while (!result_stream->Done(&err) && result_stream->RowName() == *row) {
        while (transaction_threads_->PendingNum() > FLAGS_observer_max_pending_task) {
            VLOG(12) << "transaction_threads pending: " << transaction_threads_->PendingNum();
            MutexLock locker(&quit_mutex_);
            if (quit_) {
                return false;
            }
            cond_.TimeWaitInUs(kObserverWaitTime);
        }
        std::string ob_cf;
        std::string ob_qu;

        if (!ParseNotifyQualifier(result_stream->Qualifier(), &ob_cf, &ob_qu)) {
            LOG(WARNING) << "parse notify qualifier failed: " << result_stream->Qualifier();
            result_stream->Next();
            continue;
        }

        Column ob_col = {table_name, ob_cf, ob_qu};
        if (columns.end() == columns.find(ob_col)) {
            LOG(WARNING) << "miss observed column, table_name" << table_name <<
                " cf=" << ob_cf << " qu=" << ob_qu;
            result_stream->Next();
            continue;
        }
        vec_col->push_back(ob_col);
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

bool ScannerImpl::DoReadValue(std::shared_ptr<NotifyCell> notify_cell) {
    VLOG(12) <<"[time] do read value start. [row] " << notify_cell->row;
    std::unique_ptr<tera::RowReader> row_reader(notify_cell->table->NewRowReader(notify_cell->row));
    assert(row_reader.get() != NULL);
    row_reader->AddColumn(notify_cell->observed_column.family, notify_cell->observed_column.qualifier);
    // transaction read
    if (notify_cell->transaction != NULL) {
        notify_cell->transaction->Get(row_reader.get());
    } else {
        notify_cell->table->Get(row_reader.get());
    }
    VLOG(12) <<"[time] do read value finish. [row] " << notify_cell->row;
    if (tera::ErrorCode::kOK == row_reader->GetError().GetType()) {
        notify_cell->value = row_reader->Value();
        notify_cell->timestamp = row_reader->Timestamp();

        std::shared_ptr<std::map<std::string, TableObserveInfo>> table_observe_info_read_copy;
        {
            MutexLock locker(&table_mutex_);
            // shared_ptr ref +1
            table_observe_info_read_copy = table_observe_info_;
        }

        auto it = table_observe_info_read_copy->find(notify_cell->observed_column.table_name);
        if (it == table_observe_info_read_copy->end()) {
            LOG(WARNING) << "table not found: " << notify_cell->observed_column.table_name;
            return false;
        }

        if (it->second.observe_columns.find(notify_cell->observed_column) == it->second.observe_columns.end()) {
            LOG(WARNING) << "column not found. cf: " << notify_cell->observed_column.family
                << "  qu: " << notify_cell->observed_column.qualifier;
            return false;
        }

        if (it->second.observe_columns[notify_cell->observed_column].size() == 0) {
                        LOG(WARNING) << "no match observers, table=" << notify_cell->observed_column.table_name <<
                " cf=" << notify_cell->observed_column.family << " qu=" << notify_cell->observed_column.qualifier;
            return false;
        }

        std::set<Observer*>& observer_set = (*table_observe_info_read_copy)[notify_cell->observed_column.table_name].observe_columns[notify_cell->observed_column];

        // only gtxn check ack
        if ((*observer_set.begin())->GetTransactionType() == kGlobalTransaction
            && !CheckConflictOnAckColumn(notify_cell, observer_set)) {
            LOG(WARNING) <<  "Ack failed ! row=" << notify_cell->row << " cf=" << notify_cell->observed_column.family <<
                " qu=" << notify_cell->observed_column.qualifier;;
            return false;
        }
        // every column may have more than one observers
        for (auto observer = observer_set.begin(); observer != observer_set.end(); ++observer) {

        	transaction_threads_->AddTask( [=] (int64_t) {
                total_counter_.Inc();
                std::unique_ptr<Notification> notification(GetNotification(notify_cell->transaction)); 
        		tera::ErrorCode err = (*observer)->OnNotify(notify_cell->transaction, tera_client_, notify_cell->observed_column.table_name,
                             	      notify_cell->observed_column.family, notify_cell->observed_column.qualifier,
                                      notify_cell->row, notify_cell->value, notify_cell->timestamp, notification.get());
                if (err.GetType() != tera::ErrorCode::kOK) {
                    LOG(WARNING) << "OnNotify failed! reason: " << err.GetReason();
                    fail_counter_.Inc();
                }
        	});
        }

    } else {
        LOG(WARNING) << "[read failed] table=" << notify_cell->table->GetName() << " cf=" << notify_cell->observed_column.family <<
            " qu=" << notify_cell->observed_column.qualifier << " row=" << notify_cell->row <<
            " err=" << row_reader->GetError().GetType() << row_reader->GetError().GetReason();
        return false;
    }

    return true;
}

void ScannerImpl::GetObserveColumns(const std::string& table_name, std::set<Column>* columns) {
    columns->clear();

    std::shared_ptr<std::map<std::string, TableObserveInfo>> table_observe_info_read_copy;
    {
        MutexLock locker(&table_mutex_);
        // shared_ptr ref +1
        table_observe_info_read_copy = table_observe_info_;
    }

    for (auto it : (*table_observe_info_read_copy)[table_name].observe_columns) {
    	columns->insert(it.first);
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
    while (true) {
        {
            MutexLock locker(&quit_mutex_);
            if (quit_) {
                return;
            }
            cond_.TimeWaitInUs(kObserverWaitTime);
        }
        LOG(INFO) << "[Observer Profiling Info]  total: " << total_counter_.Get() <<
            " failed: " << fail_counter_.Get() << "  transaction pending: " <<
            transaction_threads_->PendingNum();
        total_counter_.Clear();
        fail_counter_.Clear();
    }
}

bool ScannerImpl::CheckConflictOnAckColumn(std::shared_ptr<NotifyCell> notify_cell,
                                           const std::set<Observer*>& observers) {
    VLOG(12) <<"[time] Check ACK start. [cf:qu] " << notify_cell->observed_column.family
             << notify_cell->observed_column.qualifier;
    bool is_collision = false;
    std::vector<std::string> ack_qualifier_list;
    std::string ack_qualifier_prefix = GetAckQualifierPrefix(notify_cell->observed_column.family,
                                                             notify_cell->observed_column.qualifier);

    // use transaction to read column Ack
    std::unique_ptr<tera::Transaction> row_transaction(notify_cell->table->StartRowTransaction(notify_cell->row));

    // read Acks
    std::unique_ptr<tera::RowReader> row_reader(notify_cell->table->NewRowReader(notify_cell->row));
    for (auto it : observers) {
        std::string ack_qualifier = GetAckQualifier(ack_qualifier_prefix, it->GetObserverName());
        ack_qualifier_list.push_back(ack_qualifier);

        row_reader->AddColumn(notify_cell->observed_column.family, ack_qualifier);
    }
    row_transaction->Get(row_reader.get());
    if (tera::ErrorCode::kOK == row_reader->GetError().GetType()) {
        while (!row_reader->Done()) {
            int64_t latest_observer_start_ts = 0;
            if (!StringToNumber(row_reader->Value(), &latest_observer_start_ts)) {
                LOG(ERROR) << "Convert string to timestamp failed! String: " << row_reader->Value() <<
                    " row=" << notify_cell->row << " cf=" << notify_cell->observed_column.family <<
                    " qu=" << notify_cell->observed_column.qualifier;
                is_collision = true;
                break;
            }

            // collision check： ack ts later than notify ts &&
            if (latest_observer_start_ts >= notify_cell->timestamp &&
                notify_cell->transaction->GetStartTimestamp() - latest_observer_start_ts
                < FLAGS_observer_ack_conflict_timeout) {
                // time too short, collisision, ignore

                is_collision = true;
                LOG(INFO) << "own collision. row=" << notify_cell->row <<
                    " cf=" << notify_cell->observed_column.family << " qu=" <<
                    notify_cell->observed_column.qualifier <<
                    ", latest observer start_ts=" << latest_observer_start_ts <<
                    ", observer start_ts=" << notify_cell->transaction->GetStartTimestamp() <<
                    ", data commit_ts=" << notify_cell->timestamp;
                break;

            }
            row_reader->Next();
        }
    } else {
        LOG(INFO) << "read Acks failed, err=" << row_reader->GetError().GetReason() <<
            " row=" << notify_cell->row << " cf=" << notify_cell->observed_column.family <<
            " qu=" << notify_cell->observed_column.qualifier;
    }

    if (!is_collision) {
        // set Acks
        std::unique_ptr<tera::RowMutation> mutation(notify_cell->table->NewRowMutation(notify_cell->row));
        for (size_t idx = 0; idx < ack_qualifier_list.size(); ++idx) {
            mutation->Put(notify_cell->observed_column.family, ack_qualifier_list[idx],
                std::to_string(notify_cell->transaction->GetStartTimestamp()));
        }
        row_transaction->ApplyMutation(mutation.get());
        notify_cell->table->CommitRowTransaction(row_transaction.get());
        if (row_transaction->GetError().GetType() != tera::ErrorCode::kOK) {
            LOG(INFO) << "write Ack failed, row=" << notify_cell->row << " err=" <<
                row_transaction->GetError().GetReason() << " cf=" <<
                notify_cell->observed_column.family << " qu=" <<
                notify_cell->observed_column.qualifier;
            is_collision = true;
        }
    }
    VLOG(12) <<"[time] Check ACK finish. [cf:qu] " << notify_cell->observed_column.family
             << notify_cell->observed_column.qualifier;

    return !is_collision;
}

std::string ScannerImpl::GetAckQualifierPrefix(const std::string& family,
                                               const std::string& qualifier) const {
    return family + ":" + qualifier;
}

std::string ScannerImpl::GetAckQualifier(const std::string& prefix,
                                         const std::string& observer_name) const {
    return prefix + "+ack_" + observer_name;
}

bool ScannerImpl::TryLockRow(const std::string& table_name,
                             const std::string& row) const {
    VLOG(12) << "[time] trylock " << table_name << " " << row;
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

    if (!rowlock_client->TryLock(&request, &response)) {
        LOG(ERROR) << "TryLock rpc fail, row: " << row;
        return false;
    }
    if (response.lock_status() != kLockSucc) {
        LOG(INFO) << "Lock row fail, row: " << row;
        return false;
    }
    VLOG(12) << "[time] trylock finish " << table_name << " " << row;
    return true;
}

bool ScannerImpl::CheckTransactionTypeLegalForTable(TransactionType type,
                                                    TransactionType table_type) {
    if (type == table_type) {
        return true;
    }

    if (type == kNoneTransaction && table_type ==  kSingleRowTransaction) {
        return true;
    }

    return false;
}

TransactionType ScannerImpl::GetTableTransactionType(tera::Table* table) {
    tera::ErrorCode err;
    TableImpl* table_impl(dynamic_cast<ClientImpl*>(tera_client_)->OpenTableInternal(table->GetName(), &err));
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

} // namespace observer
} // namespace tera
