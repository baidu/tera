// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_EXECUTOR_SCANNER_IMPL_H_
#define TERA_OBSERVER_EXECUTOR_SCANNER_IMPL_H_

#include <mutex>
#include <pthread.h>

#include "common/counter.h"
#include "common/mutex.h"
#include "common/semaphore.h"
#include "common/thread_pool.h"
#include "common/thread.h"
#include "common/this_thread.h"
#include "common/timer.h"
#include "observer/executor/notify_cell.h"
#include "observer/executor/observer.h"
#include "observer/executor/scanner.h"
#include "tera.h"

namespace tera {
namespace observer {

class Observer;
class KeySelector;

class ScannerImpl : public Scanner {
private:
    struct TableObserveInfo {
        std::map<Column, std::set<Observer*>> observe_columns;
        tera::Table* table;
        TransactionType type;
    };
    
    struct NotificationContext {
        std::shared_ptr<NotifyCell> notify_cell;
        ScannerImpl* scanner_impl;
        std::string ack_qualifier;
        std::shared_ptr<tera::Transaction> ack_transaction; // ValidateAckConfict transaction
        int64_t ts;
        NotificationContext() {
            ts = get_micros();
            VLOG(12) << "NotificationContext create " << ts;
        }
        ~NotificationContext() {
            VLOG(12) << "NotificationContext destory " << ts;
        }
    };

public:
    ScannerImpl();
    virtual ~ScannerImpl();

    virtual ErrorCode Observe(const std::string& table_name,
                              const std::string& column_family,
                              const std::string& qualifier,
                              Observer* observer);

    virtual bool Init();

    virtual bool Start();

    virtual void Exit();
    
    tera::Client* GetTeraClient() const;

    static ScannerImpl* GetInstance();

    void ValidateAckConfict(RowReader* ack_reader);
    void SetAckVersionCallBack(Transaction* ack_transaction);

private:
    void ScanTable();

    bool DoScanTable(tera::Table* table,
                     const std::set<Column>& column_set,
                     const std::string& start_key,
                     const std::string& end_key);

    void AsyncReadCell(std::shared_ptr<NotifyCell> notify_cell);
    void ValidateCellValue(RowReader* value_reader);

    bool ParseNotifyQualifier(const std::string& notify_qualifier,
                              std::string* data_family,
                              std::string* data_qualfier);

    void GetObserveColumns(const std::string& table_name, 
                           std::set<Column>* columns);

    tera::Table* GetTable(const std::string table_name);

    bool NextRow(tera::ResultStream* result_stream, 
                 const std::string& table_name, bool* finished, 
                 std::string* row, std::vector<Column>* notify_columns);

    void Profiling();

    void AsyncReadAck(std::shared_ptr<NotifyCell> notify_cell);
    std::string GetAckQualifierPrefix(const std::string& family, const std::string& qualifier) const;
    std::string GetAckQualifier(const std::string& prefix, const std::string& observer_name) const;
    bool TryLockRow(const std::string& table_name, 
                    const std::string& row) const;

    bool CheckTransactionTypeLegalForTable(TransactionType transaction_type, TransactionType table_type);
    TransactionType GetTableTransactionType(tera::Table* table);

    void ObserveCell(std::shared_ptr<NotifyCell> notify_cell);

    void PrepareNotifyCell(tera::Table* table,
                           const std::string& rowkey,
                           const std::set<Column>& observe_columns,
                           const std::vector<Column>& notify_columns, 
                           std::shared_ptr<AutoRowUnlocker> unlocker,
                           std::vector<std::shared_ptr<NotifyCell>>* notify_cells);

    void SetAckVersion(NotificationContext* ack_context);

private:
    mutable Mutex table_mutex_;
    std::unique_ptr<tera::Client> tera_client_;
    std::unique_ptr<KeySelector> key_selector_;

    // map<table name, table observe info:table ptr, map<column, observer>>
    std::shared_ptr<std::map<std::string, TableObserveInfo>> table_observe_info_;
    // This set stores unique user-define observer addresses. 
    // Release user-define observers when scanner destruct 
    std::set<Observer*> observers_;

    std::unique_ptr<common::ThreadPool> scan_table_threads_;
    std::unique_ptr<common::ThreadPool> observer_threads_;
    std::unique_ptr<common::ThreadPool> transaction_callback_threads_;

    // for quit
    std::atomic<bool> quit_; 

    common::Thread profiling_thread_;
    Counter total_counter_;
    Counter fail_counter_;
    common::Semaphore semaphore_;

    static ScannerImpl* scanner_instance_;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_EXECUTOR_SCANNER_IMPL_H_
