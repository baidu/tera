// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_OBSERVER_EXECUTOR_SCANNER_IMPL_H_
#define TERA_OBSERVER_EXECUTOR_SCANNER_IMPL_H_

#include <mutex>
#include <pthread.h>

#include "common/counter.h"
#include "common/mutex.h"
#include "common/thread_pool.h"
#include "common/thread.h"
#include "common/this_thread.h"
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

private:
    void ScanTable();

    bool DoScanTable(tera::Table* table,
                     const std::set<Column>& column_set,
                     const std::string& start_key,
                     const std::string& end_key);

    bool DoReadValue(std::shared_ptr<NotifyCell> notify_cell);

    bool ParseNotifyQualifier(const std::string& notify_qualifier,
                              std::string* data_family,
                              std::string* data_qualfier);

    void GetObserveColumns(const std::string& table_name, 
                           std::set<Column>* column_set);

    tera::Table* GetTable(const std::string table_name);

    bool NextRow(const std::set<Column>& columns, tera::ResultStream* result_stream, 
                 const std::string& table_name, bool* finished, 
                 std::string* row, std::vector<Column>* vec_col);

    void Profiling();

    bool CheckConflictOnAckColumn(std::shared_ptr<NotifyCell> notify_cell, 
                                  const std::set<Observer*>& observers);
    std::string GetAckQualifierPrefix(const std::string& family, const std::string& qualifier) const;
    std::string GetAckQualifier(const std::string& prefix, const std::string& observer_name) const;
    bool TryLockRow(const std::string& table_name, 
                    const std::string& row) const;

    bool CheckTransactionTypeLegalForTable(TransactionType type, TransactionType table_type);
    TransactionType GetTableTransactionType(tera::Table* table);

private:
    mutable Mutex table_mutex_;
    tera::Client* tera_client_;
    std::unique_ptr<KeySelector> key_selector_;

    // map<table name, table observe info:table ptr, map<column, observer>>
    std::shared_ptr<std::map<std::string, TableObserveInfo>> table_observe_info_;
    // This set stores unique user-define observer addresses. 
    // Release user-define observers when scanner destruct 
    std::set<Observer*> observers_;

    std::unique_ptr<common::ThreadPool> scan_table_threads_;
    std::unique_ptr<common::ThreadPool> transaction_threads_;

    // for quit
    bool quit_; 
    mutable Mutex quit_mutex_;
    common::CondVar cond_;

    common::Thread profiling_thread_;
    Counter total_counter_;
    Counter fail_counter_;

    static ScannerImpl* scanner_instance_;
};

} // namespace observer
} // namespace tera

#endif  // TERA_OBSERVER_EXECUTOR_SCANNER_IMPL_H_
