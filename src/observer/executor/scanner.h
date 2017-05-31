// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef OBSERVER_SCANNER_H_
#define OBSERVER_SCANNER_H_

#include "common/thread.h"
#include "executor_impl.h"

namespace observer {

class ExecutorImpl;

class Scanner {
public:
    Scanner(ExecutorImpl* executor_impl);
    ~Scanner();

    bool Init();

    bool Close();
    
    // 执行scan操作
    void DoScan(tera::Table* table, ColumnSet& column_set);

private:
    Scanner(const Scanner&);
    void operator=(const Scanner&);

    // 数据列family+qualifier构成通知列的qualifier
    bool ParseNotifyQualifier(const std::string& notify_qualifier, 
            std::string* data_family, 
            std::string* data_qualfier);
    
    // table的一次scan
    bool ScanTable(tera::Table* table, 
            ColumnSet& column_set,
            const std::string& start_key, 
            const std::string& end_key);
    
    bool DoReadValue(TuplePtr tuple);
    
    bool RandomStartKey(tera::Table* table, std::string* Key);

private:
    ExecutorImpl* executor_impl_;
    std::vector<common::Thread> scan_thread_list_;
    scoped_ptr<common::ThreadPool> read_thread_pool_;
};

} // namespace observer

#endif  // OBSERVER_SCANNER_H_
