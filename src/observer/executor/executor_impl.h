// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef OBSERVER_EXECUTOR_IMPL_H_
#define OBSERVER_EXECUTOR_IMPL_H_

#include <set>
#include "common/base/scoped_ptr.h"
#include "common/thread_pool.h"
#include "common/this_thread.h"
#include "tuple.h"
#include "scanner.h"
#include "observer/executor.h"

namespace observer {

enum NotificationType {
    kSetNotification = 1,
    kClearNotification = 2,
};

typedef std::set<Observer*> ObserverSet;
typedef std::vector<Observer*> ObserverList;

// <Column, ObserverList>
typedef std::map<Column, ObserverList> ObserverMap;
// <TableName, Table>
typedef std::map<std::string, tera::Table*> TableMap;
// <Table, ObserveColumnList>
typedef std::map<tera::Table*, ColumnSet> ColumnReduceMap;

class Scanner;

class ExecutorImpl : public Executor {
public:
    ExecutorImpl();
    virtual ~ExecutorImpl();

    // 注册需要运行的Observer
    virtual bool RegisterObserver(Observer* observer);
    
    // 启动接口
    virtual bool Run();

    bool Process(TuplePtr tuple);
    
    bool ProcTaskPendingFull();

    // 进程退出
    void Quit() {
        quit_ = true;
    }

    bool GetQuit() const {
        return quit_;
    }
    
    ColumnReduceMap& GetColumnReduceMap() {
        return reduce_column_map_;
    }
    
    static bool SetOrClearNotification(ColumnList& columns,
        const std::string& row,
        int64_t timestamp, 
        NotificationType type);

private:
    ExecutorImpl(const ExecutorImpl&);
    void operator=(const ExecutorImpl&);

    bool DoNotify(TuplePtr tuple, Observer* observer);

private:
    volatile bool quit_;
    scoped_ptr<common::ThreadPool> proc_thread_pool_;
    Scanner* scanner_;
    
    ObserverSet observer_set_;
    // 每个列对应多个Observer
    ObserverMap observer_map_;
    // 每个table对应多个被观察列
    ColumnReduceMap reduce_column_map_;
};

} // namespace observer

#endif  // OBSERVER_EXECUTOR_H_
