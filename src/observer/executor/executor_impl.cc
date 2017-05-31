// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <functional>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include "executor_impl.h"

DECLARE_string(observer_tera_flag_file);
DECLARE_int32(observer_proc_thread_num);
DECLARE_int64(observer_proc_pending_num_max);
DECLARE_string(observer_notify_column_name);

namespace observer {

static tera::Client* g_tera_client = NULL;
static Mutex g_table_mutex;
static TableMap g_table_map;

Executor* Executor::NewExecutor() {
    return new ExecutorImpl();
}

ExecutorImpl::ExecutorImpl() : quit_(false), 
        proc_thread_pool_(new common::ThreadPool(FLAGS_observer_proc_thread_num)),
        scanner_(NULL) {
    observer_set_.clear();
    observer_map_.clear();
    reduce_column_map_.clear();
}

ExecutorImpl::~ExecutorImpl() {
}

bool ExecutorImpl::RegisterObserver(Observer* observer) {
    observer_set_.insert(observer);

    // init tera client
    tera::ErrorCode err;
    if (NULL == g_tera_client) {
        g_tera_client = tera::Client::NewClient(FLAGS_observer_tera_flag_file, &err);
        if (tera::ErrorCode::kOK != err.GetType()) {
            LOG(ERROR) << "init tera client [" << FLAGS_observer_tera_flag_file << "] failed";
            return false;
        }
    }

    ColumnMap& column_map = observer->GetColumnMap();
    ColumnMap::iterator it = column_map.begin();
    for (; it != column_map.end(); ++it) {
        MutexLock locker(&g_table_mutex);
        if (g_table_map.end() == g_table_map.find(it->first)) {
            // init table
            tera::Table* new_table = g_tera_client->OpenTable(it->first, &err);
            if (tera::ErrorCode::kOK != err.GetType()) {
                LOG(ERROR) << "open tera table [" << it->first << "] failed";
                return false;
            }
            LOG(INFO) << "open tera table [" << it->first << "] succ";

            // build map: <table_name, table>
            g_table_map[it->first] = new_table;
        }
        for (size_t idx = 0; idx < it->second.size(); ++idx) {
            // build map: <column, observers>
            observer_map_[it->second[idx]].push_back(observer);
            
            // build map: <table, columnset>
            reduce_column_map_[g_table_map[it->first]].insert(it->second[idx]);
        }
    }

    return true;
}

bool ExecutorImpl::Run() {
    if (0 == observer_map_.size()) {
        LOG(ERROR) << "no observer, please register observers first";
        return false;
    }

    // init scanner
    scanner_ = new Scanner(this);
    if (!scanner_->Init()) {
        LOG(ERROR) << "init Scanner failed";
        Quit();
        return false;
    }
   
    // init observers (user definition)
    for (ObserverSet::iterator it = observer_set_.begin(); it != observer_set_.end(); ++it) {
        (*it)->Init();
    }

    while (!quit_) {
        ThisThread::Sleep(1);
    }
    
    // close observers (user definition)
    for (ObserverSet::iterator it = observer_set_.begin(); it != observer_set_.end(); ++it) {
        (*it)->Close();
    }
    
    // close scanner
    if (scanner_ != NULL) {
        scanner_->Close();
        delete scanner_;
    }
    
    // close table
    for (TableMap::iterator it = g_table_map.begin(); it != g_table_map.end(); ++it) {
        if (it->second != NULL) {
            delete it->second;
        }
    }

    // close tera client
    if (g_tera_client != NULL) {
        delete g_tera_client; 
    }

    return true;
}

bool ExecutorImpl::Process(TuplePtr tuple) {
    // find observers
    ObserverMap::iterator it = observer_map_.find(tuple->observed_column);
    if (observer_map_.end() == it) {
        LOG(ERROR) << "no match observers, table=" << tuple->observed_column.table_name << 
            " cf=" << tuple->observed_column.family << " qu=" << tuple->observed_column.qualifier;
        return false;
    } 
    // notify observers
    for (size_t idx = 0; idx < it->second.size(); ++idx) {
        proc_thread_pool_->AddTask(std::bind(&ExecutorImpl::DoNotify, this, tuple, it->second[idx]));
    }

    return true;
}

bool ExecutorImpl::DoNotify(TuplePtr tuple, Observer* observer) {
    return observer->OnNotify(tuple->t, 
            tuple->table, 
            tuple->row, 
            tuple->observed_column, 
            tuple->value, 
            tuple->timestamp);
}

bool ExecutorImpl::ProcTaskPendingFull() {
    return (proc_thread_pool_->PendingNum() > FLAGS_observer_proc_pending_num_max);
}

bool ExecutorImpl::SetOrClearNotification(ColumnList& columns, 
        const std::string& row, 
        int64_t timestamp, 
        NotificationType type) {
    bool ret = true;
    tera::ErrorCode err;
    // reduce columns
    ColumnReduceMap reduce_map;
    for (size_t idx = 0; idx < columns.size(); ++idx) {
        MutexLock locker(&g_table_mutex);
        TableMap::iterator it = g_table_map.find(columns[idx].table_name);
        if (g_table_map.end() == it) {
            tera::Table* new_table = g_tera_client->OpenTable(columns[idx].table_name, &err);
            if (tera::ErrorCode::kOK != err.GetType()) {
                LOG(ERROR) << "open table failed, name=" << columns[idx].table_name << 
                    " err=" << err.GetReason();
                return false;
            }
            g_table_map[columns[idx].table_name] = new_table;
        }
        reduce_map[it->second].insert(columns[idx]);
    }
    // set or clear notification columns
    ColumnReduceMap::iterator table_it = reduce_map.begin();
    for (; table_it != reduce_map.end(); ++table_it) {
        tera::RowMutation* mutation = table_it->first->NewRowMutation(row);
        ColumnSet::iterator column_it = table_it->second.begin();
        for (; column_it != table_it->second.end(); ++column_it) {
            std::string notify_qualifier = column_it->family + "+" + column_it->qualifier;
            if (kSetNotification == type) {
                mutation->Put(FLAGS_observer_notify_column_name, notify_qualifier, "1", timestamp);
            } else if (kClearNotification == type) {
                // 删除t->StartTimestamp之前的通知标记, 避免通知标记发生变更引起数据丢失
                mutation->DeleteColumns(FLAGS_observer_notify_column_name, notify_qualifier, timestamp);
            } else {
                LOG(ERROR) << "error notification type=" << type;
            }
        }
        table_it->first->ApplyMutation(mutation);
        if (mutation->GetError().GetType() != tera::ErrorCode::kOK) {
            LOG(ERROR) << "set or clear notification failed, err=" << mutation->GetError().GetReason();
            ret = false;
        }
        delete mutation;
    }
    return ret;
}

} // namespace observer
