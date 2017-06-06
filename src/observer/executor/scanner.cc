// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <functional>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include "scanner.h"

DECLARE_int32(observer_scan_thread_num);
DECLARE_bool(observer_scan_async_switch);
DECLARE_int32(observer_read_thread_num);
DECLARE_string(observer_notify_column_name);

namespace observer {

Scanner::Scanner(ExecutorImpl* executor_impl) : executor_impl_(executor_impl), 
    read_thread_pool_(new common::ThreadPool(FLAGS_observer_read_thread_num)) {
}

Scanner::~Scanner() {
}

bool Scanner::Init() {
    ColumnReduceMap& column_map = executor_impl_->GetColumnReduceMap();
    if (FLAGS_observer_scan_thread_num < (int)column_map.size()) {
        LOG(ERROR) << "some tables can't be scaned, scanner_num=" << 
            FLAGS_observer_scan_thread_num << " table_num=" << column_map.size();
    }

    // 启动scan线程
    ColumnReduceMap::iterator it = column_map.begin();
    scan_thread_list_.resize(FLAGS_observer_scan_thread_num);
    for (size_t idx = 0; idx < scan_thread_list_.size(); ++idx) {
        scan_thread_list_[idx].Start(std::bind(&Scanner::DoScan, this, it->first, it->second));
        if (column_map.end() == ++it) {
            it = column_map.begin();
        }
    }
    
    return true;
}

bool Scanner::Close() {
    for (size_t idx = 0; idx < scan_thread_list_.size(); ++idx) {
        scan_thread_list_[idx].Join();
    }
    return true;
}

bool Scanner::ParseNotifyQualifier(const std::string& notify_qualifier, 
        std::string* data_family, 
        std::string* data_qualifier) {
    // <notify_qualifier> = <data_family>+<data_qualifier>
    std::string delim = "+";
    std::vector<std::string> frags;
    std::size_t pos = std::string::npos;
    std::size_t start_pos = 0;
    std::string frag = "";
    while (std::string::npos != (pos = notify_qualifier.find(delim, start_pos))) {
        frag = notify_qualifier.substr(start_pos, pos - start_pos);
        frags.push_back(frag);
        start_pos = pos + 1;
    }
    std::size_t str_len = notify_qualifier.length();
    if (start_pos <= str_len - 1) {
        frag = notify_qualifier.substr(start_pos, str_len - start_pos);
        frags.push_back(frag);
    }

    if (2 != frags.size()) {
        return false;
    }
    *data_family = frags[0];
    *data_qualifier = frags[1];
    return true;
}

void Scanner::DoScan(tera::Table* table, ColumnSet& column_set) {
    std::string start_key;
    RandomStartKey(table, &start_key);
    while (true) {
        if (executor_impl_->GetQuit()) {
            return;
        }
        if (!ScanTable(table, column_set, start_key, "")) {
            // scan失败重新随机选startkey
            RandomStartKey(table, &start_key);
        } else {
            // scan正常结束从表头继续
            start_key = "";
        }
        ThisThread::Sleep(1);
    }
}

bool Scanner::ScanTable(tera::Table* table, 
        ColumnSet& column_set,
        const std::string& start_key, 
        const std::string& end_key) {
    bool ret = true;
    tera::ScanDescriptor desc(start_key);
    desc.SetAsync(FLAGS_observer_scan_async_switch);
    // Notify列存储在单独lg
    desc.AddColumnFamily(FLAGS_observer_notify_column_name);
    tera::ErrorCode err;
    tera::ResultStream* result_stream = table->Scan(desc, &err);
    if (NULL == result_stream || tera::ErrorCode::kOK != err.GetType()) {
        LOG(ERROR) << "table scan init failed";
        return false;
    }
    while (!result_stream->Done(&err)) {
        if (executor_impl_->GetQuit()) {
            ret = true;
            break;
        }
        if (tera::ErrorCode::kOK != err.GetType()) {
            LOG(ERROR) << "table scanning failed";
            ret = false;
            break;
        }

        // 控制scanner给observer发送数据的速度
        while (executor_impl_->ProcTaskPendingFull()) {
            if (executor_impl_->GetQuit()) {
                return true;
            }
            ThisThread::Sleep(1);
        }

        // todo: try lock row

        std::string ob_family;
        std::string ob_qualifier;
        std::string rowkey = result_stream->RowName();
        // 遍历cell
        while (result_stream->RowName() == rowkey) {
            if (ParseNotifyQualifier(result_stream->Qualifier(), &ob_family, &ob_qualifier)) {
                Column ob_column = {table->GetName(), ob_family, ob_qualifier};
                if (column_set.end() != column_set.find(ob_column)) {
                    TuplePtr tuple(new Tuple());
                    // 创建跨行事务
                    tuple->t = tera::NewTransaction();
                    tuple->table = table;
                    tuple->row = rowkey;
                    tuple->observed_column = ob_column;
                    read_thread_pool_->AddTask(std::bind(&Scanner::DoReadValue, this, tuple));
                } else {
                    LOG(ERROR) << "miss observed column, table_name" << table->GetName() << 
                        " cf=" << ob_family << " qu=" << ob_qualifier;
                }
            } else {
                LOG(ERROR) << "parse notify qualifier failed: " << result_stream->Qualifier();
            }
            
            result_stream->Next();
            if (result_stream->Done(&err)) {
                break;
            }
        }
    }
    delete result_stream;
    return ret;
}

bool Scanner::DoReadValue(TuplePtr tuple) { 
    bool ret = true;
    tera::RowReader* row_reader = tuple->table->NewRowReader(tuple->row);
    row_reader->AddColumn(tuple->observed_column.family, tuple->observed_column.qualifier);
    // 事务读
    tuple->t->Get(row_reader);
    if (tera::ErrorCode::kOK == row_reader->GetError().GetType()) {
        tuple->value = row_reader->Value();
        tuple->timestamp = row_reader->Timestamp();
        // 触发observer计算
        executor_impl_->Process(tuple);
    } else {
        LOG(ERROR) << "[read failed] cf=" << tuple->observed_column.family << 
            " qu=" << tuple->observed_column.qualifier << " row=" << tuple->row << 
            " err=" << row_reader->GetError().GetReason();
        ret = false;
    }
    delete row_reader;
    return ret;
}

bool Scanner::RandomStartKey(tera::Table* table, std::string* Key) {
    // todo
    *Key = "";
    return true;
}

} // namespace observer
