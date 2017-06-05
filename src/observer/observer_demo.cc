// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <signal.h>
#include <glog/logging.h>
#include "observer/executor.h"
#include "observer/observer.h"

namespace observer {
namespace demo {

/// Parser Worker ///
class Parser : public observer::Observer {
public:
    Parser(const std::string& observer_name, 
            observer::ColumnList& observed_columns): 
        observer::Observer(observer_name, observed_columns) {}
    virtual ~Parser() {}
    virtual bool OnNotify(tera::Transaction* t, 
            tera::Table* table, 
            const std::string& row, 
            const observer::Column& column, 
            const std::string& value, 
            int64_t timestamp);
};

bool Parser::OnNotify(tera::Transaction* t, 
            tera::Table* table, 
            const std::string& row, 
            const observer::Column& column, 
            const std::string& value, 
            int64_t timestamp) {
    LOG(INFO) << "[Notify Parser] table:family:qualifer=" << 
        column.table_name << ":" << column.family << ":" << 
        column.qualifier << " row=" << row << 
        " value=" << value << " timestamp=" << timestamp;

    // todo: read other columns ...
    // write ForwordIndex column
    tera::RowMutation* mutation = table->NewRowMutation(row);
    mutation->Put("Data", "ForwordIndex", "FIValue_" + row);
    t->ApplyMutation(mutation);
    // t->Notify() 
    // t->Ack()
    t->Commit();
    delete mutation;
   
    // notify downstream observers, equal to t->Notify()
    observer::ColumnList notify_columns;
    observer::Column c1 = {"observer_test_table", "Data", "ForwordIndex"};
    notify_columns.push_back(c1);
    // GetStartTimestamp接口暂不支持
    // Notify(notify_columns, row, t->GetStartTimestamp());
    Notify(notify_columns, row, -1);
    
    // clear notification, equal to t->Ack()
    observer::ColumnList ack_columns;
    observer::Column c2 = {"observer_test_table", "Data", "Page"};
    observer::Column c3 = {"observer_test_table", "Data", "Link"};
    // ...
    ack_columns.push_back(c2);
    ack_columns.push_back(c3);
    // Ack(ack_columns, row, t->GetStartTimestamp());
    Ack(ack_columns, row, -1);

    return true;
}

/// Builder Worker ///
class Builder : public observer::Observer {
public:
    Builder(const std::string& observer_name, 
            observer::ColumnList& observed_columns): 
        observer::Observer(observer_name, observed_columns) {}
    virtual ~Builder() {}
    virtual bool OnNotify(tera::Transaction* t, 
            tera::Table* table, 
            const std::string& row, 
            const observer::Column& column, 
            const std::string& value, 
            int64_t timestamp);
};

bool Builder::OnNotify(tera::Transaction* t, 
            tera::Table* table, 
            const std::string& row, 
            const observer::Column& column, 
            const std::string& value, 
            int64_t timestamp) {
    LOG(INFO) << "[Notify Builder] table:family:qualifer=" << column.table_name << ":" << 
        column.family << ":" << column.qualifier << " row=" << row << 
        " value=" << value << " timestamp=" << timestamp;
    
    // todo: read other columns ...
    // todo: write InvertIndex columns ...
    // t->Notify() 
    // t->Ack()
    // t->Commit(); 
    
    // clear notification, equal to t->Ack()
    observer::ColumnList ack_columns;
    observer::Column c1 = {"observer_test_table", "Data", "ForwordIndex"};
    ack_columns.push_back(c1);
    // Ack(ack_columns, row, t->GetStartTimestamp());
    Ack(ack_columns, row, -1);

    return true;
}

} // namespace demo
} // namespace observer

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    
    // 创建observer
    // 传入唯一标示名和需要观察的列(table_name+family+qualifier)
    observer::ColumnList columns;
    observer::Column c1 = {"observer_test_table", "Data", "Page"};
    observer::Column c2 = {"observer_test_table", "Data", "Link"};
    columns.push_back(c1);
    columns.push_back(c2);
    observer::demo::Parser parser("Parser", columns);
    
    columns.clear();
    observer::Column c3 = {"observer_test_table", "Data", "ForwordIndex"};
    columns.push_back(c3);
    observer::demo::Builder builder("Builder", columns);

    // 注册并启动observers
    observer::Executor* executor = observer::Executor::NewExecutor();
    executor->RegisterObserver(&parser);
    executor->RegisterObserver(&builder);
    executor->Run();
    
    return 0;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
