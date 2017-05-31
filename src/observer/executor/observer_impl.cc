// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/observer.h"
#include "executor_impl.h"

namespace observer {

Observer::Observer(const std::string& observer_name, 
        ColumnList& observed_columns): observer_name_(observer_name) {
    for (size_t idx = 0; idx < observed_columns.size(); ++idx) {
        column_map_[observed_columns[idx].table_name].push_back(observed_columns[idx]);
    } 
}

Observer::~Observer() {
}

bool Observer::OnNotify(tera::Transaction* t, 
        tera::Table* table, 
        const std::string& row, 
        const Column& column, 
        const std::string& value, 
        int64_t timestamp) {
    return true;
}

bool Observer::Init() {
    return true;
}
    
bool Observer::Close() {
    return true;
}

std::string Observer::GetName() const {
    return observer_name_;
}

ColumnMap& Observer::GetColumnMap() {
    return column_map_;
}

bool Observer::Ack(ColumnList& columns, const std::string& row, int64_t timestamp) {
    return ExecutorImpl::SetOrClearNotification(columns, row, timestamp, kClearNotification);
}

bool Observer::Notify(ColumnList& columns, const std::string& row, int64_t timestamp) {
    return ExecutorImpl::SetOrClearNotification(columns, row, timestamp, kSetNotification);
}

} // namespace observer
