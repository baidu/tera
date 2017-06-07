// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef OBSERVER_OBSERVER_H_
#define OBSERVER_OBSERVER_H_

#include <string>
#include <vector>
#include <map>
#include "tera.h"

#pragma GCC visibility push(default)
namespace observer {

struct Column {
    std::string table_name;
    std::string family;
    std::string qualifier;  
    bool operator<(const Column& other) const {
        std::string str1 = table_name + family + qualifier;
        std::string str2 = other.table_name + other.family + other.qualifier;
        return str1 < str2;
    }
};

typedef std::vector<Column> ColumnList;
// <TableName, ColumnList>
typedef std::map<std::string, ColumnList> ColumnMap;

/// 基于Tera跨行事务, 实现大规模表格上增量实时触发计算框架
class Observer {
public:
    // 传入观察者唯一标示名以及被观察列
    Observer(const std::string& observer_name, ColumnList& observed_columns);
    virtual ~Observer();

    // 用户实现此接口拿到观察列上变化数据, 完成计算
    virtual bool OnNotify(tera::Transaction* t, 
            tera::Table* table, 
            const std::string& row, 
            const Column& column, 
            const std::string& value, 
            int64_t timestamp);

    // 用户实现此接口做初始化操作
    virtual bool Init();
    
    // 用户实现此接口做结束操作
    virtual bool Close();

    // 清除通知
    bool Ack(ColumnList& columns, const std::string& row, int64_t timestamp);

    // 设置通知, 触发下游observer
    bool Notify(ColumnList& columns, const std::string& row, int64_t timestamp);

    std::string GetName() const;
    ColumnMap& GetColumnMap();
private:
    Observer(const Observer&);
    void operator=(const Observer&);

private:
    std::string observer_name_;
    ColumnMap column_map_;
};

} // namespace observer
#pragma GCC visibility pop

#endif  // ONSERVER_OBSERVER_H_
