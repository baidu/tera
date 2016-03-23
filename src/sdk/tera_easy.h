// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  TERA_TERA_EASY_H_
#define  TERA_TERA_EASY_H_

#include <stdint.h>

#include <map>
#include <string>

#pragma GCC visibility push(default)

namespace teraeasy {

typedef std::string Key;
typedef std::string ColumnKey;
typedef std::string Value;
typedef int64_t Timestamp;
typedef std::map<Timestamp, Value> Column;
typedef std::map<ColumnKey, Column> Record;
typedef std::pair<Key, Record> KVPair;

class Table {
public:
    Table() {}

    virtual ~Table() {}

    virtual bool Read(const Key& row_key, Record* record) = 0;

    virtual bool Write(const Key& row_key, const Record& record) = 0;

    virtual void Flush() = 0;

    virtual bool Delete(const Key& row_key) = 0;

    virtual bool SetScanner(const Key& start, const Key& end) = 0;

    virtual bool NextPair(KVPair* kv_pair) = 0;

private:
    Table(const Table&);
    void operator=(const Table&);
};

Table* OpenTable(const std::string& table_name, const std::string& conf_path = "");
}

#pragma GCC visibility pop

#endif  // TERA_TERA_EASY_H_
