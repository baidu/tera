// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// A RowReader describes a Get operation for the specified row and returns
// filtered cells from server.
// User should call RowReader::GetError() to check if operation is successful.

#ifndef  TERA_READER_H_
#define  TERA_READER_H_

#include <stdint.h>
#include <map>
#include <set>
#include <string>

#include "error_code.h"

#pragma GCC visibility push(default)
namespace tera {

class RowReaderImpl;
class Transaction;
class Table;
class RowReader {
public:

    // Set request filter conditions. If none, returns all cells of this row.
    // Only returns cells in these columns.
    virtual void AddColumnFamily(const std::string& family) = 0;
    virtual void AddColumn(const std::string& family, const std::string& qualifier) = 0;
    // Set the maximum number of versions of each column.
    virtual void SetMaxVersions(uint32_t max_version) = 0;

    // If set, only returns cells of which update timestamp is within [ts_start, ts_end].
    virtual void SetTimeRange(int64_t ts_start, int64_t ts_end) = 0;

    // Access received data.
    // Use RowReader as an iterator. While Done() returns false, one cell is
    // accessible.
    virtual bool Done() = 0;
    virtual void Next() = 0;
    // Access present cell.
    // Only RowKey&Value are effective in key-value storage.
    virtual const std::string& RowKey() = 0;
    virtual std::string Value() = 0;
    virtual std::string Family() = 0;
    virtual std::string Qualifier() = 0;
    virtual int64_t Timestamp() = 0;

    // Returns all cells in this row as a nested std::map.
    typedef std::map<int64_t, std::string> TColumn;
    typedef std::map<std::string, TColumn> TColumnFamily;
    typedef std::map<std::string, TColumnFamily> TRow;
    virtual void ToMap(TRow* rowmap) = 0;

    // The status of this row reader. Returns kOK on success and a non-OK
    // status on error.
    virtual ErrorCode GetError() = 0;

    // Users are allowed to register a callback/context two-tuples that
    // will be invoked when this reader is finished.
    typedef void (*Callback)(RowReader* param);
    virtual void SetCallBack(Callback callback) = 0;
    virtual void SetContext(void* context) = 0;
    virtual void* GetContext() = 0;
    virtual void SetTimeOut(int64_t timeout_ms) = 0;

    // Get column filters map.
    typedef std::map<std::string, std::set<std::string> >ReadColumnList;
    virtual const ReadColumnList& GetReadColumnList() = 0;

    // DEVELOPING
    virtual void SetSnapshot(uint64_t snapshot_id) = 0;
    virtual uint64_t GetSnapshot() = 0;
    virtual bool IsFinished() const = 0;

    // DEPRECATED
    // Use RowKey() instead.
    virtual const std::string& RowName() = 0;
    // Use SetTimeRange(ts, ts) instead.
    virtual void SetTimestamp(int64_t ts) = 0;
    virtual int64_t GetTimestamp() = 0;
    // Use new ToMap(TRow* rowmap) instead.
    typedef std::map< std::string, std::map<int64_t, std::string> > Map;
    virtual void ToMap(Map* rowmap) = 0;
    // Use 'Family() + ":" + Qualifier()' instead.
    virtual std::string ColumnName() = 0;
    virtual int64_t ValueInt64() = 0;
    virtual void SetAsync() = 0;
    virtual uint32_t GetReadColumnNum() = 0;

    RowReader() {};
    virtual ~RowReader() {};

    // Set the the max qualifiers of each column family when read this row
    // This is useful when a column family contains too many qualifiers
    // If this value is not set, the default value is std::numeric_limits<uint64_t>::max()
    virtual void SetMaxQualifiers(uint64_t max_qualifiers) = 0;

private:
    RowReader(const RowReader&);
    void operator=(const RowReader&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_READER_H_
