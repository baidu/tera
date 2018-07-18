// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SCAN_H_
#define  TERA_SCAN_H_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "error_code.h"

#pragma GCC visibility push(default)
namespace tera {

// Return a heap-allocated iterator over the contents of the table.
// User must call Done() to check whether scan job is finished.
//
// Caller should delete iterator when it is no longer needed.
// The returned iterator should be deleted before this table is deleted.
class ResultStream {
public:
    ResultStream() {}
    virtual ~ResultStream() {}
    // Check wether iterator was positioned at the end, and return error code for failure check.
    virtual bool Done(ErrorCode* err = NULL) = 0;

    // Moves to next cell. After this call, Done() is true iff
    // the iterator was not positioned at the last cell in the source or scan error occurs.
    virtual void Next() = 0;

    // Return the row key name in current cell. The current cell's content changes only until
    // the next modification of the iterator.
    virtual std::string RowName() const = 0;
    // Return column family in current cell.
    virtual std::string Family() const = 0;
    // Return qualifier in current cell.
    virtual std::string Qualifier() const = 0;
    // Return timestamp in current cell.
    virtual int64_t Timestamp() const = 0;
    // Return value in current cell.
    virtual std::string Value() const = 0;
    virtual int64_t ValueInt64() const = 0;

    // DEPRECATED
    virtual bool LookUp(const std::string& row_key) = 0;
    // Return column in current cell, which looks like cf:qualifier.
    // Use Family():Qualifier() instead.
    virtual std::string ColumnName() const = 0;

private:
    ResultStream(const ResultStream&);
    void operator=(const ResultStream&);
};

class ScanDescImpl;
// Describe a scan job in tera client endian. Control scan behaviour.
class ScanDescriptor {
public:
    // 'rowkey' is the start row key in the scan job.
    ScanDescriptor(const std::string& rowkey);
    ~ScanDescriptor();
    // the end row key in the scan job, which means scan all keys less than the end row key.
    // Not required.
    void SetEnd(const std::string& rowkey);

    // Set target column family for the scan result,
    // which likes the SQL statement (SELECT cf1, cf2, ..., cfn From Table).
    void AddColumnFamily(const std::string& cf);

    // Set target column for the scan result,
    // which likes the SQL statement (SELECT cf1:qu, cf2:qu, ..., cfn:qu From Table).
    void AddColumn(const std::string& cf, const std::string& qualifier);

    // Set max version number per column.
    void SetMaxVersions(int32_t versions);

    // Set the the max qualifiers of each column family when read this row
    // This is useful when a column family contains too many qualifiers
    // If this value is not set, the default value is std::numeric_limits<uint64_t>::max()
    void SetMaxQualifiers(uint64_t max_qualifiers);

    // Set time range for the scan result,
    // which likes the SQL statement (SELECT * from Table WHERE timestamp in [ts_start, ts_end]).
    // Return the newest value first.
    void SetTimeRange(int64_t ts_end, int64_t ts_start);

    // Set batch scan mode, which largely speeds up scan task.
    void SetAsync(bool async);
    // Test the scan jobs, whether in batch scan mode.
    bool IsAsync() const;

    // Set timeout for each internal scan jobs, which avoids long-term scan jobs to trigger rpc's timeout.
    // Not required.
    void SetPackInterval(int64_t timeout);

    // Set buffersize for each internal scan jobs, which avoids scan result buffer growing too much.
    // Default: 64KB
    void SetBufferSize(int64_t buf_size);

    // Set the limitation of cell number for each internal scan jobs,
    // which acquires lower latency in interactive scan task.
    // Not required.
    void SetNumberLimit(int64_t number_limit);
    int64_t GetNumberLimit();

    // EXPRIMENTAL
    bool SetFilter(const std::string& schema);
    typedef bool (*ValueConverter)(const std::string& in,
                                   const std::string& type,
                                   std::string* out);
    // Set custom defined value convert funtion
    void SetValueConverter(ValueConverter converter);

    ScanDescImpl* GetImpl() const;

    // DEVELOPING
    void SetSnapshot(uint64_t snapshot_id);

private:
    ScanDescriptor(const ScanDescriptor&);
    void operator=(const ScanDescriptor&);
    ScanDescImpl* impl_;
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_SCAN_H_
