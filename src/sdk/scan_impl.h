// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_SCAN_IMPL_H_
#define  TERA_SDK_SCAN_IMPL_H_

#include <list>
#include <queue>
#include <string>
#include <vector>

#include "common/event.h"
#include "common/thread.h"
#include "proto/tabletnode_rpc.pb.h"
#include "sdk/sdk_task.h"
#include "sdk/tera.h"
#include "types.h"
#include "utils/timer.h"


namespace tera {

class TableImpl;

class ResultStreamImpl : public ResultStream {
public:
    ResultStreamImpl(TableImpl* table, ScanDescImpl* scan_desc_impl);
    virtual ~ResultStreamImpl();

    bool LookUp(const std::string& row_key) = 0;
    bool Done(ErrorCode* err) = 0;
    void Next() = 0;

    std::string RowName() const = 0;
    std::string Family() const = 0;
    std::string ColumnName() const = 0;
    std::string Qualifier() const = 0;
    int64_t Timestamp() const = 0;
    std::string Value() const = 0;

public:
    ScanDescImpl* GetScanDesc();

    virtual void GetRpcHandle(ScanTabletRequest** request,
                              ScanTabletResponse** response) = 0;
    virtual void ReleaseRpcHandle(ScanTabletRequest* request,
                                  ScanTabletResponse* response) = 0;
    virtual void OnFinish(ScanTabletRequest* request,
                          ScanTabletResponse* response) = 0;
    std::string GetNextStartPoint(const std::string& str);

protected:
    tera::ScanDescImpl* _scan_desc_impl;
    TableImpl* _table_ptr;

private:
    ResultStreamImpl(const ResultStreamImpl&);
    void operator=(const ResultStreamImpl&);
};

class ResultStreamAsyncImpl : public ResultStreamImpl {
public:
    ResultStreamAsyncImpl(TableImpl* table, ScanDescImpl* scan_desc_impl);
    virtual ~ResultStreamAsyncImpl();

    bool LookUp(const std::string& row_key);
    bool Done(ErrorCode* err);
    void Next();

    std::string RowName() const;
    std::string Family() const;
    std::string ColumnName() const;
    std::string Qualifier() const;
    int64_t Timestamp() const;
    std::string Value() const;

public:
    void GetRpcHandle(ScanTabletRequest** request,
                      ScanTabletResponse** response);
    void ReleaseRpcHandle(ScanTabletRequest* request,
                          ScanTabletResponse* response);
    void OnFinish(ScanTabletRequest* request,
                  ScanTabletResponse* response);

private:
    void DoScan();
    void DoScanCallback(int64_t session_id, ScanTabletResponse* response,
                        bool failed);

    void RebuildStream(ScanTabletResponse* response);

private:
    std::queue<RowResult> _row_results_cache;
    common::Thread _scan_thread;
    AutoResetEvent _scan_event;
    AutoResetEvent _scan_done_event;
    AutoResetEvent _scan_push_event;
    AutoResetEvent _scan_push_order_event;
    AutoResetEvent _scan_pop_event;
    uint64_t _cache_max_size;
    int64_t _session_id;
    bool _stopped;
    bool _part_of_session;
    int32_t _invoked_count;
    int32_t _queue_size;
    int32_t _cur_data_id;
    int32_t _result_pos;

    ScanTabletResponse _last_response;

    Mutex _mutex;
};

class ResultStreamSyncImpl : public ResultStreamImpl {
public:
    ResultStreamSyncImpl(TableImpl* table, ScanDescImpl* scan_desc_impl);
    virtual ~ResultStreamSyncImpl();

    bool LookUp(const std::string& row_key);
    bool Done(ErrorCode* err);
    void Next();

    std::string RowName() const;
    std::string Family() const;
    std::string ColumnName() const;
    std::string Qualifier() const;
    int64_t Timestamp() const;
    std::string Value() const;

public:
    void GetRpcHandle(ScanTabletRequest** request,
                      ScanTabletResponse** response);
    void ReleaseRpcHandle(ScanTabletRequest* request,
                          ScanTabletResponse* response);
    void OnFinish(ScanTabletRequest* request,
                  ScanTabletResponse* response);

public:
    void Wait();

private:
    void Signal();
    void Reset();

private:
    tera::ScanTabletResponse* _response;
    int32_t _result_pos;
    mutable Mutex _finish_mutex;
    common::CondVar _finish_cond;
    bool _finish;
};

struct ScanTask : public SdkTask {
    ResultStreamImpl* stream;
    tera::ScanTabletRequest* request;
    tera::ScanTabletResponse* response;

    uint32_t retry_times;
    void IncRetryTimes() { retry_times++; }
    uint32_t RetryTimes() { return retry_times; }
    ScanTask() : SdkTask(SdkTask::SCAN), stream(NULL), request(NULL),
        response(NULL), retry_times(0) {}
};

typedef ScanDescriptor::ValueConverter ValueConverter;

class ScanDescImpl {
public:
    ScanDescImpl(const std::string& rowkey);

    ScanDescImpl(const ScanDescImpl& impl);

    ~ScanDescImpl();

    void SetEnd(const std::string& rowkey);

    void AddColumnFamily(const std::string& cf);

    void AddColumn(const std::string& cf, const std::string& qualifier);

    void SetMaxVersions(int32_t versions);

    void SetPackInterval(int64_t timeout);

    void SetTimeRange(int64_t ts_end, int64_t ts_start);

    bool SetFilterString(const std::string& filter_string);

    void SetValueConverter(ValueConverter converter);

    void SetSnapshot(uint64_t snapshot_id);

    void SetBufferSize(int64_t buf_size);

    void SetAsync(bool async);

    void SetStart(const std::string& row_key, const std::string& column_family = "",
                  const std::string& qualifier = "", int64_t time_stamp = kLatestTs);

    const std::string& GetStartRowKey() const;

    const std::string& GetEndRowKey() const;

    const std::string& GetStartColumnFamily() const;

    const std::string& GetStartQualifier() const;

    int64_t GetStartTimeStamp() const;

    int32_t GetSizeofColumnFamilyList() const;

    const tera::ColumnFamily* GetColumnFamily(int32_t num) const;

    const tera::TimeRange* GetTimerRange() const;

    const std::string& GetFilterString() const;

    const FilterList& GetFilterList() const;

    const ValueConverter GetValueConverter() const;

    int32_t GetMaxVersion() const;

    int64_t GetPackInterval() const;

    uint64_t GetSnapshot() const;

    int64_t GetBufferSize() const;

    bool IsAsync() const;

    void SetTableSchema(const TableSchema& schema);

    bool ParseFilterString();

    bool IsKvOnlyTable();

private:
    bool ParseSubFilterString(const std::string& filter_str, Filter* filter);

    bool ParseValueCompareFilter(const std::string& filter_str, Filter* filter);

private:
    std::string _start_key;
    std::string _end_key;
    std::string _start_column_family;
    std::string _start_qualifier;
    int64_t _start_timestamp;
    std::vector<tera::ColumnFamily*> _cf_list;
    tera::TimeRange* _timer_range;
    int64_t _buf_size;
    bool _is_async;
    int32_t _max_version;
    int64_t _pack_interval;
    uint64_t _snapshot;
    std::string _filter_string;
    FilterList _filter_list;
    ValueConverter _value_converter;
    TableSchema _table_schema;
};

} // namespace tera

#endif  // TERA_SDK_SCAN_IMPL_H_
