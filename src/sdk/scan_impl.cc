// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/scan_impl.h"

#include <boost/bind.hpp>

#include "common/this_thread.h"
#include "common/base/closure.h"
#include "common/base/string_ext.h"

#include "proto/proto_helper.h"
#include "proto/table_schema.pb.h"
#include "sdk/table_impl.h"
#include "sdk/filter_utils.h"
#include "utils/atomic.h"
#include "utils/timer.h"

DECLARE_bool(tera_sdk_scan_async_enabled);
DECLARE_int64(tera_sdk_scan_async_cache_size);
DECLARE_int32(tera_sdk_scan_async_parallel_max_num);

namespace tera {

ResultStreamImpl::ResultStreamImpl(TableImpl* table,
                                   ScanDescImpl* scan_desc_impl)
    : _scan_desc_impl(new ScanDescImpl(*scan_desc_impl)),
      _table_ptr(table) {
}

ResultStreamImpl::~ResultStreamImpl() {
    if (_scan_desc_impl != NULL) {
        delete _scan_desc_impl;
    }
}

ScanDescImpl* ResultStreamImpl::GetScanDesc() {
    return _scan_desc_impl;
}

/*
 * scan的时候，tabletnode攒满一个buffer就会返回给sdk，sdk再接着scan，
 * 新的scan逻辑（tabletnode）在返回scan结果的同时，
 * 还会告知sdk下次scan的启动点(start_point)，保证scan不重、不漏；
 *
 * 旧的scan逻辑（tabletnode）不会告知sdk下次scan时的启动点(start_point)，
 * 这个启动点是sdk自己计算出来的：由上次scan结果的最后一条加上"某个最小值"。
 *
 * rawkey类型为Readble时，rawkey不能包含'\0'；
 * rawkey类型为其它类型（如Binary）时，rawkey可以包含任意字符（包括'\0'）；
 * (不建议新表继续使用Readble类型，未来可能不再支持)
 *
 * 综上，
 * 对于rawkey类型为Readble的表来说，sdk计算下次scan的启动点时需要加'\x1'；
 * 否则，加'\x0'（也就是'\0'）。
 */
std::string ResultStreamImpl::GetNextStartPoint(const std::string& str) {
    const static std::string x0("\x0", 1);
    const static std::string x1("\x1");
    RawKey rawkey_type = _table_ptr->GetTableSchema().raw_key();
    return rawkey_type == Readable ? str + x1 : str + x0;
}

ResultStreamAsyncImpl::ResultStreamAsyncImpl(TableImpl* table, ScanDescImpl* scan_desc_impl)
    : ResultStreamImpl(table, scan_desc_impl),
      _session_id(get_micros()), _stopped(false), _part_of_session(false), _invoked_count(0),
      _queue_size(0), _cur_data_id(0), _result_pos(0) {

    _last_response.set_complete(true);
    _last_response.set_end(scan_desc_impl->GetEndRowKey());

    _cache_max_size = ((FLAGS_tera_sdk_scan_async_cache_size << 20)
            + _scan_desc_impl->GetBufferSize() - 1) / _scan_desc_impl->GetBufferSize();
    VLOG(6) << "scan: cache max slot: " << _cache_max_size
        << ", cache size: " << FLAGS_tera_sdk_scan_async_cache_size << " MB"
        << ", trans buf size: " << _scan_desc_impl->GetBufferSize() << " bytes";
    _scan_thread.Start(boost::bind(&ResultStreamAsyncImpl::DoScan, this));
    ThisThread::Yield();
}

ResultStreamAsyncImpl::~ResultStreamAsyncImpl() {
    if (_scan_desc_impl != NULL) {
        delete _scan_desc_impl;
    }

    _stopped = true;
    while (_invoked_count != 0) {
        _scan_done_event.Wait();
    }
}

bool ResultStreamAsyncImpl::LookUp(const string& row_key) {
    return true;
}

bool ResultStreamAsyncImpl::Done(ErrorCode* err) {
    if (_stopped && _queue_size == 0) {
        if (err) {
            err->SetFailed(ErrorCode::kOK);
        }
        return true;
    }
    while (!_stopped && _queue_size == 0) {
        _scan_push_event.Wait();
    }
    if (err) {
        err->SetFailed(ErrorCode::kOK);
    }
    return false;
}

void ResultStreamAsyncImpl::Next() {
    bool drop = false;
    ++_result_pos;
    const RowResult& results = _row_results_cache.front();
    if (_result_pos >= results.key_values_size()) {
        drop = true;
        _result_pos = 0;
    }
    if (drop) {
        _row_results_cache.pop();
        atomic_dec(&_queue_size);
        _scan_pop_event.Set();
    }
}

void ResultStreamAsyncImpl::DoScan() {
    VLOG(6) << "streaming begin (session id: " << _session_id << ")";
    _part_of_session = false;
    while (!_stopped) {
        _table_ptr->ScanTabletAsync(this);
        _part_of_session = true;

        atomic_inc(&_invoked_count);

        uint32_t sleep_time = 1;
        while (_invoked_count == FLAGS_tera_sdk_scan_async_parallel_max_num) {
            LOG(INFO) << "scan sleep time: " << sleep_time;
            ThisThread::Sleep(500 * sleep_time);
            sleep_time++;
        }
    }
    VLOG(6) << "streaming is finished (session id: " << _session_id << ")";
}

void ResultStreamAsyncImpl::DoScanCallback(int64_t session_id, ScanTabletResponse* response,
                                           bool failed) {
    VLOG(15) << "ResultStreamAsyncImpl::DoScanCallback():"
        << "failed: " << failed
        << ", sequece id: " << response->sequence_id()
        << ", record id: " << response->results_id()
        << ", compeled: " << response->complete()
        << ", invoked_count: " << _invoked_count
        << ", queue size: " << _queue_size;
    bool need_rebuild_stream = false;
    bool stream_broken = false;
    if (session_id != _session_id) {
        LOG(WARNING) << "invalid response [cur session id: " << _session_id
            << ", input session id: " << session_id
            << "], skip";
    } else if (failed) {
        LOG(WARNING) << "streaming is broken (session id: "
            << _session_id << "), rebuild ...";
        need_rebuild_stream = true;
        stream_broken = true;
    } else if (static_cast<uint64_t>(_cur_data_id) > response->results_id()) {
        LOG(WARNING) << "invalid response [cur data id: " << _cur_data_id
            << ", record id: " << response->results_id()
            << "], skip";
    } else {
        while (static_cast<uint64_t>(_cur_data_id) != response->results_id()) {
            VLOG(15) << "id: " << _cur_data_id
                << ", result id: " << response->results_id()
                << ", waiting ...";
            _scan_push_order_event.Wait();
        }

        atomic_inc(&_cur_data_id);
        VLOG(15) << "id: " << response->results_id()
            << ", size: " << response->results().key_values_size()
            << ", next id: " << _cur_data_id;
        if (response->has_results() && response->results().key_values_size() > 0) {
            while (static_cast<uint64_t>(_queue_size) == _cache_max_size) {
                _scan_pop_event.Wait();
            }
            {
                MutexLock locker(&_mutex);
                _row_results_cache.push(response->results());
                _last_response.CopyFrom(*response);
            }
            atomic_inc(&_queue_size);
        }
        if (response->complete()) {
            const string& tablet_end_key = response->end();
            const string& scan_end_key = _scan_desc_impl->GetEndRowKey();
            if (tablet_end_key == "" || (scan_end_key != "" && tablet_end_key >= scan_end_key)) {
                _stopped = true;
            } else {
                need_rebuild_stream = true;
            }
        }
    }

    atomic_dec(&_invoked_count);
    _scan_push_event.Set();
    _scan_push_order_event.Set();
    _scan_done_event.Set();

    if (!_stopped && need_rebuild_stream) {
        if (stream_broken) {
            RebuildStream(&_last_response);
        } else {
            RebuildStream(response);
        }
    }
}

void ResultStreamAsyncImpl::RebuildStream(ScanTabletResponse* response) {
    VLOG(15) << "rebuild stream (session id: " << _session_id << ")";

    if (!response->complete()) {
        int32_t last_result_pos = response->results().key_values_size() - 1;
        const KeyValuePair& kv = response->results().key_values(last_result_pos);
        if (kv.timestamp() == 0) {
            _scan_desc_impl->SetStart(kv.key(), kv.column_family(),
                                      GetNextStartPoint(kv.qualifier()), kv.timestamp());
        } else {
            _scan_desc_impl->SetStart(kv.key(), kv.column_family(),
                                      kv.qualifier(), kv.timestamp() - 1);
        }
    } else {
        _scan_desc_impl->SetStart(response->end());
    }
    _session_id = get_micros();
    _part_of_session = false;
    _cur_data_id = 0;
    VLOG(15) << "rebuild new stream, session id: " << _session_id;
}

void ResultStreamAsyncImpl::GetRpcHandle(ScanTabletRequest** request,
                                         ScanTabletResponse** response) {
    *request = new ScanTabletRequest;
    *response = new ScanTabletResponse;

    (*request)->set_part_of_session(_part_of_session);
    (*request)->set_session_id(_session_id);
}

void ResultStreamAsyncImpl::ReleaseRpcHandle(ScanTabletRequest* request,
                                             ScanTabletResponse* response) {
    delete request;
    delete response;
}

void ResultStreamAsyncImpl::OnFinish(ScanTabletRequest* request,
                                     ScanTabletResponse* response) {
    if (request->session_id() != _session_id) {
        LOG(WARNING) << "invalid session id: " << request->session_id()
            << " (current: " << _session_id << "), skip";
        return;
    }
    DoScanCallback(request->session_id(), response, response->status() != kTabletNodeOk);
}

string ResultStreamAsyncImpl::RowName() const {
    const RowResult* results = NULL;
    {
        MutexLock locker(const_cast<Mutex*>(&_mutex));
        results = &_row_results_cache.front();
    }
    return results->key_values(_result_pos).key();
}

string ResultStreamAsyncImpl::ColumnName() const {
    const RowResult* results = NULL;
    {
        MutexLock locker(const_cast<Mutex*>(&_mutex));
        results = &_row_results_cache.front();
    }
    if (results->key_values(_result_pos).has_column_family()) {
        const string& family =  results->key_values(_result_pos).column_family();
        if (results->key_values(_result_pos).has_qualifier()) {
            return (family + ":" + results->key_values(_result_pos).qualifier());
        }
        return family;
    }
    return "";
}

string ResultStreamAsyncImpl::Family() const {
    const RowResult* results = NULL;
    {
        MutexLock locker(const_cast<Mutex*>(&_mutex));
        results = &_row_results_cache.front();
    }
    if (results->key_values(_result_pos).has_column_family()) {
        return results->key_values(_result_pos).column_family();
    }
    return "";
}

string ResultStreamAsyncImpl::Qualifier() const {
    const RowResult* results = NULL;
    {
        MutexLock locker(const_cast<Mutex*>(&_mutex));
        results = &_row_results_cache.front();
    }
    if (results->key_values(_result_pos).has_qualifier()) {
        return results->key_values(_result_pos).qualifier();
    }
    return "";
}

int64_t ResultStreamAsyncImpl::Timestamp() const {
    const RowResult* results = NULL;
    {
        MutexLock locker(const_cast<Mutex*>(&_mutex));
        results = &_row_results_cache.front();
    }
    if (results->key_values(_result_pos).has_timestamp()) {
        return results->key_values(_result_pos).timestamp();
    }
    return 0;
}

string ResultStreamAsyncImpl::Value() const {
    const RowResult* results = NULL;
    {
        MutexLock locker(const_cast<Mutex*>(&_mutex));
        results = &_row_results_cache.front();
    }
    if (results->key_values(_result_pos).has_value()) {
        return results->key_values(_result_pos).value();
    }
    return "";
}

ResultStreamSyncImpl::ResultStreamSyncImpl(TableImpl* table,
                                           ScanDescImpl* scan_desc_impl)
    : ResultStreamImpl(table, scan_desc_impl),
      _response(new tera::ScanTabletResponse),
      _result_pos(0),
      _finish_cond(&_finish_mutex),
      _finish(false) {
    _table_ptr->ScanTabletSync(this);
}

ResultStreamSyncImpl::~ResultStreamSyncImpl() {
    if (_response != NULL) {
        delete _response;
    }
}

bool ResultStreamSyncImpl::LookUp(const string& row_key) {
    return true;
}

bool ResultStreamSyncImpl::Done(ErrorCode* err) {
    while (1) {
        const string& scan_end_key = _scan_desc_impl->GetEndRowKey();
        /// scan failed
        if (_response->status() != kTabletNodeOk) {
            if (err) {
                err->SetFailed(ErrorCode::kSystem,
                               StatusCodeToString(_response->status()));
            }
            return true;
        }
        if (_result_pos < _response->results().key_values_size()) {
            break;
        }
        const string& tablet_end_key = _response->end();
        if (_response->complete() &&
            (tablet_end_key == "" || (scan_end_key != "" && tablet_end_key >= scan_end_key))) {
            if (err) {
                err->SetFailed(ErrorCode::kOK);
            }
            return true;
        }

        // Newer version of TS will return next_start_point when the opration is timeout
        if (!_response->complete()) {
            // Without next_start_point, kv is the last kv pair from last scan
            if (_response->next_start_point().key() == "") {
                const KeyValuePair& kv = _response->results().key_values(_result_pos - 1);
                if (_scan_desc_impl->IsKvOnlyTable()) {
                    _scan_desc_impl->SetStart(GetNextStartPoint(kv.key()), kv.column_family(),
                                              kv.qualifier(), kv.timestamp());
                } else if (kv.timestamp() == 0) {
                    _scan_desc_impl->SetStart(kv.key(), kv.column_family(),
                                              GetNextStartPoint(kv.qualifier()), kv.timestamp());
                } else {
                    _scan_desc_impl->SetStart(kv.key(), kv.column_family(),
                                              kv.qualifier(), kv.timestamp() - 1);
                }
            // next_start_point is where the next scan should start
            } else {
                const KeyValuePair& kv = _response->next_start_point();
                if (_scan_desc_impl->IsKvOnlyTable()) {
                    _scan_desc_impl->SetStart(kv.key(), kv.column_family(),
                                              kv.qualifier(), kv.timestamp());
                } else {
                    _scan_desc_impl->SetStart(kv.key(), kv.column_family(),
                                              kv.qualifier(), kv.timestamp());
                }
            }
        } else {
            _scan_desc_impl->SetStart(tablet_end_key);
        }
        _result_pos = 0;
        delete _response;
        _response = new tera::ScanTabletResponse;
        Reset();
        _table_ptr->ScanTabletSync(this);
    }
    return false;
}

void ResultStreamSyncImpl::Next() {
    ++_result_pos;
}

string ResultStreamSyncImpl::RowName() const {
    return _response->results().key_values(_result_pos).key();
}

string ResultStreamSyncImpl::ColumnName() const {
    if (_response->results().key_values(_result_pos).has_column_family()) {
        const string& family =  _response->results().key_values(_result_pos).column_family();
        if (_response->results().key_values(_result_pos).has_qualifier()) {
            return (family + ":" + _response->results().key_values(_result_pos).qualifier());
        }
        return family;
    }
    return "";
}

string ResultStreamSyncImpl::Family() const {
    if (_response->results().key_values(_result_pos).has_column_family()) {
        return _response->results().key_values(_result_pos).column_family();
    }
    return "";
}

string ResultStreamSyncImpl::Qualifier() const {
    if (_response->results().key_values(_result_pos).has_qualifier()) {
        return _response->results().key_values(_result_pos).qualifier();
    }
    return "";
}

int64_t ResultStreamSyncImpl::Timestamp() const {
    if (_response->results().key_values(_result_pos).has_timestamp()) {
        return _response->results().key_values(_result_pos).timestamp();
    }
    return 0;
}

string ResultStreamSyncImpl::Value() const {
    if (_response->results().key_values(_result_pos).has_value()) {
        return _response->results().key_values(_result_pos).value();
    }
    return "";
}

void ResultStreamSyncImpl::GetRpcHandle(ScanTabletRequest** request,
                                    ScanTabletResponse** response) {
    *request = new ScanTabletRequest;
    *response = _response;
}

void ResultStreamSyncImpl::ReleaseRpcHandle(ScanTabletRequest* request,
                                            ScanTabletResponse* response) {
    delete request;
    Signal();
}

void ResultStreamSyncImpl::OnFinish(ScanTabletRequest* request,
                                    ScanTabletResponse* response) {
}

void ResultStreamSyncImpl::Wait() {
    MutexLock locker(&_finish_mutex);
    while (!_finish) {
        _finish_cond.Wait();
    }
}

void ResultStreamSyncImpl::Signal() {
    MutexLock locker(&_finish_mutex);
    _finish = true;
    _finish_cond.Signal();
}

void ResultStreamSyncImpl::Reset() {
    MutexLock locker(&_finish_mutex);
    _finish = false;
}

///////////////////////// ScanDescImpl ///////////////////////

ScanDescImpl::ScanDescImpl(const string& rowkey)
    : _start_timestamp(0),
      _timer_range(NULL),
      _buf_size(65536),
      _is_async(FLAGS_tera_sdk_scan_async_enabled),
      _max_version(1),
      _pack_interval(5000),
      _snapshot(0),
      _value_converter(&DefaultValueConverter) {
    SetStart(rowkey);
}

ScanDescImpl::ScanDescImpl(const ScanDescImpl& impl)
    : _start_key(impl._start_key),
      _end_key(impl._end_key),
      _start_column_family(impl._start_column_family),
      _start_qualifier(impl._start_qualifier),
      _start_timestamp(impl._start_timestamp),
      _buf_size(impl._buf_size),
      _is_async(impl._is_async),
      _max_version(impl._max_version),
      _pack_interval(impl._pack_interval),
      _snapshot(impl._snapshot),
      _table_schema(impl._table_schema) {
    _value_converter = impl.GetValueConverter();
    _filter_string = impl.GetFilterString();
    _filter_list = impl.GetFilterList();
    if (impl.GetTimerRange() != NULL) {
        _timer_range = new tera::TimeRange;
        _timer_range->CopyFrom(*(impl.GetTimerRange()));
    } else {
        _timer_range = NULL;
    }
    for (int32_t i = 0; i < impl.GetSizeofColumnFamilyList(); ++i) {
        _cf_list.push_back(new tera::ColumnFamily(*(impl.GetColumnFamily(i))));
    }
}

ScanDescImpl::~ScanDescImpl() {
    if (_timer_range != NULL) {
        delete _timer_range;
    }
    for (uint32_t i = 0; i < _cf_list.size(); ++i) {
        delete _cf_list[i];
    }
}

void ScanDescImpl::SetStart(const string& row_key, const string& column_family,
                            const string& qualifier, int64_t time_stamp)
{
    _start_key = row_key;
    _start_column_family = column_family;
    _start_qualifier = qualifier;
    _start_timestamp = time_stamp;
}

void ScanDescImpl::SetEnd(const string& rowkey) {
    _end_key = rowkey;
}

void ScanDescImpl::AddColumnFamily(const string& cf) {
    AddColumn(cf, "");
}

void ScanDescImpl::AddColumn(const string& cf, const string& qualifier) {
    for (uint32_t i = 0; i < _cf_list.size(); ++i) {
        if (_cf_list[i]->family_name() == cf) {
            if (qualifier != "") {
                _cf_list[i]->add_qualifier_list(qualifier);
            }
            return;
        }
    }
    tera::ColumnFamily* column_family = new tera::ColumnFamily;
    column_family->set_family_name(cf);
    if (qualifier != "") {
        column_family->add_qualifier_list(qualifier);
    }
    _cf_list.push_back(column_family);
}

void ScanDescImpl::SetMaxVersions(int32_t versions) {
    _max_version = versions;
}

void ScanDescImpl::SetPackInterval(int64_t interval) {
    _pack_interval = interval;
}

void ScanDescImpl::SetTimeRange(int64_t ts_end, int64_t ts_start) {
    if (_timer_range == NULL) {
        _timer_range = new tera::TimeRange;
    }
    _timer_range->set_ts_start(ts_start);
    _timer_range->set_ts_end(ts_end);
}

bool ScanDescImpl::SetFilterString(const string& filter_string) {
    if (!CheckFilterString(filter_string)) {
        return false;
    }
    _filter_string = filter_string;
    return true;
}

void ScanDescImpl::SetValueConverter(ValueConverter convertor) {
    _value_converter = convertor;
}

void ScanDescImpl::SetSnapshot(uint64_t snapshot_id) {
    _snapshot = snapshot_id;
}

uint64_t ScanDescImpl::GetSnapshot() const {
    return _snapshot;
}

void ScanDescImpl::SetBufferSize(int64_t buf_size) {
    _buf_size = buf_size;
}

void ScanDescImpl::SetAsync(bool async) {
    _is_async = async;
}

const string& ScanDescImpl::GetStartRowKey() const {
    return _start_key;
}

const string& ScanDescImpl::GetEndRowKey() const {
    return _end_key;
}

const string& ScanDescImpl::GetStartColumnFamily() const {
    return _start_column_family;
}

const string& ScanDescImpl::GetStartQualifier() const {
    return _start_qualifier;
}

int64_t ScanDescImpl::GetStartTimeStamp() const {
    return _start_timestamp;
}

int32_t ScanDescImpl::GetSizeofColumnFamilyList() const {
    return _cf_list.size();
}

const tera::ColumnFamily* ScanDescImpl::GetColumnFamily(int32_t num) const {
    if (static_cast<uint64_t>(num) >= _cf_list.size()) {
        return NULL;
    }
    return _cf_list[num];
}

int32_t ScanDescImpl::GetMaxVersion() const {
    return _max_version;
}

int64_t ScanDescImpl::GetPackInterval() const {
    return _pack_interval;
}

const tera::TimeRange* ScanDescImpl::GetTimerRange() const {
    return _timer_range;
}

const string& ScanDescImpl::GetFilterString() const {
    return _filter_string;
}

const FilterList& ScanDescImpl::GetFilterList() const {
    return _filter_list;
}

const ValueConverter ScanDescImpl::GetValueConverter() const {
    return _value_converter;
}

int64_t ScanDescImpl::GetBufferSize() const {
    return _buf_size;
}

bool ScanDescImpl::IsAsync() const {
    return _is_async;
}

void ScanDescImpl::SetTableSchema(const TableSchema& schema) {
    _table_schema = schema;
}

bool ScanDescImpl::ParseFilterString() {
    const char* and_op = " AND ";
    _filter_list.Clear();
    std::vector<string> filter_v;
    SplitString(_filter_string, and_op, &filter_v);
    for (size_t i = 0; i < filter_v.size(); ++i) {
        Filter filter;
        if (ParseSubFilterString(filter_v[i], &filter)) {
            Filter* pf = _filter_list.add_filter();
            pf->CopyFrom(filter);
        } else {
            LOG(ERROR) << "fail to parse expression: " << filter_v[i];
            return false;
        }
    }

    return true;
}

bool ScanDescImpl::IsKvOnlyTable() {
    return _table_schema.kv_only();
}

bool ScanDescImpl::ParseSubFilterString(const string& filter_str,
                                        Filter* filter) {
    string filter_t = RemoveInvisibleChar(filter_str);
    if (filter_t.size() < 3) {
        LOG(ERROR) << "illegal filter expression: " << filter_t;
        return false;
    }
    if (filter_t.find("@") == string::npos) {
        // default filter, value compare filter
        if (!ParseValueCompareFilter(filter_t, filter)) {
            return false;
        }
    } else {
        // TODO: other filter
        LOG(ERROR) << "illegal filter expression: " << filter_t;
        return false;
    }
    return true;
}

bool ScanDescImpl::ParseValueCompareFilter(const string& filter_str,
                                           Filter* filter) {
    if (filter == NULL) {
        LOG(ERROR) << "filter ptr is NULL.";
        return false;
    }

    if (_max_version != 1) {
        LOG(ERROR) << "only support 1 version scan if there is a value filter: "
            << filter_str;
        return false;
    }
    string::size_type type_pos;
    string::size_type cf_pos;
    if ((type_pos = filter_str.find("int64")) != string::npos) {
        filter->set_value_type(kINT64);
        cf_pos = type_pos + 5;
    } else {
        LOG(ERROR) << "only support int64 value filter, but got: "
            << filter_str;
        return false;
    }

    string cf_name, value;
    string::size_type op_pos;
    BinCompOp comp_op = UNKNOWN;
    if ((op_pos = filter_str.find(">=")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 2, filter_str.size() - op_pos - 2);
        comp_op = GE;
    } else if ((op_pos = filter_str.find(">")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 1, filter_str.size() - op_pos - 1);
        comp_op = GT;
    } else if ((op_pos = filter_str.find("<=")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 2, filter_str.size() - op_pos - 2);
        comp_op = LE;
    } else if ((op_pos = filter_str.find("<")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 1, filter_str.size() - op_pos - 1);
        comp_op = LT;
    } else if ((op_pos = filter_str.find("==")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 2, filter_str.size() - op_pos - 2);
        comp_op = EQ;
    } else if ((op_pos = filter_str.find("!=")) != string::npos) {
        cf_name = filter_str.substr(cf_pos, op_pos - cf_pos);
        value = filter_str.substr(op_pos + 2, filter_str.size() - op_pos - 2);
        comp_op = NE;
    } else {
        LOG(ERROR) << "fail to parse expression: " << filter_str;
        return false;
    }
    string type;
    if (filter->value_type() == kINT64) {
        type = "int64";
    } else {
        assert(false);
    }

    string value_internal;
    if (!_value_converter(value, type, &value_internal)) {
        LOG(ERROR) << "fail to convert value: \""<< value << "\"(" << type << ")";
        return false;
    }

    filter->set_type(BinComp);
    filter->set_bin_comp_op(comp_op);
    filter->set_field(ValueFilter);
    filter->set_content(cf_name);
    filter->set_ref_value(value_internal);
    return true;
}

} // namespace tera

