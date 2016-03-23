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

DECLARE_bool(tera_sdk_batch_scan_enabled);
DECLARE_int32(tera_sdk_max_batch_scan_req);

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

///////////////////////////////////////
/////    high performance scan    /////
///////////////////////////////////////
ResultStreamBatchImpl::ResultStreamBatchImpl(TableImpl* table, ScanDescImpl* scan_desc)
    : ResultStreamImpl(table, scan_desc),
    cv_(&mu_), ref_count_(1) {
    // do something startup
    sliding_window_.resize(FLAGS_tera_sdk_max_batch_scan_req);
    session_end_key_ = _scan_desc_impl->GetStartRowKey();
    mu_.Lock();
    ScanSessionReset();
    mu_.Unlock();
}

void ResultStreamBatchImpl::GetRpcHandle(ScanTabletRequest** request_ptr,
                                         ScanTabletResponse** response_ptr) {
    *request_ptr = new ScanTabletRequest;
    *response_ptr = new ScanTabletResponse;

    MutexLock mutex(&mu_);
    (*request_ptr)->set_part_of_session(part_of_session_);
    (*request_ptr)->set_session_id((int64_t)session_id_);
    VLOG(28) << "Get rpc handle, part_of_session_ " << part_of_session_
        << ", session_id_ " << session_id_ << ", response " << (uint64_t)(*response_ptr);
}

// insure table_impl no more use scan_impl
void ResultStreamBatchImpl::ReleaseRpcHandle(ScanTabletRequest* request,
                                             ScanTabletResponse* response) {
    delete request;
    delete response;

    MutexLock mutex(&mu_);
    ref_count_--;
    VLOG(28) << "release rpc handle and wakeup, ref_count_ " << ref_count_;
    cv_.Signal();
}

// scan request callback trigger:
//      1. drop resp condition:
//          1.1. stale session_id
//          1.2. stale result_id
//      2. handle session broken:
//          2.1. stop scan, and report error to user
//      3. scan success, notify user to consume result
void ResultStreamBatchImpl::OnFinish(ScanTabletRequest* request,
                                     ScanTabletResponse* response) {
    MutexLock mutex(&mu_);
    // check session id
    if (request->session_id() != (int64_t)session_id_) {
        LOG(WARNING) << "session_id not match, " << request->session_id() << ", " << session_id_;
    } else if (response->status() != kTabletNodeOk) {
        // rpc or ts error, session broken and report error
        session_error_ = response->status();
        LOG(WARNING) << "session_id " <<session_id_ <<", broken " << StatusCodeToString(session_error_);
        session_done_ = true;
    } else if ((response->results_id() == 0) &&
               (response->results().key_values_size() == 0) &&
               request->part_of_session()) {
        // handle old ts, results_id not init
        VLOG(28) << "batch scan old ts";
    } else if ((response->results_id() < session_data_idx_) ||
               (response->results_id() >= session_data_idx_ +
                    FLAGS_tera_sdk_max_batch_scan_req)) {
        if (response->results_id() != std::numeric_limits<unsigned long>::max()) {
            LOG(WARNING) << "ScanCallback session_id " << session_id_
                << ", session_data_idx " << session_data_idx_
                << ", stale result_id " << response->results_id()
                << ", response " << (uint64_t)response;
            session_done_ = true;
            // TODO: ts state no known
            session_error_ = kRPCTimeout;
        }
    } else { // scan success, cache result
        int32_t slot_idx = ((response->results_id() - session_data_idx_)
                            + sliding_window_idx_) % FLAGS_tera_sdk_max_batch_scan_req;
        VLOG(28) << "scan suc, session_id " << session_id_ << ", slot_idx " << slot_idx << ", result_id " << response->results_id()
            << ", session_data_idx_ " << session_data_idx_
            << ", sliding_window_idx_ " << sliding_window_idx_
            << ", resp.kv.size() " << response->results().key_values_size();
        ScanSlot* slot = &(sliding_window_[slot_idx]);
        if (slot->state_ == SCANSLOT_INVALID) {
            slot->state_ = SCANSLOT_VALID;
            slot->cell_ = response->results();
            VLOG(28) << "cache scan result, session_id " << session_id_ << ", slot_idx " << slot_idx
                << ", kv.size() " << slot->cell_.key_values_size()
                << ", resp.kv.size() " << response->results().key_values_size();
        }
        if (response->complete()) {
            session_last_idx_ = (session_last_idx_ > response->results_id()) ?
                                    response->results_id(): session_last_idx_;
            session_end_key_ = response->end();
            VLOG(28) << "scan complete: session_id " << session_id_ << ", session_end_key " << session_end_key_
                << ", session_last_idx " << session_last_idx_;
            session_done_ = true;
        }
    }
    return;
}

ResultStreamBatchImpl::~ResultStreamBatchImpl() {
    // do something cleanup
    MutexLock mutex(&mu_);
    ref_count_--;
    VLOG(28) << "before wait scan task finsh, ref_count " << ref_count_;
    while (ref_count_ != 0) { cv_.Wait();}
}

void ResultStreamBatchImpl::ScanSessionReset() {
    mu_.AssertHeld();
    // reset session parameter
    uint64_t tid = (uint64_t)pthread_self();
    session_id_ = ((tid << 48) | ((uint64_t)get_micros())) & (0x7ffffffffffff);
    session_done_ = false;
    session_error_ = kTabletNodeOk;
    part_of_session_ = false;
    session_data_idx_ = 0;
    session_last_idx_ = UINT32_MAX;
    sliding_window_idx_= 0;
    next_idx_ = 0;

    // set all slot invalid
    std::vector<ScanSlot>::iterator it = sliding_window_.begin();
    for (; it != sliding_window_.end(); ++it) {
        it->state_ = SCANSLOT_INVALID;
        it->cell_.Clear();
    }

    ref_count_ += FLAGS_tera_sdk_max_batch_scan_req;
    _scan_desc_impl->SetStart(session_end_key_);
    VLOG(28) << "scan session reset, start key " << session_end_key_
        << ", ref_count " << ref_count_;
    mu_.Unlock();
    // do io, release lock
    for (int32_t i = 0; i < FLAGS_tera_sdk_max_batch_scan_req; i++) {
        _table_ptr->ScanTabletAsync(this);
        part_of_session_ = true;
    }
    mu_.Lock();
}

void ResultStreamBatchImpl::ClearAndScanNextSlot(bool scan_next) {
    mu_.AssertHeld();
    ScanSlot* slot = &(sliding_window_[sliding_window_idx_]);
    slot->cell_.Clear();
    slot->state_ = SCANSLOT_INVALID;
    next_idx_ = 0;
    session_data_idx_++;
    sliding_window_idx_ = (sliding_window_idx_ + 1) % FLAGS_tera_sdk_max_batch_scan_req;
    VLOG(28) << "session_id " << session_id_ << ", session_data_idx_ " << session_data_idx_ << ", sliding_window_idx_ "
        << sliding_window_idx_ << ", ref_count_ " << ref_count_;
    if (scan_next) {
        ref_count_++;
        mu_.Unlock();
        _table_ptr->ScanTabletAsync(this);
        mu_.Lock();
    }
    return;
}

bool ResultStreamBatchImpl::Done(ErrorCode* error) {
    if (error) {
        error->SetFailed(ErrorCode::kOK);
    }
    MutexLock mutex(&mu_);
    while (1) {
        // not wait condition:
        //  1. current slot valid, or
        //  2. ts not available, or
        //  3. rpc not available, or
        ScanSlot* slot = &(sliding_window_[sliding_window_idx_]);
        while(slot->state_ == SCANSLOT_INVALID) {
            // stale results_id, re-enable another scan req
            if (session_error_ != kTabletNodeOk) {
                // TODO: kKeyNotInRange, do reset session
                LOG(WARNING) << "scan done: session error " << StatusCodeToString(session_error_);
                if (error) {
                    error->SetFailed(ErrorCode::kSystem, StatusCodeToString(session_error_));
                }
                return true;
            }
            if (ref_count_ == 1) {
                // ts refuse scan...
                LOG(WARNING) << "ts refuse scan, scan later...\n";
                return true;
            }
            cv_.Wait();
        }
        if (next_idx_ < slot->cell_.key_values_size()) { break; }

        VLOG(28) << "session_done_ " << session_done_ << ", session_data_idx_ "
            << session_data_idx_ << ", session_last_idx_ " << session_last_idx_;
        // current slot finish and session not finish, scan next slot
        if (!session_done_) {
            ClearAndScanNextSlot(true);
            continue;
        }

        // session finish, read rest data
        if (session_data_idx_ != session_last_idx_) {
            ClearAndScanNextSlot(false);
            continue;
        }

        // scan finish, exit
        const string& scan_end_key = _scan_desc_impl->GetEndRowKey();
        if (session_end_key_ == "" || (scan_end_key != "" && session_end_key_ >= scan_end_key)) {
            VLOG(28) << "scan done, scan_end_key " << scan_end_key << ", session_end_key " << session_end_key_;
            return true;
        }

        // scan next tablet
        ScanSessionReset();
    }
    return false;
}

void ResultStreamBatchImpl::Next() { next_idx_++; }
bool ResultStreamBatchImpl::LookUp(const std::string& row_key) { return true;}
std::string ResultStreamBatchImpl::RowName() const {
    const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
    return row.has_key() ? row.key(): "";
}
std::string ResultStreamBatchImpl::Family() const {
    const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
    return row.has_column_family() ? row.column_family(): "";
}
std::string ResultStreamBatchImpl::Qualifier() const {
    const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
    return row.has_qualifier() ? row.qualifier(): "";
}
std::string ResultStreamBatchImpl::ColumnName() const {
    const std::string& cf = Family();
    const std::string& qu = Qualifier();
    return cf + ":" + qu;
}
int64_t ResultStreamBatchImpl::Timestamp() const {
    const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
    return row.has_timestamp() ? row.timestamp(): 0;
}
std::string ResultStreamBatchImpl::Value() const {
    const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
    return row.has_value() ? row.value(): "";
}
int64_t ResultStreamBatchImpl::ValueInt64() const {
    std::string v = Value();
    return (v.size() == sizeof(int64_t)) ? *(int64_t*)v.c_str() : 0;
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

int64_t ResultStreamSyncImpl::ValueInt64() const {
    std::string v;
    if (_response->results().key_values(_result_pos).has_value()) {
        v = _response->results().key_values(_result_pos).value();
    }
    return (v.size() == sizeof(int64_t)) ? *(int64_t*)v.c_str() : 0;
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
      _is_async(FLAGS_tera_sdk_batch_scan_enabled),
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

bool ScanDescImpl::IsKvOnlyTable() {
    return _table_schema.kv_only();
}

// SELECT * WHERE <type> <cf0> <op0> <value0> AND <type> <cf1> <op1> <value1>
bool ScanDescImpl::SetFilter(const std::string& schema) {
    std::string select;
    std::string where;
    std::string::size_type pos;
    if ((pos = schema.find("SELECT ")) != 0) {
        LOG(ERROR) << "illegal scan expression: should be begin with \"SELECT\"";
        return false;
    }
    if ((pos = schema.find(" WHERE ")) != string::npos) {
        select = schema.substr(7, pos - 7);
        where = schema.substr(pos + 7, schema.size() - pos - 7);
    } else {
        select = schema.substr(7);
    }
    // parse select
    {
        select = RemoveInvisibleChar(select);
        if (select != "*") {
            std::vector<string> cfs;
            SplitString(select, ",", &cfs);
            for (size_t i = 0; i < cfs.size(); ++i) {
                // add columnfamily
                AddColumnFamily(cfs[i]);
                VLOG(10) << "add cf: " << cfs[i] << " to scan descriptor";
            }
        }
    }
    // parse where
    if (where != "") {
        _filter_string = where;
        if (!ParseFilterString()) {
            return false;
        }
    }
    return true;
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

