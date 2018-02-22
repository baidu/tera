// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/scan_impl.h"

#include <functional>
#include <algorithm>

#include "common/this_thread.h"
#include "common/base/string_ext.h"

#include "proto/proto_helper.h"
#include "proto/table_schema.pb.h"
#include "sdk/filter_utils.h"
#include "sdk/sdk_utils.h"
#include "sdk/table_impl.h"
#include "common/atomic.h"
#include "common/timer.h"

DECLARE_bool(tera_sdk_batch_scan_enabled);
DECLARE_int64(tera_sdk_scan_number_limit);
DECLARE_int64(tera_sdk_scan_buffer_size);
DECLARE_int32(tera_sdk_max_batch_scan_req);
DECLARE_int32(tera_sdk_batch_scan_max_retry);
DECLARE_int32(tera_sdk_sync_scan_max_retry);
DECLARE_int64(tera_sdk_scan_timeout);
DECLARE_int64(batch_scan_delay_retry_in_us);
DECLARE_int64(sync_scan_delay_retry_in_ms);

namespace tera {

ResultStreamImpl::ResultStreamImpl(TableImpl* table,
                                   ScanDescImpl* scan_desc_impl)
    : scan_desc_impl_(new ScanDescImpl(*scan_desc_impl)),
      table_ptr_(table) {
}

ResultStreamImpl::~ResultStreamImpl() {
    if (scan_desc_impl_ != NULL) {
        delete scan_desc_impl_;
    }
}

ScanDescImpl* ResultStreamImpl::GetScanDesc() {
    return scan_desc_impl_;
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
    RawKey rawkey_type = table_ptr_->GetTableSchema().raw_key();
    return rawkey_type == Readable ? str + x1 : str + x0;
}

///////////////////////////////////////
/////    high performance scan    /////
///////////////////////////////////////
ResultStreamBatchImpl::ResultStreamBatchImpl(TableImpl* table, ScanDescImpl* scan_desc)
    : ResultStreamImpl(table, scan_desc),
    cv_(&mu_), session_retry_(0), ref_count_(1) {
    // do something startup
    sliding_window_.resize(FLAGS_tera_sdk_max_batch_scan_req);
    session_end_key_ = scan_desc_impl_->GetStartRowKey();
    slot_last_key_.set_key(session_end_key_);
    slot_last_key_.set_timestamp(INT64_MAX);

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
    VLOG(28) << "get rpc handle, part_of_session_ " << part_of_session_
        << ", session_id_ " << session_id_ << ", response " << (uint64_t)(*response_ptr);
}

// insure table_impl no more use scan_impl
void ResultStreamBatchImpl::ReleaseRpcHandle(ScanTabletRequest* request,
                                             ScanTabletResponse* response) {
    delete request;
    uint64_t response_ptr = (uint64_t)(response);
    delete response;

    MutexLock mutex(&mu_);
    ref_count_--;
    VLOG(28) << "release rpc handle and wakeup, ref_count_ " << ref_count_
        << ", response " << response_ptr;
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
            slot->cell_.CopyFrom(response->results());
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

void ResultStreamBatchImpl::ComputeStartKey(const KeyValuePair& kv, KeyValuePair* start_key) {
    if (scan_desc_impl_->IsKvOnlyTable()) { // kv, set next key
        start_key->set_key(GetNextStartPoint(kv.key()));
        start_key->set_column_family(kv.column_family());
        start_key->set_qualifier(kv.qualifier());
        start_key->set_timestamp(kv.timestamp());
    } else if (kv.timestamp() == 0) { // table timestamp == 0
        start_key->set_key(kv.key());
        start_key->set_column_family(kv.column_family());
        start_key->set_qualifier(GetNextStartPoint(kv.qualifier()));
        start_key->set_timestamp(INT64_MAX);
    } else { // table has timestamp > 0
        start_key->set_key(kv.key());
        start_key->set_column_family(kv.column_family());
        start_key->set_qualifier(kv.qualifier());
        start_key->set_timestamp(kv.timestamp() - 1);
    }
    return;
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
    KeyValuePair start_key;
    ComputeStartKey(slot_last_key_, &start_key);
    scan_desc_impl_->SetStart(start_key.key(), start_key.column_family(),
                              start_key.qualifier(), start_key.timestamp());
    VLOG(28) << "scan session reset, start key " << start_key.key()
        << ", ref_count " << ref_count_;
    mu_.Unlock();
    // do io, release lock
    for (int32_t i = 0; i < FLAGS_tera_sdk_max_batch_scan_req; i++) {
        table_ptr_->ScanTabletAsync(this);
        part_of_session_ = true;
    }
    mu_.Lock();
}

void ResultStreamBatchImpl::ClearAndScanNextSlot(bool scan_next) {
    mu_.AssertHeld();
    ScanSlot* slot = &(sliding_window_[sliding_window_idx_]);
    assert(next_idx_ == slot->cell_.key_values_size());
    if (next_idx_ > 0) { // update last slot kv_pair
        slot_last_key_.CopyFrom(slot->cell_.key_values(next_idx_ - 1));
    }
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
        table_ptr_->ScanTabletAsync(this);
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
        while (slot->state_ == SCANSLOT_INVALID) {
            // stale results_id, re-enable another scan req
            if (session_error_ != kTabletNodeOk) {
                // TODO: kKeyNotInRange, do reset session
                LOG(WARNING) << "[RETRY " << ++session_retry_ << "] scan session error: "
                    << StatusCodeToString(session_error_)
                    << ", data_idx " << session_data_idx_ << ", slice_idx " << sliding_window_idx_;
                assert(session_done_);
                if (session_retry_ <= FLAGS_tera_sdk_batch_scan_max_retry) {
                    break;
                }

                // give up scan, report session error
                if (error) {
                    error->SetFailed(ErrorCode::kSystem, StatusCodeToString(session_error_));
                }
                return true;
            }
            if (ref_count_ == 1) {
                // check wether ts refuse scan
                if (error) {
                    error->SetFailed(ErrorCode::kSystem, StatusCodeToString(session_error_));
                }
                LOG(WARNING) << "[CHECK]: ts refuse scan, scan later.\n";
                return true;
            }
            cv_.Wait();
        }
        if (slot->state_ == SCANSLOT_INVALID) { // TODO: error break,  maybe delay retry
            while (ref_count_ > 1) { cv_.Wait();}
            cv_.TimeWaitInUs(FLAGS_batch_scan_delay_retry_in_us, "BatchScanRetryTimeWait");
            ScanSessionReset();
            continue;
        }

        // slot valid
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
        const string& scan_end_key = scan_desc_impl_->GetEndRowKey();
        if (session_end_key_ == "" || (scan_end_key != "" && session_end_key_ >= scan_end_key)) {
            VLOG(28) << "scan done, scan_end_key " << scan_end_key << ", session_end_key " << session_end_key_;
            return true;
        }

        // scan next tablet
        slot_last_key_.set_key(session_end_key_);
        slot_last_key_.set_timestamp(INT64_MAX);
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
      response_(new tera::ScanTabletResponse),
      result_pos_(0),
      finish_cond_(&finish_mutex_),
      retry_times_(0),
      finish_(false) {
    table_ptr_->ScanTabletSync(this);
}

ResultStreamSyncImpl::~ResultStreamSyncImpl() {
    if (response_ != NULL) {
        delete response_;
    }
}

bool ResultStreamSyncImpl::LookUp(const string& row_key) {
    return true;
}

bool ResultStreamSyncImpl::Done(ErrorCode* err) {
    while (1) {
        const string& scan_end_key = scan_desc_impl_->GetEndRowKey();
        /// scan failed
        while (response_->status() != kTabletNodeOk &&
               retry_times_ <= FLAGS_tera_sdk_sync_scan_max_retry) {
            LOG(WARNING) << "[RETRY " << ++retry_times_ << "] scan error: "
                         << StatusCodeToString(response_->status());

            int64_t wait_time;
            if(response_->status() == kKeyNotInRange) {
                wait_time = FLAGS_sync_scan_delay_retry_in_ms;
            } else {
                /// Wait less than 60 seconds
                wait_time = std::min(static_cast<int64_t>(FLAGS_sync_scan_delay_retry_in_ms * (1 << (retry_times_ - 1))),
                                     static_cast<int64_t>(60000));
            }

            delete response_;
            response_ = new tera::ScanTabletResponse;
            result_pos_ = 0;
            Reset();

            ThisThread::Sleep(wait_time);
            table_ptr_->ScanTabletSync(this);
        }

        if(response_->status() != kTabletNodeOk) {
            if (err) {
                err->SetFailed(ErrorCode::kSystem,
                                StatusCodeToString(response_->status()));
            }
            return true;
        }

        if (result_pos_ < response_->results().key_values_size()) {
            break;
        }
        const string& tablet_end_key = response_->end();
        if (response_->complete() &&
            (tablet_end_key == "" || (scan_end_key != "" && tablet_end_key >= scan_end_key))) {
            if (err) {
                err->SetFailed(ErrorCode::kOK);
            }
            return true;
        }

        // Newer version of TS will return next_start_point when the opration is timeout
        if (!response_->complete()) {
            // Without next_start_point, kv is the last kv pair from last scan
            if (response_->next_start_point().key() == "") {
                const KeyValuePair& kv = response_->results().key_values(result_pos_ - 1);
                if (scan_desc_impl_->IsKvOnlyTable()) {
                    scan_desc_impl_->SetStart(GetNextStartPoint(kv.key()), kv.column_family(),
                                              kv.qualifier(), kv.timestamp());
                } else if (kv.timestamp() == 0) {
                    scan_desc_impl_->SetStart(kv.key(), kv.column_family(),
                                              GetNextStartPoint(kv.qualifier()), INT64_MAX);
                } else {
                    scan_desc_impl_->SetStart(kv.key(), kv.column_family(),
                                              kv.qualifier(), kv.timestamp() - 1);
                }
            // next_start_point is where the next scan should start
            } else {
                const KeyValuePair& kv = response_->next_start_point();
                scan_desc_impl_->SetStart(kv.key(), kv.column_family(),
                                          kv.qualifier(), kv.timestamp());
            }
        } else {
            scan_desc_impl_->SetStart(tablet_end_key);
        }
        result_pos_ = 0;
        delete response_;
        response_ = new tera::ScanTabletResponse;
        Reset();
        table_ptr_->ScanTabletSync(this);
    }
    return false;
}

void ResultStreamSyncImpl::Next() {
    ++result_pos_;
}

string ResultStreamSyncImpl::RowName() const {
    return response_->results().key_values(result_pos_).key();
}

string ResultStreamSyncImpl::ColumnName() const {
    if (response_->results().key_values(result_pos_).has_column_family()) {
        const string& family =  response_->results().key_values(result_pos_).column_family();
        if (response_->results().key_values(result_pos_).has_qualifier()) {
            return (family + ":" + response_->results().key_values(result_pos_).qualifier());
        }
        return family;
    }
    return "";
}

string ResultStreamSyncImpl::Family() const {
    if (response_->results().key_values(result_pos_).has_column_family()) {
        return response_->results().key_values(result_pos_).column_family();
    }
    return "";
}

string ResultStreamSyncImpl::Qualifier() const {
    if (response_->results().key_values(result_pos_).has_qualifier()) {
        return response_->results().key_values(result_pos_).qualifier();
    }
    return "";
}

int64_t ResultStreamSyncImpl::Timestamp() const {
    if (response_->results().key_values(result_pos_).has_timestamp()) {
        return response_->results().key_values(result_pos_).timestamp();
    }
    return 0;
}

string ResultStreamSyncImpl::Value() const {
    if (response_->results().key_values(result_pos_).has_value()) {
        return response_->results().key_values(result_pos_).value();
    }
    return "";
}

int64_t ResultStreamSyncImpl::ValueInt64() const {
    std::string v;
    if (response_->results().key_values(result_pos_).has_value()) {
        v = response_->results().key_values(result_pos_).value();
    }
    return (v.size() == sizeof(int64_t)) ? *(int64_t*)v.c_str() : 0;
}

void ResultStreamSyncImpl::GetRpcHandle(ScanTabletRequest** request,
                                    ScanTabletResponse** response) {
    *request = new ScanTabletRequest;
    *response = response_;
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
    MutexLock locker(&finish_mutex_);
    while (!finish_) {
        finish_cond_.Wait();
    }
}

void ResultStreamSyncImpl::Signal() {
    MutexLock locker(&finish_mutex_);
    finish_ = true;
    finish_cond_.Signal();
}

void ResultStreamSyncImpl::Reset() {
    MutexLock locker(&finish_mutex_);
    finish_ = false;
}

///////////////////////// ScanDescImpl ///////////////////////

ScanDescImpl::ScanDescImpl(const string& rowkey)
    : start_timestamp_(0),
      timer_range_(NULL),
      buf_size_(FLAGS_tera_sdk_scan_buffer_size),
      number_limit_(FLAGS_tera_sdk_scan_number_limit),
      is_async_(FLAGS_tera_sdk_batch_scan_enabled),
      max_version_(1),
      max_qualifiers_(std::numeric_limits<uint64_t>::max()),
      pack_interval_(FLAGS_tera_sdk_scan_timeout),
      snapshot_(0),
      value_converter_(&DefaultValueConverter) {
    SetStart(rowkey);
}

ScanDescImpl::ScanDescImpl(const ScanDescImpl& impl)
    : start_key_(impl.start_key_),
      end_key_(impl.end_key_),
      start_column_family_(impl.start_column_family_),
      start_qualifier_(impl.start_qualifier_),
      start_timestamp_(impl.start_timestamp_),
      buf_size_(impl.buf_size_),
      number_limit_(impl.number_limit_),
      is_async_(impl.is_async_),
      max_version_(impl.max_version_),
      max_qualifiers_(impl.max_qualifiers_),
      pack_interval_(impl.pack_interval_),
      snapshot_(impl.snapshot_),
      table_schema_(impl.table_schema_) {
    value_converter_ = impl.GetValueConverter();
    filter_string_ = impl.GetFilterString();
    filter_list_ = impl.GetFilterList();
    if (impl.GetTimerRange() != NULL) {
        timer_range_ = new tera::TimeRange;
        timer_range_->CopyFrom(*(impl.GetTimerRange()));
    } else {
        timer_range_ = NULL;
    }
    for (int32_t i = 0; i < impl.GetSizeofColumnFamilyList(); ++i) {
        cf_list_.push_back(new tera::ColumnFamily(*(impl.GetColumnFamily(i))));
    }
}

ScanDescImpl::~ScanDescImpl() {
    if (timer_range_ != NULL) {
        delete timer_range_;
    }
    for (uint32_t i = 0; i < cf_list_.size(); ++i) {
        delete cf_list_[i];
    }
}

void ScanDescImpl::SetStart(const string& row_key, const string& column_family,
                            const string& qualifier, int64_t time_stamp)
{
    start_key_ = row_key;
    start_column_family_ = column_family;
    start_qualifier_ = qualifier;
    start_timestamp_ = time_stamp;
}

void ScanDescImpl::SetEnd(const string& rowkey) {
    end_key_ = rowkey;
}

void ScanDescImpl::AddColumnFamily(const string& cf) {
    AddColumn(cf, "");
}

void ScanDescImpl::AddColumn(const string& cf, const string& qualifier) {
    for (uint32_t i = 0; i < cf_list_.size(); ++i) {
        if (cf_list_[i]->family_name() == cf) {
            if (qualifier != "") {
                cf_list_[i]->add_qualifier_list(qualifier);
            }
            return;
        }
    }
    tera::ColumnFamily* column_family = new tera::ColumnFamily;
    column_family->set_family_name(cf);
    if (qualifier != "") {
        column_family->add_qualifier_list(qualifier);
    }
    cf_list_.push_back(column_family);
}

void ScanDescImpl::SetMaxVersions(int32_t versions) {
    max_version_ = versions;
}

void ScanDescImpl::SetMaxQualifiers(int64_t max_qualifiers) {
    max_qualifiers_ = max_qualifiers;
}

void ScanDescImpl::SetPackInterval(int64_t interval) {
    pack_interval_ = interval;
}

void ScanDescImpl::SetTimeRange(int64_t ts_end, int64_t ts_start) {
    if (timer_range_ == NULL) {
        timer_range_ = new tera::TimeRange;
    }
    timer_range_->set_ts_start(ts_start);
    timer_range_->set_ts_end(ts_end);
}

void ScanDescImpl::SetValueConverter(ValueConverter convertor) {
    value_converter_ = convertor;
}

void ScanDescImpl::SetSnapshot(uint64_t snapshot_id) {
    snapshot_ = snapshot_id;
}

uint64_t ScanDescImpl::GetSnapshot() const {
    return snapshot_;
}

void ScanDescImpl::SetBufferSize(int64_t buf_size) {
    buf_size_ = buf_size;
}

void ScanDescImpl::SetNumberLimit(int64_t number_limit) {
    number_limit_ = number_limit;
}

void ScanDescImpl::SetAsync(bool async) {
    is_async_ = async;
}

const string& ScanDescImpl::GetStartRowKey() const {
    return start_key_;
}

const string& ScanDescImpl::GetEndRowKey() const {
    return end_key_;
}

const string& ScanDescImpl::GetStartColumnFamily() const {
    return start_column_family_;
}

const string& ScanDescImpl::GetStartQualifier() const {
    return start_qualifier_;
}

int64_t ScanDescImpl::GetStartTimeStamp() const {
    return start_timestamp_;
}

int32_t ScanDescImpl::GetSizeofColumnFamilyList() const {
    return cf_list_.size();
}

const tera::ColumnFamily* ScanDescImpl::GetColumnFamily(int32_t num) const {
    if (static_cast<uint64_t>(num) >= cf_list_.size()) {
        return NULL;
    }
    return cf_list_[num];
}

int32_t ScanDescImpl::GetMaxVersion() const {
    return max_version_;
}

int64_t ScanDescImpl::GetMaxQualifiers() const {
    return max_qualifiers_;
}

int64_t ScanDescImpl::GetPackInterval() const {
    return pack_interval_;
}

const tera::TimeRange* ScanDescImpl::GetTimerRange() const {
    return timer_range_;
}

const string& ScanDescImpl::GetFilterString() const {
    return filter_string_;
}

const FilterList& ScanDescImpl::GetFilterList() const {
    return filter_list_;
}

const ValueConverter ScanDescImpl::GetValueConverter() const {
    return value_converter_;
}

int64_t ScanDescImpl::GetBufferSize() const {
    return buf_size_;
}

int64_t ScanDescImpl::GetNumberLimit() {
    return number_limit_;
}

bool ScanDescImpl::IsAsync() const {
    return is_async_;
}

void ScanDescImpl::SetTableSchema(const TableSchema& schema) {
    table_schema_ = schema;
}

bool ScanDescImpl::IsKvOnlyTable() {
    return IsKvTable(table_schema_);
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
        filter_string_ = where;
        if (!ParseFilterString()) {
            return false;
        }
    }
    return true;
}

bool ScanDescImpl::ParseFilterString() {
    const char* and_op = " AND ";
    filter_list_.Clear();
    std::vector<string> filter_v;
    SplitString(filter_string_, and_op, &filter_v);
    for (size_t i = 0; i < filter_v.size(); ++i) {
        Filter filter;
        if (ParseSubFilterString(filter_v[i], &filter)) {
            Filter* pf = filter_list_.add_filter();
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

    if (max_version_ != 1) {
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
    if (!value_converter_(value, type, &value_internal)) {
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

