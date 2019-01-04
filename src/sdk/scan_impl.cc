// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/scan_impl.h"

#include <functional>
#include <algorithm>
#include <unistd.h>

#include "common/this_thread.h"
#include "common/base/string_ext.h"

#include "proto/proto_helper.h"
#include "proto/table_schema.pb.h"
#include "sdk/filter_utils.h"
#include "sdk/sdk_utils.h"
#include "sdk/table_impl.h"
#include "common/atomic.h"
#include "common/timer.h"

DECLARE_int64(tera_sdk_scan_number_limit);
DECLARE_int64(tera_sdk_scan_buffer_size);
DECLARE_int32(tera_sdk_max_batch_scan_req);
DECLARE_int32(tera_sdk_scan_max_retry);
DECLARE_int64(tera_sdk_scan_timeout);
DECLARE_int64(tera_sdk_scan_delay_retry_in_us);

DECLARE_bool(debug_tera_sdk_scan);

namespace tera {

ResultStreamImpl::ResultStreamImpl(TableImpl* table, ScanDescImpl* scan_desc_impl)
    : cv_(&mu_),
      scan_desc_impl_(new ScanDescImpl(*scan_desc_impl)),
      table_ptr_(table),
      session_retry_(0),
      ref_count_(1),
      data_size_(0),
      row_count_(0),
      last_key_(""),
      canceled_(false) {
  // do something startup
  sliding_window_.resize(FLAGS_tera_sdk_max_batch_scan_req);
  session_end_key_ = scan_desc_impl_->GetStartRowKey();
  slot_last_key_.set_key(session_end_key_);
  slot_last_key_.set_timestamp(INT64_MAX);
  mu_.Lock();
  ScanSessionReset(true);
  mu_.Unlock();
}

ResultStreamImpl::~ResultStreamImpl() {
  // do something cleanup
  MutexLock mutex(&mu_);
  if (scan_desc_impl_ != NULL) {
    delete scan_desc_impl_;
  }
  ref_count_--;
  SCAN_LOG << "before wait scan task finsh, ref_count " << ref_count_;
  while (ref_count_ != 0) {
    cv_.Wait();
  }
}

ScanDescImpl* ResultStreamImpl::GetScanDesc() { return scan_desc_impl_; }

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

void ResultStreamImpl::GetRpcHandle(ScanTabletRequest** request_ptr,
                                    ScanTabletResponse** response_ptr) {
  *request_ptr = new ScanTabletRequest;
  *response_ptr = new ScanTabletResponse;

  MutexLock mutex(&mu_);
  (*request_ptr)->set_part_of_session(part_of_session_);
  (*request_ptr)->set_session_id((int64_t)session_id_);
  SCAN_LOG << "get rpc handle, part_of_session_ " << part_of_session_ << ", response "
           << (uint64_t)(*response_ptr);
}

// insure table_impl no more use scan_impl
void ResultStreamImpl::ReleaseRpcHandle(ScanTabletRequest* request, ScanTabletResponse* response) {
  delete request;
  uint64_t response_ptr = (uint64_t)(response);
  delete response;

  MutexLock mutex(&mu_);
  ref_count_--;
  SCAN_LOG << "release rpc handle and wakeup, ref_count_ " << ref_count_ << ", response "
           << response_ptr;
  cv_.Signal();
}

// scan request callback trigger:
//      1. drop resp condition:
//          1.1. stale session_id
//          1.2. stale result_id
//      2. handle session broken:
//          2.1. stop scan, and report error to user
//      3. scan success, notify user to consume result
void ResultStreamImpl::OnFinish(ScanTabletRequest* request, ScanTabletResponse* response) {
  MutexLock mutex(&mu_);
  // check session id
  if (request->session_id() != (int64_t)session_id_) {
    SCAN_LOG << "[OnFinish]session_id not match, request session id" << request->session_id();
  } else if (response->status() != kTabletNodeOk ||
             (response->status() == kTabletNodeOk && response->status() == kNotPermission)) {
    // rpc or ts error, session broken and report error
    session_error_ = response->status();
    SCAN_WLOG << "[OnFinish]broken error " << StatusCodeToString(session_error_);
    session_done_ = true;
  } else if (response->results_id() == 0 && response->results().key_values_size() == 0 &&
             request->part_of_session()) {
    // handle old ts, results_id not init
    SCAN_WLOG << "[OnFinish]batch scan old ts";
  } else if ((response->results_id() < session_data_idx_) ||
             (response->results_id() >= session_data_idx_ + FLAGS_tera_sdk_max_batch_scan_req)) {
    if (response->results_id() != std::numeric_limits<unsigned long>::max()) {
      SCAN_WLOG << "[OnFinish]session_data_idx " << session_data_idx_ << ", stale result_id "
                << response->results_id() << ", response " << (uint64_t)response;
      session_done_ = true;
      // TODO: ts state no known
      session_error_ = kRPCTimeout;
    }
  } else {  // scan success, cache result
    int32_t slot_idx = ((response->results_id() - session_data_idx_) + sliding_window_idx_) %
                       FLAGS_tera_sdk_max_batch_scan_req;
    SCAN_LOG << "[OnFinish]scan suc, slot_idx " << slot_idx << ", result_id "
             << response->results_id() << ", session_data_idx_ " << session_data_idx_
             << ", sliding_window_idx_ " << sliding_window_idx_ << ", resp.kv.size() "
             << response->results().key_values_size() << ", data_size " << response->data_size()
             << ", row_count " << response->row_count();
    UpdateRowCount(response->row_count());
    UpdateDataSize(response->data_size());
    ScanSlot* slot = &(sliding_window_[slot_idx]);
    if (slot->state_ == SCANSLOT_INVALID) {
      slot->state_ = SCANSLOT_VALID;
      slot->cell_.CopyFrom(response->results());
      if (slot->cell_.key_values_size() > 0) {
        UpdateLastKey(slot->cell_.key_values(slot->cell_.key_values_size() - 1));
      }
      SCAN_LOG << "[OnFinish]cache scan result, slot_idx " << slot_idx << ", kv.size() "
               << slot->cell_.key_values_size() << ", resp.kv.size() "
               << response->results().key_values_size();
    }
    if (response->complete()) {
      session_last_idx_ =
          (session_last_idx_ > response->results_id()) ? response->results_id() : session_last_idx_;
      session_end_key_ = response->end();
      SCAN_LOG << "[OnFinish]scan complete: session_end_key " << session_end_key_
               << ", session_last_idx " << session_last_idx_;
      session_done_ = true;
    }
  }
  return;
}

void ResultStreamImpl::ComputeStartKey(const KeyValuePair& kv, KeyValuePair* start_key) {
  if (scan_desc_impl_->IsKvOnlyTable()) {  // kv, set next key
    start_key->set_key(GetNextStartPoint(kv.key()));
    start_key->set_column_family(kv.column_family());
    start_key->set_qualifier(kv.qualifier());
    start_key->set_timestamp(kv.timestamp());
  } else if (kv.timestamp() == 0) {  // table timestamp == 0
    start_key->set_key(kv.key());
    start_key->set_column_family(kv.column_family());
    start_key->set_qualifier(GetNextStartPoint(kv.qualifier()));
    start_key->set_timestamp(INT64_MAX);
  } else {  // table has timestamp > 0
    start_key->set_key(kv.key());
    start_key->set_column_family(kv.column_family());
    start_key->set_qualifier(kv.qualifier());
    start_key->set_timestamp(kv.timestamp() - 1);
  }
  return;
}

void ResultStreamImpl::ScanSessionReset(bool reset_retry) {
  mu_.AssertHeld();
  // reset session parameter
  uint64_t pre_session_id = session_id_;
  StatusCode pre_session_error = session_error_;
  bool pre_session_done = session_done_;
  std::string pre_session_end_key = session_end_key_;
  if (reset_retry) {
    session_retry_ = 0;
  }
  uint64_t tid = (uint64_t)pthread_self();
  session_id_ = ((tid << 48) | ((uint64_t)get_micros())) & (0x7ffffffffffff);
  session_done_ = false;
  session_error_ = kTabletNodeOk;
  part_of_session_ = false;
  session_data_idx_ = 0;
  session_last_idx_ = UINT32_MAX;
  sliding_window_idx_ = 0;
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
  scan_desc_impl_->SetStart(start_key.key(), start_key.column_family(), start_key.qualifier(),
                            start_key.timestamp());
  SCAN_LOG << "session reset [start key " << start_key.key() << ", session_retry " << session_retry_
           << ", ref_count " << ref_count_ << "], previous session info [session_id "
           << pre_session_id << ", session_error " << StatusCodeToString(pre_session_error)
           << ", session_done " << pre_session_done << ", session_end_key " << pre_session_end_key
           << "]";
  mu_.Unlock();
  // do io, release lock
  for (int32_t i = 0; i < FLAGS_tera_sdk_max_batch_scan_req; i++) {
    table_ptr_->ScanTabletAsync(this);
    part_of_session_ = true;
  }
  mu_.Lock();
}

void ResultStreamImpl::ClearAndScanNextSlot(bool scan_next) {
  mu_.AssertHeld();
  ScanSlot* slot = &(sliding_window_[sliding_window_idx_]);
  assert(next_idx_ == slot->cell_.key_values_size());
  if (next_idx_ > 0) {  // update last slot kv_pair
    slot_last_key_.CopyFrom(slot->cell_.key_values(next_idx_ - 1));
  }
  slot->cell_.Clear();
  slot->state_ = SCANSLOT_INVALID;
  next_idx_ = 0;
  session_data_idx_++;
  sliding_window_idx_ = (sliding_window_idx_ + 1) % FLAGS_tera_sdk_max_batch_scan_req;
  SCAN_LOG << " session_data_idx_ " << session_data_idx_ << ", sliding_window_idx_ "
           << sliding_window_idx_ << ", ref_count_ " << ref_count_;
  if (scan_next) {
    ref_count_++;
    mu_.Unlock();
    table_ptr_->ScanTabletAsync(this);
    mu_.Lock();
  }
  return;
}

bool ResultStreamImpl::Done(ErrorCode* error) {
  if (error) {
    error->SetFailed(ErrorCode::kOK);
  }
  MutexLock mutex(&mu_);
  while (1) {
    if (canceled_) {
      LOG(INFO) << "This scan is cancelled.\n";
      return true;
    }
    // not wait condition:
    //  1. current slot valid, or
    //  2. ts not available, or
    //  3. rpc not available, or
    ScanSlot* slot = &(sliding_window_[sliding_window_idx_]);
    while (slot->state_ == SCANSLOT_INVALID) {
      // stale results_id, re-enable another scan req
      if (session_error_ != kTabletNodeOk) {
        // TODO: kKeyNotInRange, do reset session
        SCAN_LOG << "[RETRY " << ++session_retry_
                 << "] scan session error: " << StatusCodeToString(session_error_)
                 << ", session_end_key " << session_end_key_ << ", data_idx " << session_data_idx_
                 << ", slice_idx " << sliding_window_idx_;
        assert(session_done_);
        if (session_retry_ <= FLAGS_tera_sdk_scan_max_retry) {
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
    if (slot->state_ == SCANSLOT_INVALID) {  // TODO: error break,  maybe delay retry
      while (ref_count_ > 1) {
        cv_.Wait();
      }
      cv_.TimeWaitInUs(FLAGS_tera_sdk_scan_delay_retry_in_us, "ScanRetryTimeWait");
      ScanSessionReset(false);
      continue;
    }

    // slot valid, break here to read current slot continue
    if (next_idx_ < slot->cell_.key_values_size()) {
      break;
    }

    SCAN_LOG << "session_done_ " << session_done_ << ", session_data_idx_ " << session_data_idx_
             << ", session_last_idx_ " << session_last_idx_;
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
      SCAN_LOG << "scan done, scan_end_key " << scan_end_key << ", session_end_key "
               << session_end_key_;
      return true;
    }

    // scan next tablet
    slot_last_key_.set_key(session_end_key_);
    slot_last_key_.set_timestamp(INT64_MAX);
    ScanSessionReset(true);
  }
  return false;
}

void ResultStreamImpl::Next() {
  assert(!canceled_);
  next_idx_++;
}
bool ResultStreamImpl::LookUp(const std::string& row_key) { return true; }
std::string ResultStreamImpl::RowName() const {
  const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
  if (!row.has_key()) {
    return "";
  }
  return row.key();
}
std::string ResultStreamImpl::Family() const {
  const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
  return row.has_column_family() ? row.column_family() : "";
}
std::string ResultStreamImpl::Qualifier() const {
  const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
  return row.has_qualifier() ? row.qualifier() : "";
}
std::string ResultStreamImpl::ColumnName() const {
  const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
  if (!row.has_column_family() && !row.has_qualifier()) {
    return "";
  } else {
    const std::string& cf = Family();
    const std::string& qu = Qualifier();
    return cf + ":" + qu;
  }
}
int64_t ResultStreamImpl::Timestamp() const {
  const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
  return row.has_timestamp() ? row.timestamp() : 0;
}
std::string ResultStreamImpl::Value() const {
  const KeyValuePair& row = sliding_window_[sliding_window_idx_].cell_.key_values(next_idx_);
  return row.has_value() ? row.value() : "";
}
int64_t ResultStreamImpl::ValueInt64() const {
  std::string v = Value();
  return (v.size() == sizeof(int64_t)) ? *(int64_t*)v.c_str() : 0;
}
// scan query and cancel
uint64_t ResultStreamImpl::GetDataSize() const { return data_size_; }
uint64_t ResultStreamImpl::GetRowCount() const { return row_count_; }
std::string ResultStreamImpl::GetLastKey() const { return last_key_; }
void ResultStreamImpl::Cancel() { canceled_ = true; }

void ResultStreamImpl::UpdateRowCount(uint32_t row_count) { row_count_ += row_count; }
void ResultStreamImpl::UpdateDataSize(uint32_t data_size) { data_size_ += data_size; }
void ResultStreamImpl::UpdateLastKey(const KeyValuePair& kv) { last_key_ = kv.key(); }

///////////////////////// ScanDescImpl ///////////////////////
ScanDescImpl::ScanDescImpl(const string& rowkey)
    : start_timestamp_(0),
      timer_range_(NULL),
      buf_size_(FLAGS_tera_sdk_scan_buffer_size),
      number_limit_(FLAGS_tera_sdk_scan_number_limit),
      max_version_(1),
      max_qualifiers_(std::numeric_limits<uint64_t>::max()),
      scan_slot_timeout_(FLAGS_tera_sdk_scan_timeout),
      snapshot_(0),
      filter_desc_(NULL) {
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
      max_version_(impl.max_version_),
      max_qualifiers_(impl.max_qualifiers_),
      scan_slot_timeout_(impl.scan_slot_timeout_),
      snapshot_(impl.snapshot_),
      table_schema_(impl.table_schema_) {
  if (impl.GetFilterDesc()) {
    filter_desc_ = new filter::FilterDesc();
    filter_desc_->CopyFrom(*(impl.GetFilterDesc()));
  } else {
    filter_desc_ = NULL;
  }
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
  if (filter_desc_) {
    delete filter_desc_;
  }
}

void ScanDescImpl::SetStart(const string& row_key, const string& column_family,
                            const string& qualifier, int64_t time_stamp) {
  start_key_ = row_key;
  start_column_family_ = column_family;
  start_qualifier_ = qualifier;
  start_timestamp_ = time_stamp;
}

void ScanDescImpl::SetEnd(const string& rowkey) { end_key_ = rowkey; }

void ScanDescImpl::AddColumnFamily(const string& cf) { AddColumn(cf, ""); }

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

void ScanDescImpl::SetMaxVersions(int32_t versions) { max_version_ = versions; }

void ScanDescImpl::SetMaxQualifiers(int64_t max_qualifiers) { max_qualifiers_ = max_qualifiers; }

void ScanDescImpl::SetPackInterval(int64_t timeout) { scan_slot_timeout_ = timeout; }

void ScanDescImpl::SetTimeRange(int64_t ts_end, int64_t ts_start) {
  if (timer_range_ == NULL) {
    timer_range_ = new tera::TimeRange;
  }
  timer_range_->set_ts_start(ts_start);
  timer_range_->set_ts_end(ts_end);
}

void ScanDescImpl::SetSnapshot(uint64_t snapshot_id) { snapshot_ = snapshot_id; }

uint64_t ScanDescImpl::GetSnapshot() const { return snapshot_; }

void ScanDescImpl::SetBufferSize(int64_t buf_size) { buf_size_ = buf_size; }

void ScanDescImpl::SetNumberLimit(int64_t number_limit) { number_limit_ = number_limit; }

const string& ScanDescImpl::GetStartRowKey() const { return start_key_; }

const string& ScanDescImpl::GetEndRowKey() const { return end_key_; }

const string& ScanDescImpl::GetStartColumnFamily() const { return start_column_family_; }

const string& ScanDescImpl::GetStartQualifier() const { return start_qualifier_; }

int64_t ScanDescImpl::GetStartTimeStamp() const { return start_timestamp_; }

int32_t ScanDescImpl::GetSizeofColumnFamilyList() const { return cf_list_.size(); }

const tera::ColumnFamily* ScanDescImpl::GetColumnFamily(int32_t num) const {
  if (static_cast<uint64_t>(num) >= cf_list_.size()) {
    return NULL;
  }
  return cf_list_[num];
}

int32_t ScanDescImpl::GetMaxVersion() const { return max_version_; }

int64_t ScanDescImpl::GetMaxQualifiers() const { return max_qualifiers_; }

int64_t ScanDescImpl::GetPackInterval() const { return scan_slot_timeout_; }

const tera::TimeRange* ScanDescImpl::GetTimerRange() const { return timer_range_; }

filter::FilterDesc* ScanDescImpl::GetFilterDesc() const { return filter_desc_; }

int64_t ScanDescImpl::GetBufferSize() const { return buf_size_; }

int64_t ScanDescImpl::GetNumberLimit() { return number_limit_; }

void ScanDescImpl::SetTableSchema(const TableSchema& schema) { table_schema_ = schema; }

bool ScanDescImpl::IsKvOnlyTable() { return IsKvTable(table_schema_); }

bool ScanDescImpl::SetFilter(const filter::FilterPtr& filter) {
  if (max_version_ != 1) {
    LOG(ERROR) << "only support max_version of scanner == 1";
    return false;
  }
  if (!filter) {
    LOG(ERROR) << "filter is NULL";
    return false;
  }
  filter_desc_ = new filter::FilterDesc;
  filter_desc_->set_type(TransFilterType(filter->Type()));
  int ret = filter->SerializeTo(filter_desc_->mutable_serialized_filter());
  if (!ret) {
    delete filter_desc_;
    filter_desc_ = nullptr;
    LOG(ERROR) << "Filter Serialize Error";
    return false;
  }
  return true;
}

}  // namespace tera
