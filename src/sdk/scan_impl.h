// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_SCAN_IMPL_H_
#define TERA_SDK_SCAN_IMPL_H_

#include <list>
#include <queue>
#include <string>
#include <vector>

#include "common/event.h"
#include "proto/tabletnode_rpc.pb.h"
#include "sdk/sdk_task.h"
#include "tera.h"
#include "types.h"
#include "common/timer.h"
#include "proto/filter.pb.h"

namespace tera {

class TableImpl;

class ResultStreamImpl : public ResultStream {
 public:
  ResultStreamImpl(TableImpl* table, ScanDescImpl* scan_desc_impl);
  virtual ~ResultStreamImpl();

  bool LookUp(const std::string& row_key);  // TODO: result maybe search like a map
  bool Done(ErrorCode* err);                // wait until slot become valid
  void Next();                              // get next kv in RowResult

  std::string RowName() const;     // get row key
  std::string Family() const;      // get cf
  std::string Qualifier() const;   // get qu
  std::string ColumnName() const;  // get cf:qu
  int64_t Timestamp() const;       // get ts
  std::string Value() const;       // get value
  int64_t ValueInt64() const;      // get value as int64_t
  uint64_t GetDataSize() const;    // get total data size until last slot scan
  uint64_t GetRowCount() const;    // get total row count(kv count) until last slot scan
  std::string GetLastKey() const;  // get last key string until last slot scan
  void Cancel();                   // cancel the scan task

 public:
  ScanDescImpl* GetScanDesc();
  void GetRpcHandle(ScanTabletRequest** request,
                    ScanTabletResponse** response);  // alloc resource for scan session
  void ReleaseRpcHandle(ScanTabletRequest* request,
                        ScanTabletResponse* response);  // free resource for scan session
  void OnFinish(ScanTabletRequest* request, ScanTabletResponse* response);  // scan callback
  std::string GetNextStartPoint(const std::string& str);                    // for session reset

 private:
  void ClearAndScanNextSlot(bool scan_next);
  void ComputeStartKey(const KeyValuePair& kv, KeyValuePair* start_key);
  void ScanSessionReset(bool reset_retry);
  void UpdateDataSize(uint32_t data_size);     // get total data size until last slot scan
  void UpdateRowCount(uint32_t row_count);     // get total row count until last slot scan
  void UpdateLastKey(const KeyValuePair& kv);  // get last key until last slot scan

 private:
  mutable Mutex mu_;
  CondVar cv_;
  tera::ScanDescImpl* scan_desc_impl_;
  TableImpl* table_ptr_;

  int32_t session_retry_;
  int32_t ref_count_;  // use for scan_impl destory

  // session control
  uint64_t session_id_;        // client and ts use session id to finish channel negotiation
  bool session_done_;          // session is finish
  StatusCode session_error_;   // if error occur during scan, set error code.
  uint32_t session_data_idx_;  // current result id wait
  bool part_of_session_;       // TODO, should be deleted
  std::string session_end_key_;
  KeyValuePair slot_last_key_;
  uint32_t session_last_idx_;  // if session done, point to the last data_idx

  // sliding window control
  enum ScanSlotState {
    SCANSLOT_INVALID = 0,  // init state
    SCANSLOT_VALID = 1,    // slot can be read
  };
  typedef struct ScanSlot {
    uint64_t state_;  // ScanSlotState
    RowResult cell_;  // kv result
  } ScanSlot;
  std::vector<ScanSlot> sliding_window_;  // scan_slot buffer
  int32_t sliding_window_idx_;            // current slot index
  int32_t next_idx_;                      // offset in sliding_window[cur_buffer_idx]
  uint64_t data_size_;
  uint64_t row_count_;
  std::string last_key_;
  bool canceled_;

 private:
  ResultStreamImpl(const ResultStreamImpl&);
  void operator=(const ResultStreamImpl&);
};

class ScanDescImpl {
 public:
  ScanDescImpl(const std::string& rowkey);

  ScanDescImpl(const ScanDescImpl& impl);

  ~ScanDescImpl();

  void SetEnd(const std::string& rowkey);

  void AddColumnFamily(const std::string& cf);

  void AddColumn(const std::string& cf, const std::string& qualifier);

  void SetMaxVersions(int32_t versions);

  void SetMaxQualifiers(int64_t max_qualifiers);

  void SetPackInterval(int64_t timeout);

  void SetTimeRange(int64_t ts_end, int64_t ts_start);

  bool SetFilter(const filter::FilterPtr& filter);

  void SetSnapshot(uint64_t snapshot_id);

  void SetBufferSize(int64_t buf_size);

  void SetNumberLimit(int64_t number_limit);

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

  filter::FilterDesc* GetFilterDesc() const;

  int32_t GetMaxVersion() const;

  int64_t GetMaxQualifiers() const;

  int64_t GetPackInterval() const;

  uint64_t GetSnapshot() const;

  int64_t GetBufferSize() const;

  int64_t GetNumberLimit();

  void SetTableSchema(const TableSchema& schema);

  bool IsKvOnlyTable();

 private:
  std::string start_key_;
  std::string end_key_;
  std::string start_column_family_;
  std::string start_qualifier_;
  int64_t start_timestamp_;
  std::vector<tera::ColumnFamily*> cf_list_;
  tera::TimeRange* timer_range_;
  int64_t buf_size_;
  int64_t number_limit_;
  int32_t max_version_;
  int64_t max_qualifiers_;
  int64_t scan_slot_timeout_;
  uint64_t snapshot_;
  TableSchema table_schema_;
  filter::FilterDesc* filter_desc_;
};

struct ScanTask : public SdkTask {
  ResultStreamImpl* stream;
  tera::ScanTabletRequest* request;
  tera::ScanTabletResponse* response;

  ScanTask() : SdkTask(SdkTask::SCAN), stream(NULL), request(NULL), response(NULL) {}

  virtual bool IsAsync() { return false; }
  virtual uint32_t Size() { return 0; }
  virtual void SetTimeOut(int64_t timeout) {}
  virtual int64_t TimeOut() { return 0; }
  virtual void Wait() {}
  virtual void SetError(ErrorCode::ErrorCodeType err, const std::string& reason) {}
  std::string InternalRowKey() { return stream->GetScanDesc()->GetStartRowKey(); }

  virtual void RunCallback() { abort(); }         // Not implement this method
  virtual int64_t GetCommitTimes() { return 0; }  // Not implement this method
};

#define SCAN_LOG                                                               \
  LOG_IF(INFO, FLAGS_debug_tera_sdk_scan) << "sdk-scan[" << session_id_ << "]" \
                                                                           " "
#define SCAN_WLOG LOG(WARNING) << "sdk-scan[" << session_id_ << "] "

}  // namespace tera

#endif  // TERA_SDK_SCAN_IMPL_H_
