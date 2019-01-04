// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef TERA_SDK_GLOBAL_TXN_INTERNAL_H_
#define TERA_SDK_GLOBAL_TXN_INTERNAL_H_

#include <atomic>
#include <map>
#include <string>
#include <set>
#include <utility>

#include "common/mutex.h"
#include "io/coding.h"
#include "sdk/global_txn.h"
#include "sdk/test/global_txn_testutils.h"
#include "sdk/sdk_utils.h"
#include "sdk/single_row_txn.h"
#include "sdk/table_impl.h"
#include "sdk/read_impl.h"
#include "sdk/timeoracle_client_impl.h"
#include "tera.h"
#include "common/timer.h"

namespace tera {

class Cell;
class GlobalTxnTestHelper;
class Write;

inline void PrintCostTime(const std::string& msg, int64_t begin_time) {
  VLOG(12) << msg << " cost: " << get_micros() - begin_time;
}

inline std::string Int64ToEncodedString(int64_t n) {
  char buf[sizeof(int64_t)];
  io::EncodeBigEndian(buf, n);
  std::string s(buf, sizeof(int64_t));
  return s;
}

inline int64_t EncodedStringToInt64(const std::string& s) { return io::DecodeBigEndain(s.c_str()); }

inline std::string PackLockName(const std::string& qualifier) { return "!L" + qualifier; }

inline std::string PackWriteName(const std::string& qualifier) { return "!W" + qualifier; }

// make sure 'data' column sort after 'lock' and 'write' columns in same row
inline std::string PackDataName(const std::string& qualifier) { return qualifier; }

inline std::string EncodeLockValue(int type, const std::string& primary_str) {
  return (char)type + primary_str;
}

inline bool DecodeLockValue(const std::string& value, int* type, tera::PrimaryInfo* info) {
  if (value.length() > 1) {
    *type = (int)value[0];
    return info->ParseFromString(value.substr(1));
  } else {
    *type = -1;
    return false;
  }
}

inline std::string EncodeWriteValue(int type, int64_t timestamp) {
  return (char)type + Int64ToEncodedString(timestamp);
}

inline bool DecodeWriteValue(const std::string& value, int* type, int64_t* timestamp) {
  if (value.length() > 1) {
    *type = (int)value[0];
    *timestamp = EncodedStringToInt64(value.substr(1));
    return true;
  } else {
    *type = -1;
    *timestamp = -1;
    return false;
  }
}

inline std::string PackNotifyName(const std::string& column_family, const std::string& qualifier) {
  return column_family + ":" + qualifier;
}

inline bool BadQualifier(const std::string& qualifier) {
  size_t q_len = qualifier.length();
  return q_len > 0 && qualifier[0] == '!';
}

class Cell {
 public:
  Cell(tera::Table* table, const std::string& row_key, const std::string& column_family,
       const std::string& qualifier, const int64_t timestamp = 0, const std::string& value = "")
      : table_(table),
        row_key_(row_key),
        column_family_(column_family),
        qualifier_(qualifier),
        timestamp_(timestamp),
        value_(value),
        tablename_("") {
    assert(table_ != NULL);
    tablename_ = table_->GetName();
  }

  tera::Table* Table() const { return table_; }

  const std::string TableName() const { return tablename_; }
  const std::string& RowKey() const { return row_key_; }
  const std::string& ColFamily() const { return column_family_; }
  const std::string& Qualifier() const { return qualifier_; }
  const std::string LockName() const { return PackLockName(qualifier_); }
  const std::string WriteName() const { return PackWriteName(qualifier_); }
  const std::string DataName() const { return PackDataName(qualifier_); }
  const std::string NotifyName() const { return PackNotifyName(column_family_, qualifier_); }
  const int64_t Timestamp() const { return timestamp_; }
  void SetTimestamp(const int64_t timestamp) { timestamp_ = timestamp; }
  const std::string& Value() const { return value_; }
  void SetValue(const std::string& value) { value_ = value; }

 private:
  tera::Table* table_;
  std::string row_key_;
  std::string column_family_;
  std::string qualifier_;
  int64_t timestamp_;
  std::string value_;
  std::string tablename_;
};

class Write {
 public:
  Write(const Cell& cell, const int& type = 0) : cell_(cell), type_(type), is_primary_(false) {}

  int WriteType() const { return type_; }
  bool IsPrimary() const { return is_primary_; }
  tera::Table* Table() const { return cell_.Table(); }
  const std::string TableName() const { return cell_.TableName(); }
  const std::string& RowKey() const { return cell_.RowKey(); }
  const std::string& ColFamily() const { return cell_.ColFamily(); }
  const std::string& Qualifier() const { return cell_.Qualifier(); }
  const std::string LockName() const { return cell_.LockName(); }
  const std::string WriteName() const { return cell_.WriteName(); }
  const std::string DataName() const { return cell_.DataName(); }
  const std::string NotifyName() const { return cell_.NotifyName(); }
  const int64_t Timestamp() const { return cell_.Timestamp(); }
  const std::string& Value() const { return cell_.Value(); }
  const int64_t GetSize() {
    return cell_.RowKey().length() + cell_.ColFamily().length() + cell_.Qualifier().length() +
           cell_.Value().length();
  }
  bool IsSameRow(Write* w) { return RowKey() == w->RowKey() && Table() == w->Table(); }

  void Serialize(const int64_t start_ts, const std::string& session, std::string* primary_info) {
    tera::PrimaryInfo primary;
    primary.set_table_name(TableName());
    primary.set_row_key(RowKey());
    primary.set_column_family(ColFamily());
    primary.set_qualifier(Qualifier());
    primary.set_gtxn_start_ts(start_ts);
    primary.set_client_session(session), primary.SerializeToString(primary_info);
  }

  const std::string DebugString() const {
    std::stringstream ss;
    ss << "[" << TableName() << ":" << RowKey() << ":" << ColFamily() << ":" << Qualifier() << "]";
    return ss.str();
  }

 private:
  tera::Cell cell_;
  int type_;
  bool is_primary_;
};

struct PrewriteContext {
  std::vector<Write>* ws;
  Transaction* gtxn;
  Transaction* stxn;
  std::string table_name;
  std::string row_key;
  ErrorCode status;
  PrewriteContext(std::vector<Write>* same_row_ws, Transaction* g, Transaction* s,
                  const std::string& tablename, const std::string& rowkey)
      : ws(same_row_ws), gtxn(g), stxn(s), table_name(tablename), row_key(rowkey) {
    status.SetFailed(ErrorCode::kOK);
  }
  const std::string DebugString() const {
    return "[tablename=" + table_name + ",rowkey=" + row_key + "]" + status.ToString();
  }
};
// one user reader will have one InternalReaderContext
struct InternalReaderContext {
  int expected_cell_cnt;
  int active_cell_cnt;
  int fail_cell_cnt;
  int not_found_cnt;
  RowReader* user_reader;
  Transaction* gtxn;
  std::map<Cell*, int> cell_map;
  RowResult results;
  ErrorCode last_err;

  InternalReaderContext(int expected_cnt, RowReader* reader, Transaction* txn)
      : expected_cell_cnt(expected_cnt),
        active_cell_cnt(0),
        fail_cell_cnt(0),
        not_found_cnt(0),
        user_reader(reader),
        gtxn(txn) {}

  ~InternalReaderContext() {
    for (auto it = cell_map.begin(); it != cell_map.end();) {
      Cell* cell = it->first;
      cell_map.erase(it++);
      delete cell;
      cell = NULL;
    }
  }
};

// one cell reader will have one CellReaderContext
struct CellReaderContext {
  Cell* cell;
  InternalReaderContext* internal_reader_ctx;
  ErrorCode status;
  CellReaderContext(Cell* c, InternalReaderContext* ctx) : cell(c), internal_reader_ctx(ctx) {}
};

struct PrimaryTxnContext {
  Transaction* gtxn;
  Transaction* stxn;
  PrimaryTxnContext(Transaction* g, Transaction* s) : gtxn(g), stxn(s) {}
};

class GlobalTxnInternal {
 public:
  friend class GlobalTxn;
  GlobalTxnInternal(std::shared_ptr<tera::ClientImpl> client_impl);

  ~GlobalTxnInternal();
  // for common
  void SetStartTimestamp(int64_t ts);

  bool CheckTable(Table* table, ErrorCode* status);

  Table* FindTable(const std::string& tablename);

  bool IsPrimary(const tera::Cell& cell, const tera::PrimaryInfo& primary_info);

  bool IsGTxnColumnFamily(const std::string& tablename, const std::string& column_family);

  // for get
  bool VerifyUserRowReader(RowReader* user_reader);

  bool PrimaryIsLocked(const tera::PrimaryInfo& primary_info, const int64_t lock_ts,
                       ErrorCode* status);

  bool IsLockedByOthers(RowReader::TRow& row, const tera::Cell& cell);

  bool SuspectLive(const tera::PrimaryInfo& primary_info);

  // for prewrite
  void BuildRowReaderForPrewrite(const std::vector<Write>& ws, RowReader* reader);

  void BuildRowMutationForPrewrite(std::vector<Write>* ws, RowMutation* txn_mu,
                                   const std::string& primary_info);

  bool ConflictWithOtherWrite(const std::vector<Write>* ws,
                              const std::unique_ptr<RowReaderImpl>& reader, ErrorCode* status);

  // for applyMutation
  bool VerifyUserRowMutation(RowMutation* user_mu);
  bool VerifyWritesSize(RowMutation* user_mu, int64_t* size);

  // for commit
  void BuildRowMutationForCommit(std::vector<Write>* ws, RowMutation* txn_mu,
                                 const int64_t commit_ts);

  void BuildRowMutationForAck(std::vector<Write>* ws, RowMutation* txn_mu);

  void BuildRowMutationForNotify(std::vector<Write>* ws, RowMutation* txn_mu,
                                 const int64_t commit_ts);

  void SetPrewriteStartTimestamp(const int64_t prewrite_start_ts);

  // for timeout
  void SetCommitDuration(int64_t timeout_ms);
  void SetInternalSdkTaskTimeout(RowMutation* mutation);
  void SetInternalSdkTaskTimeout(RowReader* reader);
  bool IsTimeOut();

  // for other transaction alive
  std::string GetClientSession();

 private:
  // for pref
  void UpdateTimerCounter(Counter* c) { c->Set(get_micros() - c->Get()); }

  // for debug and test
  std::string DebugString(const tera::Cell& cell, const std::string& msg) const;
  int64_t TEST_Init(const std::string& conf_file);
  void TEST_Sleep();
  void TEST_GetSleep();
  void TEST_Destory();
  int64_t TEST_GetCommitTimestamp();
  int64_t TEST_GetPrewriteStartTimestamp();

  void PerfReadDelay(int64_t begin_time, int64_t finish_time);
  void PerfCommitDelay(int64_t begin_time, int64_t finish_time);
  void PerfPrewriteDelay(int64_t begin_time, int64_t finish_time);
  void PerfPrimaryCommitDelay(int64_t begin_time, int64_t finish_time);
  void PerfSecondariesCommitDelay(int64_t begin_time, int64_t finish_time);
  void PerfAckDelay(int64_t begin_time, int64_t finish_time);
  void PerfNotifyDelay(int64_t begin_time, int64_t finish_time);

  void PerfReport();

 private:
  GlobalTxnInternal(const GlobalTxnInternal&) = delete;
  GlobalTxnInternal& operator=(const GlobalTxnInternal&) = delete;
  // for test
  GlobalTxnTestHelper* TEST_GtxnTestHelper_;
  // tablename-> (Table*, set(gtxn_cf_name))
  typedef std::map<std::string, std::pair<Table*, std::set<std::string> > > TableInfoMap;
  TableInfoMap tables_;
  mutable Mutex tables_mu_;
  int64_t start_ts_;
  int64_t prewrite_start_ts_;

  // for record this transaction perf
  Counter read_cost_time_;
  Counter commit_cost_time_;
  Counter prewrite_cost_time_;
  Counter primary_cost_time_;
  Counter secondaries_cost_time_;
  Counter acks_cost_time_;
  Counter notifies_cost_time_;

  int64_t terminal_time_;
  std::atomic<bool> is_timeout_;
  std::shared_ptr<tera::ClientImpl> client_impl_;
};

}  // namespace tera

#endif  // TERA_SDK_GLOBAL_TXN_INTERNAL_H_
