// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "sdk/global_txn_internal.h"

#include "common/metric/metric_counter.h"
#include "common/this_thread.h"
#include "proto/table_meta.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "sdk/global_txn.h"
#include "sdk/read_impl.h"
#include "sdk/sdk_metric_name.h"

DECLARE_bool(tera_gtxn_test_opened);
DECLARE_string(tera_gtxn_test_flagfile);
DECLARE_int32(tera_gtxn_all_puts_size_limit);
DECLARE_int32(tera_sdk_read_timeout);
DECLARE_int32(tera_sdk_write_timeout);

namespace tera {

// for record sdk all transactions perf
tera::MetricCounter gtxn_read_delay_us(kGTxnReadDelayMetric, kGTxnLabelRead);
tera::MetricCounter gtxn_read_cnt(kGTxnReadCountMetric, kGTxnLabelRead);
tera::MetricCounter gtxn_read_fail_cnt(kGTxnReadFailCountMetric, kGTxnLabelRead);
tera::MetricCounter gtxn_read_retry_cnt(kGTxnReadRetryCountMetric, kGTxnLabelRead);
tera::MetricCounter gtxn_read_rollback_cnt(kGTxnReadRollBackCountMetric, kGTxnLabelRead);
tera::MetricCounter gtxn_read_rollforward_cnt(kGTxnReadRollForwardCountMetric, kGTxnLabelRead);

tera::MetricCounter gtxn_commit_delay_us(kGTxnCommitDelayMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_commit_cnt(kGTxnCommitCountMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_commit_fail_cnt(kGTxnCommitFailCountMetric, kGTxnLabelCommit);

tera::MetricCounter gtxn_prewrite_delay_us(kGTxnPrewriteDelayMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_prewrite_cnt(kGTxnPrewriteCountMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_prewrite_fail_cnt(kGTxnPrewriteFailCountMetric, kGTxnLabelCommit);

tera::MetricCounter gtxn_primary_delay_us(kGTxnPrimaryDelayMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_primary_cnt(kGTxnPrimaryCountMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_primary_fail_cnt(kGTxnPrimaryFailCountMetric, kGTxnLabelCommit);

tera::MetricCounter gtxn_secondaries_delay_us(kGTxnSecondariesDelayMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_secondaries_cnt(kGTxnSecondariesCountMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_secondaries_fail_cnt(kGTxnSecondariesFailCountMetric, kGTxnLabelCommit);

tera::MetricCounter gtxn_acks_delay_us(kGTxnAcksDelayMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_acks_cnt(kGTxnAcksCountMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_acks_fail_cnt(kGTxnAcksFailCountMetric, kGTxnLabelCommit);

tera::MetricCounter gtxn_notifies_delay_us(kGTxnNotifiesDelayMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_notifies_cnt(kGTxnNotifiesCountMetric, kGTxnLabelCommit);
tera::MetricCounter gtxn_notifies_fail_cnt(kGTxnNotifiesFailCountMetric, kGTxnLabelCommit);

tera::MetricCounter gtxn_tso_delay_us(kGTxnTsoDelayMetric, kGTxnLabelTso);
tera::MetricCounter gtxn_tso_req_cnt(kGTxnTsoRequestCountMetric, kGTxnLabelTso);

GlobalTxnInternal::GlobalTxnInternal(std::shared_ptr<tera::ClientImpl> client_impl)
    : TEST_GtxnTestHelper_(NULL),
      start_ts_(0),
      prewrite_start_ts_(0),
      terminal_time_(0),
      is_timeout_(false),
      client_impl_(client_impl) {}

GlobalTxnInternal::~GlobalTxnInternal() { PerfReport(); }

void GlobalTxnInternal::SetStartTimestamp(int64_t ts) {
  start_ts_ = ts;
  prewrite_start_ts_ = ts;
}

bool GlobalTxnInternal::CheckTable(Table* table, ErrorCode* status) {
  assert(table != NULL);
  MutexLock lock(&tables_mu_);
  TableInfoMap::const_iterator tables_it = tables_.find(table->GetName());
  if (tables_it == tables_.end()) {
    TableImpl* table_impl = static_cast<TableImpl*>(table);
    TableSchema schema = table_impl->GetTableSchema();
    if (IsTransactionTable(schema)) {
      std::set<std::string> gtxn_cfs;
      FindGlobalTransactionCfs(schema, &gtxn_cfs);
      if (gtxn_cfs.size() > 0) {
        tables_[table->GetName()] = std::pair<Table*, std::set<std::string> >(table, gtxn_cfs);
        return true;
      } else {
        status->SetFailed(ErrorCode::kBadParam,
                          "schema check fail: " + table->GetName() + " haven't gtxn cf");
        return false;
      }
    } else {
      status->SetFailed(ErrorCode::kBadParam,
                        "schema check fail: " + table->GetName() + " not txn table");
      return false;
    }
  }
  return true;
}

bool GlobalTxnInternal::IsLockedByOthers(RowReader::TRow& row, const Cell& cell) {
  if (row[cell.ColFamily()].find(cell.LockName()) != row[cell.ColFamily()].end()) {
    for (auto k = row[cell.ColFamily()][cell.LockName()].rbegin();
         k != row[cell.ColFamily()][cell.LockName()].rend(); ++k) {
      if (k->first < start_ts_) {
        return true;
      }
    }
  }
  return false;
}

bool GlobalTxnInternal::SuspectLive(const tera::PrimaryInfo& primary_info) {
  std::string session_str = primary_info.client_session();
  VLOG(12) << "suppect_live : " << session_str;
  return client_impl_->IsClientAlive(session_str);
}

bool GlobalTxnInternal::VerifyUserRowReader(RowReader* user_reader) {
  RowReaderImpl* reader_impl = static_cast<RowReaderImpl*>(user_reader);
  const RowReader::ReadColumnList& read_col_list = user_reader->GetReadColumnList();
  ErrorCode status;
  bool schema_valid = true;
  std::string reason("");

  Table* table = reader_impl->GetTable();
  if (!CheckTable(table, &status)) {
    // table schema error for gtxn
    reader_impl->SetError(status.GetType(), status.GetReason());
    return false;
  } else if (read_col_list.size() == 0) {
    // TODO support read full
    reason = "not support read full line in global transaction";
    LOG(ERROR) << "[gtxn][get] " << reason;
    reader_impl->SetError(ErrorCode::kBadParam, reason);
    return false;
  } else if (reader_impl->GetSnapshot() != 0) {
    reason = "not support read a snapshot in global transaction";
    LOG(ERROR) << "[gtxn][get] " << reason;
    reader_impl->SetError(ErrorCode::kBadParam, reason);
    return false;
  }

  // check schema valid
  const std::string& tablename = table->GetName();

  for (auto it = read_col_list.begin(); it != read_col_list.end(); ++it) {
    const std::string& column_family = it->first;
    const std::set<std::string>& qualifier_set = it->second;

    if (qualifier_set.size() == 0) {
      reason = "not set any qualifier";
      LOG(ERROR) << "[gtxn][get] " << reason;
      reader_impl->SetError(ErrorCode::kBadParam, reason);
      schema_valid = false;
      break;
    }
    if (!IsGTxnColumnFamily(tablename, column_family)) {
      reason = "table:" + tablename + ",cf:" + column_family + " not set gtxn=\"on\"";
      LOG(ERROR) << "[gtxn][get] " << reason;
      reader_impl->SetError(ErrorCode::kBadParam, reason);
      schema_valid = false;
      break;
    }
    for (auto q_it = qualifier_set.begin(); q_it != qualifier_set.end(); ++q_it) {
      const std::string& qualifier = *q_it;

      if (BadQualifier(qualifier)) {
        reason = "table:" + tablename + ",qu:" + qualifier + " can't end with \"_*_\"";
        LOG(ERROR) << "[gtxn][get] " << reason;
        reader_impl->SetError(ErrorCode::kBadParam, reason);
        schema_valid = false;
        break;
      }
    }
  }
  return schema_valid;
}

bool GlobalTxnInternal::VerifyUserRowMutation(RowMutation* user_mu) {
  RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(user_mu);
  Table* table = row_mu_impl->GetTable();

  ErrorCode status;
  if (!CheckTable(table, &status)) {
    // table schema error for gtxn;
    row_mu_impl->SetError(status.GetType(), status.GetReason());
    return false;
  } else if (row_mu_impl->MutationNum() <= 0) {
    // nothing to mutation
    row_mu_impl->SetError(ErrorCode::kBadParam, "nothing to mutation");
    return false;
  }

  std::string reason("");
  const std::string& tablename = table->GetName();

  for (size_t i = 0; i < user_mu->MutationNum(); ++i) {
    const RowMutation::Mutation& mu = user_mu->GetMutation(i);
    // check this qualifier is right
    if (BadQualifier(mu.qualifier)) {
      reason = "@table" + tablename + ",qu:" + mu.qualifier + " can't end with \"_*_\"";
      LOG(ERROR) << "[gtxn][apply_mutation] " << reason;
      row_mu_impl->SetError(ErrorCode::kBadParam, reason);
      return false;
    } else if (!IsGTxnColumnFamily(tablename, mu.family)) {
      // check column has set gtxn="on"
      reason = "@table" + tablename + ",cf:" + mu.family + " not set gtxn=\"on\"";
      LOG(ERROR) << "[gtxn][apply_mutation] " << reason;
      row_mu_impl->SetError(ErrorCode::kBadParam, reason);
      return false;
    } else if (mu.type != RowMutation::kPut && mu.type != RowMutation::kDeleteColumn &&
               mu.type != RowMutation::kDeleteColumns) {
      reason = "@table " + tablename + ",row mutation type is " + std::to_string(mu.type);
      LOG(ERROR) << "[gtxn][apply_mutation] " << reason;
      row_mu_impl->SetError(ErrorCode::kGTxnNotSupport, reason);
      return false;
    }
  }
  return true;
}

bool GlobalTxnInternal::VerifyWritesSize(RowMutation* user_mu, int64_t* size) {
  RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(user_mu);
  *size += row_mu_impl->Size();
  if (*size > FLAGS_tera_gtxn_all_puts_size_limit) {
    LOG(ERROR) << "[gtxn][apply_mutation][" << start_ts_ << "] failed, "
               << "mutations size " << *size << " > limit (" << FLAGS_tera_gtxn_all_puts_size_limit
               << ")";
    row_mu_impl->SetError(ErrorCode::kGTxnDataTooLarge);
    return false;
  } else if (*size <= 0) {
    LOG(ERROR) << "[gtxn][apply_mutation][" << start_ts_ << "] failed, "
               << "mutaions size " << *size;
    row_mu_impl->SetError(ErrorCode::kBadParam);
    return false;
  }
  return true;
}

bool GlobalTxnInternal::PrimaryIsLocked(const tera::PrimaryInfo& primary, const int64_t lock_ts,
                                        ErrorCode* status) {
  Table* table = FindTable(primary.table_name());
  if (table == NULL) {
    status->SetFailed(ErrorCode::kGTxnPrimaryLost, "not found primary table and open failed");
    return false;
  }
  if (!CheckTable(table, status)) {
    status->SetFailed(ErrorCode::kGTxnPrimaryLost,
                      "primary table check failed" + status->ToString());
    return false;
  }
  const Cell& cell = Cell(table, primary.row_key(), primary.column_family(), primary.qualifier());

  std::unique_ptr<RowReader> reader(table->NewRowReader(cell.RowKey()));
  reader->AddColumn(cell.ColFamily(), cell.LockName());
  reader->SetTimeRange(lock_ts, lock_ts);
  table->Get(reader.get());

  if (reader->GetError().GetType() != tera::ErrorCode::kOK &&
      reader->GetError().GetType() != tera::ErrorCode::kNotFound) {
    *status = reader->GetError();
    return false;
  }
  while (!reader->Done()) {
    if (reader->Timestamp() == lock_ts) {
      VLOG(12) << DebugString(cell, "other transaction on prewrite @" + std::to_string(lock_ts));
      return true;
    }
    reader->Next();
  }
  return false;
}

void GlobalTxnInternal::BuildRowReaderForPrewrite(const std::vector<Write>& ws, RowReader* reader) {
  for (auto& w : ws) {
    reader->AddColumn(w.ColFamily(), w.DataName());
    reader->AddColumn(w.ColFamily(), w.LockName());
    reader->AddColumn(w.ColFamily(), w.WriteName());
    reader->SetTimeRange(0, kMaxTimeStamp);
    reader->SetMaxVersions(UINT32_MAX);
  }
}

void GlobalTxnInternal::BuildRowMutationForPrewrite(std::vector<Write>* ws,
                                                    RowMutation* prewrite_mu,
                                                    const std::string& primary_info) {
  for (auto it = ws->begin(); it != ws->end(); ++it) {
    const Write& w = *it;  // one cell
    prewrite_mu->Put(w.ColFamily(), w.LockName(), EncodeLockValue(w.WriteType(), primary_info),
                     (int64_t)prewrite_start_ts_);
    prewrite_mu->Put(w.ColFamily(), w.DataName(), w.Value(), (int64_t)prewrite_start_ts_);
    VLOG(12) << "[gtxn][prewrite][lock] " << w.DebugString()
             << " LockName:Type:Info:prewrite_start_ts " << w.LockName() << ":" << w.WriteType()
             << ":" << primary_info << ":" << prewrite_start_ts_ << " WriteName:Value "
             << w.DataName() << ":" << w.Value();
  }
}

void GlobalTxnInternal::BuildRowMutationForCommit(std::vector<Write>* ws, RowMutation* commit_mu,
                                                  const int64_t commit_ts) {
  for (auto it = ws->begin(); it != ws->end(); ++it) {
    const Write& w = *it;  // one cell
    // value = type + start_ts
    commit_mu->Put(w.ColFamily(), w.WriteName(),
                   EncodeWriteValue(w.WriteType(), prewrite_start_ts_), commit_ts);
    commit_mu->DeleteColumns(w.ColFamily(), w.LockName(), commit_ts);
  }
}

void GlobalTxnInternal::BuildRowMutationForAck(std::vector<Write>* ws, RowMutation* commit_mu) {
  for (auto it = ws->begin(); it != ws->end(); ++it) {
    const Write& w = *it;  // one cell
    commit_mu->DeleteColumns(kNotifyColumnFamily, w.NotifyName(), start_ts_);
  }
}

void GlobalTxnInternal::BuildRowMutationForNotify(std::vector<Write>* ws, RowMutation* commit_mu,
                                                  const int64_t commit_ts) {
  for (auto it = ws->begin(); it != ws->end(); ++it) {
    const Write& w = *it;  // one cell
    commit_mu->Put(kNotifyColumnFamily, w.NotifyName(), Int64ToEncodedString(commit_ts), commit_ts);
  }
}

void GlobalTxnInternal::SetCommitDuration(int64_t timeout_ms) {
  terminal_time_ = timeout_ms + get_millis();
}

void GlobalTxnInternal::SetInternalSdkTaskTimeout(RowReader* reader) {
  int64_t duration = terminal_time_ - get_millis();
  if (duration < 0) {
    is_timeout_ = true;
    duration = 1;
  }
  // duration should not larger than FLAGS_tera_sdk_read_timeout
  duration = duration > FLAGS_tera_sdk_read_timeout ? FLAGS_tera_sdk_read_timeout : duration;
  reader->SetTimeOut(duration);
}

void GlobalTxnInternal::SetInternalSdkTaskTimeout(RowMutation* mutation) {
  int64_t duration = terminal_time_ - get_millis();
  if (duration < 0) {
    is_timeout_ = true;
    duration = 1;
  }
  // duration should not larger than FLAGS_tera_sdk_write_timeout
  duration = duration > FLAGS_tera_sdk_write_timeout ? FLAGS_tera_sdk_write_timeout : duration;
  mutation->SetTimeOut(duration);
}

bool GlobalTxnInternal::IsTimeOut() { return is_timeout_; }

bool GlobalTxnInternal::IsPrimary(const tera::Cell& cell, const tera::PrimaryInfo& primary_info) {
  return primary_info.table_name() == cell.TableName() && primary_info.row_key() == cell.RowKey() &&
         primary_info.column_family() == cell.ColFamily() &&
         primary_info.qualifier() == cell.Qualifier();
}

Table* GlobalTxnInternal::FindTable(const std::string& tablename) {
  assert(!tablename.empty());
  MutexLock lock(&tables_mu_);
  TableInfoMap::const_iterator it = tables_.find(tablename);
  if (it == tables_.end()) {
    ErrorCode status;
    Table* t = client_impl_->OpenTable(tablename, &status);
    if (t == NULL || status.GetType() != ErrorCode::kOK) {
      LOG(ERROR) << "[gtxn] can't create table :" << tablename << "," << status.ToString();
      return NULL;
    }
    return t;
  }
  return (it->second).first;
}

bool GlobalTxnInternal::ConflictWithOtherWrite(const std::vector<Write>* ws,
                                               const std::unique_ptr<RowReaderImpl>& reader,
                                               ErrorCode* status) {
  RowReader::TRow row;
  reader->ToMap(&row);

  // check every cell
  for (auto it = ws->begin(); it != ws->end(); ++it) {
    const Write& w = *it;
    const std::string& w_cf = w.ColFamily();
    if (row.find(w_cf) == row.end()) {
      VLOG(12) << "[gtxn][prewrite][stxn_read]" << w.DebugString() << "not found [" << w_cf << "]";
      continue;
    } else {
      // check Write column
      const std::string& w_write = w.WriteName();
      if (row[w_cf].find(w_write) != row[w_cf].end()) {
        for (auto k = row[w_cf][w_write].rbegin(); k != row[w_cf][w_write].rend(); ++k) {
          std::string write_value = k->second;
          int write_type;
          int64_t data_start_ts;
          DecodeWriteValue(write_value, &write_type, &data_start_ts);
          VLOG(12) << "[gtxn][prewrite][stxn_read]" << w.DebugString()
                   << " prewrite_start_ts:" << prewrite_start_ts_ << " found _W_ :" << k->first
                   << " type: " << write_type << " data_ts: " << data_start_ts;
          if (k->first >= prewrite_start_ts_) {
            status->SetFailed(ErrorCode::kGTxnWriteConflict,
                              "writing by others ts:" + std::to_string(k->first));
            return true;
          }
        }
      } else {
        VLOG(12) << "[gtxn][prewrite][stxn_read]" << w.DebugString() << "not found _W_ col";
      }
      // check Lock column
      const std::string& w_lock = w.LockName();
      if (row[w_cf].find(w_lock) != row[w_cf].end()) {
        auto k = row[w_cf][w_lock].rbegin();
        if (k != row[w_cf][w_lock].rend()) {
          VLOG(12) << "[gtxn][prewrite][stxn_read]" << w.DebugString() << "locked@: " << k->first;
          status->SetFailed(ErrorCode::kGTxnLockConflict,
                            w.DebugString() + "locked@:" + std::to_string(k->first));
          return true;
        }
      }
    }
  }
  return false;
}

void GlobalTxnInternal::SetPrewriteStartTimestamp(const int64_t prewrite_start_ts) {
  prewrite_start_ts_ = prewrite_start_ts;
}

bool GlobalTxnInternal::IsGTxnColumnFamily(const std::string& tablename,
                                           const std::string& column_family) {
  MutexLock lock(&tables_mu_);
  auto it = tables_.find(tablename);
  if (it != tables_.end()) {
    std::set<std::string>& gtxn_cfs = (it->second).second;
    auto cfs_it = gtxn_cfs.find(column_family);
    if (cfs_it != gtxn_cfs.end()) {
      return true;
    }
  }
  return false;
}

std::string GlobalTxnInternal::GetClientSession() { return client_impl_->ClientSession(); }

std::string GlobalTxnInternal::DebugString(const Cell& cell, const std::string& msg) const {
  std::stringstream ss;
  ss << msg << " @ [" << cell.Table()->GetName() << ":" << cell.RowKey() << ":" << cell.ColFamily()
     << ":" << cell.Qualifier() << ":" << cell.Timestamp() << "]";
  return ss.str();
}

int64_t GlobalTxnInternal::TEST_Init(const std::string& conf_file) {
  if (FLAGS_tera_gtxn_test_opened) {
    TEST_GtxnTestHelper_ = new GlobalTxnTestHelper(conf_file);
    TEST_GtxnTestHelper_->LoadTxnConf();
    start_ts_ = TEST_GtxnTestHelper_->GetStartTs();
    prewrite_start_ts_ = TEST_GtxnTestHelper_->GetPrewriteStartTs();
  }
  return start_ts_;
}

void GlobalTxnInternal::TEST_GetSleep() {
  if (FLAGS_tera_gtxn_test_opened) {
    TEST_GtxnTestHelper_->GetWait(start_ts_);
  }
}

void GlobalTxnInternal::TEST_Sleep() {
  if (FLAGS_tera_gtxn_test_opened) {
    TEST_GtxnTestHelper_->Wait(start_ts_);
  }
}

void GlobalTxnInternal::TEST_Destory() {
  if (FLAGS_tera_gtxn_test_opened) {
    delete TEST_GtxnTestHelper_;
  }
}

int64_t GlobalTxnInternal::TEST_GetCommitTimestamp() { return TEST_GtxnTestHelper_->GetCommitTs(); }

int64_t GlobalTxnInternal::TEST_GetPrewriteStartTimestamp() {
  return TEST_GtxnTestHelper_->GetPrewriteStartTs();
}

void GlobalTxnInternal::PerfReadDelay(int64_t begin_time, int64_t finish_time) {
  read_cost_time_.Add(finish_time - begin_time);
}
void GlobalTxnInternal::PerfCommitDelay(int64_t begin_time, int64_t finish_time) {
  commit_cost_time_.Add(finish_time - begin_time);
}

void GlobalTxnInternal::PerfPrewriteDelay(int64_t begin_time, int64_t finish_time) {
  prewrite_cost_time_.Add(finish_time - begin_time);
}

void GlobalTxnInternal::PerfPrimaryCommitDelay(int64_t begin_time, int64_t finish_time) {
  primary_cost_time_.Add(finish_time - begin_time);
}

void GlobalTxnInternal::PerfSecondariesCommitDelay(int64_t begin_time, int64_t finish_time) {
  secondaries_cost_time_.Add(finish_time - begin_time);
}

void GlobalTxnInternal::PerfAckDelay(int64_t begin_time, int64_t finish_time) {
  acks_cost_time_.Add(finish_time - begin_time);
}

void GlobalTxnInternal::PerfNotifyDelay(int64_t begin_time, int64_t finish_time) {
  notifies_cost_time_.Add(finish_time - begin_time);
}

void GlobalTxnInternal::PerfReport() {
  gtxn_read_delay_us.Add(read_cost_time_.Clear());
  gtxn_commit_delay_us.Add(commit_cost_time_.Clear());
  gtxn_prewrite_delay_us.Add(prewrite_cost_time_.Clear());
  gtxn_primary_delay_us.Add(primary_cost_time_.Clear());
  gtxn_secondaries_delay_us.Add(secondaries_cost_time_.Clear());
  gtxn_acks_delay_us.Add(acks_cost_time_.Clear());
  gtxn_notifies_delay_us.Add(notifies_cost_time_.Clear());
}

}  // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
