// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include <functional>
#include <thread> 

#include "common/metric/metric_counter.h"
#include "common/this_thread.h"
#include "common/thread.h"
#include "proto/table_meta.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "sdk/global_txn.h"
#include "sdk/read_impl.h"
#include "sdk/timeoracle_client_impl.h"

DECLARE_bool(tera_gtxn_test_opened);
DECLARE_string(tera_gtxn_test_flagfile);
DECLARE_int32(tera_gtxn_get_waited_times_limit);
DECLARE_int32(tera_gtxn_timeout_ms);
DECLARE_bool(tera_sdk_tso_client_enabled);

namespace tera {

extern tera::MetricCounter gtxn_read_cnt;
extern tera::MetricCounter gtxn_read_fail_cnt;
extern tera::MetricCounter gtxn_read_retry_cnt;
extern tera::MetricCounter gtxn_read_rollback_cnt;
extern tera::MetricCounter gtxn_read_rollforward_cnt;
extern tera::MetricCounter gtxn_commit_cnt;
extern tera::MetricCounter gtxn_commit_fail_cnt;
extern tera::MetricCounter gtxn_prewrite_cnt;
extern tera::MetricCounter gtxn_prewrite_fail_cnt;
extern tera::MetricCounter gtxn_primary_cnt;
extern tera::MetricCounter gtxn_primary_fail_cnt;
extern tera::MetricCounter gtxn_secondaries_cnt;
extern tera::MetricCounter gtxn_secondaries_fail_cnt;
extern tera::MetricCounter gtxn_acks_cnt;
extern tera::MetricCounter gtxn_acks_fail_cnt;
extern tera::MetricCounter gtxn_notifies_cnt;
extern tera::MetricCounter gtxn_notifies_fail_cnt;

Transaction* GlobalTxn::NewGlobalTxn(tera::Client* client, 
                                     common::ThreadPool* thread_pool,
                                     sdk::ClusterFinder* tso_cluster) {
    if (client != NULL) {
            return new GlobalTxn(client, thread_pool, tso_cluster);
    }
    LOG(ERROR) << "client or tso_cluster is NULL";
    return NULL;
}

GlobalTxn::GlobalTxn(tera::Client* client,
        common::ThreadPool* thread_pool,
        sdk::ClusterFinder* tso_cluster) :
    gtxn_internal_(new GlobalTxnInternal(client)),
    status_returned_(false),
    primary_write_(NULL),
    writes_size_(0), 
    commit_ts_(0),
    isolation_level_(IsolationLevel::kSnapshot),
    serialized_primary_(""),
    finish_(false),
    finish_cond_(&finish_mutex_),
    has_commited_(false),
    user_commit_callback_(NULL),
    user_commit_context_(NULL),
    thread_pool_(thread_pool),
    tso_cluster_(tso_cluster),
    timeout_ms_(FLAGS_tera_gtxn_timeout_ms),
    all_task_pushed_(false) {
    if (FLAGS_tera_gtxn_test_opened) {
        VLOG(12) << "conf_file = " << FLAGS_tera_gtxn_test_flagfile;
        start_ts_ = gtxn_internal_->TEST_Init(FLAGS_tera_gtxn_test_flagfile);
    } else if (!FLAGS_tera_sdk_tso_client_enabled) {
        start_ts_ = get_micros();
    } else {
        timeoracle::TimeoracleClientImpl tsoc(thread_pool_, tso_cluster_); 
        start_ts_ = tsoc.GetTimestamp(1);
        if (start_ts_ == 0) {
            status_.SetFailed(ErrorCode::kGTxnTimestampLost);
            status_returned_ = true;
        }
    }
    prewrite_start_ts_ = start_ts_;
    gtxn_internal_->SetStartTimestamp(start_ts_);
}

GlobalTxn::~GlobalTxn() {
}

void GlobalTxn::SetIsolation(const IsolationLevel& isolation_level) {
    assert(has_commited_ == false);
    isolation_level_ = isolation_level;
}

void GlobalTxn::SetTimeout(int64_t timeout_ms) {
    timeout_ms_ = timeout_ms;
}

int64_t GlobalTxn::Timeout() {
    return timeout_ms_;
}

void GlobalTxn::SetReaderStatusAndRunCallback(RowReaderImpl* reader_impl, 
                                              ErrorCode* status) {
    gtxn_read_cnt.Inc();
    gtxn_internal_->PerfReadDelay(0, get_micros()); // finish_time
    VLOG(12) << "[gtxn][get][" << start_ts_ << "][status] :" << status->ToString();
    reader_impl->SetError(status->GetType(), status->GetReason());
    thread_pool_->AddTask(std::bind(&RowReaderImpl::RunCallback, reader_impl));
}

ErrorCode GlobalTxn::Get(RowReader* row_reader) {
    assert(row_reader != NULL);
    gtxn_internal_->PerfReadDelay(get_micros(), 0); // begin_time
    gtxn_internal_->TEST_GetSleep(); 
    
    RowReaderImpl* reader_impl = static_cast<RowReaderImpl*>(row_reader);
    reader_impl->SetTransaction(this);
    
    // Pre Check can read
    ErrorCode status;
    status.SetFailed(ErrorCode::kOK);
    if (has_commited_.load()) {
        std::string reason = "get failed, txn has commited @ [" + 
            std::to_string(start_ts_) + "," + std::to_string(commit_ts_);
        LOG(ERROR) << "[gtxn][get][" << start_ts_ <<"] " << reason;
        status.SetFailed(ErrorCode::kGTxnOpAfterCommit, reason);
        SetReaderStatusAndRunCallback(reader_impl, &status);
        return status;
    }

    Table* table = row_reader->GetTable();
    const std::string& row_key = row_reader->RowKey();
    // Check UserReader and Build cells
    if (!gtxn_internal_->VerifyUserRowReader(row_reader)) {
        status = reader_impl->GetError();
        SetReaderStatusAndRunCallback(reader_impl, &status);
        return status;
    }

    std::vector<Cell*> cells;
    for (auto it : row_reader->GetReadColumnList()) {
        const std::string& column_family = it.first;
        const std::set<std::string>& qualifier_set = it.second;

        for (auto q_it = qualifier_set.begin(); q_it != qualifier_set.end(); ++q_it) {
            const std::string& qualifier = *q_it;
            cells.push_back(new Cell(table, row_key, column_family, qualifier));
        }
    }
    int expected_cells_cnt = cells.size();

    InternalReaderContext* ctx = new InternalReaderContext(expected_cells_cnt, reader_impl, this);
    for(auto& cell : cells) {
        ctx->cell_map[cell] = 0; // cell* -> try_time, default = 0
        AsyncGetCell(cell, reader_impl, ctx);
    }
    
    // sync wait and set status
    if(!reader_impl->IsAsync()) {
        reader_impl->Wait();
        status = reader_impl->GetError();
        return status;
    }
    return status;
}

void GlobalTxn::AsyncGetCell(Cell* cell,
                             RowReaderImpl* user_reader_impl, 
                             InternalReaderContext* ctx) {
    VLOG(12) << "[gtxn][get][" << start_ts_ << "] " 
             << gtxn_internal_->DebugString(*cell, "TryGet times(" + std::to_string(ctx->cell_map[cell]) + ")");
    
    Table* table = cell->Table();
    RowReader* reader = table->NewRowReader(cell->RowKey());
    reader->AddColumn(cell->ColFamily(), cell->LockName());
    reader->AddColumn(cell->ColFamily(), cell->WriteName());
    reader->AddColumn(cell->ColFamily(), cell->Qualifier());
    reader->SetTimeRange(0, kMaxTimeStamp);
    reader->SetMaxVersions(UINT32_MAX);
    reader->SetCallBack([] (RowReader* r) {
        CellReaderContext* ctx = (CellReaderContext*)r->GetContext();
        GlobalTxn* gtxn = static_cast<GlobalTxn*>(ctx->internal_reader_ctx->gtxn);
        gtxn->thread_pool_->AddTask(std::bind(&GlobalTxn::DoGetCellReaderCallback, 
            gtxn, static_cast<RowReaderImpl*>(r)));
    });
    reader->SetContext(new CellReaderContext(cell, ctx));
    table->Get(reader);
}

void GlobalTxn::DoGetCellReaderCallback(RowReader* reader) {
    ErrorCode status = reader->GetError();
    if (status.GetType() != ErrorCode::kOK) {
        MergeCellToRow(reader, status);
        return;
    }

    RowReader::TRow row;
    reader->ToMap(&row);
    CellReaderContext* ctx = (CellReaderContext*)reader->GetContext();
    Cell* cell = ctx->cell;
    if (row.find(cell->ColFamily()) == row.end()) {
        status.SetFailed(ErrorCode::kNotFound, "columnfamily not found");
        MergeCellToRow(reader, status);
        return;
    }
    // local check lock
    if (gtxn_internal_->IsLockedByOthers(row, *cell)) {
        // sync operate
        status.SetFailed(ErrorCode::kOK);
        InternalReaderContext* internal_reader_ctx = ctx->internal_reader_ctx;
        bool do_clean = false;
        // check clean lock before read cell next time, 
        // when read times >= limit - 1 do clean lock opreations 
        if (internal_reader_ctx->cell_map[cell] >= FLAGS_tera_gtxn_get_waited_times_limit - 1) {
            do_clean = true;   
        }
        BackoffAndMaybeCleanupLock(row, *cell, do_clean, &status);
        if (status.GetType() == ErrorCode::kOK) {
            // call Next time to async GetCell
            // don't merge until next time ok or failed
            ++ internal_reader_ctx->cell_map[cell];
            gtxn_read_retry_cnt.Inc();
            AsyncGetCell(cell, 
                         static_cast<RowReaderImpl*>(internal_reader_ctx->user_reader), 
                         internal_reader_ctx);
            return;
        }
    } else if (!FindValueFromResultRow(row, cell)) {
        status.SetFailed(ErrorCode::kNotFound, "build data col from write col failed");
    }
    MergeCellToRow(reader, status);
}

void GlobalTxn::MergeCellToRow(RowReader* internal_reader, 
                               const ErrorCode& status) {
    CellReaderContext* ctx = (CellReaderContext*)internal_reader->GetContext();
    ctx->status = status;
    VLOG(12) << "[gtxn][get][" << start_ts_ << "] " 
             << gtxn_internal_->DebugString(*(ctx->cell), status.ToString());
    GetCellCallback(ctx);
    // next time internal read will new next RowReader
    delete internal_reader;
}

void GlobalTxn::GetCellCallback(CellReaderContext* ctx) {
    InternalReaderContext* internal_reader_ctx = ctx->internal_reader_ctx;
    Cell* cell = ctx->cell;
    bool last_cell = false;
    {
        MutexLock lock(&mu_);
        ++internal_reader_ctx->active_cell_cnt;
        if (internal_reader_ctx->fail_cell_cnt == 0 && ctx->status.GetType() == ErrorCode::kOK) {
            KeyValuePair* kv = internal_reader_ctx->results.add_key_values();
            kv->set_key(cell->RowKey());
            kv->set_column_family(cell->ColFamily());
            kv->set_qualifier(cell->Qualifier());
            kv->set_timestamp(cell->Timestamp());
            kv->set_value(cell->Value());
        } else if (ctx->status.GetType() != ErrorCode::kNotFound) {
            ++internal_reader_ctx->fail_cell_cnt;
            internal_reader_ctx->results.clear_key_values();
            internal_reader_ctx->last_err = ctx->status;
        } else {
            ++internal_reader_ctx->not_found_cnt;
        }
        last_cell = (internal_reader_ctx->active_cell_cnt == internal_reader_ctx->expected_cell_cnt);
    }
    if (last_cell) {
        ErrorCode last_err = internal_reader_ctx->last_err;
        RowReaderImpl* reader_impl = static_cast<RowReaderImpl*>(internal_reader_ctx->user_reader);
        if (internal_reader_ctx->fail_cell_cnt > 0) {
            gtxn_read_fail_cnt.Inc();
        } else if (internal_reader_ctx->not_found_cnt == internal_reader_ctx->expected_cell_cnt) {
            // all cell not found
            last_err.SetFailed(ErrorCode::kNotFound); 
        } else {
            reader_impl->SetResult(internal_reader_ctx->results);
            last_err.SetFailed(ErrorCode::kOK); 
        }
        delete internal_reader_ctx;
        SetReaderStatusAndRunCallback(reader_impl, &last_err);
    }
}

bool GlobalTxn::FindValueFromResultRow(RowReader::TRow& result_row, Cell* target_cell) {
    
    auto write_col_it = result_row[target_cell->ColFamily()].find(target_cell->WriteName()); 
    auto data_col_it = result_row[target_cell->ColFamily()].find(target_cell->Qualifier());

    // check write col and data col exsit
    if (write_col_it == result_row[target_cell->ColFamily()].end()
        || data_col_it == result_row[target_cell->ColFamily()].end()) {
        return false;
    }
    auto write_col = result_row[target_cell->ColFamily()][target_cell->WriteName()];
    auto data_col = result_row[target_cell->ColFamily()][target_cell->Qualifier()];

    for (auto k1 = write_col.rbegin(); k1 != write_col.rend(); ++k1) {
        int64_t write_ts = k1->first;
        std::string write_value = k1->second;
        VLOG(12) << "[gtxn][get][" << start_ts_ << "] found write col, ts=" 
                 << write_ts << ", internal val = " << write_value;
        int write_type;
        int64_t data_ts;
        // skip new version value or skip error write format version
        if (write_ts > start_ts_ || !DecodeWriteValue(write_value, &write_type, &data_ts)) {
            continue;
        }
        VLOG(12) << "[gtxn][get][" << start_ts_ << "] decode write col, ts=" 
                 << write_ts << ", type=" << write_type << ", value=" << data_ts;
        // get data col , ts == data_ts
        for (auto k2 = data_col.rbegin(); k2 != data_col.rend(); ++k2) {
            VLOG(12) << "[gtxn][get][" << start_ts_ << "] found data col, ts=" 
                     << k2->first << ", internal val = " << k2->second;
            if (k2->first == data_ts && write_type == RowMutation::kPut) {
                target_cell->SetTimestamp(data_ts);
                target_cell->SetValue(k2->second);
                return true;
            } else if (k2->first < data_ts) {
                VLOG(12) << "[gtxn][get][" << start_ts_ 
                         << "] data cell version not found, v=" << k2->first;
                break;
            }
        }
        VLOG(12) << "[gtxn][get][" << start_ts_ << "] check data col failed, no data";
        break;
    }
    VLOG(12) << "[gtxn][get][" << start_ts_ 
             << "] write col versions count" << write_col.size();
    return false;
}

void GlobalTxn::BackoffAndMaybeCleanupLock(RowReader::TRow& row, const Cell& cell,
                                           const bool try_clean, ErrorCode* status) {
    VLOG(12) << gtxn_internal_->DebugString(cell, "[gtxn][get][" + 
            std::to_string(start_ts_) + " backoff or cleanup lock");
    // get lock ts
    int64_t lock_ts = -1;
    int lock_type = -1;
    tera::PrimaryInfo primary_info;
    for (auto k = row[cell.ColFamily()][cell.LockName()].rbegin();
             k != row[cell.ColFamily()][cell.LockName()].rend(); ++k) {
        if (k->first < start_ts_) {
            lock_ts = k->first;
            VLOG(12) << "lock_ts=" << lock_ts << ", primary_str=" << k->second;
            if (!DecodeLockValue(k->second, &lock_type, &primary_info)) {
                status->SetFailed(ErrorCode::kGTxnPrimaryLost, "can't found primary");
                return;
            }
            break;
        }
    }
    // get primary lock
    const std::string& process = "[gtxn][get][" + std::to_string(start_ts_) 
        + "][check locked and writed]";
    bool ret = gtxn_internal_->PrimaryIsLocked(primary_info, lock_ts, status);
    if (status->GetType() != ErrorCode::kOK && status->GetType() != ErrorCode::kNotFound) {
        LOG(ERROR) << gtxn_internal_->DebugString(cell, process + " failed," + status->ToString());
        return;
    } else if (ret) {
        // NotFound means : other txn on prewrite process
        // and this cell locked but primary unlocked(failed)
        VLOG(12) << gtxn_internal_->DebugString(cell, process + " succeed");
        // primary at prewrite do (1) clean or (2) wait
        if (try_clean) {
            CleanLock(cell, primary_info, status);
        } else if (gtxn_internal_->SuspectLive(primary_info)) { 
            // TODO add a better sleep strategy
            ThisThread::Sleep(100); 
        } else {
            CleanLock(cell, primary_info, status);
        }
    } else {
        if (!gtxn_internal_->IsPrimary(cell, primary_info)) {
            VLOG(12) << gtxn_internal_->DebugString(cell, process + ", will do rollforward");
            // primary maybe at commited do roll_forward
            RollForward(cell, primary_info, lock_type, status);
            if (status->GetType() == ErrorCode::kGTxnPrimaryLost) {
                VLOG(12) << gtxn_internal_->DebugString(cell, process + ", rollforward failed, try clean lock");
                // primary prewrite failed
                status->SetFailed(ErrorCode::kOK);
                if (try_clean) {
                    CleanLock(cell, primary_info, status);
                } else if (gtxn_internal_->SuspectLive(primary_info)) {
                    ThisThread::Sleep(100);
                } else {
                    CleanLock(cell, primary_info, status);
                }
            }
        } else { 
            VLOG(12) << gtxn_internal_->DebugString(cell, process + ", ignore(primary)");
        }
    } 
}

void GlobalTxn::CleanLock(const Cell& cell, const tera::PrimaryInfo& primary, ErrorCode* status) {
    gtxn_read_rollback_cnt.Inc();
    Table* primary_table = gtxn_internal_->FindTable(primary.table_name());
    assert(primary_table != NULL);
    const Cell& primary_cell = Cell(primary_table, primary.row_key(), 
                                    primary.column_family(), primary.qualifier());
    // if now cell is primary
    bool is_same = cell.Table()->GetName() == primary_table->GetName() 
                   && cell.RowKey() == primary_cell.RowKey() 
                   && cell.ColFamily() == primary_cell.ColFamily() 
                   && cell.LockName() == primary_cell.LockName();
    if (!is_same) {
        VLOG(12) << "[gtxn][get][" << start_ts_ << "] " 
                 << gtxn_internal_->DebugString(primary_cell, "clean lock primary");
        RowMutation* pri_mu = primary_table->NewRowMutation(primary_cell.RowKey());
        // delete all info between [0, start_ts_] at lock col
        pri_mu->DeleteColumns(primary_cell.ColFamily(), primary_cell.LockName(), start_ts_);
        primary_table->ApplyMutation(pri_mu);
        if (pri_mu->GetError().GetType() != tera::ErrorCode::kOK) {
            LOG(WARNING) << pri_mu->GetError().ToString();
            *status = pri_mu->GetError();
        }
        delete pri_mu;
    }  
    VLOG(12) << "[gtxn][get][" << start_ts_ << "] " 
             << gtxn_internal_->DebugString(cell, "clean lock this cell");
    RowMutation* this_mu = (cell.Table())->NewRowMutation(cell.RowKey());
    // delete all info between [0, start_ts_] at lock col
    this_mu->DeleteColumns(cell.ColFamily(), cell.LockName(), start_ts_);
    (cell.Table())->ApplyMutation(this_mu);
    if (this_mu->GetError().GetType() != tera::ErrorCode::kOK) {
        LOG(WARNING) << "[gtxn][get][" << start_ts_ << "] clean lock failed :" 
                     << this_mu->GetError().ToString();
        *status = this_mu->GetError();
    }
    delete this_mu;
}

void GlobalTxn::RollForward(const Cell& cell, const tera::PrimaryInfo& primary, 
                            int lock_type, ErrorCode* status) {
    gtxn_read_rollforward_cnt.Inc();
    // find primary write col start_ts
    Table* pri_table = gtxn_internal_->FindTable(primary.table_name());
    assert(pri_table != NULL);
    std::unique_ptr<Cell> primary_cell(new Cell(pri_table, primary.row_key(), 
                                                primary.column_family(), 
                                                primary.qualifier()));
    RowReader* reader = pri_table->NewRowReader(primary_cell->RowKey());
    reader->AddColumn(primary_cell->ColFamily(), primary_cell->WriteName());
    reader->SetTimeRange(0, kMaxTimeStamp);
    reader->SetMaxVersions(UINT32_MAX);
    pri_table->Get(reader);
    if (reader->GetError().GetType() != ErrorCode::kOK) {
        if (reader->GetError().GetType() == ErrorCode::kNotFound) {
            status->SetFailed(ErrorCode::kGTxnPrimaryLost, "primary lost, not 'lock' and 'write'");
        } else {
            LOG(WARNING) << status->GetReason();
            *status = reader->GetError();
        }
        delete reader;
        return;
    }
    int64_t commit_ts = -1;
    int write_type;
    int64_t data_ts = -1;
    while (!reader->Done()) {
        // decode primary cell write col value
        std::string reader_value = reader->Value();
        DecodeWriteValue(reader_value, &write_type, &data_ts);
        VLOG(12) << "[gtxn][get][ " << start_ts_ << "] decode primary 'write', ts=" << reader->Timestamp()
                 << ", type=" << write_type << ", value=" << data_ts;
        VLOG(12) << "[gtxn][get][ " << start_ts_ << "] primary start_ts=" << primary.gtxn_start_ts();
        if (data_ts > 0 && data_ts < primary.gtxn_start_ts()) {
            status->SetFailed(ErrorCode::kGTxnPrimaryLost, "primary lost, not 'lock' and 'write'");
            delete reader;
            return;      
        } else if (data_ts == primary.gtxn_start_ts()) {
            commit_ts = reader->Timestamp();
            break;
        }
        reader->Next();
    }
    delete reader;

    if (commit_ts > 0) {
        RowMutation* this_mu = cell.Table()->NewRowMutation(cell.RowKey());
        this_mu->Put(cell.ColFamily(), 
                     cell.WriteName(), 
                     EncodeWriteValue(lock_type, data_ts), 
                     commit_ts);
        this_mu->DeleteColumns(cell.ColFamily(), cell.LockName(), commit_ts);
        cell.Table()->ApplyMutation(this_mu);
        if (this_mu->GetError().GetType() != tera::ErrorCode::kOK) {
            LOG(WARNING) << this_mu->GetError().GetReason();
            *status = this_mu->GetError();
        }
        delete this_mu;
    } else {
        status->SetFailed(ErrorCode::kGTxnPrimaryLost, "not found primary cell");
    }
}

void GlobalTxn::SaveWrite(const std::string& tablename, const std::string& row_key, 
                         tera::Write& w) {
    MutexLock lock(&mu_);
    TableWithRowkey twr(tablename, row_key);
    auto it = writes_.find(twr);
    if (it != writes_.end()) {
        std::vector<Write>* ws_ptr = &(writes_[twr]);
        ws_ptr->push_back(w);
    } else {
        std::vector<Write> ws;
        ws.push_back(w);   
        writes_[twr] = ws; 
        writes_cnt_.Inc();
    }
}

void GlobalTxn::SetLastStatus(ErrorCode* status) {
    MutexLock lock(&mu_);
    if (!status_returned_) {
        VLOG(12) << "[gtxn][commit][status][" << start_ts_ << "]" << status->ToString();
        status_.SetFailed(status->GetType(), status->GetReason());
        status_returned_ = true;
    }
}

void GlobalTxn::RunUserCallback() {
    if (status_.GetType() == ErrorCode::kOK) {
        gtxn_commit_cnt.Inc();
    } else {
        gtxn_commit_fail_cnt.Inc();
    }
    gtxn_internal_->PerfCommitDelay(0, get_micros()); // finish_time
    if (user_commit_callback_ != NULL) {
        VLOG(12) << "[gtxn][commit][callback][" << start_ts_ << "]" << status_.ToString();
        user_commit_callback_(this);
    } else {
        MutexLock lock(&finish_mutex_);
        VLOG(12) << "[gtxn][commit][finish][" << start_ts_ << "]" << status_.ToString();
        finish_ = true;
        finish_cond_.Signal(); 
    }
}

ErrorCode GlobalTxn::Commit() { 
    /// begin commit
    gtxn_internal_->TEST_Sleep(); 
    gtxn_internal_->PerfCommitDelay(get_micros(), 0); // begin_time
    ErrorCode status;
    if (put_fail_cnt_.Get() > 0 || has_commited_) {
        std::string reason("commit failed, has_commited[" + 
                std::to_string(has_commited_.load()) +
                "], put_fail_cnt[" + std::to_string(put_fail_cnt_.Get()) + "]");
        VLOG(12) << reason;
        status.SetFailed(ErrorCode::kGTxnOpAfterCommit, reason);
        SetLastStatus(&status);
        // Callback Point : put applyMutation failed or has commited
        RunUserCallback();
        return status;
    }
    has_commited_ = true;
    // don't have any writes
    if (writes_cnt_.Get() == 0) {
        status.SetFailed(ErrorCode::kOK, "No modification exists");
        SetLastStatus(&status);
        // Callback Point
        RunUserCallback();
        return status;
    }
    thread_pool_->AddTask(std::bind(&GlobalTxn::InternalCommit, this));

    if (user_commit_callback_ == NULL) {
        WaitForComplete();
    }
    return status_;
}

void GlobalTxn::InternalCommit() {
    gtxn_internal_->SetCommitDuration(timeout_ms_);

    /// begin prewrite
    gtxn_internal_->TEST_Sleep();

    // on ReadCommitedSnapshot level will get new timestamp before prewrite
    if (isolation_level_ == IsolationLevel::kReadCommitedSnapshot) {
        if (FLAGS_tera_gtxn_test_opened) {
            prewrite_start_ts_ = gtxn_internal_->TEST_GetPrewriteStartTimestamp();
        } else if (!FLAGS_tera_sdk_tso_client_enabled) {
            start_ts_ = get_micros();
        } else {
            timeoracle::TimeoracleClientImpl tsoc(thread_pool_, tso_cluster_); 
            prewrite_start_ts_ = tsoc.GetTimestamp(1);
        }
        if (prewrite_start_ts_ < start_ts_) {
            ErrorCode status;
            LOG(ERROR) << "[gtxn][prewrite][" << start_ts_ <<"] get prewrite new ts failed";
            status.SetFailed(ErrorCode::kGTxnTimestampLost, "get prewrite new ts failed");
            SetLastStatus(&status);
            RunUserCallback();
            return;
        }
        gtxn_internal_->SetPrewriteStartTimestamp(prewrite_start_ts_);
    }
    VLOG(12) << "[gtxn][prewrite][" << start_ts_ << "]";
    gtxn_internal_->PerfPrewriteDelay(get_micros(), 0); // begin_time
    gtxn_prewrite_cnt.Inc();

    prewrite_iterator_ = writes_.begin();
    primary_write_ = &(prewrite_iterator_->second[0]);
    primary_write_->Serialize(prewrite_start_ts_, 
                              gtxn_internal_->GetClientSession(), 
                              &serialized_primary_);
    AsyncPrewrite(&prewrite_iterator_->second);
}

// [prewrite] Step(1): 
//      read "lock", "write" column from tera
//
// aysnc prewrite one row use single_row_txn
//
void GlobalTxn::AsyncPrewrite(std::vector<Write>* ws) {
    assert(ws->size() > 0);
    // find table and rowkey to new reader and single row txn
    Write w = *(ws->begin());
    Table* table = w.Table();
    Transaction* single_row_txn = table->StartRowTransaction(w.RowKey());
    RowReader* reader = table->NewRowReader(w.RowKey());
    // set internal reader timeout 
    gtxn_internal_->SetInternalSdkTaskTimeout(reader);
    // set cf qu and timerange for reader
    gtxn_internal_->BuildRowReaderForPrewrite(*ws, reader);
    // set callback, context, single row txn for reader
    reader->SetCallBack([](RowReader* r){
        GlobalTxn* gtxn = static_cast<GlobalTxn*>(((PrewriteContext*)r->GetContext())->gtxn);
        gtxn->thread_pool_->AddTask(std::bind(&GlobalTxn::DoPrewriteReaderCallback, gtxn, r));
    });
    PrewriteContext* ctx = new PrewriteContext(ws, this, w.TableName(), w.RowKey());
    if (gtxn_internal_->IsTimeOut()) {
        ctx->status.SetFailed(ErrorCode::kGTxnPrewriteTimeout, "global transaction prewrite timeout");
        VLOG(12) << "[gtxn][prewrite][stxn_read] ignored : " << ctx->DebugString();
        RunAfterPrewriteFailed(ctx);
    } else {
        reader->SetContext(ctx);
        // get async
        VLOG(12) << "[gtxn][prewrite][stxn_read] invoked : " << ctx->DebugString();
        single_row_txn->Get(reader);
    }
}

// [prewrite] Step(2): 
//      a) verify [prewrite] step(1) read result status and no conflict 
//      b) write "lock" and "data" column to tera, through same single_row_txn in step(1)
//
// call by [prewrite] step(1),through reader callback
// 
void GlobalTxn::DoPrewriteReaderCallback(RowReader* reader) {
    PrewriteContext* ctx = (PrewriteContext*)reader->GetContext();
    if (reader->GetError().GetType() != ErrorCode::kNotFound
        && reader->GetError().GetType() != ErrorCode::kOK) {
        ctx->status = reader->GetError();
        VLOG(12) << "[gtxn][prewrite][stxn_read] failed : " << ctx->status.ToString();
        if (gtxn_internal_->IsTimeOut() || reader->GetError().GetType() == ErrorCode::kTimeout) {
            ctx->status.SetFailed(ErrorCode::kGTxnPrewriteTimeout, ctx->status.ToString());
        }
        delete reader;
        RunAfterPrewriteFailed(ctx);
    } else if (gtxn_internal_->ConflictWithOtherWrite(ctx->ws, reader, &(ctx->status))) {
        VLOG(12) << "[gtxn][prewrite][stxn_read] failed : " << ctx->status.ToString();
        delete reader;
        RunAfterPrewriteFailed(ctx);
    } else {
        VLOG(12) << "[gtxn][prewrite][stxn_read] succeed, table=" << ctx->DebugString();
        Table* t = reader->GetTable();
        RowMutation* prewrite_mu = t->NewRowMutation(reader->RowKey());
        // set internal task timeout
        gtxn_internal_->SetInternalSdkTaskTimeout(prewrite_mu);
        gtxn_internal_->BuildRowMutationForPrewrite(ctx->ws, prewrite_mu, 
                                                    serialized_primary_);
        
        // commit single_row_txn
        SingleRowTxn* single_row_txn = static_cast<SingleRowTxn*>(reader->GetTransaction());
        delete reader;
        single_row_txn->SetContext(ctx);
        single_row_txn->SetCommitCallback([](Transaction* single_txn) {
            GlobalTxn* gtxn = static_cast<GlobalTxn*>(((PrewriteContext*)single_txn->GetContext())->gtxn);
            SingleRowTxn* stxn = static_cast<SingleRowTxn*>(single_txn); 
            gtxn->thread_pool_->AddTask(std::bind(&GlobalTxn::DoPrewriteCallback, gtxn, stxn));
        });
        if (gtxn_internal_->IsTimeOut()) {
            ctx->status.SetFailed(ErrorCode::kGTxnPrewriteTimeout, "global transaction prewrite timeout");
            VLOG(12) << "[gtxn][prewrite][stxn_commit] ignored : " << ctx->DebugString();
            delete single_row_txn;
            delete prewrite_mu;
            RunAfterPrewriteFailed(ctx);
        } else {
            single_row_txn->ApplyMutation(prewrite_mu);
            VLOG(12) << "[gtxn][prewrite][stxn_commit] invoked : " << ctx->DebugString();
            t->CommitRowTransaction(single_row_txn);
            delete prewrite_mu;
        }
    }
}

// prewrite Step(3):
//      verify [prewrite] step(2) single_row_txn commit status,
//      if the last prewrite callback and status ok, will call [commit]
//
// call by [prewrite] step(2), through single_row_txn commit callback
//      
void GlobalTxn::DoPrewriteCallback(SingleRowTxn* single_row_txn) {
    ErrorCode status = single_row_txn->GetError();
    PrewriteContext* ctx = (PrewriteContext*)single_row_txn->GetContext();
    delete single_row_txn;
    if (gtxn_internal_->IsTimeOut() || status.GetType() != ErrorCode::kOK) {
        // wapper timeout status for global transaction 
        if (gtxn_internal_->IsTimeOut() || status.GetType() == ErrorCode::kTimeout) {
            ctx->status.SetFailed(ErrorCode::kGTxnPrewriteTimeout, status.ToString()); 
        } else {
            ctx->status.SetFailed(status.GetType(), status.ToString());
        }
        VLOG(12) << "[gtxn][prewrite][stxn_commit] failed : " << ctx->DebugString();
        RunAfterPrewriteFailed(ctx);
    } else if (++prewrite_iterator_ != writes_.end()) {
        thread_pool_->AddTask(std::bind(&GlobalTxn::AsyncPrewrite, this, &(prewrite_iterator_->second)));
    } else {
        gtxn_internal_->PerfPrewriteDelay(0, get_micros()); // finish_time
        VLOG(12) << "prewrite done, next step";
        InternalCommitPhase2();
    }
}

void GlobalTxn::RunAfterPrewriteFailed(PrewriteContext* ctx) {
    gtxn_internal_->PerfPrewriteDelay(0, get_micros()); // finish_time
    gtxn_prewrite_fail_cnt.Inc();
    if (gtxn_internal_->IsTimeOut() || ctx->status.GetType() == ErrorCode::kTimeout) {
        ctx->status.SetFailed(ErrorCode::kGTxnPrewriteTimeout, ctx->status.ToString()); 
    }
    SetLastStatus(&ctx->status);
    delete ctx;
    RunUserCallback();
}

// commit phase2 Step(1):
//      a) get timestamp from timeoracle for commit_ts
//      b) sync commit primary write through single_row_txn
//         (for this gtxn, on this step only one thread can work)
//      c) loop call [commit phase2] step(2)
//
// call by [prewrite] step(3)
void GlobalTxn::InternalCommitPhase2() {
    gtxn_internal_->PerfPrimaryCommitDelay(get_micros(), 0); // begin_time
    gtxn_primary_cnt.Inc();
    gtxn_internal_->TEST_Sleep(); // end prewrite
    ErrorCode status;
    status.SetFailed(ErrorCode::kOK);
    gtxn_internal_->TEST_Sleep(); // wait to begin commit

    if (FLAGS_tera_gtxn_test_opened) {
        commit_ts_ = gtxn_internal_->TEST_GetCommitTimestamp();
    } else if (!FLAGS_tera_sdk_tso_client_enabled) {
        start_ts_ = get_micros();
    } else {
        timeoracle::TimeoracleClientImpl tsoc(thread_pool_, tso_cluster_); 
        commit_ts_ = tsoc.GetTimestamp(1);
    }
    if (commit_ts_ < prewrite_start_ts_) {
        LOG(ERROR) << "[gtxn][commit] get commit ts failed";
        status.SetFailed(ErrorCode::kGTxnTimestampLost, "get commit ts failed");
        SetLastStatus(&status);
        gtxn_internal_->PerfPrimaryCommitDelay(0, get_micros());
        gtxn_primary_fail_cnt.Inc();
        RunUserCallback();
        return;
    }

    VLOG(12) << "[gtxn][commit] commit_ts:" << commit_ts_;
    gtxn_internal_->TEST_Sleep(); // wait to begin primary commit
    
    /// begin to commit primary
    VerifyPrimaryLocked();
}

void GlobalTxn::VerifyPrimaryLocked() {
    Table* pri_t = primary_write_->Table();
    tera::Transaction* pri_txn = pri_t->StartRowTransaction(primary_write_->RowKey());
    RowReader* reader = pri_t->NewRowReader(primary_write_->RowKey());
    // set internal task timeout
    gtxn_internal_->SetInternalSdkTaskTimeout(reader); 
    reader->AddColumn(primary_write_->ColFamily(), primary_write_->LockName());
    reader->SetTimeRange(prewrite_start_ts_, prewrite_start_ts_);
    reader->SetCallBack([](RowReader* r) {
            ((GlobalTxn*)r->GetContext())->DoVerifyPrimaryLockedCallback(r);});
    reader->SetContext(this);
    pri_txn->Get(reader);
}

void GlobalTxn::DoVerifyPrimaryLockedCallback(RowReader* reader) {
    ErrorCode status = reader->GetError();
    SingleRowTxn* pri_txn = static_cast<SingleRowTxn*>(reader->GetTransaction());
    delete reader;

    if (status.GetType() == ErrorCode::kOK) {
        CommitPrimary(pri_txn);
    } else {
        delete pri_txn;
        if (status.GetType() == ErrorCode::kNotFound) {
            status.SetFailed(ErrorCode::kGTxnPrimaryLost, "primary 'lock' lost before commit");
        } else if (status.GetType() == ErrorCode::kTimeout) {
            status.SetFailed(ErrorCode::kGTxnPrimaryCommitTimeout, status.ToString()); 
        }
        SetLastStatus(&status);
        gtxn_primary_fail_cnt.Inc();
        gtxn_internal_->PerfPrimaryCommitDelay(0, get_micros()); // finish_time
        RunUserCallback();
    }
}

void GlobalTxn::CommitPrimary(SingleRowTxn* pri_txn) {
    Table* pri_t = primary_write_->Table();
    RowMutation* primary_mu = pri_t->NewRowMutation(primary_write_->RowKey());
    // set internal task timeout
    gtxn_internal_->SetInternalSdkTaskTimeout(primary_mu); 
    primary_mu->Put(primary_write_->ColFamily(), primary_write_->WriteName(), 
                    EncodeWriteValue(primary_write_->WriteType(), prewrite_start_ts_), commit_ts_);
    primary_mu->DeleteColumns(primary_write_->ColFamily(), primary_write_->LockName(), commit_ts_);
    pri_txn->ApplyMutation(primary_mu);
    pri_txn->SetCommitCallback([] (Transaction* txn) {
        ((GlobalTxn*)txn->GetContext())->CheckPrimaryStatusAndCommmitSecondaries(txn);
    });
    pri_txn->SetContext(this);
    pri_txn->Commit();
    delete primary_mu;
}

void GlobalTxn::CheckPrimaryStatusAndCommmitSecondaries(Transaction* pri_txn) {
	ErrorCode status = pri_txn->GetError();
    delete pri_txn;
    gtxn_internal_->TEST_Sleep();
    // primary commit failed callback and return
    if (status.GetType() != tera::ErrorCode::kOK) {
        VLOG(12) << "[gtxn][commit] primary failed :[" << status.ToString() << "]";
        // Callback Point : primary commit failed
        if (status.GetType() == ErrorCode::kTimeout) {
            status.SetFailed(ErrorCode::kGTxnPrimaryCommitTimeout, status.ToString()); 
        }
        SetLastStatus(&status);
        gtxn_primary_fail_cnt.Inc();
        gtxn_internal_->PerfPrimaryCommitDelay(0, get_micros()); // finish_time
        RunUserCallback();
        return;
    }
    gtxn_internal_->PerfPrimaryCommitDelay(0, get_micros()); // finish_time
    if (acks_cnt_.Get() == 0 && notifies_cnt_.Get() == 0) {
        SetLastStatus(&status);
    }
    // wait primary commit done
	VLOG(12) << "[gtxn][commit] succeed :[" << start_ts_ 
              << "," << prewrite_start_ts_ << "," << commit_ts_ << "]";

    std::vector<Write>* ws = &(writes_.begin()->second);
    if (ws->size() == 1) {
        writes_.erase(writes_.begin());
        writes_cnt_.Dec();
    } else {
        ws->erase(ws->begin());
    }

    all_task_pushed_ = false;
    /// begin commit secondaries
    for (auto &same_row_writes : writes_) {
        thread_pool_->AddTask(std::bind(&GlobalTxn::AsyncCommitSecondaries, 
                                       this, &(same_row_writes.second)));
    }

    /// begin ack
    for (auto &same_row_acks : acks_) {
        thread_pool_->AddTask(std::bind(&GlobalTxn::AsyncAck, 
                                       this, &(same_row_acks.second)));
    }
    /// begin notify
    for (auto &same_row_notifies : notifies_) {
        thread_pool_->AddTask(std::bind(&GlobalTxn::AsyncNotify, 
                                       this, &(same_row_notifies.second)));
    }
    bool should_callback = false;
    {
        MutexLock lock(&mu_);
        all_task_pushed_ = true;
        should_callback = commit_secondaries_done_cnt_.Get() == writes_cnt_.Get() 
                            && acks_cnt_.Get() == ack_done_cnt_.Get() 
                            && notifies_cnt_.Get() == notify_done_cnt_.Get()
                            && all_task_pushed_ == true;
    }
    if (should_callback) {
        RunUserCallback();
    }

}

void GlobalTxn::AsyncAck(std::vector<Write>* ws) {
    gtxn_internal_->PerfAckDelay(get_micros(), 0);
    gtxn_acks_cnt.Inc();
    assert(ws->size() > 0);
    Write w = *(ws->begin());
    Table* table = w.Table();
    RowMutation* mu = table->NewRowMutation(w.RowKey());
    gtxn_internal_->SetInternalSdkTaskTimeout(mu); 
    gtxn_internal_->BuildRowMutationForAck(ws, mu);
    mu->SetCallBack([](RowMutation* row_mu) {
            ((GlobalTxn*)row_mu->GetContext())->DoAckCallback(row_mu);});
    mu->SetContext(this);
    table->ApplyMutation(mu);
}

void GlobalTxn::DoAckCallback(RowMutation* mutation) {
    if (mutation->GetError().GetType() != tera::ErrorCode::kOK) {
        LOG(WARNING) << "[gtxn][commit][ack], failed"
                     << mutation->GetError().GetReason();
        ErrorCode status;
        status.SetFailed(ErrorCode::kGTxnOKButAckFailed, mutation->GetError().ToString());
        SetLastStatus(&status);
        gtxn_acks_fail_cnt.Inc();
    }
    delete mutation;
    bool should_callback = false;
    {
        MutexLock lock(&mu_);
        ack_done_cnt_.Inc();
        gtxn_internal_->PerfAckDelay(0, get_micros());
        should_callback = commit_secondaries_done_cnt_.Get() == writes_cnt_.Get() 
                            && acks_cnt_.Get() == ack_done_cnt_.Get() 
                            && notifies_cnt_.Get() == notify_done_cnt_.Get();
    }

    if (should_callback) {
        RunUserCallback();
    }
}

void GlobalTxn::AsyncNotify(std::vector<Write>* ws) {
    gtxn_internal_->PerfNotifyDelay(get_micros(), 0);
    gtxn_notifies_cnt.Inc();
    assert(ws->size() > 0);
    Write w = *(ws->begin());
    Table* table = w.Table();
    RowMutation* mu = table->NewRowMutation(w.RowKey());
    gtxn_internal_->SetInternalSdkTaskTimeout(mu); 
    gtxn_internal_->BuildRowMutationForNotify(ws, mu, commit_ts_);
    mu->SetCallBack([](RowMutation* row_mu) {
            ((GlobalTxn*)row_mu->GetContext())->DoNotifyCallback(row_mu);});
    mu->SetContext(this);
    table->ApplyMutation(mu);
}

void GlobalTxn::DoNotifyCallback(RowMutation* mutation) {
    if (mutation->GetError().GetType() != tera::ErrorCode::kOK) {
        LOG(WARNING) << "[gtxn][commit][notify], failed"
                     << mutation->GetError().GetReason();
        ErrorCode status;
        status.SetFailed(ErrorCode::kGTxnOKButNotifyFailed, mutation->GetError().ToString());
        gtxn_notifies_fail_cnt.Inc();
        SetLastStatus(&status);
    }
    delete mutation;

    bool should_callback = false;
    {
        MutexLock lock(&mu_);
        notify_done_cnt_.Inc();
        gtxn_internal_->PerfNotifyDelay(0, get_micros());
        should_callback = commit_secondaries_done_cnt_.Get() == writes_cnt_.Get() 
                            && acks_cnt_.Get() == ack_done_cnt_.Get() 
                            && notifies_cnt_.Get() == notify_done_cnt_.Get()
                            && all_task_pushed_ == true;
    }

    if (should_callback) {
        RunUserCallback();
    }
}

void GlobalTxn::AsyncCommitSecondaries(std::vector<Write>* ws) {
    gtxn_internal_->PerfSecondariesCommitDelay(get_micros(), 0); // begin time
    gtxn_secondaries_cnt.Inc();
    assert(ws->size() > 0);
    Write w = *(ws->begin());
    Table* table = w.Table();
    RowMutation* mu = table->NewRowMutation(w.RowKey());
    gtxn_internal_->SetInternalSdkTaskTimeout(mu); 
    gtxn_internal_->BuildRowMutationForCommit(ws, mu, commit_ts_);
    mu->SetCallBack([](RowMutation* row_mu) {
            ((GlobalTxn*)row_mu->GetContext())->DoCommitSecondariesCallback(row_mu);});
    mu->SetContext(this);
    table->ApplyMutation(mu);
}

void GlobalTxn::DoCommitSecondariesCallback(RowMutation* mutation) {
    if (mutation->GetError().GetType() != tera::ErrorCode::kOK) {
        LOG(WARNING) << "[gtxn][commit][secondaries], failed"
                     << mutation->GetError().GetReason();
        gtxn_secondaries_fail_cnt.Inc();
    }
    delete mutation;

    bool should_callback = false;
    {
        MutexLock lock(&mu_);
        commit_secondaries_done_cnt_.Inc();
        gtxn_internal_->PerfSecondariesCommitDelay(0, get_micros()); // finish time
        should_callback = commit_secondaries_done_cnt_.Get() == writes_cnt_.Get() 
                            && acks_cnt_.Get() == ack_done_cnt_.Get() 
                            && notifies_cnt_.Get() == notify_done_cnt_.Get()
                            && all_task_pushed_ == true;
    }
        
    if (should_callback) {
        RunUserCallback();
    }
}

void GlobalTxn::ApplyMutation(RowMutation* row_mu) {
    assert(row_mu != NULL);

    RowMutationImpl* row_mu_impl = static_cast<RowMutationImpl*>(row_mu);
    row_mu_impl->SetTransaction(this);
    row_mu_impl->SetError(ErrorCode::kOK);

    bool can_apply = false;
	if (!has_commited_.load()) {
		assert(put_fail_cnt_.Get() > -1);
		put_fail_cnt_.Inc();
		// check writes_size_ over limit
        MutexLock lock(&mu_);
		can_apply = gtxn_internal_->VerifyWritesSize(row_mu, &writes_size_);
	} else {
        std::string reason = "ApplyMutation failed, txn has committed at [" 
            + std::to_string(commit_ts_) + "]";
		LOG(ERROR) << "[gtxn][apply_mutation][" << start_ts_ << "]" << reason;
		row_mu_impl->SetError(ErrorCode::kGTxnOpAfterCommit, reason);
	}

    size_t writes_cnt = 0;
    
    if (can_apply && gtxn_internal_->VerifyUserRowMutation(row_mu)) {
        Table* table = row_mu->GetTable();
        const std::string& tablename = table->GetName();
        const std::string& row_key = row_mu->RowKey();
        for (size_t i = 0; i < row_mu->MutationNum(); ++i) {
            const RowMutation::Mutation& mu = row_mu->GetMutation(i);
            Cell cell(table, row_key, mu.family, mu.qualifier, start_ts_, mu.value);
            Write w(cell, mu.type);
            ++writes_cnt;
            SaveWrite(tablename, row_key, w);
        }
    }

    bool is_async = row_mu_impl->IsAsync();
    ErrorCode mu_err = row_mu_impl->GetError();
    
    if (mu_err.GetType() != ErrorCode::kOK || writes_cnt == 0) {
        if (!status_returned_) {
            status_.SetFailed(mu_err.GetType(), mu_err.GetReason());
            status_returned_ = true;
        }
        if (is_async) {
            thread_pool_->AddTask(std::bind(&RowMutationImpl::RunCallback, row_mu_impl));
        } else {
            // nothing to do
            // sync mu_err != ok will return before put_fail_cnt -1
        }
        return;
    }
    if (is_async) {
        thread_pool_->AddTask(std::bind(&RowMutationImpl::RunCallback, row_mu_impl));
    }
    // only succes put will -1
    assert(put_fail_cnt_.Get() > 0);
    put_fail_cnt_.Dec();
}

// for wait commit 
void GlobalTxn::WaitForComplete() {
    MutexLock lock(&finish_mutex_);
    while(!finish_) {
        finish_cond_.Wait();
    }
}

void GlobalTxn::Ack(Table* t, 
                    const std::string& row_key, 
                    const std::string& column_family, 
                    const std::string& qualifier) {
    if (t == NULL) {
        LOG(ERROR) << "set ack cell failed";
        return;
    }
    const std::string& tablename = t->GetName();
    Cell cell(t, row_key, column_family, qualifier);
    Write w(cell);
    TableWithRowkey twr(tablename, row_key);
    MutexLock lock(&mu_);
    auto it = acks_.find(twr);
    if (it != acks_.end()) {
        std::vector<Write>* acks_ptr = &(acks_[twr]);
        acks_ptr->push_back(w);
    } else {
        std::vector<Write> acks;
        acks.push_back(w);   
        acks_[twr] = acks; 
        acks_cnt_.Inc();
    }
}

void GlobalTxn::Notify(Table* t,
                       const std::string& row_key, 
                       const std::string& column_family, 
                       const std::string& qualifier) {
    if (t == NULL) {
        LOG(ERROR) << "set ack cell failed";
        return;
    }
    const std::string& tablename = t->GetName();
    Cell cell(t, row_key, column_family, qualifier);
    Write w(cell);
    TableWithRowkey twr(tablename, row_key);
    MutexLock lock(&mu_);
    auto it = notifies_.find(twr);
    if (it != notifies_.end()) {
        std::vector<Write>* notifies_ptr = &(notifies_[twr]);
        notifies_ptr->push_back(w);
    } else {
        std::vector<Write> notifies;
        notifies.push_back(w);   
        notifies_[twr] = notifies; 
        notifies_cnt_.Inc();
    }
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
