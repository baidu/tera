// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/tablet_writer.h"

#include <set>

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/this_thread.h"
#include "io/coding.h"
#include "io/io_utils.h"
#include "io/tablet_io.h"
#include "leveldb/lg_coding.h"
#include "proto/proto_helper.h"
#include "utils/counter.h"
#include "utils/string_util.h"
#include "utils/timer.h"

DECLARE_int32(tera_asyncwriter_pending_limit);
DECLARE_bool(tera_enable_level0_limit);
DECLARE_int32(tera_asyncwriter_sync_interval);
DECLARE_int32(tera_asyncwriter_sync_size_threshold);
DECLARE_int32(tera_asyncwriter_batch_size);
DECLARE_bool(tera_sync_log);

namespace tera {
namespace io {

TabletWriter::TabletWriter(TabletIO* tablet_io)
    : tablet_(tablet_io), stopped_(true),
      sync_timestamp_(0),
      active_buffer_instant_(false),
      active_buffer_size_(0),
      tablet_busy_(false) {
    active_buffer_ = new WriteTaskBuffer;
    sealed_buffer_ = new WriteTaskBuffer;
}

TabletWriter::~TabletWriter() {
    Stop();
    delete active_buffer_;
    delete sealed_buffer_;
}

void TabletWriter::Start() {
    {
        MutexLock lock(&status_mutex_);
        if (!stopped_) {
            LOG(WARNING) << "tablet writer has been started";
            return;
        }
        stopped_ = false;
    }
    LOG(INFO) << "start tablet writer ...";
    thread_.Start(boost::bind(&TabletWriter::DoWork, this));
    ThisThread::Yield();
}

void TabletWriter::Stop() {
    {
        MutexLock lock(&status_mutex_);
        if (stopped_) {
            return;
        }
        stopped_ = true;
    }

    worker_done_event_.Wait();

    FlushToDiskBatch(sealed_buffer_);
    FlushToDiskBatch(active_buffer_);

    LOG(INFO) << "tablet writer is stopped";
}

uint64_t TabletWriter::CountRequestSize(std::vector<const RowMutationSequence*>& row_mutation_vec,
                                        bool kv_only) {
    uint64_t data_size = 0;
    for (uint32_t i = 0; i < row_mutation_vec.size(); i++) {
        const RowMutationSequence& mu_seq = *row_mutation_vec[i];
        int32_t mu_num = mu_seq.mutation_sequence_size();
        for (int32_t j = 0; j < mu_num; j++) {
            const Mutation& mu = mu_seq.mutation_sequence(j);
            data_size += mu_seq.row_key().size() + mu.value().size();
            if (!kv_only) {
                data_size += mu.family().size()
                    + mu.qualifier().size()
                    + sizeof(mu.timestamp());
            }
        }
    }
    return data_size;
}

bool TabletWriter::Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
                         std::vector<StatusCode>* status_vec, bool is_instant,
                         WriteCallback callback, StatusCode* status) {
    static uint32_t last_print = time(NULL);
    const uint64_t MAX_PENDING_SIZE = FLAGS_tera_asyncwriter_pending_limit * 1024UL;

    MutexLock lock(&task_mutex_);
    if (stopped_) {
        LOG(ERROR) << "tablet writer is stopped";
        SetStatusCode(kAsyncNotRunning, status);
        return false;
    }
    if (active_buffer_size_ >= MAX_PENDING_SIZE || tablet_busy_) {
        uint32_t now_time = time(NULL);
        if (now_time > last_print) {
            LOG(WARNING) << "[" << tablet_->GetTablePath()
                << "] is too busy, active_buffer_size_: "
                << (active_buffer_size_>>10) << "KB, tablet_busy_: "
                << tablet_busy_;
            last_print = now_time;
        }
        SetStatusCode(kTabletNodeIsBusy, status);
        return false;
    }

    uint64_t request_size = CountRequestSize(*row_mutation_vec, tablet_->KvOnly());
    WriteTask task;
    task.row_mutation_vec = row_mutation_vec;
    task.status_vec = status_vec;
    task.callback = callback;

    active_buffer_->push_back(task);
    active_buffer_size_ += request_size;
    active_buffer_instant_ |= is_instant;
    if (active_buffer_size_ >= FLAGS_tera_asyncwriter_sync_size_threshold * 1024UL ||
        active_buffer_instant_) {
        write_event_.Set();
    }
    return true;
}

void TabletWriter::DoWork() {
    sync_timestamp_ = GetTimeStampInMs();
    int32_t sync_interval = FLAGS_tera_asyncwriter_sync_interval;
    if (sync_interval == 0) {
        sync_interval = 1;
    }

    while (!stopped_) {
        int64_t sleep_duration = sync_timestamp_ + sync_interval - GetTimeStampInMs();
        // 如果没数据, 等
        if (!SwapActiveBuffer(sleep_duration <= 0)) {
            if (sleep_duration <= 0) {
                sync_timestamp_ = GetTimeStampInMs();
            } else {
                write_event_.TimeWait(sleep_duration);
            }
            continue;
        }
        // 否则 flush
        VLOG(7) << "write data, sleep_duration: " << sleep_duration;

        FlushToDiskBatch(sealed_buffer_);
        sealed_buffer_->clear();
        sync_timestamp_ = GetTimeStampInMs();
    }
    LOG(INFO) << "AsyncWriter::DoWork done";
    worker_done_event_.Set();
}

bool TabletWriter::SwapActiveBuffer(bool force) {
    const uint64_t SYNC_SIZE = FLAGS_tera_asyncwriter_sync_size_threshold * 1024UL;
    if (FLAGS_tera_enable_level0_limit == true) {
        tablet_busy_ = tablet_->IsBusy();
    }

    MutexLock lock(&task_mutex_);
    if (active_buffer_->size() <= 0) {
        return false;
    }
    if (!force && !active_buffer_instant_ && active_buffer_size_ < SYNC_SIZE) {
        return false;
    }
    VLOG(7) << "SwapActiveBuffer, buffer:" << active_buffer_size_
        << ":" <<active_buffer_->size() << ", force:" << force
        << ", instant:" << active_buffer_instant_;
    WriteTaskBuffer* temp = active_buffer_;
    active_buffer_ = sealed_buffer_;
    sealed_buffer_ = temp;
    CHECK_EQ(0U, active_buffer_->size());

    active_buffer_size_ = 0;
    active_buffer_instant_ = false;

    return true;
}

void TabletWriter::BatchRequest(const std::vector<const RowMutationSequence*>& row_mutation_vec,
                                leveldb::WriteBatch* batch) {
    int64_t timestamp_old = 0;

    for (uint32_t i = 0; i < row_mutation_vec.size(); ++i) {
        const RowMutationSequence& row_mu = *row_mutation_vec[i];
        const std::string& row_key = row_mu.row_key();
        int32_t mu_num = row_mu.mutation_sequence().size();
        if (mu_num == 0) {
            continue;
        }
        if (tablet_->KvOnly()) {
            // only the last mutation take effect for kv
            const Mutation& mu = row_mu.mutation_sequence().Get(mu_num - 1);
            std::string tera_key;
            if (tablet_->GetSchema().raw_key() == TTLKv) { // TTL-KV
                if (mu.ttl() == -1) { // never expires
                    tablet_->GetRawKeyOperator()->EncodeTeraKey(row_key, "", "",
                            kLatestTs, leveldb::TKT_FORSEEK, &tera_key);
                } else { // no check of overflow risk ...
                    tablet_->GetRawKeyOperator()->EncodeTeraKey(row_key, "", "",
                            get_micros() / 1000000 + mu.ttl(), leveldb::TKT_FORSEEK, &tera_key);
                }
            } else { // Readable-KV
                tera_key.assign(row_key);
            }
            if (mu.type() == kPut) {
                batch->Put(tera_key, mu.value());
            } else {
                batch->Delete(tera_key);
            }
        } else {
            for (int32_t t = 0; t < mu_num; ++t) {
                const Mutation& mu = row_mu.mutation_sequence().Get(t);
                std::string tera_key;
                leveldb::TeraKeyType type = leveldb::TKT_VALUE;
                switch (mu.type()) {
                    case kDeleteRow:
                        type = leveldb::TKT_DEL;
                        break;
                    case kDeleteFamily:
                        type = leveldb::TKT_DEL_COLUMN;
                        break;
                    case kDeleteColumn:
                        type = leveldb::TKT_DEL_QUALIFIER;
                        break;
                    case kDeleteColumns:
                        type = leveldb::TKT_DEL_QUALIFIERS;
                        break;
                    case kAdd:
                        type = leveldb::TKT_ADD;
                        break;
                    case kAddInt64:
                        type = leveldb::TKT_ADDINT64;
                        break;
                    case kPutIfAbsent:
                        type = leveldb::TKT_PUT_IFABSENT;
                        break;
                    case kAppend:
                        type = leveldb::TKT_APPEND;
                        break;
                    default:
                        break;
                }
                int64_t timestamp = get_unique_micros(timestamp_old);
                timestamp_old = timestamp;
                if (!tablet_->GetSchema().enable_txn() &&
                    leveldb::TeraKey::IsTypeAllowUserSetTimestamp(type) &&
                    mu.has_timestamp() && mu.timestamp() < timestamp) {
                    timestamp = mu.timestamp();
                }
                tablet_->GetRawKeyOperator()->EncodeTeraKey(row_key, mu.family(), mu.qualifier(),
                                                            timestamp, type, &tera_key);
                uint32_t lg_id = 0;
                size_t lg_num = tablet_->ldb_options_.exist_lg_list->size();
                if (lg_num > 1) {
                    if (type != leveldb::TKT_DEL) {
                        lg_id = tablet_->GetLGidByCFName(mu.family());
                        leveldb::PutFixed32LGId(&tera_key, lg_id);
                        VLOG(10) << "Batch Request, key: " << DebugString(row_key)
                            << " family: " << mu.family() << ", lg_id: " << lg_id;
                        batch->Put(tera_key, mu.value());
                    } else {
                        // put row_del mark to all LGs
                        for (lg_id = 0; lg_id < lg_num; ++lg_id) {
                            std::string tera_key_tmp = tera_key;
                            leveldb::PutFixed32LGId(&tera_key_tmp, lg_id);
                            VLOG(10) << "Batch Request, key: " << DebugString(row_key)
                                << " family: " << mu.family() << ", lg_id: " << lg_id;
                            batch->Put(tera_key_tmp, mu.value());
                        }
                    }
                } else {
                    VLOG(10) << "Batch Request, key: " << DebugString(row_key)
                        << " family: " << mu.family() << ", qualifier " << mu.qualifier()
                        << ", ts " << timestamp << ", type " << type << ", lg_id: " << lg_id;
                    batch->Put(tera_key, mu.value());
                }
            }
        }
    }
}

bool TabletWriter::CheckConflict(const RowMutationSequence& row_mu,
                                 std::set<std::string>* commit_row_key_set,
                                 StatusCode* status) {
    const std::string& row_key = row_mu.row_key();
    if (row_mu.txn_read_info().has_read()) {
        if (!tablet_->GetSchema().enable_txn()) {
            VLOG(10) << "txn of row " << DebugString(row_key)
                     << " is interrupted: txn not enabled";
            SetStatusCode(kTxnFail, status);
            return false;
        }
        if (commit_row_key_set->find(row_key) != commit_row_key_set->end()) {
            VLOG(10) << "txn of row " << DebugString(row_key)
                     << " is interrupted: found same row in one batch";
            SetStatusCode(kTxnFail, status);
            return false;
        }
        if (!tablet_->SingleRowTxnCheck(row_key, row_mu.txn_read_info(), status)) {
            VLOG(10) << "txn of row " << DebugString(row_key)
                     << " is interrupted: check fail, status: "
                     << StatusCodeToString(*status);
            return false;
        }
        VLOG(10) << "txn of row " << DebugString(row_key) << " check pass";
    }
    commit_row_key_set->insert(row_key);
    return true;
}

void TabletWriter::FinishTask(const WriteTask& task, StatusCode status) {
    int32_t row_num = task.row_mutation_vec->size();
    tablet_->GetCounter().write_rows.Add(row_num);
    for (int32_t i = 0; i < row_num; i++) {
        tablet_->GetCounter().write_kvs.Add((*task.row_mutation_vec)[i]->mutation_sequence_size());
        if ((*task.status_vec)[i] == kTabletNodeOk) {
            (*task.status_vec)[i] = status;
        }
    }
    task.callback(task.row_mutation_vec, task.status_vec);
}

StatusCode TabletWriter::FlushToDiskBatch(WriteTaskBuffer* task_buffer) {
    size_t task_num = task_buffer->size();
    leveldb::WriteBatch batch;

    std::set<std::string> commit_row_key_set;
    std::vector<const RowMutationSequence*> commit_row_mu_vec;
    for (size_t i = 0; i < task_num; ++i) {
        WriteTask& task = (*task_buffer)[i];
        std::vector<const RowMutationSequence*>& row_mutation_vec = *task.row_mutation_vec;
        std::vector<StatusCode>& status_vec = *task.status_vec;
        for (size_t j = 0; j < row_mutation_vec.size(); ++j) {
            const RowMutationSequence* row_mu = row_mutation_vec[j];
            if (CheckConflict(*row_mu, &commit_row_key_set, &status_vec[j])) {
                commit_row_mu_vec.push_back(row_mu);
                status_vec[j] = kTabletNodeOk;
            }
        }
    }
    BatchRequest(commit_row_mu_vec, &batch);

    StatusCode status = kTabletNodeOk;
    const bool disable_wal = false;
    tablet_->WriteBatch(&batch, disable_wal, FLAGS_tera_sync_log, &status);
    batch.Clear();
    for (size_t i = 0; i < task_num; i++) {
        FinishTask((*task_buffer)[i], status);
    }
    VLOG(7) << "finish a batch: " << task_num;
    return status;
}

} // namespace tabletnode
} // namespace tera
