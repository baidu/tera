// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/tablet_writer.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/this_thread.h"
#include "io/coding.h"
#include "io/io_utils.h"
#include "io/tablet_io.h"
#include "leveldb/lg_coding.h"
#include "lock.h"
#include "proto/proto_helper.h"
#include "utils/counter.h"
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
    : m_tablet(tablet_io),
      m_lock_manager(&tablet_io->m_lock_manager),
      m_stopped(true),
      m_sync_timestamp(0),
      m_active_buffer_instant(false),
      m_active_buffer_size(0),
      m_tablet_busy(false) {
    m_active_buffer = new WriteTaskBuffer;
    m_sealed_buffer = new WriteTaskBuffer;
}

TabletWriter::~TabletWriter() {
    Stop();
    delete m_active_buffer;
    delete m_sealed_buffer;
}

void TabletWriter::Start() {
    {
        MutexLock lock(&m_status_mutex);
        if (!m_stopped) {
            LOG(WARNING) << "tablet writer has been started";
            return;
        }
        m_stopped = false;
    }
    LOG(INFO) << "start tablet writer ...";
    m_thread.Start(boost::bind(&TabletWriter::DoWork, this));
    ThisThread::Yield();
}

void TabletWriter::Stop() {
    {
        MutexLock lock(&m_status_mutex);
        if (m_stopped) {
            return;
        }
        m_stopped = true;
    }

    m_worker_done_event.Wait();

    FlushToDiskBatch(m_sealed_buffer);
    FlushToDiskBatch(m_active_buffer);

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

    MutexLock lock(&m_task_mutex);
    if (m_stopped) {
        LOG(ERROR) << "tablet writer is stopped";
        SetStatusCode(kAsyncNotRunning, status);
        return false;
    }
    if (m_active_buffer_size >= MAX_PENDING_SIZE || m_tablet_busy) {
        uint32_t now_time = time(NULL);
        if (now_time > last_print) {
            LOG(WARNING) << "[" << m_tablet->GetTablePath()
                << "] is too busy, m_active_buffer_size: "
                << (m_active_buffer_size>>10) << "KB, m_tablet_busy: "
                << m_tablet_busy;
            last_print = now_time;
        }
        SetStatusCode(kTabletNodeIsBusy, status);
        return false;
    }

    uint64_t request_size = CountRequestSize(*row_mutation_vec, m_tablet->KvOnly());
    WriteTask task;
    task.row_mutation_vec = row_mutation_vec;
    task.status_vec = status_vec;
    task.callback = callback;

    m_active_buffer->push_back(task);
    m_active_buffer_size += request_size;
    m_active_buffer_instant |= is_instant;
    if (m_active_buffer_size >= FLAGS_tera_asyncwriter_sync_size_threshold * 1024UL ||
        m_active_buffer_instant) {
        m_write_event.Set();
    }
    return true;
}

void TabletWriter::DoWork() {
    m_sync_timestamp = GetTimeStampInMs();
    int32_t sync_interval = FLAGS_tera_asyncwriter_sync_interval;
    if (sync_interval == 0) {
        sync_interval = 1;
    }

    while (!m_stopped) {
        int64_t sleep_duration = m_sync_timestamp + sync_interval - GetTimeStampInMs();
        // 如果没数据, 等
        if (!SwapActiveBuffer(sleep_duration <= 0)) {
            if (sleep_duration <= 0) {
                m_sync_timestamp = GetTimeStampInMs();
            } else {
                m_write_event.TimeWait(sleep_duration);
            }
            continue;
        }
        // 否则 flush
        VLOG(7) << "write data, sleep_duration: " << sleep_duration;

        FlushToDiskBatch(m_sealed_buffer);
        m_sealed_buffer->clear();
        m_sync_timestamp = GetTimeStampInMs();
    }
    LOG(INFO) << "AsyncWriter::DoWork done";
    m_worker_done_event.Set();
}

bool TabletWriter::SwapActiveBuffer(bool force) {
    const uint64_t SYNC_SIZE = FLAGS_tera_asyncwriter_sync_size_threshold * 1024UL;
    if (FLAGS_tera_enable_level0_limit == true) {
        m_tablet_busy = m_tablet->IsBusy();
    }

    MutexLock lock(&m_task_mutex);
    if (m_active_buffer->size() <= 0) {
        return false;
    }
    if (!force && !m_active_buffer_instant && m_active_buffer_size < SYNC_SIZE) {
        return false;
    }
    VLOG(7) << "SwapActiveBuffer, buffer:" << m_active_buffer_size
        << ":" <<m_active_buffer->size() << ", force:" << force
        << ", instant:" << m_active_buffer_instant;
    WriteTaskBuffer* temp = m_active_buffer;
    m_active_buffer = m_sealed_buffer;
    m_sealed_buffer = temp;
    CHECK_EQ(0U, m_active_buffer->size());

    m_active_buffer_size = 0;
    m_active_buffer_instant = false;

    return true;
}

void TabletWriter::BatchRequest(WriteTask& task, leveldb::WriteBatch* batch) {
    std::vector<const RowMutationSequence*>& row_mutation_vec = *task.row_mutation_vec;
    std::vector<StatusCode>& status_vec = *task.status_vec;
    for (uint32_t i = 0; i < row_mutation_vec.size(); ++i) {
        const RowMutationSequence& row_mu = *row_mutation_vec[i];
        StatusCode* status = &status_vec[i];
        *status = kTableOk;
        if (m_tablet->KvOnly()) {
            BatchKvRequest(row_mu, batch);
        } else {
            BatchTableRequest(row_mu, batch, status);
        }
    }
}

void TabletWriter::BatchKvRequest(const RowMutationSequence& row_mu,
                                  leveldb::WriteBatch* batch) {
    const std::string& row_key = row_mu.row_key();
    int32_t mu_num = row_mu.mutation_sequence().size();
    // only the last mutation take effect for kv
    const Mutation& mu = row_mu.mutation_sequence().Get(mu_num - 1);
    std::string tera_key;
    if (m_tablet->GetSchema().raw_key() == TTLKv) { // TTL-KV
        if (mu.ttl() == -1) { // never expires
            m_tablet->GetRawKeyOperator()->EncodeTeraKey(row_key, "", "",
                    kLatestTs, leveldb::TKT_FORSEEK, &tera_key);
        } else { // no check of overflow risk ...
            m_tablet->GetRawKeyOperator()->EncodeTeraKey(row_key, "", "",
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
}

bool TabletWriter::BatchTableRequest(const RowMutationSequence& row_mu,
                                     leveldb::WriteBatch* batch,
                                     StatusCode* status) {
    int64_t timestamp_old = 0;
    const std::string& row_key = row_mu.row_key();
    if (m_tablet->GetSchema().enable_txn()) {
        if (!HandleLockBeforeWrite(row_mu, batch, status)) {
            return false;
        }
    }

    int32_t mu_num = row_mu.mutation_sequence().size();
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
        if (mu.has_timestamp() && mu.timestamp() < timestamp) {
            timestamp = mu.timestamp();
        }
        m_tablet->GetRawKeyOperator()->EncodeTeraKey(row_key, mu.family(), mu.qualifier(),
                                                     timestamp, type, &tera_key);
        uint32_t lg_id = 0;
        size_t lg_num = m_tablet->GetLGNum();
        if (type != leveldb::TKT_DEL) {
            lg_id = m_tablet->GetLGidByCFName(mu.family());
            leveldb::PutFixed32LGId(&tera_key, lg_id);
            VLOG(10) << "Batch Request, key:" << row_key << ",family:"
                << mu.family() << ",lg_id:" << lg_id;
            batch->Put(tera_key, mu.value());
        } else {
            // put row_del mark to all LGs
            for (lg_id = 0; lg_id < lg_num; ++lg_id) {
                std::string tera_key_tmp = tera_key;
                leveldb::PutFixed32LGId(&tera_key_tmp, lg_id);
                VLOG(10) << "Batch Request, key:" << row_key << ",family:"
                    << mu.family() << ",lg_id:" << lg_id;
                batch->Put(tera_key_tmp, mu.value());
            }
        }
    }
    return true;
}

bool TabletWriter::HandleLockBeforeWrite(const RowMutationSequence& mu_seq,
                                         leveldb::WriteBatch* batch,
                                         StatusCode* status) {
    const std::string& row_key = mu_seq.row_key();

    if (mu_seq.lock_before_write()) {
        if (!m_lock_manager->Lock(row_key, mu_seq.lock_id(),
                                  mu_seq.lock_annotation(), status)) {
            return false;
        }
        BatchLock(row_key, get_micros(), mu_seq.lock_id(),
                  mu_seq.lock_annotation(), batch);
        return true;
    }

    if (mu_seq.unlock_after_write()) {
        if (!m_lock_manager->IsLocked(row_key, mu_seq.lock_id(), NULL, status)) {
            return false;
        }
        BatchUnlock(row_key, batch);
        return true;
    }

    StatusCode lock_status = kTableOk;
    if (m_lock_manager->IsLocked(row_key, mu_seq.lock_id(), NULL, &lock_status)) {
        return true;
    } else if (lock_status == kLockNotOwn || mu_seq.insure_locked_already()) {
        SetStatusCode(lock_status, status);
        return false;
    } else {
        CHECK_EQ(lock_status, kLockNotExist);
        return true;
    }
}

void TabletWriter::HandleLockAfterWrite(const RowMutationSequence& mu_seq,
                                        const StatusCode& write_status) {
    if (mu_seq.unlock_after_write()) {
        if (write_status != kTableOk) {
            LOG(WARNING) << "fail to write lock-delete mark: "
                << StatusCodeToString(write_status)
                << ", abort unload, key: " << mu_seq.row_key();
        } else {
            CHECK(m_lock_manager->Unlock(mu_seq.row_key(), mu_seq.lock_id(), NULL, NULL));
        }
    }
}

void TabletWriter::BatchLock(const std::string& row_key, int64_t timestamp,
                             uint64_t lock_id, const std::string& annotation,
                             leveldb::WriteBatch* batch) {
    std::string lock_key;
    m_tablet->GetRawKeyOperator()->EncodeTeraKey(row_key, "", "", kMaxTimeStamp,
                                                 leveldb::TKT_LOCK, &lock_key);
    std::string lock_value;
    m_tablet->EncodeLock(lock_id, timestamp, annotation, &lock_value);

    // put lock to all LGs
    size_t lg_num = m_tablet->GetLGNum();
    for (size_t lg_id = 0; lg_id < lg_num; ++lg_id) {
        std::string tera_key_tmp = lock_key;
        leveldb::PutFixed32LGId(&tera_key_tmp, lg_id);
        VLOG(10) << "Batch lock, key:" << row_key << ",lg_id:" << lg_id;
        batch->Put(tera_key_tmp, lock_value);
    }
    std::string tera_key_tmp = lock_key;
    leveldb::PutFixed32LGId(&tera_key_tmp, m_tablet->m_meta_lg_id);
    VLOG(10) << "Batch lock, key:" << row_key << ",lg_id:" << m_tablet->m_meta_lg_id;
    batch->Put(tera_key_tmp, lock_value);
}

void TabletWriter::BatchUnlock(const std::string& row_key, leveldb::WriteBatch* batch) {
    std::string lock_key;
    m_tablet->GetRawKeyOperator()->EncodeTeraKey(row_key, "", "", kMaxTimeStamp,
                                                 leveldb::TKT_LOCK, &lock_key);
    // put lock-delete mark to all LGs
    size_t lg_num = m_tablet->GetLGNum();
    for (size_t lg_id = 0; lg_id < lg_num; ++lg_id) {
        std::string tera_key_tmp = lock_key;
        leveldb::PutFixed32LGId(&tera_key_tmp, lg_id);
        VLOG(10) << "Batch unlock, key:" << row_key << ",lg_id:" << lg_id;
        batch->Delete(tera_key_tmp);
    }
    std::string tera_key_tmp = lock_key;
    leveldb::PutFixed32LGId(&tera_key_tmp, m_tablet->m_meta_lg_id);
    VLOG(10) << "Batch unlock, key:" << row_key << ",lg_id:" << m_tablet->m_meta_lg_id;
    batch->Delete(tera_key_tmp);
}

void TabletWriter::FinishTask(const WriteTask& task, StatusCode status) {
    int32_t row_num = task.row_mutation_vec->size();
    m_tablet->GetCounter().write_rows.Add(row_num);
    for (int32_t i = 0; i < row_num; i++) {
        const RowMutationSequence& row_mu = *(*task.row_mutation_vec)[i];
        StatusCode& row_status = (*task.status_vec)[i];
        if (row_status == kTableOk) {
            row_status = status;
        }
        if (!m_tablet->KvOnly() && m_tablet->GetSchema().enable_txn()) {
            HandleLockAfterWrite(row_mu, row_status);
        }
        m_tablet->GetCounter().write_kvs.Add(row_mu.mutation_sequence_size());
    }
    task.callback(task.row_mutation_vec, task.status_vec);
}

StatusCode TabletWriter::FlushToDiskBatch(WriteTaskBuffer* task_buffer) {
    size_t task_num = task_buffer->size();
    leveldb::WriteBatch batch;

    for (size_t i = 0; i < task_num; ++i) {
        BatchRequest((*task_buffer)[i], &batch);
    }

    StatusCode status = kTableOk;
    const bool disable_wal = false;
    m_tablet->WriteBatch(&batch, disable_wal, FLAGS_tera_sync_log, &status);
    batch.Clear();
    for (size_t i = 0; i < task_num; i++) {
        FinishTask((*task_buffer)[i], status);
    }
    VLOG(7) << "finish a batch: " << task_num;
    return status;
}

} // namespace tabletnode
} // namespace tera
