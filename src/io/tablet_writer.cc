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
    : m_tablet(tablet_io), m_stopped(true),
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

uint64_t TabletWriter::CountRequestSize(const WriteTabletRequest& request,
                                        const std::vector<int32_t>& index_list,
                                        bool kv_only) {
    uint64_t data_size = 0;
    int32_t index_num = index_list.size();

    for (int32_t i = 0; i < index_num; i++) {
        int32_t index = index_list[i];
        const RowMutationSequence& mu_seq = request.row_list(index);
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

void TabletWriter::Write(const WriteTabletRequest* request,
                         WriteTabletResponse* response,
                         google::protobuf::Closure* done,
                         const std::vector<int32_t>* index_list,
                         Counter* done_counter, WriteRpcTimer* timer) {
    static uint32_t last_print = time(NULL);
    const uint64_t MAX_PENDING_SIZE = FLAGS_tera_asyncwriter_pending_limit * 1024UL;
    uint64_t request_size = CountRequestSize(*request, *index_list,
                                             m_tablet->KvOnly());
    WriteTask task;
    task.request = request;
    task.response = response;
    task.done = done;
    task.index_list = index_list;
    task.done_counter = done_counter;
    task.timer = timer;
    int32_t row_num = request->row_list_size();

    MutexLock lock(&m_task_mutex);
    StatusCode code = kTabletNodeOk;
    if (m_stopped) {
        LOG(ERROR) << "tablet writer is stopped";
        code = kAsyncNotRunning;
    } else if (m_active_buffer_size >= MAX_PENDING_SIZE || m_tablet_busy) {
        uint32_t now_time = time(NULL);
        if (now_time > last_print) {
            LOG(WARNING) << "[" << m_tablet->GetTablePath()
                << "] is too busy, m_active_buffer_size: "
                << (m_active_buffer_size>>10) << "KB, m_tablet_busy: "
                << m_tablet_busy;
            last_print = now_time;
        }
        code = kTabletNodeIsBusy;
    }

    if (code != kTabletNodeOk) {
        int32_t index_num = index_list->size();
        for (int32_t i = 0; i < index_num; i++) {
            int32_t index = (*index_list)[i];
            response->mutable_row_status_list()->Set(index, code);
        }

        delete index_list;
        if (done_counter->Add(index_num) == row_num) {
            done->Run();
            delete done_counter;
            if (NULL != timer) {
                RpcTimerList::Instance()->Erase(timer);
                delete timer;
            }
        }

        return;
    }

    m_active_buffer->push_back(task);
    m_active_buffer_size += request_size;
    m_active_buffer_instant |= request->is_instant();
    if (m_active_buffer_size >= FLAGS_tera_asyncwriter_sync_size_threshold * 1024UL ||
        request->is_instant()) {
        m_write_event.Set();
    }
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

bool TabletWriter::BatchRequest(const WriteTabletRequest& request,
                                const std::vector<int32_t>& index_list,
                                leveldb::WriteBatch* batch,
                                bool kv_only) {
    int32_t index_num = index_list.size();
    int64_t timestamp_old = 0;

    for (int32_t i = 0; i < index_num; ++i) {
        int32_t index = index_list[i];
        const RowMutationSequence& row_mu = request.row_list(index);
        const std::string& row_key = row_mu.row_key();
        int32_t mu_num = row_mu.mutation_sequence().size();
        if (kv_only) {
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
                if (mu.has_timestamp() && mu.timestamp() < timestamp) {
                    timestamp = mu.timestamp();
                }
                m_tablet->GetRawKeyOperator()->EncodeTeraKey(row_key, mu.family(), mu.qualifier(),
                                                            timestamp, type, &tera_key);
                uint32_t lg_id = 0;
                size_t lg_num = m_tablet->m_ldb_options.exist_lg_list->size();
                if (lg_num > 1) {
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
                } else {
                    VLOG(10) << "Batch Request, key:" << row_key << ",family:"
                        << mu.family() << ",lg_id:" << lg_id;
                    batch->Put(tera_key, mu.value());
                }
            }
        }
    }
    return true;
}

void TabletWriter::FinishTask(const WriteTask& task, StatusCode status) {
    const WriteTabletRequest* request = task.request;
    int32_t row_num = request->row_list_size();

    int32_t index_num = task.index_list->size();
    m_tablet->GetCounter().write_rows.Add(index_num);

    WriteTabletResponse* response = task.response;
    for (int32_t i = 0; i < index_num; i++) {
        int32_t index = (*task.index_list)[i];
        const RowMutationSequence& row_list = request->row_list(index);
        m_tablet->GetCounter().write_kvs.Add(row_list.mutation_sequence_size());
        response->mutable_row_status_list()->Set(index, status);
    }

    delete task.index_list;
    if (task.done_counter->Add(index_num) == row_num) {
        task.done->Run();
        delete task.done_counter;
        WriteRpcTimer* timer = task.timer;
        if (NULL != timer) {
            RpcTimerList::Instance()->Erase(timer);
            delete timer;
        }
    }
}

StatusCode TabletWriter::FlushToDiskBatch(WriteTaskBuffer* task_buffer) {
    size_t task_num = task_buffer->size();
    leveldb::WriteBatch batch;

    for (size_t i = 0; i < task_num; ++i) {
        const WriteTask& task = (*task_buffer)[i];
        const std::vector<int32_t>* index_list = task.index_list;
        BatchRequest(*(task.request), *index_list, &batch, m_tablet->KvOnly());
    }

    StatusCode status = kTabletNodeOk;
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
