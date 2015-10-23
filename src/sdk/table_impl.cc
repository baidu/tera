// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/table_impl.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fstream>
#include <sstream>

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include "common/base/string_format.h"
#include "common/file/file_path.h"
#include "common/file/recordio/record_io.h"

#include "io/coding.h"
#include "proto/kv_helper.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode_client.h"
#include "sdk/mutate_impl.h"
#include "sdk/read_impl.h"
#include "sdk/scan_impl.h"
#include "sdk/schema_impl.h"
#include "sdk/sdk_zk.h"
#include "sdk/tera.h"
#include "utils/crypt.h"
#include "utils/string_util.h"
#include "utils/timer.h"

DECLARE_string(tera_master_meta_table_name);
DECLARE_int32(tera_sdk_delay_send_internal);
DECLARE_int32(tera_sdk_retry_times);
DECLARE_int32(tera_sdk_retry_period);
DECLARE_int32(tera_sdk_update_meta_internal);
DECLARE_bool(tera_sdk_write_sync);
DECLARE_int32(tera_sdk_batch_size);
DECLARE_int32(tera_sdk_write_send_interval);
DECLARE_int32(tera_sdk_read_send_interval);
DECLARE_int64(tera_sdk_max_mutation_pending_num);
DECLARE_int64(tera_sdk_max_reader_pending_num);
DECLARE_bool(tera_sdk_async_blocking_enabled);
DECLARE_int32(tera_sdk_timeout);
DECLARE_int32(tera_sdk_scan_buffer_limit);
DECLARE_int32(tera_sdk_update_meta_concurrency);
DECLARE_int32(tera_sdk_update_meta_buffer_limit);
DECLARE_bool(tera_sdk_cookie_enabled);
DECLARE_string(tera_sdk_cookie_path);
DECLARE_int32(tera_sdk_cookie_update_interval);
DECLARE_bool(tera_sdk_perf_counter_enabled);
DECLARE_int64(tera_sdk_perf_counter_log_interval);
DECLARE_int32(FLAGS_tera_rpc_timeout_period);

namespace tera {

TableImpl::TableImpl(const std::string& table_name,
                     const std::string& zk_root_path,
                     const std::string& zk_addr_list,
                     common::ThreadPool* thread_pool,
                     sdk::ClusterFinder* cluster)
    : _name(table_name),
      _last_sequence_id(0),
      _timeout(FLAGS_tera_sdk_timeout),
      _commit_size(FLAGS_tera_sdk_batch_size),
      _write_commit_timeout(FLAGS_tera_sdk_write_send_interval),
      _read_commit_timeout(FLAGS_tera_sdk_read_send_interval),
      _max_commit_pending_num(FLAGS_tera_sdk_max_mutation_pending_num),
      _max_reader_pending_num(FLAGS_tera_sdk_max_reader_pending_num),
      _meta_cond(&_meta_mutex),
      _meta_updating_count(0),
      _table_meta_cond(&_table_meta_mutex),
      _table_meta_updating(false),
      _zk_root_path(zk_root_path),
      _zk_addr_list(zk_addr_list),
      _thread_pool(thread_pool),
      _cluster(cluster),
      _cluster_private(false),
      _pending_timeout_ms(FLAGS_tera_rpc_timeout_period) {
    if (_cluster == NULL) {
        _cluster = new sdk::ClusterFinder(zk_root_path, zk_addr_list);
        _cluster_private = true;
    }
}

TableImpl::~TableImpl() {
    ClearDelayTask();
    if (FLAGS_tera_sdk_cookie_enabled) {
        DoDumpCookie();
    }
    if (_cluster_private) {
        delete _cluster;
    }
}

RowMutation* TableImpl::NewRowMutation(const std::string& row_key) {
    RowMutationImpl* row_mu = new RowMutationImpl(this, row_key);
    return row_mu;
}

RowReader* TableImpl::NewRowReader(const std::string& row_key) {
    RowReaderImpl* row_rd = new RowReaderImpl(this, row_key);
    return row_rd;
}

void TableImpl::ApplyMutation(RowMutation* row_mu) {
    std::vector<RowMutationImpl*> mu_list;
    mu_list.push_back(static_cast<RowMutationImpl*>(row_mu));
    DistributeMutations(mu_list, true);
}

void TableImpl::ApplyMutation(const std::vector<RowMutation*>& row_mutations) {
    std::vector<RowMutationImpl*> mu_list(row_mutations.size());
    for (uint32_t i = 0; i < row_mutations.size(); i++) {
        mu_list[i] = static_cast<RowMutationImpl*>(row_mutations[i]);
    }
    DistributeMutations(mu_list, true);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const int64_t value,
                    ErrorCode* err) {
    std::string value_str((char*)&value, sizeof(int64_t));
    return Put(row_key, family, qualifier, value_str, err);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    int64_t timestamp, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, timestamp, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    int32_t ttl, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, value, ttl);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    int64_t timestamp, int32_t ttl, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, timestamp, value, ttl);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Add(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t delta, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Add(family, qualifier, delta);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::AddInt64(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t delta, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->AddInt64(family, qualifier, delta);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::PutIfAbsent(const std::string& row_key, const std::string& family,
                            const std::string& qualifier, const std::string& value,
                            ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->PutIfAbsent(family, qualifier, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Append(const std::string& row_key, const std::string& family,
                       const std::string& qualifier, const std::string& value,
                       ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Append(family, qualifier, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Flush() {
    return false;
}

bool TableImpl::CheckAndApply(const std::string& rowkey, const std::string& cf_c,
                              const std::string& value, const RowMutation& row_mu,
                              ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return false;
}

int64_t TableImpl::IncrementColumnValue(const std::string& row,
                                        const std::string& family,
                                        const std::string& qualifier,
                                        int64_t amount, ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return 0L;
}

void TableImpl::SetWriteTimeout(int64_t timeout_ms) {
}

void TableImpl::Get(RowReader* row_reader) {
    std::vector<RowReaderImpl*> row_reader_list;
    row_reader_list.push_back(static_cast<RowReaderImpl*>(row_reader));
    DistributeReaders(row_reader_list, true);
}

void TableImpl::Get(const std::vector<RowReader*>& row_readers) {
    std::vector<RowReaderImpl*> row_reader_list(row_readers.size());
    for (uint32_t i = 0; i < row_readers.size(); ++i) {
        row_reader_list[i] = static_cast<RowReaderImpl*>(row_readers[i]);
    }
    DistributeReaders(row_reader_list, true);
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    ErrorCode* err, uint64_t snapshot_id) {
    std::string value_str;
    if (Get(row_key, family, qualifier, &value_str, err, snapshot_id)
        && value_str.size() == sizeof(int64_t)) {
        *value = *(int64_t*)value_str.c_str();
        return true;
    }
    return false;
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, std::string* value,
                    ErrorCode* err, uint64_t snapshot_id) {
    RowReader* row_reader = NewRowReader(row_key);
    row_reader->AddColumn(family, qualifier);
    row_reader->SetSnapshot(snapshot_id);
    Get(row_reader);
    *err = row_reader->GetError();
    if (err->GetType() == ErrorCode::kOK) {
        *value = row_reader->Value();
        return true;
    }
    return false;
}

ResultStream* TableImpl::Scan(const ScanDescriptor& desc, ErrorCode* err) {
    ScanDescImpl * impl = desc.GetImpl();
    impl->SetTableSchema(_table_schema);
    if (impl->GetFilterString() != "") {
        MutexLock lock(&_table_meta_mutex);
        if (!impl->ParseFilterString()) {
            // fail to parse filter string
            return NULL;
        }
    }
    ResultStream * results = NULL;
    if (desc.IsAsync() && !_table_schema.kv_only()) {
        VLOG(6) << "activate async-scan";
        results = new ResultStreamAsyncImpl(this, impl);
    } else {
        VLOG(6) << "activate sync-scan";
        results = new ResultStreamSyncImpl(this, impl);
    }
    return results;
}

void TableImpl::ScanTabletSync(ResultStreamSyncImpl* stream) {
    ScanTabletAsync(stream);
    stream->Wait();
}

void TableImpl::ScanTabletAsync(ResultStreamImpl* stream) {
    ScanTask* scan_task = new ScanTask;
    scan_task->stream = stream;
    stream->GetRpcHandle(&scan_task->request, &scan_task->response);
    ScanTabletAsync(scan_task, true);
}

void TableImpl::ScanTabletAsync(ScanTask* scan_task, bool called_by_user) {
    if (called_by_user) {
        scan_task->SetId(_next_task_id.Inc());
        _task_pool.PutTask(scan_task);
    }

    const std::string& row_key = scan_task->stream->GetScanDesc()->GetStartRowKey();
    std::string server_addr;
    if (GetTabletAddrOrScheduleUpdateMeta(row_key, scan_task, &server_addr)) {
        CommitScan(scan_task, server_addr);
    }
}

void TableImpl::CommitScan(ScanTask* scan_task,
                           const std::string& server_addr) {
    tabletnode::TabletNodeClient tabletnode_client(server_addr);
    ResultStreamImpl* stream = scan_task->stream;
    ScanTabletRequest* request = scan_task->request;
    ScanTabletResponse* response = scan_task->response;
    response->Clear();

    ScanDescImpl* impl = stream->GetScanDesc();
    request->set_sequence_id(_last_sequence_id++);
    request->set_table_name(_name);
    request->set_start(impl->GetStartRowKey());
    request->set_end(impl->GetEndRowKey());
    request->set_snapshot_id(impl->GetSnapshot());
    request->set_timeout(impl->GetPackInterval());
    if (impl->GetStartColumnFamily() != "") {
        request->set_start_family(impl->GetStartColumnFamily());
    }
    if (impl->GetStartQualifier() != "") {
        request->set_start_qualifier(impl->GetStartQualifier());
    }
    if (impl->GetStartTimeStamp() != 0) {
        request->set_start_timestamp(impl->GetStartTimeStamp());
    }
    if (impl->GetMaxVersion() != 0) {
        request->set_max_version(impl->GetMaxVersion());
    }
    if (impl->GetBufferSize() != 0) {
        request->set_buffer_limit(impl->GetBufferSize());
    }
    if (impl->GetTimerRange() != NULL) {
        TimeRange* time_range = request->mutable_timerange();
        time_range->CopyFrom(*(impl->GetTimerRange()));
    }
    if (impl->GetFilterString().size() > 0) {
        FilterList* filter_list = request->mutable_filter_list();
        filter_list->CopyFrom(impl->GetFilterList());
    }
    for (int32_t i = 0; i < impl->GetSizeofColumnFamilyList(); ++i) {
        tera::ColumnFamily* column_family = request->add_cf_list();
        column_family->CopyFrom(*(impl->GetColumnFamily(i)));
    }

    request->set_timestamp(common::timer::get_micros());
    Closure<void, ScanTabletRequest*, ScanTabletResponse*, bool, int>* done =
        NewClosure(this, &TableImpl::ScanCallBack, scan_task);
    tabletnode_client.ScanTablet(request, response, done);
}

void TableImpl::ScanCallBack(ScanTask* scan_task,
                             ScanTabletRequest* request,
                             ScanTabletResponse* response,
                             bool failed, int error_code) {
    _perf_counter.rpc_w.Add(common::timer::get_micros() - request->timestamp());
    _perf_counter.rpc_w_cnt.Inc();
    ResultStreamImpl* stream = scan_task->stream;

    if (failed) {
        if (error_code == sofa::pbrpc::RPC_ERROR_SERVER_SHUTDOWN ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNREACHABLE ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNAVAILABLE) {
            response->set_status(kServerError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_CANCELED ||
                   error_code == sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL) {
            response->set_status(kClientError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_CONNECTION_CLOSED ||
                   error_code == sofa::pbrpc::RPC_ERROR_RESOLVE_ADDRESS) {
            response->set_status(kConnectError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_TIMEOUT) {
            response->set_status(kRPCTimeout);
        } else {
            response->set_status(kRPCError);
        }
    }

    StatusCode err = response->status();
    if (err != kTabletNodeOk && err != kSnapshotNotExist) {
        VLOG(10) << "fail to scan table: " << _name
            << " errcode: " << StatusCodeToString(err);
    }

    scan_task->SetInternalError(err);
    if (err == kTabletNodeOk
        || err == kSnapshotNotExist
        || scan_task->RetryTimes() >= static_cast<uint32_t>(FLAGS_tera_sdk_retry_times)) {
        if (err == kKeyNotInRange || err == kConnectError) {
            ScheduleUpdateMeta(stream->GetScanDesc()->GetStartRowKey(),
                               scan_task->GetMetaTimeStamp());
        }
        stream->OnFinish(request, response);
        stream->ReleaseRpcHandle(request, response);
        _task_pool.PopTask(scan_task->GetId());
        CHECK_EQ(scan_task->GetRef(), 2);
        delete scan_task;
    } else if (err == kKeyNotInRange) {
        scan_task->IncRetryTimes();
        ScanTabletAsync(scan_task, false);
    } else {
        scan_task->IncRetryTimes();
        ThreadPool::Task retry_task =
            boost::bind(&TableImpl::ScanTabletAsync, this, scan_task, false);
        _thread_pool->DelayTask(
            FLAGS_tera_sdk_retry_period * scan_task->RetryTimes(), retry_task);
    }
}

void TableImpl::SetReadTimeout(int64_t timeout_ms) {
}

bool TableImpl::LockRow(const std::string& rowkey, RowLock* lock, ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return false;
}

bool TableImpl::GetStartEndKeys(std::string* start_key, std::string* end_key,
                                ErrorCode* err) {
    err->SetFailed(ErrorCode::kNotImpl);
    return false;
}

bool TableImpl::OpenInternal(ErrorCode* err) {
    if (!UpdateTableMeta(err)) {
        LOG(ERROR) << "fail to update table meta.";
        return false;
    }
    if (FLAGS_tera_sdk_cookie_enabled) {
        if (!RestoreCookie()) {
            LOG(ERROR) << "fail to restore cookie.";
            return false;
        }
        EnableCookieUpdateTimer();
    }
    if (FLAGS_tera_sdk_perf_counter_enabled) {
        DumpPerfCounterLogDelay();
    }
    return true;
}

struct MuFlushPair {
    std::vector<RowMutationImpl*> mu_list;
    bool flush;
    MuFlushPair() : flush(false) {}
};

void TableImpl::DistributeMutations(const std::vector<RowMutationImpl*>& mu_list,
                                    bool called_by_user) {
    typedef std::map<std::string, MuFlushPair> TsMuMap;
    TsMuMap ts_mu_list;

    int64_t sync_min_timeout = -1;
    std::vector<RowMutationImpl*> sync_mu_list;

    // evaluate minimum timeout of sync requests
    if (called_by_user) {
        for (uint32_t i = 0; i < mu_list.size(); i++) {
            RowMutationImpl* row_mutation = (RowMutationImpl*)mu_list[i];
            if (!row_mutation->IsAsync()) {
                sync_mu_list.push_back(row_mutation);
                int64_t row_timeout = row_mutation->TimeOut() > 0 ? row_mutation->TimeOut() : _timeout;
                if (row_timeout > 0 && (sync_min_timeout <= 0 || sync_min_timeout > row_timeout)) {
                    sync_min_timeout = row_timeout;
                }
            }
        }
    }

    for (uint32_t i = 0; i < mu_list.size(); i++) {
        RowMutationImpl* row_mutation = (RowMutationImpl*)mu_list[i];

        if (called_by_user) {
            row_mutation->SetId(_next_task_id.Inc());
            _task_pool.PutTask(row_mutation);

            int64_t row_timeout = -1;
            if (!row_mutation->IsAsync()) {
                row_timeout = sync_min_timeout;
            } else {
                row_timeout = row_mutation->TimeOut() > 0 ? row_mutation->TimeOut() : _timeout;
            }
            if (row_timeout > 0) {
                ThreadPool::Task task =
                    boost::bind(&TableImpl::MutationTimeout, this, row_mutation->GetId());
                _thread_pool->DelayTask(row_timeout, task);
            }
        }

        // flow control
        if (called_by_user
            && _cur_commit_pending_counter.Add(row_mutation->MutationNum()) > _max_commit_pending_num
            && row_mutation->IsAsync()) {
            if (FLAGS_tera_sdk_async_blocking_enabled) {
                while (_cur_commit_pending_counter.Get() > _max_commit_pending_num) {
                    usleep(100000);
                }
            } else {
                _cur_commit_pending_counter.Sub(row_mutation->MutationNum());
                row_mutation->SetError(ErrorCode::kBusy, "pending too much mutations, try it later.");
                ThreadPool::Task task =
                    boost::bind(&TableImpl::BreakRequest, this, row_mutation->GetId());
                row_mutation->DecRef();
                _thread_pool->AddTask(task);
                continue;
            }
        }

        std::string server_addr;
        if (!GetTabletAddrOrScheduleUpdateMeta(row_mutation->RowKey(),
                                               row_mutation, &server_addr)) {
            continue;
        }

        MuFlushPair& mu_flush_pair = ts_mu_list[server_addr];
        std::vector<RowMutationImpl*>& ts_row_mutations = mu_flush_pair.mu_list;
        ts_row_mutations.push_back(row_mutation);

        if (!row_mutation->IsAsync()) {
            mu_flush_pair.flush = true;
        }
    }

    TsMuMap::iterator it = ts_mu_list.begin();
    for (; it != ts_mu_list.end(); ++it) {
        MuFlushPair& mu_flush_pair = it->second;
        PackMutations(it->first, mu_flush_pair.mu_list, mu_flush_pair.flush);
    }
    // 从现在开始，所有异步的row_mutation都不可以再操作了，因为随时会被用户释放

    // 不是用户调用的，立即返回
    if (!called_by_user) {
        return;
    }

    // 等待同步操作返回或超时
    for (uint32_t i = 0; i < sync_mu_list.size(); i++) {
        while (_cur_commit_pending_counter.Get() > _max_commit_pending_num) {
            usleep(100000);
        }

        RowMutationImpl* row_mutation = (RowMutationImpl*)sync_mu_list[i];
        row_mutation->Wait();
    }
}

void TableImpl::DistributeMutationsById(std::vector<int64_t>* mu_id_list) {
    std::vector<RowMutationImpl*> mu_list;
    for (uint32_t i = 0; i < mu_id_list->size(); ++i) {
        int64_t mu_id = (*mu_id_list)[i];
        SdkTask* task = _task_pool.GetTask(mu_id);
        if (task == NULL) {
            VLOG(10) << "mutation " << mu_id << " timeout when retry mutate";;
            continue;
        }
        CHECK_EQ(task->Type(), SdkTask::MUTATION);
        RowMutationImpl* row_mutation = (RowMutationImpl*)task;
        mu_list.push_back(row_mutation);
    }
    DistributeMutations(mu_list, false);
    delete mu_id_list;
}

void TableImpl::PackMutations(const std::string& server_addr,
                              std::vector<RowMutationImpl*>& mu_list,
                              bool flush) {
    if (flush) {
        CommitMutations(server_addr, mu_list);
        return;
    }

    MutexLock lock(&_mutation_batch_mutex);
    TaskBatch* mutation_batch = NULL;
    std::map<std::string, TaskBatch>::iterator it =
        _mutation_batch_map.find(server_addr);
    if (it == _mutation_batch_map.end()) {
        mutation_batch = &_mutation_batch_map[server_addr];
        mutation_batch->row_id_list = new std::vector<int64_t>;
        ThreadPool::Task task =
            boost::bind(&TableImpl::MutationBatchTimeout, this, server_addr);
        int64_t timer_id = _thread_pool->DelayTask(_write_commit_timeout, task);
        mutation_batch->timer_id = timer_id;
    } else {
        mutation_batch = &it->second;
    }

    for (size_t i = 0; i < mu_list.size(); ++i) {
        RowMutationImpl* row_mutation = mu_list[i];
        mutation_batch->row_id_list->push_back(row_mutation->GetId());
        row_mutation->DecRef();
    }

    if (mutation_batch->row_id_list->size() >= _commit_size) {
        std::vector<int64_t>* mu_id_list = mutation_batch->row_id_list;
        uint64_t timer_id = mutation_batch->timer_id;
        _mutation_batch_mutex.Unlock();
        if (_thread_pool->CancelTask(timer_id)) {
            _mutation_batch_mutex.Lock();
            _mutation_batch_map.erase(server_addr);
            _mutation_batch_mutex.Unlock();
            CommitMutationsById(server_addr, *mu_id_list);
            delete mu_id_list;
        }
        _mutation_batch_mutex.Lock();
    }
}

void TableImpl::MutationBatchTimeout(std::string server_addr) {
    std::vector<int64_t>* mu_id_list = NULL;
    {
        MutexLock lock(&_mutation_batch_mutex);
        std::map<std::string, TaskBatch>::iterator it =
            _mutation_batch_map.find(server_addr);
        if (it == _mutation_batch_map.end()) {
            return;
        }
        TaskBatch* mutation_batch = &it->second;
        mu_id_list = mutation_batch->row_id_list;
        _mutation_batch_map.erase(it);
    }
    CommitMutationsById(server_addr, *mu_id_list);
    delete mu_id_list;
}

void TableImpl::CommitMutationsById(const std::string& server_addr,
                                    std::vector<int64_t>& mu_id_list) {
    std::vector<RowMutationImpl*> mu_list;
    for (size_t i = 0; i < mu_id_list.size(); i++) {
        int64_t mu_id = mu_id_list[i];
        SdkTask* task = _task_pool.GetTask(mu_id);
        if (task == NULL) {
            VLOG(10) << "mutation " << mu_id << " timeout";
            continue;
        }
        CHECK_EQ(task->Type(), SdkTask::MUTATION);
        mu_list.push_back((RowMutationImpl*)task);
    }
    CommitMutations(server_addr, mu_list);
}

void TableImpl::CommitMutations(const std::string& server_addr,
                                std::vector<RowMutationImpl*>& mu_list) {
    tabletnode::TabletNodeClient tabletnode_client_async(server_addr);
    WriteTabletRequest* request = new WriteTabletRequest;
    WriteTabletResponse* response = new WriteTabletResponse;
    request->set_sequence_id(_last_sequence_id++);
    request->set_tablet_name(_name);
    request->set_is_sync(FLAGS_tera_sdk_write_sync);

    std::vector<int64_t>* mu_id_list = new std::vector<int64_t>;
    for (uint32_t i = 0; i < mu_list.size(); ++i) {
        RowMutationImpl* row_mutation = mu_list[i];
        RowMutationSequence* mu_seq = request->add_row_list();
        mu_seq->set_row_key(row_mutation->RowKey());
        for (uint32_t j = 0; j < row_mutation->MutationNum(); j++) {
            const RowMutation::Mutation& mu = row_mutation->GetMutation(j);
            tera::Mutation* mutation = mu_seq->add_mutation_sequence();
            SerializeMutation(mu, mutation);
        }
        mu_id_list->push_back(row_mutation->GetId());
        row_mutation->DecRef();
    }

    request->set_timestamp(common::timer::get_micros());
    Closure<void, WriteTabletRequest*, WriteTabletResponse*, bool, int>* done =
        NewClosure(this, &TableImpl::MutateCallBack, mu_id_list);
    tabletnode_client_async.WriteTablet(request, response, done);
}

void TableImpl::MutateCallBack(std::vector<int64_t>* mu_id_list,
                               WriteTabletRequest* request,
                               WriteTabletResponse* response,
                               bool failed, int error_code) {
    _perf_counter.rpc_w.Add(common::timer::get_micros() - request->timestamp());
    _perf_counter.rpc_w_cnt.Inc();
    if (failed) {
        if (error_code == sofa::pbrpc::RPC_ERROR_SERVER_SHUTDOWN ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNREACHABLE ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNAVAILABLE) {
            response->set_status(kServerError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_CANCELED ||
                   error_code == sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL) {
            response->set_status(kClientError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_CONNECTION_CLOSED ||
                   error_code == sofa::pbrpc::RPC_ERROR_RESOLVE_ADDRESS) {
            response->set_status(kConnectError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_TIMEOUT) {
            response->set_status(kRPCTimeout);
        } else {
            response->set_status(kRPCError);
        }
    }

    std::map<uint32_t, std::vector<int64_t>* > retry_times_list;
    std::vector<RowMutationImpl*> not_in_range_list;
    for (uint32_t i = 0; i < mu_id_list->size(); ++i) {
        const std::string& row = request->row_list(i).row_key();
        int64_t mu_id = (*mu_id_list)[i];
        StatusCode err = response->status();
        if (err == kTabletNodeOk) {
            err = response->row_status_list(i);
        }

        if (err == kTabletNodeOk) {
            SdkTask* task = _task_pool.PopTask(mu_id);
            if (task == NULL) {
                VLOG(10) << "mutation " << mu_id << " success but timeout: " << DebugString(row);
                continue;
            }
            CHECK_EQ(task->Type(), SdkTask::MUTATION);
            CHECK_EQ(task->GetRef(), 1);
            RowMutationImpl* row_mutation = (RowMutationImpl*)task;
            row_mutation->SetError(ErrorCode::kOK);
            // only for flow control
            _cur_commit_pending_counter.Sub(row_mutation->MutationNum());
            int64_t perf_time = common::timer::get_micros();
            row_mutation->RunCallback();
            _perf_counter.user_callback.Add(common::timer::get_micros() - perf_time);
            _perf_counter.user_callback_cnt.Inc();
            continue;
        }

        VLOG(10) << "fail to mutate table: " << _name
            << " row: " << DebugString(row)
            << " errcode: " << StatusCodeToString(err);

        SdkTask* task = _task_pool.GetTask(mu_id);
        if (task == NULL) {
            VLOG(10) << "mutation " << mu_id << " timeout: " << DebugString(row);
            continue;
        }
        CHECK_EQ(task->Type(), SdkTask::MUTATION);
        RowMutationImpl* row_mutation = (RowMutationImpl*)task;
        row_mutation->SetInternalError(err);

        if (err == kKeyNotInRange) {
            row_mutation->IncRetryTimes();
            not_in_range_list.push_back(row_mutation);
        } else {
            row_mutation->IncRetryTimes();
            std::vector<int64_t>* retry_mu_id_list = NULL;
            std::map<uint32_t, std::vector<int64_t>* >::iterator it =
                retry_times_list.find(row_mutation->RetryTimes());
            if (it != retry_times_list.end()) {
                retry_mu_id_list = it->second;
            } else {
                retry_mu_id_list = new std::vector<int64_t>;
                retry_times_list[row_mutation->RetryTimes()] = retry_mu_id_list;
            }
            retry_mu_id_list->push_back(mu_id);
            row_mutation->DecRef();
        }
    }

    if (not_in_range_list.size() > 0) {
        DistributeMutations(not_in_range_list, false);
    }
    std::map<uint32_t, std::vector<int64_t>* >::iterator it;
    for (it = retry_times_list.begin(); it != retry_times_list.end(); ++it) {
        int64_t retry_interval =
            static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal, it->first) * 1000);
        ThreadPool::Task retry_task =
            boost::bind(&TableImpl::DistributeMutationsById, this, it->second);
        _thread_pool->DelayTask(retry_interval, retry_task);
    }

    delete request;
    delete response;
    delete mu_id_list;
}

void TableImpl::MutationTimeout(int64_t mutation_id) {
    SdkTask* task = _task_pool.PopTask(mutation_id);
    if (task == NULL) {
        return;
    }
    CHECK_NOTNULL(task);
    CHECK_EQ(task->Type(), SdkTask::MUTATION);

    RowMutationImpl* row_mutation = (RowMutationImpl*)task;
    row_mutation->ExcludeOtherRef();

    StatusCode err = row_mutation->GetInternalError();
    if (err == kKeyNotInRange || err == kConnectError) {
        ScheduleUpdateMeta(row_mutation->RowKey(),
                           row_mutation->GetMetaTimeStamp());
    }
    if (row_mutation->RetryTimes() == 0) {
        row_mutation->SetError(ErrorCode::kTimeout);
    } else {
        std::string err_reason = StringFormat("retry %u last error %s", row_mutation->RetryTimes(),
                                              StatusCodeToString(err).c_str());
        row_mutation->SetError(ErrorCode::kSystem, err_reason);
    }
    // only for flow control
    _cur_commit_pending_counter.Sub(row_mutation->MutationNum());
    int64_t perf_time = common::timer::get_micros();
    row_mutation->RunCallback();
    _perf_counter.user_callback.Add(common::timer::get_micros() - perf_time);
    _perf_counter.user_callback_cnt.Inc();
}

bool TableImpl::GetTabletLocation(std::vector<TabletInfo>* tablets,
                                  ErrorCode* err) {
    return false;
}

bool TableImpl::GetDescriptor(TableDescriptor* desc, ErrorCode* err) {
    return false;
}

void TableImpl::DistributeReaders(const std::vector<RowReaderImpl*>& row_reader_list,
                                  bool called_by_user) {
    typedef std::map<std::string, std::vector<RowReaderImpl*> > TsReaderMap;
    TsReaderMap ts_reader_list;

    int64_t sync_min_timeout = -1;
    std::vector<RowReaderImpl*> sync_reader_list;

    if (called_by_user) {
        for (uint32_t i = 0; i < row_reader_list.size(); i++) {
            RowReaderImpl* row_reader = (RowReaderImpl*)row_reader_list[i];
            if (row_reader->IsAsync()) {
                continue;
            }
            sync_reader_list.push_back(row_reader);
            int64_t row_timeout = row_reader->TimeOut() > 0 ? row_reader->TimeOut() : _timeout;
            if (row_timeout > 0 && (sync_min_timeout <= 0 || sync_min_timeout > row_timeout)) {
                sync_min_timeout = row_timeout;
            }
        }
    }

    for (uint32_t i = 0; i < row_reader_list.size(); i++) {
        RowReaderImpl* row_reader = (RowReaderImpl*)row_reader_list[i];
        if (called_by_user) {
            row_reader->SetId(_next_task_id.Inc());
            _task_pool.PutTask(row_reader);

            int64_t row_timeout = sync_min_timeout;
            if (row_reader->IsAsync()) {
                row_timeout = row_reader->TimeOut() > 0 ? row_reader->TimeOut() : _timeout;
            }
            if (row_timeout >= 0) {
                ThreadPool::Task task =
                    boost::bind(&TableImpl::ReaderTimeout, this, row_reader->GetId());
                _thread_pool->DelayTask(row_timeout, task);
            }
        }

        // flow control
        if (called_by_user
            && _cur_reader_pending_counter.Inc() > _max_reader_pending_num
            && row_reader->IsAsync()) {
            if (FLAGS_tera_sdk_async_blocking_enabled) {
                while (_cur_reader_pending_counter.Get() > _max_reader_pending_num) {
                    usleep(100000);
                }
            } else {
                _cur_reader_pending_counter.Dec();
                row_reader->SetError(ErrorCode::kBusy, "pending too much readers, try it later.");
                ThreadPool::Task task =
                    boost::bind(&TableImpl::BreakRequest, this, row_reader->GetId());
                row_reader->DecRef();
                _thread_pool->AddTask(task);
                continue;
            }
        }

        std::string server_addr;
        if (!GetTabletAddrOrScheduleUpdateMeta(row_reader->RowName(), row_reader,
                                               &server_addr)) {
            continue;
        }

        std::vector<RowReaderImpl*>& ts_row_readers = ts_reader_list[server_addr];
        ts_row_readers.push_back(row_reader);
    }

    TsReaderMap::iterator it = ts_reader_list.begin();
    for (; it != ts_reader_list.end(); ++it) {
        std::vector<RowReaderImpl*>& reader_list = it->second;
        PackReaders(it->first, reader_list);
    }
    // 从现在开始，所有异步的row_reader都不可以再操作了，因为随时会被用户释放

    // 不是用户调用的，立即返回
    if (!called_by_user) {
        return;
    }

    // 等待同步操作返回或超时
    for (uint32_t i = 0; i < sync_reader_list.size(); i++) {
        while (_cur_reader_pending_counter.Get() > _max_reader_pending_num) {
            usleep(100000);
        }

        RowReaderImpl* row_reader = (RowReaderImpl*)sync_reader_list[i];
        row_reader->Wait();
    }
}

void TableImpl::PackReaders(const std::string& server_addr,
                            std::vector<RowReaderImpl*>& reader_list) {
    MutexLock lock(&_reader_batch_mutex);
    TaskBatch* reader_buffer = NULL;
    std::map<std::string, TaskBatch>::iterator it =
        _reader_batch_map.find(server_addr);
    if (it == _reader_batch_map.end()) {
        reader_buffer = &_reader_batch_map[server_addr];
        reader_buffer->row_id_list = new std::vector<int64_t>;
        ThreadPool::Task task =
            boost::bind(&TableImpl::ReaderBatchTimeout, this, server_addr);
        uint64_t timer_id = _thread_pool->DelayTask(_read_commit_timeout, task);
        reader_buffer->timer_id = timer_id;
    } else {
        reader_buffer = &it->second;
    }

    for (size_t i = 0; i < reader_list.size(); ++i) {
        RowReaderImpl* reader = reader_list[i];
        reader_buffer->row_id_list->push_back(reader->GetId());
        reader->DecRef();
    }

    if (reader_buffer->row_id_list->size() >= _commit_size) {
        std::vector<int64_t>* reader_id_list = reader_buffer->row_id_list;
        uint64_t timer_id = reader_buffer->timer_id;
        _reader_batch_mutex.Unlock();
        if (_thread_pool->CancelTask(timer_id)) {
            _reader_batch_mutex.Lock();
            _reader_batch_map.erase(server_addr);
            _reader_batch_mutex.Unlock();
            CommitReadersById(server_addr, *reader_id_list);
            delete reader_id_list;
        }
        _reader_batch_mutex.Lock();
    }
}

void TableImpl::ReaderBatchTimeout(std::string server_addr) {
    std::vector<int64_t>* reader_id_list = NULL;
    {
        MutexLock lock(&_reader_batch_mutex);
        std::map<std::string, TaskBatch>::iterator it =
            _reader_batch_map.find(server_addr);
        if (it == _reader_batch_map.end()) {
            return;
        }
        TaskBatch* reader_buffer = &it->second;
        reader_id_list = reader_buffer->row_id_list;
        _reader_batch_map.erase(it);
    }
    CommitReadersById(server_addr, *reader_id_list);
    delete reader_id_list;
}

void TableImpl::CommitReadersById(const std::string server_addr,
                                  std::vector<int64_t>& reader_id_list) {
    std::vector<RowReaderImpl*> reader_list;
    for (size_t i = 0; i < reader_id_list.size(); ++i) {
        int64_t reader_id = reader_id_list[i];
        SdkTask* task = _task_pool.GetTask(reader_id);
        if (task == NULL) {
            VLOG(10) << "reader " << reader_id << " timeout when commit read";;
            continue;
        }
        CHECK_EQ(task->Type(), SdkTask::READ);
        RowReaderImpl* reader = (RowReaderImpl*)task;
        reader_list.push_back(reader);
    }
    CommitReaders(server_addr, reader_list);
}

void TableImpl::CommitReaders(const std::string server_addr,
                              std::vector<RowReaderImpl*>& reader_list) {
    std::vector<int64_t>* reader_id_list = new std::vector<int64_t>;
    tabletnode::TabletNodeClient tabletnode_client_async(server_addr);
    ReadTabletRequest* request = new ReadTabletRequest;
    ReadTabletResponse* response = new ReadTabletResponse;
    request->set_sequence_id(_last_sequence_id++);
    request->set_tablet_name(_name);
    request->set_client_timeout_ms(_pending_timeout_ms);
    for (uint32_t i = 0; i < reader_list.size(); ++i) {
        RowReaderImpl* row_reader = reader_list[i];
        RowReaderInfo* row_reader_info = request->add_row_info_list();
        request->set_snapshot_id(row_reader->GetSnapshot());
        row_reader->ToProtoBuf(row_reader_info);
        // row_reader_info->CopyFrom(row_reader->GetRowReaderInfo());
        reader_id_list->push_back(row_reader->GetId());
        row_reader->DecRef();
    }
    request->set_timestamp(common::timer::get_micros());
    Closure<void, ReadTabletRequest*, ReadTabletResponse*, bool, int>* done =
        NewClosure(this, &TableImpl::ReaderCallBack, reader_id_list);
    tabletnode_client_async.ReadTablet(request, response, done);
}

void TableImpl::ReaderCallBack(std::vector<int64_t>* reader_id_list,
                               ReadTabletRequest* request,
                               ReadTabletResponse* response,
                               bool failed, int error_code) {
    _perf_counter.rpc_r.Add(common::timer::get_micros() - request->timestamp());
    _perf_counter.rpc_r_cnt.Inc();
    if (failed) {
        if (error_code == sofa::pbrpc::RPC_ERROR_SERVER_SHUTDOWN ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNREACHABLE ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNAVAILABLE) {
            response->set_status(kServerError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_CANCELED ||
                   error_code == sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL) {
            response->set_status(kClientError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_CONNECTION_CLOSED ||
                   error_code == sofa::pbrpc::RPC_ERROR_RESOLVE_ADDRESS) {
            response->set_status(kConnectError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_TIMEOUT) {
            response->set_status(kRPCTimeout);
        } else {
            response->set_status(kRPCError);
        }
    }

    std::map<uint32_t, std::vector<int64_t>* > retry_times_list;
    std::vector<RowReaderImpl*> not_in_range_list;
    uint32_t row_result_num = 0;
    for (uint32_t i = 0; i < reader_id_list->size(); ++i) {
        int64_t reader_id = (*reader_id_list)[i];

        StatusCode err = response->status();
        if (err == kTabletNodeOk) {
            err = response->detail().status(i);
        }
        if (err == kTabletNodeOk || err == kKeyNotExist || err == kSnapshotNotExist) {
            SdkTask* task = _task_pool.PopTask(reader_id);
            if (task == NULL) {
                VLOG(10) << "reader " << reader_id << " success but timeout";
                continue;
            }
            CHECK_EQ(task->Type(), SdkTask::READ);
            CHECK_EQ(task->GetRef(), 1);

            RowReaderImpl* row_reader = (RowReaderImpl*)task;
            if (err == kTabletNodeOk) {
                row_reader->SetResult(response->detail().row_result(row_result_num++));
                row_reader->SetError(ErrorCode::kOK);
            } else if (err == kKeyNotExist) {
                row_reader->SetError(ErrorCode::kNotFound, "not found");
            } else { // err == kSnapshotNotExist
                row_reader->SetError(ErrorCode::kNotFound, "snapshot not found");
            }
            int64_t perf_time = common::timer::get_micros();
            row_reader->RunCallback();
            _perf_counter.user_callback.Add(common::timer::get_micros() - perf_time);
            _perf_counter.user_callback_cnt.Inc();
            // only for flow control
            _cur_reader_pending_counter.Dec();
            continue;
        }

        VLOG(10) << "fail to read table: " << _name
            << " errcode: " << StatusCodeToString(err);

        SdkTask* task = _task_pool.GetTask(reader_id);
        if (task == NULL) {
            VLOG(10) << "reader " << reader_id << " fail but timeout";
            continue;
        }
        CHECK_EQ(task->Type(), SdkTask::READ);
        RowReaderImpl* row_reader = (RowReaderImpl*)task;
        row_reader->SetInternalError(err);

        if (err == kKeyNotInRange) {
            row_reader->IncRetryTimes();
            not_in_range_list.push_back(row_reader);
        } else {
            row_reader->IncRetryTimes();
            std::vector<int64_t>* retry_reader_id_list = NULL;
            std::map<uint32_t, std::vector<int64_t>* >::iterator it =
                retry_times_list.find(row_reader->RetryTimes());
            if (it != retry_times_list.end()) {
                retry_reader_id_list = it->second;
            } else {
                retry_reader_id_list = new std::vector<int64_t>;
                retry_times_list[row_reader->RetryTimes()] = retry_reader_id_list;
            }
            retry_reader_id_list->push_back(row_reader->GetId());
            row_reader->DecRef();
        }
    }

    if (not_in_range_list.size() > 0) {
        DistributeReaders(not_in_range_list, false);
    }
    std::map<uint32_t, std::vector<int64_t>* >::iterator it;
    for (it = retry_times_list.begin(); it != retry_times_list.end(); ++it) {
        int64_t retry_interval =
            static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal, it->first) * 1000);
        ThreadPool::Task retry_task =
            boost::bind(&TableImpl::DistributeReadersById, this, it->second);
        _thread_pool->DelayTask(retry_interval, retry_task);
    }

    delete request;
    delete response;
    delete reader_id_list;
}

void TableImpl::DistributeReadersById(std::vector<int64_t>* reader_id_list) {
    std::vector<RowReaderImpl*> reader_list;
    for (size_t i = 0; i < reader_id_list->size(); ++i) {
        int64_t reader_id = (*reader_id_list)[i];
        SdkTask* task = _task_pool.GetTask(reader_id);
        if (task == NULL) {
            VLOG(10) << "reader " << reader_id << " timeout when retry read";
            continue;
        }
        CHECK_EQ(task->Type(), SdkTask::READ);
        reader_list.push_back((RowReaderImpl*)task);
    }
    DistributeReaders(reader_list, false);
    delete reader_id_list;
}

void TableImpl::ReaderTimeout(int64_t reader_id) {
    SdkTask* task = _task_pool.PopTask(reader_id);
    if (task == NULL) {
        return;
    }
    CHECK_NOTNULL(task);
    CHECK_EQ(task->Type(), SdkTask::READ);

    RowReaderImpl* row_reader = (RowReaderImpl*)task;
    row_reader->ExcludeOtherRef();

    StatusCode err = row_reader->GetInternalError();
    if (err == kKeyNotInRange || err == kConnectError) {
        ScheduleUpdateMeta(row_reader->RowName(),
                           row_reader->GetMetaTimeStamp());
    }
    if (row_reader->RetryTimes() == 0) {
        row_reader->SetError(ErrorCode::kTimeout);
    } else {
        std::string err_reason = StringFormat("retry %u last error %s", row_reader->RetryTimes(),
                                              StatusCodeToString(err).c_str());
        row_reader->SetError(ErrorCode::kSystem, err_reason);
    }
    int64_t perf_time = common::timer::get_micros();
    row_reader->RunCallback();
    _perf_counter.user_callback.Add(common::timer::get_micros() - perf_time);
    _perf_counter.user_callback_cnt.Inc();
    // only for flow control
    _cur_reader_pending_counter.Dec();
}

bool TableImpl::GetTabletMetaForKey(const std::string& key, TabletMeta* meta) {
    MutexLock lock(&_meta_mutex);
    TabletMetaNode* node = GetTabletMetaNodeForKey(key);
    if (node == NULL) {
        VLOG(10) << "no meta for key: " << key;
        return false;
    }
    meta->CopyFrom(node->meta);
    return true;
}

void TableImpl::BreakScan(ScanTask* scan_task) {
    ResultStreamImpl* stream = scan_task->stream;
    stream->OnFinish(scan_task->request,
            scan_task->response);
    stream->ReleaseRpcHandle(scan_task->request,
            scan_task->response);
    delete scan_task;
}

bool TableImpl::GetTabletAddrOrScheduleUpdateMeta(const std::string& row,
                                                  SdkTask* task,
                                                  std::string* server_addr) {
    CHECK_NOTNULL(task);
    MutexLock lock(&_meta_mutex);
    TabletMetaNode* node = GetTabletMetaNodeForKey(row);
    if (node == NULL) {
        VLOG(10) << "no meta for key: " << row;
        _pending_task_id_list[row].push_back(task->GetId());
        task->DecRef();
        TabletMetaNode& new_node = _tablet_meta_list[row];
        new_node.meta.mutable_key_range()->set_key_start(row);
        new_node.meta.mutable_key_range()->set_key_end(row + '\0');
        new_node.status = WAIT_UPDATE;
        UpdateMetaAsync();
        return false;
    }
    if (node->status != NORMAL) {
        VLOG(10) << "abnormal meta for key: " << row;
        _pending_task_id_list[row].push_back(task->GetId());
        task->DecRef();
        return false;
    }
    if ((task->GetInternalError() == kKeyNotInRange || task->GetInternalError() == kConnectError)
            && task->GetMetaTimeStamp() >= node->update_time) {
        _pending_task_id_list[row].push_back(task->GetId());
        task->DecRef();
        int64_t update_interval = node->update_time
            + FLAGS_tera_sdk_update_meta_internal - get_micros() / 1000;
        if (update_interval <= 0) {
            VLOG(10) << "update meta now for key: " << row;
            node->status = WAIT_UPDATE;
            UpdateMetaAsync();
        } else {
            VLOG(10) << "update meta in " << update_interval << " (ms) for key:" << row;
            node->status = DELAY_UPDATE;
            ThreadPool::Task delay_task =
                boost::bind(&TableImpl::DelayUpdateMeta, this,
                            node->meta.key_range().key_start(),
                            node->meta.key_range().key_end());
            _thread_pool->DelayTask(update_interval, delay_task);
        }
        return false;
    }
    CHECK_EQ(node->status, NORMAL);
    task->SetMetaTimeStamp(node->update_time);
    *server_addr = node->meta.server_addr();
    return true;
}

TableImpl::TabletMetaNode* TableImpl::GetTabletMetaNodeForKey(const std::string& key) {
    _meta_mutex.AssertHeld();
    if (_tablet_meta_list.size() == 0) {
        VLOG(10) << "the meta list is empty";
        return NULL;
    }
    std::map<std::string, TabletMetaNode>::iterator it =
        _tablet_meta_list.upper_bound(key);
    if (it == _tablet_meta_list.begin()) {
        return NULL;
    } else {
        --it;
    }
    const std::string& end_key = it->second.meta.key_range().key_end();
    if (end_key != "" && end_key <= key) {
        return NULL;
    }
    return &(it->second);
}

void TableImpl::DelayUpdateMeta(std::string start_key, std::string end_key) {
    MutexLock lock(&_meta_mutex);
    std::map<std::string, TabletMetaNode>::iterator it =
            _tablet_meta_list.lower_bound(start_key);
    for (; it != _tablet_meta_list.end(); ++it) {
        TabletMetaNode& node = it->second;
        if (node.meta.key_range().key_end() > end_key) {
            break;
        }
        if (node.status != DELAY_UPDATE) {
            continue;
        }
        node.status = WAIT_UPDATE;
    }
    UpdateMetaAsync();
}

void TableImpl::UpdateMetaAsync() {
    _meta_mutex.AssertHeld();
    if (_meta_updating_count >= static_cast<uint32_t>(FLAGS_tera_sdk_update_meta_concurrency)) {
        return;
    }
    bool need_update = false;
    std::string update_start_key;
    std::string update_end_key;
    std::string update_expand_end_key; // update more tablet than need
    std::map<std::string, TabletMetaNode>::iterator it = _tablet_meta_list.begin();
    for (; it != _tablet_meta_list.end(); ++it) {
        TabletMetaNode& node = it->second;
        if (node.status != WAIT_UPDATE && need_update) {
            update_expand_end_key = node.meta.key_range().key_start();
            break;
        } else if (node.status != WAIT_UPDATE) {
            continue;
        } else if (!need_update) {
            need_update = true;
            update_start_key = node.meta.key_range().key_start();
            update_end_key = node.meta.key_range().key_end();
        } else if (node.meta.key_range().key_start() == update_end_key) {
            update_end_key = node.meta.key_range().key_end();
        } else {
            CHECK_GT(node.meta.key_range().key_start(), update_end_key);
            update_expand_end_key = node.meta.key_range().key_start();
            break;
        }
        node.status = UPDATING;
    }
    if (!need_update) {
        return;
    }
    _meta_updating_count++;
    ScanMetaTableAsync(update_start_key, update_end_key, update_expand_end_key, false);
}

void TableImpl::ScanMetaTable(const std::string& key_start,
                              const std::string& key_end) {
    MutexLock lock(&_meta_mutex);
    _meta_updating_count++;
    ScanMetaTableAsync(key_start, key_end, key_end, false);
    while (_meta_updating_count > 0) {
        _meta_cond.Wait();
    }
}

void TableImpl::ScanMetaTableAsyncInLock(std::string key_start, std::string key_end,
                                         std::string expand_key_end, bool zk_access) {
    MutexLock lock(&_meta_mutex);
    ScanMetaTableAsync(key_start, key_end, expand_key_end, zk_access);
}

void TableImpl::ScanMetaTableAsync(const std::string& key_start, const std::string& key_end,
                                   const std::string& expand_key_end, bool zk_access) {
    _meta_mutex.AssertHeld();
    CHECK(expand_key_end == "" || expand_key_end >= key_end);

    std::string meta_addr = _cluster->RootTableAddr(zk_access);
    if (meta_addr.empty() && !zk_access) {
        meta_addr = _cluster->RootTableAddr(true);
    }

    if (meta_addr.empty()) {
        VLOG(6) << "root is empty";

        ThreadPool::Task retry_task =
            boost::bind(&TableImpl::ScanMetaTableAsyncInLock, this, key_start, key_end,
                        expand_key_end, true);
        _thread_pool->DelayTask(FLAGS_tera_sdk_update_meta_internal, retry_task);
        return;
    }

    VLOG(6) << "root: " << meta_addr;
    tabletnode::TabletNodeClient tabletnode_client_async(meta_addr);
    ScanTabletRequest* request = new ScanTabletRequest;
    ScanTabletResponse* response = new ScanTabletResponse;
    request->set_sequence_id(_last_sequence_id++);
    request->set_table_name(FLAGS_tera_master_meta_table_name);
    MetaTableScanRange(_name, key_start, expand_key_end,
                       request->mutable_start(),
                       request->mutable_end());
    request->set_buffer_limit(FLAGS_tera_sdk_update_meta_buffer_limit);
    request->set_round_down(true);

    Closure<void, ScanTabletRequest*, ScanTabletResponse*, bool, int>* done =
        NewClosure(this, &TableImpl::ScanMetaTableCallBack, key_start, key_end, expand_key_end);
    tabletnode_client_async.ScanTablet(request, response, done);
}

void TableImpl::ScanMetaTableCallBack(std::string key_start,
                                      std::string key_end,
                                      std::string expand_key_end,
                                      ScanTabletRequest* request,
                                      ScanTabletResponse* response,
                                      bool failed, int error_code) {
    if (failed) {
        if (error_code == sofa::pbrpc::RPC_ERROR_SERVER_SHUTDOWN ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNREACHABLE ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNAVAILABLE) {
            response->set_status(kServerError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_CANCELED ||
                   error_code == sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL) {
            response->set_status(kClientError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_CONNECTION_CLOSED ||
                   error_code == sofa::pbrpc::RPC_ERROR_RESOLVE_ADDRESS) {
            response->set_status(kConnectError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_TIMEOUT) {
            response->set_status(kRPCTimeout);
        } else {
            response->set_status(kRPCError);
        }
    }

    StatusCode err = response->status();
    if (err != kTabletNodeOk) {
        VLOG(10) << "fail to scan meta table [" << request->start()
            << ", " << request->end() << "]: " << StatusCodeToString(err);
        {
            MutexLock lock(&_meta_mutex);
            GiveupUpdateTabletMeta(key_start, key_end);
        }
        ThreadPool::Task retry_task =
            boost::bind(&TableImpl::ScanMetaTableAsyncInLock, this, key_start, key_end,
                        expand_key_end, true);
        _thread_pool->DelayTask(FLAGS_tera_sdk_update_meta_internal, retry_task);
        delete request;
        delete response;
        return;
    }

    std::string return_start, return_end;
    const RowResult& scan_result = response->results();
    for (int32_t i = 0; i < scan_result.key_values_size(); i++) {
        const KeyValuePair& kv = scan_result.key_values(i);

        TabletMeta meta;
        ParseMetaTableKeyValue(kv.key(), kv.value(), &meta);

        if (i == 0) {
            return_start = meta.key_range().key_start();
        }
        if (i == scan_result.key_values_size() - 1) {
            return_end = meta.key_range().key_end();
        }

        MutexLock lock(&_meta_mutex);
        UpdateTabletMetaList(meta);
    }
    VLOG(10) << "scan meta table [" << request->start()
        << ", " << request->end() << "] success: return "
        << scan_result.key_values_size() << " records, is_complete: " << response->complete();
    bool scan_meta_error = false;
    if (scan_result.key_values_size() == 0
        || return_start > key_start
        || (response->complete() && !return_end.empty() && (key_end.empty() || return_end < key_end))) {
        LOG(ERROR) << "scan meta table [" << key_start << ", " << key_end
            << "] return [" << return_start << ", " << return_end << "]";
        // TODO(lk): process omitted tablets
        scan_meta_error = true;
    }

    MutexLock lock(&_meta_mutex);
    if (scan_meta_error) {
        ScanMetaTableAsync(key_start, key_end, expand_key_end, false);
    } else if (!return_end.empty() && (key_end.empty() || return_end < key_end)) {
        CHECK(!response->complete());
        ScanMetaTableAsync(return_end, key_end, expand_key_end, false);
    } else {
        _meta_updating_count--;
        _meta_cond.Signal();
        UpdateMetaAsync();
    }
    delete request;
    delete response;
}

void TableImpl::GiveupUpdateTabletMeta(const std::string& key_start,
                                       const std::string& key_end) {
    std::map<std::string, std::list<int64_t> >::iterator ilist =
            _pending_task_id_list.lower_bound(key_start);
    while (ilist != _pending_task_id_list.end()) {
        if (!key_end.empty() && ilist->first >= key_end) {
            break;
        }
        std::list<int64_t>& task_id_list = ilist->second;
        for (std::list<int64_t>::iterator itask = task_id_list.begin();
                itask != task_id_list.end();) {
            int64_t task_id = *itask;
            SdkTask* task = _task_pool.GetTask(task_id);
            if (task == NULL) {
                VLOG(10) << "task " << task_id << " timeout when update meta fail";
                itask = task_id_list.erase(itask);
            } else {
                task->DecRef();
            }
            ++itask;
        }
        if (task_id_list.empty()) {
            _pending_task_id_list.erase(ilist++);
        } else {
            ++ilist;
        }
    }
}

void TableImpl::UpdateTabletMetaList(const TabletMeta& new_meta) {
    _meta_mutex.AssertHeld();
    const std::string& new_start = new_meta.key_range().key_start();
    const std::string& new_end = new_meta.key_range().key_end();
    std::map<std::string, TabletMetaNode>::iterator it =
        _tablet_meta_list.upper_bound(new_start);
    if (_tablet_meta_list.size() > 0 && it != _tablet_meta_list.begin()) {
        --it;
    }
    while (it != _tablet_meta_list.end()) {
        TabletMetaNode& old_node = it->second;
        std::map<std::string, TabletMetaNode>::iterator tmp = it;
        ++it;

        const std::string& old_start = old_node.meta.key_range().key_start();
        const std::string& old_end = old_node.meta.key_range().key_end();
        // update overlaped old nodes
        if (old_start < new_start) {
            if (!old_end.empty() && old_end <= new_start) {
                //*************************************************
                //* |---old---|                                   *
                //*             |------new------|                 *
                //*************************************************
            } else if (new_end.empty() || (!old_end.empty() && old_end <= new_end)) {
                //*************************************************
                //*         |---old---|                           *
                //*             |------new------|                 *
                //*************************************************
                VLOG(10) << "meta [" << old_start << ", " << old_end << "] "
                    << "shrink to [" << old_start << ", " << new_start << "]";
                old_node.meta.mutable_key_range()->set_key_end(new_start);
            } else {
                //*************************************************
                //*         |----------old-----------|            *
                //*             |------new------|                 *
                //*************************************************
                VLOG(10) << "meta [" << old_start << ", " << old_end << "] "
                    << "split to [" << old_start << ", " << new_start << "] "
                    << "and [" << new_end << ", " << old_end << "]";
                TabletMetaNode& copy_node = _tablet_meta_list[new_end];
                copy_node = old_node;
                copy_node.meta.mutable_key_range()->set_key_start(new_end);
                old_node.meta.mutable_key_range()->set_key_end(new_start);
            }
        } else if (new_end.empty() || old_start < new_end) {
            if (new_end.empty() || (!old_end.empty() && old_end <= new_end)) {
                //*************************************************
                //*                |---old---|                    *
                //*             |------new------|                 *
                //*************************************************
                VLOG(10) << "meta [" << old_start << ", " << old_end << "] "
                    << "is covered by [" << new_start << ", " << new_end << "]";
                _tablet_meta_list.erase(tmp);
            } else {
                //*************************************************
                //*                  |-----old------|             *
                //*             |------new------|                 *
                //*************************************************
                VLOG(10) << "meta [" << old_start << ", " << old_end << "] "
                    << "shrink to [" << new_end << ", " << old_end << "]";
                TabletMetaNode& copy_node = _tablet_meta_list[new_end];
                copy_node = old_node;
                copy_node.meta.mutable_key_range()->set_key_start(new_end);
                _tablet_meta_list.erase(tmp);
            }
        } else { // !new_end.empty() && old_start >= new_end
            //*****************************************************
            //*                                   |---old---|     *
            //*                 |------new------|                 *
            //*****************************************************
            break;
        }
    }

    TabletMetaNode& new_node = _tablet_meta_list[new_start];
    new_node.meta.CopyFrom(new_meta);
    new_node.status = NORMAL;
    new_node.update_time = get_micros() / 1000;
    VLOG(10) << "add new meta [" << new_start << ", " << new_end << "]: "
        << new_meta.server_addr();
    WakeUpPendingRequest(new_node);
}

void TableImpl::WakeUpPendingRequest(const TabletMetaNode& node) {
    _meta_mutex.AssertHeld();
    const std::string& start_key = node.meta.key_range().key_start();
    const std::string& end_key = node.meta.key_range().key_end();
    const std::string& server_addr = node.meta.server_addr();
    int64_t meta_timestamp = node.update_time;

    std::vector<RowMutationImpl*> mutation_list;
    std::vector<RowReaderImpl*> reader_list;

    std::map<std::string, std::list<int64_t> >::iterator it =
        _pending_task_id_list.lower_bound(start_key);
    while (it != _pending_task_id_list.end()) {
        if (!end_key.empty() && it->first >= end_key) {
            break;
        }
        std::list<int64_t>& task_id_list = it->second;
        for (std::list<int64_t>::iterator itask = task_id_list.begin();
                itask != task_id_list.end(); ++itask) {
            int64_t task_id = *itask;
            SdkTask* task = _task_pool.GetTask(task_id);
            if (task == NULL) {
                VLOG(10) << "task " << task_id << " timeout when update meta success";
                continue;
            }
            task->SetMetaTimeStamp(meta_timestamp);

            switch (task->Type()) {
            case SdkTask::READ: {
                RowReaderImpl* reader = (RowReaderImpl*)task;
                reader_list.push_back(reader);
            } break;
            case SdkTask::MUTATION: {
                RowMutationImpl* mutation = (RowMutationImpl*)task;
                mutation_list.push_back(mutation);
            } break;
            case SdkTask::SCAN: {
                ScanTask* scan_task = (ScanTask*)task;
                CommitScan(scan_task, server_addr);
            } break;
            default:
                CHECK(false);
                break;
            }
        }
        std::map<std::string, std::list<int64_t> >::iterator tmp = it;
        ++it;
        _pending_task_id_list.erase(tmp);
    }

    if (mutation_list.size() > 0) {
        // TODO: flush ?
        PackMutations(server_addr, mutation_list, false);
    }
    if (reader_list.size() > 0) {
        PackReaders(server_addr, reader_list);
    }
}

void TableImpl::ScheduleUpdateMeta(const std::string& row,
                                   int64_t meta_timestamp) {
    MutexLock lock(&_meta_mutex);
    TabletMetaNode* node = GetTabletMetaNodeForKey(row);
    if (node == NULL) {
        TabletMetaNode& new_node = _tablet_meta_list[row];
        new_node.meta.mutable_key_range()->set_key_start(row);
        new_node.meta.mutable_key_range()->set_key_end(row + '\0');
        new_node.status = WAIT_UPDATE;
        UpdateMetaAsync();
        return;
    }
    if (node->status == NORMAL && meta_timestamp >= node->update_time) {
        int64_t update_interval = node->update_time
            + FLAGS_tera_sdk_update_meta_internal - get_micros() / 1000;
        if (update_interval <= 0) {
            node->status = WAIT_UPDATE;
            UpdateMetaAsync();
        } else {
            node->status = DELAY_UPDATE;
            ThreadPool::Task delay_task =
                boost::bind(&TableImpl::DelayUpdateMeta, this,
                            node->meta.key_range().key_start(),
                            node->meta.key_range().key_end());
            _thread_pool->DelayTask(update_interval, delay_task);
        }
    }
}

bool TableImpl::UpdateTableMeta(ErrorCode* err) {
    MutexLock lock(&_table_meta_mutex);
    _table_meta_updating = true;

    _table_meta_mutex.Unlock();
    ReadTableMetaAsync(err, 0, false);
    _table_meta_mutex.Lock();

    while (_table_meta_updating) {
        _table_meta_cond.Wait();
    }
    if (err->GetType() != ErrorCode::kOK) {
        return false;
    }
    return true;
}

void TableImpl::ReadTableMetaAsync(ErrorCode* ret_err, int32_t retry_times,
                                   bool zk_access) {
    std::string meta_server = _cluster->RootTableAddr(zk_access);
    if (meta_server.empty() && !zk_access) {
        meta_server = _cluster->RootTableAddr(true);
    }
    if (meta_server.empty()) {
        VLOG(10) << "root is empty";

        MutexLock lock(&_table_meta_mutex);
        CHECK(_table_meta_updating);
        if (retry_times >= FLAGS_tera_sdk_retry_times) {
            ret_err->SetFailed(ErrorCode::kSystem);
            _table_meta_updating = false;
            _table_meta_cond.Signal();
        } else {
            int64_t retry_interval =
                static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal, retry_times) * 1000);
            ThreadPool::Task retry_task =
                boost::bind(&TableImpl::ReadTableMetaAsync, this, ret_err, retry_times + 1, true);
            _thread_pool->DelayTask(retry_interval, retry_task);
        }
        return;
    }

    tabletnode::TabletNodeClient tabletnode_client_async(meta_server);
    ReadTabletRequest* request = new ReadTabletRequest;
    ReadTabletResponse* response = new ReadTabletResponse;
    request->set_sequence_id(_last_sequence_id++);
    request->set_tablet_name(FLAGS_tera_master_meta_table_name);
    RowReaderInfo* row_info = request->add_row_info_list();
    MakeMetaTableKey(_name, row_info->mutable_key());

    Closure<void, ReadTabletRequest*, ReadTabletResponse*, bool, int>* done =
        NewClosure(this, &TableImpl::ReadTableMetaCallBack, ret_err, retry_times);
    tabletnode_client_async.ReadTablet(request, response, done);
}

void TableImpl::ReadTableMetaCallBack(ErrorCode* ret_err,
                                      int32_t retry_times,
                                      ReadTabletRequest* request,
                                      ReadTabletResponse* response,
                                      bool failed, int error_code) {
    if (failed) {
        if (error_code == sofa::pbrpc::RPC_ERROR_SERVER_SHUTDOWN ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNREACHABLE ||
            error_code == sofa::pbrpc::RPC_ERROR_SERVER_UNAVAILABLE) {
            response->set_status(kServerError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_CANCELED ||
                   error_code == sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL) {
            response->set_status(kClientError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_CONNECTION_CLOSED ||
                   error_code == sofa::pbrpc::RPC_ERROR_RESOLVE_ADDRESS) {
            response->set_status(kConnectError);
        } else if (error_code == sofa::pbrpc::RPC_ERROR_REQUEST_TIMEOUT) {
            response->set_status(kRPCTimeout);
        } else {
            response->set_status(kRPCError);
        }
    }

    StatusCode err = response->status();
    if (err == kTabletNodeOk && response->detail().status_size() < 1) {
        err = kKeyNotExist;
        LOG(ERROR) << "read table meta: status size is 0";
    }
    if (err == kTabletNodeOk) {
        err = response->detail().status(0);
    }
    if (err == kTabletNodeOk && response->detail().row_result_size() < 1) {
        err = kKeyNotExist;
        LOG(ERROR) << "read table meta: row result size is 0";
    }
    if (err == kTabletNodeOk && response->detail().row_result(0).key_values_size() < 1) {
        err = kKeyNotExist;
        LOG(ERROR) << "read table meta: row result kv size is 0";
    }

    if (err != kTabletNodeOk && err != kKeyNotExist && err != kSnapshotNotExist) {
        VLOG(10) << "fail to read meta table, retry: " << retry_times
            << ", errcode: " << StatusCodeToString(err);
    }

    MutexLock lock(&_table_meta_mutex);
    CHECK(_table_meta_updating);

    if (err == kTabletNodeOk) {
        TableMeta table_meta;
        const KeyValuePair& kv = response->detail().row_result(0).key_values(0);
        ParseMetaTableKeyValue(kv.key(), kv.value(), &table_meta);
        _table_schema.CopyFrom(table_meta.schema());
        _create_time = table_meta.create_time();
        ret_err->SetFailed(ErrorCode::kOK);
        _table_meta_updating = false;
        _table_meta_cond.Signal();
    } else if (err == kKeyNotExist || err == kSnapshotNotExist) {
        ret_err->SetFailed(ErrorCode::kNotFound);
        _table_meta_updating = false;
        _table_meta_cond.Signal();
    } else if (retry_times >= FLAGS_tera_sdk_retry_times) {
        ret_err->SetFailed(ErrorCode::kSystem);
        _table_meta_updating = false;
        _table_meta_cond.Signal();
    } else {
        int64_t retry_interval =
            static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal, retry_times) * 1000);
        ThreadPool::Task retry_task =
            boost::bind(&TableImpl::ReadTableMetaAsync, this, ret_err, retry_times + 1, true);
        _thread_pool->DelayTask(retry_interval, retry_task);
    }

    delete request;
    delete response;
}

static bool CalculateChecksumOfData(std::fstream& outfile, long size, std::string* hash_str) {
    // 100 MB, (100 * 1024 * 1024) / 250 = 419,430
    // cookie文件中，每个tablet的缓存大小约为100~200 Bytes，不妨计为250 Bytes，
    // 那么，100MB可以支持约40万个tablets
    const long MAX_SIZE = 100 * 1024 * 1024;

    if(size > MAX_SIZE || size <= 0) {
        LOG(INFO) << "[SDK COOKIE] invalid size : " << size;
        return false;
    }
    if(hash_str == NULL) {
        LOG(INFO) << "[SDK COOKIE] input argument `hash_str' is NULL";
        return false;
    }
    std::string data(size, '\0');
    outfile.read(const_cast<char*>(data.data()), size);
    if(outfile.fail()) {
        LOG(INFO) << "[SDK COOKIE] fail to read cookie file";
        return false;
    }
    if (GetHashString(data, 0, hash_str) != 0) {
        return false;
    }
    return true;
}

static bool IsCookieChecksumRight(const std::string& cookie_file) {
    std::fstream outfile(cookie_file.c_str(), std::ios_base::in | std::ios_base::out);
    int errno_saved = errno;
    if(outfile.fail()) {
        LOG(INFO) << "[SDK COOKIE] fail to open " << cookie_file.c_str()
            << " " << strerror(errno_saved);
        return false;
    }

    // gets file size, in bytes
    outfile.seekp(0, std::ios_base::end);
    long file_size = outfile.tellp();
    if(file_size < HASH_STRING_LEN) {
        LOG(INFO) << "[SDK COOKIE] invalid file size: " << file_size;
        return false;
    }

    // calculates checksum according to cookie file content
    std::string hash_str;
    outfile.seekp(0, std::ios_base::beg);
    if(!CalculateChecksumOfData(outfile, file_size - HASH_STRING_LEN, &hash_str)) {
        return false;
    }
    LOG(INFO) << "[SDK COOKIE] checksum rebuild: " << hash_str;

    // gets checksum in cookie file
    char hash_str_saved[HASH_STRING_LEN + 1] = {'\0'};
    outfile.read(hash_str_saved, HASH_STRING_LEN);
    if(outfile.fail()) {
        int errno_saved = errno;
        LOG(INFO) << "[SDK COOKIE] fail to get checksum: " << strerror(errno_saved);
        return false;
    }
    LOG(INFO) << "[SDK COOKIE] checksum in file: " << hash_str_saved;

    outfile.close();
    return strncmp(hash_str.c_str(), hash_str_saved, HASH_STRING_LEN) == 0;
}

bool TableImpl::RestoreCookie() {
    const std::string& cookie_dir = FLAGS_tera_sdk_cookie_path;
    if (!IsExist(cookie_dir)) {
        if (!CreateDirWithRetry(cookie_dir)) {
            LOG(INFO) << "[SDK COOKIE] fail to create cookie dir: " << cookie_dir;
            return false;
        } else {
            return true;
        }
    }
    std::string cookie_file = GetCookieFilePathName();
    if (!IsExist(cookie_file)) {
        // cookie file is not exist
        return true;
    }
    if(!IsCookieChecksumRight(cookie_file)) {
        if(unlink(cookie_file.c_str()) == -1) {
            int errno_saved = errno;
            LOG(INFO) << "[SDK COOKIE] fail to delete broken cookie file: " << cookie_file
                << ". reason: " << strerror(errno_saved);
        } else {
            LOG(INFO) << "[SDK COOKIE] delete broken cookie file" << cookie_file;
        }
        return true;
    }

    FileStream fs;
    if (!fs.Open(cookie_file, FILE_READ)) {
        LOG(INFO) << "[SDK COOKIE] fail to open " << cookie_file;
        return true;
    }
    SdkCookie cookie;
    RecordReader record_reader;
    record_reader.Reset(&fs);
    if (!record_reader.ReadNextMessage(&cookie)) {
        LOG(INFO) << "[SDK COOKIE] fail to parse sdk cookie, file: " << cookie_file;
        return true;
    }
    fs.Close();

    if (cookie.table_name() != _name) {
        LOG(INFO) << "[SDK COOKIE] cookie name error: " << cookie.table_name()
            << ", should be: " << _name;
        return true;
    }

    MutexLock lock(&_meta_mutex);
    for (int i = 0; i < cookie.tablets_size(); ++i) {
        const TabletMeta& meta = cookie.tablets(i).meta();
        const std::string& start_key = meta.key_range().key_start();
        LOG(INFO) << "[SDK COOKIE] restore tablet, range [" << DebugString(start_key)
            << " : " << DebugString(meta.key_range().key_end()) << "]";
        TabletMetaNode& node = _tablet_meta_list[start_key];
        node.meta = meta;
        node.update_time = cookie.tablets(i).update_time();
        node.status = NORMAL;
    }
    LOG(INFO) << "[SDK COOKIE] restore finished, tablet num: " << cookie.tablets_size();
    return true;
}

std::string TableImpl::GetCookieFilePathName(void) {
    return FLAGS_tera_sdk_cookie_path + "/"
        + GetCookieFileName(_name, _zk_addr_list, _zk_root_path, _create_time);
}

std::string TableImpl::GetCookieLockFilePathName(void) {
    return GetCookieFilePathName() + ".LOCK";
}

/*
 * If there is overtime/legacy cookie-lock-file, then delete it.
 * Normally, process which created cookie-lock-file would delete it after dumped.
 * But if this process crashed before delete cookie-lock-file.
 * Another process could call this function to delete legacy cookie-lock-file.
 *
 * create_time = [time of cookie-lock-file creation]
 * timeout     = [input parameter `timeout_secondes']
 *
 * create_time + timeout > current_time :=> legacy cookie-lock-file
 */
void TableImpl::DeleteLegacyCookieLockFile(const std::string& lock_file, int timeout_seconds) {
    struct stat lock_stat;
    int ret = stat(lock_file.c_str(), &lock_stat);
    if (ret == -1) {
        return;
    }
    time_t curr_time = time(NULL);
    if (((unsigned int)curr_time - lock_stat.st_atime) > timeout_seconds) {
        // It's a long time since creation of cookie-lock-file, dumping must has done.
        // So, delete the cookie-lock-file is safe.
        int errno_saved = -1;
        if (unlink(lock_file.c_str()) == -1) {
            errno_saved = errno;
            LOG(INFO) << "[SDK COOKIE] fail to delete cookie-lock-file: " << lock_file
                       << ". reason: " << strerror(errno_saved);
        }
    }
}

void TableImpl::CloseAndRemoveCookieLockFile(int lock_fd, const std::string& cookie_lock_file) {
    if (lock_fd < 0) {
        return;
    }
    close(lock_fd);
    if (unlink(cookie_lock_file.c_str()) == -1) {
        int errno_saved = errno;
        LOG(INFO) << "[SDK COOKIE] fail to delete cookie-lock-file: " << cookie_lock_file
                   << ". reason: " << strerror(errno_saved);
    }
}

static bool AppendChecksumToCookie(const std::string& cookie_file) {
    std::fstream outfile(cookie_file.c_str(), std::ios_base::in | std::ios_base::out);
    int errno_saved = errno;
    if(outfile.fail()) {
        LOG(INFO) << "[SDK COOKIE] fail to open " << cookie_file.c_str()
            << " " << strerror(errno_saved);
        return false;
    }

    // get file size, in bytes
    outfile.seekp(0, std::ios_base::end);
    long file_size = outfile.tellp();
    if(file_size < HASH_STRING_LEN) {
        LOG(INFO) << "[SDK COOKIE] invalid file size: " << file_size;
        return false;
    }

    // calculates checksum according to cookie file content
    outfile.seekp(0, std::ios_base::beg);
    std::string hash_str;
    if(!CalculateChecksumOfData(outfile, file_size, &hash_str)) {
        return false;
    }
    LOG(INFO) << "[SDK COOKIE] file checksum: " << hash_str;

    // append checksum to the end of cookie file
    outfile.seekp(0, std::ios_base::end);
    outfile.write(hash_str.c_str(), hash_str.length());
    if(outfile.fail()) {
        LOG(INFO) << "[SDK COOKIE] fail to append checksum";
        return false;
    }
    outfile.close();
    return true;
}

static bool AddOtherUserWritePermission(const std::string& cookie_file) {
    struct stat st;
    int ret = stat(cookie_file.c_str(), &st);
    if(ret != 0) {
        return false;
    }
    if((st.st_mode & S_IWOTH) == S_IWOTH) {
        // other user has write permission already
        return true;
    }
    return chmod(cookie_file.c_str(),
             S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH) == 0;
}

void TableImpl::DoDumpCookie() {
    std::string cookie_file = GetCookieFilePathName();
    std::string cookie_lock_file = GetCookieLockFilePathName();

    int cookie_lock_file_timeout = 10; // in seconds
    DeleteLegacyCookieLockFile(cookie_lock_file, cookie_lock_file_timeout);
    int lock_fd = open(cookie_lock_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (lock_fd == -1) {
        int errno_saved = errno;
        LOG(INFO) << "[SDK COOKIE] faild to create cookie-lock-file" << cookie_lock_file
                   << ". reason: " << strerror(errno_saved)
                   << ". If reason is \"File exists\", means lock is held by another process"
                   << "otherwise, IO error";
        return;
    }

    SdkCookie cookie;
    cookie.set_table_name(_name);
    {
        MutexLock lock(&_meta_mutex);
        std::map<std::string, TabletMetaNode>::iterator it = _tablet_meta_list.begin();
        for (; it != _tablet_meta_list.end(); ++it) {
            const TabletMetaNode& node = it->second;
            if (!node.meta.has_table_name() || !node.meta.has_path()) {
                continue;
            }
            SdkTabletCookie* tablet = cookie.add_tablets();
            tablet->mutable_meta()->CopyFrom(node.meta);
            tablet->set_update_time(node.update_time);
            tablet->set_status(node.status);
        }
    }

    FileStream fs;
    if (!fs.Open(cookie_file, FILE_WRITE)) {
        LOG(INFO) << "[SDK COOKIE] fail to open " << cookie_file;
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }
    RecordWriter record_writer;
    record_writer.Reset(&fs);
    if (!record_writer.WriteMessage(cookie)) {
        LOG(INFO) << "[SDK COOKIE] fail to write cookie file " << cookie_file;
        fs.Close();
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }
    fs.Close();

    if(!AppendChecksumToCookie(cookie_file)) {
        LOG(INFO) << "[SDK COOKIE] fail to append checksum to cookie file " << cookie_file;
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }
    if(!AddOtherUserWritePermission(cookie_file)) {
        LOG(INFO) << "[SDK COOKIE] fail to chmod cookie file " << cookie_file;
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }

    CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
    LOG(INFO) << "[SDK COOKIE] update local cookie success: " << cookie_file;
}

void TableImpl::DumpCookie() {
    DoDumpCookie();
    ThreadPool::Task task = boost::bind(&TableImpl::DumpCookie, this);
    AddDelayTask(FLAGS_tera_sdk_cookie_update_interval * 1000, task);
}

void TableImpl::EnableCookieUpdateTimer() {
    ThreadPool::Task task = boost::bind(&TableImpl::DumpCookie, this);
    AddDelayTask(FLAGS_tera_sdk_cookie_update_interval * 1000, task);
}

std::string TableImpl::GetCookieFileName(const std::string& tablename,
                                         const std::string& zk_addr,
                                         const std::string& zk_path,
                                         int64_t create_time) {
    uint32_t hash = 0;
    if (GetHashNumber(zk_addr, hash, &hash) != 0
        || GetHashNumber(zk_path, hash, &hash) != 0) {
        LOG(FATAL) << "invalid arguments";
    }
    char hash_str[9] = {'\0'};
    sprintf(hash_str, "%08x", hash);
    std::stringstream fname;
    fname << tablename << "-" << create_time << "-" << hash_str;
    return fname.str();
}

void TableImpl::DumpPerfCounterLogDelay() {
    DoDumpPerfCounterLog();
    ThreadPool::Task task =
        boost::bind(&TableImpl::DumpPerfCounterLogDelay, this);
    AddDelayTask(FLAGS_tera_sdk_perf_counter_log_interval * 1000, task);
}

void TableImpl::DoDumpPerfCounterLog() {
    LOG(INFO) << "[Table " << _name << "] " <<  _perf_counter.ToLog()
        << "pending_r: " << _cur_reader_pending_counter.Get() << ", "
        << "pending_w: " << _cur_commit_pending_counter.Get();
}

void TableImpl::DelayTaskWrapper(ThreadPool::Task task, int64_t task_id) {
    {
        MutexLock lock(&_delay_task_id_mutex);
        if (_delay_task_ids.erase(task_id) == 0) {
            // this task has been canceled
            return;
        }
    }
    task(task_id);
}
int64_t TableImpl::AddDelayTask(int64_t delay_time, ThreadPool::Task task) {
    MutexLock lock(&_delay_task_id_mutex);
    ThreadPool::Task t =
        boost::bind(&TableImpl::DelayTaskWrapper, this, task, _1);
    int64_t t_id = _thread_pool->DelayTask(delay_time, t);
    _delay_task_ids.insert(t_id);
    return t_id;
}
void TableImpl::ClearDelayTask() {
    MutexLock lock(&_delay_task_id_mutex);
    std::set<int64_t>::iterator it = _delay_task_ids.begin();
    for (; it != _delay_task_ids.end(); ++it) {
        _thread_pool->CancelTask(*it);
    }
    _delay_task_ids.clear();
}

static int64_t CalcAverage(Counter& sum, Counter& cnt, int64_t interval) {
    if (cnt.Get() == 0 || interval == 0) {
        return 0;
    } else {
        return sum.Clear() * 1000 / cnt.Clear() / interval / 1000;
    }
}

std::string TableImpl::PerfCounter::ToLog() {
    std::stringstream ss;
    int64_t ts = common::timer::get_micros();
    int64_t interval = (ts - start_time) / 1000;
    ss << "rpc_r: " << CalcAverage(rpc_r, rpc_r_cnt, interval) << ", ";
    ss << "rpc_w: " << CalcAverage(rpc_w, rpc_w_cnt, interval) << ", ";
    ss << "rpc_s: " << CalcAverage(rpc_s, rpc_s_cnt, interval) << ", ";
    ss << "callback: " << CalcAverage(user_callback, user_callback_cnt, interval) << ", ";
    start_time = ts;
    return ss.str();
}

void TableImpl::BreakRequest(int64_t task_id) {
    SdkTask* task = _task_pool.PopTask(task_id);
    if (task == NULL) {
        VLOG(10) << "task " << task_id << " timeout when brankrequest";
        return;
    }
    CHECK_EQ(task->GetRef(), 1);
    switch (task->Type()) {
    case SdkTask::MUTATION:
        ((RowMutationImpl*)task)->RunCallback();
        break;
    case SdkTask::READ:
        ((RowReaderImpl*)task)->RunCallback();
        break;
    default:
        CHECK(false);
        break;
    }
}

std::string CounterCoding::EncodeCounter(int64_t counter) {
    char counter_buf[sizeof(int64_t)];
    io::EncodeBigEndian(counter_buf, counter);
    return std::string(counter_buf, sizeof(counter_buf));
}

bool CounterCoding::DecodeCounter(const std::string& buf,
                                  int64_t* counter) {
    assert(counter);
    if (buf.size() != sizeof(int64_t)) {
        *counter = 0;
        return false;
    }
    *counter = io::DecodeBigEndainSign(buf.data());
    return true;
}

} // namespace tera
