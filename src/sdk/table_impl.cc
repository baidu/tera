// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/table_impl.h"

#include <boost/bind.hpp>

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "common/base/string_format.h"
#include "common/file/file_path.h"
#include "common/file/recordio/record_io.h"
#include "gflags/gflags.h"

#include "proto/kv_helper.h"
#include "proto/master_client.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode_client.h"
#include "proto/tabletnode_client_async.h"
#include "sdk/mutate_impl.h"
#include "sdk/read_impl.h"
#include "sdk/scan_impl.h"
#include "sdk/schema_impl.h"
#include "sdk/sdk_zk.h"
#include "sdk/tera.h"
#include "utils/string_util.h"
#include "utils/timer.h"

DECLARE_string(tera_master_meta_table_name);
DECLARE_int32(tera_sdk_delay_send_internal);
DECLARE_int32(tera_sdk_retry_times);
DECLARE_int32(tera_sdk_retry_period);
DECLARE_int32(tera_sdk_update_meta_internal);
DECLARE_bool(tera_sdk_write_sync);
DECLARE_int32(tera_sdk_batch_size);
DECLARE_int32(tera_sdk_batch_send_interval);
DECLARE_int64(tera_sdk_max_mutation_pending_num);
DECLARE_int64(tera_sdk_max_reader_pending_num);
DECLARE_bool(tera_sdk_async_blocking_enabled);
DECLARE_int32(tera_sdk_sync_wait_timeout);
DECLARE_int32(tera_sdk_scan_buffer_limit);
DECLARE_int32(tera_sdk_update_meta_concurrency);
DECLARE_bool(tera_sdk_cookie_enabled);
DECLARE_string(tera_sdk_cookie_path);
DECLARE_int32(tera_sdk_cookie_update_interval);
DECLARE_bool(tera_sdk_pend_request_while_scan_meta_enabled);

namespace tera {

TableImpl::TableImpl(const std::string& table_name,
            const std::string& zk_root_path,
            const std::string& zk_addr_list,
            common::ThreadPool* thread_pool) 
    : _name(table_name),
      _last_sequence_id(0),
      _timeout(FLAGS_tera_sdk_sync_wait_timeout),
      _commit_size(FLAGS_tera_sdk_batch_size),
      _commit_timeout(FLAGS_tera_sdk_batch_send_interval),
      _max_commit_pending_num(FLAGS_tera_sdk_max_mutation_pending_num),
      _max_reader_pending_num(FLAGS_tera_sdk_max_reader_pending_num),
      _meta_cond(&_meta_mutex),
      _meta_updating_count(0),
      _table_meta_cond(&_table_meta_mutex),
      _table_meta_updating(false),
      _zk_root_path(zk_root_path),
      _zk_addr_list(zk_addr_list),
      _thread_pool(thread_pool) {
    _tabletnode_client = new tabletnode::TabletNodeClient();
    _master_client = new master::MasterClient();
    _cluster = new sdk::ClusterFinder(zk_root_path, zk_addr_list);
}

TableImpl::~TableImpl() {
    if (FLAGS_tera_sdk_cookie_enabled) {
        DoDumpCookie();
    }

    delete _tabletnode_client;
    delete _master_client;
    delete _cluster;
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
    ApplyMutation(mu_list, true);
}

void TableImpl::ApplyMutation(const std::vector<RowMutation*>& row_mutations) {
    std::vector<RowMutationImpl*> mu_list(row_mutations.size());
    for (uint32_t i = 0; i < row_mutations.size(); i++) {
        mu_list[i] = static_cast<RowMutationImpl*>(row_mutations[i]);
    }
    ApplyMutation(mu_list, true);
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
    ReadRows(row_reader_list, true);
}

void TableImpl::Get(const std::vector<RowReader*>& row_readers) {
    std::vector<RowReaderImpl*> row_reader_list(row_readers.size());
    for (uint32_t i = 0; i < row_readers.size(); ++i) {
        row_reader_list[i] = static_cast<RowReaderImpl*>(row_readers[i]);
    }
    ReadRows(row_reader_list, true);
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, std::string* value,
                    ErrorCode* err) {
    RowReader* row_reader = NewRowReader(row_key);
    row_reader->AddColumn(family, qualifier);
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
    if (impl->GetFilterString() != "") {
        MutexLock lock(&_table_meta_mutex);
        impl->SetTableSchema(_table_schema);
        if (!impl->ParseFilterString()) {
            // fail to parse filter string
            return NULL;
        }
    }
    ResultStream * results = NULL;
    if (desc.IsAsync() && !IsKvOnlyTable()) {
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
    ScanTabletAsync(scan_task, 0);
}

void TableImpl::ScanTabletAsync(ScanTask* scan_task, int) {
    const std::string& row_key = scan_task->stream->GetScanDesc()->GetStartRowKey();
    std::string server_addr;
    if (GetTabletAddrOrScheduleUpdateMeta(row_key, scan_task, &server_addr)) {
        CommitScan(scan_task, server_addr);
    }
}

void TableImpl::CommitScan(ScanTask* scan_task,
                           const std::string& server_addr) {
    tabletnode::TabletNodeClientAsync tabletnode_client(server_addr);
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
    for (uint32_t i = 0; i < impl->GetSizeofColumnFamilyList(); ++i) {
        tera::ColumnFamily* column_family = request->add_cf_list();
        column_family->CopyFrom(*(impl->GetColumnFamily(i)));
    }

    Closure<void, ScanTabletRequest*, ScanTabletResponse*, bool, int>* done =
        NewClosure(this, &TableImpl::ScanCallBack, scan_task);
    tabletnode_client.ScanTablet(request, response, done);
}

void TableImpl::ScanCallBack(ScanTask* scan_task,
                             ScanTabletRequest* request,
                             ScanTabletResponse* response,
                             bool failed, int error_code) {
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
        || scan_task->RetryTimes() >= FLAGS_tera_sdk_retry_times) {
        if (err == kKeyNotInRange || err == kConnectError) {
            ScheduleUpdateMeta(stream->GetScanDesc()->GetStartRowKey(),
                               scan_task->GetMetaTimeStamp());
        }
        stream->OnFinish(request, response);
        stream->ReleaseRpcHandle(request, response);
        delete scan_task;
    } else if (err == kKeyNotInRange) {
        scan_task->IncRetryTimes();
        ScanTabletAsync(scan_task, 0);
    } else {
        scan_task->IncRetryTimes();
        boost::function<void ()> retry_closure =
            boost::bind(&TableImpl::ScanTabletAsync, this, scan_task, 0);
        _thread_pool->DelayTask(
            FLAGS_tera_sdk_retry_period * scan_task->RetryTimes(), retry_closure);
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

bool TableImpl::IsKvOnlyTable() {
    if (_table_schema.column_families_size() > 0) {
        return true;
    }
    return false;
}

bool TableImpl::OpenInternal(ErrorCode* err) {
    if (!UpdateTableMeta(err)) {
        return false;
    }
    if (FLAGS_tera_sdk_cookie_enabled) {
        if (!RestoreCookie()) {
            return false;
        }
        EnableCookieUpdateTimer();
    }
    return true;
}

void TableImpl::ApplyMutation(const std::vector<RowMutationImpl*>& mu_list,
                              bool called_by_user) {
    // 1st: mu_list, 2nd: auto_flush
    typedef std::pair<std::vector<RowMutationImpl*>*, bool> MuFlushPair;
    typedef std::map<std::string, MuFlushPair> TsMuMap;
    TsMuMap ts_mu_list;

    int64_t sync_min_timeout = -1;
    std::vector<RowMutationImpl*> sync_mu_list;

    for (uint32_t i = 0; i < mu_list.size(); i++) {
        RowMutationImpl* row_mutation = (RowMutationImpl*)mu_list[i];

        if (!row_mutation->IsAsync()) {
            sync_mu_list.push_back(row_mutation);
            int64_t row_timeout = row_mutation->TimeOut() > 0 ? row_mutation->TimeOut() : _timeout;
            if (row_timeout > 0 && (sync_min_timeout <= 0 || sync_min_timeout > row_timeout)) {
                sync_min_timeout = row_timeout;
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
                boost::function<void ()> closure =
                    boost::bind(&TableImpl::BreakRequest<RowMutationImpl>, this, row_mutation);
                _thread_pool->DelayTask(1, closure);
                continue;
            }
        }

        std::string server_addr;
        if (!GetTabletAddrOrScheduleUpdateMeta(row_mutation->RowKey(),
                                               row_mutation, &server_addr)) {
            continue;
        }

        std::pair<TsMuMap::iterator, bool> insert_ret = ts_mu_list.insert(
            std::pair<std::string, MuFlushPair>(server_addr, MuFlushPair(NULL, false)));
        TsMuMap::iterator it = insert_ret.first;
        MuFlushPair& mu_flush_pair = it->second;
        if (insert_ret.second) {
            mu_flush_pair.first = new std::vector<RowMutationImpl*>;
            mu_flush_pair.second = false;
        }
        std::vector<RowMutationImpl*>* ts_row_mutations = mu_flush_pair.first;
        ts_row_mutations->push_back(row_mutation);

        if (!row_mutation->IsAsync()) {
            mu_flush_pair.second = true;
        }
    }

    int64_t sync_wait_abs_time = (sync_min_timeout > 0) ? GetTimeStampInMs() + sync_min_timeout : -1;

    TsMuMap::iterator it = ts_mu_list.begin();
    for (; it != ts_mu_list.end(); ++it) {
        MuFlushPair& mu_flush_pair = it->second;
        ApplyMutation(it->first, mu_flush_pair.first, mu_flush_pair.second);
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
        if (!row_mutation->Wait(sync_wait_abs_time)) {
            row_mutation->SetError(ErrorCode::kTimeout, "timeout");
        }
    }
}

void TableImpl::RetryApplyMutation(std::vector<RowMutationImpl*>* retry_mu_list) {
    ApplyMutation(*retry_mu_list, false);
    delete retry_mu_list;
}

void TableImpl::ApplyMutation(const std::string& server_addr,
                              std::vector<RowMutationImpl*>* mu_list,
                              bool flush) {
    if (flush) {
        CommitMutation(server_addr, mu_list);
        return;
    }

    MutexLock lock(&_commit_buffer_mutex);
    CommitBuffer* commit_buffer = NULL;
    std::map<std::string, CommitBuffer>::iterator it =
        _commit_buffers.find(server_addr);
    if (it == _commit_buffers.end()) {
        commit_buffer = &_commit_buffers[server_addr];
        commit_buffer->_row_list = new std::vector<RowMutationImpl*>;
        boost::function<void ()> closure =
            boost::bind(&TableImpl::CommitMutationBuffer, this, server_addr);
        int64_t timer_id = _thread_pool->DelayTask(_commit_timeout, closure);
        commit_buffer->_timer_id = timer_id;
    } else {
        commit_buffer = &it->second;
    }
    commit_buffer->_row_list->insert(commit_buffer->_row_list->end(),
                                     mu_list->begin(), mu_list->end());
    if (commit_buffer->_row_list->size() >= _commit_size) {
        std::vector<RowMutationImpl*>* commit_mu_list = commit_buffer->_row_list;
        _thread_pool->CancelTask(commit_buffer->_timer_id);
        _commit_buffers.erase(server_addr);
        _commit_buffer_mutex.Unlock();
        CommitMutation(server_addr, commit_mu_list);
        _commit_buffer_mutex.Lock();
    }

    delete mu_list;
}

void TableImpl::CommitMutationBuffer(std::string server_addr) {
    std::vector<RowMutationImpl*>* commit_mu_list = NULL;
    {
        MutexLock lock(&_commit_buffer_mutex);
        std::map<std::string, CommitBuffer>::iterator it =
            _commit_buffers.find(server_addr);
        if (it == _commit_buffers.end()) {
            return;
        }
        CommitBuffer* commit_buffer = &it->second;
        commit_mu_list = commit_buffer->_row_list;
        _commit_buffers.erase(it);
    }
    CommitMutation(server_addr, commit_mu_list);
}

void TableImpl::CommitMutation(const std::string& server_addr,
                               std::vector<RowMutationImpl*>* mu_list) {
    tabletnode::TabletNodeClientAsync tabletnode_client_async(server_addr);
    WriteTabletRequest* request = new WriteTabletRequest;
    WriteTabletResponse* response = new WriteTabletResponse;
    request->set_sequence_id(_last_sequence_id++);
    request->set_tablet_name(_name);
    request->set_is_sync(FLAGS_tera_sdk_write_sync);
    //KeyValuePair* pair = request->mutable_pair_list()->Add();
    for (uint32_t i = 0; i < mu_list->size(); ++i) {
        RowMutationImpl* row_mutation = (*mu_list)[i];
        RowMutationSequence* mu_seq = request->add_row_list();
        mu_seq->set_row_key(row_mutation->RowKey());
        for (uint32_t j = 0; j < row_mutation->MutationNum(); j++) {
            const RowMutation::Mutation& mu = row_mutation->GetMutation(j);
            tera::Mutation* mutation = mu_seq->add_mutation_sequence();
            SerializeMutation(mu, mutation);
            //mutation->CopyFrom(row_mutation->Mutation(j));
        }
    }

    Closure<void, WriteTabletRequest*, WriteTabletResponse*, bool, int>* done =
        NewClosure(this, &TableImpl::MutateCallBack, mu_list);
    tabletnode_client_async.WriteTablet(request, response, done);
}

void TableImpl::MutateCallBack(std::vector<RowMutationImpl*>* mu_list,
                               WriteTabletRequest* request,
                               WriteTabletResponse* response,
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

    std::map<uint32_t, std::vector<RowMutationImpl*>* > retry_times_list;
    std::vector<RowMutationImpl*>* not_in_range_list = NULL;
    for (uint32_t i = 0; i < mu_list->size(); ++i) {
        StatusCode err = response->status();
        if (err == kTabletNodeOk) {
            err = response->row_status_list(i);
        }
        if (err != kTabletNodeOk) {
            VLOG(10) << "fail to mutate table: " << _name
                << " errcode: " << StatusCodeToString(err);
        }

        RowMutationImpl* row_mutation = (*mu_list)[i];
        row_mutation->SetInternalError(err);
        if (err == kTabletNodeOk) {
            row_mutation->SetError(ErrorCode::kOK);
            // only for flow control
            _cur_commit_pending_counter.Sub(row_mutation->MutationNum());
            row_mutation->RunCallback();
        } else if (row_mutation->RetryTimes() >= FLAGS_tera_sdk_retry_times) {
            if (err == kKeyNotInRange || err == kConnectError) {
                ScheduleUpdateMeta(row_mutation->RowKey(),
                                   row_mutation->GetMetaTimeStamp());
            }
            std::string err_reason = StringFormat("Reach the limit of retry times, error: %s",
                                StatusCodeToString(err).c_str());
            row_mutation->SetError(ErrorCode::kSystem, err_reason);
            // only for flow control
            _cur_commit_pending_counter.Sub(row_mutation->MutationNum());
            row_mutation->RunCallback();
        } else if (err == kKeyNotInRange) {
            row_mutation->IncRetryTimes();
            if (not_in_range_list == NULL) {
                not_in_range_list = new std::vector<RowMutationImpl*>;
            }
            not_in_range_list->push_back(row_mutation);
        } else {
            row_mutation->IncRetryTimes();
            std::vector<RowMutationImpl*>* retry_mu_list = NULL;
            std::map<uint32_t, std::vector<RowMutationImpl*>* >::iterator it =
                retry_times_list.find(row_mutation->RetryTimes());
            if (it != retry_times_list.end()) {
                retry_mu_list = it->second;
            } else {
                retry_mu_list = new std::vector<RowMutationImpl*>;
                retry_times_list[row_mutation->RetryTimes()] = retry_mu_list;
            }
            retry_mu_list->push_back(row_mutation);
        }
    }

    if (not_in_range_list != NULL) {
        RetryApplyMutation(not_in_range_list);
    }
    std::map<uint32_t, std::vector<RowMutationImpl*>* >::iterator it;
    for (it = retry_times_list.begin(); it != retry_times_list.end(); ++it) {
        int64_t retry_interval =
            static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal, it->first) * 1000);
        boost::function<void ()> retry_closure =
            boost::bind(&TableImpl::RetryApplyMutation, this, it->second);
        _thread_pool->DelayTask(retry_interval, retry_closure);
    }

    delete request;
    delete response;
    delete mu_list;
}

bool TableImpl::GetTabletLocation(std::vector<TabletInfo>* tablets,
                                  ErrorCode* err) {
    return false;
}

bool TableImpl::GetDescriptor(TableDescriptor* desc, ErrorCode* err) {
    return false;
}

void TableImpl::ReadRows(const std::vector<RowReaderImpl*>& row_reader_list,
                         bool called_by_user) {
    typedef std::map<std::string, std::vector<RowReaderImpl*>* > TsReaderMap;
    TsReaderMap ts_reader_list;

    int64_t sync_min_timeout = -1;
    std::vector<RowReaderImpl*> sync_reader_list;

    for (uint32_t i = 0; i < row_reader_list.size(); i++) {
        RowReaderImpl* row_reader = (RowReaderImpl*)row_reader_list[i];

        if (!row_reader->IsAsync()) {
            sync_reader_list.push_back(row_reader);
            int64_t row_timeout = row_reader->TimeOut() > 0 ? row_reader->TimeOut() : _timeout;
            if (row_timeout > 0 && (sync_min_timeout <= 0 || sync_min_timeout > row_timeout)) {
                sync_min_timeout = row_timeout;
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
                boost::function<void ()> closure =
                    boost::bind(&TableImpl::BreakRequest<RowReaderImpl>, this, row_reader);
                _thread_pool->DelayTask(1, closure);
                continue;
            }
        }

        std::string server_addr;
        if (!GetTabletAddrOrScheduleUpdateMeta(row_reader->RowName(), row_reader,
                                               &server_addr)) {
            continue;
        }

        std::pair<TsReaderMap::iterator, bool> insert_ret = ts_reader_list.insert(
            std::pair<std::string, std::vector<RowReaderImpl*>* >(server_addr, NULL));
        TsReaderMap::iterator it = insert_ret.first;
        if (insert_ret.second) {
            it->second = new std::vector<RowReaderImpl*>;
        }
        std::vector<RowReaderImpl*>* ts_row_readers = it->second;
        ts_row_readers->push_back(row_reader);
    }
    int64_t sync_wait_abs_time = (sync_min_timeout > 0) ? GetTimeStampInMs() + sync_min_timeout : -1;

    TsReaderMap::iterator it = ts_reader_list.begin();
    for (; it != ts_reader_list.end(); ++it) {
        std::vector<RowReaderImpl*>* reader_list = it->second;
        ReadRows(it->first, reader_list);
    }
    // 从现在开始，所有异步的row_mutation都不可以再操作了，因为随时会被用户释放

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
        if (!row_reader->Wait(sync_wait_abs_time)) {
            row_reader->SetError(ErrorCode::kTimeout, "timeout");
        }
    }
}

void TableImpl::ReadRows(const std::string& server_addr,
                         std::vector<RowReaderImpl*>* reader_list) {
    MutexLock lock(&_reader_buffer_mutex);
    ReaderBuffer* reader_buffer = NULL;
    std::map<std::string, ReaderBuffer>::iterator it =
        _reader_buffers.find(server_addr);
    if (it == _reader_buffers.end()) {
        reader_buffer = &_reader_buffers[server_addr];
        reader_buffer->_reader_list = new std::vector<RowReaderImpl*>;
        boost::function<void ()> closure =
            boost::bind(&TableImpl::CommitReaderBuffer, this, server_addr);
        uint64_t timer_id = _thread_pool->DelayTask(_commit_timeout, closure);
        reader_buffer->_timer_id = timer_id;
    } else {
        reader_buffer = &it->second;
    }
    reader_buffer->_reader_list->insert(reader_buffer->_reader_list->end(),
                                        reader_list->begin(), reader_list->end());
    if (reader_buffer->_reader_list->size() >= _commit_size) {
        std::vector<RowReaderImpl*>* commit_reader_list = reader_buffer->_reader_list;
        _thread_pool->CancelTask(reader_buffer->_timer_id);
        _reader_buffers.erase(server_addr);
        _reader_buffer_mutex.Unlock();
        CommitReaders(server_addr, commit_reader_list);
        _reader_buffer_mutex.Lock();
    }

    delete reader_list;
}

void TableImpl::CommitReaderBuffer(std::string server_addr) {
    std::vector<RowReaderImpl*>* commit_reader_list = NULL;
    {
        MutexLock lock(&_reader_buffer_mutex);
        std::map<std::string, ReaderBuffer>::iterator it =
            _reader_buffers.find(server_addr);
        if (it == _reader_buffers.end()) {
            return;
        }
        ReaderBuffer* reader_buffer = &it->second;
        commit_reader_list = reader_buffer->_reader_list;
        _reader_buffers.erase(it);
    }
    CommitReaders(server_addr, commit_reader_list);

}

void TableImpl::CommitReaders(const std::string server_addr,
                              std::vector<RowReaderImpl*>* reader_list) {
    tabletnode::TabletNodeClientAsync tabletnode_client_async(server_addr);
    ReadTabletRequest* request = new ReadTabletRequest;
    ReadTabletResponse* response = new ReadTabletResponse;
    request->set_sequence_id(_last_sequence_id++);
    request->set_tablet_name(_name);
    for (uint32_t i = 0; i < reader_list->size(); ++i) {
        RowReaderImpl* row_reader = (*reader_list)[i];
        RowReaderInfo* row_reader_info = request->add_row_info_list();
        request->set_snapshot_id(row_reader->GetSnapshot());
        row_reader->ToProtoBuf(row_reader_info);
        //row_reader_info->CopyFrom(row_reader->GetRowReaderInfo());
    }
    Closure<void, ReadTabletRequest*, ReadTabletResponse*, bool, int>* done =
        NewClosure(this, &TableImpl::ReaderCallBack, reader_list);
    tabletnode_client_async.ReadTablet(request, response, done);

}

void TableImpl::ReaderCallBack(std::vector<RowReaderImpl*>* reader_list,
                               ReadTabletRequest* request,
                               ReadTabletResponse* response,
                               bool failed,int error_code) {
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

    std::map<uint32_t, std::vector<RowReaderImpl*>* > retry_times_list;
    std::vector<RowReaderImpl*>* not_in_range_list = NULL;
    uint32_t row_result_num = 0;
    for (uint32_t i = 0; i < reader_list->size(); ++i) {
        StatusCode err = response->status();
        if (err == kTabletNodeOk) {
            err = response->detail().status(i);
        }
        if (err != kTabletNodeOk && err != kKeyNotExist && err != kSnapshotNotExist) {
            VLOG(10) << "fail to read table: " << _name
                << " errcode: " << StatusCodeToString(err);
        }

        RowReaderImpl* row_reader = (*reader_list)[i];
        row_reader->SetInternalError(err);
        if (err == kTabletNodeOk) {
            row_reader->SetResult(response->detail().row_result(row_result_num++));
            row_reader->SetError(ErrorCode::kOK);
            row_reader->RunCallback();
            // only for flow control
            _cur_reader_pending_counter.Dec();
        } else if (err == kKeyNotExist) {
            row_reader->SetError(ErrorCode::kNotFound, "not found");
            row_reader->RunCallback();
            // only for flow control
            _cur_reader_pending_counter.Dec();
        } else if (err == kSnapshotNotExist) {
            row_reader->SetError(ErrorCode::kNotFound, "snapshot not found");
            row_reader->RunCallback();
            // only for flow control
            _cur_reader_pending_counter.Dec();
        } else if (row_reader->RetryTimes() >= FLAGS_tera_sdk_retry_times) {
            if (err == kKeyNotInRange || err == kConnectError) {
                ScheduleUpdateMeta(row_reader->RowName(),
                                   row_reader->GetMetaTimeStamp());
            }
            std::string err_reason = StringFormat("Reach the limit of retry times, error: %s",
                    StatusCodeToString(err).c_str());
            row_reader->SetError(ErrorCode::kSystem, err_reason);
            row_reader->RunCallback();
            // only for flow control
            _cur_reader_pending_counter.Dec();
        } else if (err == kKeyNotInRange) {
            row_reader->IncRetryTimes();
            if (not_in_range_list == NULL) {
                not_in_range_list = new std::vector<RowReaderImpl*>;
            }
            not_in_range_list->push_back(row_reader);
        } else {
            row_reader->IncRetryTimes();
            std::vector<RowReaderImpl*>* retry_reader_list = NULL;
            std::map<uint32_t, std::vector<RowReaderImpl*>* >::iterator it =
                retry_times_list.find(row_reader->RetryTimes());
            if (it != retry_times_list.end()) {
                retry_reader_list = it->second;
            } else {
                retry_reader_list = new std::vector<RowReaderImpl*>;
                retry_times_list[row_reader->RetryTimes()] = retry_reader_list;
            }
            retry_reader_list->push_back(row_reader);
        }
    }

    if (not_in_range_list != NULL) {
        RetryReadRows(not_in_range_list);
    }
    std::map<uint32_t, std::vector<RowReaderImpl*>* >::iterator it;
    for (it = retry_times_list.begin(); it != retry_times_list.end(); ++it) {
        int64_t retry_interval =
            static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal, it->first) * 1000);
        boost::function<void ()> retry_closure =
            boost::bind(&TableImpl::RetryReadRows, this, it->second);
        _thread_pool->DelayTask(retry_interval, retry_closure);
    }

    delete request;
    delete response;
    delete reader_list;
}

void TableImpl::RetryReadRows(std::vector<RowReaderImpl*>* retry_reader_list) {
    ReadRows(*retry_reader_list, false);
    delete retry_reader_list;
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

void TableImpl::ProcessTaskPendingForMeta(const std::string& row, SdkTask* task) {
    if (FLAGS_tera_sdk_pend_request_while_scan_meta_enabled) {
        _pending_task_list[row].push_back(task);
    } else {
        switch (task->Type()) {
            case SdkTask::READ: {
                RowReaderImpl* reader = (RowReaderImpl*)task;
                reader->IncRetryTimes();
                if (reader->RetryTimes() >= FLAGS_tera_sdk_retry_times) {
                    VLOG(7) << "ProcessTaskPendingForMeta-RowReader key:[" << row << "] "
                            << " Reach the limit of retry times, error: meta not feteched.";
                    _cur_reader_pending_counter.Dec();
                    reader->SetError(ErrorCode::kSystem,
                            "Reach the limit of retry times, error: meta not feteched");
                    _thread_pool->DelayTask(1,
                            boost::bind(&TableImpl::BreakRequest<RowReaderImpl>, this, reader));
                } else {
                    VLOG(7) << "ProcessTaskPendingForMeta-RowReader key:[" << row << "] "
                            << " Retry for " << reader->RetryTimes() << " times";
                    std::vector<RowReaderImpl*>* reader_list = new std::vector<RowReaderImpl*>;
                    reader_list->push_back(reader);
                    int64_t retry_interval = static_cast<int64_t>(
                            pow(FLAGS_tera_sdk_delay_send_internal, reader->RetryTimes()) * 1000);
                    boost::function<void ()> retry_closure = 
                        boost::bind(&TableImpl::RetryReadRows, this, reader_list);
                    _thread_pool->DelayTask(retry_interval, retry_closure);
                }
        } break;
        case SdkTask::MUTATION: {
            RowMutationImpl* mutation = (RowMutationImpl*)task;
            mutation->IncRetryTimes();
            if (mutation->RetryTimes() >= FLAGS_tera_sdk_retry_times) {
                VLOG(7) << "ProcessTaskPendingForMeta-RowMutation key:[" << row << "] "
                        << " Reach the limit of retry times, error: meta not feteched.";
                _cur_commit_pending_counter.Sub(mutation->MutationNum());
                mutation->SetError(ErrorCode::kSystem,
                        "Reach the limit of retry times, error: meta not feteched");
                _thread_pool->DelayTask(1,
                        boost::bind(&TableImpl::BreakRequest<RowMutationImpl>, this, mutation));
            } else {
                VLOG(7) << "ProcessTaskPendingForMeta-RowMutation key:[" << row << "] "
                        << " Retry for " << mutation->RetryTimes() << " times";
                std::vector<RowMutationImpl*>* mutation_list = new std::vector<RowMutationImpl*>;
                mutation_list->push_back(mutation);
                int64_t retry_interval = static_cast<int64_t>(
                        pow(FLAGS_tera_sdk_delay_send_internal, mutation->RetryTimes()) * 1000);
                boost::function<void ()> retry_closure =
                    boost::bind(&TableImpl::RetryApplyMutation, this, mutation_list);
                _thread_pool->DelayTask(retry_interval, retry_closure);
            }
        } break;
        case SdkTask::SCAN: {
            ScanTask* scan_task = (ScanTask*)task;
            scan_task->IncRetryTimes();
            if (scan_task->RetryTimes() >= FLAGS_tera_sdk_retry_times) {
                VLOG(7) << "ProcessTaskPendingForMeta-Scan key:[" << row << "] "
                        << " Reach the limit of retry times, error: meta not feteched.";
                scan_task->response->set_status(kRPCError);
                _thread_pool->DelayTask(1,
                        boost::bind(&TableImpl::BreakScan, this, scan_task));
            } else {
                VLOG(7) << "ProcessTaskPendingForMeta-Scan key:[" << row << "] "
                        << " Retry for " << scan_task->RetryTimes() << " times";
                boost::function<void ()> retry_closure =
                    boost::bind(&TableImpl::ScanTabletAsync, this, scan_task, 0);
                _thread_pool->DelayTask(
                    FLAGS_tera_sdk_retry_period * scan_task->RetryTimes(), retry_closure);
            }
        } break;
        default: {
            CHECK(false);
        } break;
        }
    }
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
    MutexLock lock(&_meta_mutex);
    TabletMetaNode* node = GetTabletMetaNodeForKey(row);
    if (node == NULL) {
        VLOG(10) << "no meta for key: " << row;
        ProcessTaskPendingForMeta(row, task);
        TabletMetaNode& new_node = _tablet_meta_list[row];
        new_node.meta.mutable_key_range()->set_key_start(row);
        new_node.meta.mutable_key_range()->set_key_end(row + '\0');
        new_node.status = WAIT_UPDATE;
        UpdateMetaAsync();
        return false;
    }
    if (node->status != NORMAL) {
        VLOG(10) << "abnormal meta for key: " << row;
        ProcessTaskPendingForMeta(row, task);
        return false;
    }
    if ((task->GetInternalError() == kKeyNotInRange || task->GetInternalError() == kConnectError)
            && task->GetMetaTimeStamp() >= node->update_time) {
        ProcessTaskPendingForMeta(row, task);
        int64_t update_interval = node->update_time
            + FLAGS_tera_sdk_update_meta_internal - get_micros() / 1000;
        if (update_interval <= 0) {
            VLOG(10) << "update meta now for key: " << row;
            node->status = WAIT_UPDATE;
            UpdateMetaAsync();
        } else {
            VLOG(10) << "update meta in " << update_interval << " (ms) for key:" << row;
            node->status = DELAY_UPDATE;
            boost::function<void ()> delay_closure =
                boost::bind(&TableImpl::DelayUpdateMeta, this,
                           node->meta.key_range().key_start(),
                           node->meta.key_range().key_end());
            _thread_pool->DelayTask(update_interval, delay_closure);
        }
        return false;
    }
    task->SetMetaTimeStamp(node->update_time);
    *server_addr = node->meta.server_addr();
    return true;
}

TableImpl::TabletMetaNode* TableImpl::GetTabletMetaNodeForKey(const std::string& key) {
    //CHECK(_meta_mutex.IsLocked());
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
    //CHECK(_meta_mutex.IsLocked());
    if (_meta_updating_count >= FLAGS_tera_sdk_update_meta_concurrency) {
        return;
    }
    bool need_update = false;
    std::string update_start_key;
    std::string update_end_key;
    std::map<std::string, TabletMetaNode>::iterator it = _tablet_meta_list.begin();
    for (; it != _tablet_meta_list.end(); ++it) {
        TabletMetaNode& node = it->second;
        if (node.status != WAIT_UPDATE && need_update) {
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
            CHECK(node.meta.key_range().key_start() > update_end_key);
            break;
        }
        node.status = UPDATING;
    }
    if (!need_update) {
        return;
    }
    _meta_updating_count++;
    ScanMetaTableAsync(update_start_key, update_end_key, false);
}

void TableImpl::ScanMetaTable(const std::string& key_start,
                              const std::string& key_end) {
    MutexLock lock(&_meta_mutex);
    _meta_updating_count++;
    ScanMetaTableAsync(key_start, key_end, false);
    while (_meta_updating_count > 0) {
        _meta_cond.Wait();
    }
}

void TableImpl::ScanMetaTableAsyncInLock(std::string key_start, std::string key_end,
                                   bool zk_access) {
    MutexLock lock(&_meta_mutex);
    ScanMetaTableAsync(key_start, key_end, zk_access);
}

void TableImpl::ScanMetaTableAsync(std::string key_start, std::string key_end,
                                   bool zk_access) {
    //CHECK(_meta_mutex.IsLocked());

    std::string meta_addr = _cluster->RootTableAddr(zk_access);
    if (meta_addr.empty() && !zk_access) {
        meta_addr = _cluster->RootTableAddr(true);
    }

    if (meta_addr.empty()) {
        VLOG(6) << "root is empty";

        boost::function<void ()> retry_closure =
            boost::bind(&TableImpl::ScanMetaTableAsyncInLock, this, key_start, key_end, true);
        _thread_pool->DelayTask(FLAGS_tera_sdk_update_meta_internal, retry_closure);
        return;
    }

    VLOG(6) << "root: " << meta_addr;
    tabletnode::TabletNodeClientAsync tabletnode_client_async(meta_addr);
    ScanTabletRequest* request = new ScanTabletRequest;
    ScanTabletResponse* response = new ScanTabletResponse;
    request->set_sequence_id(_last_sequence_id++);
    request->set_table_name(FLAGS_tera_master_meta_table_name);
    MetaTableScanRange(_name, key_start, key_end,
                       request->mutable_start(),
                       request->mutable_end());
    request->set_buffer_limit(FLAGS_tera_sdk_scan_buffer_limit);
    request->set_round_down(true);

    Closure<void, ScanTabletRequest*, ScanTabletResponse*, bool, int>* done =
        NewClosure(this, &TableImpl::ScanMetaTableCallBack, key_start, key_end);
    tabletnode_client_async.ScanTablet(request, response, done);
}

void TableImpl::ScanMetaTableCallBack(std::string key_start,
                                      std::string key_end,
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
        boost::function<void ()> retry_closure =
            boost::bind(&TableImpl::ScanMetaTableAsyncInLock, this, key_start, key_end, true);
        _thread_pool->DelayTask(FLAGS_tera_sdk_update_meta_internal, retry_closure);
        delete request;
        delete response;
        return;
    }

    std::string last_key;
    std::string return_start, return_end;
    const RowResult& scan_result = response->results();
    for (int32_t i = 0; i < scan_result.key_values_size(); i++) {
        const KeyValuePair& kv = scan_result.key_values(i);
        last_key = kv.key();

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
        << scan_result.key_values_size() << " records";
    if (scan_result.key_values_size() == 0
        || return_start > key_start
        || !return_end.empty() && (key_end.empty()|| return_end < key_end)) {
        LOG(ERROR) << "scan meta table [" << key_start << ", " << key_end
            << "] return [" << return_start << ", " << return_end << "]";
        //TODO(lk): process omitted tablets
    }

    std::string end = request->end();
    delete request;

    MutexLock lock(&_meta_mutex);
    if (!response->complete()) {
        ScanMetaTableAsync(last_key, end, false);
    } else {
        _meta_updating_count--;
        _meta_cond.Signal();
        UpdateMetaAsync();
    }
    delete response;
}

void TableImpl::GiveupUpdateTabletMeta(const std::string& key_start,
        const std::string& key_end) {
    std::map<std::string, std::list<SdkTask*> >::iterator ilist =
            _pending_task_list.lower_bound(key_start);
    while (ilist != _pending_task_list.end()) {
        if (!key_end.empty() && ilist->first >= key_end) {
            break;
        }
        std::list<SdkTask*>& task_list = ilist->second;
        for (std::list<SdkTask*>::iterator itask = task_list.begin();
                itask != task_list.end();) {
            SdkTask* task = *itask;
            switch (task->Type()) {
                case SdkTask::READ: {
                    RowReaderImpl* reader = (RowReaderImpl*) task;
                    reader->IncRetryTimes();
                    if (reader->RetryTimes() >= FLAGS_tera_sdk_retry_times) {
                        itask = task_list.erase(itask);
                        _cur_reader_pending_counter.Dec();
                        reader->SetError(ErrorCode::kSystem,
                                "Reach the limit of retry times, error: meta not feteched");
                        reader->RunCallback();
                        continue;
                    }
                } break;
                case SdkTask::MUTATION: {
                    RowMutationImpl* mutation = (RowMutationImpl*) task;
                    mutation->IncRetryTimes();
                    if (mutation->RetryTimes() >= FLAGS_tera_sdk_retry_times) {
                        itask = task_list.erase(itask);
                        _cur_commit_pending_counter.Sub(
                                mutation->MutationNum());
                        mutation->SetError(ErrorCode::kSystem,
                                "Reach the limit of retry times, error: meta not feteched");
                        mutation->RunCallback();
                        continue;
                    }
                } break;
                case SdkTask::SCAN: {
                    ScanTask* scan_task = (ScanTask*) task;
                    scan_task->IncRetryTimes();
                    if (scan_task->RetryTimes() >= FLAGS_tera_sdk_retry_times) {
                        itask = task_list.erase(itask);
                        ResultStreamImpl* stream = scan_task->stream;
                        scan_task->response->set_status(kRPCError);
                        stream->OnFinish(scan_task->request,
                                scan_task->response);
                        stream->ReleaseRpcHandle(scan_task->request,
                                scan_task->response);
                        delete scan_task;
                        continue;
                    }
                } break;
                default: {
                    CHECK(false);
                } break;
            }
            ++itask;
        }
        if (task_list.empty()) {
            _pending_task_list.erase(ilist++);
        } else {
            ++ilist;
        }
    }
}

void TableImpl::UpdateTabletMetaList(const TabletMeta& new_meta) {
    //CHECK(_meta_mutex.IsLocked());
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
            } else if (new_end.empty() || !old_end.empty() && old_end <= new_end) {
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
            if (new_end.empty() || !old_end.empty() && old_end <= new_end) {
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
    //CHECK(_meta_mutex.IsLocked());
    const std::string& start_key = node.meta.key_range().key_start();
    const std::string& end_key = node.meta.key_range().key_end();
    const std::string& server_addr = node.meta.server_addr();
    int64_t meta_timestamp = node.update_time;

    std::vector<RowMutationImpl*>* mutation_list = NULL;
    std::vector<RowReaderImpl*>* reader_list = NULL;

    std::map<std::string, std::list<SdkTask*> >::iterator it =
        _pending_task_list.lower_bound(start_key);
    while (it != _pending_task_list.end()) {
        if (!end_key.empty() && it->first >= end_key) {
            break;
        }
        std::list<SdkTask*>& task_list = it->second;
        for (std::list<SdkTask*>::iterator itask = task_list.begin();
                itask != task_list.end(); ++itask) {
            SdkTask* task = *itask;
            task->SetMetaTimeStamp(meta_timestamp);

            switch (task->Type()) {
            case SdkTask::READ: {
                RowReaderImpl* reader = (RowReaderImpl*)task;
                if (reader_list == NULL) {
                    reader_list = new std::vector<RowReaderImpl*>;
                }
                reader_list->push_back(reader);
            } break;
            case SdkTask::MUTATION: {
                RowMutationImpl* mutation = (RowMutationImpl*)task;
                if (mutation_list == NULL) {
                    mutation_list = new std::vector<RowMutationImpl*>;
                }
                mutation_list->push_back(mutation);
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
        std::map<std::string, std::list<SdkTask*> >::iterator tmp = it;
        ++it;
        _pending_task_list.erase(tmp);
    }

    if (mutation_list != NULL) {
        // TODO: flush ?
        ApplyMutation(server_addr, mutation_list, false);
    }
    if (reader_list != NULL) {
        ReadRows(server_addr, reader_list);
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
            boost::function<void ()> delay_closure =
                boost::bind(&TableImpl::DelayUpdateMeta, this,
                            node->meta.key_range().key_start(),
                            node->meta.key_range().key_end());
            _thread_pool->DelayTask(update_interval, delay_closure);
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
            boost::function<void ()> retry_closure =
                boost::bind(&TableImpl::ReadTableMetaAsync, this, ret_err, retry_times + 1, true);
            _thread_pool->DelayTask(retry_interval, retry_closure);
        }
        return;
    }

    tabletnode::TabletNodeClientAsync tabletnode_client_async(meta_server);
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
                                      bool failed,int error_code) {
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
        boost::function<void ()> retry_closure =
            boost::bind(&TableImpl::ReadTableMetaAsync, this, ret_err, retry_times + 1, true);
        _thread_pool->DelayTask(retry_interval, retry_closure);
    }

    delete request;
    delete response;
}

bool TableImpl::RestoreCookie() {
    const std::string& cookie_dir = FLAGS_tera_sdk_cookie_path;
    if (!IsExist(cookie_dir)) {
        if (!CreateDirWithRetry(cookie_dir)) {
            LOG(ERROR) << "[SDK COOKIE] fail to create cookie dir: " << cookie_dir;
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

    FileStream fs;
    if (!fs.Open(cookie_file, FILE_READ)) {
        LOG(ERROR) << "[SDK COOKIE] fail to open " << cookie_file;
        return true;
    }
    SdkCookie cookie;
    RecordReader record_reader;
    record_reader.Reset(&fs);
    if (!record_reader.ReadNextMessage(&cookie)) {
        LOG(ERROR) << "[SDK COOKIE] fail to parse sdk cookie, file: " << cookie_file;
        return true;
    }
    fs.Close();

    if (cookie.table_name() != _name) {
        LOG(ERROR) << "[SDK COOKIE] cookie name error: " << cookie.table_name()
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
        + GetCookieFileName(_name, _zk_addr_list, _zk_root_path);
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
            LOG(ERROR) << "[SDK COOKIE] fail to delete cookie-lock-file: " << lock_file
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
        LOG(ERROR) << "[SDK COOKIE] fail to delete cookie-lock-file: " << cookie_lock_file
                   << ". reason: " << strerror(errno_saved);
    }
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
        LOG(ERROR) << "[SDK COOKIE] fail to open " << cookie_file;
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }
    RecordWriter record_writer;
    record_writer.Reset(&fs);
    if (!record_writer.WriteMessage(cookie)) {
        LOG(ERROR) << "[SDK COOKIE] fail to write cookie file " << cookie_file;
        fs.Close();
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }
    fs.Close();

    CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
    LOG(INFO) << "[SDK COOKIE] update local cookie success: " << cookie_file;
}

void TableImpl::DumpCookie() {
    DoDumpCookie();
    boost::function<void ()> closure = boost::bind(&TableImpl::DumpCookie, this);
    _thread_pool->DelayTask(FLAGS_tera_sdk_cookie_update_interval * 1000, closure);
}

void TableImpl::EnableCookieUpdateTimer() {
    boost::function<void ()> closure = boost::bind(&TableImpl::DumpCookie, this);
    _thread_pool->DelayTask(FLAGS_tera_sdk_cookie_update_interval * 1000, closure);
}

// copy from leveldb/util/hash.h
static uint32_t Hash(const char* data, size_t n, uint32_t seed) {
  // Similar to murmur hash
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = seed ^ (n * m);

  // Pick up four bytes at a time
  while (data + 4 <= limit) {
    uint32_t w = *(uint32_t*)data;
    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  // Pick up remaining bytes
  switch (limit - data) {
    case 3:
      h += data[2] << 16;
    case 2:
      h += data[1] << 8;
    case 1:
      h += data[0];
      h *= m;
      h ^= (h >> r);
      break;
  }
  return h;
}

std::string TableImpl::GetCookieFileName(const std::string& tablename,
                                         const std::string& zk_addr,
                                         const std::string& zk_path) {
    uint32_t hash = 0;
    hash = Hash(tablename.data(), tablename.size(), hash);
    hash = Hash(zk_addr.data(), zk_addr.size(), hash);
    hash = Hash(zk_path.data(), zk_path.size(), hash);

    char hash_str[8] = {'\0'};
    sprintf(hash_str, "%08x", hash);
    return std::string((char*)hash_str, 8);
}
} // namespace tera
