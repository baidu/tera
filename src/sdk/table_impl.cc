// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "table_impl.h"

#include <errno.h>
#include <fcntl.h>
#include <functional>
#include <math.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <iomanip>
#include <fstream>
#include <sstream>

#include <gflags/gflags.h>

#include "common/base/string_format.h"
#include "common/file/file_path.h"
#include "common/file/recordio/record_io.h"
#include "io/coding.h"
#include "proto/kv_helper.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode_client.h"
#include "sdk/cookie.h"
#include "sdk/mutate_impl.h"
#include "sdk/read_impl.h"
#include "sdk/single_row_txn.h"
#include "sdk/scan_impl.h"
#include "sdk/schema_impl.h"
#include "sdk/sdk_zk.h"
#include "tera.h"
#include "utils/crypt.h"
#include "utils/string_util.h"
#include "common/timer.h"

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
DECLARE_int32(tera_rpc_timeout_period);

using namespace std::placeholders;

namespace tera {

TableImpl::TableImpl(const std::string& table_name,
                     common::ThreadPool* thread_pool,
                     sdk::ClusterFinder* cluster)
    : name_(table_name),
      create_time_(0),
      last_sequence_id_(0),
      timeout_(FLAGS_tera_sdk_timeout),
      commit_size_(FLAGS_tera_sdk_batch_size),
      write_commit_timeout_(FLAGS_tera_sdk_write_send_interval),
      read_commit_timeout_(FLAGS_tera_sdk_read_send_interval),
      max_commit_pending_num_(FLAGS_tera_sdk_max_mutation_pending_num),
      max_reader_pending_num_(FLAGS_tera_sdk_max_reader_pending_num),
      meta_cond_(&meta_mutex_),
      meta_updating_count_(0),
      table_meta_cond_(&table_meta_mutex_),
      table_meta_updating_(false),
      task_pool_(thread_pool),
      master_client_(NULL),
      tabletnode_client_(NULL),
      thread_pool_(thread_pool),
      cluster_(cluster),
      cluster_private_(false),
      pending_timeout_ms_(FLAGS_tera_rpc_timeout_period) {
    if (cluster_ == NULL) {
        cluster_ = sdk::NewClusterFinder();
        cluster_private_ = true;
    }
}

TableImpl::~TableImpl() {
    ClearDelayTask();
    if (FLAGS_tera_sdk_cookie_enabled) {
        DoDumpCookie();
    }
    if (cluster_private_) {
        delete cluster_;
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

void TableImpl::Put(RowMutation* row_mu) {
    ApplyMutation(row_mu);
}

void TableImpl::Put(const std::vector<RowMutation*>& row_mutations) {
    ApplyMutation(row_mutations);
}

void OpStatCallback(Table* table, SdkTask* task) {
    if (task->Type() == SdkTask::MUTATION) {
        ((TableImpl*)table)->StatUserPerfCounter(task->Type(),
                                      ((RowMutationImpl*)task)->GetError().GetType(),
                                      get_micros() - ((RowMutationImpl*)task)->GetStartTime());
    } else if (task->Type() == SdkTask::READ) {
        ((TableImpl*)table)->StatUserPerfCounter(task->Type(),
                                      ((RowReaderImpl*)task)->GetError().GetType(),
                                      get_micros() - ((RowReaderImpl*)task)->GetStartTime());
    }
}

void TableImpl::ApplyMutation(RowMutation* row_mu) {
    perf_counter_.user_mu_cnt.Add(1);
    ((RowMutationImpl*)row_mu)->Prepare(OpStatCallback);
    if (row_mu->GetError().GetType() != ErrorCode::kOK) { // local check fail
        if (!((RowMutationImpl*)row_mu)->IsAsync()) {
            ((RowMutationImpl*)row_mu)->RunCallback();
            return;
        }
        ThreadPool::Task task =
            std::bind(&RowMutationImpl::RunCallback,
                      static_cast<RowMutationImpl*>(row_mu));
        thread_pool_->AddTask(task);
        return;
    }
    std::vector<SdkTask*> task_list;
    task_list.push_back(static_cast<SdkTask*>((RowMutationImpl*)row_mu));
    int64_t ts = get_micros();
    DistributeTasks(task_list, true, SdkTask::MUTATION);
    perf_counter_.hist_async_cost.Add(get_micros() - ts);
}

void TableImpl::ApplyMutation(const std::vector<RowMutation*>& row_mutations) {
    std::vector<SdkTask*> task_list;
    for (uint32_t i = 0; i < row_mutations.size(); i++) {
        perf_counter_.user_mu_cnt.Add(1);
        ((RowMutationImpl*)row_mutations[i])->Prepare(OpStatCallback);
        if (row_mutations[i]->GetError().GetType() != ErrorCode::kOK) { // local check fail
            if (!((RowMutationImpl*)row_mutations[i])->IsAsync()) {
                ((RowMutationImpl*)row_mutations[i])->RunCallback();
                continue;
            }
            ThreadPool::Task task =
                std::bind(&RowMutationImpl::RunCallback,
                          static_cast<RowMutationImpl*>(row_mutations[i]));
            thread_pool_->AddTask(task);
            continue;
        }
        task_list.push_back(static_cast<SdkTask*>((RowMutationImpl*)row_mutations[i]));
    }
    int64_t ts = get_micros();
    DistributeTasks(task_list, true, SdkTask::MUTATION);
    perf_counter_.hist_async_cost.Add(get_micros() - ts);
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
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    int64_t timestamp, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, timestamp, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    int32_t ttl, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, value, ttl);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Put(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, const std::string& value,
                    int64_t timestamp, int32_t ttl, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Put(family, qualifier, timestamp, value, ttl);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Add(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t delta, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Add(family, qualifier, delta);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::AddInt64(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t delta, ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->AddInt64(family, qualifier, delta);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::PutIfAbsent(const std::string& row_key, const std::string& family,
                            const std::string& qualifier, const std::string& value,
                            ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->PutIfAbsent(family, qualifier, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
    return (err->GetType() == ErrorCode::kOK ? true : false);
}

bool TableImpl::Append(const std::string& row_key, const std::string& family,
                       const std::string& qualifier, const std::string& value,
                       ErrorCode* err) {
    RowMutation* row_mu = NewRowMutation(row_key);
    row_mu->Append(family, qualifier, value);
    ApplyMutation(row_mu);
    *err = row_mu->GetError();
    delete row_mu;
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
    perf_counter_.user_read_cnt.Add(1);
    ((RowReaderImpl*)row_reader)->Prepare(OpStatCallback);
    std::vector<RowReaderImpl*> row_reader_list;
    row_reader_list.push_back(static_cast<RowReaderImpl*>(row_reader));
    DistributeReaders(row_reader_list, true);
}

void TableImpl::Get(const std::vector<RowReader*>& row_readers) {
    std::vector<RowReaderImpl*> row_reader_list(row_readers.size());
    for (uint32_t i = 0; i < row_readers.size(); ++i) {
        perf_counter_.user_read_cnt.Add(1);
        ((RowReaderImpl*)row_readers[i])->Prepare(OpStatCallback);
        row_reader_list[i] = static_cast<RowReaderImpl*>(row_readers[i]);
    }
    DistributeReaders(row_reader_list, true);
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    ErrorCode* err) {
    return Get(row_key, family, qualifier, value, 0, err);
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, std::string* value,
                    ErrorCode* err) {
    return Get(row_key, family, qualifier, value, 0, err);
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    ErrorCode* err, uint64_t snapshot_id) {
    return Get(row_key, family, qualifier, value, snapshot_id, err);
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    uint64_t snapshot_id, ErrorCode* err) {
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
    return Get(row_key, family, qualifier, value, snapshot_id, err);
}

bool TableImpl::Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, std::string* value,
                    uint64_t snapshot_id, ErrorCode* err) {
    RowReader* row_reader = NewRowReader(row_key);
    row_reader->AddColumn(family, qualifier);
    row_reader->SetSnapshot(snapshot_id);
    Get(row_reader);
    *err = row_reader->GetError();
    if (err->GetType() == ErrorCode::kOK) {
        *value = row_reader->Value();
        delete row_reader;
        return true;
    }
    delete row_reader;
    return false;
}

ResultStream* TableImpl::Scan(const ScanDescriptor& desc, ErrorCode* err) {
    ScanDescImpl * impl = desc.GetImpl();
    impl->SetTableSchema(table_schema_);
    ResultStream * results = NULL;
    if (desc.IsAsync() &&
        (table_schema_.raw_key() == Binary || table_schema_.raw_key() == Readable)) {
        VLOG(6) << "activate async-scan";
        results = new ResultStreamBatchImpl(this, impl);
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
        scan_task->SetId(next_task_id_.Inc());
        task_pool_.PutTask(scan_task);
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
    request->set_sequence_id(last_sequence_id_++);
    request->set_table_name(name_);
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
    request->set_max_qualifiers(impl->GetMaxQualifiers());
    if (impl->GetBufferSize() != 0) {
        request->set_buffer_limit(impl->GetBufferSize());
    }
    if (impl->GetNumberLimit() != 0) {
        request->set_number_limit(impl->GetNumberLimit());
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

    VLOG(20) << "table " << request->table_name()
        << ", start_key " << request->start()
        << ", end_key " << request->end()
        << ", scan to " << server_addr;
    request->set_timestamp(get_micros());
    std::function<void (ScanTabletRequest*, ScanTabletResponse*, bool, int)> done =
        std::bind(&TableImpl::ScanCallBack, this, scan_task, _1, _2, _3, _4);
    tabletnode_client.ScanTablet(request, response, done);
}

void TableImpl::ScanCallBack(ScanTask* scan_task,
                             ScanTabletRequest* request,
                             ScanTabletResponse* response,
                             bool failed, int error_code) {
    perf_counter_.rpc_s.Add(get_micros() - request->timestamp());
    perf_counter_.rpc_s_cnt.Inc();
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
        VLOG(10) << "fail to scan table: " << name_
            << " errcode: " << StatusCodeToString(err);
    }

    scan_task->SetInternalError(err);
    if (err == kTabletNodeOk ||
        err == kSnapshotNotExist ||
        stream->GetScanDesc()->IsAsync() || // batch scan retry internal
        scan_task->RetryTimes() >= static_cast<uint32_t>(FLAGS_tera_sdk_retry_times)) {
        if (err == kKeyNotInRange || err == kConnectError) {
            ScheduleUpdateMeta(stream->GetScanDesc()->GetStartRowKey(),
                               scan_task->GetMetaTimeStamp());
        }
        stream->OnFinish(request, response);
        stream->ReleaseRpcHandle(request, response);
        task_pool_.PopTask(scan_task->GetId());
        CHECK_EQ(scan_task->GetRef(), 2);
        delete scan_task;
    } else {
        scan_task->IncRetryTimes();
        ThreadPool::Task retry_task =
            std::bind((void (TableImpl::*)(ScanTask*, bool))&TableImpl::ScanTabletAsync,
                      this, scan_task, false);
        CHECK(scan_task->RetryTimes() > 0);
        int64_t retry_interval =
            static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal,
                                     scan_task->RetryTimes() - 1) * 1000);
        thread_pool_->DelayTask(retry_interval, retry_task);
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
    LOG(INFO) << "open table " << name_ << " at cluster " << cluster_->ClusterId();
    return true;
}

void TableImpl::DistributeTasks(const std::vector<SdkTask*>& task_list,
                                bool called_by_user,
                                SdkTask::TYPE task_type) {
    typedef std::map<std::string, std::vector<SdkTask*> > TsTaskMap;
    TsTaskMap ts_task_list;
    int64_t sync_min_timeout = -1;
    std::vector<SdkTask*> sync_task_list;

    int64_t max_pending_counter;
    Counter* task_cnt = NULL;
    Counter* pending_counter = NULL;
    SdkTask::TimeoutFunc timeout_task;
    std::string err_reason;
    if (task_type == SdkTask::MUTATION) {
        task_cnt = &(perf_counter_.mutate_cnt);
        pending_counter = &(cur_commit_pending_counter_);
        max_pending_counter = max_commit_pending_num_;
        err_reason = "pending too much mutations, try it later.";
        timeout_task = std::bind(&TableImpl::MutationTimeout, this, _1);
    } else if (task_type == SdkTask::READ) {
        task_cnt = &(perf_counter_.reader_cnt);
        pending_counter = &(cur_reader_pending_counter_);
        max_pending_counter = max_reader_pending_num_;
        err_reason = "pending too much readers, try it later.";
        timeout_task = std::bind(&TableImpl::ReaderTimeout, this, _1);
    } else {
        assert(0);
    }

    for (uint32_t i = 0; called_by_user && i < task_list.size(); i++) {
        SdkTask* task = (SdkTask*)task_list[i];
        if (!task->IsAsync()) {
            sync_task_list.push_back(task);
            int64_t task_timeout = task->TimeOut() > 0 ? task->TimeOut() : timeout_;
            if (task_timeout > 0 && (sync_min_timeout <= 0 || sync_min_timeout > task_timeout)) {
                sync_min_timeout = task_timeout;
            }
        }
    }

    for (uint32_t i = 0; i < task_list.size(); i++) {
        SdkTask* task = (SdkTask*)task_list[i];
        task_cnt->Inc();
        if (called_by_user) {
            task->SetId(next_task_id_.Inc());

            int64_t task_timeout = -1;
            if (!task->IsAsync()) {
                task_timeout = sync_min_timeout;
            } else {
                task_timeout = task->TimeOut() > 0 ? task->TimeOut() : timeout_;
            }
            perf_counter_.total_task_cnt.Inc();
            task_pool_.PutTask(task, task_timeout, timeout_task);
        }

        // flow control
        if (called_by_user
            && pending_counter->Inc() > max_pending_counter
            && task->IsAsync()) {
            if (FLAGS_tera_sdk_async_blocking_enabled) {
                while (pending_counter->Get() > max_pending_counter) {
                    usleep(100000);
                }
            } else {
                pending_counter->Dec();
                task->SetError(ErrorCode::kBusy, err_reason);
                ThreadPool::Task break_task =
                    std::bind(&TableImpl::BreakRequest, this, task->GetId());
                task->DecRef();
                thread_pool_->AddTask(break_task);
                continue;
            }
        }

        std::string server_addr;
        if (!GetTabletAddrOrScheduleUpdateMeta(task->RowKey(),
                                               task, &server_addr)) {
            perf_counter_.meta_sched_cnt.Inc();
            continue;
        }
        ts_task_list[server_addr].push_back(task);
    }

    TsTaskMap::iterator it = ts_task_list.begin();
    for (; it != ts_task_list.end(); ++it) {
        PackSdkTasks(it->first, it->second, task_type);
    }

    // 从现在开始，所有异步的row_mutation都不可以再操作了，因为随时会被用户释放
    // 不是用户调用的，立即返回
    if (!called_by_user) {
        return;
    }

    // 等待同步操作返回或超时
    for (uint32_t i = 0; i < sync_task_list.size(); i++) {
        while (pending_counter->Get() > max_pending_counter) {
            usleep(100000);
        }
        SdkTask* task = (SdkTask*)sync_task_list[i];
        task->Wait();
    }
}

void TableImpl::CommitMutations(const std::string& server_addr,
                                std::vector<RowMutationImpl*>& mu_list) {
    tabletnode::TabletNodeClient tabletnode_client_async(server_addr);
    WriteTabletRequest* request = new WriteTabletRequest;
    WriteTabletResponse* response = new WriteTabletResponse;
    request->set_sequence_id(last_sequence_id_++);
    request->set_tablet_name(name_);
    request->set_is_sync(FLAGS_tera_sdk_write_sync);

    bool is_instant = false;
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
        SingleRowTxn* txn = (SingleRowTxn*)(row_mutation->GetTransaction());
        if (txn != NULL) {
            txn->Serialize(mu_seq);
        }
        mu_id_list->push_back(row_mutation->GetId());
        is_instant |= !row_mutation->IsAsync();
        row_mutation->AddCommitTimes();
        row_mutation->DecRef();
    }
    request->set_is_instant(is_instant);

    VLOG(20) << "commit " << mu_list.size() << " mutations to " << server_addr;
    request->set_timestamp(get_micros());
    std::function<void (WriteTabletRequest*, WriteTabletResponse*, bool, int)> done =
        std::bind(&TableImpl::MutateCallBack, this, mu_id_list, _1, _2, _3, _4);
    tabletnode_client_async.WriteTablet(request, response, done);
}

void TableImpl::MutateCallBack(std::vector<int64_t>* mu_id_list,
                               WriteTabletRequest* request,
                               WriteTabletResponse* response,
                               bool failed, int error_code) {
    perf_counter_.rpc_w.Add(get_micros() - request->timestamp());
    perf_counter_.rpc_w_cnt.Inc();
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
    std::vector<SdkTask*> not_in_range_list;
    for (uint32_t i = 0; i < mu_id_list->size(); ++i) {
        const std::string& row = request->row_list(i).row_key();
        int64_t mu_id = (*mu_id_list)[i];
        StatusCode err = response->status();
        if (err == kTabletNodeOk) {
            err = response->row_status_list(i);
        }

        if (err == kTabletNodeOk || err == kTxnFail || err == kTableInvalidArg) {
            perf_counter_.mutate_ok_cnt.Inc();
            SdkTask* task = task_pool_.PopTask(mu_id);
            if (task == NULL) {
                VLOG(10) << "mutation " << mu_id << " finish but timeout: " << DebugString(row);
                continue;
            }
            CHECK_EQ(task->Type(), SdkTask::MUTATION);
            CHECK_EQ(task->GetRef(), 1);
            RowMutationImpl* row_mutation = (RowMutationImpl*)task;
            if (err == kTabletNodeOk) {
                row_mutation->SetError(ErrorCode::kOK);
            } else if (err == kTxnFail) {
                row_mutation->SetError(ErrorCode::kTxnFail, "transaction commit fail");
            } else {
                row_mutation->SetError(ErrorCode::kBadParam, "illegal arg error");
            }

            // only for flow control
            cur_commit_pending_counter_.Dec();
            int64_t perf_time = get_micros();
            row_mutation->RunCallback();
            perf_counter_.user_callback.Add(get_micros() - perf_time);
            perf_counter_.user_callback_cnt.Inc();
            continue;
        }
        perf_counter_.mutate_fail_cnt.Inc();

        VLOG(10) << "fail to mutate table: " << name_
            << " row: " << DebugString(row)
            << " errcode: " << StatusCodeToString(err);

        SdkTask* task = task_pool_.GetTask(mu_id);
        if (task == NULL) {
            VLOG(10) << "mutation " << mu_id << " timeout: " << DebugString(row);
            continue;
        }
        CHECK_EQ(task->Type(), SdkTask::MUTATION);
        RowMutationImpl* row_mutation = (RowMutationImpl*)task;
        row_mutation->SetInternalError(err);

        if (err == kKeyNotInRange) {
            perf_counter_.mutate_range_cnt.Inc();
            row_mutation->IncRetryTimes();
            not_in_range_list.push_back(task);
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
        DistributeTasks(not_in_range_list, false, SdkTask::MUTATION);
    }
    std::map<uint32_t, std::vector<int64_t>* >::iterator it;
    for (it = retry_times_list.begin(); it != retry_times_list.end(); ++it) {
        int64_t retry_interval =
            static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal, it->first) * 1000);
        ThreadPool::Task retry_task =
            std::bind(&TableImpl::DistributeMutationsById, this, it->second);
        thread_pool_->DelayTask(retry_interval, retry_task);
    }

    delete request;
    delete response;
    delete mu_id_list;
}

void TableImpl::DistributeMutationsById(std::vector<int64_t>* mu_id_list) {
    std::vector<SdkTask*> task_list;
    for (uint32_t i = 0; i < mu_id_list->size(); ++i) {
        int64_t mu_id = (*mu_id_list)[i];
        SdkTask* task = task_pool_.GetTask(mu_id);
        if (task == NULL) {
            VLOG(10) << "mutation " << mu_id << " timeout when retry mutate";;
            continue;
        }
        CHECK_EQ(task->Type(), SdkTask::MUTATION);
        task_list.push_back(task);
    }
    DistributeTasks(task_list, false, SdkTask::MUTATION);
    delete mu_id_list;
}

void TableImpl::MutationTimeout(SdkTask* task) {
    perf_counter_.mutate_timeout_cnt.Inc();
    CHECK_NOTNULL(task);
    CHECK_EQ(task->Type(), SdkTask::MUTATION);

    RowMutationImpl* row_mutation = (RowMutationImpl*)task;
    row_mutation->ExcludeOtherRef();

    StatusCode err = row_mutation->GetInternalError();
    if (err == kKeyNotInRange || err == kConnectError) {
        ScheduleUpdateMeta(row_mutation->RowKey(),
                           row_mutation->GetMetaTimeStamp());
    }

    std::string err_reason;
    if (row_mutation->RetryTimes() == 0) {
        perf_counter_.mutate_queue_timeout_cnt.Inc();
        err_reason = StringFormat("commit %lld times, retry 0 times, in %u ms.",
                                  row_mutation->GetCommitTimes(), timeout_);
    } else {
        err_reason = StringFormat("commit %lld times, retry %u times, in %u ms. last error: %s",
                                  row_mutation->GetCommitTimes(), row_mutation->RetryTimes(),
                                  timeout_, StatusCodeToString(err).c_str());
    }
    row_mutation->SetError(ErrorCode::kTimeout, err_reason);
    // only for flow control
    cur_commit_pending_counter_.Dec();
    int64_t perf_time = get_micros();
    row_mutation->RunCallback();
    perf_counter_.user_callback.Add(get_micros() - perf_time);
    perf_counter_.user_callback_cnt.Inc();
}

void TableImpl::DistributeReaders(const std::vector<RowReaderImpl*>& row_reader_list,
                                  bool called_by_user) {
    std::vector<SdkTask*> task_list;
    for (size_t i = 0; i < row_reader_list.size(); ++i) {
        task_list.push_back((SdkTask*)(row_reader_list[i]));
    }
    DistributeTasks(task_list, called_by_user, SdkTask::READ);
}

void TableImpl::CommitReaders(const std::string server_addr,
                              std::vector<RowReaderImpl*>& reader_list) {
    std::vector<int64_t>* reader_id_list = new std::vector<int64_t>;
    tabletnode::TabletNodeClient tabletnode_client_async(server_addr);
    ReadTabletRequest* request = new ReadTabletRequest;
    ReadTabletResponse* response = new ReadTabletResponse;
    request->set_sequence_id(last_sequence_id_++);
    request->set_tablet_name(name_);
    request->set_client_timeout_ms(pending_timeout_ms_);
    for (uint32_t i = 0; i < reader_list.size(); ++i) {
        RowReaderImpl* row_reader = reader_list[i];
        RowReaderInfo* row_reader_info = request->add_row_info_list();
        request->set_snapshot_id(row_reader->GetSnapshot());
        row_reader->ToProtoBuf(row_reader_info);
        // row_reader_info->CopyFrom(row_reader->GetRowReaderInfo());
        reader_id_list->push_back(row_reader->GetId());
        row_reader->AddCommitTimes();
        row_reader->DecRef();
    }
    VLOG(20) << "commit " << reader_list.size() << " reads to " << server_addr;
    request->set_timestamp(get_micros());
    std::function<void (ReadTabletRequest*, ReadTabletResponse*, bool, int)> done =
        std::bind(&TableImpl::ReaderCallBack, this, reader_id_list, _1, _2, _3, _4);
    tabletnode_client_async.ReadTablet(request, response, done);
}

void TableImpl::ReaderCallBack(std::vector<int64_t>* reader_id_list,
                               ReadTabletRequest* request,
                               ReadTabletResponse* response,
                               bool failed, int error_code) {
    perf_counter_.rpc_r.Add(get_micros() - request->timestamp());
    perf_counter_.rpc_r_cnt.Inc();
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
    uint32_t row_result_index = 0;
    for (uint32_t i = 0; i < reader_id_list->size(); ++i) {
        int64_t reader_id = (*reader_id_list)[i];

        StatusCode err = response->status();
        if (err == kTabletNodeOk) {
            err = response->detail().status(i);
        }
        if (err == kTabletNodeOk || err == kKeyNotExist || err == kSnapshotNotExist) {
            perf_counter_.reader_ok_cnt.Inc();
            SdkTask* task = task_pool_.PopTask(reader_id);
            if (task == NULL) {
                VLOG(10) << "reader " << reader_id << " success but timeout";
                if (err == kTabletNodeOk) {
                    // result is timeout, discard it
                    row_result_index++;
                }
                continue;
            }
            CHECK_EQ(task->Type(), SdkTask::READ);
            CHECK_EQ(task->GetRef(), 1);

            RowReaderImpl* row_reader = (RowReaderImpl*)task;
            if (err == kTabletNodeOk) {
                row_reader->SetResult(response->detail().row_result(row_result_index++));
                row_reader->SetError(ErrorCode::kOK);
            } else if (err == kKeyNotExist) {
                row_reader->SetError(ErrorCode::kNotFound, "not found");
            } else { // err == kSnapshotNotExist
                row_reader->SetError(ErrorCode::kNotFound, "snapshot not found");
            }
            int64_t perf_time = get_micros();
            row_reader->RunCallback();
            perf_counter_.user_callback.Add(get_micros() - perf_time);
            perf_counter_.user_callback_cnt.Inc();
            // only for flow control
            cur_reader_pending_counter_.Dec();
            continue;
        }
        perf_counter_.reader_fail_cnt.Inc();

        VLOG(10) << "fail to read table: " << name_
            << " errcode: " << StatusCodeToString(err);

        SdkTask* task = task_pool_.GetTask(reader_id);
        if (task == NULL) {
            VLOG(10) << "reader " << reader_id << " fail but timeout";
            continue;
        }
        CHECK_EQ(task->Type(), SdkTask::READ);
        RowReaderImpl* row_reader = (RowReaderImpl*)task;
        row_reader->SetInternalError(err);

        if (err == kKeyNotInRange) {
            perf_counter_.reader_range_cnt.Inc();
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
            std::bind(&TableImpl::DistributeReadersById, this, it->second);
        thread_pool_->DelayTask(retry_interval, retry_task);
    }

    delete request;
    delete response;
    delete reader_id_list;
}

void TableImpl::DistributeReadersById(std::vector<int64_t>* reader_id_list) {
    std::vector<RowReaderImpl*> reader_list;
    for (size_t i = 0; i < reader_id_list->size(); ++i) {
        int64_t reader_id = (*reader_id_list)[i];
        SdkTask* task = task_pool_.GetTask(reader_id);
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

void TableImpl::ReaderTimeout(SdkTask* task) {
    perf_counter_.reader_timeout_cnt.Inc();
    CHECK_NOTNULL(task);
    CHECK_EQ(task->Type(), SdkTask::READ);

    RowReaderImpl* row_reader = (RowReaderImpl*)task;
    row_reader->ExcludeOtherRef();

    StatusCode err = row_reader->GetInternalError();
    if (err == kKeyNotInRange || err == kConnectError) {
        ScheduleUpdateMeta(row_reader->RowName(),
                           row_reader->GetMetaTimeStamp());
    }

    std::string err_reason;
    if (row_reader->RetryTimes() == 0) {
        perf_counter_.reader_queue_timeout_cnt.Inc();
        err_reason = StringFormat("commit %lld times, retry 0 times, in %u ms.",
                                  row_reader->GetCommitTimes(), timeout_);
    } else {
        err_reason = StringFormat("commit %lld times, retry %u times, in %u ms. last error: %s",
                                  row_reader->GetCommitTimes(),  row_reader->RetryTimes(),
                                  timeout_, StatusCodeToString(err).c_str());
    }
    row_reader->SetError(ErrorCode::kTimeout, err_reason);
    int64_t perf_time = get_micros();
    row_reader->RunCallback();
    perf_counter_.user_callback.Add(get_micros() - perf_time);
    perf_counter_.user_callback_cnt.Inc();
    // only for flow control
    cur_reader_pending_counter_.Dec();
}

void TableImpl::PackSdkTasks(const std::string& server_addr,
                             std::vector<SdkTask*>& task_list,
                             SdkTask::TYPE task_type) {
    Mutex* mutex = NULL;
    std::map<std::string, TaskBatch*>* task_batch_map = NULL;
    SdkTask::TimeoutFunc task;
    uint64_t commit_timeout = 10000;
    uint32_t commit_size = commit_size_;
    if (task_type == SdkTask::MUTATION) {
        mutex = &mutation_batch_mutex_;
        task_batch_map = &mutation_batch_map_;
        commit_timeout = write_commit_timeout_;
    } else if (task_type == SdkTask::READ) {
        mutex = &reader_batch_mutex_;
        task_batch_map = &reader_batch_map_;
        commit_timeout = read_commit_timeout_;
    } else {
        assert(0);
    }

    TaskBatch* task_batch = NULL;
    bool is_instant = false;
    MutexLock lock(mutex);
    for (size_t i = 0; i < task_list.size(); ++i) {
        // find existing batch or create a new batch
        if (task_batch == NULL) {
            std::map<std::string, TaskBatch*>::iterator it = task_batch_map->find(server_addr);
            if (it != task_batch_map->end()) {
                task_batch = it->second;
            } else {
                task_batch = new TaskBatch;
                task_batch->type = task_type;
                task_batch->mutex = mutex;
                task_batch->task_batch_map = task_batch_map;
                task_batch->byte_size = 0;
                task_batch->server_addr = server_addr;
                task_batch->row_id_list = new std::vector<int64_t>;

                task_batch->SetId(next_task_id_.Inc());
                (*task_batch_map)[server_addr] = task_batch;
                SdkTask::TimeoutFunc task = std::bind(&TableImpl::TaskBatchTimeout, this, _1);
                task_pool_.PutTask(task_batch, commit_timeout, task);
                task_batch->DecRef();
            }
        }

        // put task into the batch
        SdkTask* sdk_task = task_list[i];
        task_batch->row_id_list->push_back(sdk_task->GetId());
        task_batch->byte_size += sdk_task->Size();
        is_instant |= !sdk_task->IsAsync();
        sdk_task->DecRef();

        // commit the batch if:
        // 1) batch_byte_size >= max_rpc_byte_size
        // for the *LAST* batch, commit it if:
        // 2) any mutation is sync (flush == true)
        // 3) batch_row_num >= min_batch_row_num
        // 4) commit timeout
        if (task_batch->byte_size >= kMaxRpcSize ||
            ((i == task_list.size() - 1) &&
             (is_instant ||
              (task_batch->row_id_list->size() >= commit_size)))) {
            std::vector<int64_t>* task_id_list = task_batch->row_id_list;
            task_batch->row_id_list = NULL;
            task_batch_map->erase(server_addr);
            mutex->Unlock();

            CommitTasksById(server_addr, *task_id_list, task_type);
            delete task_id_list;
            task_batch = NULL;
            is_instant = false;
            mutex->Lock();
        }
    }
}

void TableImpl::TaskBatchTimeout(SdkTask* task) {
    std::vector<int64_t>* task_id_list = NULL;
    CHECK_NOTNULL(task);
    CHECK_EQ(task->Type(), SdkTask::TASKBATCH);
    TaskBatch* task_batch = (TaskBatch*)task;
    task_batch->ExcludeOtherRef();

    const std::string& server_addr = task_batch->server_addr;
    SdkTask::TYPE task_type = task_batch->type;
    Mutex* mutex = task_batch->mutex;
    std::map<std::string, TaskBatch*>* task_batch_map = task_batch->task_batch_map;
    {
        MutexLock lock(mutex);
        std::map<std::string, TaskBatch*>::iterator it =
            task_batch_map->find(server_addr);
        if (it != task_batch_map->end() &&
            task_batch->GetId() == it->second->GetId()) {
            task_id_list = task_batch->row_id_list;
            task_batch->row_id_list = NULL;
            task_batch_map->erase(it);
        }
    }

    if (task_id_list != NULL) {
        CommitTasksById(server_addr, *task_id_list, task_type);
        delete task_id_list;
    }
    delete task_batch;
}

void TableImpl::CommitTasksById(const std::string& server_addr,
                                std::vector<int64_t>& task_id_list,
                                SdkTask::TYPE task_type) {
    std::vector<RowMutationImpl*> mutation_list;
    std::vector<RowReaderImpl*> reader_list;

    for (size_t i = 0; i < task_id_list.size(); i++) {
        int64_t task_id = task_id_list[i];
        SdkTask* task = task_pool_.GetTask(task_id);
        if (task == NULL) {
            VLOG(10) << "commit task, type " << task_type << ", id " << task_id << " timeout";
            continue;
        }
        perf_counter_.total_commit_cnt.Inc();
        CHECK_EQ(task->Type(), task_type);
        if (task_type == SdkTask::MUTATION) {
            mutation_list.push_back((RowMutationImpl*)task);
        } else if (task_type == SdkTask::READ) {
            reader_list.push_back((RowReaderImpl*)task);
        }
    }
    if (task_type == SdkTask::MUTATION) {
        CommitMutations(server_addr, mutation_list);
    } else if (task_type == SdkTask::READ) {
        CommitReaders(server_addr, reader_list);
    }
}

bool TableImpl::GetTabletMetaForKey(const std::string& key, TabletMeta* meta) {
    MutexLock lock(&meta_mutex_);
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
    MutexLock lock(&meta_mutex_);
    TabletMetaNode* node = GetTabletMetaNodeForKey(row);
    if (node == NULL) {
        VLOG(10) << "no meta for key: " << row;
        pending_task_id_list_[row].push_back(task->GetId());
        task->DecRef();
        TabletMetaNode& new_node = tablet_meta_list_[row];
        new_node.meta.mutable_key_range()->set_key_start(row);
        new_node.meta.mutable_key_range()->set_key_end(row + '\0');
        new_node.status = WAIT_UPDATE;
        UpdateMetaAsync();
        return false;
    }
    if (node->status != NORMAL) {
        VLOG(10) << "abnormal meta for key: " << row;
        pending_task_id_list_[row].push_back(task->GetId());
        task->DecRef();
        return false;
    }
    if ((task->GetInternalError() == kKeyNotInRange || task->GetInternalError() == kConnectError)
            && task->GetMetaTimeStamp() >= node->update_time) {
        pending_task_id_list_[row].push_back(task->GetId());
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
                std::bind(&TableImpl::DelayUpdateMeta, this,
                          node->meta.key_range().key_start(),
                          node->meta.key_range().key_end());
            thread_pool_->DelayTask(update_interval, delay_task);
        }
        return false;
    }
    CHECK_EQ(node->status, NORMAL);
    task->SetMetaTimeStamp(node->update_time);
    *server_addr = node->meta.server_addr();
    return true;
}

TableImpl::TabletMetaNode* TableImpl::GetTabletMetaNodeForKey(const std::string& key) {
    meta_mutex_.AssertHeld();
    if (tablet_meta_list_.size() == 0) {
        VLOG(10) << "the meta list is empty";
        return NULL;
    }
    std::map<std::string, TabletMetaNode>::iterator it =
        tablet_meta_list_.upper_bound(key);
    if (it == tablet_meta_list_.begin()) {
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
    MutexLock lock(&meta_mutex_);
    std::map<std::string, TabletMetaNode>::iterator it =
            tablet_meta_list_.lower_bound(start_key);
    for (; it != tablet_meta_list_.end(); ++it) {
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
    meta_mutex_.AssertHeld();
    if (meta_updating_count_ >= static_cast<uint32_t>(FLAGS_tera_sdk_update_meta_concurrency)) {
        return;
    }
    bool need_update = false;
    std::string update_start_key;
    std::string update_end_key;
    std::string update_expand_end_key; // update more tablet than need
    std::map<std::string, TabletMetaNode>::iterator it = tablet_meta_list_.begin();
    for (; it != tablet_meta_list_.end(); ++it) {
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
    meta_updating_count_++;
    ScanMetaTableAsync(update_start_key, update_end_key, update_expand_end_key, false);
}

void TableImpl::ScanMetaTable(const std::string& key_start,
                              const std::string& key_end) {
    MutexLock lock(&meta_mutex_);
    meta_updating_count_++;
    ScanMetaTableAsync(key_start, key_end, key_end, false);
    while (meta_updating_count_ > 0) {
        meta_cond_.Wait();
    }
}

void TableImpl::ScanMetaTableAsyncInLock(std::string key_start, std::string key_end,
                                         std::string expand_key_end, bool zk_access) {
    MutexLock lock(&meta_mutex_);
    ScanMetaTableAsync(key_start, key_end, expand_key_end, zk_access);
}

void TableImpl::ScanMetaTableAsync(const std::string& key_start, const std::string& key_end,
                                   const std::string& expand_key_end, bool zk_access) {
    meta_mutex_.AssertHeld();
    CHECK(expand_key_end == "" || expand_key_end >= key_end);

    std::string meta_addr = cluster_->RootTableAddr(zk_access);
    if (meta_addr.empty() && !zk_access) {
        meta_addr = cluster_->RootTableAddr(true);
    }

    if (meta_addr.empty()) {
        VLOG(6) << "root is empty";

        ThreadPool::Task retry_task =
            std::bind(&TableImpl::ScanMetaTableAsyncInLock, this, key_start, key_end,
                      expand_key_end, true);
        thread_pool_->DelayTask(FLAGS_tera_sdk_update_meta_internal, retry_task);
        return;
    }

    VLOG(6) << "root: " << meta_addr;
    tabletnode::TabletNodeClient tabletnode_client_async(meta_addr);
    ScanTabletRequest* request = new ScanTabletRequest;
    ScanTabletResponse* response = new ScanTabletResponse;
    request->set_sequence_id(last_sequence_id_++);
    request->set_table_name(FLAGS_tera_master_meta_table_name);
    MetaTableScanRange(name_, key_start, expand_key_end,
                       request->mutable_start(),
                       request->mutable_end());
    request->set_buffer_limit(FLAGS_tera_sdk_update_meta_buffer_limit);
    request->set_round_down(true);

    std::function<void (ScanTabletRequest*, ScanTabletResponse*, bool, int)> done =
        std::bind(&TableImpl::ScanMetaTableCallBack, this, key_start, key_end,
                  expand_key_end, get_micros(), _1, _2, _3, _4);
    tabletnode_client_async.ScanTablet(request, response, done);
}

void TableImpl::ScanMetaTableCallBack(std::string key_start,
                                      std::string key_end,
                                      std::string expand_key_end,
                                      int64_t start_time,
                                      ScanTabletRequest* request,
                                      ScanTabletResponse* response,
                                      bool failed, int error_code) {
    perf_counter_.get_meta.Add(get_micros() - start_time);
    perf_counter_.get_meta_cnt.Inc();
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
            MutexLock lock(&meta_mutex_);
            GiveupUpdateTabletMeta(key_start, key_end);
        }
        ThreadPool::Task retry_task =
            std::bind(&TableImpl::ScanMetaTableAsyncInLock, this, key_start, key_end,
                      expand_key_end, true);
        thread_pool_->DelayTask(FLAGS_tera_sdk_update_meta_internal, retry_task);
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

        MutexLock lock(&meta_mutex_);
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

    MutexLock lock(&meta_mutex_);
    if (scan_meta_error) {
        ScanMetaTableAsync(key_start, key_end, expand_key_end, false);
    } else if (!return_end.empty() && (key_end.empty() || return_end < key_end)) {
        CHECK(!response->complete());
        ScanMetaTableAsync(return_end, key_end, expand_key_end, false);
    } else {
        meta_updating_count_--;
        meta_cond_.Signal();
        UpdateMetaAsync();
    }
    delete request;
    delete response;
}

void TableImpl::GiveupUpdateTabletMeta(const std::string& key_start,
                                       const std::string& key_end) {
    std::map<std::string, std::list<int64_t> >::iterator ilist =
            pending_task_id_list_.lower_bound(key_start);
    while (ilist != pending_task_id_list_.end()) {
        if (!key_end.empty() && ilist->first >= key_end) {
            break;
        }
        std::list<int64_t>& task_id_list = ilist->second;
        for (std::list<int64_t>::iterator itask = task_id_list.begin();
                itask != task_id_list.end();) {
            int64_t task_id = *itask;
            SdkTask* task = task_pool_.GetTask(task_id);
            if (task == NULL) {
                VLOG(10) << "task " << task_id << " timeout when update meta fail";
                itask = task_id_list.erase(itask);
            } else {
                task->DecRef();
            }
            ++itask;
        }
        if (task_id_list.empty()) {
            pending_task_id_list_.erase(ilist++);
        } else {
            ++ilist;
        }
    }
}

void TableImpl::UpdateTabletMetaList(const TabletMeta& new_meta) {
    meta_mutex_.AssertHeld();
    const std::string& new_start = new_meta.key_range().key_start();
    const std::string& new_end = new_meta.key_range().key_end();
    std::map<std::string, TabletMetaNode>::iterator it =
        tablet_meta_list_.upper_bound(new_start);
    if (tablet_meta_list_.size() > 0 && it != tablet_meta_list_.begin()) {
        --it;
    }
    while (it != tablet_meta_list_.end()) {
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
                TabletMetaNode& copy_node = tablet_meta_list_[new_end];
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
                tablet_meta_list_.erase(tmp);
            } else {
                //*************************************************
                //*                  |-----old------|             *
                //*             |------new------|                 *
                //*************************************************
                VLOG(10) << "meta [" << old_start << ", " << old_end << "] "
                    << "shrink to [" << new_end << ", " << old_end << "]";
                TabletMetaNode& copy_node = tablet_meta_list_[new_end];
                copy_node = old_node;
                copy_node.meta.mutable_key_range()->set_key_start(new_end);
                tablet_meta_list_.erase(tmp);
            }
        } else { // !new_end.empty() && old_start >= new_end
            //*****************************************************
            //*                                   |---old---|     *
            //*                 |------new------|                 *
            //*****************************************************
            break;
        }
    }

    TabletMetaNode& new_node = tablet_meta_list_[new_start];
    new_node.meta.CopyFrom(new_meta);
    new_node.status = NORMAL;
    new_node.update_time = get_micros() / 1000;
    VLOG(10) << "add new meta [" << new_start << ", " << new_end << "]: "
        << new_meta.server_addr();
    WakeUpPendingRequest(new_node);
}

void TableImpl::WakeUpPendingRequest(const TabletMetaNode& node) {
    meta_mutex_.AssertHeld();
    const std::string& start_key = node.meta.key_range().key_start();
    const std::string& end_key = node.meta.key_range().key_end();
    const std::string& server_addr = node.meta.server_addr();
    int64_t meta_timestamp = node.update_time;

    std::vector<SdkTask*> mutation_list;
    std::vector<SdkTask*> reader_list;

    std::map<std::string, std::list<int64_t> >::iterator it =
        pending_task_id_list_.lower_bound(start_key);
    while (it != pending_task_id_list_.end()) {
        if (!end_key.empty() && it->first >= end_key) {
            break;
        }
        std::list<int64_t>& task_id_list = it->second;
        for (std::list<int64_t>::iterator itask = task_id_list.begin();
                itask != task_id_list.end(); ++itask) {
            perf_counter_.meta_update_cnt.Inc();
            int64_t task_id = *itask;
            SdkTask* task = task_pool_.GetTask(task_id);
            if (task == NULL) {
                VLOG(10) << "task " << task_id << " timeout when update meta success";
                continue;
            }
            task->SetMetaTimeStamp(meta_timestamp);

            switch (task->Type()) {
            case SdkTask::READ: {
                reader_list.push_back(task);
            } break;
            case SdkTask::MUTATION: {
                mutation_list.push_back(task);
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
        pending_task_id_list_.erase(tmp);
    }

    if (mutation_list.size() > 0) {
        PackSdkTasks(server_addr, mutation_list, SdkTask::MUTATION);
    }
    if (reader_list.size() > 0) {
        PackSdkTasks(server_addr, reader_list, SdkTask::READ);
    }
}

void TableImpl::ScheduleUpdateMeta(const std::string& row,
                                   int64_t meta_timestamp) {
    MutexLock lock(&meta_mutex_);
    TabletMetaNode* node = GetTabletMetaNodeForKey(row);
    if (node == NULL) {
        TabletMetaNode& new_node = tablet_meta_list_[row];
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
                std::bind(&TableImpl::DelayUpdateMeta, this,
                          node->meta.key_range().key_start(),
                          node->meta.key_range().key_end());
            thread_pool_->DelayTask(update_interval, delay_task);
        }
    }
}

bool TableImpl::UpdateTableMeta(ErrorCode* err) {
    MutexLock lock(&table_meta_mutex_);
    table_meta_updating_ = true;

    table_meta_mutex_.Unlock();
    ReadTableMetaAsync(err, 0, false);
    table_meta_mutex_.Lock();

    while (table_meta_updating_) {
        table_meta_cond_.Wait();
    }
    if (err->GetType() != ErrorCode::kOK) {
        return false;
    }
    return true;
}

void TableImpl::ReadTableMetaAsync(ErrorCode* ret_err, int32_t retry_times,
                                   bool zk_access) {
    std::string meta_server = cluster_->RootTableAddr(zk_access);
    if (meta_server.empty() && !zk_access) {
        meta_server = cluster_->RootTableAddr(true);
    }
    if (meta_server.empty()) {
        VLOG(10) << "root is empty";

        MutexLock lock(&table_meta_mutex_);
        CHECK(table_meta_updating_);
        if (retry_times >= FLAGS_tera_sdk_retry_times) {
            ret_err->SetFailed(ErrorCode::kSystem);
            table_meta_updating_ = false;
            table_meta_cond_.Signal();
        } else {
            int64_t retry_interval =
                static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal, retry_times) * 1000);
            ThreadPool::Task retry_task =
                std::bind(&TableImpl::ReadTableMetaAsync, this, ret_err, retry_times + 1, true);
            thread_pool_->DelayTask(retry_interval, retry_task);
        }
        return;
    }

    tabletnode::TabletNodeClient tabletnode_client_async(meta_server);
    ReadTabletRequest* request = new ReadTabletRequest;
    ReadTabletResponse* response = new ReadTabletResponse;
    request->set_sequence_id(last_sequence_id_++);
    request->set_tablet_name(FLAGS_tera_master_meta_table_name);
    RowReaderInfo* row_info = request->add_row_info_list();
    MakeMetaTableKey(name_, row_info->mutable_key());

    std::function<void (ReadTabletRequest*, ReadTabletResponse*, bool, int)> done =
        std::bind(&TableImpl::ReadTableMetaCallBack, this, ret_err, retry_times, _1, _2, _3, _4);
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

    MutexLock lock(&table_meta_mutex_);
    CHECK(table_meta_updating_);

    if (err == kTabletNodeOk) {
        TableMeta table_meta;
        const KeyValuePair& kv = response->detail().row_result(0).key_values(0);
        ParseMetaTableKeyValue(kv.key(), kv.value(), &table_meta);
        table_schema_.CopyFrom(table_meta.schema());
        create_time_ = table_meta.create_time();
        ret_err->SetFailed(ErrorCode::kOK);
        table_meta_updating_ = false;
        table_meta_cond_.Signal();
    } else if (err == kKeyNotExist || err == kSnapshotNotExist) {
        ret_err->SetFailed(ErrorCode::kNotFound);
        table_meta_updating_ = false;
        table_meta_cond_.Signal();
    } else if (retry_times >= FLAGS_tera_sdk_retry_times) {
        ret_err->SetFailed(ErrorCode::kSystem);
        table_meta_updating_ = false;
        table_meta_cond_.Signal();
    } else {
        int64_t retry_interval =
            static_cast<int64_t>(pow(FLAGS_tera_sdk_delay_send_internal, retry_times) * 1000);
        ThreadPool::Task retry_task =
            std::bind(&TableImpl::ReadTableMetaAsync, this, ret_err, retry_times + 1, true);
        thread_pool_->DelayTask(retry_interval, retry_task);
    }

    delete request;
    delete response;
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
    SdkCookie cookie;
    std::string cookie_file = GetCookieFilePathName();
    if (!::tera::sdk::RestoreCookie(cookie_file, true, &cookie)) {
        return true;
    }
    if (cookie.table_name() != name_) {
        LOG(INFO) << "[SDK COOKIE] cookie name error: " << cookie.table_name()
            << ", should be: " << name_;
        return true;
    }

    MutexLock lock(&meta_mutex_);
    for (int i = 0; i < cookie.tablets_size(); ++i) {
        const TabletMeta& meta = cookie.tablets(i).meta();
        const std::string& start_key = meta.key_range().key_start();
        LOG(INFO) << "[SDK COOKIE] restore:" << meta.path()
            << " range [" << DebugString(start_key)
            << " : " << DebugString(meta.key_range().key_end()) << "]";
        TabletMetaNode& node = tablet_meta_list_[start_key];
        node.meta = meta;
        node.update_time = cookie.tablets(i).update_time();
        node.status = NORMAL;
    }
    LOG(INFO) << "[SDK COOKIE] restore finished, tablet num: " << cookie.tablets_size();
    return true;
}

std::string TableImpl::GetCookieFilePathName(void) {
    return FLAGS_tera_sdk_cookie_path + "/"
        + GetCookieFileName(name_, cluster_->ClusterId(), create_time_);
}

std::string TableImpl::GetCookieLockFilePathName(void) {
    return GetCookieFilePathName() + ".LOCK";
}

void TableImpl::DoDumpCookie() {
    std::string cookie_file = GetCookieFilePathName();
    std::string cookie_lock_file = GetCookieLockFilePathName();
    SdkCookie cookie;
    cookie.set_table_name(name_);
    {
        MutexLock lock(&meta_mutex_);
        std::map<std::string, TabletMetaNode>::iterator it = tablet_meta_list_.begin();
        for (; it != tablet_meta_list_.end(); ++it) {
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
    if (!IsExist(FLAGS_tera_sdk_cookie_path) && !CreateDirWithRetry(FLAGS_tera_sdk_cookie_path)) {
        LOG(ERROR) << "[SDK COOKIE] fail to create cookie dir: " << FLAGS_tera_sdk_cookie_path;
        return;
    }
    ::tera::sdk::DumpCookie(cookie_file, cookie_lock_file, cookie);
}

void TableImpl::DumpCookie() {
    DoDumpCookie();
    ThreadPool::Task task = std::bind(&TableImpl::DumpCookie, this);
    AddDelayTask(FLAGS_tera_sdk_cookie_update_interval * 1000LL, task);
}

void TableImpl::EnableCookieUpdateTimer() {
    ThreadPool::Task task = std::bind(&TableImpl::DumpCookie, this);
    AddDelayTask(FLAGS_tera_sdk_cookie_update_interval * 1000LL, task);
}

std::string TableImpl::GetCookieFileName(const std::string& tablename,
                                         const std::string& cluster_id,
                                         int64_t create_time) {
    uint32_t hash = 0;
    if (GetHashNumber(cluster_id, hash, &hash) != 0) {
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
        std::bind(&TableImpl::DumpPerfCounterLogDelay, this);
    AddDelayTask(FLAGS_tera_sdk_perf_counter_log_interval * 1000, task);
}

void TableImpl::DoDumpPerfCounterLog() {
    LOG(INFO) << "[table " << name_ << " PerfCounter][pending]"
        << " pending_r: " << cur_reader_pending_counter_.Get()
        << " pending_w: " << cur_commit_pending_counter_.Get();
    perf_counter_.DoDumpPerfCounterLog("[table " + name_ + " PerfCounter]");
}

void TableImpl::PerfCounter::DoDumpPerfCounterLog(const std::string& log_prefix) {
    LOG(INFO) << log_prefix << "[delay](ms)"
        << " get meta: " << (get_meta_cnt.Get() > 0 ? get_meta.Clear() /  get_meta_cnt.Clear() / 1000 : 0)
        << " callback: " << (user_callback_cnt.Get() > 0 ? user_callback.Clear() / user_callback_cnt.Clear() / 1000 : 0)
        << " rpc_r: " << (rpc_r_cnt.Get() > 0 ? rpc_r.Clear() / rpc_r_cnt.Clear() / 1000 : 0)
        << " rpc_w: " << (rpc_w_cnt.Get() > 0 ? rpc_w.Clear() / rpc_w_cnt.Clear() / 1000 : 0)
        << " rpc_s: " << (rpc_s_cnt.Get() > 0 ? rpc_s.Clear() / rpc_s_cnt.Clear() / 1000 : 0);

    LOG(INFO) << log_prefix << "[mutation]"
        << " all: " << mutate_cnt.Clear()
        << " ok: " << mutate_ok_cnt.Clear()
        << " fail: " << mutate_fail_cnt.Clear()
        << " range: " << mutate_range_cnt.Clear()
        << " timeout: " << mutate_timeout_cnt.Clear()
        << " queue_timeout: " << mutate_queue_timeout_cnt.Clear();

    LOG(INFO) << log_prefix << "[reader]"
        << " all: " << reader_cnt.Clear()
        << " ok: " << reader_ok_cnt.Clear()
        << " fail: " << reader_fail_cnt.Clear()
        << " range: " << reader_range_cnt.Clear()
        << " timeout: " << reader_timeout_cnt.Clear()
        << " queue_timeout: " << reader_queue_timeout_cnt.Clear();

    LOG(INFO) << log_prefix << "[user_mu]"
        << " cnt: " << user_mu_cnt.Clear()
        << " suc: " << user_mu_suc.Clear()
        << " fail: " << user_mu_fail.Clear();
    LOG(INFO) << log_prefix << "[user_mu_cost]" << std::fixed << std::setprecision(2)
        << " cost_ave: " << hist_mu_cost.Average()
        << " cost_50: " << hist_mu_cost.Percentile(50)
        << " cost_90: " << hist_mu_cost.Percentile(90)
        << " cost_99: " << hist_mu_cost.Percentile(99);
    hist_mu_cost.Clear();

    LOG(INFO) << log_prefix << "[user_rd]"
        << " cnt: " << user_read_cnt.Clear()
        << " suc: " << user_read_suc.Clear()
        << " notfound: " << user_read_notfound.Clear()
        << " fail: " << user_read_fail.Clear();
    LOG(INFO) << log_prefix << "[user_rd_cost]" << std::fixed << std::setprecision(2)
        << " cost_ave: " << hist_read_cost.Average()
        << " cost_50: " << hist_read_cost.Percentile(50)
        << " cost_90: " << hist_read_cost.Percentile(90)
        << " cost_99: " << hist_read_cost.Percentile(99);
    hist_read_cost.Clear();

    LOG(INFO) << log_prefix << "[hist_async_cost]"
        << " cost_ave: " << hist_async_cost.Average()
        << " cost_50: " << hist_async_cost.Percentile(50)
        << " cost_90: " << hist_async_cost.Percentile(90)
        << " cost_99: " << hist_async_cost.Percentile(99);
    hist_async_cost.Clear();

    LOG(INFO) << log_prefix << "[total]"
        << " meta_sched_cnt: " << meta_sched_cnt.Get()
        << " meta_update_cnt: " << meta_update_cnt.Get()
        << " total_task_cnt: " << total_task_cnt.Get()
        << " total_commit_cnt: " << total_commit_cnt.Get();
}

void TableImpl::DelayTaskWrapper(ThreadPool::Task task, int64_t task_id) {
    task(task_id);
    {
        MutexLock lock(&delay_task_id_mutex_);
        delay_task_ids_.erase(task_id);
    }
}

int64_t TableImpl::AddDelayTask(int64_t delay_time, ThreadPool::Task task) {
    MutexLock lock(&delay_task_id_mutex_);
    ThreadPool::Task t =
        std::bind(&TableImpl::DelayTaskWrapper, this, task, _1);
    int64_t t_id = thread_pool_->DelayTask(delay_time, t);
    delay_task_ids_.insert(t_id);
    return t_id;
}

void TableImpl::ClearDelayTask() {
    MutexLock lock(&delay_task_id_mutex_);
    std::set<int64_t>::iterator it = delay_task_ids_.begin();
    while (it != delay_task_ids_.end()) {
        int64_t task_id = *it;
        // may deadlock, MUST unlock
        delay_task_id_mutex_.Unlock();
        bool cancelled = thread_pool_->CancelTask(*it);
        delay_task_id_mutex_.Lock();
        if (cancelled) {
            delay_task_ids_.erase(task_id);
        }
        it = delay_task_ids_.begin();
    }
}

void TableImpl::BreakRequest(int64_t task_id) {
    SdkTask* task = task_pool_.PopTask(task_id);
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

void TableImpl::StatUserPerfCounter(enum SdkTask::TYPE op, ErrorCode::ErrorCodeType code, int64_t cost_time) {
    switch (op) {
    case SdkTask::MUTATION:
        if (code == ErrorCode::kOK) {
            perf_counter_.user_mu_suc.Inc();
        } else {
            perf_counter_.user_mu_fail.Inc();
        }
        perf_counter_.hist_mu_cost.Add(cost_time);
        break;
    case SdkTask::READ:
        if (code == ErrorCode::kOK) {
            perf_counter_.user_read_suc.Inc();
        } else if (code == ErrorCode::kNotFound) {
            perf_counter_.user_read_notfound.Inc();
        } else {
            perf_counter_.user_read_fail.Inc();
        }
        perf_counter_.hist_read_cost.Add(cost_time);
        break;
    default:
        break;
    }
}

bool TableImpl::GetTabletLocation(std::vector<TabletInfo>* tablets,
                                  ErrorCode* err) {
    return false;
}

bool TableImpl::GetDescriptor(TableDescriptor* desc, ErrorCode* err) {
    return false;
}

/// 创建事务
Transaction* TableImpl::StartRowTransaction(const std::string& row_key) {
    return new SingleRowTxn((Table*)this, row_key, thread_pool_);
}

/// 提交事务
void TableImpl::CommitRowTransaction(Transaction* transaction) {
    SingleRowTxn* row_txn_impl = (SingleRowTxn*)transaction;
    row_txn_impl->Commit();
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
