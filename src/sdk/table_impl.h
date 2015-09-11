// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_TABLE_IMPL_H_
#define  TERA_SDK_TABLE_IMPL_H_

#include "common/mutex.h"
#include "common/timer.h"
#include "common/thread_pool.h"

#include "proto/table_meta.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "sdk/sdk_task.h"
#include "sdk/sdk_zk.h"
#include "sdk/tera.h"
#include "utils/counter.h"

namespace tera {

namespace master {
class MasterClient;
}

namespace tabletnode {
class TabletNodeClient;
}

class RowMutation;
class RowMutationImpl;
class ResultStreamImpl;
class ResultStreamSyncImpl;
class ScanTask;
class ScanDescImpl;
class WriteTabletRequest;
class WriteTabletResponse;
class RowReaderImpl;
class ReadTabletRequest;
class ReadTabletResponse;

class SyncMutationBatch {
public:
    std::vector<RowMutation*> _row_list;
    mutable Mutex _finish_mutex;
    common::CondVar _finish_cond;
    uint32_t _unfinished_count;

    SyncMutationBatch(const std::vector<RowMutation*>& row_list)
        : _finish_cond(&_finish_mutex) {
        for (uint32_t i = 0; i < row_list.size(); i++) {
            RowMutation* mutation = row_list[i];
            if (!mutation->IsAsync()) {
                _row_list.push_back(mutation);
            }
        }
        _unfinished_count = _row_list.size();
    }

    void AddMutation(RowMutation* mutation) {
        MutexLock lock(&_finish_mutex);
        _row_list.push_back(mutation);
        _unfinished_count++;
    }

    void WaitUntilFinish() {
        _finish_mutex.Lock();
        while (0 != _unfinished_count) {
            _finish_cond.Wait();
        }
        _finish_mutex.Unlock();
    }

    void OnFinishOne() {
        MutexLock lock(&_finish_mutex);
        if (--_unfinished_count == 0) {
            _finish_cond.Signal();
        }
    }
};

class TableImpl : public Table {
    friend class MutationCommitBuffer;
public:
    TableImpl(const std::string& table_name,
              const TableOptions& options,
              const std::string& zk_root_path,
              const std::string& zk_addr_list,
              ThreadPool* thread_pool);

    virtual ~TableImpl();

    virtual RowMutation* NewRowMutation(const std::string& row_key);

    virtual RowReader* NewRowReader(const std::string& row_key);

    virtual void ApplyMutation(RowMutation* row_mu);
    virtual void ApplyMutation(const std::vector<RowMutation*>& row_mutations);

    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     ErrorCode* err);
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     int64_t timestamp, ErrorCode* err);
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const int64_t value,
                     ErrorCode* err);
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     int32_t ttl, ErrorCode* err);
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     int64_t timestamp, int32_t ttl, ErrorCode* err);

    virtual bool Add(const std::string& row_key,
                     const std::string& family,
                     const std::string& qualifier,
                     int64_t delta,
                     ErrorCode* err);
    virtual bool AddInt64(const std::string& row_key,
                     const std::string& family,
                     const std::string& qualifier,
                     int64_t delta,
                     ErrorCode* err);

    virtual bool PutIfAbsent(const std::string& row_key,
                             const std::string& family,
                             const std::string& qualifier,
                             const std::string& value,
                             ErrorCode* err);

    /// 原子操作：追加内容到一个Cell
    virtual bool Append(const std::string& row_key, const std::string& family,
                        const std::string& qualifier, const std::string& value,
                        ErrorCode* err);

    virtual void Get(RowReader* row_reader);
    virtual void Get(const std::vector<RowReader*>& row_readers);
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err, uint64_t snapshot_id = 0);
    virtual bool Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    ErrorCode* err, uint64_t snapshot_id = 0);

    virtual bool IsPutFinished() { return _cur_commit_pending_counter.Get() == 0; }

    virtual bool IsGetFinished() { return _cur_reader_pending_counter.Get() == 0; }

    virtual ResultStream* Scan(const ScanDescriptor& desc, ErrorCode* err);

    virtual const std::string GetName() { return _name; }

    virtual bool Flush();

    virtual bool CheckAndApply(const std::string& rowkey, const std::string& cf_c,
                               const std::string& value, const RowMutation& row_mu,
                               ErrorCode* err);

    virtual int64_t IncrementColumnValue(const std::string& row, const std::string& family,
                                         const std::string& qualifier, int64_t amount,
                                         ErrorCode* err);

    virtual void SetWriteTimeout(int64_t timeout_ms);
    virtual void SetReadTimeout(int64_t timeout_ms);

    virtual bool LockRow(const std::string& rowkey, RowLock* lock, ErrorCode* err);

    virtual bool GetStartEndKeys(std::string* start_key, std::string* end_key,
                                 ErrorCode* err);

    virtual bool GetTabletLocation(std::vector<TabletInfo>* tablets,
                                   ErrorCode* err);

    virtual bool GetDescriptor(TableDescriptor* desc, ErrorCode* err);

    virtual void SetMaxMutationPendingNum(uint64_t max_pending_num) {
        _max_commit_pending_num = max_pending_num;
    }
    virtual void SetMaxReaderPendingNum(uint64_t max_pending_num) {
        _max_reader_pending_num = max_pending_num;
    }

public:
    bool OpenInternal(ErrorCode* err);

    void ScanTabletSync(ResultStreamSyncImpl* stream);
    void ScanTabletAsync(ResultStreamImpl* stream);

    void ScanMetaTable(const std::string& key_start,
                       const std::string& key_end);

    bool GetTabletMetaForKey(const std::string& key, TabletMeta* meta);

    uint64_t GetMaxMutationPendingNum() { return _max_commit_pending_num; }
    uint64_t GetMaxReaderPendingNum() { return _max_reader_pending_num; }

    struct PerfCounter {
        int64_t start_time;
        Counter rpc_r;
        Counter rpc_r_cnt;

        Counter rpc_w;
        Counter rpc_w_cnt;

        Counter rpc_s;
        Counter rpc_s_cnt;

        Counter user_callback;
        Counter user_callback_cnt;

        std::string ToLog();

        PerfCounter() {
            start_time = common::timer::get_micros();
        }
    };
private:
    bool ScanTabletNode(const TabletMeta & tablet_meta,
                        const std::string& key_start,
                        const std::string& key_end,
                        std::vector<KeyValuePair>* kv_list,
                        ErrorCode* err);

    void ApplyMutation(const std::vector<RowMutationImpl*>& mu_list,
                       bool called_by_user);

    void RetryApplyMutation(std::vector<RowMutationImpl*>* retry_mu_list);

    void ApplyMutation(const std::string& server_addr,
                       std::vector<RowMutationImpl*>* mu_list,
                       bool flush);

    void CommitMutationBuffer(std::string server_addr);

    void CommitMutation(const std::string& server_addr,
                        std::vector<RowMutationImpl*>* mu_list);

    void MutateCallBack(std::vector<RowMutationImpl*>* mu_list,
                        WriteTabletRequest* request,
                        WriteTabletResponse* response,
                        bool failed, int error_code);

    void ReadRows(const std::vector<RowReaderImpl*>& row_reader_list,
                  bool called_by_user);

    void ReadRows(const std::string& server_addr,
                  std::vector<RowReaderImpl*>* reader_list);

    void CommitReaderBuffer(std::string server_addr);

    void CommitReaders(const std::string server_addr,
                       std::vector<RowReaderImpl*>* reader_list);

    void ReaderCallBack(std::vector<RowReaderImpl*>* reader_list,
                        ReadTabletRequest* request,
                        ReadTabletResponse* response,
                        bool failed, int error_code);

    void RetryReadRows(std::vector<RowReaderImpl*>* retry_reader_list);

    void ScanTabletAsync(ScanTask* scan_task, int);

    void CommitScan(ScanTask* scan_task, const std::string& server_addr);

    void ScanCallBack(ScanTask* scan_task, ScanTabletRequest* request,
                      ScanTabletResponse* response, bool failed, int error_code);

    template <class T>
    void BreakRequest(T* row_request) {
        row_request->RunCallback();
    }
    void BreakScan(ScanTask* scan_task);

    void ProcessTaskPendingForMeta(const std::string& row, SdkTask* task);

    enum TabletMetaStatus {
        NORMAL,
        DELAY_UPDATE,
        WAIT_UPDATE,
        UPDATING
    };
    struct TabletMetaNode {
        TabletMeta meta;
        int64_t update_time;
        TabletMetaStatus status;

        TabletMetaNode() : update_time(0), status(NORMAL) {}
    };

    bool GetTabletAddrOrScheduleUpdateMeta(const std::string& row,
                                           SdkTask* request,
                                           std::string* server_addr);

    bool GetTabletMetaOrScheduleUpdateMeta(const std::string& row,
                                           SdkTask* task, bool task_wait,
                                           const TabletMetaNode** tablet_meta);

    TabletMetaNode* GetTabletMetaNodeForKey(const std::string& key);

    void DelayUpdateMeta(std::string start_key, std::string end_key);

    void UpdateMetaAsync();

    void ScanMetaTableAsync(const std::string& key_start, const std::string& key_end,
                            const std::string& expand_key_end, bool zk_access);

    void ScanMetaTableAsyncInLock(std::string key_start, std::string key_end,
                                  std::string expand_key_end, bool zk_access);

    void ScanMetaTableCallBack(std::string key_start,
                               std::string key_end,
                               std::string expand_key_end,
                               ScanTabletRequest* request,
                               ScanTabletResponse* response,
                               bool failed, int error_code);

    void UpdateTabletMetaList(const TabletMeta& meta);

    void GiveupUpdateTabletMeta(const std::string& key_start, const std::string& key_end);

    void WakeUpPendingRequest(const TabletMetaNode& node);

    void ScheduleUpdateMeta(const std::string& row, int64_t meta_timestamp);

    bool UpdateTableMeta(ErrorCode* err);
    void ReadTableMetaAsync(ErrorCode* ret_err, int32_t retry_times, bool zk_access);
    void ReadTableMetaCallBack(ErrorCode* ret_err, int32_t retry_times,
                               ReadTabletRequest* request,
                               ReadTabletResponse* response,
                               bool failed, int error_code);
    bool RestoreCookie();
    void EnableCookieUpdateTimer();
    void DumpCookie();
    void DoDumpCookie();
    std::string GetCookieFileName(const std::string& tablename,
                                  const std::string& zk_addr,
                                  const std::string& zk_path,
                                  int64_t create_time);
    std::string GetCookieFilePathName();
    std::string GetCookieLockFilePathName();
    void DeleteLegacyCookieLockFile(const std::string& lock_file, int timeout_seconds);
    void CloseAndRemoveCookieLockFile(int lock_fd, const std::string& cookie_lock_file);

    void CommitSequentialMutation();
    void RetryCommitSequentialMutation();
    void DelayCommitSequentionMutation();
    bool CommitNextTabletSequentialMutation();

    void DumpPerfCounterLogDelay();
    void DoDumpPerfCounterLog();

private:
    TableImpl(const TableImpl&);
    void operator=(const TableImpl&);

    struct CommitBuffer {
        uint64_t _timer_id;
        std::vector<RowMutationImpl*>* _row_list;
    };
    struct ReaderBuffer {
        uint64_t _timer_id;
        std::vector<RowReaderImpl*>* _reader_list;
    };

    std::string _name;
    int64_t _create_time;
    const TableOptions _options;
    uint64_t _last_sequence_id;
    uint32_t _timeout;

    mutable Mutex _commit_buffer_mutex;
    mutable Mutex _reader_buffer_mutex;
    uint32_t _commit_size;
    uint64_t _write_commit_timeout;
    uint64_t _read_commit_timeout;
    std::map<std::string, CommitBuffer> _commit_buffers;
    std::map<std::string, ReaderBuffer> _reader_buffers;
    Counter _cur_commit_pending_counter;
    Counter _cur_reader_pending_counter;
    int64_t _max_commit_pending_num;
    int64_t _max_reader_pending_num;

    // meta management
    mutable Mutex _meta_mutex;
    common::CondVar _meta_cond;
    std::map<std::string, std::list<SdkTask*> > _pending_task_list;
    uint32_t _meta_updating_count;
    std::map<std::string, TabletMetaNode> _tablet_meta_list;
    // end of meta management

    // table meta managerment
    mutable Mutex _table_meta_mutex;
    common::CondVar _table_meta_cond;
    bool _table_meta_updating;
    TableSchema _table_schema;
    // end of table meta managerment

    master::MasterClient* _master_client;
    tabletnode::TabletNodeClient* _tabletnode_client;

    // flags
    std::string _zk_root_path;
    std::string _zk_addr_list;

    ThreadPool* _thread_pool;
    /// _cluster could cache the master_addr & root_table_addr.
    /// if there is no _cluster,
    ///    we have to access zookeeper whenever we need master_addr or root_table_addr.
    /// if there is _cluster,
    ///    we save master_addr & root_table_addr in _cluster, access zookeeper only once.
    sdk::ClusterFinder* _cluster;

    PerfCounter _perf_counter;  // calc time consumption, for performance analysis
    int64_t _perf_log_task_id;

    // sequential mutation
    mutable Mutex _seq_mutation_mutex;
    std::string _seq_mutation_last_accept_row;
    std::list<RowMutationImpl*> _seq_mutation_accept_list;

    uint64_t _seq_mutation_session;
    uint64_t _seq_mutation_last_sequence;
    std::string _seq_mutation_server_addr;

    std::vector<RowMutationImpl*>* _seq_mutation_commit_list;
    uint64_t _seq_mutation_commit_timer_id;

    std::vector<RowMutationImpl*> _seq_mutation_retry_list;
    int64_t _seq_mutation_error_occur_time; // in ms
    bool _seq_mutation_wait_to_update_meta;
    bool _seq_mutation_wait_to_retry;
    uint64_t _seq_mutation_pending_rpc_count;

    /// read request will contain this member,
    /// so tabletnodes can drop the read-request that timeouted
    uint64_t _pending_timeout_ms;
};

} // namespace tera

#endif  // TERA_SDK_TABLE_IMPL_H_
