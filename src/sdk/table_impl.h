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
#include "sdk/client_impl.h"
#include "sdk/sdk_task.h"
#include "sdk/sdk_zk.h"
#include "tera.h"
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
    std::vector<RowMutation*> row_list_;
    mutable Mutex finish_mutex_;
    common::CondVar finish_cond_;
    uint32_t unfinished_count_;

    SyncMutationBatch(const std::vector<RowMutation*>& row_list)
        : finish_cond_(&finish_mutex_) {
        for (uint32_t i = 0; i < row_list.size(); i++) {
            RowMutation* mutation = row_list[i];
            if (!mutation->IsAsync()) {
                row_list_.push_back(mutation);
            }
        }
        unfinished_count_ = row_list_.size();
    }

    void AddMutation(RowMutation* mutation) {
        MutexLock lock(&finish_mutex_);
        row_list_.push_back(mutation);
        unfinished_count_++;
    }

    void WaitUntilFinish() {
        finish_mutex_.Lock();
        while (0 != unfinished_count_) {
            finish_cond_.Wait();
        }
        finish_mutex_.Unlock();
    }

    void OnFinishOne() {
        MutexLock lock(&finish_mutex_);
        if (--unfinished_count_ == 0) {
            finish_cond_.Signal();
        }
    }
};

class TableImpl : public Table {
    friend class MutationCommitBuffer;
public:
    TableImpl(const std::string& table_name,
              ThreadPool* thread_pool,
              sdk::ClusterFinder* cluster);

    virtual ~TableImpl();

    virtual RowMutation* NewRowMutation(const std::string& row_key);

    virtual RowReader* NewRowReader(const std::string& row_key);

    virtual void ApplyMutation(RowMutation* row_mu);
    virtual void ApplyMutation(const std::vector<RowMutation*>& row_mutations);

    virtual void Put(RowMutation* row_mu);
    virtual void Put(const std::vector<RowMutation*>& row_mutations);

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
                     ErrorCode* err);
    virtual bool Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    ErrorCode* err);
    virtual bool Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, std::string* value,
                    uint64_t snapshot_id, ErrorCode* err);
    virtual bool Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    uint64_t snapshot_id, ErrorCode* err);
    virtual bool Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, std::string* value,
                    ErrorCode* err, uint64_t snapshot_id);
    virtual bool Get(const std::string& row_key, const std::string& family,
                    const std::string& qualifier, int64_t* value,
                    ErrorCode* err, uint64_t snapshot_id);

    virtual bool IsPutFinished() { return cur_commit_pending_counter_.Get() == 0; }

    virtual bool IsGetFinished() { return cur_reader_pending_counter_.Get() == 0; }

    virtual ResultStream* Scan(const ScanDescriptor& desc, ErrorCode* err);

    virtual const std::string GetName() { return name_; }

    virtual bool Flush();

    virtual bool CheckAndApply(const std::string& rowkey, const std::string& cf_c,
                               const std::string& value, const RowMutation& row_mu,
                               ErrorCode* err);

    virtual int64_t IncrementColumnValue(const std::string& row, const std::string& family,
                                         const std::string& qualifier, int64_t amount,
                                         ErrorCode* err);

    /// 创建事务
    virtual Transaction* StartRowTransaction(const std::string& row_key);
    /// 提交事务
    virtual void CommitRowTransaction(Transaction* transaction);

    virtual void SetWriteTimeout(int64_t timeout_ms);
    virtual void SetReadTimeout(int64_t timeout_ms);

    virtual bool LockRow(const std::string& rowkey, RowLock* lock, ErrorCode* err);

    virtual bool GetStartEndKeys(std::string* start_key, std::string* end_key,
                                 ErrorCode* err);

    virtual bool GetTabletLocation(std::vector<TabletInfo>* tablets,
                                   ErrorCode* err);

    virtual bool GetDescriptor(TableDescriptor* desc, ErrorCode* err);

    virtual void SetMaxMutationPendingNum(uint64_t max_pending_num) {
        max_commit_pending_num_ = max_pending_num;
    }
    virtual void SetMaxReaderPendingNum(uint64_t max_pending_num) {
        max_reader_pending_num_ = max_pending_num;
    }

public:
    bool OpenInternal(ErrorCode* err);

    void ScanTabletSync(ResultStreamSyncImpl* stream);
    void ScanTabletAsync(ResultStreamImpl* stream);

    void ScanMetaTable(const std::string& key_start,
                       const std::string& key_end);

    bool GetTabletMetaForKey(const std::string& key, TabletMeta* meta);

    uint64_t GetMaxMutationPendingNum() { return max_commit_pending_num_; }
    uint64_t GetMaxReaderPendingNum() { return max_reader_pending_num_; }
    TableSchema GetTableSchema() { return  table_schema_; }

    struct PerfCounter {
        int64_t start_time;
        Counter rpc_r;                    // 读取的耗时
        Counter rpc_r_cnt;                // 读取的次数

        Counter rpc_w;                    // 写入的耗时
        Counter rpc_w_cnt;                // 写入的次数

        Counter rpc_s;                    // scan的耗时
        Counter rpc_s_cnt;                // scan的次数

        Counter user_callback;            // 运行用户callback的耗时
        Counter user_callback_cnt;        // 运行用户callback的次数

        Counter get_meta;                 // 更新meta的耗时
        Counter get_meta_cnt;             // 更新meta的次数

        Counter mutate_cnt;               // 分发mutation的次数
        Counter mutate_ok_cnt;            // mutation回调成功的次数
        Counter mutate_fail_cnt;          // mutation回调失败的次数
        Counter mutate_range_cnt;         // mutation回调失败-原因为not in range
        Counter mutate_timeout_cnt;       // mutation在sdk队列中超时
        Counter mutate_queue_timeout_cnt; // mutation在sdk队列中超时，且之前从未被重试过

        Counter reader_cnt;               // 分发reader的次数
        Counter reader_ok_cnt;            // reader回调成功的次数
        Counter reader_fail_cnt;          // reader回调失败的次数
        Counter reader_range_cnt;         // reader回调失败-原因为not in range
        Counter reader_timeout_cnt;       // reader在sdk队列中超时
        Counter reader_queue_timeout_cnt; // raader在sdk队列中超时，且之前从未被重试过

        void DoDumpPerfCounterLog(const std::string& log_prefix);

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

    // 将一批mutation根据rowkey分配给各个TS
    void DistributeMutations(const std::vector<RowMutationImpl*>& mu_list,
                            bool called_by_user);

    void DistributeMutationsById(std::vector<int64_t>* retry_mu_id_list);

    // 分配完成后将mutation打包
    void PackMutations(const std::string& server_addr,
                       std::vector<RowMutationImpl*>& mu_list,
                       bool flush);

    // mutation打包不满但到达最大等待时间
    void MutationBatchTimeout(std::string server_addr, uint64_t batch_seq);

    // 通过异步RPC将mutation提交至TS
    void CommitMutationsById(const std::string& server_addr,
                             std::vector<int64_t>& mu_id_list);
    void CommitMutations(const std::string& server_addr,
                         std::vector<RowMutationImpl*>& mu_list);

    // mutate RPC回调
    void MutateCallBack(std::vector<int64_t>* mu_id_list,
                        WriteTabletRequest* request,
                        WriteTabletResponse* response,
                        bool failed, int error_code);

    // mutation到达用户设置的超时时间但尚未处理完
    void MutationTimeout(SdkTask* sdk_task);

    // 将一批reader根据rowkey分配给各个TS
    void DistributeReaders(const std::vector<RowReaderImpl*>& row_reader_list,
                           bool called_by_user);

    void DistributeReadersById(std::vector<int64_t>* reader_id_list);

    // 分配完成后将reader打包
    void PackReaders(const std::string& server_addr,
                     std::vector<RowReaderImpl*>& reader_list);

    // reader打包不满但到达最大等待时间
    void ReaderBatchTimeout(std::string server_addr, uint64_t batch_seq);

    // 通过异步RPC将reader提交至TS
    void CommitReadersById(const std::string server_addr,
                           std::vector<int64_t>& reader_id_list);
    void CommitReaders(const std::string server_addr,
                       std::vector<RowReaderImpl*>& reader_list);

    // reader RPC回调
    void ReaderCallBack(std::vector<int64_t>* reader_id_list,
                        ReadTabletRequest* request,
                        ReadTabletResponse* response,
                        bool failed, int error_code);

    // reader到达用户设置的超时时间但尚未处理完
    void ReaderTimeout(SdkTask* sdk_task);

    void ScanTabletAsync(ScanTask* scan_task, bool called_by_user);

    void CommitScan(ScanTask* scan_task, const std::string& server_addr);

    void ScanCallBack(ScanTask* scan_task, ScanTabletRequest* request,
                      ScanTabletResponse* response, bool failed, int error_code);

    void BreakRequest(int64_t task_id);
    void BreakScan(ScanTask* scan_task);

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
                               int64_t start_time,
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
                                  const std::string& cluster_id,
                                  int64_t create_time);
    std::string GetCookieFilePathName();
    std::string GetCookieLockFilePathName();
    void DeleteLegacyCookieLockFile(const std::string& lock_file, int timeout_seconds);
    void CloseAndRemoveCookieLockFile(int lock_fd, const std::string& cookie_lock_file);

    void DumpPerfCounterLogDelay();
    void DoDumpPerfCounterLog();

    void DelayTaskWrapper(ThreadPool::Task task, int64_t task_id);
    int64_t AddDelayTask(int64_t delay_time, ThreadPool::Task task);
    void ClearDelayTask();

private:
    TableImpl(const TableImpl&);
    void operator=(const TableImpl&);

    struct TaskBatch {
        uint64_t sequence_num;
        uint64_t timer_id;
        uint64_t byte_size;
        std::vector<int64_t>* row_id_list;
    };

    std::string name_;
    int64_t create_time_;
    uint64_t last_sequence_id_;
    uint32_t timeout_;

    mutable Mutex mutation_batch_mutex_;
    mutable Mutex reader_batch_mutex_;
    uint32_t commit_size_;
    uint64_t write_commit_timeout_;
    uint64_t read_commit_timeout_;
    std::map<std::string, TaskBatch> mutation_batch_map_;
    std::map<std::string, TaskBatch> reader_batch_map_;
    uint64_t mutation_batch_seq_;
    uint64_t reader_batch_seq_;
    Counter cur_commit_pending_counter_;
    Counter cur_reader_pending_counter_;
    int64_t max_commit_pending_num_;
    int64_t max_reader_pending_num_;

    // meta management
    mutable Mutex meta_mutex_;
    common::CondVar meta_cond_;
    std::map<std::string, std::list<int64_t> > pending_task_id_list_;
    uint32_t meta_updating_count_;
    std::map<std::string, TabletMetaNode> tablet_meta_list_;
    // end of meta management

    // table meta managerment
    mutable Mutex table_meta_mutex_;
    common::CondVar table_meta_cond_;
    bool table_meta_updating_;
    TableSchema table_schema_;
    // end of table meta managerment

    SdkTimeoutManager task_pool_;
    Counter next_task_id_;

    master::MasterClient* master_client_;
    tabletnode::TabletNodeClient* tabletnode_client_;

    ThreadPool* thread_pool_;
    mutable Mutex delay_task_id_mutex_;
    std::set<int64_t> delay_task_ids_;
    /// cluster_ could cache the master_addr & root_table_addr.
    /// if there is no cluster_,
    ///    we have to access zookeeper whenever we need master_addr or root_table_addr.
    /// if there is cluster_,
    ///    we save master_addr & root_table_addr in cluster_, access zookeeper only once.
    sdk::ClusterFinder* cluster_;
    bool cluster_private_;

    PerfCounter perf_counter_;  // calc time consumption, for performance analysis

    /// read request will contain this member,
    /// so tabletnodes can drop the read-request that timeouted
    uint64_t pending_timeout_ms_;
};

class TableWrapper: public Table {
public:
    explicit TableWrapper(Table* impl, ClientImpl* client)
        : impl_(impl), client_(client) {}
    virtual ~TableWrapper() {
        client_->CloseTable(impl_->GetName());
    }
    virtual RowMutation* NewRowMutation(const std::string& row_key) {
        return impl_->NewRowMutation(row_key);
    }
    virtual RowReader* NewRowReader(const std::string& row_key) {
        return impl_->NewRowReader(row_key);
    }
    virtual void Put(RowMutation* row_mu) {
        impl_->Put(row_mu);
    }
    virtual void Put(const std::vector<RowMutation*>& row_mu_list) {
        impl_->Put(row_mu_list);
    }
    virtual void ApplyMutation(RowMutation* row_mu) {
        impl_->ApplyMutation(row_mu);
    }
    virtual void ApplyMutation(const std::vector<RowMutation*>& row_mu_list) {
        impl_->ApplyMutation(row_mu_list);
    }
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     ErrorCode* err) {
        return impl_->Put(row_key, family, qualifier, value, err);
    }
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const int64_t value,
                     ErrorCode* err) {
        return impl_->Put(row_key, family, qualifier, value, err);
    }
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     int32_t ttl, ErrorCode* err) {
        return impl_->Put(row_key, family, qualifier, value, ttl, err);
    }
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     int64_t timestamp, int32_t ttl, ErrorCode* err) {
        return impl_->Put(row_key, family, qualifier, value, timestamp, ttl, err);
    }
    virtual bool Add(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t delta,
                     ErrorCode* err) {
        return impl_->Add(row_key, family, qualifier, delta, err);
    }
    virtual bool AddInt64(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t delta,
                     ErrorCode* err) {
        return impl_->AddInt64(row_key, family, qualifier, delta, err);
    }

    virtual bool PutIfAbsent(const std::string& row_key,
                             const std::string& family,
                             const std::string& qualifier,
                             const std::string& value,
                             ErrorCode* err) {
        return impl_->PutIfAbsent(row_key, family, qualifier, value, err);
    }

    virtual bool Append(const std::string& row_key, const std::string& family,
                        const std::string& qualifier, const std::string& value,
                        ErrorCode* err) {
        return impl_->Append(row_key, family, qualifier, value, err);
    }
    virtual void Get(RowReader* row_reader) {
        impl_->Get(row_reader);
    }
    virtual void Get(const std::vector<RowReader*>& row_readers) {
        impl_->Get(row_readers);
    }
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err) {
        return impl_->Get(row_key, family, qualifier, value, err);
    }
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t* value,
                     ErrorCode* err) {
        return impl_->Get(row_key, family, qualifier, value, err);
    }
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err, uint64_t snapshot_id) {
        return impl_->Get(row_key, family, qualifier, value, snapshot_id, err);
    }
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     uint64_t snapshot_id, ErrorCode* err) {
        return impl_->Get(row_key, family, qualifier, value, snapshot_id, err);
    }
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t* value,
                     ErrorCode* err, uint64_t snapshot_id) {
        return impl_->Get(row_key, family, qualifier, value, snapshot_id, err);
    }
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t* value,
                     uint64_t snapshot_id, ErrorCode* err) {
        return impl_->Get(row_key, family, qualifier, value, snapshot_id, err);
    }

    virtual bool IsPutFinished() {
        return impl_->IsPutFinished();
    }
    virtual bool IsGetFinished() {
        return impl_->IsGetFinished();
    }

    virtual ResultStream* Scan(const ScanDescriptor& desc, ErrorCode* err) {
        return impl_->Scan(desc, err);
    }

    virtual const std::string GetName() {
        return impl_->GetName();
    }

    virtual bool Flush() {
        return impl_->Flush();
    }
    virtual bool CheckAndApply(const std::string& rowkey, const std::string& cf_c,
                               const std::string& value, const RowMutation& row_mu,
                               ErrorCode* err) {
        return impl_->CheckAndApply(rowkey, cf_c, value, row_mu, err);
    }
    virtual int64_t IncrementColumnValue(const std::string& row, const std::string& family,
                                         const std::string& qualifier, int64_t amount,
                                         ErrorCode* err) {
        return impl_->IncrementColumnValue(row, family, qualifier, amount, err);
    }
    virtual Transaction* StartRowTransaction(const std::string& row_key) {
        return impl_->StartRowTransaction(row_key);
    }
    virtual void CommitRowTransaction(Transaction* transaction) {
        impl_->CommitRowTransaction(transaction);
    }
    virtual void SetWriteTimeout(int64_t timeout_ms) {
        impl_->SetWriteTimeout(timeout_ms);
    }
    virtual void SetReadTimeout(int64_t timeout_ms) {
        impl_->SetReadTimeout(timeout_ms);
    }

    virtual bool LockRow(const std::string& rowkey, RowLock* lock, ErrorCode* err) {
        return impl_->LockRow(rowkey, lock, err);
    }

    virtual bool GetStartEndKeys(std::string* start_key, std::string* end_key,
                                 ErrorCode* err) {
        return impl_->GetStartEndKeys(start_key, end_key, err);
    }

    virtual bool GetTabletLocation(std::vector<TabletInfo>* tablets,
                                   ErrorCode* err) {
        return impl_->GetTabletLocation(tablets, err);
    }
    virtual bool GetDescriptor(TableDescriptor* desc, ErrorCode* err) {
        return impl_->GetDescriptor(desc, err);
    }

    virtual void SetMaxMutationPendingNum(uint64_t max_pending_num) {
        impl_->SetMaxMutationPendingNum(max_pending_num);
    }
    virtual void SetMaxReaderPendingNum(uint64_t max_pending_num) {
        impl_->SetMaxReaderPendingNum(max_pending_num);
    }

private:
    Table* impl_;
    ClientImpl* client_;
};

} // namespace tera

#endif  // TERA_SDK_TABLE_IMPL_H_
