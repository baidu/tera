// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef  TERA_SDK_GLOBAL_TXN_H_
#define  TERA_SDK_GLOBAL_TXN_H_

#include <map>
#include <string>
#include <set>
#include <utility> 

#include "common/mutex.h"
#include "io/coding.h"
#include "proto/table_meta.pb.h"
#include "sdk/global_txn_internal.h"
#include "sdk/single_row_txn.h"
#include "sdk/sdk_utils.h"
#include "sdk/table_impl.h"
#include "sdk/sdk_zk.h"
#include "tera.h"
#include "common/counter.h"
#include "common/timer.h"

namespace tera {

class Cell;
class Write;
class GlobalTxnInternal;
class CellReaderContext;
class InternalReaderContext;
class PrewriteContext;

class GlobalTxn : public Transaction {
public:
    static Transaction* NewGlobalTxn(tera::Client* client, 
            common::ThreadPool* thread_pool, 
            sdk::ClusterFinder* tso_cluster);

    virtual ~GlobalTxn();

    virtual void ApplyMutation(RowMutation* row_mu);
    virtual ErrorCode Get(RowReader* row_reader);
    virtual ErrorCode Commit();
    
    virtual int64_t GetStartTimestamp() { return start_ts_; }
    virtual int64_t GetCommitTimestamp() { return commit_ts_; }

    virtual const ErrorCode& GetError() { return status_; }

    typedef void (*Callback)(Transaction* transaction);
    
    virtual void SetCommitCallback(Callback callback) {
        user_commit_callback_ = callback;
    }

    virtual Callback GetCommitCallback() { 
        return user_commit_callback_; 
    }

    virtual void SetContext(void* context) {
        user_commit_context_ = context;
    }
    
    virtual void* GetContext() {
        return user_commit_context_;
    }

    virtual void Ack(Table* t, 
                     const std::string& row_key, 
                     const std::string& column_family, 
                     const std::string& qualifier);

    virtual void Notify(Table* t,
                        const std::string& row_key, 
                        const std::string& column_family, 
                        const std::string& qualifier);

    virtual void SetIsolation(const IsolationLevel& isolation_level);

    virtual IsolationLevel Isolation() { return isolation_level_; }

    virtual void SetTimeout(int64_t timeout_ms);

    virtual int64_t Timeout();

private:    
    // ----------------------- begin get process --------------------------- //
    // read one cell from db
    // 
    // read "lock", "write", "data" columns result from db,
    // use async interface of tera [RowReader]
    void AsyncGetCell(Cell* cell, RowReaderImpl* user_reader_impl, InternalReaderContext* ctx);

    // check lock write and build cell result
    // (1) check read result, if failed will call [MergeCellToRow]
    // (2) maybe call [BackoffAndMaybeCleanupLock] and call [AsyncGetCell] retry
    // (3) maybe call [FindValueFromResultRow] and call [MergeCellToRow]
    void DoGetCellReaderCallback(RowReader* reader);

    // check "lock" and "write" columns, do like percolator
    // maybe call CleanLock, RollForward or wait some times
    //
    // if try_clean == true will be CleanLock not wait
    void BackoffAndMaybeCleanupLock(RowReader::TRow& row, 
                                    const Cell& cell, 
                                    const bool try_clean, 
                                    ErrorCode* status);
    void CleanLock(const Cell& cell, const tera::PrimaryInfo& primary,  
                   ErrorCode* status);
    
    void RollForward(const Cell& cell, 
                     const tera::PrimaryInfo& primary,
                     int lock_type, 
                     ErrorCode* status);

    // get result form "result_row" and set into "target_cell"
    bool FindValueFromResultRow(RowReader::TRow& result_row, Cell* target_cell);

    // call GetCellCallback function @ other thread
    void MergeCellToRow(RowReader* internal_reader, const ErrorCode& status);

    // set cell result, merge to value_list and call user_reader_callback
    void GetCellCallback(CellReaderContext* ctx);
    
    void SetReaderStatusAndRunCallback(RowReaderImpl* reader_impl, ErrorCode* status);

    // ------------- begin commit prewrite (commit phase1) ----------------- //
    void SaveWrite(const std::string& tablename, 
                  const std::string& row_key, 
                  tera::Write& w);

    // commit entry
    //
    // do [commit phase1], [commit phase2] will begin at callback
    void InternalCommit();

    // [prewrite] Step(1): 
    //      read "data", "lock", "write" column from tera
    //
    // aysnc prewrite one row use single_row_txn
    void AsyncPrewrite(std::vector<Write>* same_row_writes);
    
    // [prewrite] Step(2): 
    //      a) verify [prewrite] step(1) read result status and no conflict 
    //      b) write "lock" and "data" column to tera, 
    //         through same single_row_txn in step(1)
    //
    // call by [prewrite] step(1),through reader callback
    void DoPrewriteReaderCallback(RowReader* reader);

    // prewrite Step(3):
    //      verify [prewrite] step(2) single_row_txn commit status,
    //      if the last prewrite callback and status ok, will call [commit]
    //
    // call by [prewrite] step(2), through single_row_txn commit callback
    void DoPrewriteCallback(SingleRowTxn* single_row_txn);
    void RunAfterPrewriteFailed(PrewriteContext* ctx);

    // --------------------- begin commit phase2 ---------------------- //
    
    // commit phase2 Step(1):
    //      a) get timestamp from timeoracle for commit_ts
    //      b) sync commit primary write through single_row_txn
    //         (for this gtxn, on this step only one thread can work)
    //      c) call [commit phase2] step(2) in a loop
    //
    // call by [prewrite] step(3)
    void InternalCommitPhase2(); 

    void VerifyPrimaryLocked();

    void DoVerifyPrimaryLockedCallback(RowReader* reader);
    
    void CommitPrimary(SingleRowTxn* primary_single_txn);

    void CheckPrimaryStatusAndCommmitSecondaries(Transaction* primary_single_txn);

    // commit phase2 Step(2):
    //      async commit secondaries writes through RowMutaion
    //
    // call by [commit phase2] step(1)
    void AsyncCommitSecondaries(std::vector<Write>* same_row_writes);
    
    void DoCommitSecondariesCallback(RowMutation* mutation);

    // commit phase2 Step(3):
    //      async do ack through RowMutaion
    //
    // call by [commit phase2] step(1)
    void AsyncAck(std::vector<Write>* same_row_acks);
    
    void DoAckCallback(RowMutation* mutation);

    // commit phase2 Step(4):
    //      async do notify through RowMutaion
    //
    // call by [commit phase2] step(1)
    void AsyncNotify(std::vector<Write>* same_row_notifies);
    
    void DoNotifyCallback(RowMutation* mutation);

    /// if user want to delete this transaction, 
    /// before any async tasks of this transaction finished for failed
    void WaitForComplete();
    
    void SetLastStatus(ErrorCode* status);

    void RunUserCallback();

    // -------------------- end commit phase1 and phase2 ------------------- //
private:
    GlobalTxn(tera::Client* client, 
              common::ThreadPool* thread_pool, 
              sdk::ClusterFinder* tso_cluster);

    GlobalTxn(const GlobalTxn&) = delete;
    void operator=(const GlobalTxn&) = delete;    

    // <tablename, row_key> 
    typedef std::pair<std::string, std::string> TableWithRowkey;
    // tableWithRowkey -> set(write)
    typedef std::map<TableWithRowkey, std::vector<Write>> WriterMap;

    std::unique_ptr<GlobalTxnInternal> gtxn_internal_;
    ErrorCode status_;
    bool status_returned_; // if true gtxn will not change "status_"
    
    Write* primary_write_;
    WriterMap writes_;
    WriterMap::iterator prewrite_iterator_;
    int64_t writes_size_;
    
    int64_t start_ts_;
    int64_t prewrite_start_ts_;
    int64_t commit_ts_;
    IsolationLevel isolation_level_;
	std::string serialized_primary_;

    WriterMap acks_;
    WriterMap notifies_;
    
    mutable Mutex mu_;
    std::atomic<bool> finish_;
    mutable Mutex finish_mutex_;
    common::CondVar finish_cond_;
    
    std::atomic<bool> has_commited_;
    
    Callback user_commit_callback_;
    void* user_commit_context_;

    common::ThreadPool* thread_pool_;
    sdk::ClusterFinder* tso_cluster_;

    int64_t timeout_ms_;
   
    Counter put_fail_cnt_; // put begin +1, done -1
    Counter commit_secondaries_done_cnt_;
    Counter ack_done_cnt_;
    Counter notify_done_cnt_;

    Counter writes_cnt_;
    Counter acks_cnt_;
    Counter notifies_cnt_;
    std::atomic<bool> all_task_pushed_;
};

} // namespace tera

#endif  // TERA_SDK_GLOBAL_TXN_H_
