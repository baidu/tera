// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_TXN_H_
#define  TERA_SDK_TXN_H_

#include <string>

#include "sdk/mutate_impl.h"
#include "tera.h"

namespace tera {

class MultiRowTxn: public Transaction {
public:
    static Transaction* NewMultiRowTxn();
    virtual ~MultiRowTxn();

    /// 提交一个修改操作
    virtual void ApplyMutation(RowMutation* row_mu);
    /// 读取操作
    virtual void Get(RowReader* row_reader);

    virtual ErrorCode Commit();

    typedef void (*Callback)(Transaction* transaction);
    virtual void SetCommitCallback(Callback callback) {}
    virtual Callback GetCommitCallback() { return NULL; }
    virtual void SetContext(void* context) {}
    virtual void* GetContext() { return NULL; }
    virtual const ErrorCode& GetError() {
        abort();
        return status_;
    }
    virtual int64_t GetStartTimestamp() { return start_ts_; }
private:
    MultiRowTxn(int64_t start_ts);
    MultiRowTxn(const MultiRowTxn&);
    void operator=(const MultiRowTxn&);

    bool IsWritingByOthers(RowMutation* mu, RowReader* reader);
    bool IsLockedByOthers(RowMutation* mu, RowReader* reader);
    void BuildReaderForPrewrite(RowMutation* w, RowReader* r);
    void BuildRowMutationForPrewrite(RowMutation* user_mu,
                                     RowMutation* txn_mu,
                                     const std::string& primary_info);
    ErrorCode Prewrite(RowMutation* w, RowMutation* primary);

    bool LockExistsOrUnknown(tera::Transaction* single_row_txn, RowMutation* mu);
    void BuildRowReaderForCommit(RowMutation* user_mu, RowReader* reader);
    void BuildRowMutationForCommit(RowMutation* user_mu, RowMutation* txn_mu, int64_t commit_ts);

    void CheckPrimaryLockAndTimestamp(RowReader* reader, const std::string& cf, const std::string& qu,
                                      bool* lock_exists, int64_t* lock_timestamp);
    bool MaybePrimaryLockTimeout(int64_t ts);
    bool CleanupLockAndData(Transaction* single_row_txn,
                            RowReader* reader,
                            const std::string& row,
                            const std::string& cf,
                            const std::string& qu,
                            int64_t start_ts);
    void RollForwardCell(tera::Transaction* target_row_txn, RowReader* reader,
                         const std::string& cf, const std::string qu,
                         int64_t start_ts, int64_t commit_ts);
    void BackoffAndMaybeCleanupLock(tera::Transaction* target_row_txn, RowReader* user_reader, const std::string& primary_info,
                                    const std::string& cf, const std::string& qu, int64_t last_txn_start_ts);
    bool IsLockedBeforeMe(RowReader* user_reader, RowReader* txn_reader, std::string* primary,
                          std::string* cf, std::string* qu, int64_t* last_txn_start_ts);
    void FillReadResult(RowReader* txn_reader, RowReader* user_reader);
private:
    int64_t start_ts_;
    std::vector<RowMutation*> writes_;
    ErrorCode status_;
};

} // namespace tera

#endif  // TERA_SDK_TXN_H_
