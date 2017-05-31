// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_TXN_H_
#define  TERA_SDK_TXN_H_

#include <string>
#include <vector>

#include "tera.h"

namespace tera {

/// cross-row, cross-table transaction
/// 跨行，跨表事务

class MultiRowTxn: public Transaction {
public:
    static Transaction* NewMultiRowTxn();
    virtual ~MultiRowTxn();

    virtual ErrorCode Get(RowReader* row_reader);
    virtual void ApplyMutation(RowMutation* row_mu);
    /// 提交事务
    /// 同步模式下，Commit()的返回值代表了提交操作的结果(成功 或者 失败及其原因)
    /// 异步模式下，通过GetError()获取提交结果
    virtual ErrorCode Commit();

    typedef void (*Callback)(Transaction* transaction);
    virtual void SetCommitCallback(Callback callback) {}
    virtual Callback GetCommitCallback() { return NULL; }
    virtual void SetContext(void* context) {}
    virtual void* GetContext() { return NULL; }
    virtual const ErrorCode& GetError() { return status_; }
    virtual int64_t GetStartTimestamp() { return 0; }

private:
    MultiRowTxn(int64_t start_ts);
    MultiRowTxn(const MultiRowTxn&);
    void operator=(const MultiRowTxn&);

    bool IsWritingByOthers(RowMutation* row_mu, RowReader* reader);
    bool IsLockedByOthers(RowMutation* row_mu, RowReader* reader);
    bool LockExists(tera::Transaction* single_row_txn, RowMutation* row_mu);
    ErrorCode Prewrite(RowMutation* w, RowMutation* primary);

private:
    int64_t start_ts_;
    std::vector<RowMutation*> writes_;
    ErrorCode status_;
};

} // namespace tera

#endif  // TERA_SDK_TXN_H_
