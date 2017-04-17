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
class MultiRowTxn : public Transaction {
public:
    /// 提交一个修改操作
    virtual void ApplyMutation(RowMutation* row_mu) = 0;
    /// 读取操作
    virtual ErrorCode Get(RowReader* row_reader) = 0;

    /// 回调函数原型
    typedef void (*Callback)(Transaction* transaction);
    /// 设置提交回调, 提交操作会异步返回
    virtual void SetCommitCallback(Callback callback) = 0;
    /// 获取提交回调
    virtual Callback GetCommitCallback() = 0;

    /// 设置用户上下文，可在回调函数中获取
    virtual void SetContext(void* context) = 0;
    /// 获取用户上下文
    virtual void* GetContext() = 0;

    /// 获得结果错误码
    virtual const ErrorCode& GetError() = 0;

    /// 提交事务
    /// 同步模式下，Commit()的返回值代表了提交操作的结果(成功 或者 失败及其原因)
    /// 异步模式下，通过GetError()获取提交结果
    virtual ErrorCode Commit() = 0;

    MultiRowTxn() {}
    virtual ~MultiRowTxn() {}

private:
    MultiRowTxn(const MultiRowTxn&);
    void operator=(const MultiRowTxn&);
};

class MultiRowTxnSync: public MultiRowTxn {
public:
    static Transaction* NewMultiRowTxnSync();
    virtual ~MultiRowTxnSync();

    virtual ErrorCode Get(RowReader* row_reader);
    virtual void ApplyMutation(RowMutation* row_mu);
    virtual ErrorCode Commit();

    /// make gcc happy. these functions will don't work in sync-impl
    typedef void (*Callback)(Transaction* transaction);
    virtual void SetCommitCallback(Callback callback) {}
    virtual Callback GetCommitCallback() { return NULL; }
    virtual void SetContext(void* context) {}
    virtual void* GetContext() { return NULL; }
    virtual const ErrorCode& GetError() { return status_; }

private:
    MultiRowTxnSync(int64_t start_ts);
    MultiRowTxnSync(const MultiRowTxn&);
    void operator=(const MultiRowTxnSync&);

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
