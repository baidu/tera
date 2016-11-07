// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TRANSACTION_H_
#define  TERA_TRANSACTION_H_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#pragma GCC visibility push(default)
namespace tera {


class RowReader;
class RowMutation;

/// 事务操作接口
class Transaction {
public:
    /// 提交一个修改操作
    virtual void ApplyMutation(RowMutation* row_mu) = 0;
    /// 读取操作
    virtual void Get(RowReader* row_reader) = 0;

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

    Transaction() {}
    virtual ~Transaction() {}

private:
    Transaction(const Transaction&);
    void operator=(const Transaction&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_TRANSACTION_H_
