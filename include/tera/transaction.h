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
class Table;

/// 事务隔离级别
enum class IsolationLevel {
    kReadCommitedSnapshot = 0,
    kSnapshot             = 1
};

/// 事务操作接口
class Transaction {
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

    /// 获取事务开始时间戳
    virtual int64_t GetStartTimestamp() = 0;

    /// 获取事务提交时间戳
    virtual int64_t GetCommitTimestamp() = 0;

    /// 仅全局事务支持
    virtual void Ack(Table* t, 
                     const std::string& row_key, 
                     const std::string& column_family, 
                     const std::string& qualifier) = 0;

    /// 仅全局事务支持
    virtual void Notify(Table* t,
                        const std::string& row_key, 
                        const std::string& column_family, 
                        const std::string& qualifier) = 0;

    /// 设置隔离级别
    virtual void SetIsolation(const IsolationLevel& isolation_level) = 0;

    /// 获取隔离级别
    virtual IsolationLevel Isolation() = 0;

    // Set timeout(ms).
    virtual void SetTimeout(int64_t timeout_ms) = 0;

    // Get timeout(ms).
    virtual int64_t Timeout() = 0;

    Transaction() {}
    virtual ~Transaction() {}

private:
    Transaction(const Transaction&);
    void operator=(const Transaction&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_TRANSACTION_H_
