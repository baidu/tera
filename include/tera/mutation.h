// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_MUTATION_H_
#define  TERA_MUTATION_H_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#pragma GCC visibility push(default)
namespace tera {

class RowLock {
};

class Transaction;
class Table;
/// 修改操作
class RowMutation {
/// rowkey的限制：大小 [0, 64KB)，任意二进制串
public:
    enum Type {
        kPut,
        kDeleteColumn,
        kDeleteColumns,
        kDeleteFamily,
        kDeleteRow,
        kAdd,
        kPutIfAbsent,
        kAppend,
        kAddInt64
    };
    struct Mutation {
        Type type;

        /// 大小 [0, 64KB)，cf名本身是可打印字符串，不限制数量
        std::string family;

        /// 大小 [0, 64KB)，任意二进制串
        std::string qualifier;

        /// 大小 [0, 32MB)
        std::string value;

        int64_t timestamp;
        int32_t ttl;
    };

    RowMutation();
    virtual ~RowMutation();

    virtual const std::string& RowKey() = 0;

    virtual void Reset(const std::string& row_key) = 0;

    /// 修改指定列
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const int64_t value) = 0;
    /// 修改指定列
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const std::string& value) = 0;
    /// 带TTL的修改一个列
    virtual void Put(const std::string& family, const std::string& qualifier,
                     const std::string& value, int32_t ttl) = 0;
    // 原子加一个Cell
    virtual void Add(const std::string& family, const std::string& qualifier,
                     const int64_t delta) = 0;
    // 原子加一个Cell
    virtual void AddInt64(const std::string& family, const std::string& qualifier,
                     const int64_t delta) = 0;

    // 原子操作：如果不存在才能Put成功
    virtual void PutIfAbsent(const std::string& family,
                             const std::string& qualifier,
                             const std::string& value) = 0;

    /// 原子操作：追加内容到一个Cell
    virtual void Append(const std::string& family, const std::string& qualifier,
                        const std::string& value) = 0;

    /// 修改一个列的特定版本
    virtual void Put(const std::string& family, const std::string& qualifier,
                     int64_t timestamp, const std::string& value) = 0;
    /// 带TTL的修改一个列的特定版本
    virtual void Put(const std::string& family, const std::string& qualifier,
                     int64_t timestamp, const std::string& value, int32_t ttl) = 0;
    /// 修改默认列
    virtual void Put(const std::string& value) = 0;
    /// 修改默认列
    virtual void Put(const int64_t value) = 0;

    /// 带TTL的修改默认列
    virtual void Put(const std::string& value, int32_t ttl) = 0;

    /// 修改默认列的特定版本
    virtual void Put(int64_t timestamp, const std::string& value) = 0;

    /// 删除一个列的最新版本
    virtual void DeleteColumn(const std::string& family,
                              const std::string& qualifier) = 0;
    /// 删除一个列的指定版本
    virtual void DeleteColumn(const std::string& family,
                              const std::string& qualifier,
                              int64_t timestamp) = 0;
    /// 删除一个列的全部版本
    virtual void DeleteColumns(const std::string& family,
                               const std::string& qualifier) = 0;
    /// 删除一个列的指定范围版本
    virtual void DeleteColumns(const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp) = 0;
    /// 删除一个列族的所有列的全部版本
    virtual void DeleteFamily(const std::string& family) = 0;
    /// 删除一个列族的所有列的指定范围版本
    virtual void DeleteFamily(const std::string& family, int64_t timestamp) = 0;
    /// 删除整行的全部数据
    virtual void DeleteRow() = 0;
    /// 删除整行的指定范围版本
    virtual void DeleteRow(int64_t timestamp) = 0;

    /// 修改锁住的行, 必须提供行锁
    virtual void SetLock(RowLock* rowlock) = 0;
    /// 设置超时时间(只影响当前操作,不影响Table::SetWriteTimeout设置的默认写超时)
    virtual void SetTimeOut(int64_t timeout_ms) = 0;
    /// 返回超时时间
    virtual int64_t TimeOut() = 0;
    /// 设置异步回调, 操作会异步返回
    typedef void (*Callback)(RowMutation* param);
    virtual void SetCallBack(Callback callback) = 0;
    // 返回异步回调函数
    virtual Callback GetCallBack() = 0;
    /// 设置用户上下文，可在回调函数中获取
    virtual void SetContext(void* context) = 0;
    /// 获得用户上下文
    virtual void* GetContext() = 0;
    /// 获得结果错误码
    virtual const ErrorCode& GetError() = 0;
    /// 是否异步操作
    virtual bool IsAsync() = 0;
    /// 异步操作是否完成
    /// !!! Not implemented
    virtual bool IsFinished() const = 0;
    /// mutation数量
    virtual uint32_t MutationNum() = 0;
    virtual uint32_t Size() = 0;
    virtual uint32_t RetryTimes() = 0;
    virtual const RowMutation::Mutation& GetMutation(uint32_t index) = 0;
    /// 返回所属事务
    virtual Transaction* GetTransaction() = 0;

private:
    RowMutation(const RowMutation&);
    void operator=(const RowMutation&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_MUTATION_H_
