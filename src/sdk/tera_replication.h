// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TERA_REPLICATION_H_
#define  TERA_TERA_REPLICATION_H_

#include <string>

#include "tera.h"

#pragma GCC visibility push(default)
namespace tera {

/// 修改操作
class RowMutationReplicate {
public:
    RowMutationReplicate() {}
    virtual ~RowMutationReplicate() {}

    /// 修改默认列
    virtual void Put(const std::string& value) = 0;

    /// 带TTL的修改默认列
    virtual void Put(const std::string& value, int32_t ttl) = 0;

    /// 删除整行的全部数据
    virtual void DeleteRow() = 0;

    /// 设置异步回调, 操作会异步返回
    typedef void (*Callback)(RowMutationReplicate* param);
    virtual void SetCallBack(Callback callback) = 0;

    /// 设置用户上下文，可在回调函数中获取
    virtual void SetContext(void* context) = 0;
    /// 获得用户上下文
    virtual void* GetContext() = 0;

    /// 获得结果错误码
    virtual const ErrorCode& GetError() = 0;

private:
    RowMutationReplicate(const RowMutationReplicate&);
    void operator=(const RowMutationReplicate&);
};

class RowReaderReplicate {
public:
    RowReaderReplicate() {};
    virtual ~RowReaderReplicate() {};

    /// 设置异步回调, 操作会异步返回
    typedef void (*Callback)(RowReaderReplicate* param);
    virtual void SetCallBack(Callback callback) = 0;

    /// 设置用户上下文，可在回调函数中获取
    virtual void SetContext(void* context) = 0;
    /// 获得用户上下文
    virtual void* GetContext() = 0;

    /// 获得结果错误码
    virtual ErrorCode GetError() = 0;
    /// 读取的结果
    virtual std::string Value() = 0;

private:
    RowReaderReplicate(const RowReaderReplicate&);
    void operator=(const RowReaderReplicate&);
};


/// 表接口
class TableReplicate {
public:
    TableReplicate() {}
    virtual ~TableReplicate() {}

    /// 返回一个新的RowMutation
    virtual RowMutationReplicate* NewRowMutation(const std::string& row_key) = 0;
    /// 提交一个修改操作, 同步操作返回是否成功, 异步操作永远返回true
    virtual void ApplyMutation(RowMutationReplicate* row_mu) = 0;

    /// 返回一个新的RowReader
    virtual RowReaderReplicate* NewRowReader(const std::string& row_key) = 0;
    /// 读取一个指定行
    virtual void Get(RowReaderReplicate* row_reader) = 0;

private:
    TableReplicate(const TableReplicate&);
    void operator=(const TableReplicate&);
};

class ClientReplicate {
public:
    /// 使用glog的用户必须调用此接口，避免glog被重复初始化
    static void SetGlogIsInitialized();

    static ClientReplicate* NewClient(const std::string& confpath,
                                      const std::string& log_prefix,
                                      ErrorCode* err = NULL);

    static ClientReplicate* NewClient(const std::string& confpath, ErrorCode* err = NULL);

    static ClientReplicate* NewClient();

    /// 打开表格, 失败返回NULL
    virtual TableReplicate* OpenTable(const std::string& table_name, ErrorCode* err) = 0;

    ClientReplicate() {}
    virtual ~ClientReplicate() {}

private:
    ClientReplicate(const ClientReplicate&);
    void operator=(const ClientReplicate&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_TERA_REPLICATION_H_
