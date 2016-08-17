// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_READER_H_
#define  TERA_READER_H_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#pragma GCC visibility push(default)
namespace tera {

//用于解析原子计数器
class CounterCoding {
public:
    //整数编码为字节buffer
    static std::string EncodeCounter(int64_t counter);
    //字节buffer解码为整数
    static bool DecodeCounter(const std::string& buf,
                              int64_t* counter);
};

class RowReaderImpl;
class Transaction;
class Table;
/// 读取操作
class RowReader {
public:
    /// 保存用户读取列的请求，格式为map<family, set<qualifier> >，若map大小为0，表示读取整行
    typedef std::map<std::string, std::set<std::string> >ReadColumnList;

    RowReader();
    virtual ~RowReader();
    /// 返回row key
    virtual const std::string& RowName() = 0;
    /// 设置读取特定版本
    virtual void SetTimestamp(int64_t ts) = 0;
    /// 返回读取时间戳
    virtual int64_t GetTimestamp() = 0;
    /// 设置快照id
    virtual void SetSnapshot(uint64_t snapshot_id) = 0;
    /// 取出快照id
    virtual uint64_t GetSnapshot() = 0;
    /// 读取整个family
    virtual void AddColumnFamily(const std::string& family) = 0;
    virtual void AddColumn(const std::string& cf_name,
                           const std::string& qualifier) = 0;

    virtual void SetTimeRange(int64_t ts_start, int64_t ts_end) = 0;
    virtual void SetMaxVersions(uint32_t max_version) = 0;
    virtual void SetTimeOut(int64_t timeout_ms) = 0;
    /// 设置异步回调, 操作会异步返回
    typedef void (*Callback)(RowReader* param);
    virtual void SetCallBack(Callback callback) = 0;
    /// 设置用户上下文，可在回调函数中获取
    virtual void SetContext(void* context) = 0;
    virtual void* GetContext() = 0;
    /// 设置异步返回 !!! NOT implemented
    virtual void SetAsync() = 0;
    /// 异步操作是否完成
    /// !!! Not implemented
    virtual bool IsFinished() const = 0;

    /// 获得结果错误码
    virtual ErrorCode GetError() = 0;
    /// 是否到达结束标记
    virtual bool Done() = 0;
    /// 迭代下一个cell
    virtual void Next() = 0;
    /// 读取的结果
    virtual std::string Value() = 0;
    virtual int64_t ValueInt64() = 0;
    virtual std::string Family() = 0;
    virtual std::string ColumnName() = 0;
    virtual std::string Qualifier() = 0;
    virtual int64_t Timestamp() = 0;

    virtual uint32_t GetReadColumnNum() = 0;
    virtual const ReadColumnList& GetReadColumnList() = 0;
    /// 将结果转存到一个std::map中, 格式为: map<column, map<timestamp, value>>
    typedef std::map< std::string, std::map<int64_t, std::string> > Map;
    virtual void ToMap(Map* rowmap) = 0;
    /// 返回所属事务
    virtual Transaction* GetTransaction() = 0;

private:
    RowReader(const RowReader&);
    void operator=(const RowReader&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_READER_H_
