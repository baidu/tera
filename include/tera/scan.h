// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SCAN_H_
#define  TERA_SCAN_H_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "error_code.h"

#pragma GCC visibility push(default)
namespace tera {

/// 从表格里读取的结果流
class ResultStream {
public:
    virtual bool LookUp(const std::string& row_key) = 0;
    /// 是否到达结束标记
    virtual bool Done(ErrorCode* err = NULL) = 0;
    /// 迭代下一个cell
    virtual void Next() = 0;
    /// RowKey
    virtual std::string RowName() const = 0;
    /// Column(格式为cf:qualifier)
    virtual std::string ColumnName() const = 0;
    /// Column family
    virtual std::string Family() const = 0;
    /// Qualifier
    virtual std::string Qualifier() const = 0;
    /// Cell对应的时间戳
    virtual int64_t Timestamp() const = 0;
    /// Value
    virtual std::string Value() const = 0;
    virtual int64_t ValueInt64() const = 0;
    ResultStream() {}
    virtual ~ResultStream() {}

private:
    ResultStream(const ResultStream&);
    void operator=(const ResultStream&);
};

class ScanDescImpl;
class ScanDescriptor {
public:
    /// 通过起始行构造
    ScanDescriptor(const std::string& rowkey);
    ~ScanDescriptor();
    /// 设置scan的结束行(不包含在返回结果内), 非必要
    void SetEnd(const std::string& rowkey);
    /// 设置要读取的cf, 可以添加多个
    void AddColumnFamily(const std::string& cf);
    /// 设置要读取的column(cf:qualifier)
    void AddColumn(const std::string& cf, const std::string& qualifier);
    /// 设置最多返回的版本数
    void SetMaxVersions(int32_t versions);
    /// 设置scan的超时时间
    void SetPackInterval(int64_t timeout);
    /// 设置返回版本的时间范围
    void SetTimeRange(int64_t ts_end, int64_t ts_start);

    bool SetFilter(const std::string& schema);
    typedef bool (*ValueConverter)(const std::string& in,
                                   const std::string& type,
                                   std::string* out);
    /// 设置自定义类型转换函数（定义带类型过滤时使用）
    void SetValueConverter(ValueConverter converter);

    void SetSnapshot(uint64_t snapshot_id);
    /// 设置预读的buffer大小, 默认64K
    void SetBufferSize(int64_t buf_size);

    /// set number limit for each buffer
    void SetNumberLimit(int64_t number_limit);
    int64_t GetNumberLimit();

    /// 设置async, 缺省true
    void SetAsync(bool async);

    /// 判断当前scan是否是async
    bool IsAsync() const;

    ScanDescImpl* GetImpl() const;

private:
    ScanDescriptor(const ScanDescriptor&);
    void operator=(const ScanDescriptor&);
    ScanDescImpl* _impl;
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_SCAN_H_
