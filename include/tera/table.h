// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TABLE_H_
#define  TERA_TABLE_H_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "error_code.h"
#include "mutation.h"
#include "reader.h"
#include "scan.h"
#include "table_descriptor.h"

#pragma GCC visibility push(default)
namespace tera {

struct TableInfo {
    TableDescriptor* table_desc;
    std::string status;
};

/// tablet分布信息
struct TabletInfo {
    std::string table_name;     ///< 表名
    std::string path;           ///< 路径
    std::string server_addr;    ///< tabletserver地址, ip:port格式
    std::string start_key;      ///< 起始rowkey(包含)
    std::string end_key;        ///< 终止rowkey(不包含)
    int64_t data_size;          ///< 数据量, 为估计值
    std::string status;
};

class RowMutation;
class RowReader;
class Transaction;
/// 表接口
class Table {
public:
    Table() {}
    virtual ~Table() {}
    /// 返回一个新的RowMutation
    virtual RowMutation* NewRowMutation(const std::string& row_key) = 0;
    /// 返回一个新的RowReader
    virtual RowReader* NewRowReader(const std::string& row_key) = 0;
    /// 提交一个修改操作, 同步操作返回是否成功, 异步操作永远返回true
    virtual void ApplyMutation(RowMutation* row_mu) = 0;
    /// 提交一批修改操作, 多行之间不保证原子性, 通过RowMutation的GetError获取各自的返回码
    virtual void ApplyMutation(const std::vector<RowMutation*>& row_mu_list) = 0;
    /// 修改指定列, 当作为kv或二维表格使用时的便捷接口
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     ErrorCode* err) = 0;
    /// 修改指定列, 当作为kv或二维表格使用时的便捷接口
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const int64_t value,
                     ErrorCode* err) = 0;
    /// 带TTL修改指定列, 当作为kv或二维表格使用时的便捷接口
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     int32_t ttl, ErrorCode* err) = 0;
    /// 带TTL修改指定列的指定版本, 当作为kv或二维表格使用时的便捷接口
    virtual bool Put(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, const std::string& value,
                     int64_t timestamp, int32_t ttl, ErrorCode* err) = 0;
    /// 原子加一个Cell
    virtual bool Add(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t delta,
                     ErrorCode* err) = 0;
    /// 原子加一个Cell
    virtual bool AddInt64(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t delta,
                     ErrorCode* err) = 0;

    /// 原子操作：如果不存在才能Put成功
    virtual bool PutIfAbsent(const std::string& row_key,
                             const std::string& family,
                             const std::string& qualifier,
                             const std::string& value,
                             ErrorCode* err) = 0;

    /// 原子操作：追加内容到一个Cell
    virtual bool Append(const std::string& row_key, const std::string& family,
                        const std::string& qualifier, const std::string& value,
                        ErrorCode* err) = 0;
    /// 读取一个指定行
    virtual void Get(RowReader* row_reader) = 0;
    /// 读取多行
    virtual void Get(const std::vector<RowReader*>& row_readers) = 0;
    /// 读取指定cell, 当作为kv或二维表格使用时的便捷接口
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, std::string* value,
                     ErrorCode* err, uint64_t snapshot_id = 0) = 0;
    /// 读取指定cell, 当作为kv或二维表格使用时的便捷接口
    virtual bool Get(const std::string& row_key, const std::string& family,
                     const std::string& qualifier, int64_t* value,
                     ErrorCode* err, uint64_t snapshot_id = 0) = 0;

    virtual bool IsPutFinished() = 0;
    virtual bool IsGetFinished() = 0;

    /// Scan, 流式读取, 返回一个数据流, 失败返回NULL
    virtual ResultStream* Scan(const ScanDescriptor& desc, ErrorCode* err) = 0;

    virtual const std::string GetName() = 0;

    /// !!! Not implemented
    virtual bool Flush() = 0;
    /// 条件修改, 指定Cell的值为value时, 才执行row_mu
    virtual bool CheckAndApply(const std::string& rowkey, const std::string& cf_c,
                               const std::string& value, const RowMutation& row_mu,
                               ErrorCode* err) = 0;
    /// 计数器++操作
    virtual int64_t IncrementColumnValue(const std::string& row, const std::string& family,
                                         const std::string& qualifier, int64_t amount,
                                         ErrorCode* err) = 0;

    /// 创建事务
    virtual Transaction* StartRowTransaction(const std::string& row_key) = 0;
    /// 提交事务
    virtual void CommitRowTransaction(Transaction* transaction) = 0;

    /// 设置表格写操作默认超时
    /// !!! Not implemented
    virtual void SetWriteTimeout(int64_t timeout_ms) = 0;
    /// !!! Not implemented
    virtual void SetReadTimeout(int64_t timeout_ms) = 0;

    /// 创建行锁
    virtual bool LockRow(const std::string& rowkey, RowLock* lock, ErrorCode* err) = 0;

    /// 获取表格的最小最大key
    virtual bool GetStartEndKeys(std::string* start_key, std::string* end_key,
                                 ErrorCode* err) = 0;

    /// 获取表格分布信息
    /// !!! Not implemented
    virtual bool GetTabletLocation(std::vector<TabletInfo>* tablets,
                                   ErrorCode* err) = 0;
    /// 获取表格描述符
    /// !!! Not implemented
    virtual bool GetDescriptor(TableDescriptor* desc, ErrorCode* err) = 0;

    virtual void SetMaxMutationPendingNum(uint64_t max_pending_num) = 0;
    virtual void SetMaxReaderPendingNum(uint64_t max_pending_num) = 0;

private:
    Table(const Table&);
    void operator=(const Table&);
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_TABLE_H_

