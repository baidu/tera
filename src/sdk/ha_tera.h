// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef HA_TERA_TERA_H_
#define HA_TERA_TERA_H_

#include "sdk/tera.h"
#include "proto/table_meta.pb.h"

namespace tera {

class HATable {
public:
    HATable() {}
    ~HATable() {}

    void Add(Table *t);
    /// 返回一个新的RowMutation
    RowMutation* NewRowMutation(const std::string& row_key);
    /// 返回一个新的RowReader
    RowReader* NewRowReader(const std::string& row_key);
    /// 提交一个修改操作, 同步操作返回是否成功, 异步操作永远返回true
    void ApplyMutation(RowMutation* row_mu);
    /// 提交一批修改操作, 多行之间不保证原子性, 通过RowMutation的GetError获取各自的返回码
    void ApplyMutation(const std::vector<RowMutation*>& row_mu_list);
    /// 修改指定列, 当作为kv或二维表格使用时的便捷接口
    bool Put(const std::string& row_key, const std::string& family,
             const std::string& qualifier, const std::string& value,
             ErrorCode* err);
    /// 修改指定列, 当作为kv或二维表格使用时的便捷接口
    bool Put(const std::string& row_key, const std::string& family,
             const std::string& qualifier, const int64_t value,
             ErrorCode* err);
    /// 带TTL修改指定列, 当作为kv或二维表格使用时的便捷接口
    bool Put(const std::string& row_key, const std::string& family,
             const std::string& qualifier, const std::string& value,
             int32_t ttl, ErrorCode* err);
    /// 带TTL修改指定列的指定版本, 当作为kv或二维表格使用时的便捷接口
    bool Put(const std::string& row_key, const std::string& family,
             const std::string& qualifier, const std::string& value,
             int64_t timestamp, int32_t ttl, ErrorCode* err);
    /// 原子加一个Cell
    bool Add(const std::string& row_key, const std::string& family,
             const std::string& qualifier, int64_t delta,
             ErrorCode* err);
    /// 原子加一个Cell
    bool AddInt64(const std::string& row_key, const std::string& family,
                  const std::string& qualifier, int64_t delta,
                  ErrorCode* err);

    /// 原子操作：如果不存在才能Put成功
    bool PutIfAbsent(const std::string& row_key,
                     const std::string& family,
                     const std::string& qualifier,
                     const std::string& value,
                     ErrorCode* err);

    /// 原子操作：追加内容到一个Cell
    bool Append(const std::string& row_key, const std::string& family,
                const std::string& qualifier, const std::string& value,
                ErrorCode* err);
    void LGet(RowReader* row_reader);
    void LGet(const std::vector<RowReader*>& row_readers);

    /// 读取一个指定行
    void Get(RowReader* row_reader);
    /// 读取多行
    void Get(const std::vector<RowReader*>& row_readers);
    /// 读取指定cell, 当作为kv或二维表格使用时的便捷接口
    bool Get(const std::string& row_key, const std::string& family,
             const std::string& qualifier, std::string* value,
             ErrorCode* err, uint64_t snapshot_id = 0);
    /// 读取指定cell, 当作为kv或二维表格使用时的便捷接口
    bool Get(const std::string& row_key, const std::string& family,
             const std::string& qualifier, int64_t* value,
             ErrorCode* err, uint64_t snapshot_id = 0);

    bool IsPutFinished();
    bool IsGetFinished();

    /// Scan, 流式读取, 返回一个数据流, 失败返回NULL
    ResultStream* Scan(const ScanDescriptor& desc, ErrorCode* err);

    const std::string GetName();

    bool Flush();
    /// 条件修改, 指定Cell的值为value时, 才执行row_mu
    bool CheckAndApply(const std::string& rowkey, const std::string& cf_c,
                       const std::string& value, const RowMutation& row_mu,
                       ErrorCode* err);
    /// 计数器++操作
    int64_t IncrementColumnValue(const std::string& row, const std::string& family,
                                 const std::string& qualifier, int64_t amount,
                                 ErrorCode* err);
    /// 设置表格写操作默认超时
    void SetWriteTimeout(int64_t timeout_ms);
    void SetReadTimeout(int64_t timeout_ms);

    /// 创建行锁
    bool LockRow(const std::string& rowkey, RowLock* lock, ErrorCode* err);

    /// 获取表格的最小最大key
    bool GetStartEndKeys(std::string* start_key, std::string* end_key,
                         ErrorCode* err);

    /// 获取表格分布信息
    bool GetTabletLocation(std::vector<TabletInfo>* tablets,
                           ErrorCode* err);
    /// 获取表格描述符
    bool GetDescriptor(TableDescriptor* desc, ErrorCode* err);

    void SetMaxMutationPendingNum(uint64_t max_pending_num);
    void SetMaxReaderPendingNum(uint64_t max_pending_num);

    // 获取指定的集群
    Table* GetClusterHandle(size_t i);
    static void MergeResult(const std::vector<RowResult>& results, RowResult& res, uint32_t max_size);
    static void ShuffleArray(std::vector<Table*>& table_set);
private:

    HATable(const HATable&);
    void operator=(const HATable&);
    std::vector<Table*> _tables;
};


class HAClient {
public:
    /// 使用glog的用户必须调用此接口，避免glog被重复初始化
    static void SetGlogIsInitialized();

    static HAClient* NewClient(const std::vector<std::string>& confpaths,
                               const std::string& log_prefix,
                               ErrorCode* err = NULL);
    ~HAClient() {for (size_t i = 0; i < _clients.size(); i++) { delete _clients[i]; } _clients.clear(); }

    bool Init(ErrorCode* err = NULL);

    /// 创建表格
    bool CreateTable(const TableDescriptor& desc, ErrorCode* err);
    bool CreateTable(const TableDescriptor& desc,
                     const std::vector<std::string>& tablet_delim,
                     ErrorCode* err);
    /// 更新表格Schema
    bool UpdateTable(const TableDescriptor& desc, ErrorCode* err);
    /// 删除表格
    bool DeleteTable(std::string name, ErrorCode* err);
    /// 停止表格服务
    bool DisableTable(std::string name, ErrorCode* err);
    /// 恢复表格服务
    bool EnableTable(std::string name, ErrorCode* err);

    /// acl
    bool CreateUser(const std::string& user,
                    const std::string& password, ErrorCode* err);
    bool DeleteUser(const std::string& user, ErrorCode* err);
    bool ChangePwd(const std::string& user,
                   const std::string& password, ErrorCode* err);
    bool ShowUser(const std::string& user, std::vector<std::string>& user_groups,
                  ErrorCode* err);
    bool AddUserToGroup(const std::string& user,
                        const std::string& group, ErrorCode* err);
    bool DeleteUserFromGroup(const std::string& user,
                             const std::string& group, ErrorCode* err);
    /// 打开表格, 失败返回NULL
    HATable* OpenTable(const std::string& table_name, ErrorCode* err);
    /// 获取表格分布信息
    bool GetTabletLocation(const std::string& table_name,
                           std::vector<TabletInfo>* tablets,
                           ErrorCode* err);
    /// 获取表格Schema
    TableDescriptor* GetTableDescriptor(const std::string& table_name,
                                        ErrorCode* err);

    bool List(std::vector<TableInfo>* table_list,
              ErrorCode* err);

    bool List(const std::string& table_name,
              TableInfo* table_info,
              std::vector<TabletInfo>* tablet_list,
              ErrorCode* err);

    bool IsTableExist(const std::string& table_name, ErrorCode* err);

    bool IsTableEnabled(const std::string& table_name, ErrorCode* err);

    bool IsTableEmpty(const std::string& table_name, ErrorCode* err);

    bool GetSnapshot(const std::string& name, uint64_t* snapshot, ErrorCode* err);
    bool DelSnapshot(const std::string& name, uint64_t snapshot,ErrorCode* err);
    bool Rollback(const std::string& name, uint64_t snapshot,
                  const std::string& rollback_name, ErrorCode* err);

    bool CmdCtrl(const std::string& command,
                 const std::vector<std::string>& arg_list,
                 bool* bool_result,
                 std::string* str_result,
                 ErrorCode* err);
    bool Rename(const std::string& old_table_name,
                const std::string& new_table_name,
                ErrorCode* err);
    // 获取指定的集群
    Client* GetClusterClient(size_t i);
private:
    HAClient(const std::vector<Client*> &clients) {
        _clients = clients;
    }
    HAClient(const HAClient&);
    void operator=(const HAClient&);

    std::vector<Client*> _clients;
};
}


#endif // HA_TERA_TERA_H_

