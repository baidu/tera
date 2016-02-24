// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TERA_H_
#define  TERA_TERA_H_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace tera {

/// 操作错误码
class ErrorCode {
public:
    enum ErrorCodeType {
        kOK = 0,
        kNotFound,
        kBadParam,
        kSystem,
        kTimeout,
        kBusy,
        kNoQuota,
        kNoAuth,
        kUnknown,
        kNotImpl
    };
    ErrorCode();
    void SetFailed(ErrorCodeType err, const std::string& reason = "");
    std::string GetReason() const;
    ErrorCodeType GetType() const;

private:
    ErrorCodeType _err;
    std::string _reason;
};

/// 将tera错误吗转化为可读字符串
const char* strerr(ErrorCode error_code);

enum CompressType {
    kNoneCompress = 1,
    kSnappyCompress = 2,
};

enum StoreType {
    kInDisk = 0,
    kInFlash = 1,
    kInMemory = 2,
};

/// RawKey拼装模式，kReadable性能较高，但key中不允许'\0'，kBinary性能低一些，允许所有字符
enum RawKeyType {
    kReadable = 0,
    kBinary = 1,
    kTTLKv = 2,
    kGeneralKv = 3,
};

extern const int64_t kLatestTimestamp;
extern const int64_t kOldestTimestamp;

/// ACL
struct ACL {
    int32_t owner;  ///< 所属用户id
    int32_t role;
    int64_t acl;
};

class ColumnFamilyDescriptor {
public:
    ColumnFamilyDescriptor() {}
    virtual ~ColumnFamilyDescriptor() {}
    virtual int32_t Id() const = 0;
    virtual const std::string& Name() const = 0;
    /// 属于哪个lg, 不可更改
    virtual const std::string& LocalityGroup() const = 0;
    /// 历史版本保留时间, 不设置时为0， 表示无限大永久保存
    virtual void SetTimeToLive(int32_t ttl) = 0;
    virtual int32_t TimeToLive() const = 0;
    /// 在TTL内,最多存储的版本数, 默认3, 设为0时, 关闭多版本
    virtual void SetMaxVersions(int32_t max_versions) = 0;
    virtual int32_t MaxVersions() const = 0;
    /// 最少存储的版本数,即使超出TTL,也至少保留min_versions个版本
    virtual void SetMinVersions(int32_t min_versions) = 0;
    virtual int32_t MinVersions() const = 0;
    /// 存储限额, MBytes
    virtual void SetDiskQuota(int64_t quota) = 0;
    virtual int64_t DiskQuota() const = 0;
    /// ACL
    virtual void SetAcl(ACL acl) = 0;
    virtual ACL Acl() const = 0;

    virtual void SetType(const std::string& type) = 0;
    virtual const std::string& Type() const = 0;

private:
    ColumnFamilyDescriptor(const ColumnFamilyDescriptor&);
    void operator=(const ColumnFamilyDescriptor&);
};

/// 局部性群组描述
class LocalityGroupDescriptor {
public:
    LocalityGroupDescriptor() {}
    virtual ~LocalityGroupDescriptor() {}
    /// Id read only
    virtual int32_t Id() const = 0;
    /// Name
    virtual const std::string& Name() const = 0;
    /// Compress type
    virtual void SetCompress(CompressType type) = 0;
    virtual CompressType Compress() const = 0;
    /// Block size
    virtual void SetBlockSize(int block_size) = 0;
    virtual int BlockSize() const = 0;
    /// Store type
    virtual void SetStore(StoreType type) = 0;
    virtual StoreType Store() const = 0;
    /// Bloomfilter
    virtual void SetUseBloomfilter(bool use_bloomfilter) = 0;
    virtual bool UseBloomfilter() const = 0;
    /// Memtable On Leveldb (disable/enable)
    virtual bool UseMemtableOnLeveldb() const = 0;
    virtual void SetUseMemtableOnLeveldb(bool use_mem_ldb) = 0;
    /// Memtable-LDB WriteBuffer Size
    virtual int32_t MemtableLdbWriteBufferSize() const = 0;
    virtual void SetMemtableLdbWriteBufferSize(int32_t buffer_size) = 0;
    /// Memtable-LDB Block Size
    virtual int32_t MemtableLdbBlockSize() const = 0;
    virtual void SetMemtableLdbBlockSize(int32_t block_size) = 0;

    /// sst file size, in Bytes
    virtual int32_t SstSize() const = 0;
    virtual void SetSstSize(int32_t sst_size) = 0;

private:
    LocalityGroupDescriptor(const LocalityGroupDescriptor&);
    void operator=(const LocalityGroupDescriptor&);
};

class TableDescImpl;

class TableDescriptor {
public:
    /// 表格名字仅允许使用字母、数字和下划线构造,长度不超过256；默认是非kv表
    TableDescriptor(const std::string& tb_name = "");

    ~TableDescriptor();

    void SetTableName(const std::string& name);
    std::string TableName() const;

    /// 增加一个localitygroup, 名字仅允许使用字母、数字和下划线构造,长度不超过256
    LocalityGroupDescriptor* AddLocalityGroup(const std::string& lg_name);
    LocalityGroupDescriptor* DefaultLocalityGroup();
    /// 删除一个localitygroup, 当还有cf属于这个lg时, 会删除失败.
    bool RemoveLocalityGroup(const std::string& lg_name);
    /// 获取id对应的localitygroup
    const LocalityGroupDescriptor* LocalityGroup(int32_t id) const;
    const LocalityGroupDescriptor* LocalityGroup(const std::string& lg_name) const;
    /// LG数量
    int32_t LocalityGroupNum() const;

    /// 增加一个columnfamily, 名字仅允许使用字母、数字和下划线构造,长度不超过256
    ColumnFamilyDescriptor* AddColumnFamily(const std::string& cf_name,
                                            const std::string& lg_name = "lg0");
    /// 删除一个columnfamily
    void RemoveColumnFamily(const std::string& cf_name);
    /// 获取id对应的CF
    const ColumnFamilyDescriptor* ColumnFamily(int32_t id) const;
    const ColumnFamilyDescriptor* ColumnFamily(const std::string& cf_name) const;
    ColumnFamilyDescriptor* DefaultColumnFamily();
    /// CF数量
    int32_t ColumnFamilyNum() const;

    /// Raw Key Mode
    void SetRawKey(RawKeyType type);
    RawKeyType RawKey() const;

    void SetSplitSize(int64_t size);
    int64_t SplitSize() const;

    void SetMergeSize(int64_t size);
    int64_t MergeSize() const;

    void DisableWal();
    bool IsWalDisabled() const;

    /// 插入snapshot
    int32_t AddSnapshot(uint64_t snapshot);
    /// 获取snapshot
    uint64_t Snapshot(int32_t id) const;
    /// Snapshot数量
    int32_t SnapshotNum() const;

    /// acl
    void SetAdminGroup(const std::string& name);
    std::string AdminGroup() const;

    void SetAdmin(const std::string& name);
    std::string Admin() const;

    /// alias
    void SetAlias(const std::string& alias);
    std::string Alias() const;

private:
    TableDescriptor(const TableDescriptor&);
    void operator=(const TableDescriptor&);
    TableDescImpl* _impl;
};

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

class RowLock {
};

class Table;
/// 修改操作
class RowMutation {
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
        std::string family;
        std::string qualifier;
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
    virtual bool IsFinished() const = 0;
    /// mutation数量
    virtual uint32_t MutationNum() = 0;
    virtual uint32_t Size() = 0;
    virtual uint32_t RetryTimes() = 0;
    virtual const RowMutation::Mutation& GetMutation(uint32_t index) = 0;

private:
    RowMutation(const RowMutation&);
    void operator=(const RowMutation&);
};

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
    /// 设置异步返回
    virtual void SetAsync() = 0;
    /// 异步操作是否完成
    virtual bool IsFinished() const = 0;

    /// 获得结果错误码
    virtual ErrorCode GetError() = 0;
    /// 是否到达结束标记
    virtual bool Done() = 0;
    /// 迭代下一个cell
    virtual void Next() = 0;
    /// 读取的结果
    virtual std::string Value() = 0;
    virtual std::string Family() = 0;
    virtual std::string ColumnName() = 0;
    virtual std::string Qualifier() = 0;
    virtual int64_t Timestamp() = 0;

    virtual uint32_t GetReadColumnNum() = 0;
    virtual const ReadColumnList& GetReadColumnList() = 0;
    /// 将结果转存到一个std::map中, 格式为: map<column, map<timestamp, value>>
    typedef std::map< std::string, std::map<int64_t, std::string> > Map;
    virtual void ToMap(Map* rowmap) = 0;

private:
    RowReader(const RowReader&);
    void operator=(const RowReader&);
};

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

    virtual bool Flush() = 0;
    /// 条件修改, 指定Cell的值为value时, 才执行row_mu
    virtual bool CheckAndApply(const std::string& rowkey, const std::string& cf_c,
                               const std::string& value, const RowMutation& row_mu,
                               ErrorCode* err) = 0;
    /// 计数器++操作
    virtual int64_t IncrementColumnValue(const std::string& row, const std::string& family,
                                         const std::string& qualifier, int64_t amount,
                                         ErrorCode* err) = 0;
    /// 设置表格写操作默认超时
    virtual void SetWriteTimeout(int64_t timeout_ms) = 0;
    virtual void SetReadTimeout(int64_t timeout_ms) = 0;

    /// 创建行锁
    virtual bool LockRow(const std::string& rowkey, RowLock* lock, ErrorCode* err) = 0;

    /// 获取表格的最小最大key
    virtual bool GetStartEndKeys(std::string* start_key, std::string* end_key,
                                 ErrorCode* err) = 0;

    /// 获取表格分布信息
    virtual bool GetTabletLocation(std::vector<TabletInfo>* tablets,
                                   ErrorCode* err) = 0;
    /// 获取表格描述符
    virtual bool GetDescriptor(TableDescriptor* desc, ErrorCode* err) = 0;

    virtual void SetMaxMutationPendingNum(uint64_t max_pending_num) = 0;
    virtual void SetMaxReaderPendingNum(uint64_t max_pending_num) = 0;

private:
    Table(const Table&);
    void operator=(const Table&);
};

class Client {
public:
    /// 使用glog的用户必须调用此接口，避免glog被重复初始化
    static void SetGlogIsInitialized();

    static Client* NewClient(const std::string& confpath,
                             const std::string& log_prefix,
                             ErrorCode* err = NULL);

    static Client* NewClient(const std::string& confpath,
                             ErrorCode* err = NULL);

    static Client* NewClient();

    /// 创建表格
    virtual bool CreateTable(const TableDescriptor& desc, ErrorCode* err) = 0;
    virtual bool CreateTable(const TableDescriptor& desc,
                             const std::vector<std::string>& tablet_delim,
                             ErrorCode* err) = 0;
    /// 更新表格Schema
    virtual bool UpdateTable(const TableDescriptor& desc, ErrorCode* err) = 0;
    virtual bool UpdateCheck(const std::string& table_name, bool* done, ErrorCode* err) = 0;
    /// 删除表格
    virtual bool DeleteTable(const std::string& name, ErrorCode* err) = 0;
    /// 停止表格服务
    virtual bool DisableTable(const std::string& name, ErrorCode* err) = 0;
    /// 恢复表格服务
    virtual bool EnableTable(const std::string& name, ErrorCode* err) = 0;

    /// acl
    virtual bool CreateUser(const std::string& user,
                            const std::string& password, ErrorCode* err) = 0;
    virtual bool DeleteUser(const std::string& user, ErrorCode* err) = 0;
    virtual bool ChangePwd(const std::string& user,
                           const std::string& password, ErrorCode* err) = 0;
    virtual bool ShowUser(const std::string& user, std::vector<std::string>& user_groups,
                          ErrorCode* err) = 0;
    virtual bool AddUserToGroup(const std::string& user,
                                const std::string& group, ErrorCode* err) = 0;
    virtual bool DeleteUserFromGroup(const std::string& user,
                                     const std::string& group, ErrorCode* err) = 0;
    /// 打开表格, 失败返回NULL
    virtual Table* OpenTable(const std::string& table_name, ErrorCode* err) = 0;
    /// 获取表格分布信息
    virtual bool GetTabletLocation(const std::string& table_name,
                                   std::vector<TabletInfo>* tablets,
                                   ErrorCode* err) = 0;
    /// 获取表格Schema
    virtual TableDescriptor* GetTableDescriptor(const std::string& table_name,
                                                ErrorCode* err) = 0;

    virtual bool List(std::vector<TableInfo>* table_list,
                      ErrorCode* err) = 0;

    virtual bool List(const std::string& table_name,
                      TableInfo* table_info,
                      std::vector<TabletInfo>* tablet_list,
                      ErrorCode* err) = 0;

    virtual bool IsTableExist(const std::string& table_name, ErrorCode* err) = 0;

    virtual bool IsTableEnabled(const std::string& table_name, ErrorCode* err) = 0;

    virtual bool IsTableEmpty(const std::string& table_name, ErrorCode* err) = 0;

    virtual bool GetSnapshot(const std::string& name, uint64_t* snapshot, ErrorCode* err) = 0;
    virtual bool DelSnapshot(const std::string& name, uint64_t snapshot,ErrorCode* err) = 0;
    virtual bool Rollback(const std::string& name, uint64_t snapshot,
                          const std::string& rollback_name, ErrorCode* err) = 0;

    virtual bool CmdCtrl(const std::string& command,
                         const std::vector<std::string>& arg_list,
                         bool* bool_result,
                         std::string* str_result,
                         ErrorCode* err) = 0;
    virtual bool Rename(const std::string& old_table_name,
                        const std::string& new_table_name,
                        ErrorCode* err) = 0 ;
    Client() {}
    virtual ~Client() {}

private:
    Client(const Client&);
    void operator=(const Client&);
};
} // namespace tera
#endif  // TERA_TERA_H_
