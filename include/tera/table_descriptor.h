// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_TABLE_DESCRIPTOR_
#define  TERA_TABLE_DESCRIPTOR_

#include <stdint.h>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#pragma GCC visibility push(default)
namespace tera {

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

    // 事务
    void EnableTxn();
    bool IsTxnEnabled() const;

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

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_TABLE_DESCRIPTOR_
