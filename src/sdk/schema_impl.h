// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_SCHEMA_IMPL_H_
#define  TERA_SDK_SCHEMA_IMPL_H_

#include <string>

#include "proto/table_meta.pb.h"
#include "tera.h"

namespace tera {

/// 列族描述
class CFDescImpl : public ColumnFamilyDescriptor {
public:
    /// 列族名字仅允许使用字母、数字和下划线构造, 长度不超过256
    CFDescImpl(const std::string& cf_name, int32_t id, const std::string& lg_name);
    /// id
    int32_t Id() const;
    const std::string& Name() const;

    const std::string& LocalityGroup() const;

    /// 历史版本保留时间, 不设置时为0， 表示无限大永久保存
    void SetTimeToLive(int32_t ttl);

    int32_t TimeToLive() const;

    /// 在TTL内,最多存储的版本数
    void SetMaxVersions(int32_t max_versions);

    int32_t MaxVersions() const;

    /// 最少存储的版本数,即使超出TTL,也至少保留min_versions个版本
    void SetMinVersions(int32_t min_versions);

    int32_t MinVersions() const;

    /// 存储限额, MBytes
    void SetDiskQuota(int64_t quota);

    int64_t DiskQuota() const;

    /// ACL
    void SetAcl(ACL acl);

    ACL Acl() const;

    void EnableGlobalTransaction();

    void DisableGlobalTransaction();
    
    bool GlobalTransaction() const;

    void EnableNotify();

    void DisableNotify();

    bool IsNotifyEnabled() const;

    void SetType(const std::string& type);

    const std::string& Type() const;

private:
    int32_t id_;
    std::string name_;
    std::string lg_name_;
    int32_t max_versions_;
    int32_t min_versions_;
    int32_t ttl_;
    int64_t acl_;
    int32_t owner_;
    int32_t disk_quota_;
    std::string type_;
    bool is_global_transaction_;
    bool is_notify_enabled_;
};

/// 局部性群组描述
class LGDescImpl : public LocalityGroupDescriptor {
public:
    /// 局部性群组名字仅允许使用字母、数字和下划线构造,长度不超过256
    LGDescImpl(const std::string& lg_name, int32_t id);

    /// Id read only
    int32_t Id() const;

    /// Name read only
    const std::string& Name() const;

    /// Compress type
    void SetCompress(CompressType type);

    CompressType Compress() const;

    /// Block size
    void SetBlockSize(int block_size);

    int BlockSize() const;

    /// Store type
    void SetStore(StoreType type);

    StoreType Store() const;

    /// Bloomfilter
    void SetUseBloomfilter(bool use_bloomfilter);

    bool UseBloomfilter() const;

    /// Memtable On Leveldb (disable/enable)
    bool UseMemtableOnLeveldb() const;

    void SetUseMemtableOnLeveldb(bool use_mem_ldb);

    /// Memtable-LDB WriteBuffer Size
    int32_t MemtableLdbWriteBufferSize() const;

    void SetMemtableLdbWriteBufferSize(int32_t buffer_size);

    /// Memtable-LDB Block Size
    int32_t MemtableLdbBlockSize() const;

    void SetMemtableLdbBlockSize(int32_t block_size);

    /// sst file size, in Bytes
    int32_t SstSize() const;
    void SetSstSize(int32_t sst_size);

private:
    int32_t         id_;
    std::string     name_;
    CompressType    compress_type_;
    StoreType       store_type_;
    int             block_size_;
    bool            use_bloomfilter_;
    bool            use_memtable_on_leveldb_;
    int32_t         memtable_ldb_write_buffer_size_;
    int32_t         memtable_ldb_block_size_;
    int32_t         sst_size_; // in bytes
};

/// 表描述符.
class TableDescImpl {
public:
    /// 表格名字仅允许使用字母、数字和下划线构造,长度不超过256
    TableDescImpl(const std::string& tb_name);
    ~TableDescImpl();
    void SetTableName(const std::string& name);
    std::string TableName() const;
    /// 增加一个localitygroup
    LocalityGroupDescriptor* AddLocalityGroup(const std::string& lg_name);
    /// 获取默认localitygroup，仅用于kv表
    LocalityGroupDescriptor* DefaultLocalityGroup();
    /// 删除一个localitygroup
    bool RemoveLocalityGroup(const std::string& lg_name);
    /// 获取localitygroup
    const LocalityGroupDescriptor* LocalityGroup(int32_t id) const;
    const LocalityGroupDescriptor* LocalityGroup(const std::string& lg_name) const;
    /// 获取localitygroup数量
    int32_t LocalityGroupNum() const;
    /// 增加一个columnfamily
    ColumnFamilyDescriptor* AddColumnFamily(const std::string& cf_name,
                                            const std::string& lg_name);
    ColumnFamilyDescriptor* DefaultColumnFamily();
    /// 删除一个columnfamily
    void RemoveColumnFamily(const std::string& cf_name);
    /// 获取所有的colmnfamily
    const ColumnFamilyDescriptor* ColumnFamily(int32_t id) const;
    const ColumnFamilyDescriptor* ColumnFamily(const std::string& cf_name) const;
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

    void EnableTxn();
    bool IsTxnEnabled() const;

    /// 插入snapshot
    int32_t AddSnapshot(uint64_t snapshot);
    /// 获取snapshot
    uint64_t Snapshot(int32_t id) const;
    /// Snapshot数量
    int32_t SnapshotNum() const;

    void SetAdminGroup(const std::string& name);
    std::string AdminGroup() const;

    void SetAdmin(const std::string& name);
    std::string Admin() const;

    static const std::string DEFAULT_LG_NAME;
    static const std::string NOTIFY_LG_NAME;
    static const std::string DEFAULT_CF_NAME;

private:
    typedef std::map<std::string, LGDescImpl*> LGMap;
    typedef std::map<std::string, CFDescImpl*> CFMap;
    std::string     name_;
    LGMap           lg_map_;
    std::vector<LGDescImpl*> lgs_;
    CFMap           cf_map_;
    std::vector<CFDescImpl*> cfs_;
    int32_t         next_lg_id_;
    int32_t         next_cf_id_;
    std::vector<uint64_t> snapshots_;
    RawKeyType      raw_key_type_;
    int64_t         split_size_;
    int64_t         merge_size_;
    bool            disable_wal_;
    bool            enable_txn_;
    std::string     admin_group_;
    std::string     admin_;
};

} // namespace tera

#endif  // TERA_SDK_SCHEMA_IMPL_H_
