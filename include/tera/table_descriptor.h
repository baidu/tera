// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// TableDescriptor/LocalityGroupDescriptor/ColumnFamilyDescriptor are used to
// describe the schema of a table.
// It's not a nessesary to use these classes. User is encouraged to describe a table
// by string which is easier. See more:
//   https://github.com/baidu/tera/blob/master/doc/sdk_reference/table_descriptor.md

#ifndef  TERA_TABLE_DESCRIPTOR_
#define  TERA_TABLE_DESCRIPTOR_

#include <stdint.h>
#include <string>

#pragma GCC visibility push(default)
namespace tera {

extern const int64_t kLatestTimestamp;
extern const int64_t kOldestTimestamp;

struct ACL {
    int32_t owner;
    int32_t role;
    int64_t acl;
};

class ColumnFamilyDescriptor {
public:
    // Returns name of this column family
    virtual const std::string& Name() const = 0;

    // Set/get TTL(in second) of cells of this column family.
    virtual void SetTimeToLive(int32_t ttl) = 0;
    virtual int32_t TimeToLive() const = 0;

    // Set/get maximum versions of cells of this column family.
    virtual void SetMaxVersions(int32_t max_versions) = 0;
    virtual int32_t MaxVersions() const = 0;

    // Get name of locality group which this column family belong to.
    virtual const std::string& LocalityGroup() const = 0;

    // Get internal id.
    virtual int32_t Id() const = 0;

    // DEVELOPING
    virtual void SetType(const std::string& type) = 0;
    virtual const std::string& Type() const = 0;
    virtual void SetMinVersions(int32_t min_versions) = 0;
    virtual int32_t MinVersions() const = 0;
    virtual void SetDiskQuota(int64_t quota) = 0;
    virtual int64_t DiskQuota() const = 0;
    virtual void SetAcl(ACL acl) = 0;
    virtual ACL Acl() const = 0;
    virtual void EnableGlobalTransaction() = 0;
    virtual void DisableGlobalTransaction() = 0;
    virtual bool GlobalTransaction() const = 0;
    virtual void EnableNotify() = 0;
    virtual void DisableNotify() = 0;
    virtual bool IsNotifyEnabled() const = 0;

    ColumnFamilyDescriptor() {}
    virtual ~ColumnFamilyDescriptor() {}

private:
    ColumnFamilyDescriptor(const ColumnFamilyDescriptor&);
    void operator=(const ColumnFamilyDescriptor&);
};

// Describes compress type for a locality group
enum CompressType {
    kNoneCompress = 1,
    kSnappyCompress = 2,
};

// Describes store type for a locality group
enum StoreType {
    kInDisk = 0,
    kInFlash = 1,
    kInMemory = 2,
};

class LocalityGroupDescriptor {
public:
    // Returns name of this locality group
    virtual const std::string& Name() const = 0;

    // Set/get store medium.
    virtual void SetStore(StoreType type) = 0;
    virtual StoreType Store() const = 0;

    // Set/get block size in KB.
    virtual void SetBlockSize(int block_size) = 0;
    virtual int BlockSize() const = 0;

    // Set/get sst size in MB.
    virtual int32_t SstSize() const = 0;
    virtual void SetSstSize(int32_t sst_size) = 0;

    // Set/get compress type.
    virtual void SetCompress(CompressType type) = 0;
    virtual CompressType Compress() const = 0;

    // Set/get if use bloomfilter.
    virtual void SetUseBloomfilter(bool use_bloomfilter) = 0;
    virtual bool UseBloomfilter() const = 0;

    // Memtable on leveldb (disable/enable)
    virtual bool UseMemtableOnLeveldb() const = 0;
    virtual void SetUseMemtableOnLeveldb(bool use_mem_ldb) = 0;

    // Memtable-LDB write buffer size
    virtual int32_t MemtableLdbWriteBufferSize() const = 0;
    virtual void SetMemtableLdbWriteBufferSize(int32_t buffer_size) = 0;

    // Memtable-LDB block size
    virtual int32_t MemtableLdbBlockSize() const = 0;
    virtual void SetMemtableLdbBlockSize(int32_t block_size) = 0;

    // Get internal id.
    virtual int32_t Id() const = 0;

    LocalityGroupDescriptor() {}
    virtual ~LocalityGroupDescriptor() {}

private:
    LocalityGroupDescriptor(const LocalityGroupDescriptor&);
    void operator=(const LocalityGroupDescriptor&);
};

// Describes internal raw key type
enum RawKeyType {
    kReadable = 0,
    kBinary = 1,
    kTTLKv = 2,
    kGeneralKv = 3,
};

class TableDescImpl;
class TableDescriptor {
public:
    // Only {[a-z],[A-Z],[0-9],'_','-'} are allowed in table name.
    // Length of a table name should be less than 256.
    TableDescriptor(const std::string& name = "");
    ~TableDescriptor();

    // Set/Get table name
    void SetTableName(const std::string& name);
    std::string TableName() const;

    // Add a locality group named "lg_name".
    // Only {[a-z],[A-Z],[0-9],'_','-'} are allowed in locality group name.
    // Length of a locality group name should be less than 256.
    LocalityGroupDescriptor* AddLocalityGroup(const std::string& lg_name);
    // Remove a locality group by name. Operation returns false if there is a
    // column family in this locality group.
    bool RemoveLocalityGroup(const std::string& lg_name);
    // Get locality group handle by id.
    const LocalityGroupDescriptor* LocalityGroup(int32_t id) const;
    // Get locality group handle by name.
    const LocalityGroupDescriptor* LocalityGroup(const std::string& lg_name) const;
    // Return locality group number in this table.
    int32_t LocalityGroupNum() const;

    // Add a column family named "cf_name" into the locality group named
    // "lg_name".
    // Only {[a-z],[A-Z],[0-9],'_','-'} are allowed in column family name.
    // Length of a column family name should be less than 256.
    ColumnFamilyDescriptor* AddColumnFamily(const std::string& cf_name,
                                            const std::string& lg_name = "lg0");
    // Remove a column family by name.
    void RemoveColumnFamily(const std::string& cf_name);
    // Get column family handle by id.
    const ColumnFamilyDescriptor* ColumnFamily(int32_t id) const;
    // Get column family handle by name.
    const ColumnFamilyDescriptor* ColumnFamily(const std::string& cf_name) const;
    // Return column family number in this table.
    int32_t ColumnFamilyNum() const;

    // Set/get raw key type.
    void SetRawKey(RawKeyType type);
    RawKeyType RawKey() const;

    // Set/get the split size in MB.
    void SetSplitSize(int64_t size);
    int64_t SplitSize() const;

    // Set/get the merge size in MB.
    // mergesize should be less than splitsize / 3.
    void SetMergeSize(int64_t size);
    int64_t MergeSize() const;

    // Enable/disable write-ahead-log on this table.
    void DisableWal();
    bool IsWalDisabled() const;

    // Enable/disable transaction on this table.
    void EnableTxn();
    bool IsTxnEnabled() const;

    // Set/get admin of this table.
    void SetAdmin(const std::string& name);
    std::string Admin() const;

    // Set/get admin group of this table.
    void SetAdminGroup(const std::string& name);
    std::string AdminGroup() const;

    // DEVELOPING
    int32_t AddSnapshot(uint64_t snapshot);
    uint64_t Snapshot(int32_t id) const;
    int32_t SnapshotNum() const;

    // DEPRECATED
    void SetAlias(const std::string& alias);
    std::string Alias() const;
    LocalityGroupDescriptor* DefaultLocalityGroup();
    ColumnFamilyDescriptor* DefaultColumnFamily();

private:
    TableDescriptor(const TableDescriptor&);
    void operator=(const TableDescriptor&);
    TableDescImpl* impl_;
};

} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_TABLE_DESCRIPTOR_
