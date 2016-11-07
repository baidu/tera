// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/schema_impl.h"
#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_int64(tera_tablet_write_block_size);
DECLARE_int64(tera_tablet_ldb_sst_size);
DECLARE_int64(tera_master_split_tablet_size);
DECLARE_int64(tera_master_merge_tablet_size);

namespace tera {

const std::string TableDescImpl::DEFAULT_LG_NAME = "lg0";
const std::string TableDescImpl::DEFAULT_CF_NAME = "";

/// 列族名字仅允许使用字母、数字和下划线构造, 长度不超过256
CFDescImpl::CFDescImpl(const std::string& cf_name,
                       int32_t id,
                       const std::string& lg_name)
    : id_(id),
      name_(cf_name),
      lg_name_(lg_name),
      max_versions_(1),
      min_versions_(1),
      ttl_(0),
      disk_quota_(-1),
      type_("") {
}

int32_t CFDescImpl::Id() const {
    return id_;
}

const std::string& CFDescImpl::Name() const {
    return name_;
}

const std::string& CFDescImpl::LocalityGroup() const {
    return lg_name_;
}

/// 历史版本保留时间, 不设置时为-1， 表示无限大永久保存
void CFDescImpl::SetTimeToLive(int32_t ttl) {
    ttl_ = ttl;
}

int32_t CFDescImpl::TimeToLive() const {
    return ttl_;
}

/// 在TTL内,最多存储的版本数
void CFDescImpl::SetMaxVersions(int32_t max_versions) {
    max_versions_ = max_versions;
}

int32_t CFDescImpl::MaxVersions() const {
    return max_versions_;
}

/// 最少存储的版本数,即使超出TTL,也至少保留min_versions个版本
void CFDescImpl::SetMinVersions(int32_t min_versions) {
    min_versions_ = min_versions;
}

int32_t CFDescImpl::MinVersions() const {
    return min_versions_;
}

/// 存储限额, MBytes
void CFDescImpl::SetDiskQuota(int64_t quota) {
    disk_quota_ = quota;
}

int64_t CFDescImpl::DiskQuota() const {
    return disk_quota_;
}

/// ACL
void CFDescImpl::SetAcl(ACL acl) {
}

ACL CFDescImpl::Acl() const {
    return ACL();
}

void CFDescImpl::SetType(const std::string& type) {
    type_ = type;
}

const std::string& CFDescImpl::Type() const {
    return type_;
}

/// 局部性群组名字仅允许使用字母、数字和下划线构造,长度不超过256
LGDescImpl::LGDescImpl(const std::string& lg_name, int32_t id)
    : id_(id),
      name_(lg_name),
      compress_type_(kSnappyCompress),
      store_type_(kInDisk),
      block_size_(FLAGS_tera_tablet_write_block_size),
      use_memtable_on_leveldb_(false),
      memtable_ldb_write_buffer_size_(0),
      memtable_ldb_block_size_(0),
      sst_size_(FLAGS_tera_tablet_ldb_sst_size << 20){
}

/// Id read only
int32_t LGDescImpl::Id() const {
    return id_;
}

const std::string& LGDescImpl::Name() const {
    return name_;
}

/// Compress type
void LGDescImpl::SetCompress(CompressType type) {
    compress_type_ = type;
}

CompressType LGDescImpl::Compress() const {
    return compress_type_;
}

/// Block size
void LGDescImpl::SetBlockSize(int block_size) {
    block_size_ = block_size;
}

int LGDescImpl::BlockSize() const {
    return block_size_;
}

/// Store type
void LGDescImpl::SetStore(StoreType type) {
    store_type_ = type;
}

StoreType LGDescImpl::Store() const {
    return store_type_;
}

/// Bloomfilter
void LGDescImpl::SetUseBloomfilter(bool use_bloomfilter) {
    use_bloomfilter_ = use_bloomfilter;
}

bool LGDescImpl::UseBloomfilter() const {
    return use_bloomfilter_;
}

bool LGDescImpl::UseMemtableOnLeveldb() const {
    return use_memtable_on_leveldb_;
}

void LGDescImpl::SetUseMemtableOnLeveldb(bool use_mem_ldb) {
    use_memtable_on_leveldb_ = use_mem_ldb;
}

int32_t LGDescImpl::MemtableLdbWriteBufferSize() const {
    return memtable_ldb_write_buffer_size_;
}

void LGDescImpl::SetMemtableLdbWriteBufferSize(int32_t buffer_size) {
    memtable_ldb_write_buffer_size_ = buffer_size;
}

int32_t LGDescImpl::MemtableLdbBlockSize() const {
    return memtable_ldb_block_size_;
}

void LGDescImpl::SetMemtableLdbBlockSize(int32_t block_size) {
    memtable_ldb_block_size_ = block_size;
}

int32_t LGDescImpl::SstSize() const {
    return sst_size_;
}

void LGDescImpl::SetSstSize(int32_t sst_size) {
    sst_size_ = sst_size;
}

/// 表格名字仅允许使用字母、数字和下划线构造,长度不超过256
TableDescImpl::TableDescImpl(const std::string& tb_name)
    : name_(tb_name),
      next_lg_id_(0),
      next_cf_id_(0),
      raw_key_type_(kBinary),
      split_size_(FLAGS_tera_master_split_tablet_size),
      merge_size_(FLAGS_tera_master_merge_tablet_size),
      disable_wal_(false),
      enable_txn_(false) {
}

/*
TableDescImpl::TableDescImpl(TableDescImpl& desc) {
    *this = desc;
}

TableDescImpl& TableDescImpl::operator=(const TableDescImpl& desc) {
    name_ = desc.name_;
    next_lg_id_ = desc.next_lg_id_;
    next_cf_id_ = desc.next_cf_id_;
    int32_t lg_num = desc.lgs_.size();
    lgs_.resize(lg_num);
    for (int32_t i = 0; i < lg_num; i++) {
        lgs_[i] = new LGDescImpl(*desc.lgs_[i]);
        lg_map_[desc.lgs_[i]->Name()] = lgs_[i];
    }
    int32_t cf_num = desc.cfs_.size();
    cfs_.resize(cf_num);
    for (int32_t i = 0; i < cf_num; i++) {
        cfs_[i] = new CFDescImpl(*desc.cfs_[i]);
        cf_map_[desc.cfs_[i]->Name()] = cfs_[i];
    }
    return *this;
}
*/

TableDescImpl::~TableDescImpl() {
    int32_t lg_num = lgs_.size();
    for (int32_t i = 0; i < lg_num; i++) {
        delete lgs_[i];
    }
    int32_t cf_num = cfs_.size();
    for (int32_t i = 0; i < cf_num; i++) {
        delete cfs_[i];
    }
}

void TableDescImpl::SetTableName(const std::string& name) {
    name_ = name;
}

std::string TableDescImpl::TableName() const{
    return name_;
}

void TableDescImpl::SetAdminGroup(const std::string& name) {
    admin_group_ = name;
}

std::string TableDescImpl::AdminGroup() const {
    return admin_group_;
}

void TableDescImpl::SetAdmin(const std::string& name) {
    admin_ = name;
}

std::string TableDescImpl::Admin() const {
    return admin_;
}

void TableDescImpl::SetAlias(const std::string& alias) {
    alias_ =  alias;
}

std::string TableDescImpl::Alias() const {
    return alias_;
}

/// 增加一个localitygroup
LocalityGroupDescriptor* TableDescImpl::AddLocalityGroup(const std::string& lg_name) {
    LGMap::iterator it = lg_map_.find(lg_name);
    if (it != lg_map_.end()) {
        return it->second;
    }
    int id = next_lg_id_ ++;
    LGDescImpl* lg = new LGDescImpl(lg_name, id);
    lg_map_[lg_name] = lg;
    lgs_.push_back(lg);
    return lg;
}

LocalityGroupDescriptor* TableDescImpl::DefaultLocalityGroup() {
    return lg_map_.begin()->second;
}

/// 删除一个localitygroup
bool TableDescImpl::RemoveLocalityGroup(const std::string& lg_name) {
    if (lg_map_.size() == 1 && lg_map_.begin()->first == lg_name) {
        return false;
    }
    LGMap::iterator it = lg_map_.find(lg_name);
    if (it == lg_map_.end()) {
        return false;
    }
    LGDescImpl* lg = it->second;
    for (size_t i = 0; i < cfs_.size(); i++) {
        if (cfs_[i]->LocalityGroup() == lg->Name()) {
            return false;
        }
    }
    lg_map_.erase(it);
    lgs_[lg->Id()] = lgs_[lgs_.size() - 1];
    lgs_.resize(lgs_.size() - 1);
    delete lg;
    return true;
}

/// 获取localitygroup
const LocalityGroupDescriptor* TableDescImpl::LocalityGroup(int32_t id) const {
    if (id < static_cast<int32_t>(lgs_.size())) {
        return lgs_[id];
    }
    return NULL;
}

const LocalityGroupDescriptor* TableDescImpl::LocalityGroup(const std::string& lg_name) const {
    LGMap::const_iterator it = lg_map_.find(lg_name);
    if (it != lg_map_.end()) {
        return it->second;
    } else {
        return NULL;
    }
}

/// 获取localitygroup数量
int32_t TableDescImpl::LocalityGroupNum() const {
    return lgs_.size();
}

/// 增加一个columnfamily
ColumnFamilyDescriptor* TableDescImpl::AddColumnFamily(const std::string& cf_name,
                                        const std::string& lg_name) {
    LGMap::iterator it = lg_map_.find(lg_name);
    if (it == lg_map_.end()) {
        LOG(ERROR) << "lg:" << lg_name << " not exist.";
        return NULL;
    }
    CFMap::iterator cf_it = cf_map_.find(cf_name);
    if (cf_it != cf_map_.end()) {
        return cf_it->second;
    }
    int id = next_cf_id_ ++;
    CFDescImpl* cf = new CFDescImpl(cf_name, id, lg_name);
    cf_map_[cf_name] = cf;
    cfs_.push_back(cf);
    return cf;
}

ColumnFamilyDescriptor* TableDescImpl::DefaultColumnFamily() {
    return cf_map_.begin()->second;
}

/// 删除一个columnfamily
void TableDescImpl::RemoveColumnFamily(const std::string& cf_name) {
    if (cf_map_.size() == 1 && cf_map_.begin()->first == cf_name) {
        return;
    }
    CFMap::iterator it = cf_map_.find(cf_name);
    if (it == cf_map_.end()) {
        return;
    }
    CFDescImpl* cf = it->second;
    cf_map_.erase(it);
    cfs_[cf->Id()] = cfs_[cfs_.size() - 1];
    cfs_.resize(cfs_.size() - 1);
    delete cf;
}

/// 获取所有的colmnfamily
const ColumnFamilyDescriptor* TableDescImpl::ColumnFamily(int32_t id) const {
    if (id < static_cast<int32_t>(cfs_.size())) {
        return cfs_[id];
    }
    return NULL;
}

const ColumnFamilyDescriptor* TableDescImpl::ColumnFamily(const std::string& cf_name) const {
    CFMap::const_iterator it = cf_map_.find(cf_name);
    if (it != cf_map_.end()) {
        return it->second;
    } else {
        return NULL;
    }
}

int32_t TableDescImpl::ColumnFamilyNum() const {
    return cfs_.size();
}

void TableDescImpl::SetRawKey(RawKeyType type) {
    raw_key_type_ = type;
}

RawKeyType TableDescImpl::RawKey() const {
    return raw_key_type_;
}

void TableDescImpl::SetSplitSize(int64_t size) {
    split_size_ = size;
}

int64_t TableDescImpl::SplitSize() const {
    return split_size_;
}

void TableDescImpl::SetMergeSize(int64_t size) {
    merge_size_ = size;
}

int64_t TableDescImpl::MergeSize() const {
    return merge_size_;
}

void TableDescImpl::DisableWal() {
    disable_wal_ = true;
}

bool TableDescImpl::IsWalDisabled() const {
    return disable_wal_;
}

void TableDescImpl::EnableTxn() {
    enable_txn_ = true;
}

bool TableDescImpl::IsTxnEnabled() const {
    return enable_txn_;
}

/// 插入snapshot
int32_t TableDescImpl::AddSnapshot(uint64_t snapshot) {
    snapshots_.push_back(snapshot);
    return snapshots_.size();
}
/// 获取snapshot
uint64_t TableDescImpl::Snapshot(int32_t id) const {
    return snapshots_[id];
}
/// Snapshot数量
int32_t TableDescImpl::SnapshotNum() const {
    return snapshots_.size();
}
} // namespace tera
