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
    : _id(id),
      _name(cf_name),
      _lg_name(lg_name),
      _max_versions(1),
      _min_versions(1),
      _ttl(0),
      _disk_quota(-1),
      _type("") {
}

int32_t CFDescImpl::Id() const {
    return _id;
}

const std::string& CFDescImpl::Name() const {
    return _name;
}

const std::string& CFDescImpl::LocalityGroup() const {
    return _lg_name;
}

/// 历史版本保留时间, 不设置时为-1， 表示无限大永久保存
void CFDescImpl::SetTimeToLive(int32_t ttl) {
    _ttl = ttl;
}

int32_t CFDescImpl::TimeToLive() const {
    return _ttl;
}

/// 在TTL内,最多存储的版本数
void CFDescImpl::SetMaxVersions(int32_t max_versions) {
    _max_versions = max_versions;
}

int32_t CFDescImpl::MaxVersions() const {
    return _max_versions;
}

/// 最少存储的版本数,即使超出TTL,也至少保留min_versions个版本
void CFDescImpl::SetMinVersions(int32_t min_versions) {
    _min_versions = min_versions;
}

int32_t CFDescImpl::MinVersions() const {
    return _min_versions;
}

/// 存储限额, MBytes
void CFDescImpl::SetDiskQuota(int64_t quota) {
    _disk_quota = quota;
}

int64_t CFDescImpl::DiskQuota() const {
    return _disk_quota;
}

/// ACL
void CFDescImpl::SetAcl(ACL acl) {
}

ACL CFDescImpl::Acl() const {
    return ACL();
}

void CFDescImpl::SetType(const std::string& type) {
    _type = type;
}

const std::string& CFDescImpl::Type() const {
    return _type;
}

/// 局部性群组名字仅允许使用字母、数字和下划线构造,长度不超过256
LGDescImpl::LGDescImpl(const std::string& lg_name, int32_t id)
    : _id(id),
      _name(lg_name),
      _compress_type(kSnappyCompress),
      _store_type(kInDisk),
      _block_size(FLAGS_tera_tablet_write_block_size),
      _use_memtable_on_leveldb(false),
      _memtable_ldb_write_buffer_size(0),
      _memtable_ldb_block_size(0),
      _sst_size(FLAGS_tera_tablet_ldb_sst_size << 20){
}

/// Id read only
int32_t LGDescImpl::Id() const {
    return _id;
}

const std::string& LGDescImpl::Name() const {
    return _name;
}

/// Compress type
void LGDescImpl::SetCompress(CompressType type) {
    _compress_type = type;
}

CompressType LGDescImpl::Compress() const {
    return _compress_type;
}

/// Block size
void LGDescImpl::SetBlockSize(int block_size) {
    _block_size = block_size;
}

int LGDescImpl::BlockSize() const {
    return _block_size;
}

/// Store type
void LGDescImpl::SetStore(StoreType type) {
    _store_type = type;
}

StoreType LGDescImpl::Store() const {
    return _store_type;
}

/// Bloomfilter
void LGDescImpl::SetUseBloomfilter(bool use_bloomfilter) {
    _use_bloomfilter = use_bloomfilter;
}

bool LGDescImpl::UseBloomfilter() const {
    return _use_bloomfilter;
}

bool LGDescImpl::UseMemtableOnLeveldb() const {
    return _use_memtable_on_leveldb;
}

void LGDescImpl::SetUseMemtableOnLeveldb(bool use_mem_ldb) {
    _use_memtable_on_leveldb = use_mem_ldb;
}

int32_t LGDescImpl::MemtableLdbWriteBufferSize() const {
    return _memtable_ldb_write_buffer_size;
}

void LGDescImpl::SetMemtableLdbWriteBufferSize(int32_t buffer_size) {
    _memtable_ldb_write_buffer_size = buffer_size;
}

int32_t LGDescImpl::MemtableLdbBlockSize() const {
    return _memtable_ldb_block_size;
}

void LGDescImpl::SetMemtableLdbBlockSize(int32_t block_size) {
    _memtable_ldb_block_size = block_size;
}

int32_t LGDescImpl::SstSize() const {
    return _sst_size;
}

void LGDescImpl::SetSstSize(int32_t sst_size) {
    _sst_size = sst_size;
}

/// 表格名字仅允许使用字母、数字和下划线构造,长度不超过256
TableDescImpl::TableDescImpl(const std::string& tb_name)
    : _name(tb_name),
      _next_lg_id(0),
      _next_cf_id(0),
      _raw_key_type(kReadable),
      _split_size(FLAGS_tera_master_split_tablet_size),
      _merge_size(FLAGS_tera_master_merge_tablet_size),
      _disable_wal(false) {
}

/*
TableDescImpl::TableDescImpl(TableDescImpl& desc) {
    *this = desc;
}

TableDescImpl& TableDescImpl::operator=(const TableDescImpl& desc) {
    _name = desc._name;
    _next_lg_id = desc._next_lg_id;
    _next_cf_id = desc._next_cf_id;
    int32_t lg_num = desc._lgs.size();
    _lgs.resize(lg_num);
    for (int32_t i = 0; i < lg_num; i++) {
        _lgs[i] = new LGDescImpl(*desc._lgs[i]);
        _lg_map[desc._lgs[i]->Name()] = _lgs[i];
    }
    int32_t cf_num = desc._cfs.size();
    _cfs.resize(cf_num);
    for (int32_t i = 0; i < cf_num; i++) {
        _cfs[i] = new CFDescImpl(*desc._cfs[i]);
        _cf_map[desc._cfs[i]->Name()] = _cfs[i];
    }
    return *this;
}
*/

TableDescImpl::~TableDescImpl() {
    int32_t lg_num = _lgs.size();
    for (int32_t i = 0; i < lg_num; i++) {
        delete _lgs[i];
    }
    int32_t cf_num = _cfs.size();
    for (int32_t i = 0; i < cf_num; i++) {
        delete _cfs[i];
    }
}

void TableDescImpl::SetTableName(const std::string& name) {
    _name = name;
}

std::string TableDescImpl::TableName() const{
    return _name;
}

void TableDescImpl::SetAdminGroup(const std::string& name) {
    _admin_group = name;
}

std::string TableDescImpl::AdminGroup() const {
    return _admin_group;
}

void TableDescImpl::SetAlias(const std::string& alias) {
    _alias =  alias;
}

std::string TableDescImpl::Alias() const {
    return _alias;
}

/// 增加一个localitygroup
LocalityGroupDescriptor* TableDescImpl::AddLocalityGroup(const std::string& lg_name) {
    LGMap::iterator it = _lg_map.find(lg_name);
    if (it != _lg_map.end()) {
        return it->second;
    }
    int id = _next_lg_id ++;
    LGDescImpl* lg = new LGDescImpl(lg_name, id);
    _lg_map[lg_name] = lg;
    _lgs.push_back(lg);
    return lg;
}

LocalityGroupDescriptor* TableDescImpl::DefaultLocalityGroup() {
    return _lg_map.begin()->second;
}

/// 删除一个localitygroup
bool TableDescImpl::RemoveLocalityGroup(const std::string& lg_name) {
    if (_lg_map.size() == 1 && _lg_map.begin()->first == lg_name) {
        return false;
    }
    LGMap::iterator it = _lg_map.find(lg_name);
    if (it == _lg_map.end()) {
        return false;
    }
    LGDescImpl* lg = it->second;
    for (size_t i = 0; i < _cfs.size(); i++) {
        if (_cfs[i]->LocalityGroup() == lg->Name()) {
            return false;
        }
    }
    _lg_map.erase(it);
    _lgs[lg->Id()] = _lgs[_lgs.size() - 1];
    _lgs.resize(_lgs.size() - 1);
    delete lg;
    return true;
}

/// 获取localitygroup
const LocalityGroupDescriptor* TableDescImpl::LocalityGroup(int32_t id) const {
    if (id < static_cast<int32_t>(_lgs.size())) {
        return _lgs[id];
    }
    return NULL;
}

const LocalityGroupDescriptor* TableDescImpl::LocalityGroup(const std::string& lg_name) const {
    LGMap::const_iterator it = _lg_map.find(lg_name);
    if (it != _lg_map.end()) {
        return it->second;
    } else {
        return NULL;
    }
}

/// 获取localitygroup数量
int32_t TableDescImpl::LocalityGroupNum() const {
    return _lgs.size();
}

/// 增加一个columnfamily
ColumnFamilyDescriptor* TableDescImpl::AddColumnFamily(const std::string& cf_name,
                                        const std::string& lg_name) {
    LGMap::iterator it = _lg_map.find(lg_name);
    if (it == _lg_map.end()) {
        LOG(ERROR) << "lg:" << lg_name << " not exist.";
        return NULL;
    }
    CFMap::iterator cf_it = _cf_map.find(cf_name);
    if (cf_it != _cf_map.end()) {
        return cf_it->second;
    }
    int id = _next_cf_id ++;
    CFDescImpl* cf = new CFDescImpl(cf_name, id, lg_name);
    _cf_map[cf_name] = cf;
    _cfs.push_back(cf);
    return cf;
}

ColumnFamilyDescriptor* TableDescImpl::DefaultColumnFamily() {
    return _cf_map.begin()->second;
}

/// 删除一个columnfamily
void TableDescImpl::RemoveColumnFamily(const std::string& cf_name) {
    if (_cf_map.size() == 1 && _cf_map.begin()->first == cf_name) {
        return;
    }
    CFMap::iterator it = _cf_map.find(cf_name);
    if (it == _cf_map.end()) {
        return;
    }
    CFDescImpl* cf = it->second;
    _cf_map.erase(it);
    _cfs[cf->Id()] = _cfs[_cfs.size() - 1];
    _cfs.resize(_cfs.size() - 1);
    delete cf;
}

/// 获取所有的colmnfamily
const ColumnFamilyDescriptor* TableDescImpl::ColumnFamily(int32_t id) const {
    if (id < static_cast<int32_t>(_cfs.size())) {
        return _cfs[id];
    }
    return NULL;
}

const ColumnFamilyDescriptor* TableDescImpl::ColumnFamily(const std::string& cf_name) const {
    CFMap::const_iterator it = _cf_map.find(cf_name);
    if (it != _cf_map.end()) {
        return it->second;
    } else {
        return NULL;
    }
}

int32_t TableDescImpl::ColumnFamilyNum() const {
    return _cfs.size();
}

void TableDescImpl::SetRawKey(RawKeyType type) {
    _raw_key_type = type;
}

RawKeyType TableDescImpl::RawKey() const {
    return _raw_key_type;
}

void TableDescImpl::SetSplitSize(int64_t size) {
    _split_size = size;
}

int64_t TableDescImpl::SplitSize() const {
    return _split_size;
}

void TableDescImpl::SetMergeSize(int64_t size) {
    _merge_size = size;
}

int64_t TableDescImpl::MergeSize() const {
    return _merge_size;
}

void TableDescImpl::DisableWal() {
    _disable_wal = true;
}

bool TableDescImpl::IsWalDisabled() const {
    return _disable_wal;
}

/// 插入snapshot
int32_t TableDescImpl::AddSnapshot(uint64_t snapshot) {
    _snapshots.push_back(snapshot);
    return _snapshots.size();
}
/// 获取snapshot
uint64_t TableDescImpl::Snapshot(int32_t id) const {
    return _snapshots[id];
}
/// Snapshot数量
int32_t TableDescImpl::SnapshotNum() const {
    return _snapshots.size();
}
} // namespace tera
