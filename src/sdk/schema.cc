// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/schema_impl.h"
#include "sdk/tera.h"

namespace tera {

TableDescriptor::TableDescriptor(const std::string& tb_name) {
    _impl = new TableDescImpl(tb_name);
}
TableDescriptor::~TableDescriptor() {
    delete _impl;
    _impl = NULL;
}

/*
TableDescriptor::TableDescriptor(const TableDescriptor& desc) {
    _impl = new TableDescImpl(desc._impl->TableName());
    *_impl = *desc._impl;
}

TableDescriptor& TableDescriptor::operator=(const TableDescriptor& desc) {
    delete _impl;
    _impl = new TableDescImpl(desc._impl->TableName());
    *_impl = *desc._impl;
    return *this;
}
*/

void TableDescriptor::SetTableName(const std::string& name) {
    return _impl->SetTableName(name);
}

std::string TableDescriptor::TableName() const {
    return _impl->TableName();
}

/// 增加一个localitygroup, 名字仅允许使用字母、数字和下划线构造,长度不超过256
LocalityGroupDescriptor* TableDescriptor::AddLocalityGroup(const std::string& lg_name) {
    return _impl->AddLocalityGroup(lg_name);
}
LocalityGroupDescriptor* TableDescriptor::DefaultLocalityGroup() {
    return _impl->DefaultLocalityGroup();
}
/// 删除一个localitygroup,
bool TableDescriptor::RemoveLocalityGroup(const std::string& lg_name) {
    return _impl->RemoveLocalityGroup(lg_name);
}
/// 获取localitygroup
const LocalityGroupDescriptor* TableDescriptor::LocalityGroup(int32_t id) const {
    return _impl->LocalityGroup(id);
}
const LocalityGroupDescriptor* TableDescriptor::LocalityGroup(const std::string& name) const {
    return _impl->LocalityGroup(name);
}

/// LG数量
int32_t TableDescriptor::LocalityGroupNum() const {
    return _impl->LocalityGroupNum();
}

/// 增加一个columnfamily, 名字仅允许使用字母、数字和下划线构造,长度不超过256
ColumnFamilyDescriptor* TableDescriptor::AddColumnFamily(const std::string& cf_name,
            const std::string& lg_name) {
    return _impl->AddColumnFamily(cf_name, lg_name);
}
ColumnFamilyDescriptor* TableDescriptor::DefaultColumnFamily() {
    return _impl->DefaultColumnFamily();
}
/// 删除一个columnfamily
void TableDescriptor::RemoveColumnFamily(const std::string& cf_name) {
    return _impl->RemoveColumnFamily(cf_name);
}
/// 获取所有的colmnfamily
const ColumnFamilyDescriptor* TableDescriptor::ColumnFamily(int32_t id) const {
    return _impl->ColumnFamily(id);
}

const ColumnFamilyDescriptor* TableDescriptor::ColumnFamily(const std::string& cf_name) const {
    return _impl->ColumnFamily(cf_name);
}

/// CF数量
int32_t TableDescriptor::ColumnFamilyNum() const {
    return _impl->ColumnFamilyNum();
}

void TableDescriptor::SetRawKey(RawKeyType type) {
    _impl->SetRawKey(type);
}

RawKeyType TableDescriptor::RawKey() const {
    return _impl->RawKey();
}

void TableDescriptor::SetSplitSize(int64_t size) {
    _impl->SetSplitSize(size);
}

int64_t TableDescriptor::SplitSize() const {
    return _impl->SplitSize();
}

void TableDescriptor::SetMergeSize(int64_t size) {
    _impl->SetMergeSize(size);
}

int64_t TableDescriptor::MergeSize() const {
    return _impl->MergeSize();
}

void TableDescriptor::DisableWal() {
    _impl->DisableWal();
}

bool TableDescriptor::IsWalDisabled() const {
    return _impl->IsWalDisabled();
}

int32_t TableDescriptor::AddSnapshot(uint64_t snapshot) {
    return _impl->AddSnapshot(snapshot);
}

uint64_t TableDescriptor::Snapshot(int32_t id) const {
    return _impl->Snapshot(id);
}
/// Snapshot数量
int32_t TableDescriptor::SnapshotNum() const {
    return _impl->SnapshotNum();
}

void TableDescriptor::SetAdminGroup(const std::string& name) {
    return _impl->SetAdminGroup(name);
}

std::string TableDescriptor::AdminGroup() const {
    return _impl->AdminGroup();
}

void TableDescriptor::SetAlias(const std::string& alias) {
    return _impl->SetAlias(alias);
}

std::string TableDescriptor::Alias() const {
    return _impl->Alias();
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
