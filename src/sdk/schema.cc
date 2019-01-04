// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/schema_impl.h"
#include "tera.h"

namespace tera {

TableDescriptor::TableDescriptor(const std::string& tb_name) { impl_ = new TableDescImpl(tb_name); }
TableDescriptor::~TableDescriptor() {
  delete impl_;
  impl_ = NULL;
}

/*
TableDescriptor::TableDescriptor(const TableDescriptor& desc) {
    impl_ = new TableDescImpl(desc.impl_->TableName());
    *impl_ = *desc.impl_;
}

TableDescriptor& TableDescriptor::operator=(const TableDescriptor& desc) {
    delete impl_;
    impl_ = new TableDescImpl(desc.impl_->TableName());
    *impl_ = *desc.impl_;
    return *this;
}
*/

void TableDescriptor::SetTableName(const std::string& name) { return impl_->SetTableName(name); }

std::string TableDescriptor::TableName() const { return impl_->TableName(); }

/// 增加一个localitygroup, 名字仅允许使用字母、数字和下划线构造,长度不超过256
LocalityGroupDescriptor* TableDescriptor::AddLocalityGroup(const std::string& lg_name) {
  return impl_->AddLocalityGroup(lg_name);
}
LocalityGroupDescriptor* TableDescriptor::DefaultLocalityGroup() {
  return impl_->DefaultLocalityGroup();
}
/// 删除一个localitygroup,
bool TableDescriptor::RemoveLocalityGroup(const std::string& lg_name) {
  return impl_->RemoveLocalityGroup(lg_name);
}
/// 获取localitygroup
const LocalityGroupDescriptor* TableDescriptor::LocalityGroup(int32_t id) const {
  return impl_->LocalityGroup(id);
}
const LocalityGroupDescriptor* TableDescriptor::LocalityGroup(const std::string& name) const {
  return impl_->LocalityGroup(name);
}

/// LG数量
int32_t TableDescriptor::LocalityGroupNum() const { return impl_->LocalityGroupNum(); }

/// 增加一个columnfamily, 名字仅允许使用字母、数字和下划线构造,长度不超过256
ColumnFamilyDescriptor* TableDescriptor::AddColumnFamily(const std::string& cf_name,
                                                         const std::string& lg_name) {
  return impl_->AddColumnFamily(cf_name, lg_name);
}
ColumnFamilyDescriptor* TableDescriptor::DefaultColumnFamily() {
  return impl_->DefaultColumnFamily();
}
/// 删除一个columnfamily
void TableDescriptor::RemoveColumnFamily(const std::string& cf_name) {
  return impl_->RemoveColumnFamily(cf_name);
}
/// 获取所有的colmnfamily
const ColumnFamilyDescriptor* TableDescriptor::ColumnFamily(int32_t id) const {
  return impl_->ColumnFamily(id);
}

const ColumnFamilyDescriptor* TableDescriptor::ColumnFamily(const std::string& cf_name) const {
  return impl_->ColumnFamily(cf_name);
}

/// CF数量
int32_t TableDescriptor::ColumnFamilyNum() const { return impl_->ColumnFamilyNum(); }

void TableDescriptor::SetRawKey(RawKeyType type) { impl_->SetRawKey(type); }

RawKeyType TableDescriptor::RawKey() const { return impl_->RawKey(); }

void TableDescriptor::SetSplitSize(int64_t size) { impl_->SetSplitSize(size); }

int64_t TableDescriptor::SplitSize() const { return impl_->SplitSize(); }

void TableDescriptor::SetMergeSize(int64_t size) { impl_->SetMergeSize(size); }

int64_t TableDescriptor::MergeSize() const { return impl_->MergeSize(); }

void TableDescriptor::DisableWal() { impl_->DisableWal(); }

bool TableDescriptor::IsWalDisabled() const { return impl_->IsWalDisabled(); }

void TableDescriptor::EnableTxn() { impl_->EnableTxn(); }

bool TableDescriptor::IsTxnEnabled() const { return impl_->IsTxnEnabled(); }

int32_t TableDescriptor::AddSnapshot(uint64_t snapshot) { return impl_->AddSnapshot(snapshot); }

uint64_t TableDescriptor::Snapshot(int32_t id) const { return impl_->Snapshot(id); }
/// Snapshot数量
int32_t TableDescriptor::SnapshotNum() const { return impl_->SnapshotNum(); }

void TableDescriptor::SetAdminGroup(const std::string& name) { return impl_->SetAdminGroup(name); }

void TableDescriptor::SetAdmin(const std::string& name) { return impl_->SetAdmin(name); }

std::string TableDescriptor::AdminGroup() const { return impl_->AdminGroup(); }

std::string TableDescriptor::Admin() const { return impl_->Admin(); }

void TableDescriptor::EnableHash() { impl_->EnableHash(); }
bool TableDescriptor::IsHashEnabled() const { return impl_->IsHashEnabled(); }

uint32_t TableDescriptor::BloomFilterBitsPerKey() const { return impl_->BloomFilterBitsPerKey(); }
void TableDescriptor::SetBloomFilterBitsPerKey(uint32_t val) {
  return impl_->SetBloomFilterBitsPerKey(val);
}

}  // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
