// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/scan_impl.h"
#include "tera.h"

namespace tera {

ScanDescriptor::ScanDescriptor(const std::string& rowkey) { impl_ = new ScanDescImpl(rowkey); }

ScanDescriptor::~ScanDescriptor() { delete impl_; }

void ScanDescriptor::SetEnd(const std::string& rowkey) { impl_->SetEnd(rowkey); }

void ScanDescriptor::AddColumnFamily(const std::string& cf) { impl_->AddColumnFamily(cf); }

void ScanDescriptor::AddColumn(const std::string& cf, const std::string& qualifier) {
  impl_->AddColumn(cf, qualifier);
}

void ScanDescriptor::SetMaxVersions(int32_t versions) { impl_->SetMaxVersions(versions); }

void ScanDescriptor::SetMaxQualifiers(uint64_t max_qualifiers) {
  impl_->SetMaxQualifiers(max_qualifiers);
}

void ScanDescriptor::SetPackInterval(int64_t interval) { impl_->SetPackInterval(interval); }

void ScanDescriptor::SetTimeRange(int64_t ts_end, int64_t ts_start) {
  impl_->SetTimeRange(ts_end, ts_start);
}

bool ScanDescriptor::SetFilter(const filter::FilterPtr& filter) { return impl_->SetFilter(filter); }

void ScanDescriptor::SetSnapshot(uint64_t snapshot_id) { return impl_->SetSnapshot(snapshot_id); }

void ScanDescriptor::SetBufferSize(int64_t buf_size) { impl_->SetBufferSize(buf_size); }

void ScanDescriptor::SetNumberLimit(int64_t number_limit) { impl_->SetNumberLimit(number_limit); }

int64_t ScanDescriptor::GetNumberLimit() { return impl_->GetNumberLimit(); }

ScanDescImpl* ScanDescriptor::GetImpl() const { return impl_; }

}  // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
