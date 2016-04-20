// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/scan_impl.h"
#include "sdk/tera.h"

namespace tera {

ScanDescriptor::ScanDescriptor(const std::string& rowkey) {
        _impl = new ScanDescImpl(rowkey);
}

ScanDescriptor::~ScanDescriptor() {
        delete _impl;
}

void ScanDescriptor::SetEnd(const std::string& rowkey) {
    _impl->SetEnd(rowkey);
}

void ScanDescriptor::AddColumnFamily(const std::string& cf) {
    _impl->AddColumnFamily(cf);
}

void ScanDescriptor::AddColumn(const std::string& cf, const std::string& qualifier) {
    _impl->AddColumn(cf, qualifier);
}

void ScanDescriptor::SetMaxVersions(int32_t versions) {
    _impl->SetMaxVersions(versions);
}

void ScanDescriptor::SetPackInterval(int64_t interval) {
    _impl->SetPackInterval(interval);
}

void ScanDescriptor::SetTimeRange(int64_t ts_end, int64_t ts_start) {
    _impl->SetTimeRange(ts_end, ts_start);
}

bool ScanDescriptor::SetFilter(const std::string& filter_string) {
    return _impl->SetFilter(filter_string);
}

void ScanDescriptor::SetValueConverter(ValueConverter converter) {
    _impl->SetValueConverter(converter);
}

void ScanDescriptor::SetSnapshot(uint64_t snapshot_id) {
    return _impl->SetSnapshot(snapshot_id);
}

void ScanDescriptor::SetBufferSize(int64_t buf_size) {
    _impl->SetBufferSize(buf_size);
}

void ScanDescriptor::SetNumberLimit(int64_t number_limit) {
    _impl->SetNumberLimit(number_limit);
}

int64_t ScanDescriptor::GetNumberLimit() {
    return _impl->GetNumberLimit();
}

void ScanDescriptor::SetAsync(bool async) {
    _impl->SetAsync(async);
}

bool ScanDescriptor::IsAsync() const {
    return _impl->IsAsync();
}

ScanDescImpl* ScanDescriptor::GetImpl() const {
    return _impl;
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
