// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/read_impl.h"
#include "tera.h"

namespace tera {

RowReader::RowReader() {}

RowReader::~RowReader() {}

#if 0
/// 读取操作
RowReader::RowReader(Table* table, const std::string& row_key) {
    _impl = new RowReaderImpl(this, table, row_key);
}

/// 设置读取特定版本
void RowReader::SetTimestamp(int64_t ts) {
    _impl->SetTimestamp(ts);
}

/// 设置超时时间(只影响当前操作,不影响Table::SetReadTimeout设置的默认读超时)
void RowReader::SetTimeOut(int64_t timeout_ms) {
    _impl->SetTimeOut(timeout_ms);
}

void RowReader::SetCallBack(Callback callback) {
    _impl->SetCallBack(callback);
}

/// 设置异步返回
void RowReader::SetAsync() {
    _impl->SetAsync();
}

/// 异步操作是否完成
bool RowReader::IsFinished() const {
    return _impl->IsFinished();
}

/// 获得结果错误码
ErrorCode RowReader::GetError() {
    return _impl->GetError();
}

/// 是否到达结束标记
bool RowReader::Done() {
    return _impl->Done();
}

/// 迭代下一个cell
void RowReader::Next() {
    _impl->Next();
}

/// 读取的结果
std::string RowReader::Value() {
    return _impl->Value();
}

/// Timestamp
int64_t RowReader::Timestamp() {
    return _impl->Timestamp();
}

/// Row
std::string RowReader::RowName() {
    return _impl->RowName();
}

/// Column cf:qualifier
std::string RowReader::ColumnName() {
    return _impl->ColumnName();
}

/// Column family
std::string RowReader::Family() {
    return _impl->Family();
}

/// Qualifier
std::string RowReader::Qualifier() {
    return _impl->Qualifier();
}

void RowReader::ToMap(Map* rowmap) {
    _impl->ToMap(rowmap);
}
#endif

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
