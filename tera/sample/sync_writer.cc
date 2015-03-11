// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/sample/sample.h"

SyncWriter::SyncWriter(tera::Table* table)
    : IWriter(table) {
}

SyncWriter::~SyncWriter() {
}

void SyncWriter::Write(const std::string& row, const std::string& family,
                       const std::string& qualifier, uint64_t timestamp,
                       const std::string& value) {
    CheckLimit();
    OnSent(row, family, qualifier, timestamp, value);

    OnReceive(row, family, qualifier, timestamp, value);
    tera::RowMutation* row_mu = m_table->NewRowMutation(row);
    row_mu->Put(family, qualifier, timestamp, value);
    m_table->ApplyMutation(row_mu);
    OnFinish(row, family, qualifier, timestamp, value);

    tera::ErrorCode err = row_mu->GetError();
    if (err.GetType() == tera::ErrorCode::kOK) {
        OnSuccess(row, family, qualifier, timestamp, value);
    } else {
        std::cerr << "fail to write: row=[" << row << "], column=["
            << family << ":" << qualifier << "], timestamp=["
            << timestamp << "], value=[" << value << "], status="
            << tera::strerr(err) << std::endl;
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
