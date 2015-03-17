// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sample/sample.h"

DECLARE_bool(verify);

SyncReader::SyncReader(tera::Table* table)
    : IReader(table) {
}

SyncReader::~SyncReader() {
}

void SyncReader::Read(const std::string& row, const std::string& family,
                      const std::string& qualifier, uint64_t timestamp) {
    CheckLimit();
    OnSent(row, family, qualifier, timestamp, "");

    std::string value;
    tera::ErrorCode err;

    OnReceive(row, family, qualifier, timestamp, value);
    if (m_table->Get(row, family, qualifier, &value, &err)) {
        bool is_verified = (!FLAGS_verify) || verify_md5sum(row, family, qualifier, value);
        if (!is_verified) {
            std::cerr << "fail to pass md5 verifying: row=[" << row << "], column=["
                << family << ":" << qualifier << "], timestamp=["
                << timestamp << "], status="
                << tera::strerr(err) << std::endl;
        } else {
            OnFinish(row, family, qualifier, timestamp, value);
            OnSuccess(row, family, qualifier, timestamp, value);
        }
    } else {
        OnFinish(row, family, qualifier, timestamp, value);
        std::cerr << "fail to read: row=[" << row << "], column=["
            << family << ":" << qualifier << "], timestamp=["
            << timestamp << "], value=[" << value << "], status="
            << tera::strerr(err) << std::endl;
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
