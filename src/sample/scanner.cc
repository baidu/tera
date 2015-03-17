// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>

#include "sample/sample.h"

DECLARE_int32(buf_size);

Scanner::Scanner(tera::Table* table)
    : IAdapter(table) {
}

Scanner::~Scanner() {
}

void Scanner::Scan(const std::string& start_key, const std::string& end_key,
                   const std::vector<std::string>& cf_list,
                   bool print, bool is_async) {
    tera::ScanDescriptor scan_desp(start_key);
    scan_desp.SetEnd(end_key);
    scan_desp.SetBufferSize(FLAGS_buf_size);
    scan_desp.SetAsync(is_async);
    for (size_t i = 0; i < cf_list.size(); i++) {
        scan_desp.AddColumnFamily(cf_list[i]);
    }
    tera::ErrorCode err;
    tera::ResultStream* result = m_table->Scan(scan_desp, &err);
    if (result == NULL) {
        std::cerr << "fail to scan: " << tera::strerr(err);
        return;
    }

    uint64_t count = 0;
    while (!result->Done()) {
        if (print) {
            std::cerr << count++ << "\t" << result->RowName() << "\t" <<
                result->Family() << "\t" << result->Qualifier() << "\t" <<
                result->Timestamp() << "\t" << result->Value() << std::endl;
        }
        OnFinish(result->RowName(), result->Family(), result->Qualifier(),
                 result->Timestamp(), result->Value());
        OnSuccess(result->RowName(), result->Family(), result->Qualifier(),
                  result->Timestamp(), result->Value());
        result->Next();
    }
    delete result;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
