// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <assert.h>

#include <iostream>

#include "tera/sample/sample.h"

DECLARE_bool(verify);

void sdk_read_callback(tera::RowReader* row_rd) {
    AsyncReader* reader = (AsyncReader*)row_rd->GetContext();
    const tera::RowReader::ReadColumnList& read_list = row_rd->GetReadColumnList();
    assert(read_list.size() == 1);
    const std::string& family = read_list.begin()->first;
    assert(read_list.begin()->second.size() == 1);
    const std::string& qualifier = *read_list.begin()->second.begin();
    int64_t timestamp = row_rd->GetTimestamp();
    if (row_rd->GetError().GetType() == tera::ErrorCode::kOK) {
        reader->Callback(row_rd->RowName(), family, qualifier,
                         timestamp, row_rd->Value(), row_rd->GetError());
    } else {
        reader->Callback(row_rd->RowName(), family, qualifier,
                         timestamp, "", row_rd->GetError());
    }
    delete row_rd;
}

AsyncReader::AsyncReader(tera::Table* table)
    : IReader(table) {
    pthread_mutex_init(&m_mutex, NULL);
    pthread_cond_init(&m_cond, NULL);
}

AsyncReader::~AsyncReader() {
    pthread_mutex_destroy(&m_mutex);
    pthread_cond_destroy(&m_cond);
}

void AsyncReader::SetMaxPendingSize(int64_t max_pending_size) {
    m_max_pending_size = max_pending_size << 20;
}

void AsyncReader::SetMaxPendingCount(int64_t max_pending_count) {
    m_max_pending_count = max_pending_count;
}

void AsyncReader::Read(const std::string& row, const std::string& family,
                       const std::string& qualifier, uint64_t timestamp) {
    while (m_total_count.Get() - m_finish_count.Get() > m_max_pending_count) {
        usleep(1000);
    }
    while (m_total_size.Get() - m_finish_size.Get() > m_max_pending_size) {
        usleep(1000);
    }
    CheckLimit();

    VLOG(5) << "begin read: " << row << " " << family << " " << qualifier;
    OnReceive(row, family, qualifier, timestamp, "");
    tera::RowReader* reader = m_table->NewRowReader(row);
    reader->SetCallBack(sdk_read_callback);
    reader->SetContext(this);
    reader->AddColumn(family, qualifier);
    reader->SetTimeRange(0, timestamp);
    m_table->Get(reader);
    m_pending_num.Inc();
    OnSent(row, family, qualifier, timestamp, "");
}

void AsyncReader::WaitComplete(int64_t* finish_count, int64_t* finish_size,
                               int64_t* success_count, int64_t* success_size) {
    pthread_mutex_lock(&m_mutex);
    while (0 != m_pending_num.Get()) {
        pthread_cond_wait(&m_cond, &m_mutex);
    }
    pthread_mutex_unlock(&m_mutex);
    IAdapter::WaitComplete(finish_count, finish_size, success_count, success_size);
}

void AsyncReader::Callback(const std::string& row, const std::string& family,
                           const std::string& qualifier, uint64_t timestamp,
                           const std::string& value, tera::ErrorCode err) {
    OnFinish(row, family, qualifier, timestamp, value);
    bool is_verified = (!FLAGS_verify) || verify_md5sum(row, family, qualifier, value);
    if (err.GetType() == tera::ErrorCode::kOK && is_verified) {
        OnSuccess(row, family, qualifier, timestamp, value);
    } else if (!is_verified) {
        std::cerr << "fail to pass md5 verifying: row=[" << row << "], column=["
            << family << ":" << qualifier << "], timestamp=["
            << timestamp << "], status="
            << tera::strerr(err) << std::endl;
    } else {
        std::cerr << "fail to read: row=[" << row << "], column=["
            << family << ":" << qualifier << "], timestamp=["
            << timestamp << "], status="
            << tera::strerr(err) << std::endl;
    }

    if (0 == m_pending_num.Dec()) {
        pthread_mutex_lock(&m_mutex);
        pthread_cond_signal(&m_cond);
        pthread_mutex_unlock(&m_mutex);
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
