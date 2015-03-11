// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>

#include "tera/sample/sample.h"

void sdk_write_callback(tera::RowMutation* row_mu) {
    AsyncWriter* writer = (AsyncWriter*)row_mu->GetContext();
    tera::ErrorCode err = row_mu->GetError();
    uint32_t mu_num = row_mu->MutationNum();
    for (uint32_t i = 0; i < mu_num; i++) {
        const tera::RowMutation::Mutation& mu = row_mu->GetMutation(i);
        writer->Callback(row_mu->RowKey(), mu.family, mu.qualifier, mu.timestamp,
                         mu.value, err);
    }
    delete row_mu;
}

AsyncWriter::AsyncWriter(tera::Table* table)
    : IWriter(table) {
    pthread_mutex_init(&m_mutex, NULL);
    pthread_cond_init(&m_cond, NULL);
}

AsyncWriter::~AsyncWriter() {
    pthread_mutex_destroy(&m_mutex);
    pthread_cond_destroy(&m_cond);
}

void AsyncWriter::SetMaxPendingSize(int64_t max_pending_size) {
    m_max_pending_size = max_pending_size << 20;
}

void AsyncWriter::SetMaxPendingCount(int64_t max_pending_count) {
    m_max_pending_count = max_pending_count;
}

void AsyncWriter::Write(const std::string& row, const std::string& family,
                        const std::string& qualifier, uint64_t timestamp,
                        const std::string& value) {
    while (m_total_count.Get() - m_finish_count.Get() > m_max_pending_count) {
        usleep(1000);
    }
    while (m_total_size.Get() - m_finish_size.Get() > m_max_pending_size) {
        usleep(1000);
    }
    CheckLimit();

    VLOG(5) << "begin write: " << row << " " << family << " " << qualifier
            << " " << timestamp;
    OnReceive(row, family, qualifier, timestamp, value);
    tera::RowMutation* row_mu = m_table->NewRowMutation(row);
    row_mu->SetCallBack(sdk_write_callback);
    row_mu->SetContext(this);
    row_mu->Put(family, qualifier, timestamp, value);
    m_table->ApplyMutation(row_mu);
    m_pending_num.Inc();
    OnSent(row, family, qualifier, timestamp, value);
}

void AsyncWriter::WaitComplete(int64_t* finish_count, int64_t* finish_size,
                               int64_t* success_count, int64_t* success_size) {
    pthread_mutex_lock(&m_mutex);
    while (0 != m_pending_num.Get()) {
        pthread_cond_wait(&m_cond, &m_mutex);
    }
    pthread_mutex_unlock(&m_mutex);
    IAdapter::WaitComplete(finish_count, finish_size, success_count, success_size);
}

void AsyncWriter::Callback(const std::string& row, const std::string& family,
                           const std::string& qualifier, uint64_t timestamp,
                           const std::string& value, const tera::ErrorCode& err) {
    OnFinish(row, family, qualifier, timestamp, value);
    if (err.GetType() == tera::ErrorCode::kOK) {
        OnSuccess(row, family, qualifier, timestamp, value);
    } else {
        /*std::cerr << "fail to write: row=[" << row << "], column=["
            << family << ":" << qualifier << "], timestamp=["
            << timestamp << "], value=[" << value << "], status="
            << tera::strerr(err) << std::endl;*/
    }

    if (0 == m_pending_num.Dec()) {
        pthread_mutex_lock(&m_mutex);
        pthread_cond_signal(&m_cond);
        pthread_mutex_unlock(&m_mutex);
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
