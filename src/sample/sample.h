// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author likang01(com@baidu.com)

#ifndef  TERA_SAMPLE_SAMPLE_H
#define  TERA_SAMPLE_SAMPLE_H

#include <pthread.h>
#include <iostream>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "utils/counter.h"
#include "sdk/tera.h"

class IAdapter {
public:
    IAdapter(tera::Table* table) : m_table(table),
      m_max_outflow(-1), m_max_rate(-1),
      m_last_send_size(0), m_last_send_time(0) {}
    virtual ~IAdapter() {}
    virtual void CheckStatus(int64_t* total_count, int64_t* total_size,
                             int64_t* finish_count, int64_t* finish_size,
                             int64_t* success_count, int64_t* success_size) {
        *total_count = m_total_count.Get();
        *total_size = m_total_size.Get();
        *finish_count = m_finish_count.Get();
        *finish_size = m_finish_size.Get();
        *success_count = m_success_count.Get();
        *success_size = m_success_size.Get();
    }

    virtual void WaitComplete(int64_t* finish_count, int64_t* finish_size,
                              int64_t* success_count, int64_t* success_size) {
        *finish_count = m_finish_count.Get();
        *finish_size = m_finish_size.Get();
        *success_count = m_success_count.Get();
        *success_size = m_success_size.Get();
    }

    void SetMaxOutFlow(int64_t max_out_flow) {
        m_max_outflow = max_out_flow << 20;
    }

    void SetMaxRate(int64_t max_rate) {
        m_max_rate = max_rate;
    }

protected:
    void OnReceive(const std::string& row, const std::string& family,
                   const std::string& qualifier, uint64_t timestamp,
                   const std::string& value) {
        m_total_count.Inc();
        m_total_size.Add(Size(row, family, qualifier, timestamp, value));
    }
    void OnFinish(const std::string& row, const std::string& family,
                  const std::string& qualifier, uint64_t timestamp,
                  const std::string& value) {
        m_finish_count.Inc();
        m_finish_size.Add(Size(row, family, qualifier, timestamp, value));
    }
    void OnSuccess(const std::string& row, const std::string& family,
                   const std::string& qualifier, uint64_t timestamp,
                   const std::string& value) {
        m_success_count.Inc();
        m_success_size.Add(Size(row, family, qualifier, timestamp, value));
    }
    size_t Size(const std::string& row, const std::string& family,
                const std::string& qualifier, uint64_t timestamp,
                const std::string& value) {
        size_t size = row.size() + family.size() + qualifier.size() +
                      sizeof(timestamp) + value.size();
        return size;
    }
    int64_t Now() {
        struct timeval now;
        gettimeofday(&now, NULL);
        return now.tv_sec * 1000000 + now.tv_usec;
    }
    void CheckLimit() {
        if (m_max_outflow > 0) {
            int64_t sleep_micros =
                (int64_t)(m_last_send_time + (double)m_last_send_size * 1000000.0 / m_max_outflow - Now());
            if (sleep_micros > 0) {
                usleep(sleep_micros);
            }
        }
        if (m_max_rate > 0) {
            int64_t sleep_micros =
                (int64_t)(m_last_send_time + (double)1000000.0 / m_max_rate - Now());
            if (sleep_micros > 0) {
                usleep(sleep_micros);
            }
        }
    }

    void OnSent(const std::string& row, const std::string& family,
                const std::string& qualifier, uint64_t timestamp,
                const std::string& value) {
        m_last_send_time = Now();
        m_last_send_size = Size(row, family, qualifier, timestamp, value);
    }

    tera::Table* m_table;
    tera::Counter m_total_count;
    tera::Counter m_total_size;
    tera::Counter m_finish_count;
    tera::Counter m_finish_size;
    tera::Counter m_success_count;
    tera::Counter m_success_size;

private:
    int64_t m_max_outflow;
    int64_t m_max_rate;
    size_t m_last_send_size;
    int64_t m_last_send_time;
};

class IWriter : public IAdapter {
public:
    IWriter(tera::Table* table) : IAdapter(table) {}
    virtual ~IWriter() {}
    virtual void Write(const std::string& row, const std::string& family,
                       const std::string& qualifier, uint64_t timestamp,
                       const std::string& value) = 0;
};

class SyncWriter : public IWriter {
public:
    SyncWriter(tera::Table* table);
    virtual ~SyncWriter();
    virtual void Write(const std::string& row, const std::string& family,
                       const std::string& qualifier, uint64_t timestamp,
                       const std::string& value);
};

class AsyncWriter : public IWriter {
public:
    AsyncWriter(tera::Table* table);
    virtual ~AsyncWriter();
    void SetMaxPendingSize(int64_t max_pending_size);
    void SetMaxPendingCount(int64_t max_pending_count);
    virtual void Write(const std::string& row, const std::string& family,
                       const std::string& qualifier, uint64_t timestamp,
                       const std::string& value);
    virtual void WaitComplete(int64_t* finish_count, int64_t* finish_size,
                              int64_t* success_count, int64_t* success_size);
    void Callback(const std::string& row, const std::string& family,
                  const std::string& qualifier, uint64_t timestamp,
                  const std::string& value, const tera::ErrorCode& err);

private:
    int64_t m_max_pending_size;
    int64_t m_max_pending_count;
    tera::Counter m_pending_num;
    pthread_mutex_t m_mutex;
    pthread_cond_t m_cond;
};

class IReader : public IAdapter {
public:
    IReader(tera::Table* table) : IAdapter(table) {}
    virtual ~IReader() {}
    virtual void Read(const std::string& row, const std::string& family,
                      const std::string& qualifier, uint64_t timestamp) = 0;
};

class SyncReader : public IReader {
public:
    SyncReader(tera::Table* table);
    virtual ~SyncReader();
    virtual void Read(const std::string& row, const std::string& family,
                      const std::string& qualifier, uint64_t timestamp);
};

class AsyncReader : public IReader {
public:
    AsyncReader(tera::Table* table);
    ~AsyncReader();
    void SetMaxPendingSize(int64_t max_pending_size);
    void SetMaxPendingCount(int64_t max_pending_count);
    virtual void Read(const std::string& row, const std::string& family,
                      const std::string& qualifier, uint64_t timestamp);
    virtual void WaitComplete(int64_t* finish_count, int64_t* finish_size,
                              int64_t* success_count, int64_t* success_size);
    void Callback(const std::string& row, const std::string& family,
                  const std::string& qualifier, uint64_t timestamp,
                  const std::string& value, tera::ErrorCode err);

private:
    int64_t m_max_pending_size;
    int64_t m_max_pending_count;
    tera::Counter m_pending_num;
    pthread_mutex_t m_mutex;
    pthread_cond_t m_cond;
};

class Scanner : public IAdapter {
public:
    Scanner(tera::Table* table);
    virtual ~Scanner();
    virtual void Scan(const std::string& start_key,
                      const std::string& end_key,
                      const std::vector<std::string>& cf_list,
                      bool print = false, bool is_async =  false);
};

void add_md5sum(const std::string& rowkey, const std::string& family,
                const std::string& qualifier, std::string* value);
bool verify_md5sum(const std::string& rowkey, const std::string& family,
                   const std::string& qualifier, const std::string& value);

#endif  //TERA_SAMPLE_SAMPLE_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
