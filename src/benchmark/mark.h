// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author likang01(com@baidu.com)

#ifndef  TERA_BENCHMARK_MARK_H_
#define  TERA_BENCHMARK_MARK_H_

#include <pthread.h>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/mutex.h"
#include "sdk/tera.h"
#include "utils/counter.h"

DECLARE_int64(pend_size);
DECLARE_int64(pend_count);
DECLARE_int64(max_outflow);
DECLARE_int64(max_rate);
DECLARE_int64(batch_count);

enum MODE {
    WRITE = 1,
    READ = 2,
    SCAN = 3,
    MIX = 4,
    DELETE = 5
};
extern int mode;

enum TYPE {
    SYNC = 1,
    ASYNC = 2
};
extern int type;

enum OP {
    NONE= 0,
    PUT = 1,
    GET = 2,
    SCN = 3,
    DEL = 4
};

int64_t Now();

class Marker {
public:
    Marker(uint32_t max_latency)
        : m_latency_limit(max_latency),
          m_operation_count(0),
          m_total_latency(0),
          m_min_latency(0) {
        m_latency_vector = new uint64_t[max_latency + 1];
        memset(m_latency_vector, 0, (max_latency + 1) * sizeof(uint64_t));
        for (int i = 0; i < 11; i++) {
            m_ten_percentile_latency[i] = 0;
            m_ten_percentile_latency_count_sum[i] = 0;
        }
    }

    ~Marker() {
        delete[] m_latency_vector;
    }

    void AddLatency(uint32_t latency) {
        if (latency > m_latency_limit) {
            latency = m_latency_limit;
        }
        MutexLock lock(&m_mutex);
        m_latency_vector[latency]++;
        m_operation_count++;
        m_total_latency += latency;
        if (m_operation_count == 1) {
            m_min_latency = latency;
        }
        if (m_min_latency > latency) {
            m_min_latency = latency;
        }
        for (int i = 1; i < 11; i++) {
            if (m_ten_percentile_latency[i] < latency) {
                MoveTenPercentileLatencyRight(i);
            } else if (m_ten_percentile_latency[i] > latency) {
                m_ten_percentile_latency_count_sum[i]++;
                MoveTenPercentileLatencyLeft(i);
            }
        }
    }

    uint32_t MinLatency() {
        return PercentileLatency(0);
    }

    uint32_t MaxLatency() {
        return PercentileLatency(100);
    }

    double AverageLatency() {
        if (m_operation_count == 0) {
            return 0;
        }
        MutexLock lock(&m_mutex);
        return (double)m_total_latency / m_operation_count;
    }

    uint32_t PercentileLatency(uint32_t percentile) {
        MutexLock lock(&m_mutex);
        if (percentile > 100) {
            percentile = 100;
        }
        if (percentile == 0) {
            return m_min_latency;
        }
        if (percentile % 10 == 0) {
            return m_ten_percentile_latency[percentile / 10];
        }
        return NormalPercentileLatency(percentile);
    }

private:
    uint32_t NormalPercentileLatency(uint32_t percentile) {
        uint64_t percentile_operation_count = percentile * m_operation_count / 100;
        int ten_percentile = percentile / 10 + 1;
        uint32_t latency = m_ten_percentile_latency[ten_percentile];
        if (percentile_operation_count == 0) {
            return latency;
        }
        uint64_t count_sum = m_ten_percentile_latency_count_sum[ten_percentile];
        while (count_sum >= percentile_operation_count) {
            latency--;
            while (m_latency_vector[latency] == 0) {
                latency--;
            }
            count_sum -= m_latency_vector[latency];
        }
        return latency;
    }

    void MoveTenPercentileLatencyRight(int ten_percentile) {
        uint64_t percentile_operation_count = ten_percentile * m_operation_count / 10;
        uint32_t latency = m_ten_percentile_latency[ten_percentile];
        while (m_ten_percentile_latency_count_sum[ten_percentile]
               + m_latency_vector[latency]
               < percentile_operation_count) {
            m_ten_percentile_latency_count_sum[ten_percentile] += m_latency_vector[latency];
            latency++;
            while (m_latency_vector[latency] == 0) {
                latency++;
            }
        }
        m_ten_percentile_latency[ten_percentile] = latency;
    }

    void MoveTenPercentileLatencyLeft(int ten_percentile) {
        uint64_t percentile_operation_count = ten_percentile * m_operation_count / 10;
        if (percentile_operation_count == 0) {
            return;
        }
        uint32_t latency = m_ten_percentile_latency[ten_percentile];
        while (m_ten_percentile_latency_count_sum[ten_percentile]
               >= percentile_operation_count) {
            latency--;
            while (m_latency_vector[latency] == 0) {
                latency--;
            }
            m_ten_percentile_latency_count_sum[ten_percentile] -= m_latency_vector[latency];
        }
        m_ten_percentile_latency[ten_percentile] = latency;
    }

private:
    const uint32_t m_latency_limit;

    uint64_t m_operation_count;
    uint64_t m_total_latency;
    uint32_t m_min_latency;
    uint64_t* m_latency_vector;

    uint32_t m_ten_percentile_latency[11]; // 0, 10, 20, ..., 90, 100
    uint64_t m_ten_percentile_latency_count_sum[11];

    mutable Mutex m_mutex;
};

class Statistic {
public:
    Statistic(int opt)
        : m_opt(opt),
          m_last_send_size(0),
          m_last_send_time(0),
          m_last_total_count(0),
          m_last_total_size(0),
          m_last_finish_count(0),
          m_last_finish_size(0),
          m_last_success_count(0),
          m_last_success_size(0),
          m_finish_marker(1000000),
          m_success_marker(1000000) {}

    int GetOpt() {
        return m_opt;
    }

    void GetStatistic(int64_t* total_count, int64_t* total_size,
                      int64_t* finish_count, int64_t* finish_size,
                      int64_t* success_count, int64_t* success_size) {
        *total_count = m_last_total_count = m_total_count.Get();
        *total_size = m_last_total_size = m_total_size.Get();
        *finish_count = m_last_finish_count = m_finish_count.Get();
        *finish_size = m_last_finish_size = m_finish_size.Get();
        *success_count = m_last_success_count = m_success_count.Get();
        *success_size = m_last_success_size = m_success_size.Get();
    }

    void GetLastStatistic(int64_t* total_count, int64_t* total_size,
                          int64_t* finish_count, int64_t* finish_size,
                          int64_t* success_count, int64_t* success_size) {
        *total_count = m_last_total_count;
        *total_size = m_last_total_size;
        *finish_count = m_last_finish_count;
        *finish_size = m_last_finish_size;
        *success_count = m_last_success_count;
        *success_size = m_last_success_size;
    }

    Marker* GetFinishMarker() {
        return &m_finish_marker;
    }

    Marker* GetSuccessMarker() {
        return &m_success_marker;
    }

    void OnReceive(size_t size) {
        m_last_send_time = Now();
        m_last_send_size = size;
        m_total_count.Inc();
        m_total_size.Add(size);
    }

    void OnFinish(size_t size, uint32_t latency) {
        m_finish_count.Inc();
        m_finish_size.Add(size);
        m_finish_marker.AddLatency(latency);
    }

    void OnSuccess(size_t size, uint32_t latency) {
        m_success_count.Inc();
        m_success_size.Add(size);
        m_success_marker.AddLatency(latency);
    }

    void CheckPending() {
        int64_t max_pend_count = FLAGS_pend_count;
        int64_t max_pend_size = FLAGS_pend_size << 20;
        while (m_total_count.Get() - m_finish_count.Get() > max_pend_count) {
            usleep(1000);
        }
        while (m_total_size.Get() - m_finish_size.Get() > max_pend_size) {
            usleep(1000);
        }
    }

    void CheckLimit() {
        int64_t max_outflow = FLAGS_max_outflow << 20;
        int64_t max_rate = FLAGS_max_rate;
        if (max_outflow > 0) {
            int64_t sleep_micros =
                (int64_t)(m_last_send_time +
                        (double)m_last_send_size * 1000000.0 / max_outflow - Now());
            if (sleep_micros > 0) {
                usleep(sleep_micros);
            }
        }
        if (max_rate > 0) {
            int64_t sleep_micros =
                (int64_t)(m_last_send_time + (double)1000000.0 / max_rate - Now());
            if (sleep_micros > 0) {
                usleep(sleep_micros);
            }
        }
    }

private:
    int m_opt;

    tera::Counter m_total_count;
    tera::Counter m_total_size;
    tera::Counter m_finish_count;
    tera::Counter m_finish_size;
    tera::Counter m_success_count;
    tera::Counter m_success_size;

    size_t m_last_send_size;
    int64_t m_last_send_time;

    int64_t m_last_total_count;
    int64_t m_last_total_size;
    int64_t m_last_finish_count;
    int64_t m_last_finish_size;
    int64_t m_last_success_count;
    int64_t m_last_success_size;

    Marker m_finish_marker;
    Marker m_success_marker;
};

class Adapter {
public:
    Adapter(tera::Table* table);
    ~Adapter();

    void Write(const std::string& row,
               std::map<std::string, std::set<std::string> >& column,
               uint64_t timestamp,
               std::string& value);
    void CommitSyncWrite();
    void WriteCallback(tera::RowMutation* row_mu,
                       size_t req_size,
                       int64_t req_time);

    void Read(const std::string& row,
              const std::map<std::string, std::set<std::string> >& column,
              uint64_t largest_ts, uint64_t smallest_ts);
    void CommitSyncRead();
    void ReadCallback(tera::RowReader* reader,
                      size_t req_size,
                      int64_t req_time);

    void Delete(const std::string& row,
               std::map<std::string, std::set<std::string> >& column);

    void Scan(const std::string& start_key,
              const std::string& end_key,
              const std::vector<std::string>& cf_list,
              bool print = false, bool is_async =  false);

    void WaitComplete();

    Statistic* GetWriteMarker() {
        return &m_write_marker;
    }

    Statistic* GetReadMarker() {
        return &m_read_marker;
    }

    Statistic* GetScanMarker() {
        return &m_scan_marker;
    }

private:
    tera::Counter m_pending_num;
    pthread_mutex_t m_mutex;
    pthread_cond_t m_cond;
    tera::Table* m_table;

    Statistic m_write_marker;
    Statistic m_read_marker;
    Statistic m_scan_marker;

    std::vector<tera::RowMutation*> m_sync_mutations;
    std::vector<tera::RowReader*> m_sync_readers;
    std::vector<size_t> m_sync_req_sizes;
};

void add_checksum(const std::string& rowkey, const std::string& family,
                  const std::string& qualifier, std::string* value);
void remove_checksum(std::string* value);
bool verify_checksum(const std::string& rowkey, const std::string& family,
                     const std::string& qualifier, const std::string& value);

/*
void add_md5sum(const std::string& rowkey, const std::string& family,
                const std::string& qualifier, std::string* value);
bool verify_md5sum(const std::string& rowkey, const std::string& family,
                   const std::string& qualifier, const std::string& value);
void remove_md5sum(std::string* value);
*/

#endif  // TERA_BENCHMARK_MARK_H_
