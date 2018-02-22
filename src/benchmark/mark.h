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
#include "tera.h"
#include "common/counter.h"

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
    DEL = 4,
    PIF = 5
};

int64_t Now();

class Marker {
public:
    Marker(uint32_t max_latency)
        : latency_limit_(max_latency),
          operation_count_(0),
          total_latency_(0),
          min_latency_(0) {
        latency_vector_ = new uint64_t[max_latency + 1];
        memset(latency_vector_, 0, (max_latency + 1) * sizeof(uint64_t));
        for (int i = 0; i < 11; i++) {
            m_ten_percentile_latency[i] = 0;
            m_ten_percentile_latency_count_sum[i] = 0;
        }
    }

    ~Marker() {
        delete[] latency_vector_;
    }

    void AddLatency(uint32_t latency) {
        if (latency > latency_limit_) {
            latency = latency_limit_;
        }
        MutexLock lock(&mutex_);
        latency_vector_[latency]++;
        operation_count_++;
        total_latency_ += latency;
        if (operation_count_ == 1) {
            min_latency_ = latency;
        }
        if (min_latency_ > latency) {
            min_latency_ = latency;
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
        if (operation_count_ == 0) {
            return 0;
        }
        MutexLock lock(&mutex_);
        return (double)total_latency_ / operation_count_;
    }

    uint32_t PercentileLatency(uint32_t percentile) {
        MutexLock lock(&mutex_);
        if (percentile > 100) {
            percentile = 100;
        }
        if (percentile == 0) {
            return min_latency_;
        }
        if (percentile % 10 == 0) {
            return m_ten_percentile_latency[percentile / 10];
        }
        return NormalPercentileLatency(percentile);
    }

private:
    uint32_t NormalPercentileLatency(uint32_t percentile) {
        uint64_t percentile_operation_count = percentile * operation_count_ / 100;
        int ten_percentile = percentile / 10 + 1;
        uint32_t latency = m_ten_percentile_latency[ten_percentile];
        if (percentile_operation_count == 0) {
            return latency;
        }
        uint64_t count_sum = m_ten_percentile_latency_count_sum[ten_percentile];
        while (count_sum >= percentile_operation_count) {
            latency--;
            while (latency_vector_[latency] == 0) {
                latency--;
            }
            count_sum -= latency_vector_[latency];
        }
        return latency;
    }

    void MoveTenPercentileLatencyRight(int ten_percentile) {
        uint64_t percentile_operation_count = ten_percentile * operation_count_ / 10;
        uint32_t latency = m_ten_percentile_latency[ten_percentile];
        while (m_ten_percentile_latency_count_sum[ten_percentile]
               + latency_vector_[latency]
               < percentile_operation_count) {
            m_ten_percentile_latency_count_sum[ten_percentile] += latency_vector_[latency];
            latency++;
            while (latency_vector_[latency] == 0) {
                latency++;
            }
        }
        m_ten_percentile_latency[ten_percentile] = latency;
    }

    void MoveTenPercentileLatencyLeft(int ten_percentile) {
        uint64_t percentile_operation_count = ten_percentile * operation_count_ / 10;
        if (percentile_operation_count == 0) {
            return;
        }
        uint32_t latency = m_ten_percentile_latency[ten_percentile];
        while (m_ten_percentile_latency_count_sum[ten_percentile]
               >= percentile_operation_count) {
            latency--;
            while (latency_vector_[latency] == 0) {
                latency--;
            }
            m_ten_percentile_latency_count_sum[ten_percentile] -= latency_vector_[latency];
        }
        m_ten_percentile_latency[ten_percentile] = latency;
    }

private:
    const uint32_t latency_limit_;

    uint64_t operation_count_;
    uint64_t total_latency_;
    uint32_t min_latency_;
    uint64_t* latency_vector_;

    uint32_t m_ten_percentile_latency[11]; // 0, 10, 20, ..., 90, 100
    uint64_t m_ten_percentile_latency_count_sum[11];

    mutable Mutex mutex_;
};

class Statistic {
public:
    Statistic(int opt)
        : opt_(opt),
          last_send_size_(0),
          last_send_time_(0),
          last_total_count_(0),
          last_total_size_(0),
          last_finish_count_(0),
          last_finish_size_(0),
          last_success_count_(0),
          last_success_size_(0),
          last_conflict_count_(0),
          last_conflict_size_(0),
          finish_marker_(1000000),
          success_marker_(1000000),
          conflict_marker_(1000000) {}

    int GetOpt() {
        return opt_;
    }

    void GetStatistic(int64_t* total_count, int64_t* total_size,
                      int64_t* finish_count, int64_t* finish_size,
                      int64_t* success_count, int64_t* success_size,
                      int64_t* conflict_count, int64_t* conflict_size) {
        *total_count = last_total_count_ = total_count_.Get();
        *total_size = last_total_size_ = total_size_.Get();
        *finish_count = last_finish_count_ = finish_count_.Get();
        *finish_size = last_finish_size_ = finish_size_.Get();
        *success_count = last_success_count_ = success_count_.Get();
        *success_size = last_success_size_ = success_size_.Get();
        *conflict_count = last_conflict_count_ = conflict_count_.Get();
        *conflict_size = last_conflict_size_ = conflict_size_.Get();
    }

    void GetLastStatistic(int64_t* total_count, int64_t* total_size,
                          int64_t* finish_count, int64_t* finish_size,
                          int64_t* success_count, int64_t* success_size,
                          int64_t* conflict_count, int64_t* conflict_size) {
        *total_count = last_total_count_;
        *total_size = last_total_size_;
        *finish_count = last_finish_count_;
        *finish_size = last_finish_size_;
        *success_count = last_success_count_;
        *success_size = last_success_size_;
        *conflict_count = last_conflict_count_;
        *conflict_size = last_conflict_size_;
    }

    Marker* GetFinishMarker() {
        return &finish_marker_;
    }

    Marker* GetSuccessMarker() {
        return &success_marker_;
    }

    Marker* GetConflictMarker() {
        return &conflict_marker_;
    }

    void OnReceive(size_t size) {
        last_send_time_ = Now();
        last_send_size_ = size;
        total_count_.Inc();
        total_size_.Add(size);
    }

    void OnFinish(size_t size, uint32_t latency) {
        finish_count_.Inc();
        finish_size_.Add(size);
        finish_marker_.AddLatency(latency);
    }

    void OnSuccess(size_t size, uint32_t latency) {
        success_count_.Inc();
        success_size_.Add(size);
        success_marker_.AddLatency(latency);
    }

    void OnConflict(size_t size, uint32_t latency) {
        conflict_count_.Inc();
        conflict_size_.Add(size);
        conflict_marker_.AddLatency(latency);
    }

    void CheckPending() {
        int64_t max_pend_count = FLAGS_pend_count;
        int64_t max_pend_size = FLAGS_pend_size << 20;
        while (total_count_.Get() - finish_count_.Get() > max_pend_count) {
            usleep(1000);
        }
        while (total_size_.Get() - finish_size_.Get() > max_pend_size) {
            usleep(1000);
        }
    }

    void CheckLimit() {
        int64_t max_outflow = FLAGS_max_outflow << 20;
        int64_t max_rate = FLAGS_max_rate;
        if (max_outflow > 0) {
            int64_t sleep_micros =
                (int64_t)(last_send_time_ +
                        (double)last_send_size_ * 1000000.0 / max_outflow - Now());
            if (sleep_micros > 0) {
                usleep(sleep_micros);
            }
        }
        if (max_rate > 0) {
            int64_t sleep_micros =
                (int64_t)(last_send_time_ + (double)1000000.0 / max_rate - Now());
            if (sleep_micros > 0) {
                usleep(sleep_micros);
            }
        }
    }

private:
    int opt_;

    tera::Counter total_count_;
    tera::Counter total_size_;
    tera::Counter finish_count_;
    tera::Counter finish_size_;
    tera::Counter success_count_;
    tera::Counter success_size_;
    tera::Counter conflict_count_;
    tera::Counter conflict_size_;

    size_t last_send_size_;
    int64_t last_send_time_;

    int64_t last_total_count_;
    int64_t last_total_size_;
    int64_t last_finish_count_;
    int64_t last_finish_size_;
    int64_t last_success_count_;
    int64_t last_success_size_;
    int64_t last_conflict_count_;
    int64_t last_conflict_size_;

    Marker finish_marker_;
    Marker success_marker_;
    Marker conflict_marker_;
};

class Adapter {
public:
    Adapter(tera::Table* table);
    ~Adapter();

    void Write(int opt, const std::string& row,
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
               std::map<std::string, std::set<std::string> >& column,
               uint64_t ts);

    void Scan(const std::string& start_key,
              const std::string& end_key,
              const std::vector<std::string>& cf_list,
              bool print = false, bool is_async =  false);

    void WaitComplete();

    Statistic* GetWriteMarker() {
        return &write_marker_;
    }

    Statistic* GetReadMarker() {
        return &read_marker_;
    }

    Statistic* GetScanMarker() {
        return &scan_marker_;
    }

private:
    tera::Counter pending_num_;
    pthread_mutex_t mutex_;
    pthread_cond_t cond_;
    tera::Table* table_;

    Statistic write_marker_;
    Statistic read_marker_;
    Statistic scan_marker_;

    std::vector<tera::RowMutation*> sync_mutations_;
    std::vector<tera::RowReader*> sync_readers_;
    std::vector<size_t> sync_req_sizes_;
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
