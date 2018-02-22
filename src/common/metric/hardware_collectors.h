// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_METRIC_HARDWARE_METRICS_H_
#define TERA_COMMON_METRIC_HARDWARE_METRICS_H_
 
#include <string>

#include "common/metric/collector_report_publisher.h"
#include "common/metric/collector.h"
 
namespace tera { 

const char* const kInstCpuMetricName = "tera_instance_cpu_usage_percent";
const char* const kInstMemMetricName = "tera_instance_mem_usage_bytes";
const char* const kInstNetRXMetricName = "tera_instance_net_receive_bytes";
const char* const kInstNetTXMetricName = "tera_instance_net_transmit_bytes";

class CpuUsageCollector : public Collector {
public:
    CpuUsageCollector();
    virtual ~CpuUsageCollector();
    
    virtual int64_t Collect();
private:
    int64_t CheckCpuUsage(int64_t cur_ts, bool is_irix_on);

private:
    // proc info
    int    pid_;
    uint32_t cpu_core_num_;
    int64_t cpu_hertz_;
    std::string stat_path_;
    
    // last check info
    int64_t last_check_time_ms_;
    int64_t last_tick_total_; // cpu total ticks at last check
    int64_t cpu_usage_;       // (new_tick_total - last_tick_total_) / (total ticks in interval)
};

class MemUsageCollector : public Collector {
public:
    MemUsageCollector();
    virtual ~MemUsageCollector();
    
    virtual int64_t Collect();
private:
    int64_t CheckMemUsage(int64_t cur_ts);
    
private:
    // proc info
    int pid_;
    std::string stat_path_;
    
    // last check info
    int64_t last_check_time_ms_;
    int64_t mem_usage_;
};

enum NetUsageType {
    RECEIVE,    // net_rx
    TRANSMIT,   // net_tx
};

class NetUsageCollector : public Collector {
public:
    explicit NetUsageCollector(NetUsageType n_type);
    virtual ~NetUsageCollector();
    
    virtual int64_t Collect();
private:
    struct NetInfoChecker {
        // proc info
        int pid_;
        std::string stat_path_;
        
        // last check info
        int64_t last_check_time_ms_;
        int64_t last_rx_total_;  // total rx bytes at last check
        int64_t last_tx_total_;  // total tx bytes at last check
        
        // metric value cache
        int64_t net_rx_usage_;  // (new_rx_total - last_rx_total_) / check_interval
        int64_t net_tx_usage_;  // (new_tx_total - last_tx_total_) / check_interval
        
        NetInfoChecker();
        
        bool GetCurrentTotal(int64_t*, int64_t*);
        bool CheckNetUsage(int64_t cur_ts, int64_t* rx_usage, int64_t *tx_usage);
    };
    
    static NetInfoChecker net_info_checker_;
    
private:
    NetUsageType net_usage_type_;
};
 
} // end namespace tera 
 
#endif // TERA_COMMON_METRIC_HARDWARE_METRICS_H_
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

