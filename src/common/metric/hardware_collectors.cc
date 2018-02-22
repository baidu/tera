// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. 
#include <algorithm>
#include <iostream>
#include <sstream>
#include <stdio.h>  
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "gflags/gflags.h"
#include "glog/logging.h"
 
#include "common/timer.h"

#include "common/metric/hardware_collectors.h"

DECLARE_int64(tera_hardware_collect_period_second);
 
namespace tera { 

// return number of cpu(cores)
static uint32_t GetCpuCount() {
#if defined(_SC_NPROCESSORS_ONLN)
    return sysconf(_SC_NPROCESSORS_ONLN);
#else
    FILE *fp = fopen("/proc/stat", "r");
    if (fp == NULL) {
        LOG(ERROR) << "[HardWare Metric] open /proc/stat failed.";
        return 1;
    }
    static const size_t kLineMaxLen = 256; // enough in here
    std::unique_ptr<char[]> aline(new char[kLineMaxLen]);
    if (!aline) {
        LOG(ERROR) << "[HardWare Metric] malloc failed.";
        return 1;
    }
    static const size_t kHeaderMaxLen = 10;
    char header[kHeaderMaxLen];
    uint32_t i = 0;
    size_t len = 0;
    char* line_ptr = aline.get();
    getline(&line_ptr, &len, fp); // drop the first line
    while (getline(&line_ptr, &len, fp)) {
        i++;
        sscanf(line_ptr, "%s", header);
        if (!strncmp(header, "intr", kHeaderMaxLen)) {
            break;
        }
    }
    fclose(fp);
    return std::max(i - 1, 1);
#endif
}

// return the number of ticks(jiffies) that this process
// has been scheduled in user and kernel mode.
static bool ProcessCpuTick(const std::string& stat_path, int64_t* tick) {
    if (tick == NULL) {
        return false;
    }
    FILE *fp = fopen(stat_path.c_str(), "r");
    if (fp == NULL) {
        LOG(ERROR) << "[HardWare Metric] open " << stat_path << " failed.";
        return false;
    }
    long long utime = 0;
    long long stime = 0;
    if (fscanf(fp, "%*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %lld %lld",
               &utime, &stime) < 2) {
        fclose(fp);
        LOG(ERROR) << "[HardWare Metric] get cpu tick from " << stat_path << " failed.";
        return false;
    }
    fclose(fp);
    *tick = utime + stime;
    return true;
}

CpuUsageCollector::CpuUsageCollector():
    pid_(getpid()), 
    cpu_core_num_(GetCpuCount()), 
    cpu_hertz_(sysconf(_SC_CLK_TCK)), 
    stat_path_(std::string("/proc/") + std::to_string(pid_) + "/stat"), 
    last_check_time_ms_(get_millis()), 
    last_tick_total_(0), 
    cpu_usage_(0) {}

CpuUsageCollector::~CpuUsageCollector() {}

int64_t CpuUsageCollector::Collect() {
    int64_t cur_ts = get_millis();
    int64_t collect_period_ms = FLAGS_tera_hardware_collect_period_second * 1000;
    if (collect_period_ms > 0 && cur_ts < last_check_time_ms_ + collect_period_ms) {
        return cpu_usage_;
    } else {
        return CheckCpuUsage(cur_ts, false);
    }
}

int64_t CpuUsageCollector::CheckCpuUsage(int64_t cur_ts, bool is_irix_on) {
    int64_t new_tick_total = 0;
    if (!ProcessCpuTick(stat_path_, &new_tick_total)) {
        // read proc file failed. 
        return 0;
    }
    
    float interval_sec = static_cast<float>(cur_ts - last_check_time_ms_) / 1000.0f;
    // percentage per tick during time interval
    float interval_total_ticks = static_cast<float>(cpu_hertz_) * interval_sec;
    if (!is_irix_on) {
        interval_total_ticks *= cpu_core_num_;
    }

    float usage_percentage = static_cast<float>(new_tick_total - last_tick_total_) * 100.0f / interval_total_ticks;
    usage_percentage = std::min(usage_percentage, 99.9f);
    
    // update 
    last_tick_total_ = new_tick_total;
    cpu_usage_ = static_cast<int64_t>(usage_percentage);
    last_check_time_ms_ = cur_ts;
    VLOG(15) << "[Hardware Metric] %CPU: " << usage_percentage;
    return cpu_usage_;
}

MemUsageCollector::MemUsageCollector(): 
    pid_(getpid()),
    stat_path_(std::string("/proc/") + std::to_string(pid_) + "/statm"),
    last_check_time_ms_(get_millis()),
    mem_usage_(0) {}
    
      
MemUsageCollector::~MemUsageCollector() {}

int64_t MemUsageCollector::Collect() {
    int64_t cur_ts = get_millis();
    int64_t collect_period_ms = FLAGS_tera_hardware_collect_period_second * 1000;
    if (collect_period_ms > 0 && cur_ts < last_check_time_ms_ + collect_period_ms) {
        return mem_usage_;
    } else {
        return CheckMemUsage(cur_ts);
    }
}

int64_t MemUsageCollector::CheckMemUsage(int64_t cur_ts) {
    FILE* stat_file = fopen(stat_path_.c_str(), "r");
    if (stat_file == NULL) {
        LOG(ERROR) << "[Hardware Metric] open " << stat_path_ << " failed.";
        return false;
    }
    
    int64_t mem_pages = 0;
    fscanf(stat_file, "%*d %ld", &mem_pages);
    fclose(stat_file);
    
    mem_usage_ = mem_pages * 4 * 1024;
    last_check_time_ms_ = cur_ts;
    VLOG(15) << "[Hardware Metric] Memory: " << mem_usage_;
    return mem_usage_;
}

NetUsageCollector::NetInfoChecker NetUsageCollector::net_info_checker_;

NetUsageCollector::NetUsageCollector(NetUsageType n_type): 
    net_usage_type_(n_type) {}
    
NetUsageCollector::~NetUsageCollector() {}

int64_t NetUsageCollector::Collect() {
    int64_t cur_ts = get_millis();
    int64_t collect_period_ms = FLAGS_tera_hardware_collect_period_second * 1000;
    if (collect_period_ms > 0 && 
            cur_ts < net_info_checker_.last_check_time_ms_ + collect_period_ms) {
        return net_usage_type_ == RECEIVE ? net_info_checker_.net_rx_usage_ : net_info_checker_.net_tx_usage_;
    } else {
        int64_t value = 0;
        if (net_usage_type_ == RECEIVE) {
            // check net info and get receive usage
            net_info_checker_.CheckNetUsage(cur_ts, &value, NULL);
        } else {
            // check net info and get transmit usage
            net_info_checker_.CheckNetUsage(cur_ts, NULL, &value);
        }
        return value;
    }
}

NetUsageCollector::NetInfoChecker::NetInfoChecker()
    : pid_(getpid()), 
      stat_path_(std::string("/proc/") + std::to_string(pid_) + "/net/dev"),
      last_check_time_ms_(get_millis()),
      last_rx_total_(0),
      last_tx_total_(0),
      net_rx_usage_(0),
      net_tx_usage_(0) {
    GetCurrentTotal(&last_rx_total_, &last_tx_total_);
}

bool NetUsageCollector::NetInfoChecker::GetCurrentTotal(int64_t *rx_total, int64_t *tx_total) {
    FILE* stat_file = fopen(stat_path_.c_str(), "r");
    if (stat_file == NULL) {
        LOG(ERROR) << "[Hardware Metric] open " << stat_path_ << "failed.";
        return false;
    }
    int ret = fseek(stat_file, 327, SEEK_SET);
    CHECK_EQ(ret, 0);
    for (int i = 0; i < 10; i++) {
        while (':' != fgetc(stat_file));
        ret = fscanf(stat_file, "%ld%*d%*d%*d%*d%*d%*d%*d%ld", rx_total, tx_total);
        if (ret >= 2 && rx_total > 0 && tx_total > 0) {
            break;
        }
    }
    fclose(stat_file);

    return true;
}

bool NetUsageCollector::NetInfoChecker::CheckNetUsage(int64_t cur_ts, int64_t* rx_usage, int64_t *tx_usage) {
    int64_t new_rx_total = 0;
    int64_t new_tx_total = 0;

    if (!GetCurrentTotal(&new_rx_total, &new_tx_total)) {
        return false;
    }
    int64_t interval_ms = cur_ts - last_check_time_ms_;
    // update
    net_rx_usage_ = (new_rx_total - last_rx_total_) * 1000 / interval_ms;
    net_tx_usage_ = (new_tx_total - last_tx_total_) * 1000 / interval_ms;
    last_rx_total_ = new_rx_total;
    last_tx_total_ = new_tx_total;
    last_check_time_ms_ = cur_ts;
    
    if (rx_usage) {
        *rx_usage = net_rx_usage_;
    }
    
    if (tx_usage) {
        *tx_usage = net_tx_usage_;
    }

    VLOG(15) << "[Hardware Metric] Network RX/TX: " << last_rx_total_ << " / " << last_tx_total_;
    return true;
}
 
} // end namespace tera 
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

