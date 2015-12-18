// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#include "tabletnode_sysinfo.h"

#include <sys/time.h>
#include <unistd.h>

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "common/base/string_number.h"
#include "proto/proto_helper.h"
#include "utils/timer.h"
#include "utils/tprinter.h"
#include "utils/utils_cmd.h"

DEFINE_int32(tera_tabletnode_sysinfo_mem_collect_interval, 10, "interval of mem checking(s)");
DEFINE_int32(tera_tabletnode_sysinfo_net_collect_interval, 5, "interval of net checking(s)");
DEFINE_int32(tera_tabletnode_sysinfo_cpu_collect_interval, 5, "interval of cpu checking(s)");

namespace leveldb {
extern tera::Counter rawkey_compare_counter;

extern tera::Counter dfs_read_size_counter;
extern tera::Counter dfs_write_size_counter;
extern tera::Counter posix_read_size_counter;
extern tera::Counter posix_write_size_counter;

extern tera::Counter posix_read_counter;
extern tera::Counter posix_write_counter;
extern tera::Counter posix_sync_counter;
extern tera::Counter posix_list_counter;
extern tera::Counter posix_exists_counter;
extern tera::Counter posix_open_counter;
extern tera::Counter posix_close_counter;
extern tera::Counter posix_delete_counter;
extern tera::Counter posix_tell_counter;
extern tera::Counter posix_seek_counter;
extern tera::Counter posix_info_counter;
extern tera::Counter posix_other_counter;

extern tera::Counter snappy_before_size_counter;
extern tera::Counter snappy_after_size_counter;

extern tera::Counter dfs_read_counter;
extern tera::Counter dfs_write_counter;
extern tera::Counter dfs_read_delay_counter;
extern tera::Counter dfs_write_delay_counter;
extern tera::Counter dfs_sync_delay_counter;
extern tera::Counter dfs_sync_counter;
extern tera::Counter dfs_flush_counter;
extern tera::Counter dfs_list_counter;
extern tera::Counter dfs_exists_counter;
extern tera::Counter dfs_open_counter;
extern tera::Counter dfs_close_counter;
extern tera::Counter dfs_delete_counter;
extern tera::Counter dfs_tell_counter;
extern tera::Counter dfs_info_counter;
extern tera::Counter dfs_other_counter;

extern tera::Counter dfs_read_hang_counter;
extern tera::Counter dfs_write_hang_counter;
extern tera::Counter dfs_sync_hang_counter;
extern tera::Counter dfs_flush_hang_counter;
extern tera::Counter dfs_list_hang_counter;
extern tera::Counter dfs_exists_hang_counter;
extern tera::Counter dfs_open_hang_counter;
extern tera::Counter dfs_close_hang_counter;
extern tera::Counter dfs_delete_hang_counter;
extern tera::Counter dfs_tell_hang_counter;
extern tera::Counter dfs_info_hang_counter;
extern tera::Counter dfs_other_hang_counter;

extern tera::Counter ssd_read_counter;
extern tera::Counter ssd_read_size_counter;
extern tera::Counter ssd_write_counter;
extern tera::Counter ssd_write_size_counter;
}

tera::Counter rand_read_delay;
tera::Counter row_read_delay;
tera::Counter range_error_counter;
tera::Counter read_pending_counter;
tera::Counter write_pending_counter;
tera::Counter scan_pending_counter;
tera::Counter compact_pending_counter;

namespace tera {
namespace tabletnode {

TabletNodeSysInfo::TabletNodeSysInfo()
    : m_mem_check_ts(0),
      m_net_check_ts(0),
      m_io_check_ts(0),
      m_net_tx_total(0),
      m_net_rx_total(0),
      m_cpu_check_ts(0),
      m_tablet_check_ts(0) {
}

TabletNodeSysInfo::TabletNodeSysInfo(const TabletNodeInfo& info)
    : m_info(info),
      m_mem_check_ts(0),
      m_net_check_ts(0),
      m_net_tx_total(0),
      m_net_rx_total(0),
      m_cpu_check_ts(0),
      m_tablet_check_ts(0) {
}

TabletNodeSysInfo::~TabletNodeSysInfo() {
}

void TabletNodeSysInfo::AddExtraInfo(const std::string& name, int64_t value) {
    MutexLock lock(&m_mutex);
    ExtraTsInfo* e_info = m_info.add_extra_info();
    e_info->set_name(name);
    e_info->set_value(value);
}

void TabletNodeSysInfo::SetCurrentTime() {
    MutexLock lock(&m_mutex);
    m_info.set_timestamp(get_micros());
}

int64_t TabletNodeSysInfo::GetTimeStamp() {
    return m_info.timestamp();
}

void TabletNodeSysInfo::SetTimeStamp(int64_t ts) {
    MutexLock lock(&m_mutex);
    m_info.set_timestamp(ts);
}

void TabletNodeSysInfo::CollectTabletNodeInfo(TabletManager* tablet_manager,
                                              const string& server_addr) {
    MutexLock lock(&m_mutex);
    int64_t cur_ts = get_micros();
    int64_t interval = cur_ts - m_tablet_check_ts;
    m_tablet_check_ts = cur_ts;

    m_tablet_list.Clear();
    int64_t total_size = 0;
    int64_t low_read_cell = 0;
    int64_t scan_rows = 0;
    int64_t scan_kvs = 0;
    int64_t scan_size = 0;
    int64_t read_rows = 0;
    int64_t read_kvs = 0;
    int64_t read_size = 0;
    int64_t write_rows = 0;
    int64_t write_kvs = 0;
    int64_t write_size = 0;
    int64_t busy_cnt = 0;

    std::vector<io::TabletIO*> tablet_ios;
    tablet_manager->GetAllTablets(&tablet_ios);
    std::vector<io::TabletIO*>::iterator it = tablet_ios.begin();
    for (; it != tablet_ios.end(); ++it) {
        io::TabletIO* tablet_io = *it;
        TabletMeta* tablet_meta = m_tablet_list.add_meta();
        tablet_meta->set_status(TabletStatus(tablet_io->GetStatus()));
        tablet_meta->set_server_addr(server_addr);
        tablet_meta->set_table_name(tablet_io->GetTableName());
        tablet_meta->set_path(tablet_io->GetTablePath());
        tablet_meta->mutable_key_range()->set_key_start(tablet_io->GetStartKey());
        tablet_meta->mutable_key_range()->set_key_end(tablet_io->GetEndKey());

        std::vector<uint64_t> lgsize;
        uint64_t size;
        tablet_io->GetDataSize(&size, &lgsize);
        tablet_meta->set_size(size);
        for (size_t i = 0; i < lgsize.size(); ++i) {
            tablet_meta->add_lg_size(lgsize[i]);
        }
        tablet_meta->set_compact_status(tablet_io->GetCompactStatus());
        total_size += tablet_meta->size();

        TabletCounter* counter = m_tablet_list.add_counter();
        tablet_io->GetAndClearCounter(counter);
        low_read_cell += counter->low_read_cell();
        scan_rows += counter->scan_rows();
        scan_kvs += counter->scan_kvs();
        scan_size += counter->scan_size();
        read_rows += counter->read_rows();
        read_kvs += counter->read_kvs();
        read_size += counter->read_size();
        write_rows += counter->write_rows();
        write_kvs += counter->write_kvs();
        write_size += counter->write_size();

        if (counter->is_on_busy()) {
            busy_cnt++;
        }
        tablet_io->DecRef();
    }
    m_info.set_low_read_cell(low_read_cell * 1000000 / interval);
    m_info.set_scan_rows(scan_rows * 1000000 / interval);
    m_info.set_scan_kvs(scan_kvs * 1000000 / interval);
    m_info.set_scan_size(scan_size * 1000000 / interval);
    m_info.set_read_rows(read_rows * 1000000 / interval);
    m_info.set_read_kvs(read_kvs * 1000000 / interval);
    m_info.set_read_size(read_size * 1000000 / interval);
    m_info.set_write_rows(write_rows * 1000000 / interval);
    m_info.set_write_kvs(write_kvs * 1000000 / interval);
    m_info.set_write_size(write_size * 1000000 / interval);
    m_info.set_tablet_onbusy(busy_cnt);

    // refresh tabletnodeinfo
    m_info.set_load(total_size);
    m_info.set_tablet_total(tablet_ios.size());

    int64_t tmp;
    tmp = leveldb::dfs_read_size_counter.Clear() * 1000000 / interval;
    m_info.set_dfs_io_r(tmp);
    tmp = leveldb::dfs_write_size_counter.Clear() * 1000000 / interval;
    m_info.set_dfs_io_w(tmp);
    tmp = leveldb::posix_read_size_counter.Clear() * 1000000 / interval;
    m_info.set_local_io_r(tmp);
    tmp = leveldb::posix_write_size_counter.Clear() * 1000000 / interval;
    m_info.set_local_io_w(tmp);

    m_info.set_read_pending(read_pending_counter.Get());
    m_info.set_write_pending(write_pending_counter.Get());
    m_info.set_scan_pending(scan_pending_counter.Get());

    // collect extra infos
    m_info.clear_extra_info();
    ExtraTsInfo* einfo = m_info.add_extra_info();
    if (read_rows == 0) {
        tmp = 0;
    } else {
        tmp = rand_read_delay.Clear() / read_rows;
    }
    einfo->set_name("rand_read_delay");
    einfo->set_value(tmp / 1000);

    einfo = m_info.add_extra_info();
    if (read_rows == 0) {
        tmp = 0;
    } else {
        tmp = row_read_delay.Clear() / read_rows;
    }
    einfo->set_name("row_read_delay");
    einfo->set_value(tmp / 1000);

    einfo = m_info.add_extra_info();
    tmp = range_error_counter.Clear() * 1000000 / interval;
    einfo->set_name("range_error");
    einfo->set_value(tmp);

    einfo = m_info.add_extra_info();
    tmp = read_pending_counter.Get();
    einfo->set_name("read_pending");
    einfo->set_value(tmp);

    einfo = m_info.add_extra_info();
    tmp = write_pending_counter.Get();
    einfo->set_name("write_pending");
    einfo->set_value(tmp);

    einfo = m_info.add_extra_info();
    tmp = scan_pending_counter.Get();
    einfo->set_name("scan_pending");
    einfo->set_value(tmp);

    einfo = m_info.add_extra_info();
    tmp = compact_pending_counter.Get();
    einfo->set_name("compact_pending");
    einfo->set_value(tmp);
}

// return the number of ticks(jiffies) that this process
// has been scheduled in user and kernel mode.
static long long ProcessCpuTick() {
    const int PATH_MAX_LEN = 64;
    char path[PATH_MAX_LEN];
    sprintf(path, "/proc/%d/stat", getpid());
    FILE *fp = fopen(path, "r");
    if (fp == NULL) {
        return 0;
    }
    long long utime, stime;
    fscanf(fp, "%*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %lld %lld",
        &utime, &stime);
    fclose(fp);
    return utime + stime;
}

// return number of cpu(cores)
static int GetCpuCount() {
#ifdef _SC_NPROCESSORS_ONLN
    return sysconf(_SC_NPROCESSORS_ONLN);
#endif
    FILE *fp = fopen("/proc/stat", "r");
    if (fp == NULL) {
        return 1;
    }
    const int LINE_MAX_LEN = 256; // enough in here
    char *aline = (char*)malloc(LINE_MAX_LEN);
    if (aline == NULL) {
        LOG(ERROR) << "[HardWare System Info] malloc failed.";
        return 1;
    }
    const int HEADER_MAX_LEN = 10;
    char header[HEADER_MAX_LEN];
    int i=0;
    size_t len=0;
    getline(&aline, &len, fp); // drop the first line
    while (getline(&aline, &len, fp)) {
        i++;
        sscanf(aline, "%s", header);
        if (!strncmp(header, "intr", HEADER_MAX_LEN)) {
            break;
        }
    }
    fclose(fp);
    free(aline);
    return i-1 > 0 ? i-1 : 1;
}

// irix_on == 1 --> irix mode on
// irix_on == 0 --> irix mode off
//
// return this process's the percentage of CPU usage ( %CPU ).
//
// NOTE: the first time call this function would get 0 as result.
static float GetCpuUsage(int is_irix_on) {
    static int cpu_count = 1; // assume cpu count is not variable when process is running
    static unsigned long hertz = 0;
    if (hertz == 0) {
        hertz = sysconf(_SC_CLK_TCK);
        cpu_count = GetCpuCount();
    }

    static struct timeval oldtimev;
    struct timeval timev;
    gettimeofday(&timev, NULL);
    float et = (timev.tv_sec - oldtimev.tv_sec)
        + (float)(timev.tv_usec - oldtimev.tv_usec) / 1000000.0;
    oldtimev.tv_sec = timev.tv_sec;
    oldtimev.tv_usec = timev.tv_usec;

    float frame_etscale;
    if (is_irix_on) {
        frame_etscale = 100.0f / ((float)hertz * et);
    } else {
        frame_etscale = 100.0f / ((float)hertz * et * cpu_count);
    }

    static unsigned long oldtick;
    unsigned long newtick;
    newtick = ProcessCpuTick();
    float u = (newtick - (float)oldtick) * frame_etscale;
    oldtick = newtick;

    const float MAX_CPU_USAGE = 99.9f;
    if (u > MAX_CPU_USAGE ) {
        u = MAX_CPU_USAGE;
    }

    // rounding cpu usage to 1 decimal places
    const int USAGE_STR_MAX_LEN = 5;
    char usage_str[USAGE_STR_MAX_LEN];
    sprintf(usage_str, "%.1f\n", u);
    sscanf(usage_str, "%f", &u);
    return u;
}

void TabletNodeSysInfo::CollectHardwareInfo() {
    MutexLock lock(&m_mutex);
    int pid = getpid();
    FILE* f;
    std::ostringstream ss;
    ss << "/proc/" << pid << "/";
    int64_t cur_ts = get_micros();

    int64_t interval = cur_ts - m_mem_check_ts;
    if (interval / 1000000 > FLAGS_tera_tabletnode_sysinfo_mem_collect_interval) {
        m_mem_check_ts = cur_ts;
        int64_t mem;
        f = fopen((ss.str() + "statm").data(), "r");
        if (f == NULL) {
            return;
        }
        fscanf(f, "%*d %ld", &mem);
        mem = mem * 4 * 1024;
        fclose(f);
        m_info.set_mem_used(mem);

        VLOG(15) << "[HardWare System Info] Memory: " << mem * 4;
        return;
    }

    interval = cur_ts - m_net_check_ts;
    if (interval / 1000000 > FLAGS_tera_tabletnode_sysinfo_net_collect_interval) {
        m_net_check_ts = cur_ts;
        int64_t net_rx = 0, net_tx = 0;
        f = fopen((ss.str() + "net/dev").data(), "r");
        if (f == NULL) {
            return;
        }
        fseek(f, 327, SEEK_SET);
        for (int i = 0; i < 10; i++) {
            while (':' != fgetc(f));
            fscanf(f, "%ld%*d%*d%*d%*d%*d%*d%*d%ld", &net_rx, &net_tx);
            if (net_rx > 0 && net_tx > 0) {
                break;
            }
        }
        fclose(f);

        int64_t tmp;
        tmp = (net_rx - m_net_rx_total) * 1000000 / interval;
        m_info.set_net_rx(tmp);
        tmp = (net_tx - m_net_tx_total) * 1000000 / interval;
        m_info.set_net_tx(tmp);
        m_net_rx_total = net_rx;
        m_net_tx_total = net_tx;

        VLOG(15) << "[HardWare System Info] Network RX/TX: " << net_rx << " / " << net_tx;
        return;
    }

    interval = cur_ts - m_cpu_check_ts;
    if (interval / 1000000 > FLAGS_tera_tabletnode_sysinfo_cpu_collect_interval) {
        m_cpu_check_ts = cur_ts;
        float cpu_usage = GetCpuUsage(0);
        m_info.set_cpu_usage(cpu_usage);
        VLOG(15) << "[HardWare System Info] %CPU: "<< cpu_usage;
        return;
    }
}

void TabletNodeSysInfo::GetTabletNodeInfo(TabletNodeInfo* info) {
    MutexLock lock(&m_mutex);
    info->CopyFrom(m_info);
}

void TabletNodeSysInfo::GetTabletMetaList(TabletMetaList* meta_list) {
    MutexLock lock(&m_mutex);
    meta_list->CopyFrom(m_tablet_list);
}

void TabletNodeSysInfo::SetStatus(TabletNodeStatus status) {
    MutexLock lock(&m_mutex);
    m_info.set_status_t(kTabletNodeRegistered);
}

void TabletNodeSysInfo::DumpLog() {
    MutexLock lock(&m_mutex);

    double snappy_ratio = (double)leveldb::snappy_before_size_counter.Clear()
                          / leveldb::snappy_after_size_counter.Clear();
    LOG(INFO) << "[SysInfo]"
        << " low_level " << m_info.low_read_cell()
        << " read " << m_info.read_rows()
        << " rspeed " << utils::ConvertByteToString(m_info.read_size())
        << " write " << m_info.write_rows()
        << " wspeed " << utils::ConvertByteToString(m_info.write_size())
        << " scan " << m_info.scan_rows()
        << " sspeed " << utils::ConvertByteToString(m_info.scan_size())
        << " snappy " << snappy_ratio
        << " rawcomp " << leveldb::rawkey_compare_counter.Clear();

    // hardware info
    LOG(INFO) << "[HardWare Info] "
        << " mem_used " << m_info.mem_used() << " "
        << utils::ConvertByteToString(m_info.mem_used())
        << " net_tx " << m_info.net_tx() << " "
        << utils::ConvertByteToString(m_info.net_tx())
        << " net_rx " << m_info.net_rx() << " "
        << utils::ConvertByteToString(m_info.net_rx())
        << " cpu_usage " << m_info.cpu_usage() << "%";

    // net and io info
    LOG(INFO) << "[IO]"
        << " dfs_r " << m_info.dfs_io_r() << " "
        << utils::ConvertByteToString(m_info.dfs_io_r())
        << " dfs_w " << m_info.dfs_io_w() << " "
        << utils::ConvertByteToString(m_info.dfs_io_w())
        << " local_r " << m_info.local_io_r() << " "
        << utils::ConvertByteToString(m_info.local_io_r())
        << " local_w " << m_info.local_io_w() << " "
        << utils::ConvertByteToString(m_info.local_io_w())
        << " ssd_r " << leveldb::ssd_read_counter.Clear() << " "
        << utils::ConvertByteToString(leveldb::ssd_read_size_counter.Clear())
        << " ssd_w " << leveldb::ssd_write_counter.Clear() << " "
        << utils::ConvertByteToString(leveldb::ssd_write_size_counter.Clear());

    // extra info
    std::ostringstream ss;
    int cols = m_info.extra_info_size();
    ss << "[Pending]";
    for (int i = 0; i < cols; ++i) {
        ss << m_info.extra_info(i).name() << " " << m_info.extra_info(i).value() << " ";
    }
    LOG(INFO) << ss.str();

    // DFS info
    double rdelay = leveldb::dfs_read_counter.Get() ?
        leveldb::dfs_read_delay_counter.Clear()/1000/leveldb::dfs_read_counter.Get()
        : 0;
    double wdelay = leveldb::dfs_write_counter.Get() ?
        leveldb::dfs_write_delay_counter.Clear()/1000/leveldb::dfs_write_counter.Get()
        : 0;
    double sdelay = leveldb::dfs_sync_counter.Get() ?
        leveldb::dfs_sync_delay_counter.Clear()/1000/leveldb::dfs_sync_counter.Get()
        : 0;

    LOG(INFO) << "[Dfs] read " << leveldb::dfs_read_counter.Clear() << " "
        << leveldb::dfs_read_hang_counter.Get() << " "
        << "rdelay " << rdelay << " "
        << "write " << leveldb::dfs_write_counter.Clear() << " "
        << leveldb::dfs_write_hang_counter.Get() << " "
        << "wdelay " << wdelay << " "
        << "sync " << leveldb::dfs_sync_counter.Clear() << " "
        << leveldb::dfs_sync_hang_counter.Get() << " "
        << "sdelay " << sdelay << " "
        << "flush " << leveldb::dfs_flush_counter.Clear() << " "
        << leveldb::dfs_flush_hang_counter.Get() << " "
        << "list " << leveldb::dfs_list_counter.Clear() << " "
        << leveldb::dfs_list_hang_counter.Get() << " "
        << "info " << leveldb::dfs_info_counter.Clear() << " "
        << leveldb::dfs_info_hang_counter.Get() << " "
        << "exists " << leveldb::dfs_exists_counter.Clear() << " "
        << leveldb::dfs_exists_hang_counter.Get() << " "
        << "open " << leveldb::dfs_open_counter.Clear() << " "
        << leveldb::dfs_open_hang_counter.Get() << " "
        << "close " << leveldb::dfs_close_counter.Clear() << " "
        << leveldb::dfs_close_hang_counter.Get() << " "
        << "delete " << leveldb::dfs_delete_counter.Clear() << " "
        << leveldb::dfs_delete_hang_counter.Get() << " "
        << "tell " << leveldb::dfs_tell_counter.Clear() << " "
        << leveldb::dfs_tell_hang_counter.Get() << " "
        << "other " << leveldb::dfs_other_counter.Clear() << " "
        << leveldb::dfs_other_hang_counter.Get();

    LOG(INFO) << "[Local] read " << leveldb::posix_read_counter.Clear() << " "
        << "write " << leveldb::posix_write_counter.Clear() << " "
        << "sync " << leveldb::posix_sync_counter.Clear() << " "
        << "list " << leveldb::posix_list_counter.Clear() << " "
        << "info " << leveldb::posix_info_counter.Clear() << " "
        << "exists " << leveldb::posix_exists_counter.Clear() << " "
        << "open " << leveldb::posix_open_counter.Clear() << " "
        << "close " << leveldb::posix_close_counter.Clear() << " "
        << "delete " << leveldb::posix_delete_counter.Clear() << " "
        << "tell " << leveldb::posix_tell_counter.Clear() << " "
        << "seek " << leveldb::posix_seek_counter.Clear() << " "
        << "other " << leveldb::posix_other_counter.Clear();
}

} // namespace tabletnode
} // namespace tera
