// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#include "tabletnode_sysinfo.h"

#include <iomanip>
#include <iostream>
#include <sstream>

#include "common/base/string_number.h"
#include "proto/proto_helper.h"
#include "utils/timer.h"
#include "utils/tprinter.h"
#include "utils/utils_cmd.h"

DEFINE_int32(tera_tabletnode_sysinfo_mem_collect_interval, 10, "interval of mem checking(s)");
DEFINE_int32(tera_tabletnode_sysinfo_net_collect_interval, 5, "interval of net checking(s)");

namespace leveldb {
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
      m_tablet_check_ts(0) {
}

TabletNodeSysInfo::TabletNodeSysInfo(const TabletNodeInfo& info)
    : m_info(info),
      m_mem_check_ts(0),
      m_net_check_ts(0),
      m_net_tx_total(0),
      m_net_rx_total(0),
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
    m_info.set_time_stamp(get_micros());
}

int64_t TabletNodeSysInfo::GetTimeStamp() {
    return m_info.time_stamp();
}

void TabletNodeSysInfo::SetTimeStamp(int64_t ts) {
    MutexLock lock(&m_mutex);
    m_info.set_time_stamp(ts);
}

void TabletNodeSysInfo::CollectTabletNodeInfo(TabletManager* tablet_manager,
                                              const string& server_addr) {
    MutexLock lock(&m_mutex);
    int64_t cur_ts = get_micros();
    int64_t interval = cur_ts - m_tablet_check_ts;
    m_tablet_check_ts = cur_ts;

    m_tablet_list.Clear();
    int64_t total_size = 0;
    int32_t low_read_cell = 0;
    int32_t scan_rows = 0;
    int32_t scan_kvs = 0;
    int32_t scan_size = 0;
    int32_t read_rows = 0;
    int32_t read_kvs = 0;
    int32_t read_size = 0;
    int32_t write_rows = 0;
    int32_t write_kvs = 0;
    int32_t write_size = 0;
    int32_t busy_cnt = 0;

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
        tablet_meta->set_table_size(tablet_io->GetDataSize());
        tablet_meta->set_compact_status(tablet_io->GetCompactStatus());
        total_size += tablet_meta->table_size();

        TabletCounter* counter = m_tablet_list.add_counter();
        tablet_io->GetAndClearCounter(counter, interval);
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
    m_info.set_low_read_cell(low_read_cell);
    m_info.set_scan_rows(scan_rows);
    m_info.set_scan_kvs(scan_kvs);
    m_info.set_scan_size(scan_size);
    m_info.set_read_rows(read_rows);
    m_info.set_read_kvs(read_kvs);
    m_info.set_read_size(read_size);
    m_info.set_write_rows(write_rows);
    m_info.set_write_kvs(write_kvs);
    m_info.set_write_size(write_size);
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

    // collect extra infos
    m_info.clear_extra_info();
    ExtraTsInfo* einfo = m_info.add_extra_info();
    if (read_rows == 0) {
        tmp = 0;
    } else {
        tmp = rand_read_delay.Clear() / m_info.read_rows();
    }
    einfo->set_name("rand_read_delay");
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
        << " snappy " << snappy_ratio;

    // net and io info
    LOG(INFO) << "[IO]"
        << " net_tx " << m_info.net_tx() << " "
        << utils::ConvertByteToString(m_info.net_tx())
        << " net_rx " << m_info.net_rx() << " "
        << utils::ConvertByteToString(m_info.net_rx())
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

    LOG(INFO) << "[Dfs] read " << leveldb::dfs_read_counter.Clear() << " "
        << leveldb::dfs_read_hang_counter.Get() << " "
        << "rdelay " << rdelay << " "
        << "write " << leveldb::dfs_write_counter.Clear() << " "
        << leveldb::dfs_write_hang_counter.Get() << " "
        << "wdelay " << wdelay << " "
        << "sync " << leveldb::dfs_sync_counter.Clear() << " "
        << leveldb::dfs_sync_hang_counter.Get() << " "
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
