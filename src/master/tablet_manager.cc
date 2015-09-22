// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/tablet_manager.h"

#include <fstream>
#include <limits>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sofa/pbrpc/pbrpc.h>

#include "common/base/string_ext.h"
#include "common/base/string_format.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "db/filename.h"
#include "io/io_utils.h"
#include "io/utils_leveldb.h"
#include "master/master_impl.h"
#include "proto/kv_helper.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode_client.h"
#include "types.h"
#include "utils/string_util.h"

DECLARE_string(tera_working_dir);
DECLARE_string(tera_master_meta_table_path);
DECLARE_string(tera_master_meta_table_name);
DECLARE_bool(tera_zk_enabled);

DECLARE_int32(tera_master_impl_retry_times);
DECLARE_int32(tera_master_load_balance_accumulate_query_times);
DECLARE_int32(tera_tabletnode_connect_retry_period);

DECLARE_bool(tera_delete_obsolete_tabledir_enabled);

DECLARE_string(tera_tabletnode_path_prefix);

namespace tera {
namespace master {

std::ostream& operator << (std::ostream& o, const Tablet& tablet) {
    MutexLock lock(&tablet.m_mutex);
    o << "table: " << tablet.m_meta.table_name() << ", range: ["
      << DebugString(tablet.m_meta.key_range().key_start()) << ", "
      << DebugString(tablet.m_meta.key_range().key_end()) << "], path: "
      << tablet.m_meta.path() << ", server: "
      << tablet.m_meta.server_addr();
    return o;
}

std::ostream& operator << (std::ostream& o, const TabletPtr& tablet) {
    o << *tablet;
    return o;
}

Tablet::Tablet() {}

Tablet::Tablet(const TabletMeta& meta) : m_meta(meta) {}

Tablet::Tablet(const TabletMeta& meta, TablePtr table)
    : m_meta(meta), m_table(table) {}

Tablet::~Tablet() {
    m_table.reset();
}

void Tablet::ToMeta(TabletMeta* meta) {
    MutexLock lock(&m_mutex);
    meta->CopyFrom(m_meta);
}

const std::string& Tablet::GetTableName() {
    MutexLock lock(&m_mutex);
    return m_meta.table_name();
}

const std::string& Tablet::GetServerAddr() {
    MutexLock lock(&m_mutex);
    return m_meta.server_addr();
}

std::string Tablet::GetServerId() {
    MutexLock lock(&m_mutex);
    return m_server_id;
}

const std::string& Tablet::GetPath() {
    MutexLock lock(&m_mutex);
    return m_meta.path();
}

int64_t Tablet::GetDataSize() {
    MutexLock lock(&m_mutex);
    return m_meta.table_size();
}

const std::string& Tablet::GetKeyStart() {
    MutexLock lock(&m_mutex);
    return m_meta.key_range().key_start();
}

const std::string& Tablet::GetKeyEnd() {
    MutexLock lock(&m_mutex);
    return m_meta.key_range().key_end();
}

const KeyRange& Tablet::GetKeyRange() {
    MutexLock lock(&m_mutex);
    return m_meta.key_range();
}

const TableSchema& Tablet::GetSchema() {
    return m_table->GetSchema();
}

const TabletCounter& Tablet::GetCounter() {
    MutexLock lock(&m_mutex);
    if (m_counter_list.size() > 0) {
        return m_counter_list.back();
    } else {
        return m_average_counter;
    }
}

const TabletCounter& Tablet::GetAverageCounter() {
    MutexLock lock(&m_mutex);
    return m_average_counter;
}

TabletStatus Tablet::GetStatus() {
    MutexLock lock(&m_mutex);
    return m_meta.status();
}

CompactStatus Tablet::GetCompactStatus() {
    MutexLock lock(&m_mutex);
    return m_meta.compact_status();
}

std::string Tablet::GetExpectServerAddr() {
    MutexLock lock(&m_mutex);
    return m_expect_server_addr;
}

TablePtr Tablet::GetTable() {
    return m_table;
}

bool Tablet::IsBusy() {
    MutexLock lock(&m_mutex);
    if (m_counter_list.size() > 0) {
        return m_counter_list.back().is_on_busy();
    } else {
        return false;
    }
}

std::string Tablet::DebugString() {
    MutexLock lock(&m_mutex);
    return m_meta.DebugString();
}

void Tablet::SetCounter(const TabletCounter& counter) {
    MutexLock lock(&m_mutex);

    m_accumu_counter.low_read_cell += counter.low_read_cell();
    m_accumu_counter.scan_rows += counter.scan_rows();
    m_accumu_counter.scan_kvs += counter.scan_kvs();
    m_accumu_counter.scan_size += counter.scan_size();
    m_accumu_counter.read_rows += counter.read_rows();
    m_accumu_counter.read_kvs += counter.read_kvs();
    m_accumu_counter.read_size += counter.read_size();
    m_accumu_counter.write_rows += counter.write_rows();
    m_accumu_counter.write_kvs += counter.write_kvs();
    m_accumu_counter.write_size += counter.write_size();

    uint64_t counter_size = m_counter_list.size();
    const uint64_t max_counter_size = FLAGS_tera_master_load_balance_accumulate_query_times;
    if (counter_size >= max_counter_size) {
        CHECK_EQ(counter_size, max_counter_size);
        TabletCounter& earliest_counter = m_counter_list.front();
        m_accumu_counter.low_read_cell -= earliest_counter.low_read_cell();
        m_accumu_counter.scan_rows -= earliest_counter.scan_rows();
        m_accumu_counter.scan_kvs -= earliest_counter.scan_kvs();
        m_accumu_counter.scan_size -= earliest_counter.scan_size();
        m_accumu_counter.read_rows -= earliest_counter.read_rows();
        m_accumu_counter.read_kvs -= earliest_counter.read_kvs();
        m_accumu_counter.read_size -= earliest_counter.read_size();
        m_accumu_counter.write_rows -= earliest_counter.write_rows();
        m_accumu_counter.write_kvs -= earliest_counter.write_kvs();
        m_accumu_counter.write_size -= earliest_counter.write_size();
        m_counter_list.pop_front();
    }
    m_counter_list.push_back(counter);

    counter_size = m_counter_list.size();
    m_average_counter.set_low_read_cell(m_accumu_counter.low_read_cell / counter_size);
    m_average_counter.set_scan_rows(m_accumu_counter.scan_rows / counter_size);
    m_average_counter.set_scan_kvs(m_accumu_counter.scan_kvs / counter_size);
    m_average_counter.set_scan_size(m_accumu_counter.scan_size / counter_size);
    m_average_counter.set_read_rows(m_accumu_counter.read_rows / counter_size);
    m_average_counter.set_read_kvs(m_accumu_counter.read_kvs / counter_size);
    m_average_counter.set_read_size(m_accumu_counter.read_size / counter_size);
    m_average_counter.set_write_rows(m_accumu_counter.write_rows / counter_size);
    m_average_counter.set_write_kvs(m_accumu_counter.write_kvs / counter_size);
    m_average_counter.set_write_size(m_accumu_counter.write_size / counter_size);
    m_average_counter.set_is_on_busy(counter.is_on_busy());
}

void Tablet::SetSize(int64_t table_size) {
    MutexLock lock(&m_mutex);
    m_meta.set_table_size(table_size);
}

void Tablet::SetSize(const TabletMeta& meta) {
    MutexLock lock(&m_mutex);
    m_meta.set_table_size(meta.table_size());
    m_meta.mutable_lg_size()->CopyFrom(meta.lg_size());
}

void Tablet::SetCompactStatus(CompactStatus compact_status) {
    MutexLock lock(&m_mutex);
    m_meta.set_compact_status(compact_status);
}

void Tablet::SetAddr(const std::string& server_addr) {
    MutexLock lock(&m_mutex);
    m_meta.set_server_addr(server_addr);
}

void Tablet::SetServerId(const std::string& server_id) {
    MutexLock lock(&m_mutex);
    m_server_id = server_id;
}

void Tablet::SetExpectServerAddr(const std::string& server_addr) {
    MutexLock lock(&m_mutex);
    m_expect_server_addr = server_addr;
}

bool Tablet::SetStatus(TabletStatus new_status, TabletStatus* old_status) {
    MutexLock lock(&m_mutex);
    if (NULL != old_status) {
        *old_status = m_meta.status();
    }
    if (CheckStatusSwitch(m_meta.status(), new_status)) {
        m_meta.set_status(new_status);
        return true;
    }
    return false;
}

bool Tablet::SetStatusIf(TabletStatus new_status, TabletStatus if_status,
                         TabletStatus* old_status) {
    MutexLock lock(&m_mutex);
    if (NULL != old_status) {
        *old_status = m_meta.status();
    }
    if (m_meta.status() == if_status
        && CheckStatusSwitch(m_meta.status(), new_status)) {
        m_meta.set_status(new_status);
        return true;
    }
    return false;
}

bool Tablet::SetStatusIf(TabletStatus new_status, TabletStatus if_status,
                         TableStatus if_table_status, TabletStatus* old_status) {
    if (!IsBound()) {
        return false;
    }
    MutexLock lock(&m_table->m_mutex);
    MutexLock lock2(&m_mutex);
    if (NULL != old_status) {
        *old_status = m_meta.status();
    }
    if (m_meta.status() == if_status && m_table->m_status == if_table_status
        && CheckStatusSwitch(m_meta.status(), new_status)) {
        m_meta.set_status(new_status);
        return true;
    }
    return false;
}

bool Tablet::SetAddrIf(const std::string& server_addr, TabletStatus if_status,
                       TabletStatus* old_status) {
    MutexLock lock(&m_mutex);
    if (NULL != old_status) {
        *old_status = m_meta.status();
    }
    if (m_meta.status() == if_status) {
        m_meta.set_server_addr(server_addr);
        return true;
    }
    return false;
}

bool Tablet::SetAddrAndStatus(const std::string& server_addr,
                              TabletStatus new_status,
                              TabletStatus* old_status) {
    MutexLock lock(&m_mutex);
    if (NULL != old_status) {
        *old_status = m_meta.status();
    }
    if (CheckStatusSwitch(m_meta.status(), new_status)) {
        m_meta.set_status(new_status);
        m_meta.set_server_addr(server_addr);
        return true;
    }
    return false;
}

bool Tablet::SetAddrAndStatusIf(const std::string& server_addr,
                                TabletStatus new_status, TabletStatus if_status,
                                TabletStatus* old_status) {
    MutexLock lock(&m_mutex);
    if (NULL != old_status) {
        *old_status = m_meta.status();
    }
    if (m_meta.status() == if_status
        && CheckStatusSwitch(m_meta.status(), new_status)) {
        m_meta.set_status(new_status);
        m_meta.set_server_addr(server_addr);
        return true;
    }
    return false;
}

int32_t Tablet::AddSnapshot(uint64_t snapshot) {
    MutexLock lock(&m_mutex);
    m_meta.add_snapshot_list(snapshot);
    return m_meta.snapshot_list_size() - 1;
}

void Tablet::ListSnapshot(std::vector<uint64_t>* snapshot) {
    MutexLock lock(&m_mutex);
    for (int i = 0; i < m_meta.snapshot_list_size(); i++) {
        snapshot->push_back(m_meta.snapshot_list(i));
    }
}

void Tablet::DelSnapshot(int32_t id) {
    MutexLock lock(&m_mutex);
    google::protobuf::RepeatedField<google::protobuf::uint64>* snapshot_list =
        m_meta.mutable_snapshot_list();
    assert(id < snapshot_list->size());
    snapshot_list->SwapElements(id, snapshot_list->size() - 1);
    snapshot_list->RemoveLast();
}

int32_t Tablet::AddRollback(uint64_t rollback_point) {
    MutexLock lock(&m_mutex);
    m_meta.add_rollback_points(rollback_point);
    return m_meta.rollback_points_size() - 1;
}

void Tablet::ListRollback(std::vector<uint64_t>* rollback_points) {
    MutexLock lock(&m_mutex);
    for (int i = 0; i < m_meta.rollback_points_size(); i++) {
        rollback_points->push_back(m_meta.rollback_points(i));
    }
}

bool Tablet::IsBound() {
    TablePtr null_ptr;
    if (m_table != null_ptr) {
        return true;
    }
    return false;
}

bool Tablet::Verify(const std::string& table_name, const std::string& key_start,
            const std::string& key_end, const std::string& path,
            const std::string& server_addr, StatusCode* ret_status) {
    MutexLock lock(&m_mutex);
    if (m_meta.table_name() != table_name
        || m_meta.key_range().key_start() != key_start
        || m_meta.key_range().key_end() != key_end
        || m_meta.path() != path
        || m_meta.server_addr() != server_addr) {
        SetStatusCode(kTableInvalidArg, ret_status);
        return false;
    }
    return true;
}

void Tablet::ToMetaTableKeyValue(std::string* packed_key,
                                 std::string* packed_value) {
    MutexLock lock(&m_mutex);
    MakeMetaTableKeyValue(m_meta, packed_key, packed_value);
}

bool Tablet::CheckStatusSwitch(TabletStatus old_status,
                               TabletStatus new_status) {
    if (new_status == kTableDeleted) {
        return true;
    }

    switch (old_status) {
    case kTableNotInit:
        if (new_status == kTableReady         // tablet is loaded when master up
            || new_status == kTableOffLine) { // tablet is unload when master up
            return true;
        }
        break;
    case kTableReady:
        if (new_status == kTabletPending        // tabletnode down
            || new_status == kTableOffLine      // tabletnode down (move immidiately)
            || new_status == kTableUnLoading    // ready to move tablet
            || new_status == kTableOnSplit      // begin to split
            || new_status == kTabletOnSnapshot
            || new_status == kTabletDelSnapshot) {
            return true;
        }
        break;
    case kTabletOnSnapshot:
        if (new_status == kTableReady) {
            return true;
        }
        break;
    case kTabletDelSnapshot:
        if (new_status == kTableReady) {
            return true;
        }
        break;
    case kTableOnLoad:
        if (new_status == kTableReady           // load succe
            || new_status == kTableOffLine      // tabletnode down
            || new_status == kTableLoadFail) {  // don't know result, wait tabletnode to be killed
            return true;
        }
        break;
    case kTableLoadFail:
        if (new_status == kTableOffLine) {     // tabletnode is killed
            return true;
        }
        break;
    case kTableOnSplit:
        if (new_status == kTableReady             // request rejected
            || new_status == kTableOffLine        // split fail
            || new_status == kTableSplitFail) {   // don't know result, wait tabletnode to be killed
            return true;
        }
        break;
    case kTableSplitFail:
        if (new_status == kTableOnSplit) {       // tabletnode is killed, ready to scan meta
            return true;
        }
        break;
    case kTabletPending:
        if (new_status == kTableReady            // tabletnode up
            || new_status == kTableOffLine) {    // tabletnode down timeout
            return true;
        }
        break;
    case kTableOffLine:
        if (new_status == kTableReady            // tabletnode up
            || new_status == kTableOnLoad        // begin to load
            || new_status == kTabletPending      // tabletnode down before load
            || new_status == kTabletDisable) {   // table is disabled
            return true;
        }
        break;
    case kTableUnLoading:
        if (new_status == kTableOffLine           // unload succe
            || new_status == kTableReady          // unload status rollback when merge failed
            || new_status == kTableOnMerge        // unload success, ready to merge phase2
            || new_status == kTableUnLoadFail) {  // don't know result, wait tabletnode to be killed
            return true;
        }
        break;
    case kTableUnLoadFail:
        if (new_status == kTableOffLine) {        // tabletnode is killed, ready to load
            return true;
        }
        break;
    case kTableOnMerge:
        if (new_status == kTableOffLine) {        // merge failed, ready to reload
            return true;
        }
        break;
    case kTabletDisable:
        if (new_status == kTabletDeleting
            || new_status == kTableOffLine) {
            return true;
        }
        break;
    case kTabletDeleting:
        break;
    default:
        break;
    }

    LOG(ERROR) << "not support status switch "
        << StatusCodeToString(old_status) << " to "
        << StatusCodeToString(new_status);
    return false;
}

std::ostream& operator << (std::ostream& o, const Table& table) {
    MutexLock lock(&table.m_mutex);
    o << "table: " << table.m_name << ", schema: "
        << table.m_schema.ShortDebugString();
    return o;
}

std::ostream& operator << (std::ostream& o, const TablePtr& table) {
    o << *table;
    return o;
}

Table::Table(const std::string& table_name)
    : m_name(table_name),
      m_status(kTableEnable),
      m_deleted_tablet_num(0),
      m_max_tablet_no(0),
      m_create_time((int64_t)time(NULL)) {
}

bool Table::FindTablet(const std::string& key_start, TabletPtr* tablet) {
    MutexLock lock(&m_mutex);
    Table::TabletList::iterator it2 = m_tablets_list.find(key_start);
    if (it2 == m_tablets_list.end()) {
        return false;
    }
    *tablet = it2->second;
    return true;
}

void Table::FindTablet(const std::string& server_addr,
                       std::vector<TabletPtr>* tablet_meta_list) {
    MutexLock lock(&m_mutex);
    Table::TabletList::iterator it2 = m_tablets_list.begin();
    for (; it2 != m_tablets_list.end(); ++it2) {
        TabletPtr tablet = it2->second;
        tablet->m_mutex.Lock();
        if (tablet->m_meta.server_addr() == server_addr) {
            tablet_meta_list->push_back(tablet);
        }
        tablet->m_mutex.Unlock();
    }
}

void Table::GetTablet(std::vector<TabletPtr>* tablet_meta_list) {
    MutexLock lock(&m_mutex);
    Table::TabletList::iterator it2 = m_tablets_list.begin();
    for (; it2 != m_tablets_list.end(); ++it2) {
        TabletPtr tablet = it2->second;
        tablet_meta_list->push_back(tablet);
    }
}

const std::string& Table::GetTableName() {
    MutexLock lock(&m_mutex);
    return m_name;
}

TableStatus Table::GetStatus() {
    MutexLock lock(&m_mutex);
    return m_status;
}

bool Table::SetStatus(TableStatus new_status, TableStatus* old_status) {
    MutexLock lock(&m_mutex);
    if (NULL != old_status) {
        *old_status = m_status;
    }
    if (CheckStatusSwitch(m_status, new_status)) {
        m_status = new_status;
        return true;
    }
    return false;
}

bool Table::CheckStatusSwitch(TableStatus old_status,
                              TableStatus new_status) {
    switch (old_status) {
    // table is either in the process of being enable or is enabled
    case kTableEnable:
        if (new_status == kTableDisable) {    // begin to disable table
            return true;
        }
        break;
    // table is either in the process of being disable or is disabled
    case kTableDisable:
        if (new_status == kTableEnable         // begin to enable table
            || new_status == kTableDeleting) {  // begin to delete table
            return true;
        }
        break;
    // table is in the process of deleting
    case kTableDeleting:
        break;
    default:
        break;
    }
    return false;
}

const TableSchema& Table::GetSchema() {
    MutexLock lock(&m_mutex);
    return m_schema;
}

void Table::SetSchema(const TableSchema& schema) {
    MutexLock lock(&m_mutex);
    m_schema.CopyFrom(schema);
}

int32_t Table::AddSnapshot(uint64_t snapshot) {
    MutexLock lock(&m_mutex);
    m_snapshot_list.push_back(snapshot);
    return m_snapshot_list.size() - 1;
}

int32_t Table::DelSnapshot(uint64_t snapshot) {
    MutexLock lock(&m_mutex);
    std::vector<uint64_t>::iterator it =
        std::find(m_snapshot_list.begin(), m_snapshot_list.end(), snapshot);
    if (it == m_snapshot_list.end()) {
        return -1;
    } else {
        int id = it - m_snapshot_list.begin();
        m_snapshot_list[id] = m_snapshot_list[m_snapshot_list.size()-1];
        m_snapshot_list.resize(m_snapshot_list.size()-1);
        return id;
    }
}
void Table::ListSnapshot(std::vector<uint64_t>* snapshots) {
    MutexLock lock(&m_mutex);
    *snapshots = m_snapshot_list;
}

int32_t Table::AddRollback(uint64_t snapshot_id) {
    MutexLock lock(&m_mutex);
    m_rollback_snapshots.push_back(snapshot_id);
    return m_rollback_snapshots.size() - 1;
}

void Table::ListRollback(std::vector<uint64_t>* snapshots) {
    MutexLock lock(&m_mutex);
    *snapshots = m_rollback_snapshots;
}

void Table::AddDeleteTabletCount() {
    MutexLock lock(&m_mutex);
    m_deleted_tablet_num++;
}

bool Table::NeedDelete() {
    MutexLock lock(&m_mutex);
    if (m_deleted_tablet_num == m_tablets_list.size()) {
        return true;
    }
    return false;
}

void Table::ToMetaTableKeyValue(std::string* packed_key,
                                std::string* packed_value) {
    MutexLock lock(&m_mutex);
    TableMeta meta;
    ToMeta(&meta);
    MakeMetaTableKeyValue(meta, packed_key, packed_value);
}

void Table::ToMeta(TableMeta* meta) {
    meta->set_table_name(m_name);
    meta->set_status(m_status);
    meta->mutable_schema()->CopyFrom(m_schema);
    meta->set_create_time(m_create_time);
    for (size_t i = 0; i < m_snapshot_list.size(); i++) {
        meta->add_snapshot_list(m_snapshot_list[i]);
    }
    for (size_t i = 0; i < m_rollback_snapshots.size(); ++i) {
        meta->add_rollback_snapshot(m_rollback_snapshots[i]);
    }
}

uint64_t Table::GetNextTabletNo() {
    MutexLock lock(&m_mutex);
    m_max_tablet_no++;
    LOG(INFO) << "generate new tablet number: " << m_max_tablet_no;
    return m_max_tablet_no;
}

bool Table::GetTabletsForGc(std::set<uint64_t>* live_tablets,
                            std::set<uint64_t>* dead_tablets) {
    MutexLock lock(&m_mutex);
    std::vector<TabletPtr> tablet_list;
    Table::TabletList::iterator it = m_tablets_list.begin();
    for (; it != m_tablets_list.end(); ++it) {
        TabletPtr tablet = it->second;
        if (tablet->GetStatus() != kTableReady) {
            // any tablet not ready, stop gc
            return false;
        }
        const std::string& path = tablet->GetPath();
        live_tablets->insert(leveldb::GetTabletNumFromPath(path));
        VLOG(10) << "[gc] add live tablet: " << path;
    }

    std::vector<std::string> children;
    leveldb::Env* env = io::LeveldbBaseEnv();
    std::string table_path = FLAGS_tera_tabletnode_path_prefix + m_name;
    env->GetChildren(table_path, &children);
    for (size_t i = 0; i < children.size(); ++i) {
        if (children[i].size() < 5) {
            // skip directory . and ..
            continue;
        }
        std::string path = table_path + "/" + children[i];
        uint64_t tabletnum = leveldb::GetTabletNumFromPath(path);
        if (live_tablets->find(tabletnum) == live_tablets->end()) {
            VLOG(10) << "[gc] add dead tablet: " << path;
            dead_tablets->insert(tabletnum);
        }
    }
    if (dead_tablets->size() == 0) {
        VLOG(10) << "[gc] there is none dead tablets: " << m_name;
        return false;
    }
    return true;
}

TabletManager::TabletManager(Counter* sequence_id,
                             MasterImpl* master_impl,
                             ThreadPool* thread_pool)
    : m_this_sequence_id(sequence_id),
      m_master_impl(master_impl) {}

TabletManager::~TabletManager() {
    ClearTableList();
}

void TabletManager::Init() {
}

void TabletManager::Stop() {
}

bool TabletManager::AddTable(const std::string& table_name,
                             const TableMeta& meta,
                             TablePtr* table, StatusCode* ret_status) {
    // lock table list
    m_mutex.Lock();

    // search table
    TablePtr null_table;
    std::pair<TableList::iterator, bool> ret =
        m_all_tables.insert(std::pair<std::string, TablePtr>(table_name, null_table));
    TableList::iterator it = ret.first;
    if (!ret.second) {
        m_mutex.Unlock();
        LOG(WARNING) << "table: " << table_name << " exist";
        SetStatusCode(kTableExist, ret_status);
        return false;
    }

    it->second.reset(new Table(table_name));
    *table = it->second;
    (*table)->m_mutex.Lock();
    m_mutex.Unlock();
    (*table)->m_schema.CopyFrom(meta.schema());
    (*table)->m_status = meta.status();
    (*table)->m_create_time = meta.create_time();
    for (int32_t i = 0; i < meta.snapshot_list_size(); ++i) {
        (*table)->m_snapshot_list.push_back(meta.snapshot_list(i));
        LOG(INFO) << table_name << " add snapshot " << meta.snapshot_list(i);
    }
    for (int32_t i = 0; i < meta.rollback_snapshot_size(); ++i) {
        (*table)->m_rollback_snapshots.push_back(meta.rollback_snapshot(i));
        LOG(INFO) << table_name << " add rollback " << meta.rollback_snapshot(i);
    }
    (*table)->m_mutex.Unlock();
    return true;
}

bool TabletManager::AddTablet(const TabletMeta& meta, const TableSchema& schema,
                              TabletPtr* tablet, StatusCode* ret_status) {
    // lock table list
    m_mutex.Lock();

    // search table
    TablePtr null_table;
    std::pair<TableList::iterator, bool> ret =
        m_all_tables.insert(std::pair<std::string, TablePtr>(meta.table_name(), null_table));
    TableList::iterator it = ret.first;
    std::string key_start = meta.key_range().key_start();
    if (!ret.second) {
        // search tablet
        Table& table = *it->second;
        table.m_mutex.Lock();
        m_mutex.Unlock();
        if (table.m_tablets_list.end() != table.m_tablets_list.find(key_start)) {
            table.m_mutex.Unlock();
            LOG(WARNING) << "table: " << meta.table_name() << ", start: ["
                << DebugString(key_start) << "] exist";
            SetStatusCode(kTableExist, ret_status);
            return false;
        }
    } else {
        it->second.reset(new Table(meta.table_name()));
        Table& table = *it->second;
        table.m_mutex.Lock();
        m_mutex.Unlock();
        table.m_schema.CopyFrom(schema);
        table.m_status = kTableEnable;
    }
    TablePtr table = it->second;
    tablet->reset(new Tablet(meta, table));
    uint64_t tablet_num = leveldb::GetTabletNumFromPath(meta.path());
    if (table->m_max_tablet_no < tablet_num) {
        table->m_max_tablet_no = tablet_num;
    }
    table->m_tablets_list[key_start] = *tablet;
    table->m_mutex.Unlock();
    return true;
}

bool TabletManager::AddTablet(const std::string& table_name,
                              const std::string& key_start,
                              const std::string& key_end,
                              const std::string& path,
                              const std::string& server_addr,
                              const TableSchema& schema,
                              const TabletStatus& table_status,
                              int64_t data_size, TabletPtr* tablet,
                              StatusCode* ret_status) {
    TabletMeta meta;
    PackTabletMeta(&meta, table_name, key_start, key_end, path,
                   server_addr, table_status, data_size);

    return AddTablet(meta, schema, tablet, ret_status);
}

bool TabletManager::FindTablet(const std::string& table_name,
                               const std::string& key_start,
                               TabletPtr* tablet, StatusCode* ret_status) {
    // lock table list
    m_mutex.Lock();

    // search table
    TableList::iterator it = m_all_tables.find(table_name);
    if (it == m_all_tables.end()) {
        m_mutex.Unlock();
        VLOG(5) << "tablet: " << table_name << " [start: "
            << DebugString(key_start) << "] not exist";
        SetStatusCode(kTableNotFound, ret_status);
        return false;
    }
    Table& table = *it->second;

    // lock table
    table.m_mutex.Lock();
    m_mutex.Unlock();

    // search tablet
    Table::TabletList::iterator it2 = table.m_tablets_list.find(key_start);
    if (it2 == table.m_tablets_list.end()) {
        table.m_mutex.Unlock();
        VLOG(5) << "table: " << table_name << "[start: "
            << DebugString(key_start) << "] not exist";
        SetStatusCode(kTableNotFound, ret_status);
        return false;
    }
    *tablet = it2->second;
    table.m_mutex.Unlock();
    return true;
}

void TabletManager::FindTablet(const std::string& server_addr,
                               std::vector<TabletPtr>* tablet_meta_list) {
    m_mutex.Lock();
    TableList::iterator it = m_all_tables.begin();
    for (; it != m_all_tables.end(); ++it) {
        Table& table = *it->second;
        table.m_mutex.Lock();
        Table::TabletList::iterator it2 = table.m_tablets_list.begin();
        for (; it2 != table.m_tablets_list.end(); ++it2) {
            TabletPtr tablet = it2->second;
            tablet->m_mutex.Lock();
            if (tablet->m_meta.server_addr() == server_addr) {
                tablet_meta_list->push_back(tablet);
            }
            tablet->m_mutex.Unlock();
        }
        table.m_mutex.Unlock();
    }
    m_mutex.Unlock();
}

bool TabletManager::FindTable(const std::string& table_name,
                              std::vector<TabletPtr>* tablet_meta_list,
                              StatusCode* ret_status) {
    // lock table list
    m_mutex.Lock();

    // search table
    TableList::iterator it = m_all_tables.find(table_name);
    if (it == m_all_tables.end()) {
        m_mutex.Unlock();
        LOG(WARNING) << "table: " << table_name << " not exist";
        SetStatusCode(kTableNotFound, ret_status);
        return false;
    }
    Table& table = *it->second;

    // lock table
    table.m_mutex.Lock();
    m_mutex.Unlock();

    // search tablet
    Table::TabletList::iterator it2 = table.m_tablets_list.begin();
    for (; it2 != table.m_tablets_list.end(); ++it2) {
        TabletPtr tablet = it2->second;
        tablet_meta_list->push_back(tablet);
    }

    table.m_mutex.Unlock();
    return true;
}

bool TabletManager::FindTable(const std::string& table_name, TablePtr* tablet) {
    m_mutex.Lock();
    TableList::iterator it = m_all_tables.find(table_name);
    if (it == m_all_tables.end()) {
        m_mutex.Unlock();
        VLOG(5) << "table: " << table_name << " not exist";
        return false;
    }
    *tablet = it->second;
    m_mutex.Unlock();
    return true;
}

int64_t TabletManager::SearchTable(std::vector<TabletPtr>* tablet_meta_list,
                                   const std::string& prefix_table_name,
                                   const std::string& start_table_name,
                                   const std::string& start_tablet_key,
                                   uint32_t max_found, StatusCode* ret_status) {
    if (max_found == 0) {
        return 0;
    }
    if (start_table_name.find(prefix_table_name) != 0) {
        return 0;
    }

    m_mutex.Lock();

    TableList::iterator lower_it = m_all_tables.lower_bound(start_table_name);
    TableList::iterator upper_it = m_all_tables.upper_bound(prefix_table_name + "\xFF");
    if (upper_it == m_all_tables.begin() || lower_it == m_all_tables.end()) {
        SetStatusCode(kTableNotFound, ret_status);
        return -1;
    }

    uint32_t found_num = 0;
    for (TableList::iterator it = lower_it; it != upper_it; ++it) {
        Table& table = *it->second;
        Table::TabletList::iterator it2;
        table.m_mutex.Lock();
        if (start_table_name == it->first) {
            it2 = table.m_tablets_list.lower_bound(start_tablet_key);
        } else {
            it2 = table.m_tablets_list.begin();
        }

        for (; it2 != table.m_tablets_list.end(); ++it2) {
            TabletPtr tablet = it2->second;
            tablet_meta_list->push_back(tablet);
            if (++found_num >= max_found) {
                break;
            }
        }
        table.m_mutex.Unlock();
        if (found_num >= max_found) {
            break;
        }
    }

    m_mutex.Unlock();
    return found_num;
}

bool TabletManager::ShowTable(std::vector<TablePtr>* table_meta_list,
                              std::vector<TabletPtr>* tablet_meta_list,
                              const std::string& start_table_name,
                              const std::string& start_tablet_key,
                              uint32_t max_table_found,
                              uint32_t max_tablet_found,
                              bool* is_more, StatusCode* ret_status) {
    // lock table list
    m_mutex.Lock();

    TableList::iterator it = m_all_tables.lower_bound(start_table_name);
    if (it == m_all_tables.end()) {
        m_mutex.Unlock();
        LOG(ERROR) << "table not found: " << start_table_name;
        SetStatusCode(kTableNotFound, ret_status);
        return false;
    }

    uint32_t table_found_num = 0;
    uint32_t tablet_found_num = 0;
    for (; it != m_all_tables.end(); ++it) {
        TablePtr table = it->second;
        Table::TabletList::iterator it2;

        table->m_mutex.Lock();
        if (table_meta_list != NULL) {
            table_meta_list->push_back(table);
        }
        table_found_num++;
        if (table_found_num == 1) {
            it2 = table->m_tablets_list.lower_bound(start_tablet_key);
        } else {
            it2 = table->m_tablets_list.begin();
        }
        for (; it2 != table->m_tablets_list.end(); ++it2) {
            if (tablet_found_num >= max_tablet_found) {
                break;
            }
            TabletPtr tablet = it2->second;
            tablet_found_num++;
            if (tablet_meta_list != NULL) {
                tablet_meta_list->push_back(tablet);
            }
        }
        table->m_mutex.Unlock();
        if (table_found_num >= max_table_found) {
            break;
        }
    }

    m_mutex.Unlock();
    return true;
}

bool TabletManager::DeleteTable(const std::string& table_name,
                                StatusCode* ret_status) {
    // lock table list
    MutexLock lock(&m_mutex);

    // search table
    TableList::iterator it = m_all_tables.find(table_name);
    if (it == m_all_tables.end()) {
        LOG(WARNING) << "table: " << table_name << " not exist";
        SetStatusCode(kTableNotFound, ret_status);
        return true;
    }
    Table& table = *it->second;

    // make sure no other thread ref this table
    table.m_mutex.Lock();
    table.m_mutex.Unlock();

    table.m_tablets_list.clear();
//    // delete every tablet
//    Table::TabletList::iterator it2 = table.m_tablets_list.begin();
//    for (; it2 != table.m_tablets_list.end(); ++it) {
//        Tablet& tablet = *it2->second;
//        // make sure no other thread ref this tablet
//        tablet.m_mutex.Lock();
//        tablet.m_mutex.Unlock();
//        delete &tablet;
//        table.m_tablets_list.erase(it2);
//    }

    // delete &table;
    m_all_tables.erase(it);
    return true;
}

bool TabletManager::DeleteTablet(const std::string& table_name,
                                 const std::string& key_start,
                                 StatusCode* ret_status) {
    // lock table list
    MutexLock lock(&m_mutex);

    // search table
    TableList::iterator it = m_all_tables.find(table_name);
    if (it == m_all_tables.end()) {
        LOG(WARNING) << "table: " << table_name << " [start: "
            << DebugString(key_start) << "] not exist";
        SetStatusCode(kTableNotFound, ret_status);
        return true;
    }
    Table& table = *it->second;

    // make sure no other thread ref this table
    table.m_mutex.Lock();
    table.m_mutex.Unlock();

    // search tablet
    Table::TabletList::iterator it2 = table.m_tablets_list.find(key_start);
    if (it2 == table.m_tablets_list.end()) {
        LOG(WARNING) << "table: " << table_name << " [start: "
            << DebugString(key_start) << "] not exist";
        SetStatusCode(kTableNotFound, ret_status);
        return true;
    }
//    Tablet& tablet = *it2->second;
//    // make sure no other thread ref this tablet
//    tablet.m_mutex.Lock();
//    tablet.m_mutex.Unlock();
//    delete &tablet;
    table.m_tablets_list.erase(it2);

    if (table.m_tablets_list.empty()) {
        // clean up specific table dir in file system
        if (FLAGS_tera_delete_obsolete_tabledir_enabled &&
            !io::MoveEnvDirToTrash(table.GetTableName())) {
            LOG(ERROR) << "fail to move droped table to trash dir, tablename: "
                << table.GetTableName();
        }
        // delete &table;
        m_all_tables.erase(it);
    }
    return true;
}

void TabletManager::WriteToStream(std::ofstream& ofs,
                                  const std::string& key,
                                  const std::string& value) {
    uint32_t key_size = key.size();
    uint32_t value_size = value.size();
    ofs.write((char*)&key_size, sizeof(key_size));
    ofs.write(key.data(), key_size);
    ofs.write((char*)&value_size, sizeof(value_size));
    ofs.write(value.data(), value_size);
}

bool TabletManager::DumpMetaTableToFile(const std::string& filename,
                                        StatusCode* status) {
    std::ofstream ofs(filename.c_str(), std::ofstream::binary | std::ofstream::trunc);
    if (!ofs.is_open()) {
        LOG(WARNING) << "fail to open file " << filename << " for write";
        SetStatusCode(kIOError, status);
        return false;
    }

    // get all table and tablet meta
    std::vector<TablePtr> table_list;
    std::vector<TabletPtr> tablet_list;
    ShowTable(&table_list, &tablet_list);

    // dump table meta
    for (size_t i = 0; i < table_list.size(); i++) {
        TablePtr table = table_list[i];
        std::string key, value;
        table->ToMetaTableKeyValue(&key, &value);
        WriteToStream(ofs, key, value);
    }

    // dump tablet meta
    for (size_t i = 0; i < tablet_list.size(); i++) {
        TabletPtr tablet = tablet_list[i];
        std::string key, value;
        tablet->ToMetaTableKeyValue(&key, &value);
        WriteToStream(ofs, key, value);
    }

    if (ofs.fail()) {
        LOG(WARNING) << "fail to write to file " << filename;
        SetStatusCode(kIOError, status);
        return false;
    }
    ofs.close();
    return true;
}

void TabletManager::LoadTableMeta(const std::string& key,
                                  const std::string& value) {
    TableMeta meta;
    ParseMetaTableKeyValue(key, value, &meta);
    TablePtr table;
    StatusCode ret_status = kTabletNodeOk;
    if (meta.table_name() == FLAGS_tera_master_meta_table_name) {
        LOG(INFO) << "ignore meta table record in meta table";
    } else if (!AddTable(meta.table_name(), meta, &table, &ret_status)) {
        LOG(ERROR) << "duplicate table in meta table: table="
            << meta.table_name();
        // TODO: try correct invalid record
    } else {
        VLOG(5) << "load table record: " << table;
    }
}

void TabletManager::LoadTabletMeta(const std::string& key,
                                   const std::string& value) {
    TabletMeta meta;
    ParseMetaTableKeyValue(key, value, &meta);
    TabletPtr tablet;
    StatusCode ret_status = kTabletNodeOk;
    if (meta.table_name() == FLAGS_tera_master_meta_table_name) {
        LOG(INFO) << "ignore meta tablet record in meta table";
    } else {
        TablePtr table;
        if (!FindTable(meta.table_name(), &table)) {
            LOG(WARNING) << "table schema not exist, skip this tablet: "
                << meta.path();
            return;
        }
        meta.set_status(kTableNotInit);
        if (!AddTablet(meta, table->GetSchema(), &tablet, &ret_status)) {
            LOG(ERROR) << "duplicate tablet in meta table: table=" << meta.table_name()
                << " start=" << DebugString(meta.key_range().key_start());
            // TODO: try correct invalid record
        } else {
            for (int i = 0; i < meta.snapshot_list_size(); ++i) {
                tablet->AddSnapshot(meta.snapshot_list(i));
            }
            for (int i = 0 ; i < meta.rollback_points_size(); ++i) {
                tablet->AddRollback(meta.rollback_points(i));
            }
            VLOG(5) << "load tablet record: " << tablet;
        }
    }
}

bool TabletManager::ClearMetaTable(const std::string& meta_tablet_addr,
                                   StatusCode* ret_status) {
    WriteTabletRequest write_request;
    WriteTabletResponse write_response;

    ScanTabletRequest scan_request;
    ScanTabletResponse scan_response;
    scan_request.set_sequence_id(m_this_sequence_id->Inc());
    scan_request.set_table_name(FLAGS_tera_master_meta_table_name);
    scan_request.set_start("");
    scan_request.set_end("");

    tabletnode::TabletNodeClient meta_node_client(meta_tablet_addr);

    bool scan_success = false;
    while (meta_node_client.ScanTablet(&scan_request, &scan_response)) {
        if (scan_response.status() != kTabletNodeOk) {
            SetStatusCode(scan_response.status(), ret_status);
            LOG(WARNING) << "fail to scan meta table: "
                << StatusCodeToString(scan_response.status());
            return false;
        }
        if (scan_response.results().key_values_size() <= 0) {
            LOG(INFO) << "scan meta table success";
            scan_success = true;
            break;
        }
        uint32_t record_size = scan_response.results().key_values_size();
        std::string last_record_key;
        for (uint32_t i = 0; i < record_size; i++) {
            const KeyValuePair& record = scan_response.results().key_values(i);
            last_record_key = record.key();
            RowMutationSequence* mu_seq = write_request.add_row_list();
            mu_seq->set_row_key(record.key());
            Mutation* mutation = mu_seq->add_mutation_sequence();
            mutation->set_type(kDeleteRow);
        }
        std::string next_record_key = NextKey(last_record_key);
        scan_request.set_start(next_record_key);
        scan_request.set_end("");
        scan_request.set_sequence_id(m_this_sequence_id->Inc());
        scan_response.Clear();
    }

    if (!scan_success) {
        SetStatusCode(kRPCError, ret_status);
        LOG(WARNING) << "fail to scan meta table: "
            << StatusCodeToString(kRPCError);
        return false;
    }

    write_request.set_sequence_id(m_this_sequence_id->Inc());
    write_request.set_tablet_name(FLAGS_tera_master_meta_table_name);
    if (!meta_node_client.WriteTablet(&write_request, &write_response)) {
        SetStatusCode(kRPCError, ret_status);
        LOG(WARNING) << "fail to clear meta tablet: "
            << StatusCodeToString(kRPCError);
        return false;
    }
    StatusCode status = write_response.status();
    if (status == kTabletNodeOk && write_response.row_status_list_size() > 0) {
        status = write_response.row_status_list(0);
    }
    if (status != kTabletNodeOk) {
        SetStatusCode(status, ret_status);
        LOG(WARNING) << "fail to clear meta tablet: "
            << StatusCodeToString(status);
        return false;
    }

    LOG(INFO) << "clear meta tablet";
    return true;
}

bool TabletManager::DumpMetaTable(const std::string& meta_tablet_addr,
                                  StatusCode* ret_status) {
    std::vector<TablePtr> tables;
    std::vector<TabletPtr> tablets;
    ShowTable(&tables, &tablets);

    WriteTabletRequest request;
    WriteTabletResponse response;
    request.set_sequence_id(m_this_sequence_id->Inc());
    request.set_tablet_name(FLAGS_tera_master_meta_table_name);
    request.set_is_sync(true);
    request.set_is_instant(true);
    // dump table record
    for (size_t i = 0; i < tables.size(); i++) {
        std::string packed_key;
        std::string packed_value;
        tables[i]->ToMetaTableKeyValue(&packed_key, &packed_value);
        RowMutationSequence* mu_seq = request.add_row_list();
        mu_seq->set_row_key(packed_key);
        Mutation* mutation = mu_seq->add_mutation_sequence();
        mutation->set_type(kPut);
        mutation->set_value(packed_value);
    }
    // dump tablet record
    int32_t request_size = 0;
    for (size_t i = 0; i < tablets.size(); i++) {
        std::string packed_key;
        std::string packed_value;
        if (tablets[i]->GetPath().empty()) {
            std::string path = leveldb::GetTabletPathFromNum(tablets[i]->GetTableName(),
                                                             tablets[i]->GetTable()->GetNextTabletNo());
            tablets[i]->m_meta.set_path(path);
        }
        tablets[i]->ToMetaTableKeyValue(&packed_key, &packed_value);
        RowMutationSequence* mu_seq = request.add_row_list();
        mu_seq->set_row_key(packed_key);
        Mutation* mutation = mu_seq->add_mutation_sequence();
        mutation->set_type(kPut);
        mutation->set_value(packed_value);
        request_size += mu_seq->ByteSize();

        if (i == tablets.size() - 1 || request_size >= kMaxRpcSize) {
            tabletnode::TabletNodeClient meta_node_client(meta_tablet_addr);
            if (!meta_node_client.WriteTablet(&request, &response)) {
                SetStatusCode(kRPCError, ret_status);
                LOG(WARNING) << "fail to dump meta tablet: "
                    << StatusCodeToString(kRPCError);
                return false;
            }
            StatusCode status = response.status();
            if (status == kTabletNodeOk && response.row_status_list_size() > 0) {
                status = response.row_status_list(0);
            }
            if (status != kTabletNodeOk) {
                SetStatusCode(status, ret_status);
                LOG(WARNING) << "fail to dump meta tablet: "
                    << StatusCodeToString(status);
                return false;
            }
            request.clear_row_list();
            response.Clear();
            request_size = 0;
        }
    }

    LOG(INFO) << "dump meta tablet";
    return true;
}

void TabletManager::ClearTableList() {
    MutexLock lock(&m_mutex);
    TableList::iterator it = m_all_tables.begin();
    for (; it != m_all_tables.end(); ++it) {
        Table& table = *it->second;
        table.m_mutex.Lock();
        table.m_mutex.Unlock();
        table.m_tablets_list.clear();
        //delete &table;
    }
    m_all_tables.clear();
}

void TabletManager::PackTabletMeta(TabletMeta* meta,
                                   const std::string& table_name,
                                   const std::string& key_start,
                                   const std::string& key_end,
                                   const std::string& path,
                                   const std::string& server_addr,
                                   const TabletStatus& table_status,
                                   int64_t data_size) {
    meta->set_table_name(table_name);
    meta->set_path(path);
    meta->set_server_addr(server_addr);
    meta->set_status(table_status);
    meta->set_table_size(data_size);

    KeyRange* key_range = meta->mutable_key_range();
    key_range->set_key_start(key_start);
    key_range->set_key_end(key_end);
}

void TabletManager::UpdateTabletMeta(TabletMeta* new_meta,
                                     const TabletMeta& old_meta,
                                     const std::string* key_end,
                                     const std::string* path,
                                     const std::string* server_addr,
                                     const TabletStatus* table_status,
                                     int64_t* table_size,
                                     const CompactStatus* compact_status) {
    new_meta->set_table_name(old_meta.table_name());
    if (NULL != path) {
        new_meta->set_path(*path);
    } else {
        new_meta->set_path(old_meta.path());
    }
    if (NULL != server_addr) {
        new_meta->set_server_addr(*server_addr);
    } else {
        new_meta->set_server_addr(old_meta.server_addr());
    }
    if (NULL != table_status) {
        new_meta->set_status(*table_status);
    } else {
        new_meta->set_status(old_meta.status());
    }
    if (NULL != table_size) {
        new_meta->set_table_size(*table_size);
    } else {
        new_meta->set_table_size(old_meta.table_size());
    }
    if (NULL != compact_status) {
        new_meta->set_compact_status(*compact_status);
    } else {
        new_meta->set_compact_status(old_meta.compact_status());
    }
    KeyRange* key_range = new_meta->mutable_key_range();
    key_range->set_key_start(old_meta.key_range().key_start());
    if (NULL != key_end) {
        key_range->set_key_end(*key_end);
    } else {
        key_range->set_key_end(old_meta.key_range().key_end());
    }
}

bool TabletManager::GetMetaTabletAddr(std::string* addr) {
    TabletPtr meta_tablet;
    if (FindTablet(FLAGS_tera_master_meta_table_name, "", &meta_tablet)
        && meta_tablet->GetStatus() == kTableReady) {
        *addr = meta_tablet->GetServerAddr();
        return true;
    }
    VLOG(5) << "fail to get meta addr";
    return false;
}

bool TabletManager::PickMergeTablet(TabletPtr& tablet, TabletPtr* tablet2) {
    MutexLock lock(&m_mutex);
    std::string table_name = tablet->GetTableName();

    // search table
    TableList::iterator it = m_all_tables.find(table_name);
    if (it == m_all_tables.end()) {
        LOG(ERROR) << "[merge] table: " << table_name << " not exist";
        return false;
    }
    Table& table = *it->second;
    if (table.m_tablets_list.size() < 2) {
        VLOG(20) << "[merge] table: " << table_name << " only have 1 tablet.";
        return false;
    }

    // make sure no other thread ref this table
    table.m_mutex.Lock();
    table.m_mutex.Unlock();

    // search tablet
    Table::TabletList::iterator it2 = table.m_tablets_list.find(tablet->GetKeyStart());
    if (it2 == table.m_tablets_list.end()) {
        LOG(ERROR) << "[merge] table: " << table_name << " [start: "
            << DebugString(tablet->GetKeyStart()) << "] not exist";
        return false;
    }
    TabletPtr prev, next;
    if (it2 != table.m_tablets_list.begin()) {
        it2--;
        prev = it2->second;
        it2++;
    } else {
        // only have 1 neighbour tablet
        *tablet2 = (++it2)->second;
        if ((*tablet2)->GetDataSize() < 0) {
            // tablet not ready, skip merge
            return false;
        }
        return true;
    }
    if (++it2 != table.m_tablets_list.end()) {
        next = it2->second;
    } else {
        // only have 1 neighbour tablet
        *tablet2 = prev;
        if ((*tablet2)->GetDataSize() < 0) {
            // tablet not ready, skip merge
            return false;
        }
        return true;
    }
    if (prev->GetDataSize() < 0 || next->GetDataSize() < 0) {
        // some tablet not ready, skip merge
        return false;
    }
    // choose the smaller neighbour tablet
    *tablet2 = prev->GetDataSize() > next->GetDataSize() ? next : prev;
    return true;
}

bool TabletManager::RpcChannelHealth(int32_t err_code) {
    return err_code != sofa::pbrpc::RPC_ERROR_CONNECTION_CLOSED
        && err_code != sofa::pbrpc::RPC_ERROR_SERVER_SHUTDOWN
        && err_code != sofa::pbrpc::RPC_ERROR_SERVER_UNREACHABLE
        && err_code != sofa::pbrpc::RPC_ERROR_SERVER_UNAVAILABLE;
}

void TabletManager::TryMajorCompact(Tablet* tablet) {
    VLOG(5) << "TryMajorCompact() for " << tablet->m_meta.path();
    MutexLock lock(&tablet->m_mutex);
    if (!tablet || tablet->m_meta.compact_status() != kTableNotCompact) {
        return;
    } else {
        tablet->m_meta.set_compact_status(kTableOnCompact);
    }

    CompactTabletRequest* request = new CompactTabletRequest;
    CompactTabletResponse* response = new CompactTabletResponse;
    request->set_sequence_id(m_this_sequence_id->Inc());
    request->set_tablet_name(tablet->m_meta.table_name());
    request->mutable_key_range()->CopyFrom(tablet->m_meta.key_range());

    tabletnode::TabletNodeClient node_client(tablet->m_meta.server_addr());
    Closure<void, CompactTabletRequest*, CompactTabletResponse*, bool, int>* done =
        NewClosure(this, &TabletManager::MajorCompactCallback, tablet,
                   FLAGS_tera_master_impl_retry_times);
    node_client.CompactTablet(request, response, done);
}

void TabletManager::MajorCompactCallback(Tablet* tb, int32_t retry,
                                         CompactTabletRequest* request,
                                         CompactTabletResponse* response,
                                         bool failed, int error_code) {
    VLOG(9) << "MajorCompactCallback() for " << tb->m_meta.path()
        << ", status: " << StatusCodeToString(tb->m_meta.compact_status())
        << ", retry: " << retry;
    {
        MutexLock lock(&tb->m_mutex);
        if (tb->m_meta.compact_status() == kTableCompacted) {
            return;
        }
    }

    if (failed || response->status() != kTabletNodeOk
        || response->compact_status() == kTableOnCompact
        || response->compact_size() == 0) {
        LOG(ERROR) << "fail to major compact for " << tb->m_meta.path()
            << ", rpc status: " << StatusCodeToString(response->status())
            << ", compact status: " << StatusCodeToString(response->compact_status());
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            delete request;
            delete response;
        } else {
            int64_t wait_time = FLAGS_tera_tabletnode_connect_retry_period
                * (FLAGS_tera_master_impl_retry_times - retry);
            ThisThread::Sleep(wait_time);
            tabletnode::TabletNodeClient node_client(tb->m_meta.server_addr());
            Closure<void, CompactTabletRequest*, CompactTabletResponse*, bool, int>* done =
                NewClosure(this, &TabletManager::MajorCompactCallback, tb, retry - 1);
            node_client.CompactTablet(request, response, done);
        }
        return;
    }
    delete request;
    delete response;

    MutexLock lock(&tb->m_mutex);
    tb->m_meta.set_compact_status(kTableCompacted);
    VLOG(5) << "compact success: " << tb->m_meta.path();
}

double TabletManager::OfflineTabletRatio() {
    uint32_t offline_tablet_count = 0, tablet_count = 0;
    m_mutex.Lock();
    TableList::iterator it = m_all_tables.begin();
    for (; it != m_all_tables.end(); ++it) {
        Table& table = *it->second;
        table.m_mutex.Lock();
        Table::TabletList::iterator it2 = table.m_tablets_list.begin();
        for (; it2 != table.m_tablets_list.end(); ++it2) {
            TabletPtr tablet = it2->second;
            if (tablet->GetStatus() == kTableOffLine) {
                offline_tablet_count++;
            }
            tablet_count++;
        }
        table.m_mutex.Unlock();
    }
    m_mutex.Unlock();

    if (tablet_count == 0) {
        return 0;
    }
    return (double)offline_tablet_count / tablet_count;
}

} // namespace master
} // namespace tera
