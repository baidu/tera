// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tabletnode/tablet_manager.h"

#include "common/file/file_path.h"
#include "common/thread_pool.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "io/io_utils.h"
#include "proto/proto_helper.h"

DECLARE_int32(tera_io_retry_period);
DECLARE_int32(tera_tabletnode_retry_period);

namespace tera {
namespace tabletnode {

TabletManager::TabletManager() {}

TabletManager::~TabletManager() {}

bool TabletManager::AddTablet(const std::string& table_name,
                              const std::string& table_path,
                              const std::string& key_start,
                              const std::string& key_end,
                              io::TabletIO** tablet_io,
                              StatusCode* status) {
    MutexLock lock(&m_mutex);

    TabletRange tablet_range(table_name, key_start, key_end);
    std::map<TabletRange, io::TabletIO*>::iterator it =
        m_tablet_list.find(tablet_range);
    if (it != m_tablet_list.end()) {
        LOG(ERROR) << "tablet exist: " << table_name << ", " << key_start;
        *tablet_io = it->second;
        (*tablet_io)->AddRef();
        SetStatusCode(kTableExist, status);
        return false;
    }
    *tablet_io = m_tablet_list[tablet_range] = new io::TabletIO(key_start, key_end);
    (*tablet_io)->AddRef();
    return true;
}

bool TabletManager::RemoveTablet(const std::string& table_name,
                                 const std::string& key_start,
                                 const std::string& key_end,
                                 StatusCode* status) {
    io::TabletIO* tablet_io = NULL;
    {
        MutexLock lock(&m_mutex);
        std::map<TabletRange, io::TabletIO*>::iterator it =
            m_tablet_list.lower_bound(TabletRange(table_name, key_start, key_end));
        if (it == m_tablet_list.end() ||
            it->first.table_name != table_name ||
            it->first.key_start != key_start ||
            it->first.key_end != key_end) {
            LOG(ERROR) << "tablet not exist: " << table_name << " ["
                << key_start << ", " << key_end << "]";
            SetStatusCode(kKeyNotInRange, status);
            return false;
        }
        tablet_io = it->second;
        m_tablet_list.erase(it);
    }
    tablet_io->DecRef();
    return true;
}

io::TabletIO* TabletManager::GetTablet(const std::string& table_name,
                                       const std::string& key_start,
                                       const std::string& key_end,
                                       StatusCode* status) {
    MutexLock lock(&m_mutex);
    std::map<TabletRange, io::TabletIO*>::iterator it =
        m_tablet_list.lower_bound(TabletRange(table_name, key_start, key_end));
    if (it == m_tablet_list.end() ||
        it->first.table_name != table_name ||
        it->first.key_start != key_start ||
        it->first.key_end != key_end) {
        SetStatusCode(kKeyNotInRange, status);
        return NULL;
    }

    it->second->AddRef();
    return it->second;
}

io::TabletIO* TabletManager::GetTablet(const std::string& table_name,
                                       const std::string& key,
                                       StatusCode* status) {
    MutexLock lock(&m_mutex);
    std::map<TabletRange, io::TabletIO*>::iterator it =
        m_tablet_list.upper_bound(TabletRange(table_name, key, key));
    if (it == m_tablet_list.begin()) {
        SetStatusCode(kKeyNotInRange, status);
        return NULL;
    } else {
        --it;
    }
    const TabletRange& tablet_range = it->first;
    if (tablet_range.table_name != table_name ||
        (tablet_range.key_end != "" && tablet_range.key_end <= key)) {
        SetStatusCode(kKeyNotInRange, status);
        return NULL;
    }

    it->second->AddRef();
    return it->second;
}

void TabletManager::GetAllTabletMeta(std::vector<TabletMeta*>* tablet_meta_list) {
    MutexLock lock(&m_mutex);
    std::map<TabletRange, io::TabletIO*>::iterator it;
    for (it = m_tablet_list.begin(); it != m_tablet_list.end(); ++it) {
        const TabletRange& range = it->first;
        io::TabletIO*& tablet_io = it->second;
        if (tablet_io->GetStatus() != io::TabletIO::kReady) {
            continue;
        }
        TabletMeta* tablet_meta = new TabletMeta;
        tablet_meta->set_table_name(range.table_name);
        tablet_meta->set_path(tablet_io->GetTablePath());
        tablet_meta->mutable_key_range()->set_key_start(range.key_start);
        tablet_meta->mutable_key_range()->set_key_end(range.key_end);
        tablet_meta->set_status(TabletStatus(tablet_io->GetStatus()));
        uint64_t size;
        tablet_io->GetDataSize(&size);
        tablet_meta->set_size(size);
        tablet_meta->set_compact_status(tablet_io->GetCompactStatus());
        tablet_meta_list->push_back(tablet_meta);
        std::vector<uint64_t> snapshots;
        tablet_io->ListSnapshot(&snapshots);
        for (uint32_t i = 0; i < snapshots.size(); ++i) {
            tablet_meta->add_snapshot_list(snapshots[i]);
        }
    }
}

void TabletManager::GetAllTablets(std::vector<io::TabletIO*>* tablet_list) {
    MutexLock lock(&m_mutex);
    std::map<TabletRange, io::TabletIO*>::iterator it;
    for (it = m_tablet_list.begin(); it != m_tablet_list.end(); ++it) {
        it->second->AddRef();
        tablet_list->push_back(it->second);
    }
}

bool TabletManager::RemoveAllTablets(bool force, StatusCode* status) {
    bool all_success = true;
    MutexLock lock(&m_mutex);
    std::map<TabletRange, io::TabletIO*>::iterator it;
    for (it = m_tablet_list.begin(); it != m_tablet_list.end();) {
        StatusCode code = kTabletNodeOk;
        if (it->second->Unload(&code) || force) {
            it->second->DecRef();
            m_tablet_list.erase(it++);
        } else {
            if (all_success) {
                SetStatusCode(code, status);
                all_success = false;
            }
            ++it;
        }
    }
    return all_success;
}

uint32_t TabletManager::Size() {
    MutexLock lock(&m_mutex);
    return m_tablet_list.size();
}

} // namespace tabletnode
} // namespace tera
