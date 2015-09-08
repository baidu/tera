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

//  @TestSplitTablet:     test whether split finish
//
//  @return:              return true if split success
bool TabletManager::TestSplitTablet(const std::string& table_name,
                                    const std::string& start_key,
                                    const std::string& mid_key,
                                    const std::string& end_key) {
    TabletRange parent(table_name, start_key, end_key);
    TabletRange lchild(table_name, start_key, mid_key);
    TabletRange rchild(table_name, mid_key, end_key);
    
    MutexLock lock(&m_mutex);

    std::map<TabletRange, io::TabletIO *>::iterator it; 
    it = m_tablet_list.find(rchild);
    if (it != m_tablet_list.end()) {
        // get right child
        LOG(INFO) << __func__ << ", step into phase 2, " << table_name << ", start "
            << start_key << ", mid " << mid_key << ", end " << end_key;
        it = m_tablet_list.find(lchild);
        CHECK(it != m_tablet_list.end());
        return true;
    }
    VLOG(20) << __func__ << ", split not finish, " << table_name << ", start "
        << start_key << ", mid " << mid_key << ", end " << end_key;
    return false; 
}

// @SplitTabletIO:          delete parent tabletio, add left && right child
bool TabletManager::SplitTabletIO(const std::string& table_name,
                                  const std::string& key_start,
                                  const std::string& mid_key,
                                  const std::string& key_end,
                                  io::TabletIO *parent_tabletIO,
                                  io::TabletIO *left_tabletIO,
                                  io::TabletIO *right_tabletIO)
{
    TabletRange parent(table_name, key_start, key_end);
    TabletRange lchild(table_name, key_start, mid_key);
    TabletRange rchild(table_name, mid_key, key_end);
    
    MutexLock lock(&m_mutex);
    std::map<TabletRange, io::TabletIO *>::iterator it;
    it = m_tablet_list.find(parent);
    if (it != m_tablet_list.end()) {
        m_tablet_list.erase(it);
        parent_tabletIO->DecRef();
        
        LOG(INFO) << __func__ << ": " << table_name << ", start " << key_start
            << ", mid_key " << mid_key << ", end " << key_end;

        m_tablet_list.insert(std::pair<TabletRange, io::TabletIO*>(lchild, left_tabletIO));
        m_tablet_list.insert(std::pair<TabletRange, io::TabletIO*>(rchild, right_tabletIO));
        left_tabletIO->AddRef();
        right_tabletIO->AddRef();
        return true;
    }
    return false;
}

// @AddTablet:      add tabletIO into tabletmanager
bool TabletManager::AddTablet(const std::string& table_name,
                              const std::string& key_start,
                              const std::string& key_end,
                              io::TabletIO *tablet_io)
{
    MutexLock lock(&m_mutex);
    
    TabletRange range(table_name, key_start, key_end);
    std::map<TabletRange, io::TabletIO*>::iterator it =
        m_tablet_list.find(range);
    if (it != m_tablet_list.end()) {
        LOG(INFO) << "tablet exist: " << table_name << ", " << key_start
            << ", old endkey " << it->first.key_end << ", new endkey "
            << key_end;
        return false;
    }
    m_tablet_list.insert(std::pair<TabletRange, io::TabletIO*>(range, tablet_io));
    tablet_io->AddRef();
    return true;
}

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
    *tablet_io = m_tablet_list[tablet_range] = new io::TabletIO();
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
        tablet_meta->set_table_size(tablet_io->GetDataSize());
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
        StatusCode code = kTableOk;
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
