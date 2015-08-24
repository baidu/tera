// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/gc_strategy.h"

#include <gflags/gflags.h>

#include "db/filename.h"
#include "io/utils_leveldb.h"


DECLARE_string(tera_tabletnode_path_prefix);
DECLARE_string(tera_master_meta_table_name);

namespace tera {
namespace master {

BatchGcStrategy::BatchGcStrategy (boost::shared_ptr<TabletManager> tablet_manager)
	  : m_tablet_manager(tablet_manager) {}

bool BatchGcStrategy::PreQuery () {
    int64_t start_ts = get_micros();
    m_gc_live_files.clear();
    m_gc_tablets.clear();

    std::vector<TablePtr> tables;

    m_tablet_manager->ShowTable(&tables, NULL);
    for (size_t i = 0; i < tables.size(); ++i) {
        if (tables[i]->GetStatus() != kTableEnable ||
            tables[i]->GetTableName() == FLAGS_tera_master_meta_table_name) {
            // table not ready and skip metatable
            continue;
        }
        GcTabletSet& tablet_set = m_gc_tablets[tables[i]->GetTableName()];
        if (!tables[i]->GetTabletsForGc(&tablet_set.first, &tablet_set.second)) {
            // tablet not ready or there is none dead tablets
            m_gc_tablets.erase(tables[i]->GetTableName());
            continue;
        }
    }

    CollectDeadTabletsFiles();

    LOG(INFO) << "[gc] DoTabletNodeGc: collect all files, cost: "
        << (get_micros() - start_ts) / 1000 << "ms.";

    if (m_gc_tablets.size() == 0) {
        LOG(INFO) << "[gc] do not need gc this time.";
        return false;
    }
    return true;
}

void BatchGcStrategy::PostQuery () {
    bool is_success = true;
    std::map<std::string, GcTabletSet>::iterator it = m_gc_tablets.begin();
    for (; it != m_gc_tablets.end(); ++it) {
        if (it->second.first.size() != 0) {
            VLOG(10) << "[gc] there are tablet not ready: " << it->first;
            is_success = false;
            break;
        }
    }
    if (!is_success) {
        LOG(INFO) << "[gc] gc not success, try next time.";
        return;
    }

    int64_t start_ts = get_micros();
    DeleteObsoleteFiles();
    LOG(INFO) << "[gc] DoTabletNodeGcPhase2 finished, cost:"
        << (get_micros() - start_ts) / 1000 << "ms.";
}

void BatchGcStrategy::ProcessQueryCallbackForGc(QueryResponse* response) {
    MutexLock lock(&m_gc_mutex);
    std::set<std::string> gc_table_set;
    for (int i = 0; i < response->inh_live_files_size(); ++i) {
        const InheritedLiveFiles& live = response->inh_live_files(i);
        gc_table_set.insert(live.table_name());
    }

    for (int i = 0; i < response->tabletmeta_list().meta_size(); ++i) {
        const TabletMeta& meta = response->tabletmeta_list().meta(i);
        VLOG(10) << "[gc] try erase live tablet: " << meta.path()
            << ", tablename: " << meta.table_name();
        if (m_gc_tablets.find(meta.table_name()) != m_gc_tablets.end() &&
            gc_table_set.find(meta.table_name()) != gc_table_set.end()) {
            // erase live tablet
            VLOG(10) << "[gc] erase live tablet: " << meta.path();
            uint64_t tabletnum = leveldb::GetTabletNumFromPath(meta.path());
            m_gc_tablets[meta.table_name()].first.erase(tabletnum);
        }
    }

    // erase inherited live files
    for (int i = 0; i < response->inh_live_files_size(); ++i) {
        const InheritedLiveFiles& live = response->inh_live_files(i);
        if (m_gc_live_files.find(live.table_name()) == m_gc_live_files.end()) {
            VLOG(10) << "[gc] table: " << live.table_name() << " skip gc.";
            continue;
        }
        GcFileSet& file_set = m_gc_live_files[live.table_name()];
        int lg_num = live.lg_live_files_size();
        CHECK(static_cast<size_t>(lg_num) == file_set.size())
            << "lg_num should eq " << file_set.size();
        for (int lg = 0; lg < lg_num; ++lg) {
            const LgInheritedLiveFiles& lg_live_files = live.lg_live_files(lg);
            for (int f = 0; f < lg_live_files.file_number_size(); ++f) {
                std::string file_path = leveldb::BuildTableFilePath(
                    live.table_name(), lg, lg_live_files.file_number(f));
                VLOG(10) << "[gc] " << " erase live file: " << file_path;
                file_set[lg].erase(lg_live_files.file_number(f));
            }
        }
    }
}

void BatchGcStrategy::CollectDeadTabletsFiles() {
    std::map<std::string, GcTabletSet>::iterator table_it = m_gc_tablets.begin();
    for (; table_it != m_gc_tablets.end(); ++table_it) {
        std::set<uint64_t>& dead_tablets = table_it->second.second;
        std::set<uint64_t>::iterator tablet_it = dead_tablets.begin();
        for (; tablet_it != dead_tablets.end(); ++tablet_it) {
            CollectSingleDeadTablet(table_it->first, *tablet_it);
        }
    }
}

void BatchGcStrategy::CollectSingleDeadTablet(const std::string& tablename, uint64_t tabletnum) {
    std::string tablepath = FLAGS_tera_tabletnode_path_prefix + tablename;
    std::string tablet_path = leveldb::GetTabletPathFromNum(tablepath, tabletnum);
    leveldb::Env* env = io::LeveldbBaseEnv();
    std::vector<std::string> children;
    env->GetChildren(tablet_path, &children);
    if (children.size() == 0) {
        LOG(INFO) << "[gc] delete empty tablet dir: " << tablet_path;
        env->DeleteDir(tablet_path);
        return;
    }
    for (size_t lg = 0; lg < children.size(); ++lg) {
        std::string lg_path = tablet_path + "/" + children[lg];
        leveldb::FileType type = leveldb::kUnknown;
        uint64_t number = 0;
        if (ParseFileName(children[lg], &number, &type)) {
            LOG(INFO) << "[gc] delete log file: " << lg_path;
            env->DeleteFile(lg_path);
            continue;
        }

        leveldb::Slice rest(children[lg]);
        uint64_t lg_num = 0;
        if (!leveldb::ConsumeDecimalNumber(&rest, &lg_num)) {
            LOG(INFO) << "[gc] skip unknown dir: " << lg_path;
            continue;
        }

        std::vector<std::string> files;
        env->GetChildren(lg_path, &files);
        if (files.size() == 0) {
            LOG(INFO) << "[gc] delete empty lg dir: " << lg_path;
            env->DeleteDir(lg_path);
            continue;
        }
        for (size_t f = 0; f < files.size(); ++f) {
            std::string file_path = lg_path + "/" + files[f];
            type = leveldb::kUnknown;
            number = 0;
            if (!ParseFileName(files[f], &number, &type) ||
                type != leveldb::kTableFile) {
                // only keep sst, delete rest files
                LOG(INFO) << "[gc] delete obsolete file: " << file_path;
                env->DeleteFile(file_path);
                continue;
            }

            uint64_t full_number = leveldb::BuildFullFileNumber(lg_path, number);
            GcFileSet& file_set = m_gc_live_files[tablename];
            if (file_set.size() == 0) {
                TablePtr table;
                m_tablet_manager->FindTable(tablename, &table);
                file_set.resize(table->GetSchema().locality_groups_size());
                VLOG(10) << "[gc] resize : " << tablename
                    << " fileset lg size: " << file_set.size();
            }
            VLOG(10) << "[gc] " << tablename << " insert live file: " << file_path;
            CHECK(lg_num < file_set.size());
            file_set[lg_num].insert(full_number);
        }
    }
}

void BatchGcStrategy::DeleteObsoleteFiles() {
    leveldb::Env* env = io::LeveldbBaseEnv();
    std::map<std::string, GcFileSet>::iterator table_it = m_gc_live_files.begin();
    for (; table_it != m_gc_live_files.end(); ++table_it) {
        std::string tablepath = FLAGS_tera_tabletnode_path_prefix + table_it->first;
        GcFileSet& file_set = table_it->second;
        for (size_t lg = 0; lg < file_set.size(); ++lg) {
            std::set<uint64_t>::iterator it = file_set[lg].begin();
            for (; it != file_set[lg].end(); ++it) {
                std::string file_path = leveldb::BuildTableFilePath(tablepath, lg, *it);
                LOG(INFO) << "[gc] delete trash table file: " << file_path;
                env->DeleteFile(file_path);
            }
        }
    }
}

} // namespace master
} // namespace tera
