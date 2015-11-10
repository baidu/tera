// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/gc_strategy.h"

#include <gflags/gflags.h>
#include <boost/lexical_cast.hpp>

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

IncrementalGcStrategy::IncrementalGcStrategy(boost::shared_ptr<TabletManager> tablet_manager)
    :   m_tablet_manager(tablet_manager),
        m_last_gc_time(std::numeric_limits<int64_t>::max()),
        m_max_ts(std::numeric_limits<int64_t>::max()) {}

bool IncrementalGcStrategy::PreQuery () {
    int64_t start_ts = get_micros();
    std::vector<TablePtr> tables;
    m_tablet_manager->ShowTable(&tables, NULL);

    for (size_t i = 0; i < tables.size(); ++i) {
        TabletFiles tablet_files;
        std::string table_name = tables[i]->GetTableName();
        if (table_name == FLAGS_tera_master_meta_table_name) continue;
        m_dead_tablet_files.insert(std::make_pair(table_name, tablet_files));
        m_live_tablet_files.insert(std::make_pair(table_name, tablet_files));

        std::set<uint64_t> live_tablets, dead_tablets;
        tables[i]->GetTabletsForGc(&live_tablets, &dead_tablets);
        std::set<uint64_t>::iterator it;
        // update dead tablets
        for (it = dead_tablets.begin(); it != dead_tablets.end(); ++it) {
            TabletFiles& temp_tablet_files = m_dead_tablet_files[table_name];
            TabletFileSet tablet_file_set(get_micros() / 1000000, 0);
            bool ret = temp_tablet_files.insert(std::make_pair(*it, tablet_file_set)).second;
            if (ret) {
                CollectSingleDeadTablet(table_name, *it);
            }
        }

        // erase newly dead tablets from live tablets
        for (TabletFiles::iterator it = m_live_tablet_files[table_name].begin();
             it != m_live_tablet_files[table_name].end(); ++it) {
            if (m_dead_tablet_files[table_name].find(static_cast<uint64_t>(it->first)) != m_dead_tablet_files[table_name].end()) {
                m_live_tablet_files[table_name].erase(it);
            }
        }

        // add new live tablets
        for (it = live_tablets.begin(); it != live_tablets.end(); ++it) {
            TabletFiles& temp_tablet_files = m_live_tablet_files[table_name];
            TabletFileSet tablet_file_set;
            temp_tablet_files.insert(std::make_pair(*it, tablet_file_set));
        }
    }
    LOG(INFO) << "[gc] Gather dead tablets, cost: " << (get_micros() - start_ts) / 1000 << "ms.";

    // do not need gc if there is no new dead tablet
    if (m_dead_tablet_files.size() == 0) {
        LOG(INFO) << "[gc] Do not need gc this time";
    }
    return m_dead_tablet_files.size() != 0;
}

void IncrementalGcStrategy::ProcessQueryCallbackForGc(QueryResponse* response) {
    LOG(INFO) << "[gc] ProcessQueryCallbackForGc";
    MutexLock lock(&m_gc_mutex);

    std::set<std::string> ready_tables;
    for (int table = 0; table < response->inh_live_files_size(); ++table) {
        ready_tables.insert(response->inh_live_files(table).table_name());
    }

    // update tablet ready time
    for (int i = 0; i < response->tabletmeta_list().meta_size(); ++i) {
        const TabletMeta& meta = response->tabletmeta_list().meta(i);
        std::string table_name = meta.table_name();
        if (table_name == FLAGS_tera_master_meta_table_name) continue;
        if (m_live_tablet_files.find(table_name) == m_live_tablet_files.end() ||
            ready_tables.find(table_name) == ready_tables.end()) {
            continue;
        }
        int64_t tablet_number = static_cast<int64_t>(leveldb::GetTabletNumFromPath(meta.path()));
        if (m_live_tablet_files[table_name].find(tablet_number) == m_live_tablet_files[table_name].end()) continue;
        m_live_tablet_files[table_name][tablet_number].m_ready_time = get_micros() / 1000000;
    }

    // insert live files
    for (int table = 0; table < response->inh_live_files_size(); ++table) {
        InheritedLiveFiles live_files = response->inh_live_files(table);
        std::string table_name = live_files.table_name();
        if (table_name == FLAGS_tera_master_meta_table_name) continue;
        if (m_live_tablet_files.find(table_name) == m_live_tablet_files.end()) continue;
        // collect live files
        TabletFiles temp_tablet_files;
        for (int lg = 0; lg < live_files.lg_live_files_size(); ++lg) {
            LgInheritedLiveFiles lg_live_files = live_files.lg_live_files(lg);
            uint32_t lg_no = lg_live_files.lg_no();
            for (int i = 0; i < lg_live_files.file_number_size(); ++i) {
                uint64_t tablet_number, file;
                uint64_t file_number = lg_live_files.file_number(i);
                leveldb::ParseFullFileNumber(file_number, &tablet_number, &file);
                TabletFileSet tablet_file_set;
                temp_tablet_files.insert(std::make_pair(tablet_number, tablet_file_set));
                TabletFileSet& temp_tablet_file_set = temp_tablet_files[tablet_number];
                LgFileSet lg_files;
                temp_tablet_file_set.m_files.insert(std::make_pair(lg_no, lg_files));
                temp_tablet_file_set.m_files[lg_no].m_live_files.insert(file_number);
            }
        }
        // update live files in dead tablets
        TabletFiles::iterator tablet_it = temp_tablet_files.begin();
        TabletFiles& dead_tablets = m_dead_tablet_files[table_name];
        for (; tablet_it != temp_tablet_files.end(); ++tablet_it) {
            uint64_t tablet_number = tablet_it->first;
            if (dead_tablets.find(tablet_number) == dead_tablets.end()) continue;
            std::map<int64_t, LgFileSet>& live_lg = (tablet_it->second).m_files;
            std::map<int64_t, LgFileSet>& dead_lg = dead_tablets[tablet_number].m_files;
            std::map<int64_t, LgFileSet>::iterator lg_it = live_lg.begin();
            for (; lg_it != live_lg.end(); ++lg_it) {
                uint32_t lg_no = lg_it->first;
                LgFileSet lg_file_set;
                dead_lg.insert(std::make_pair(lg_no, lg_file_set));
                dead_lg[lg_no].m_live_files.clear();
                dead_lg[lg_no].m_live_files = live_lg[lg_no].m_live_files;
            }
        }
    }
}

void IncrementalGcStrategy::PostQuery () {
    LOG(INFO) << "[gc] PostQuery";
    int64_t start_ts = get_micros();
    TableFiles::iterator table_it = m_dead_tablet_files.begin();
    for (; table_it != m_dead_tablet_files.end(); ++table_it) {
        DeleteTableFiles(table_it->first);
    }
    LOG(INFO) << "[gc] Delete useless sst, cost: " << (get_micros() - start_ts) / 1000 << "ms.";
}

void IncrementalGcStrategy::DeleteTableFiles(const std::string& table_name) {
    std::string table_path = FLAGS_tera_tabletnode_path_prefix + table_name;
    leveldb::Env* env = io::LeveldbBaseEnv();
    TabletFiles& dead_tablets = m_dead_tablet_files[table_name];
    TabletFiles& live_tablets = m_live_tablet_files[table_name];
    int64_t earliest_ready_time = m_max_ts;
    TabletFiles::iterator tablet_it = live_tablets.begin();
    for (; tablet_it != live_tablets.end(); ++tablet_it) {
        if (tablet_it->second.m_ready_time < earliest_ready_time) {
            earliest_ready_time = tablet_it->second.m_ready_time;
        }
    }

    std::set<int64_t> gc_tablets;
    for (tablet_it = dead_tablets.begin(); tablet_it != dead_tablets.end(); ++tablet_it) {
        if (tablet_it->second.m_dead_time <= earliest_ready_time) {
            gc_tablets.insert(tablet_it->first);
        }
    }

    std::set<int64_t>::iterator gc_it = gc_tablets.begin();
    for (; gc_it != gc_tablets.end();) {
        std::map<int64_t, LgFileSet>& lg_files = dead_tablets[*gc_it].m_files;
        std::map<int64_t, LgFileSet>::iterator lg_it = lg_files.begin();
        std::string tablet_path = leveldb::GetTabletPathFromNum(table_path, *gc_it);
        for (; lg_it != lg_files.end();) {
            LgFileSet& lg_file_set = lg_it->second;
            std::set<uint64_t>::iterator file_it = lg_file_set.m_storage_files.begin();
            for (; file_it != lg_file_set.m_storage_files.end();) {
                if (lg_file_set.m_live_files.find(*file_it) == lg_file_set.m_live_files.end()) {
                    std::string file_path =
                        leveldb::BuildTableFilePath(table_path, lg_it->first, *file_it);
                    env->DeleteFile(file_path);
                    lg_file_set.m_storage_files.erase(file_it);
                }
                file_it++;
            }
            if (lg_file_set.m_storage_files.size() == 0) {
                CHECK(lg_file_set.m_live_files.size() ==0) << "[gc] still has live files";
                std::string lg_str = boost::lexical_cast<std::string>(lg_it->first);
                std::string lg_path = tablet_path + "/" + lg_str;
                LOG(INFO) << "[gc] delete empty lg dir: " << lg_path;
                env->DeleteDir(lg_path);
                lg_files.erase(lg_it);
            }
            lg_it++;
        }
        if (lg_files.size() == 0) {
            LOG(INFO) << "[gc] delete empty tablet dir: " << tablet_path;
            env->DeleteDir(tablet_path);
            dead_tablets.erase(*gc_it);
        }
        gc_it++;
    }
}

void IncrementalGcStrategy::CollectSingleDeadTablet(const std::string& tablename, uint64_t tabletnum) {
    std::string tablepath = FLAGS_tera_tabletnode_path_prefix + tablename;
    std::string tablet_path = leveldb::GetTabletPathFromNum(tablepath, tabletnum);
    leveldb::Env* env = io::LeveldbBaseEnv();
    std::vector<std::string> children;
    env->GetChildren(tablet_path, &children);

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

        int64_t lg_no = boost::lexical_cast<int64_t>(children[lg]);
        std::map<int64_t, LgFileSet>& tablet_files = m_dead_tablet_files[tablename][tabletnum].m_files;
        LgFileSet lg_file_set;
        tablet_files.insert(std::make_pair(lg_no, lg_file_set));
        LgFileSet& temp_lg_files_set = tablet_files[lg_no];
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
            temp_lg_files_set.m_storage_files.insert(full_number);
        }
    }
}

} // namespace master
} // namespace tera
