// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#ifndef TERA_MASTER_GC_STRATEGY_H_
#define TERA_MASTER_GC_STRATEGY_H_

#include "master/tablet_manager.h"
#include "proto/tabletnode_client.h"
#include "types.h"
#include "utils/counter.h"

namespace tera {
namespace master {

class TabletManager;
class TabletNodeManager;

class GcStrategy {
public:
    virtual ~GcStrategy() {}

    // get file system image before query
    // return true if need to triger gc (gc query & post query)
    virtual bool PreQuery () = 0;

    // process gc query results
    virtual void ProcessQueryCallbackForGc(QueryResponse* response) = 0;

    // delete useless files
    virtual void PostQuery () = 0;

    // clear memory when table is deleted
    virtual void Clear(std::string tablename) = 0;
};

class BatchGcStrategy : public GcStrategy {
public:
    BatchGcStrategy (boost::shared_ptr<TabletManager> tablet_manager);
    virtual ~BatchGcStrategy() {}

    // get file system image before query
    virtual bool PreQuery ();

    // compute dead files
    virtual void ProcessQueryCallbackForGc(QueryResponse* response);

    // delete dead files
    virtual void PostQuery ();

    virtual void Clear(std::string tablename);

private:
    void CollectDeadTabletsFiles();
    void CollectSingleDeadTablet(const std::string& tablename, uint64_t tabletnum);
    void DeleteObsoleteFiles();

    boost::shared_ptr<TabletManager> m_tablet_manager;

    // tabletnode garbage clean
    // first: live tablet, second: dead tablet
    typedef std::pair<std::set<uint64_t>, std::set<uint64_t> > GcTabletSet;
    typedef std::vector<std::set<uint64_t> > GcFileSet;
    mutable Mutex m_gc_mutex;
    std::map<std::string, GcTabletSet> m_gc_tablets;
    std::map<std::string, GcFileSet> m_gc_live_files;
    int64_t m_file_total_num;
    int64_t m_file_delete_num;
    tera::Counter m_list_count;
};

class IncrementalGcStrategy : public GcStrategy{
public:
    IncrementalGcStrategy(boost::shared_ptr<TabletManager> tablet_manager);
    virtual ~IncrementalGcStrategy() {}

    // get dead tablets
    virtual bool PreQuery ();

    // gather live files
    virtual void ProcessQueryCallbackForGc(QueryResponse* response);

    // delete dead files
    virtual void PostQuery ();

    // clear memory when table is deleted
    virtual void Clear(std::string tablename);

private:
    void DEBUG_print_files(bool print_dead);
    void CollectSingleDeadTablet(const std::string& tablename, uint64_t tabletnum);
    void DeleteTableFiles(const std::string& table_name);

    struct LgFileSet {
        std::set<uint64_t> m_storage_files;
        std::set<uint64_t> m_live_files;
    };

    struct TabletFileSet {
        int64_t m_dead_time;
        int64_t m_ready_time;
        std::map<int64_t, LgFileSet> m_files; // lg_no -> files
        TabletFileSet() {
            m_dead_time = std::numeric_limits<int64_t>::max();
            m_ready_time = 0;
        };
        TabletFileSet(int64_t dead_time, int64_t ready_time) {
            m_dead_time = dead_time;
            m_ready_time = ready_time;
        }
    };

    typedef std::map<int64_t, TabletFileSet> TabletFiles;  // tablet_number -> files
    typedef std::map<std::string, TabletFiles> TableFiles; // table_name -> files
    mutable Mutex m_gc_mutex;
    boost::shared_ptr<TabletManager> m_tablet_manager;
    int64_t m_last_gc_time;
    TableFiles m_dead_tablet_files;
    TableFiles m_live_tablet_files;
    int64_t m_max_ts;
    tera::Counter m_list_count;
};

} // namespace master
} // namespace tera

#endif  // TERA_MASTER_GC_STRATEGY_H_
