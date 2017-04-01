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
    BatchGcStrategy (std::shared_ptr<TabletManager> tablet_manager);
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
    bool CollectSingleDeadTablet(const std::string& tablename, uint64_t tabletnum);
    void DeleteObsoleteFiles();

    std::shared_ptr<TabletManager> tablet_manager_;

    // tabletnode garbage clean
    // first: live tablet, second: dead tablet
    typedef std::pair<std::set<uint64_t>, std::set<uint64_t> > GcTabletSet;
    typedef std::vector<std::set<uint64_t> > GcFileSet;
    mutable Mutex gc_mutex_;
    std::map<std::string, GcTabletSet> gc_tablets_;
    std::map<std::string, GcFileSet> gc_live_files_;
    int64_t file_total_num_;
    int64_t file_delete_num_;
    tera::Counter list_count_;
};

class IncrementalGcStrategy : public GcStrategy{
public:
    IncrementalGcStrategy(std::shared_ptr<TabletManager> tablet_manager);
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
    bool CollectSingleDeadTablet(const std::string& tablename, uint64_t tabletnum);
    void DeleteTableFiles(const std::string& table_name);

    struct LgFileSet {
        std::set<uint64_t> storage_files_;
        std::set<uint64_t> live_files_;
    };

    struct TabletFileSet {
        int64_t dead_time_;
        int64_t ready_time_;
        std::map<int64_t, LgFileSet> files_; // lg_no -> files
        TabletFileSet() {
            dead_time_ = std::numeric_limits<int64_t>::max();
            ready_time_ = 0;
        };
        TabletFileSet(int64_t dead_time, int64_t ready_time) {
            dead_time_ = dead_time;
            ready_time_ = ready_time;
        }
    };

    typedef std::map<int64_t, TabletFileSet> TabletFiles;  // tablet_number -> files
    typedef std::map<std::string, TabletFiles> TableFiles; // table_name -> files
    mutable Mutex gc_mutex_;
    std::shared_ptr<TabletManager> tablet_manager_;
    int64_t last_gc_time_;
    TableFiles dead_tablet_files_;
    TableFiles live_tablet_files_;
    int64_t max_ts_;
    tera::Counter list_count_;
};

} // namespace master
} // namespace tera

#endif  // TERA_MASTER_GC_STRATEGY_H_
