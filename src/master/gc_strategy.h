// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#ifndef TERA_MASTER_GC_STRATEGY_H_
#define TERA_MASTER_GC_STRATEGY_H_

#include "master/tablet_manager.h"
#include "proto/tabletnode_client.h"
#include "types.h"
#include "common/counter.h"

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

} // namespace master
} // namespace tera

#endif  // TERA_MASTER_GC_STRATEGY_H_
