// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#ifndef STORAGE_LEVELDB_INCLUDE_GC_STRATEGY_H_
#define STORAGE_LEVELDB_INCLUDE_GC_STRATEGY_H_

#include "master/tablet_manager.h"
#include "proto/tabletnode_client.h"
#include "types.h"

namespace tera {
namespace master {

class TabletManager;
class TabletNodeManager;

class GcStrategy {
public:
	virtual ~GcStrategy() {}
	virtual bool PreQuery () = 0;
	virtual void ProcessQueryCallbackForGc(QueryResponse* response) = 0;
	virtual void PostQuery () = 0;
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
};

} // namespace master
} // namespace tera

#endif  // STORAGE_LEVELDB_INCLUDE_GC_STRATEGY_H_
