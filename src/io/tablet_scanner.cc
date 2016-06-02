// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "io/tablet_scanner.h"

#include <limits>

#include <gflags/gflags.h>

#include "io/tablet_io.h"
#include "proto/status_code.pb.h"
#include "util/coding.h"

DECLARE_int32(tera_tabletnode_scanner_cache_size);

namespace tera {
namespace io {

ScanContextManager::ScanContextManager() {
    m_cache = leveldb::NewLRUCache(FLAGS_tera_tabletnode_scanner_cache_size);
}
// when tabletio unload, because scan_context->m_it has reference of version,
// so we shoud drop all cache it
ScanContextManager::~ScanContextManager() {
    MutexLock l(&m_lock);
    delete m_cache;
}

// access in m_lock context
static void LRUCacheDeleter(const ::leveldb::Slice& key, void* value) {
    ScanContext* context = reinterpret_cast<ScanContext*>(value);
    VLOG(10) << "evict from cache, " << context->session_id;
    CHECK(context->handle == NULL);
    if (context->it) {
        delete context->it;
    }
    if (context->compact_strategy) {
        delete context->compact_strategy;
    }
    delete context;
    return;
}

ScanContext* ScanContextManager::GetScanContext(TabletIO* tablet_io,
                                                const ScanTabletRequest* request,
                                                ScanTabletResponse* response,
                                                google::protobuf::Closure* done) {
    ScanContext* context = NULL;
    ::leveldb::Cache::Handle* handle = NULL;

    // init common param of response
    VLOG(10) << "push task for session id: " << request->session_id()
        << ", sequence id: " << request->sequence_id();
    response->set_results_id(std::numeric_limits<unsigned long>::max());
    response->set_complete(false);
    response->set_status(kTabletNodeOk);

    // search from cache
    MutexLock l(&m_lock);
    char buf[sizeof(int64_t)];
    ::leveldb::EncodeFixed64(buf, request->session_id());
    ::leveldb::Slice key(buf, sizeof(buf));
    handle = m_cache->Lookup(key);
    if (handle) {
        // not first session rpc, no need init scan context
        context = reinterpret_cast<ScanContext*>(m_cache->Value(handle));
        context->jobs.push(ScanJob(response, done));
        if (context->jobs.size() > 1) {
            m_cache->Release(handle);
            VLOG(10) << "push task into queue, " << request->session_id();
            return NULL;
        }
        CHECK(context->handle == NULL);
        context->handle = handle; // first one refer item in cache
        return context;
    }

    // case 1: if this session's first request not arrive, drop this one
    // case 2: client RPCtimeout resend
    if (request->part_of_session()) {
        VLOG(10) << "drop invalid request " << request->sequence_id() << ", session_id " << request->session_id();
        done->Run();
        return NULL;
    }

    // first rpc new scan context
    context = new ScanContext;
    context->session_id = request->session_id();
    context->tablet_io = tablet_io;

    context->it = NULL;
    context->compact_strategy = NULL;
    context->ret_code = kTabletNodeOk;
    context->result = NULL;
    context->data_idx = 0;
    context->complete = false;
    context->version_num = 1;

    handle = m_cache->Insert(key, context, 1, &LRUCacheDeleter);
    context->jobs.push(ScanJob(response, done));
    context->handle = handle;  // refer item in cache
    // init context other param in TabletIO context
    return context;
}

// check event bit, then schedule context
bool ScanContextManager::ScheduleScanContext(ScanContext* context) {
    while (context->ret_code == kTabletNodeOk) {
        ScanTabletResponse* response;
        ::google::protobuf::Closure* done;
        {
            MutexLock l(&m_lock);
            response = context->jobs.front().first;
            done = context->jobs.front().second;
        }
        context->result = response->mutable_results();

        context->tablet_io->ProcessScan(context);

        // reply to client
        response->set_complete(context->complete);
        response->set_status(context->ret_code);
        response->set_results_id(context->data_idx);
        (context->data_idx)++;
        context->result = NULL;
        done->Run();// TODO: try async return, time consume need test

        {
            MutexLock l(&m_lock);
            context->jobs.pop();

            // complete or io error, return all the rest request to client
            if (context->complete || (context->ret_code != kTabletNodeOk)) {
                DeleteScanContext(context); // never use context
                return true;
            }
            if (context->jobs.size() == 0) {
                ::leveldb::Cache::Handle* handle = context->handle;
                context->handle = NULL;
                m_cache->Release(handle); // unrefer cache item
                return true;
            }
        }
    }
    {
        MutexLock l(&m_lock);
        if (context->ret_code != kTabletNodeOk) {
            DeleteScanContext(context); // never use context
        }
    }
    return true;
}

// access in m_lock context
void ScanContextManager::DeleteScanContext(ScanContext* context) {
    uint32_t job_size = context->jobs.size();
    while (job_size) {
        ScanTabletResponse* response = context->jobs.front().first;
        ::google::protobuf::Closure* done = context->jobs.front().second;
        response->set_complete(context->complete);
        response->set_status(context->ret_code);
        done->Run();

        context->jobs.pop();
        job_size--;
    }

    int64_t session_id = context->session_id;
    VLOG(10) << "scan " << session_id << ", complete " << context->complete << ", ret " << StatusCode_Name(context->ret_code);
    ::leveldb::Cache::Handle* handle = context->handle;
    context->handle = NULL;
    m_cache->Release(handle); // unrefer cache item, no more use context!!!

    char buf[sizeof(int64_t)];
    ::leveldb::EncodeFixed64(buf, session_id);
    ::leveldb::Slice key(buf, sizeof(buf));
    m_cache->Erase(key);
}

} // namespace io
}//  namespace tera

