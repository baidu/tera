// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "io/tablet_io.h"
#include "io/tablet_scanner.h"
#include "proto/status_code.pb.h"
#include <gflags/gflags.h>
#include <limits>
#include "util/coding.h"

DECLARE_int32(tera_tabletnode_scanner_cache_size);

namespace tera {
namespace io {

ScanContextManager::ScanContextManager() {
    cache_ = leveldb::NewLRUCache(FLAGS_tera_tabletnode_scanner_cache_size);
}
ScanContextManager::~ScanContextManager() {}

// access in m_lock context
static void LRUCacheDeleter(const ::leveldb::Slice& key, void* value) {
    ScanContext* context = reinterpret_cast<ScanContext*>(value);
    if (context->m_it) {
        delete context->m_it;
    }
    if (context->m_compact_strategy) {
        delete context->m_compact_strategy;
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
    char buf[sizeof(request->session_id())];
    ::leveldb::EncodeFixed64(buf, request->session_id());
    ::leveldb::Slice key(buf, sizeof(buf));
    handle = cache_->Lookup(key);
    if (handle) {
        // not first session rpc, no need init scan context
        context = reinterpret_cast<ScanContext*>(cache_->Value(handle));
        context->m_jobs.push(ScanJob(response, done));
        context->m_handles.push(handle); // refer item in cache
        if (context->m_jobs.size() > 1) {
            return NULL;
        }
        return context;
    }

    // case 1: if this session's first request not arrive, drop this one
    // case 2: client RPCtimeout resend
    if (request->part_of_session()) {
        VLOG(10) << "drop invalid request " << request->sequence_id();
        done->Run();
        return NULL;
    }

    // first rpc new scan context
    context = new ScanContext;
    context->m_session_id = request->session_id();
    context->m_tablet_io = tablet_io;

    context->m_it = NULL;
    context->m_compact_strategy = NULL;
    context->m_ret_code = kTabletNodeOk;
    context->m_result = NULL;
    context->m_data_idx = 0;
    context->m_complete = false;
    context->m_version_num = 1;

    handle = cache_->Insert(key, context, 1, &LRUCacheDeleter);
    context->m_jobs.push(ScanJob(response, done));
    context->m_handles.push(handle);  // refer item in cache
    // init context other param in TabletIO context
    return context;
}

// check event bit, then schedule context
bool ScanContextManager::ScheduleScanContext(ScanContext* context) {
    while (context->m_ret_code == kTabletNodeOk) {
        ScanTabletResponse* response;
        ::google::protobuf::Closure* done;
        {
            MutexLock l(&m_lock);
            response = context->m_jobs.front().first;
            done = context->m_jobs.front().second;
        }
        context->m_result = response->mutable_results();

        ((TabletIO*)(context->m_tablet_io))->ProcessScan(context);

        // reply to client
        response->set_complete(context->m_complete);
        response->set_status(context->m_ret_code);
        response->set_results_id(context->m_data_idx);
        (context->m_data_idx)++;
        context->m_result = NULL;
        done->Run();// TODO: try async return, time consume need test

        {
            MutexLock l(&m_lock);
            context->m_jobs.pop();
            ::leveldb::Cache::Handle* handle = context->m_handles.front();
            context->m_handles.pop();
            cache_->Release(handle); // unrefer cache item

            // complete or io error, return all the rest request to client
            if (context->m_complete || (context->m_ret_code != kTabletNodeOk)) {
                DeleteScanContext(context); // never use context
                return true;
            }
            if (context->m_jobs.size() == 0) {
                return true;
            }
        }
    }
    {
        MutexLock l(&m_lock);
        if (context->m_ret_code != kTabletNodeOk) {
            DeleteScanContext(context); // never use context
        }
    }
    return true;
}

// access in m_lock context
void ScanContextManager::DeleteScanContext(ScanContext* context) {
    uint32_t job_size = context->m_jobs.size();
    while (job_size) {
        ScanTabletResponse* response = context->m_jobs.front().first;
        ::google::protobuf::Closure* done = context->m_jobs.front().second;
        response->set_complete(context->m_complete);
        response->set_status(context->m_ret_code);
        done->Run();

        context->m_jobs.pop();
        ::leveldb::Cache::Handle* handle = context->m_handles.front();
        context->m_handles.pop();
        cache_->Release(handle); // unrefer cache item
        job_size--;
    }
}

// when tabletio unload, because scan_context->m_it has reference of version,
// so we shoud drop all cache it
void ScanContextManager::DestroyScanCache() {
    MutexLock l(&m_lock);
    delete cache_;
}

} // namespace io
}//  namespace tera

