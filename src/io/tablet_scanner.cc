// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include "io/tablet_io.h"
#include "io/tablet_scanner.h"
#include "proto/status_code.pb.h"
#include <gflags/gflags.h>
#include <limits>

DECLARE_int32(tera_tabletnode_scanner_cache_size);

namespace tera {
namespace io {

ScanContextManager::ScanContextManager()
    : m_seq_no(1) {
    // init scan manager
}

ScanContextManager::~ScanContextManager() {
    // drop all scan context

}

// access in m_lock context
ScanContext* ScanContextManager::GetContextFromCache(int64_t session_id) {
    ScanContext* context = NULL;
    std::map<int64_t, ScanContext*>::iterator it = m_context_cache.find(session_id);
    if (it != m_context_cache.end()) {
        context = it->second;
        // update lru
        std::map<uint64_t, int64_t>::iterator lru_it = m_context_lru.find(context->m_lru_seq_no);
        if (lru_it != m_context_lru.end()) {
            m_context_lru.erase(lru_it);
        }
        context->m_lru_seq_no = m_seq_no++;
        m_context_lru[context->m_lru_seq_no] = session_id;
    }
    return context;
}

// access in m_lock context
bool ScanContextManager::InsertContextToCache(ScanContext* context) {
    // insert context into cache
    int64_t session_id = context->m_session_id;
    std::map<int64_t, ScanContext*>::iterator it = m_context_cache.find(session_id);
    if (it != m_context_cache.end()) {
        LOG(WARNING) << "scan, session id " << session_id << " already in cache.";
        return false;
    }
    VLOG(10) << "ll-scan insert session_id " << session_id << " to cache";
    m_context_cache[session_id] = context;

    // update lru
    context->m_lru_seq_no = m_seq_no++;
    std::map<uint64_t, int64_t>::iterator lru_it = m_context_lru.find(context->m_lru_seq_no);
    if (lru_it != m_context_lru.end()) {
        m_context_lru.erase(lru_it);
    }
    m_context_lru[context->m_lru_seq_no] = session_id;

    // try evict oldest cache item
    EvictCache();
    return true;
}

// access in m_lock context
void ScanContextManager::DeleteContextFromCache(ScanContext* context) {
    if (context->m_ref > 1) {
        LOG(WARNING) << " ref leaks, session id " << context->m_session_id;
    }
    int64_t session_id = context->m_session_id;
    std::map<int64_t, ScanContext*>::iterator it = m_context_cache.find(session_id);
    if (it != m_context_cache.end()) {
        m_context_cache.erase(it);
    }
    std::map<uint64_t, int64_t>::iterator lru_it = m_context_lru.find(context->m_lru_seq_no);
    if (lru_it != m_context_lru.end()) {
        m_context_lru.erase(lru_it);
    }
    if (context->m_it) {
        delete context->m_it;
    }
    delete context;
}

// access in m_lock context
void ScanContextManager::EvictCache() {
    if (m_context_lru.size() > (uint32_t)FLAGS_tera_tabletnode_scanner_cache_size) {
        std::map<uint64_t, int64_t>::iterator lru_it = m_context_lru.begin();
        for (; lru_it != m_context_lru.end(); ++lru_it) {
            int64_t session_id = lru_it->second;
            std::map<int64_t, ScanContext*>::iterator it = m_context_cache.find(session_id);
            if (it != m_context_cache.end()) {
                ScanContext* context = it->second;
                if (context->m_ref == 1) {
                    // release idle context
                    context->m_ref = 0;
                    m_context_cache.erase(it);
                    m_context_lru.erase(lru_it);
                    if (context->m_it) {
                        delete context->m_it;
                    }
                    delete context;
		    VLOG(10) <<"ll-scan evict session_id " << session_id << " from cache";
                    break;
                }
                // continue try evict next cache item
            } else {
                LOG(WARNING) << "scan task in lru, but not in cache, session id " << session_id;
                m_context_lru.erase(lru_it);
                break;
            }
        }
    }
}

ScanContext* ScanContextManager::GetScanContext(TabletIO* tablet_io,
                                                const ScanTabletRequest* request,
                                                ScanTabletResponse* response,
                                                google::protobuf::Closure* done) {
    // init common param of response
    VLOG(10) << "push task for session id: " << request->session_id()
        << ", sequence id: " << request->sequence_id();
    response->set_results_id(std::numeric_limits<unsigned long>::max());
    response->set_complete(false);
    response->set_status(kTabletNodeOk);

    // search from cache
    MutexLock l(&m_lock);
    ScanContext* context = GetContextFromCache(request->session_id());
    if (context) {
        // not first session rpc, no need init scan context
        context->m_jobs.push(ScanJob(response, done));
        ++(context->m_ref);
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
    context->m_ref = 1;// ref counter for cache
    context->m_tablet_io = tablet_io;

    context->m_it = NULL;
    context->m_ret_code = kTabletNodeOk;
    context->m_result.Clear();
    context->m_data_idx = 0;
    context->m_complete = false;

    InsertContextToCache(context);
    context->m_jobs.push(ScanJob(response, done));
    ++(context->m_ref);
    // init context other param in TabletIO context
    return context;
}

// check event bit, then schedule context
bool ScanContextManager::ScheduleScanContext(ScanContext* context) {
    while (context->m_ret_code == kTabletNodeOk) {
        ((TabletIO*)(context->m_tablet_io))->ProcessScan(context);

        // reply to client
        ScanTabletResponse* response;
        ::google::protobuf::Closure* done;
        {
            MutexLock l(&m_lock);
            response = context->m_jobs.front().first;
            done = context->m_jobs.front().second;
        }
        response->mutable_results()->CopyFrom(context->m_result);
        response->set_complete(context->m_complete);
        response->set_status(context->m_ret_code);
        response->set_results_id(context->m_data_idx);
        (context->m_data_idx)++;
        context->m_result.Clear();
        done->Run();// TODO: try async return, time consume need test

        {
            MutexLock l(&m_lock);
            context->m_jobs.pop();
            (context->m_ref)--;
            // complete or io error, return all the rest request to client
            if (context->m_complete || (context->m_ret_code != kTabletNodeOk)) {
                DeleteScanContext(context);
                return true;
            }
            if (context->m_ref == 1) {
                return true;
            }
        }
    }
    {
        MutexLock l(&m_lock);
        if (context->m_ret_code != kTabletNodeOk) {
            DeleteScanContext(context);
        }
    }
    return true;
}

// when tabletio unload, because scan_context->m_it has reference of version,
// so we shoud drop all cache it
void ScanContextManager::DestroyScanCache() {
    MutexLock l(&m_lock);
    while (!m_context_cache.empty()) {
    	std::map<int64_t, ScanContext*>::iterator it = m_context_cache.begin();
   	ScanContext* context = it->second;
	DeleteScanContext(context);
    }
}

// access in m_lock context
void ScanContextManager::DeleteScanContext(ScanContext* context) {
    while (context->m_ref > 1) {
        ScanTabletResponse* response = context->m_jobs.front().first;
        ::google::protobuf::Closure* done = context->m_jobs.front().second;
        response->set_complete(context->m_complete);
        response->set_status(context->m_ret_code);
        done->Run();
        context->m_jobs.pop();
        (context->m_ref)--;
    }
    DeleteContextFromCache(context);
}

} // namespace io
}//  namespace tera

