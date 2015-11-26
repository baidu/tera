// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: An Qin (qinan@baidu.com)

#include "io/stream_scan.h"

#include "common/this_thread.h"
#include "common/thread_pool.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "utils/atomic.h"

DECLARE_int64(tera_io_scan_stream_task_max_num);
DECLARE_int64(tera_io_scan_stream_task_pending_time);

DECLARE_int32(tera_io_retry_period);

namespace tera {
namespace io {

StreamScan::StreamScan()
    : m_shutdown(false), m_is_completed(false), m_status(kTabletNodeOk),
      m_ref_count(0) {}

StreamScan::~StreamScan() {
    MutexLock lock(&m_mutex);
    m_shutdown = true;
    m_push_event.Set();
    DropTask();
}

bool StreamScan::PushTask(uint64_t sequence_id, ScanTabletResponse* response,
                          google::protobuf::Closure* done) {
    // TODO: sequece id is used to combine re-try rpc
    MutexLock lock(&m_mutex);
    if (m_task_queue.size() >= static_cast<uint64_t>(FLAGS_tera_io_scan_stream_task_max_num)) {
        return false;
    }
    Task* task = new Task(response, done);
    m_task_queue.push(task);
    m_push_event.Set();

    VLOG(10) << "push task, queue size: " << m_task_queue.size();
    return true;
}

void StreamScan::DropTask() {
    while (!m_task_queue.empty()) {
        Task* task = m_task_queue.front();
        task->first->set_complete(m_is_completed);
        task->first->set_status(m_status);
        task->second->Run();
        m_task_queue.pop();
        delete task;
    }
}

bool StreamScan::PushData(uint64_t data_id, const RowResult& result) {
    VLOG(10) << "begin push data, data id: " << data_id;
    Task* task = NULL;
    while (task == NULL) {
        if (m_shutdown) {
            return false;
        }
        {
            MutexLock lock(&m_mutex);
            if (m_task_queue.size() != 0) {
                task = m_task_queue.front();
                if (task) {
                    m_task_queue.pop();
                }
            }
        }
        if (!task && !m_shutdown) {
            LOG(INFO) << "not task, waiting ...";
            if (!m_push_event.TimeWait(FLAGS_tera_io_scan_stream_task_pending_time * 1000)) {
                LOG(INFO) << "timeout, clean the session";
                return false;
            }
        }
    }

    VLOG(10) << "push data, sequence id: " << task->first->sequence_id()
        << ", data id: " << data_id;
    task->first->mutable_results()->CopyFrom(result);
    task->first->set_results_id(data_id);
    task->first->set_complete(m_is_completed);
    task->first->set_status(m_status);
    task->second->Run();

    delete task;
    return true;
}

void StreamScan::SetCompleted(bool completed) {
    m_is_completed = completed;
}

void StreamScan::SetStatusCode(StatusCode status) {
    m_status = status;
}

int32_t StreamScan::AddRef() {
    atomic_inc(&m_ref_count);
    return m_ref_count;
}

int32_t StreamScan::DecRef() {
    atomic_dec(&m_ref_count);
    return m_ref_count;
}

int32_t StreamScan::GetRef() {
    return m_ref_count;
}

StreamScanManager::StreamScanManager() {}

StreamScanManager::~StreamScanManager() {}

void StreamScanManager::PushTask(const ScanTabletRequest* request,
                                 ScanTabletResponse* response,
                                 google::protobuf::Closure* done,
                                 bool* is_first) {
    VLOG(10) << "push task for session id: " << request->session_id()
        << ", sequence id: " << request->sequence_id();
    response->set_results_id(std::numeric_limits<unsigned long>::max());
    StreamScan* scan = NULL;
    {
        MutexLock lock(&m_mutex);
        std::map<uint64_t, StreamScan*>::iterator it =
            m_session_list.find(request->session_id());
        if (it == m_session_list.end()) {
            if (request->part_of_session() == false) {
                scan = m_session_list[request->session_id()] = new StreamScan;
                *is_first = true;
                VLOG(6) << "new session id: " << request->session_id();
            } else {
                DropInvalidTask(request, response, done);
                VLOG(10) << "drop invalid rpc pack";
                return;
            }
        } else {
            scan = it->second;
        }
    }
    CHECK(scan != NULL);
    scan->AddRef();
    if (!scan->PushTask(request->sequence_id(), response, done)) {
        // too busy
        LOG(WARNING) << "session: " << request->sequence_id() << " is too busy";
        response->set_status(kTabletNodeIsBusy);
        done->Run();
    }
    scan->DecRef();
}

StreamScan* StreamScanManager::GetStream(uint64_t session_id) {
    MutexLock lock(&m_mutex);
    std::map<uint64_t, StreamScan*>::iterator it =
        m_session_list.find(session_id);
    if (it != m_session_list.end()) {
        it->second->AddRef();
        return it->second;
    }
    return NULL;
}

void StreamScanManager::RemoveSession(uint64_t session_id) {
    VLOG(6) << "remove session id: " << session_id;
    MutexLock lock(&m_mutex);
    std::map<uint64_t, StreamScan*>::iterator it =
        m_session_list.find(session_id);
    if (it == m_session_list.end()) {
        return;
    }

    uint32_t retry = 0;
    while (it->second->GetRef() > 1) {
        LOG(WARNING) << "scan stream is busy, ref: " << it->second->GetRef()
            << ", try again destruct: " << retry++;
        ThisThread::Sleep(FLAGS_tera_io_retry_period);
    }
    delete it->second;
    m_session_list.erase(it);
}

void StreamScanManager::DropInvalidTask(const ScanTabletRequest* request,
                                        ScanTabletResponse* response,
                                        google::protobuf::Closure* done) {
    response->set_complete(false);
    response->set_status(kTabletNodeOk);
    done->Run();
}

} // namespace io
}//  namespace tera

