// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: An Qin (qinan@baidu.com)

#ifndef TERA_IO_STREAM_SCAN_H_
#define TERA_IO_STREAM_SCAN_H_

#include <map>
#include <queue>

#include "common/event.h"
#include "common/mutex.h"

#include "proto/table_meta.pb.h"
#include "proto/tabletnode_rpc.pb.h"

namespace tera {
namespace io {

class StreamScan {
public:
    StreamScan();
    ~StreamScan();

    bool PushTask(uint64_t sequence_id, ScanTabletResponse* request,
                  google::protobuf::Closure* done);
    void DropTask();

    bool PushData(uint64_t data_id, const RowResult& result);

    void SetCompleted(bool completed);

    void SetStatusCode(StatusCode status);

    int32_t AddRef();
    int32_t DecRef();
    int32_t GetRef();

public:
    typedef std::pair<ScanTabletResponse*, google::protobuf::Closure*> Task;

private:
    mutable Mutex m_mutex;
    AutoResetEvent m_push_event;
    bool m_shutdown;

    std::queue<Task*> m_task_queue;
    bool m_is_completed;
    StatusCode m_status;

    int32_t m_ref_count;
};

class StreamScanManager {
public:
    StreamScanManager();
    ~StreamScanManager();

    void PushTask(const ScanTabletRequest* request,
                  ScanTabletResponse* response,
                  google::protobuf::Closure* done,
                  bool* is_first);

    StreamScan* GetStream(uint64_t session_id);

    void RemoveSession(uint64_t session_id);

private:
    void DropInvalidTask(const ScanTabletRequest* request,
                         ScanTabletResponse* response,
                         google::protobuf::Closure* done);

private:
    mutable Mutex m_mutex;
    std::map<uint64_t, StreamScan*> m_session_list;
};

} // namespace io
} // namespace tera

#endif // TERA_IO_STREAM_SCAN_H
