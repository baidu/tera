// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLET_WRITER_H_
#define TERA_TABLETNODE_TABLET_WRITER_H_

#include "common/event.h"
#include "common/mutex.h"
#include "common/thread.h"

#include "proto/status_code.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "utils/counter.h"
#include "utils/rpc_timer_list.h"

namespace leveldb {
class WriteBatch;
}

namespace tera {
namespace io {

class TabletIO;

class TabletWriter {
public:
    struct WriteTask {
        const WriteTabletRequest* request;
        WriteTabletResponse* response;
        google::protobuf::Closure* done;
        const std::vector<int32_t>* index_list;
        Counter* done_counter;
        WriteRpcTimer* timer;

        WriteTask() :
            request(NULL), response(NULL), done(NULL), index_list(NULL),
            done_counter(NULL), timer(NULL) {}
    };

    typedef std::vector<WriteTask> WriteTaskBuffer;

public:
    TabletWriter(TabletIO* tablet_io);
    ~TabletWriter();
    void Write(const WriteTabletRequest* request,
               WriteTabletResponse* response,
               google::protobuf::Closure* done,
               const std::vector<int32_t>* index_list,
               Counter* done_counter, WriteRpcTimer* timer = NULL);
    /// 初略计算一个request的数据大小
    static uint64_t CountRequestSize(const WriteTabletRequest& request,
                                     const std::vector<int32_t>& index_list,
                                     bool kv_only);
    /// 把一个request打到一个leveldbbatch里去, request是原子的, batch也是, so ..
    bool BatchRequest(const WriteTabletRequest& request,
                      const std::vector<int32_t>& index_list,
                      leveldb::WriteBatch* batch,
                      bool kv_only);
    void Start();
    void Stop();

private:
    void DoWork();
    bool SwapActiveBuffer(bool force);
    /// 任务完成, 执行回调
    bool FinishTask(const WriteTask& task, StatusCode status);
    /// 将buffer刷到磁盘(leveldb), 并sync
    void FlushToDiskBatch(WriteTaskBuffer* task_buffer);

private:
    TabletIO* m_tablet;

    mutable Mutex m_task_mutex;
    mutable Mutex m_status_mutex;
    AutoResetEvent m_write_event;       ///< 有数据可写
    AutoResetEvent m_worker_done_event; ///< worker退出

    bool m_stopped;
    common::Thread m_thread;

    WriteTaskBuffer* m_active_buffer;   ///< 前台buffer,接收写请求
    WriteTaskBuffer* m_sealed_buffer;   ///< 后台buffer,等待刷到磁盘
    int64_t m_sync_timestamp;

    bool m_active_buffer_instant;      ///< active_buffer包含instant请求
    uint64_t m_active_buffer_size;      ///< active_buffer的数据大小
    bool m_tablet_busy;                 ///< tablet处于忙碌状态
};

} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLET_WRITER_H_
