// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLET_WRITER_H_
#define TERA_TABLETNODE_TABLET_WRITER_H_

#include <functional>
#include <unordered_set>
#include <set>

#include "common/event.h"
#include "common/mutex.h"
#include "common/thread.h"

#include "proto/status_code.pb.h"
#include "proto/tabletnode_rpc.pb.h"

namespace leveldb {
class WriteBatch;
}

namespace tera {
namespace io {

class TabletIO;

class TabletWriter {
public:
    typedef std::function<void (std::vector<const RowMutationSequence*>*, \
                                std::vector<StatusCode>*)> WriteCallback;
    
    typedef std::vector<bool> IgnoreCellFlags;

    struct WriteTask {
        WriteTask():start_time(get_micros()) {}
        std::vector<const RowMutationSequence*>* row_mutation_vec;
        std::vector<StatusCode>* status_vec;
        std::vector<IgnoreCellFlags> ignore_row_vec;
        WriteCallback callback;
        int64_t start_time;
    };

    typedef std::vector<WriteTask> WriteTaskBuffer;

public:
    TabletWriter(TabletIO* tablet_io);
    ~TabletWriter();
    bool Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
               std::vector<StatusCode>* status_vec, bool is_instant,
               WriteCallback callback, StatusCode* status = NULL);
    /// 初略计算一个request的数据大小
    static uint64_t CountRequestSize(std::vector<const RowMutationSequence*>& row_mutation_vec,
                                     bool kv_only);
    void Start();
    void Stop();
    bool IsBusy();

private:
    void DoWork();
    bool SwapActiveBuffer(bool force);
    /// 把一个request打到一个leveldbbatch里去, request是原子的, batch也是, so ..
    void BatchRequest(WriteTaskBuffer* task_buffer,
                      leveldb::WriteBatch* batch);
    bool CheckSingleRowTxnConflict(const RowMutationSequence& row_mu,
                                   std::set<std::string>* commit_row_key_set,
                                   StatusCode* status);
    // mark conflict of PutIfAbsent
    void MarkPutIfAbsentConflict(const RowMutationSequence& row_mu,
                                 IgnoreCellFlags* ignore_cell_flags,
                                 std::unordered_set<std::string>* not_exist_cell_set);

    bool CheckIllegalRowArg(const RowMutationSequence& row_mu,
                            const std::set<std::string>& cf_set,
                            StatusCode* status);
    void CheckRows(WriteTaskBuffer* task_buffer);
    /// 任务完成, 执行回调
    void FinishTask(WriteTaskBuffer* task_buffer, StatusCode status);
    /// 将buffer刷到磁盘(leveldb), 并sync
    StatusCode FlushToDiskBatch(WriteTaskBuffer* task_buffer);

private:
    TabletIO* tablet_;

    mutable Mutex task_mutex_;
    mutable Mutex status_mutex_;
    AutoResetEvent write_event_;       ///< 有数据可写
    AutoResetEvent worker_done_event_; ///< worker退出

    bool stopped_;
    common::Thread thread_;

    WriteTaskBuffer* active_buffer_;   ///< 前台buffer,接收写请求
    WriteTaskBuffer* sealed_buffer_;   ///< 后台buffer,等待刷到磁盘
    int64_t sync_timestamp_;

    bool active_buffer_instant_;      ///< active_buffer包含instant请求
    uint64_t active_buffer_size_;      ///< active_buffer的数据大小
    bool tablet_busy_;                 ///< tablet处于忙碌状态
};

} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLET_WRITER_H_
