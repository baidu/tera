// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/tabletnode/remote_tabletnode.h"

#include <boost/bind.hpp>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "tera/tabletnode/tabletnode_impl.h"
#include "tera/utils/counter.h"
#include "tera/utils/timer.h"

DECLARE_int32(tera_tabletnode_write_thread_num);
DECLARE_int32(tera_tabletnode_read_thread_num);
DECLARE_int32(tera_tabletnode_manual_compact_thread_num);
DECLARE_int32(tera_request_pending_limit);
DECLARE_int32(tera_scan_request_pending_limit);

extern tera::Counter read_pending_counter;
extern tera::Counter write_pending_counter;
extern tera::Counter scan_pending_counter;
extern tera::Counter compact_pending_counter;

namespace tera {
namespace tabletnode {

enum RpcType {
    RPC_READ = 1,
    RPC_SCAN = 2
};

struct ReadRpc : public RpcTask {
    google::protobuf::RpcController* controller;
    const ReadTabletRequest* request;
    ReadTabletResponse* response;
    google::protobuf::Closure* done;
    ReadRpcTimer* timer;
    int64_t start_micros;

    ReadRpc(google::protobuf::RpcController* ctrl,
            const ReadTabletRequest* req, ReadTabletResponse* resp,
            google::protobuf::Closure* done, ReadRpcTimer* timer,
            int64_t start_micros)
      : RpcTask(RPC_READ), controller(ctrl), request(req),
        response(resp), done(done), timer(timer),
        start_micros(start_micros) {}
};

struct ScanRpc : public RpcTask {
    google::protobuf::RpcController* controller;
    const ScanTabletRequest* request;
    ScanTabletResponse* response;
    google::protobuf::Closure* done;

    ScanRpc(google::protobuf::RpcController* ctrl,
            const ScanTabletRequest* req, ScanTabletResponse* resp,
            google::protobuf::Closure* done)
      : RpcTask(RPC_SCAN), controller(ctrl), request(req),
        response(resp), done(done) {}
};

RemoteTabletNode::RemoteTabletNode(TabletNodeImpl* tabletnode_impl)
    : m_tabletnode_impl(tabletnode_impl),
      m_write_thread_pool(new ThreadPool(FLAGS_tera_tabletnode_write_thread_num)),
      m_read_thread_pool(new ThreadPool(FLAGS_tera_tabletnode_read_thread_num)),
      m_compact_thread_pool(new ThreadPool(FLAGS_tera_tabletnode_manual_compact_thread_num)),
      m_rpc_schedule(new RpcSchedule(new FairSchedulePolicy)) {}

RemoteTabletNode::~RemoteTabletNode() {}

void RemoteTabletNode::LoadTablet(google::protobuf::RpcController* controller,
                                  const LoadTabletRequest* request,
                                  LoadTabletResponse* response,
                                  google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteTabletNode::DoLoadTablet, this, controller,
                   request, response, done);
    m_write_thread_pool->AddPriorityTask(callback);
}

void RemoteTabletNode::UnloadTablet(google::protobuf::RpcController* controller,
                                    const UnloadTabletRequest* request,
                                    UnloadTabletResponse* response,
                                    google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteTabletNode::DoUnloadTablet, this, controller,
                   request, response, done);
    m_write_thread_pool->AddPriorityTask(callback);
}

void RemoteTabletNode::ReadTablet(google::protobuf::RpcController* controller,
                                  const ReadTabletRequest* request,
                                  ReadTabletResponse* response,
                                  google::protobuf::Closure* done) {
    static uint32_t last_print = time(NULL);
    if (read_pending_counter.Get() > FLAGS_tera_request_pending_limit) {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kTabletNodeIsBusy);
        done->Run();
        uint32_t now_time = time(NULL);
        if (now_time > last_print) {
            LOG(WARNING) << "Too many pending read requests, return TabletNode Is Busy!";
            last_print = now_time;
        }
    } else {
        int32_t row_num = request->row_info_list_size();
        read_pending_counter.Add(row_num);
        int64_t start_micros = get_micros();
        ReadRpcTimer* timer = new ReadRpcTimer(request, response, done, start_micros);
        RpcTimerList::Instance()->Push(timer);

        ReadRpc* rpc = new ReadRpc(controller, request, response, done,
                                   timer, start_micros);
        m_rpc_schedule->EnqueueRpc(request->tablet_name(), rpc);
        m_read_thread_pool->AddTask(boost::bind(&RemoteTabletNode::DoScheduleRpc, this));
    }
}

void RemoteTabletNode::WriteTablet(google::protobuf::RpcController* controller,
                                   const WriteTabletRequest* request,
                                   WriteTabletResponse* response,
                                   google::protobuf::Closure* done) {
    static uint32_t last_print = time(NULL);
    if (write_pending_counter.Get() > FLAGS_tera_request_pending_limit) {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kTabletNodeIsBusy);
        done->Run();
        uint32_t now_time = time(NULL);
        if (now_time > last_print) {
            LOG(WARNING) << "Too many pending write requests, return TabletNode Is Busy!";
            last_print = now_time;
        }
    } else {
        int32_t row_num = request->row_list_size();
        write_pending_counter.Add(row_num);
        int64_t start_micros = get_micros();
        WriteRpcTimer* timer = new WriteRpcTimer(request, response, done, start_micros);
        RpcTimerList::Instance()->Push(timer);
        boost::function<void ()> callback =
            boost::bind(&RemoteTabletNode::DoWriteTablet, this,
                       controller, request, response, done, timer);
        m_write_thread_pool->AddTask(callback);
    }
}

void RemoteTabletNode::GetSnapshot(google::protobuf::RpcController* controller,
                                  const SnapshotRequest* request,
                                  SnapshotResponse* response,
                                  google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteTabletNode::DoGetSnapshot, this, controller,
                    request, response, done);
    m_write_thread_pool->AddPriorityTask(callback);
}

void RemoteTabletNode::ReleaseSnapshot(google::protobuf::RpcController* controller,
                                           const ReleaseSnapshotRequest* request,
                                           ReleaseSnapshotResponse* response,
                                           google::protobuf::Closure* done) {
    boost::function<void ()> callback =
    boost::bind(&RemoteTabletNode::DoReleaseSnapshot, this, controller,
               request, response, done);
    m_write_thread_pool->AddPriorityTask(callback);
}


void RemoteTabletNode::Query(google::protobuf::RpcController* controller,
                             const QueryRequest* request,
                             QueryResponse* response,
                             google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteTabletNode::DoQuery, this, controller,
                   request, response, done);
    m_write_thread_pool->AddPriorityTask(callback);
}


void RemoteTabletNode::ScanTablet(google::protobuf::RpcController* controller,
                                  const ScanTabletRequest* request,
                                  ScanTabletResponse* response,
                                  google::protobuf::Closure* done) {
    if (scan_pending_counter.Get() > FLAGS_tera_scan_request_pending_limit) {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kTabletNodeIsBusy);
        done->Run();
    } else {
        scan_pending_counter.Inc();
        ScanRpc* rpc = new ScanRpc(controller, request, response, done);
        m_rpc_schedule->EnqueueRpc(request->table_name(), rpc);
        m_read_thread_pool->AddTask(boost::bind(&RemoteTabletNode::DoScheduleRpc, this));
    }
}

void RemoteTabletNode::SplitTablet(google::protobuf::RpcController* controller,
                                   const SplitTabletRequest* request,
                                   SplitTabletResponse* response,
                                   google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteTabletNode::DoSplitTablet, this, controller,
                    request, response, done);
    m_write_thread_pool->AddPriorityTask(callback);
}

void RemoteTabletNode::MergeTablet(google::protobuf::RpcController* controller,
                                   const MergeTabletRequest* request,
                                   MergeTabletResponse* response,
                                   google::protobuf::Closure* done) {
    boost::function<void ()> callback =
        boost::bind(&RemoteTabletNode::DoMergeTablet, this, controller,
                   request, response, done);
    m_write_thread_pool->AddPriorityTask(callback);
}

void RemoteTabletNode::CompactTablet(google::protobuf::RpcController* controller,
                                   const CompactTabletRequest* request,
                                   CompactTabletResponse* response,
                                   google::protobuf::Closure* done) {
    compact_pending_counter.Inc();
    boost::function<void ()> callback =
        boost::bind(&RemoteTabletNode::DoCompactTablet, this, controller,
                   request, response, done);
    m_compact_thread_pool->AddTask(callback);
}

void RemoteTabletNode::DoLoadTablet(google::protobuf::RpcController* controller,
                                    const LoadTabletRequest* request,
                                    LoadTabletResponse* response,
                                    google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (LoadTablet) id: " << id;
    m_tabletnode_impl->LoadTablet(request, response, done);
    LOG(INFO) << "finish RPC (LoadTablet) id: " << id;
}

void RemoteTabletNode::DoUnloadTablet(google::protobuf::RpcController* controller,
                                      const UnloadTabletRequest* request,
                                      UnloadTabletResponse* response,
                                      google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (UnloadTablet) id: " << id;
    m_tabletnode_impl->UnloadTablet(request, response, done);
    LOG(INFO) << "finish RPC (UnloadTablet) id: " << id;
}

void RemoteTabletNode::DoReadTablet(google::protobuf::RpcController* controller,
                                    int64_t start_micros,
                                    const ReadTabletRequest* request,
                                    ReadTabletResponse* response,
                                    google::protobuf::Closure* done,
                                    ReadRpcTimer* timer) {
    VLOG(8) << "accept RPC (ReadTablet)";
    int32_t row_num = request->row_info_list_size();
    read_pending_counter.Sub(row_num);
    m_tabletnode_impl->ReadTablet(start_micros, request, response, done, timer);
    VLOG(8) << "finish RPC (ReadTablet)";
}

void RemoteTabletNode::DoWriteTablet(google::protobuf::RpcController* controller,
                                     const WriteTabletRequest* request,
                                     WriteTabletResponse* response,
                                     google::protobuf::Closure* done,
                                     WriteRpcTimer* timer) {
    VLOG(8) << "accept RPC (WriteTablet)";
    int32_t row_num = request->row_list_size();
    write_pending_counter.Sub(row_num);
    m_tabletnode_impl->WriteTablet(request, response, done, timer);
    VLOG(8) << "finish RPC (WriteTablet)";
}

void RemoteTabletNode::DoScanTablet(google::protobuf::RpcController* controller,
                                    const ScanTabletRequest* request,
                                    ScanTabletResponse* response,
                                    google::protobuf::Closure* done) {
    VLOG(8) << "accept RPC (ScanTablet)";
    scan_pending_counter.Dec();
    m_tabletnode_impl->ScanTablet(request, response, done);
    VLOG(8) << "finish RPC (ScanTablet)";
}

void RemoteTabletNode::DoGetSnapshot(google::protobuf::RpcController* controller,
                                     const SnapshotRequest* request, SnapshotResponse* response,
                                     google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (GetSnapshot) id: " << id;
    m_tabletnode_impl->GetSnapshot(request, response, done);
    LOG(INFO) << "finish RPC (GetSnapshot) id: " << id;
}

void RemoteTabletNode::DoReleaseSnapshot(google::protobuf::RpcController* controller,
                                              const ReleaseSnapshotRequest* request, ReleaseSnapshotResponse* response,
                                              google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (ReleaseSnapshot) id: " << id;
    m_tabletnode_impl->ReleaseSnapshot(request, response, done);
    LOG(INFO) << "finish RPC (ReleaseSnapshot) id: " << id;
}



void RemoteTabletNode::DoQuery(google::protobuf::RpcController* controller,
                               const QueryRequest* request,
                               QueryResponse* response,
                               google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    int64_t start_micros = get_micros();
    LOG(INFO) << "accept RPC (Query) id: " << id;
    m_tabletnode_impl->Query(request, response, done);
    LOG(INFO) << "finish RPC (Query) id: " << id
        << ", cost " << (get_micros() - start_micros) / 1000 << "ms.";
}

void RemoteTabletNode::DoSplitTablet(google::protobuf::RpcController* controller,
                                     const SplitTabletRequest* request,
                                     SplitTabletResponse* response,
                                     google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (SplitTablet) id: " << id;
    m_tabletnode_impl->SplitTablet(request, response, done);
    LOG(INFO) << "finish RPC (SplitTablet) id: " << id;
}

void RemoteTabletNode::DoMergeTablet(google::protobuf::RpcController* controller,
                                     const MergeTabletRequest* request,
                                     MergeTabletResponse* response,
                                     google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (MergeTablet) id: " << id;
    m_tabletnode_impl->MergeTablet(request, response, done);
    LOG(INFO) << "finish RPC (MergeTablet) id: " << id;
}

void RemoteTabletNode::DoCompactTablet(google::protobuf::RpcController* controller,
                                     const CompactTabletRequest* request,
                                     CompactTabletResponse* response,
                                     google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (CompactTablet) id: " << id;
    compact_pending_counter.Dec();
    m_tabletnode_impl->CompactTablet(request, response, done);
    LOG(INFO) << "finish RPC (CompactTablet) id: " << id;
}

void RemoteTabletNode::DoScheduleRpc() {
    RpcTask* rpc = NULL;
    bool status = m_rpc_schedule->DequeueRpc(&rpc);
    CHECK(status);
    std::string table_name;

    switch (rpc->rpc_type) {
    case RPC_READ: {
        ReadRpc* read_rpc = (ReadRpc*)rpc;
        table_name = read_rpc->request->tablet_name();
        DoReadTablet(read_rpc->controller, read_rpc->start_micros,
                     read_rpc->request, read_rpc->response,
                     read_rpc->done,read_rpc->timer);
    } break;
    case RPC_SCAN: {
        ScanRpc* scan_rpc = (ScanRpc*)rpc;
        table_name = scan_rpc->request->table_name();
        DoScanTablet(scan_rpc->controller, scan_rpc->request,
                     scan_rpc->response, scan_rpc->done);
    } break;
    default:
        abort();
    }

    delete rpc;
    status = m_rpc_schedule->FinishRpc(table_name);
    CHECK(status);
}

} // namespace tabletnode
} // namespace tera
