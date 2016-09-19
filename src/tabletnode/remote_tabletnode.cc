// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tabletnode/remote_tabletnode.h"

#include <boost/bind.hpp>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "tabletnode/tabletnode_impl.h"
#include "utils/counter.h"
#include "utils/network_utils.h"
#include "utils/timer.h"

DECLARE_int32(tera_tabletnode_ctrl_thread_num);
DECLARE_int32(tera_tabletnode_write_thread_num);
DECLARE_int32(tera_tabletnode_read_thread_num);
DECLARE_int32(tera_tabletnode_scan_thread_num);
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
    : tabletnode_impl_(tabletnode_impl),
      ctrl_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_ctrl_thread_num)),
      write_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_write_thread_num)),
      read_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_read_thread_num)),
      scan_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_scan_thread_num)),
      compact_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_manual_compact_thread_num)),
      read_rpc_schedule_(new RpcSchedule(new FairSchedulePolicy)),
      scan_rpc_schedule_(new RpcSchedule(new FairSchedulePolicy)) {}

RemoteTabletNode::~RemoteTabletNode() {}

void RemoteTabletNode::LoadTablet(google::protobuf::RpcController* controller,
                                  const LoadTabletRequest* request,
                                  LoadTabletResponse* response,
                                  google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (LoadTablet) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        boost::bind(&RemoteTabletNode::DoLoadTablet, this, controller,
                   request, response, done);
    ctrl_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::UnloadTablet(google::protobuf::RpcController* controller,
                                    const UnloadTabletRequest* request,
                                    UnloadTabletResponse* response,
                                    google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (UnloadTablet) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        boost::bind(&RemoteTabletNode::DoUnloadTablet, this, controller,
                   request, response, done);
    ctrl_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::ReadTablet(google::protobuf::RpcController* controller,
                                  const ReadTabletRequest* request,
                                  ReadTabletResponse* response,
                                  google::protobuf::Closure* done) {
    VLOG(8) << "accept RPC (ReadTablet): " << tera::utils::GetRemoteAddress(controller);
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
        VLOG(8) << "finish RPC (ReadTablet)";
    } else {
        int32_t row_num = request->row_info_list_size();
        read_pending_counter.Add(row_num);
        int64_t start_micros = get_micros();
        ReadRpcTimer* timer = new ReadRpcTimer(request, response, done, start_micros);
        RpcTimerList::Instance()->Push(timer);

        ReadRpc* rpc = new ReadRpc(controller, request, response, done,
                                   timer, start_micros);
        read_rpc_schedule_->EnqueueRpc(request->tablet_name(), rpc);
        read_thread_pool_->AddTask(boost::bind(&RemoteTabletNode::DoScheduleRpc, this,
                                                read_rpc_schedule_.get()));
    }
}

void RemoteTabletNode::WriteTablet(google::protobuf::RpcController* controller,
                                   const WriteTabletRequest* request,
                                   WriteTabletResponse* response,
                                   google::protobuf::Closure* done) {
    VLOG(8) << "accept RPC (WriteTablet): " << tera::utils::GetRemoteAddress(controller);
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
        VLOG(8) << "finish RPC (WriteTablet)";
    } else {
        int32_t row_num = request->row_list_size();
        write_pending_counter.Add(row_num);
        int64_t start_micros = get_micros();
        WriteRpcTimer* timer = new WriteRpcTimer(request, response, done, start_micros);
        RpcTimerList::Instance()->Push(timer);
        ThreadPool::Task callback =
            boost::bind(&RemoteTabletNode::DoWriteTablet, this,
                       controller, request, response, done, timer);
        write_thread_pool_->AddTask(callback);
    }
}

void RemoteTabletNode::ScanTablet(google::protobuf::RpcController* controller,
                                  const ScanTabletRequest* request,
                                  ScanTabletResponse* response,
                                  google::protobuf::Closure* done) {
    VLOG(8) << "accept RPC (ScanTablet): " << tera::utils::GetRemoteAddress(controller);
    if (scan_pending_counter.Get() > FLAGS_tera_scan_request_pending_limit) {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kTabletNodeIsBusy);
        done->Run();
        VLOG(8) << "finish RPC (ScanTablet)";
    } else {
        scan_pending_counter.Inc();
        ScanRpc* rpc = new ScanRpc(controller, request, response, done);
        scan_rpc_schedule_->EnqueueRpc(request->table_name(), rpc);
        scan_thread_pool_->AddTask(boost::bind(&RemoteTabletNode::DoScheduleRpc,
                                                this, scan_rpc_schedule_.get()));
    }
}

void RemoteTabletNode::GetSnapshot(google::protobuf::RpcController* controller,
                                  const SnapshotRequest* request,
                                  SnapshotResponse* response,
                                  google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (GetSnapshot) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        boost::bind(&RemoteTabletNode::DoGetSnapshot, this, controller,
                    request, response, done);
    write_thread_pool_->AddPriorityTask(callback);
}

void RemoteTabletNode::ReleaseSnapshot(google::protobuf::RpcController* controller,
                                           const ReleaseSnapshotRequest* request,
                                           ReleaseSnapshotResponse* response,
                                           google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (ReleaseSnapshot) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
    boost::bind(&RemoteTabletNode::DoReleaseSnapshot, this, controller,
               request, response, done);
    write_thread_pool_->AddPriorityTask(callback);
}

void RemoteTabletNode::Rollback(google::protobuf::RpcController* controller,
                                const SnapshotRollbackRequest* request,
                                SnapshotRollbackResponse* response,
                                google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (Rollback) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
    boost::bind(&RemoteTabletNode::DoRollback, this, controller,
               request, response, done);
    write_thread_pool_->AddPriorityTask(callback);
}


void RemoteTabletNode::Query(google::protobuf::RpcController* controller,
                             const QueryRequest* request,
                             QueryResponse* response,
                             google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (Query) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        boost::bind(&RemoteTabletNode::DoQuery, this, controller,
                   request, response, done);
    ctrl_thread_pool_->AddPriorityTask(callback);
}

void RemoteTabletNode::CmdCtrl(google::protobuf::RpcController* controller,
                               const TsCmdCtrlRequest* request,
                               TsCmdCtrlResponse* response,
                               google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (CmdCtrl) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        boost::bind(&RemoteTabletNode::DoCmdCtrl, this, controller,
                    request, response, done);
    ctrl_thread_pool_->AddPriorityTask(callback);
}

void RemoteTabletNode::SplitTablet(google::protobuf::RpcController* controller,
                                   const SplitTabletRequest* request,
                                   SplitTabletResponse* response,
                                   google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (SplitTablet) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        boost::bind(&RemoteTabletNode::DoSplitTablet, this, controller,
                    request, response, done);
    ctrl_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::CompactTablet(google::protobuf::RpcController* controller,
                                   const CompactTabletRequest* request,
                                   CompactTabletResponse* response,
                                   google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (CompactTablet) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    compact_pending_counter.Inc();
    ThreadPool::Task callback =
        boost::bind(&RemoteTabletNode::DoCompactTablet, this, controller,
                   request, response, done);
    compact_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::Update(google::protobuf::RpcController* controller,
                              const UpdateRequest* request,
                              UpdateResponse* response,
                              google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (Update) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        boost::bind(&RemoteTabletNode::DoUpdate, this, controller,
                   request, response, done);
    ctrl_thread_pool_->AddTask(callback);
}

std::string RemoteTabletNode::ProfilingLog() {
    return "ctrl " + ctrl_thread_pool_->ProfilingLog()
        + " read " + read_thread_pool_->ProfilingLog()
        + " write " + write_thread_pool_->ProfilingLog()
        + " scan " + scan_thread_pool_->ProfilingLog()
        + " compact " + compact_thread_pool_->ProfilingLog();
}

void RemoteTabletNode::DoLoadTablet(google::protobuf::RpcController* controller,
                                    const LoadTabletRequest* request,
                                    LoadTabletResponse* response,
                                    google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "run RPC (LoadTablet) id: " << id;
    tabletnode_impl_->LoadTablet(request, response, done);
    LOG(INFO) << "finish RPC (LoadTablet) id: " << id;
}

void RemoteTabletNode::DoUnloadTablet(google::protobuf::RpcController* controller,
                                      const UnloadTabletRequest* request,
                                      UnloadTabletResponse* response,
                                      google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "run RPC (UnloadTablet) id: " << id;
    tabletnode_impl_->UnloadTablet(request, response, done);
    LOG(INFO) << "finish RPC (UnloadTablet) id: " << id;
}

void RemoteTabletNode::DoReadTablet(google::protobuf::RpcController* controller,
                                    int64_t start_micros,
                                    const ReadTabletRequest* request,
                                    ReadTabletResponse* response,
                                    google::protobuf::Closure* done,
                                    ReadRpcTimer* timer) {
    VLOG(8) << "run RPC (ReadTablet)";
    int32_t row_num = request->row_info_list_size();
    read_pending_counter.Sub(row_num);

    bool is_read_timeout = false;
    if (request->has_client_timeout_ms()) {
        int64_t read_timeout = request->client_timeout_ms() * 1000; // ms -> us
        int64_t detal = get_micros() - start_micros;
        if (detal > read_timeout) {
            VLOG(5) << "timeout, drop read request for:" << request->tablet_name()
                << ", detal(in us):" << detal
                << ", read_timeout(in us):" << read_timeout;
            is_read_timeout = true;
        }
    }

    if (!is_read_timeout) {
        tabletnode_impl_->ReadTablet(start_micros, request, response, done);
    } else {
        response->set_sequence_id(request->sequence_id());
        response->set_success_num(0);
        response->set_status(kTableIsBusy);
        done->Run();
    }

    if (NULL != timer) {
        RpcTimerList::Instance()->Erase(timer);
        delete timer;
    }
    VLOG(8) << "finish RPC (ReadTablet)";
}

void RemoteTabletNode::DoWriteTablet(google::protobuf::RpcController* controller,
                                     const WriteTabletRequest* request,
                                     WriteTabletResponse* response,
                                     google::protobuf::Closure* done,
                                     WriteRpcTimer* timer) {
    VLOG(8) << "run RPC (WriteTablet)";
    int32_t row_num = request->row_list_size();
    write_pending_counter.Sub(row_num);
    tabletnode_impl_->WriteTablet(request, response, done, timer);
    VLOG(8) << "finish RPC (WriteTablet)";
}

void RemoteTabletNode::DoScanTablet(google::protobuf::RpcController* controller,
                                    const ScanTabletRequest* request,
                                    ScanTabletResponse* response,
                                    google::protobuf::Closure* done) {
    VLOG(8) << "run RPC (ScanTablet)";
    scan_pending_counter.Dec();
    tabletnode_impl_->ScanTablet(request, response, done);
    VLOG(8) << "finish RPC (ScanTablet)";
}

void RemoteTabletNode::DoGetSnapshot(google::protobuf::RpcController* controller,
                                     const SnapshotRequest* request, SnapshotResponse* response,
                                     google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "run RPC (GetSnapshot) id: " << id;
    tabletnode_impl_->GetSnapshot(request, response, done);
    LOG(INFO) << "finish RPC (GetSnapshot) id: " << id;
}

void RemoteTabletNode::DoReleaseSnapshot(google::protobuf::RpcController* controller,
                                              const ReleaseSnapshotRequest* request, ReleaseSnapshotResponse* response,
                                              google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "run RPC (ReleaseSnapshot) id: " << id;
    tabletnode_impl_->ReleaseSnapshot(request, response, done);
    LOG(INFO) << "finish RPC (ReleaseSnapshot) id: " << id;
}


void RemoteTabletNode::DoRollback(google::protobuf::RpcController* controller,
                                  const SnapshotRollbackRequest* request,
                                  SnapshotRollbackResponse* response,
                                  google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "run RPC (Rollback) id: " << id;
    tabletnode_impl_->Rollback(request, response, done);
    LOG(INFO) << "finish RPC (Rollback) id: " << id;
}


void RemoteTabletNode::DoQuery(google::protobuf::RpcController* controller,
                               const QueryRequest* request,
                               QueryResponse* response,
                               google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    int64_t start_micros = get_micros();
    LOG(INFO) << "run RPC (Query) id: " << id;
    tabletnode_impl_->Query(request, response, done);
    LOG(INFO) << "finish RPC (Query) id: " << id
        << ", cost " << (get_micros() - start_micros) / 1000 << "ms.";
}

void RemoteTabletNode::DoCmdCtrl(google::protobuf::RpcController* controller,
                                 const TsCmdCtrlRequest* request,
                                 TsCmdCtrlResponse* response,
                                 google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    int64_t start_micros = get_micros();
    LOG(INFO) << "run RPC (CmdCtrl) id: " << id;
    tabletnode_impl_->CmdCtrl(request, response, done);
    LOG(INFO) << "finish RPC (CmdCtrl) id: " << id
        << ", cost " << (get_micros() - start_micros) / 1000 << "ms.";
}

void RemoteTabletNode::DoSplitTablet(google::protobuf::RpcController* controller,
                                     const SplitTabletRequest* request,
                                     SplitTabletResponse* response,
                                     google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "run RPC (SplitTablet) id: " << id;
    tabletnode_impl_->SplitTablet(request, response, done);
    LOG(INFO) << "finish RPC (SplitTablet) id: " << id;
}

void RemoteTabletNode::DoCompactTablet(google::protobuf::RpcController* controller,
                                     const CompactTabletRequest* request,
                                     CompactTabletResponse* response,
                                     google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "run RPC (CompactTablet) id: " << id;
    compact_pending_counter.Dec();
    tabletnode_impl_->CompactTablet(request, response, done);
    LOG(INFO) << "finish RPC (CompactTablet) id: " << id;
}

void RemoteTabletNode::DoUpdate(google::protobuf::RpcController* controller,
                                const UpdateRequest* request,
                                UpdateResponse* response,
                                google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (Update) id: " << id;
    tabletnode_impl_->Update(request, response, done);
    LOG(INFO) << "finish RPC (Update) id: " << id;
}

void RemoteTabletNode::DoScheduleRpc(RpcSchedule* rpc_schedule) {
    RpcTask* rpc = NULL;
    bool status = rpc_schedule->DequeueRpc(&rpc);
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
    status = rpc_schedule->FinishRpc(table_name);
    CHECK(status);
}

} // namespace tabletnode
} // namespace tera
