// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tabletnode/remote_tabletnode.h"

#include <functional>
#include <memory>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "common/metric/metric_counter.h"
#include "common/metric/ratio_subscriber.h"
#include "common/metric/prometheus_subscriber.h"
#include "tabletnode/tabletnode_impl.h"
#include "tabletnode/tabletnode_metric_name.h"
#include "utils/network_utils.h"
#include "common/timer.h"

DECLARE_int32(tera_tabletnode_ctrl_thread_num);
DECLARE_int32(tera_tabletnode_write_thread_num);
DECLARE_int32(tera_tabletnode_read_thread_num);
DECLARE_int32(tera_tabletnode_scan_thread_num);
DECLARE_int32(tera_tabletnode_manual_compact_thread_num);
DECLARE_int32(tera_request_pending_limit);
DECLARE_int32(tera_scan_request_pending_limit);

namespace tera {
namespace tabletnode {

//Add SubscriberType::SUM for caculating SLA
tera::MetricCounter read_request_counter(kRequestCountMetric, kApiLabelRead, 
                                         {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter write_request_counter(kRequestCountMetric, kApiLabelWrite, 
                                          {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter scan_request_counter(kRequestCountMetric, kApiLabelScan, {SubscriberType::QPS});

tera::MetricCounter read_pending_counter(kPendingCountMetric, kApiLabelRead, {SubscriberType::LATEST}, false);
tera::MetricCounter write_pending_counter(kPendingCountMetric, kApiLabelWrite, {SubscriberType::LATEST}, false);
tera::MetricCounter scan_pending_counter(kPendingCountMetric, kApiLabelScan, {SubscriberType::LATEST}, false);
tera::MetricCounter compact_pending_counter(kPendingCountMetric, kApiLabelCompact, {SubscriberType::LATEST}, false);

//Add SubscriberType::SUM for caculating SLA
tera::MetricCounter read_reject_counter(kRejectCountMetric, kApiLabelRead, 
                                        {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter write_reject_counter(kRejectCountMetric, kApiLabelWrite, 
                                         {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter scan_reject_counter(kRejectCountMetric, kApiLabelScan, {SubscriberType::QPS});

tera::MetricCounter finished_read_request_counter(kFinishedRequestCountMetric, kApiLabelRead, {SubscriberType::QPS});
tera::MetricCounter finished_write_request_counter(kFinishedRequestCountMetric, kApiLabelWrite, {SubscriberType::QPS});
tera::MetricCounter finished_scan_request_counter(kFinishedRequestCountMetric, kApiLabelScan, {SubscriberType::QPS});

//These three metrics are not auto registered with a subscriber, they are used for ratio subscriber.
tera::MetricCounter read_delay(kRequestDelayMetric, kApiLabelRead, {});
tera::MetricCounter write_delay(kRequestDelayMetric, kApiLabelWrite, {});
tera::MetricCounter scan_delay(kRequestDelayMetric, kApiLabelScan, {});

tera::AutoSubscriberRegister rand_read_delay_per_request(std::unique_ptr<Subscriber>(new tera::RatioSubscriber(
    MetricId("tera_ts_read_delay_us_per_request"),
    std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(MetricId(kRequestDelayMetric, kApiLabelRead), SubscriberType::SUM)),
    std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(MetricId(kFinishedRequestCountMetric, kApiLabelRead), SubscriberType::SUM)))));

tera::AutoSubscriberRegister write_delay_per_request(std::unique_ptr<Subscriber>(new tera::RatioSubscriber(
    MetricId("tera_ts_write_delay_us_per_request"),
    std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(MetricId(kRequestDelayMetric, kApiLabelWrite), SubscriberType::SUM)),
    std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(MetricId(kFinishedRequestCountMetric, kApiLabelWrite), SubscriberType::SUM)))));

tera::AutoSubscriberRegister scan_delay_per_request(std::unique_ptr<Subscriber>(new tera::RatioSubscriber(
    MetricId("tera_ts_scan_delay_us_per_request"),
    std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(MetricId(kRequestDelayMetric, kApiLabelScan), SubscriberType::SUM)),
    std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(MetricId(kFinishedRequestCountMetric, kApiLabelScan), SubscriberType::SUM)))));

void ReadDoneWrapper::Run() {
    if (response_->has_detail()) {
        int64_t now_us = get_micros();
        int64_t used_us =  now_us - start_micros_;
        if (used_us <= 0) {
            LOG(ERROR) << "now us: "<< now_us << " start_us: "<< start_micros_;
        }
        finished_read_request_counter.Add(response_->detail().status_size());
        read_delay.Add(used_us);
    }
    delete this;
}

void WriteDoneWrapper::Run() {
    if (response_->row_status_list_size() != 0) {
        int64_t now_us = get_micros();
        int64_t used_us =  now_us - start_micros_;
        if (used_us <= 0) {
            LOG(ERROR) << "now us: "<< now_us << " start_us: "<< start_micros_;
        }

        finished_write_request_counter.Add(response_->row_status_list_size());
        write_delay.Add(used_us);
    }
    delete this;
}

void ScanDoneWrapper::Run() {
    if (response_->has_results()) {
        int64_t now_us = get_micros();
        int64_t used_us =  now_us - start_micros_;
        if (used_us <= 0) {
            LOG(ERROR) << "now us: "<< now_us << " start_us: "<< start_micros_;
        }

        finished_scan_request_counter.Add(response_->results().key_values_size());
        scan_delay.Add(used_us);
    }
    delete this;
}

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
        std::bind(&RemoteTabletNode::DoLoadTablet, this, controller,
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
        std::bind(&RemoteTabletNode::DoUnloadTablet, this, controller,
                  request, response, done);
    ctrl_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::ReadTablet(google::protobuf::RpcController* controller,
                                  const ReadTabletRequest* request,
                                  ReadTabletResponse* response,
                                  google::protobuf::Closure* done) {
    int64_t start_micros = get_micros();
    done = ReadDoneWrapper::NewInstance(start_micros, response, done);
    VLOG(8) << "accept RPC (ReadTablet): [" << request->tablet_name() << "] " << tera::utils::GetRemoteAddress(controller);
    static uint32_t last_print = time(NULL);
    int32_t row_num = request->row_info_list_size();
    read_request_counter.Add(row_num);
    if (read_pending_counter.Get() > FLAGS_tera_request_pending_limit) {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kTabletNodeIsBusy);
        read_reject_counter.Add(row_num);
        done->Run();
        uint32_t now_time = time(NULL);
        if (now_time > last_print) {
            LOG(WARNING) << "Too many pending read requests, return TabletNode Is Busy!";
            last_print = now_time;
        }
        VLOG(8) << "finish RPC (ReadTablet)";
    } else {
        read_pending_counter.Add(row_num);
        ReadRpcTimer* timer = new ReadRpcTimer(request, response, done, start_micros);
        RpcTimerList::Instance()->Push(timer);

        ReadRpc* rpc = new ReadRpc(controller, request, response, done,
                                   timer, start_micros);
        read_rpc_schedule_->EnqueueRpc(request->tablet_name(), rpc);
        read_thread_pool_->AddTask(std::bind(&RemoteTabletNode::DoScheduleRpc, this,
                                             read_rpc_schedule_.get()));
    }
}

void RemoteTabletNode::WriteTablet(google::protobuf::RpcController* controller,
                                   const WriteTabletRequest* request,
                                   WriteTabletResponse* response,
                                   google::protobuf::Closure* done) {
    int64_t start_micros = get_micros();
    done = WriteDoneWrapper::NewInstance(start_micros, response, done);
    VLOG(8) << "accept RPC (WriteTablet): [" << request->tablet_name() << "] " << tera::utils::GetRemoteAddress(controller);
    static uint32_t last_print = time(NULL);
    int32_t row_num = request->row_list_size();
    write_request_counter.Add(row_num);
    if (write_pending_counter.Get() > FLAGS_tera_request_pending_limit) {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kTabletNodeIsBusy);
        write_reject_counter.Add(row_num);
        done->Run();
        uint32_t now_time = time(NULL);
        if (now_time > last_print) {
            LOG(WARNING) << "Too many pending write requests, return TabletNode Is Busy!";
            last_print = now_time;
        }
        VLOG(8) << "finish RPC (WriteTablet)";
    } else {
        write_pending_counter.Add(row_num);
        WriteRpcTimer* timer = new WriteRpcTimer(request, response, done, start_micros);
        RpcTimerList::Instance()->Push(timer);
        ThreadPool::Task callback =
            std::bind(&RemoteTabletNode::DoWriteTablet, this,
                      controller, request, response, done, timer);
        write_thread_pool_->AddTask(callback);
    }
}

void RemoteTabletNode::ScanTablet(google::protobuf::RpcController* controller,
                                  const ScanTabletRequest* request,
                                  ScanTabletResponse* response,
                                  google::protobuf::Closure* done) {
    done = ScanDoneWrapper::NewInstance(get_micros(), response, done);
    VLOG(8) << "accept RPC (ScanTablet): [" << request->table_name() << "] " << tera::utils::GetRemoteAddress(controller);
    scan_request_counter.Inc();
    if (scan_pending_counter.Get() > FLAGS_tera_scan_request_pending_limit) {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kTabletNodeIsBusy);
        scan_reject_counter.Inc();
        done->Run();
        VLOG(8) << "finish RPC (ScanTablet)";
    } else {
        scan_pending_counter.Inc();
        ScanRpc* rpc = new ScanRpc(controller, request, response, done);
        scan_rpc_schedule_->EnqueueRpc(request->table_name(), rpc);
        scan_thread_pool_->AddTask(std::bind(&RemoteTabletNode::DoScheduleRpc,
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
        std::bind(&RemoteTabletNode::DoGetSnapshot, this, controller,
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
    std::bind(&RemoteTabletNode::DoReleaseSnapshot, this, controller,
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
    std::bind(&RemoteTabletNode::DoRollback, this, controller,
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
        std::bind(&RemoteTabletNode::DoQuery, this, controller,
                  request, response, done);
    ctrl_thread_pool_->AddPriorityTask(callback);
}

void RemoteTabletNode::CmdCtrl(google::protobuf::RpcController* controller,
                               const TsCmdCtrlRequest* request,
                               TsCmdCtrlResponse* response,
                               google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (CmdCtrl) id: " << id << ", [" << request->command()
        << "] src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteTabletNode::DoCmdCtrl, this, controller,
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
        std::bind(&RemoteTabletNode::DoSplitTablet, this, controller,
                  request, response, done);
    ctrl_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::ComputeSplitKey(google::protobuf::RpcController* controller,
                                   const SplitTabletRequest* request,
                                   SplitTabletResponse* response,
                                   google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "accept RPC (ComputeSplitKey) id: " << id << ", src: " << tera::utils::GetRemoteAddress(controller);
    ThreadPool::Task callback =
        std::bind(&RemoteTabletNode::DoComputeSplitKey, this, controller,
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
        std::bind(&RemoteTabletNode::DoCompactTablet, this, controller,
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
        std::bind(&RemoteTabletNode::DoUpdate, this, controller,
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
            LOG(WARNING) << "timeout, drop read request for:" << request->tablet_name()
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
        read_reject_counter.Inc();
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

void RemoteTabletNode::DoComputeSplitKey(google::protobuf::RpcController* controller,
                                     const SplitTabletRequest* request,
                                     SplitTabletResponse* response,
                                     google::protobuf::Closure* done) {
    uint64_t id = request->sequence_id();
    LOG(INFO) << "run RPC (ComputeSplitKey) id: " << id;
    tabletnode_impl_->ComputeSplitKey(request, response, done);
    LOG(INFO) << "finish RPC (ComputeSplitKey) id: " << id;
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
