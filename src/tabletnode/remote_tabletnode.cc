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
#include "common/metric/percentile_counter.h"
#include "common/timer.h"
#include "tabletnode/tabletnode_impl.h"
#include "tabletnode/tabletnode_metric_name.h"
#include "utils/network_utils.h"
#include "quota/ts_write_flow_controller.h"

DECLARE_int32(tera_tabletnode_ctrl_thread_num);
DECLARE_int32(tera_tabletnode_lightweight_ctrl_thread_num);
DECLARE_int32(tera_tabletnode_write_thread_num);
DECLARE_int32(tera_tabletnode_read_thread_num);
DECLARE_int32(tera_tabletnode_scan_thread_num);
DECLARE_int32(tera_tabletnode_manual_compact_thread_num);
DECLARE_int32(tera_request_pending_limit);
DECLARE_int32(tera_scan_request_pending_limit);
DECLARE_string(tera_auth_policy);
DECLARE_double(tera_quota_unlimited_pending_ratio);
DECLARE_int32(tera_quota_scan_max_retry_times);
DECLARE_int32(tera_quota_scan_retry_delay_interval);
DECLARE_uint64(tera_quota_max_retry_queue_length);

namespace tera {
namespace tabletnode {

// Add SubscriberType::SUM for caculating SLA
tera::MetricCounter read_request_counter(kRequestCountMetric, kApiLabelRead,
                                         {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter write_request_counter(kRequestCountMetric, kApiLabelWrite,
                                          {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter scan_request_counter(kRequestCountMetric, kApiLabelScan, {SubscriberType::QPS});

tera::MetricCounter read_pending_counter(kPendingCountMetric, kApiLabelRead,
                                         {SubscriberType::LATEST}, false);
tera::MetricCounter write_pending_counter(kPendingCountMetric, kApiLabelWrite,
                                          {SubscriberType::LATEST}, false);
tera::MetricCounter scan_pending_counter(kPendingCountMetric, kApiLabelScan,
                                         {SubscriberType::LATEST}, false);
tera::MetricCounter compact_pending_counter(kPendingCountMetric, kApiLabelCompact,
                                            {SubscriberType::LATEST}, false);

// Add SubscriberType::SUM for caculating SLA
tera::MetricCounter read_reject_counter(kRejectCountMetric, kApiLabelRead,
                                        {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter write_reject_counter(kRejectCountMetric, kApiLabelWrite,
                                         {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter scan_reject_counter(kRejectCountMetric, kApiLabelScan, {SubscriberType::QPS});

tera::MetricCounter read_quota_rejest_counter(kQuotaRejectCountMetric, kApiLabelRead,
                                              {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter write_quota_reject_counter(kQuotaRejectCountMetric, kApiLabelWrite,
                                               {SubscriberType::QPS, SubscriberType::SUM});
tera::MetricCounter scan_quota_reject_counter(kQuotaRejectCountMetric, kApiLabelScan,
                                              {SubscriberType::QPS});

tera::MetricCounter finished_read_request_counter(kFinishedRequestCountMetric, kApiLabelRead,
                                                  {SubscriberType::QPS});
tera::MetricCounter finished_write_request_counter(kFinishedRequestCountMetric, kApiLabelWrite,
                                                   {SubscriberType::QPS});
tera::MetricCounter finished_scan_request_counter(kFinishedRequestCountMetric, kApiLabelScan,
                                                  {SubscriberType::QPS});

tera::MetricCounter read_delay(kRequestDelayMetric, kApiLabelRead, {});
tera::MetricCounter write_delay(kRequestDelayMetric, kApiLabelWrite, {});
tera::MetricCounter scan_delay(kRequestDelayMetric, kApiLabelScan, {});

tera::AutoSubscriberRegister rand_read_delay_per_request(
    std::unique_ptr<Subscriber>(new tera::RatioSubscriber(
        MetricId(kRequestDelayAvgMetric, kApiLabelRead),
        std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
            MetricId(kRequestDelayMetric, kApiLabelRead), SubscriberType::SUM)),
        std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
            MetricId(kFinishedRequestCountMetric, kApiLabelRead), SubscriberType::SUM)))));

tera::AutoSubscriberRegister write_delay_per_request(
    std::unique_ptr<Subscriber>(new tera::RatioSubscriber(
        MetricId(kRequestDelayAvgMetric, kApiLabelWrite),
        std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
            MetricId(kRequestDelayMetric, kApiLabelWrite), SubscriberType::SUM)),
        std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
            MetricId(kFinishedRequestCountMetric, kApiLabelWrite), SubscriberType::SUM)))));

tera::AutoSubscriberRegister scan_delay_per_request(
    std::unique_ptr<Subscriber>(new tera::RatioSubscriber(
        MetricId(kRequestDelayAvgMetric, kApiLabelScan),
        std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
            MetricId(kRequestDelayMetric, kApiLabelScan), SubscriberType::SUM)),
        std::unique_ptr<Subscriber>(new tera::PrometheusSubscriber(
            MetricId(kFinishedRequestCountMetric, kApiLabelScan), SubscriberType::SUM)))));

tera::PercentileCounter write_95(kRequestDelayPercentileMetric, kWriteLabelPercentile95, 95);
tera::PercentileCounter write_99(kRequestDelayPercentileMetric, kWriteLabelPercentile99, 99);
tera::PercentileCounter read_95(kRequestDelayPercentileMetric, kReadLabelPercentile95, 95);
tera::PercentileCounter read_99(kRequestDelayPercentileMetric, kReadLabelPercentile99, 99);
tera::PercentileCounter scan_95(kRequestDelayPercentileMetric, kScanLabelPercentile95, 95);
tera::PercentileCounter scan_99(kRequestDelayPercentileMetric, kScanLabelPercentile99, 99);

void ReadDoneWrapper::Run() {
  int64_t now_us = get_micros();
  int64_t used_us = now_us - start_micros_;
  int64_t row_num = request_->row_info_list_size();
  if (used_us < 0) {
    LOG(ERROR) << "now us: " << now_us << " start_us: " << start_micros_;
  }
  finished_read_request_counter.Add(row_num);
  read_delay.Add(used_us);
  if (row_num > 0) {
    read_95.Append(used_us / row_num);
    read_99.Append(used_us / row_num);
  }

  // quota entry adjuest
  if (response_->has_detail() && response_->success_num() > 0) {
    int64_t success_num = response_->success_num();
    int64_t sum_read_bytes = 0;
    int32_t row_result_size = response_->detail().row_result_size();
    for (int32_t row_result_index = 0; row_result_index < row_result_size; ++row_result_index) {
      sum_read_bytes += response_->detail().row_result(row_result_index).ByteSize();
    }
    quota_entry_->Adjust(request_->tablet_name(), kQuotaReadBytes, sum_read_bytes / success_num);
  }
  delete this;
}

void WriteDoneWrapper::Run() {
  int64_t now_us = get_micros();
  int64_t used_us = now_us - start_micros_;
  int64_t row_num = request_->row_list_size();
  if (used_us < 0) {
    LOG(ERROR) << "now us: " << now_us << " start_us: " << start_micros_;
  }
  finished_write_request_counter.Add(row_num);
  write_delay.Add(used_us);
  if (row_num > 0) {
    write_95.Append(used_us / row_num);
    write_99.Append(used_us / row_num);
  }
  delete this;
}

void ScanDoneWrapper::Run() {
  if (response_->has_results()) {
    int64_t now_us = get_micros();
    int64_t used_us = now_us - start_micros_;
    if (used_us < 0) {
      LOG(ERROR) << "now us: " << now_us << " start_us: " << start_micros_;
    }
    int64_t row_num = response_->results().key_values_size();
    finished_scan_request_counter.Add(row_num);
    scan_delay.Add(used_us);
    if (row_num > 0) {
      scan_95.Append(used_us / row_num);
      scan_99.Append(used_us / row_num);
    }

    if (response_->has_row_count() && response_->row_count() > 0) {
      quota_entry_->Adjust(request_->table_name(), kQuotaScanReqs, response_->row_count());
    }
    if (response_->has_data_size() && response_->data_size() > 0) {
      quota_entry_->Adjust(request_->table_name(), kQuotaScanBytes, response_->data_size());
    }
  }
  delete this;
}

enum RpcType { RPC_READ = 1, RPC_SCAN = 2 };

struct ReadRpc : public RpcTask {
  google::protobuf::RpcController* controller;
  const ReadTabletRequest* request;
  ReadTabletResponse* response;
  google::protobuf::Closure* done;
  ReadRpcTimer* timer;
  int64_t start_micros;

  ReadRpc(google::protobuf::RpcController* ctrl, const ReadTabletRequest* req,
          ReadTabletResponse* resp, google::protobuf::Closure* done, ReadRpcTimer* timer,
          int64_t start_micros)
      : RpcTask(RPC_READ),
        controller(ctrl),
        request(req),
        response(resp),
        done(done),
        timer(timer),
        start_micros(start_micros) {}
};

struct ScanRpc : public RpcTask {
  google::protobuf::RpcController* controller;
  const ScanTabletRequest* request;
  ScanTabletResponse* response;
  google::protobuf::Closure* done;
  int64_t retry_time;

  ScanRpc(google::protobuf::RpcController* ctrl, const ScanTabletRequest* req,
          ScanTabletResponse* resp, google::protobuf::Closure* done)
      : RpcTask(RPC_SCAN),
        controller(ctrl),
        request(req),
        response(resp),
        done(done),
        retry_time(0) {}
};

RemoteTabletNode::RemoteTabletNode(TabletNodeImpl* tabletnode_impl)
    : tabletnode_impl_(tabletnode_impl),
      ctrl_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_ctrl_thread_num)),
      lightweight_ctrl_thread_pool_(
          new ThreadPool(FLAGS_tera_tabletnode_lightweight_ctrl_thread_num)),
      write_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_write_thread_num)),
      read_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_read_thread_num)),
      scan_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_scan_thread_num)),
      compact_thread_pool_(new ThreadPool(FLAGS_tera_tabletnode_manual_compact_thread_num)),
      read_rpc_schedule_(new RpcSchedule(new FairSchedulePolicy)),
      scan_rpc_schedule_(new RpcSchedule(new FairSchedulePolicy)),
      quota_retry_rpc_schedule_(new RpcSchedule(new FairSchedulePolicy)),
      access_entry_(new auth::AccessEntry(FLAGS_tera_auth_policy)),
      quota_entry_(new quota::QuotaEntry) {}

RemoteTabletNode::~RemoteTabletNode() {}

void RemoteTabletNode::LoadTablet(google::protobuf::RpcController* controller,
                                  const LoadTabletRequest* request, LoadTabletResponse* response,
                                  google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  response->set_sequence_id(id);
  LOG(INFO) << "accept RPC (LoadTablet) id: " << id
            << ", src: " << tera::utils::GetRemoteAddress(controller);
  const std::string& tablet_path = request->path();
  std::lock_guard<std::mutex> lock(tablets_ctrl_mutex_);
  if (tablets_ctrl_status_.find(tablet_path) != tablets_ctrl_status_.end()) {
    ThreadPool::Task query_task = std::bind(&RemoteTabletNode::DoQueryTabletLoadStatus, this,
                                            controller, request, response, done);
    lightweight_ctrl_thread_pool_->AddTask(query_task);
    return;
  }
  if (ctrl_thread_pool_->PendingNum() > FLAGS_tera_tabletnode_ctrl_thread_num) {
    response->set_status(kTabletNodeIsBusy);
    done->Run();
    return;
  }

  tablets_ctrl_status_[tablet_path] = TabletCtrlStatus::kCtrlWaitLoad;
  ThreadPool::Task callback =
      std::bind(&RemoteTabletNode::DoLoadTablet, this, controller, request, response, done);
  ctrl_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::UnloadTablet(google::protobuf::RpcController* controller,
                                    const UnloadTabletRequest* request,
                                    UnloadTabletResponse* response,
                                    google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  response->set_sequence_id(id);
  LOG(INFO) << "accept RPC (UnloadTablet) id: " << id
            << ", src: " << tera::utils::GetRemoteAddress(controller);
  if (request->has_path()) {
    std::lock_guard<std::mutex> lock(tablets_ctrl_mutex_);
    const std::string& tablet_path = request->path();
    if (tablets_ctrl_status_.find(tablet_path) != tablets_ctrl_status_.end()) {
      ThreadPool::Task query_task = std::bind(&RemoteTabletNode::DoQueryTabletUnloadStatus, this,
                                              controller, request, response, done);
      lightweight_ctrl_thread_pool_->AddTask(query_task);
      return;
    }
  }

  if (ctrl_thread_pool_->PendingNum() > FLAGS_tera_tabletnode_ctrl_thread_num) {
    response->set_status(kTabletNodeIsBusy);
    done->Run();
    return;
  }
  if (request->has_path()) {
    tablets_ctrl_status_[request->path()] = TabletCtrlStatus::kCtrlWaitUnload;
  }

  ThreadPool::Task callback =
      std::bind(&RemoteTabletNode::DoUnloadTablet, this, controller, request, response, done);
  ctrl_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::ReadTablet(google::protobuf::RpcController* controller,
                                  const ReadTabletRequest* request, ReadTabletResponse* response,
                                  google::protobuf::Closure* done) {
  int64_t start_micros = get_micros();
  done = ReadDoneWrapper::NewInstance(start_micros, request, response, done, quota_entry_);
  VLOG(8) << "accept RPC (ReadTablet): [" << request->tablet_name() << "] "
          << tera::utils::GetRemoteAddress(controller);
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
    // check user identification & access
    if (!access_entry_->VerifyAndAuthorize(request, response)) {
      response->set_sequence_id(request->sequence_id());
      VLOG(20) << "Access VerifyAndAuthorize failed for ReadTablet";
      done->Run();
      return;
    }
    if (read_pending_counter.Get() >=
        FLAGS_tera_request_pending_limit * FLAGS_tera_quota_unlimited_pending_ratio) {
      if (!quota_entry_->CheckAndConsume(
              request->tablet_name(),
              quota::OpTypeAmountList{std::make_pair(kQuotaReadReqs, row_num)})) {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kQuotaLimited);
        read_quota_rejest_counter.Add(row_num);
        VLOG(20) << "quota_entry check failed for ReadTablet";
        done->Run();
        return;
      }
    }
    read_pending_counter.Add(row_num);
    ReadRpcTimer* timer = new ReadRpcTimer(request, response, done, start_micros);
    RpcTimerList::Instance()->Push(timer);

    ReadRpc* rpc = new ReadRpc(controller, request, response, done, timer, start_micros);
    read_rpc_schedule_->EnqueueRpc(request->tablet_name(), rpc);
    read_thread_pool_->AddTask(
        std::bind(&RemoteTabletNode::DoScheduleRpc, this, read_rpc_schedule_.get()));
  }
}

void RemoteTabletNode::WriteTablet(google::protobuf::RpcController* controller,
                                   const WriteTabletRequest* request, WriteTabletResponse* response,
                                   google::protobuf::Closure* done) {
  int64_t start_micros = get_micros();
  done = WriteDoneWrapper::NewInstance(start_micros, request, response, done);
  VLOG(8) << "accept RPC (WriteTablet): [" << request->tablet_name() << "] "
          << tera::utils::GetRemoteAddress(controller);
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
    // check user identification & access
    if (!access_entry_->VerifyAndAuthorize(request, response)) {
      response->set_sequence_id(request->sequence_id());
      VLOG(20) << "Access VerifyAndAuthorize failed for WriteTablet";
      done->Run();
      return;
    }

    // sum write bytes
    int64_t sum_write_bytes = 0;
    for (int32_t row_index = 0; row_index < row_num; ++row_index) {
      sum_write_bytes += request->row_list(row_index).ByteSize();
    }
    if (!TsWriteFlowController::Instance().TryWrite(sum_write_bytes)) {
      response->set_sequence_id(request->sequence_id());
      response->set_status(kFlowControlLimited);
      write_reject_counter.Add(row_num);
      VLOG(20) << "Reject write request due to write flow controller";
      done->Run();
      return;
    }
    if (write_pending_counter.Get() >=
        FLAGS_tera_request_pending_limit * FLAGS_tera_quota_unlimited_pending_ratio) {
      if (!quota_entry_->CheckAndConsume(
              request->tablet_name(),
              quota::OpTypeAmountList{std::make_pair(kQuotaWriteReqs, row_num),
                                      std::make_pair(kQuotaWriteBytes, sum_write_bytes)})) {
        response->set_sequence_id(request->sequence_id());
        response->set_status(kQuotaLimited);
        write_quota_reject_counter.Add(row_num);
        VLOG(20) << "quota_entry check failed for WriteTablet";
        done->Run();
        return;
      }
    }
    write_pending_counter.Add(row_num);
    WriteRpcTimer* timer = new WriteRpcTimer(request, response, done, start_micros);
    RpcTimerList::Instance()->Push(timer);
    ThreadPool::Task callback = std::bind(&RemoteTabletNode::DoWriteTablet, this, controller,
                                          request, response, done, timer);
    write_thread_pool_->AddTask(callback);
  }
}

bool RemoteTabletNode::DoQuotaScanRpcRetry(RpcTask* rpc) {
  CHECK(rpc->rpc_type == RPC_SCAN);
  ScanRpc* scan_rpc = (ScanRpc*)rpc;
  std::string table_name = scan_rpc->request->table_name();
  if (!quota_entry_->CheckAndConsume(table_name,
                                     quota::OpTypeAmountList{std::make_pair(kQuotaScanReqs, 1)})) {
    if ((quota_retry_rpc_schedule_->GetPendingTaskCount() <
         FLAGS_tera_quota_max_retry_queue_length) &&
        (++scan_rpc->retry_time < FLAGS_tera_quota_scan_max_retry_times)) {
      quota_retry_rpc_schedule_->EnqueueRpc(table_name, rpc);
      scan_thread_pool_->DelayTask(FLAGS_tera_quota_scan_retry_delay_interval,  // default 100ms
                                   std::bind(&RemoteTabletNode::DoQuotaRetryScheduleRpc, this,
                                             quota_retry_rpc_schedule_.get()));
    } else {
      scan_rpc->response->set_sequence_id(scan_rpc->request->sequence_id());
      scan_rpc->response->set_status(kQuotaLimited);
      scan_quota_reject_counter.Inc();
      VLOG(20) << "quota_entry check failed for ScanTablet";
      scan_rpc->done->Run();
      delete rpc;
    }
    return false;
  }
  return true;
}

void RemoteTabletNode::DoQuotaRetryScheduleRpc(RpcSchedule* rpc_schedule) {
  RpcTask* rpc = NULL;
  bool status = rpc_schedule->DequeueRpc(&rpc);
  CHECK(status);
  if (!DoQuotaScanRpcRetry(rpc)) {
    return;
  }
  CHECK(rpc->rpc_type == RPC_SCAN);
  ScanRpc* scan_rpc = (ScanRpc*)rpc;
  std::string table_name = scan_rpc->request->table_name();
  DoScanTablet(scan_rpc->controller, scan_rpc->request, scan_rpc->response, scan_rpc->done);
  delete rpc;
  status = rpc_schedule->FinishRpc(table_name);
  CHECK(status);
}

void RemoteTabletNode::ScanTablet(google::protobuf::RpcController* controller,
                                  const ScanTabletRequest* request, ScanTabletResponse* response,
                                  google::protobuf::Closure* done) {
  done = ScanDoneWrapper::NewInstance(get_micros(), request, response, done, quota_entry_);
  VLOG(8) << "accept RPC (ScanTablet): [" << request->table_name() << "] "
          << tera::utils::GetRemoteAddress(controller);
  scan_request_counter.Inc();
  if (scan_pending_counter.Get() > FLAGS_tera_scan_request_pending_limit) {
    response->set_sequence_id(request->sequence_id());
    response->set_status(kTabletNodeIsBusy);
    scan_reject_counter.Inc();
    done->Run();
    VLOG(8) << "finish RPC (ScanTablet)";
  } else {
    // check user identification & access
    if (!access_entry_->VerifyAndAuthorize(request, response)) {
      response->set_sequence_id(request->sequence_id());
      VLOG(20) << "Access VerifyAndAuthorize failed for ScanTablet";
      done->Run();
      return;
    }
    ScanRpc* rpc = new ScanRpc(controller, request, response, done);
    if (scan_pending_counter.Get() >=
        FLAGS_tera_request_pending_limit * FLAGS_tera_quota_unlimited_pending_ratio) {
      if (!DoQuotaScanRpcRetry(rpc)) {
        VLOG(8) << "ScanTablet Rpc push to QuotaRetry queue";
        return;
      }
    }
    scan_pending_counter.Inc();
    scan_rpc_schedule_->EnqueueRpc(request->table_name(), rpc);
    scan_thread_pool_->AddTask(
        std::bind(&RemoteTabletNode::DoScheduleRpc, this, scan_rpc_schedule_.get()));
  }
}

void RemoteTabletNode::Query(google::protobuf::RpcController* controller,
                             const QueryRequest* request, QueryResponse* response,
                             google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  LOG(INFO) << "accept RPC (Query) id: " << id
            << ", src: " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback =
      std::bind(&RemoteTabletNode::DoQuery, this, controller, request, response, done);
  lightweight_ctrl_thread_pool_->AddPriorityTask(callback);
}

void RemoteTabletNode::CmdCtrl(google::protobuf::RpcController* controller,
                               const TsCmdCtrlRequest* request, TsCmdCtrlResponse* response,
                               google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  LOG(INFO) << "accept RPC (CmdCtrl) id: " << id << ", [" << request->command()
            << "] src: " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback =
      std::bind(&RemoteTabletNode::DoCmdCtrl, this, controller, request, response, done);
  lightweight_ctrl_thread_pool_->AddPriorityTask(callback);
}

void RemoteTabletNode::ComputeSplitKey(google::protobuf::RpcController* controller,
                                       const SplitTabletRequest* request,
                                       SplitTabletResponse* response,
                                       google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  LOG(INFO) << "accept RPC (ComputeSplitKey) id: " << id
            << ", src: " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback =
      std::bind(&RemoteTabletNode::DoComputeSplitKey, this, controller, request, response, done);
  lightweight_ctrl_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::CompactTablet(google::protobuf::RpcController* controller,
                                     const CompactTabletRequest* request,
                                     CompactTabletResponse* response,
                                     google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  LOG(INFO) << "accept RPC (CompactTablet) id: " << id
            << ", src: " << tera::utils::GetRemoteAddress(controller);
  compact_pending_counter.Inc();
  ThreadPool::Task callback =
      std::bind(&RemoteTabletNode::DoCompactTablet, this, controller, request, response, done);
  // Reject all manual compact request when slowdown mode triggered.
  if (TsWriteFlowController::Instance().InSlowdownMode()) {
    LOG(WARNING) << "compact fail: " << request->tablet_name()
                 << " due to slowdown write mode triggered.";
    response->set_sequence_id(request->sequence_id());
    response->set_status(kFlowControlLimited);

    done->Run();
    return;
  }
  compact_thread_pool_->AddTask(callback);
}

void RemoteTabletNode::Update(google::protobuf::RpcController* controller,
                              const UpdateRequest* request, UpdateResponse* response,
                              google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  LOG(INFO) << "accept RPC (Update) id: " << id
            << ", src: " << tera::utils::GetRemoteAddress(controller);
  ThreadPool::Task callback =
      std::bind(&RemoteTabletNode::DoUpdate, this, controller, request, response, done);
  lightweight_ctrl_thread_pool_->AddTask(callback);
}

std::string RemoteTabletNode::ProfilingLog() {
  return "ctrl " + lightweight_ctrl_thread_pool_->ProfilingLog() + " read " +
         read_thread_pool_->ProfilingLog() + " write " + write_thread_pool_->ProfilingLog() +
         " scan " + scan_thread_pool_->ProfilingLog() + " compact " +
         compact_thread_pool_->ProfilingLog();
}

void RemoteTabletNode::DoQueryTabletLoadStatus(google::protobuf::RpcController* controller,
                                               const LoadTabletRequest* request,
                                               LoadTabletResponse* response,
                                               google::protobuf::Closure* done) {
  const std::string& tablet_path = request->path();
  {
    std::lock_guard<std::mutex> lock(tablets_ctrl_mutex_);
    if (tablets_ctrl_status_.find(tablet_path) != tablets_ctrl_status_.end()) {
      response->set_status(static_cast<StatusCode>(tablets_ctrl_status_[tablet_path]));
      done->Run();
      return;
    }
  }

  const std::string& start_key = request->key_range().key_start();
  const std::string& end_key = request->key_range().key_end();
  StatusCode status = tabletnode_impl_->QueryTabletStatus(tablet_path, start_key, end_key);
  response->set_status(status);
  done->Run();
}

void RemoteTabletNode::DoQueryTabletUnloadStatus(google::protobuf::RpcController* controller,
                                                 const UnloadTabletRequest* request,
                                                 UnloadTabletResponse* response,
                                                 google::protobuf::Closure* done) {
  const std::string& tablet_path = request->path();
  {
    std::lock_guard<std::mutex> lock(tablets_ctrl_mutex_);
    if (tablets_ctrl_status_.find(tablet_path) != tablets_ctrl_status_.end()) {
      response->set_status(static_cast<StatusCode>(tablets_ctrl_status_[tablet_path]));
      done->Run();
      return;
    }
  }

  const std::string& start_key = request->key_range().key_start();
  const std::string& end_key = request->key_range().key_end();
  StatusCode status =
      tabletnode_impl_->QueryTabletStatus(request->tablet_name(), start_key, end_key);
  response->set_status(status);
  done->Run();
}

void RemoteTabletNode::DoLoadTablet(google::protobuf::RpcController* controller,
                                    const LoadTabletRequest* request, LoadTabletResponse* response,
                                    google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  LOG(INFO) << "run RPC (LoadTablet) id: " << id;
  {
    std::lock_guard<std::mutex> lock(tablets_ctrl_mutex_);
    tablets_ctrl_status_[request->path()] = TabletCtrlStatus::kCtrlOnLoad;
  }
  tabletnode_impl_->LoadTablet(request, response);
  {
    std::lock_guard<std::mutex> lock(tablets_ctrl_mutex_);
    tablets_ctrl_status_.erase(request->path());
  }
  LOG(INFO) << "finish RPC (LoadTablet) id: " << id;
  done->Run();
}

void RemoteTabletNode::DoUnloadTablet(google::protobuf::RpcController* controller,
                                      const UnloadTabletRequest* request,
                                      UnloadTabletResponse* response,
                                      google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  LOG(INFO) << "run RPC (UnloadTablet) id: " << id;
  std::string tablet_path;
  if (request->has_path()) {
    tablet_path = request->path();
    std::lock_guard<std::mutex> lock(tablets_ctrl_mutex_);
    tablets_ctrl_status_[tablet_path] = TabletCtrlStatus::kCtrlUnloading;
  }

  tabletnode_impl_->UnloadTablet(request, response);
  {
    std::lock_guard<std::mutex> lock(tablets_ctrl_mutex_);
    tablets_ctrl_status_.erase(tablet_path);
  }
  LOG(INFO) << "finish RPC (UnloadTablet) id: " << id;
  done->Run();
}

void RemoteTabletNode::DoReadTablet(google::protobuf::RpcController* controller,
                                    int64_t start_micros, const ReadTabletRequest* request,
                                    ReadTabletResponse* response, google::protobuf::Closure* done,
                                    ReadRpcTimer* timer) {
  VLOG(8) << "run RPC (ReadTablet)";
  int32_t row_num = request->row_info_list_size();
  read_pending_counter.Sub(row_num);

  bool is_read_timeout = false;
  if (request->has_client_timeout_ms()) {
    int64_t read_timeout = request->client_timeout_ms() * 1000;  // ms -> us
    int64_t detal = get_micros() - start_micros;
    if (detal > read_timeout) {
      LOG(WARNING) << "timeout, drop read request for:" << request->tablet_name()
                   << ", detal(in us):" << detal << ", read_timeout(in us):" << read_timeout;
      is_read_timeout = true;
    }
  }

  if (!is_read_timeout) {
    tabletnode_impl_->ReadTablet(start_micros, request, response, done, read_thread_pool_.get());
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
                                     WriteTabletResponse* response, google::protobuf::Closure* done,
                                     WriteRpcTimer* timer) {
  VLOG(8) << "run RPC (WriteTablet)";
  int32_t row_num = request->row_list_size();
  write_pending_counter.Sub(row_num);
  tabletnode_impl_->WriteTablet(request, response, done, timer);
  VLOG(8) << "finish RPC (WriteTablet)";
}

void RemoteTabletNode::DoScanTablet(google::protobuf::RpcController* controller,
                                    const ScanTabletRequest* request, ScanTabletResponse* response,
                                    google::protobuf::Closure* done) {
  VLOG(8) << "run RPC (ScanTablet)";
  tabletnode_impl_->ScanTablet(request, response, done);
  VLOG(8) << "finish RPC (ScanTablet)";
}

void RemoteTabletNode::DoQuery(google::protobuf::RpcController* controller,
                               const QueryRequest* request, QueryResponse* response,
                               google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  int64_t start_micros = get_micros();
  LOG(INFO) << "run RPC (Query) id: " << id;
  access_entry_->GetAccessUpdater().UpdateTs(request, response);

  // Reset Quota iif version dismatch
  quota_entry_->Update(request, response);

  tabletnode_impl_->Query(request, response, done);

  LOG(INFO) << "finish RPC (Query) id: " << id << ", cost " << (get_micros() - start_micros) / 1000
            << "ms.";
}

void RemoteTabletNode::DoCmdCtrl(google::protobuf::RpcController* controller,
                                 const TsCmdCtrlRequest* request, TsCmdCtrlResponse* response,
                                 google::protobuf::Closure* done) {
  uint64_t id = request->sequence_id();
  int64_t start_micros = get_micros();
  LOG(INFO) << "run RPC (CmdCtrl) id: " << id;
  tabletnode_impl_->CmdCtrl(request, response, done);
  LOG(INFO) << "finish RPC (CmdCtrl) id: " << id << ", cost "
            << (get_micros() - start_micros) / 1000 << "ms.";
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
                                const UpdateRequest* request, UpdateResponse* response,
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
      DoReadTablet(read_rpc->controller, read_rpc->start_micros, read_rpc->request,
                   read_rpc->response, read_rpc->done, read_rpc->timer);
    } break;
    case RPC_SCAN: {
      ScanRpc* scan_rpc = (ScanRpc*)rpc;
      table_name = scan_rpc->request->table_name();
      scan_pending_counter.Dec();
      DoScanTablet(scan_rpc->controller, scan_rpc->request, scan_rpc->response, scan_rpc->done);
    } break;
    default:
      abort();
  }

  delete rpc;
  status = rpc_schedule->FinishRpc(table_name);
  CHECK(status);
}

}  // namespace tabletnode
}  // namespace tera
