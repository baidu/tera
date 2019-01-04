// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/timeoracle_client_impl.h"
#include <mutex>
#include <memory>

#include "common/timer.h"

namespace tera {
namespace timeoracle {

TimeoracleClientImpl::TimeoracleClientImpl(ThreadPool* thread_pool,
                                           sdk::ClusterFinder* cluster_finder, int32_t rpc_timeout)
    : RpcClient<TimeoracleServer::Stub>(cluster_finder->TimeoracleAddr()),
      thread_pool_(thread_pool),
      rpc_timeout_(rpc_timeout),
      update_timestamp_(0),
      cluster_finder_(cluster_finder) {}

void TimeoracleClientImpl::refresh_timeoracle_address(int64_t last_timestamp) {
  std::unique_lock<std::mutex> lock_guard(mutex_);
  if (last_timestamp > 0 && last_timestamp < update_timestamp_) {
    return;
  }

  LOG(INFO) << "TimeoracleClientImpl try to update cluster, before is " << GetConnectAddr();
  std::string addr = cluster_finder_->TimeoracleAddr(true);
  ResetClient(addr);
  LOG(INFO) << "TimeoracleClientImpl update cluster, current is " << GetConnectAddr();
  update_timestamp_ = get_micros();
}

int64_t TimeoracleClientImpl::GetTimestamp(uint32_t count) {
  GetTimestampRequest request;
  GetTimestampResponse response;

  request.set_count(count);

  std::function<void(const GetTimestampRequest*, GetTimestampResponse*, bool, int)> done;

  if (SendMessageWithRetry(&TimeoracleServer::Stub::GetTimestamp, &request, &response, done,
                           "GetTimestamp", rpc_timeout_, thread_pool_)) {
    int code = response.status();
    if (code != kTimeoracleOk) {
      // Internel Error
      return 0;
    }
    return response.start_timestamp();
  }

  // Rpc Failed
  refresh_timeoracle_address(0);
  return 0;
}

bool TimeoracleClientImpl::GetTimestamp(uint32_t count, std::function<void(int64_t)> callback) {
  auto request = new GetTimestampRequest();
  auto response = new GetTimestampResponse();
  request->set_count(count);
  int64_t start_time = get_micros();

  std::function<void(const GetTimestampRequest*, GetTimestampResponse*, bool, int)> done =
      std::bind(&TimeoracleClientImpl::OnRpcFinished, this, start_time, callback,
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
                std::placeholders::_4);

  if (SendMessageWithRetry(&TimeoracleServer::Stub::GetTimestamp, request, response, done,
                           "GetTimestamp", rpc_timeout_, thread_pool_)) {
    return true;
  }

  // Rpc Failed
  refresh_timeoracle_address(0);
  return false;
}

void TimeoracleClientImpl::OnRpcFinished(int64_t start_time, std::function<void(int64_t)> callback,
                                         const GetTimestampRequest* request,
                                         GetTimestampResponse* response, bool rpc_error,
                                         int error_code) {
  std::unique_ptr<const GetTimestampRequest> req_hold(request);
  std::unique_ptr<GetTimestampResponse> res_hold(response);

  if (rpc_error) {
    LOG(ERROR) << "RpcRequest failed for GetTimestamp, errno=" << error_code;
    callback(0);
    refresh_timeoracle_address(start_time);
    return;
  }

  int64_t ts = response->start_timestamp();

  int code = response->status();

  if (code != kTimeoracleOk) {
    ts = 0;
  }

  callback(ts);
}

}  // namespace timeoracle
}  // namespace tera
