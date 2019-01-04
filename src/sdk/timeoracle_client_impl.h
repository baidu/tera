// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_TIMEORACLE_CLIENT_IMPL_H_
#define TERA_SDK_TIMEORACLE_CLIENT_IMPL_H_

#include <memory>
#include <mutex>
#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include "proto/timeoracle_rpc.pb.h"
#include "proto/rpc_client.h"
#include "sdk/sdk_zk.h"

DECLARE_int32(tera_rpc_timeout_period);

namespace tera {
namespace timeoracle {

class TimeoracleClientImpl : public RpcClient<TimeoracleServer::Stub> {
 public:
  TimeoracleClientImpl(ThreadPool* thread_pool, sdk::ClusterFinder* cluster_finder,
                       int32_t rpc_timeout = FLAGS_tera_rpc_timeout_period);

  ~TimeoracleClientImpl() {}

  int64_t GetTimestamp(uint32_t count);

  bool GetTimestamp(uint32_t count, std::function<void(int64_t)> callback);

 private:
  void refresh_timeoracle_address(int64_t last_timestamp);

  void OnRpcFinished(int64_t start_time, std::function<void(int64_t)> callback,
                     const GetTimestampRequest* request, GetTimestampResponse* response,
                     bool rpc_error, int error_code);

 private:
  ThreadPool* thread_pool_;
  int32_t rpc_timeout_;

  std::mutex mutex_;
  int64_t update_timestamp_;
  sdk::ClusterFinder* cluster_finder_;
};

}  // namespace timeoracle
}  // namespace tera

#endif  // TERA_SDK_TIMEORACLE_CLIENT_IMPL_H_
