// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_RPC_COMPACTOR_H_
#define TERA_TABLETNODE_RPC_COMPACTOR_H_

#include <glog/logging.h>

#include "common/base/scoped_ptr.h"
#include "common/mutex.h"
#include "proto/master_rpc.pb.h"
#include "proto/tabletnode_rpc.pb.h"

namespace tera {
namespace tabletnode {

template <class ResponseType>
class RpcCompactor {
 public:
  RpcCompactor()
      : last_success_sequence_id_(0),
        curr_sequence_id_(0),
        last_success_response_(new ResponseType()) {}
  ~RpcCompactor() {}

  bool RpcExceptionHappened(uint64_t request_sequence_id, ResponseType* response,
                            google::protobuf::Closure* done) {
    MutexLock lock(&mutex_);
    if (request_sequence_id < last_success_sequence_id_) {
      LOG(ERROR) << "invalid sequence id: " << request_sequence_id
                 << ", last succeed sequence is: " << last_success_sequence_id_;
      response->set_status(kInvalidSequenceId);
      done->Run();
      return false;
    } else if (request_sequence_id == last_success_sequence_id_) {
      response->CopyFrom(*last_success_response_);
      done->Run();
      return false;
    } else if (request_sequence_id < curr_sequence_id_) {
      LOG(ERROR) << "invalid sequence id: " << request_sequence_id
                 << ", current is: " << curr_sequence_id_;
      response->set_status(kInvalidSequenceId);
      done->Run();
    } else if (request_sequence_id == curr_sequence_id_) {
      LOG(WARNING) << "same sequence id: " << request_sequence_id;
      ResponseNode response_node(response, done);
      done_list_.push_back(response_node);
      return true;
    } else {
      LOG(WARNING) << "sequence id: " << request_sequence_id
                   << " should not larger than current: " << curr_sequence_id_;
    }
    CHECK(done_list_.size() == 0);
    curr_sequence_id_ = request_sequence_id;
    ResponseNode response_node(response, done);
    done_list_.push_back(response_node);

    return true;
  }

  void FillResponseAndDone(ResponseType* response) {
    MutexLock lock(&mutex_);
    last_success_response_->CopyFrom(*response);
    last_success_sequence_id_ = curr_sequence_id_;

    for (uint32_t i = 0; i < done_list_.size(); ++i) {
      done_list_[i].first->CopyFrom(*response);
      done_list_[i].second->Run();
    }
    done_list_.clear();
  }

 private:
  typedef std::pair<ResponseType*, google::protobuf::Closure*> ResponseNode;

  mutable Mutex mutex_;
  std::vector<ResponseNode> done_list_;
  uint64_t last_success_sequence_id_;
  uint64_t curr_sequence_id_;
  scoped_ptr<ResponseType> last_success_response_;
};

}  // namespace tabletnode
}  // namespace tera

#endif  // TERA_TABLETNODE_RPC_COMPACTOR_H_
