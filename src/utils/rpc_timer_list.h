// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_UTILS_RPC_TIMER_LIST_H_
#define TERA_UTILS_RPC_TIMER_LIST_H_

#include "common/mutex.h"
#include "types.h"

namespace google {
namespace protobuf {
class Closure;
}
}

namespace tera {

class WriteTabletRequest;
class WriteTabletResponse;
class ReadTabletRequest;
class ReadTabletResponse;

struct RpcTimer {
  RpcTimer* prev;
  RpcTimer* next;
  int64_t time;

  RpcTimer(int64_t t) : prev(NULL), next(NULL), time(t) {}
  virtual ~RpcTimer() {}
};

template <typename REQ, typename RESP>
struct SpecRpcTimer : public RpcTimer {
  const REQ* request;
  RESP* response;
  google::protobuf::Closure* done;

  SpecRpcTimer(const REQ* req, RESP* resp, google::protobuf::Closure* d, int64_t t)
      : RpcTimer(t), request(req), response(resp), done(d) {}
  virtual ~SpecRpcTimer() {}
};

typedef SpecRpcTimer<WriteTabletRequest, WriteTabletResponse> WriteRpcTimer;
typedef SpecRpcTimer<ReadTabletRequest, ReadTabletResponse> ReadRpcTimer;

class RpcTimerList {
 public:
  RpcTimerList();
  ~RpcTimerList();

  static RpcTimerList* Instance();

  bool TopTime(int64_t* time);

  void Push(RpcTimer* item);

  void Erase(RpcTimer* item);

  size_t Size();

 private:
  mutable Mutex mutex_;
  RpcTimer* head_;
  RpcTimer* tail_;
  size_t size_;
  static RpcTimerList* s_instance;
};

}  // namespace tera

#endif  // TERA_UTILS_RPC_TIMER_LIST_H_
