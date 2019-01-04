#pragma once
#include <google/protobuf/stubs/common.h>

namespace tera {
class RequestDoneWrapper : public google::protobuf::Closure {
 public:
  static google::protobuf::Closure* NewInstance(google::protobuf::Closure* done) {
    return new RequestDoneWrapper(done);
  }

  // Self-Deleted, never access it after Run();
  // Default do nothing;
  virtual void Run() override { delete this; }

  virtual ~RequestDoneWrapper() { done_->Run(); }

 protected:
  // Can Only Create on Heap;
  RequestDoneWrapper(google::protobuf::Closure* done) : done_(done) {}

 private:
  google::protobuf::Closure* done_;
};
}