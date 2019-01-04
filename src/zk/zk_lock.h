// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: likang01(com@baidu.com)

#ifndef TERA_ZK_ZK_LOCK_H_
#define TERA_ZK_ZK_LOCK_H_

#include <stdint.h>
#include <queue>
#include <string>
#include <sys/time.h>

namespace tera {
namespace zk {

struct SeqNode {
  std::string name;
  int32_t seq;
};

class SeqNodeComp {
 public:
  bool operator()(const SeqNode& i, const SeqNode& j) { return i.seq >= j.seq; }
};

typedef void (*LOCK_CALLBACK)(const std::string& path, int err, void* param);

class ZooKeeperAdapter;

class ZooKeeperLock {
 public:
  ZooKeeperLock(ZooKeeperAdapter* adapter, const std::string& lock_path, LOCK_CALLBACK func,
                void* param);
  ~ZooKeeperLock();
  bool BeginLock(int* zk_errno);
  bool CancelLock(int* zk_errno);
  bool Unlock(int* zk_errno);
  bool IsAcquired() { return is_acquired_; }
  void OnWatchNodeDeleted(const std::string& path);
  bool CheckAndWatchNodeForLock(int* zk_errno);
  bool CheckSelfNodePath(const std::string& path);

 private:
  ZooKeeperAdapter* adapter_;
  std::string lock_path_;
  struct SeqNode self_node_;
  std::priority_queue<SeqNode, std::vector<SeqNode>, SeqNodeComp> node_list_;
  std::string watch_path_;
  pthread_mutex_t mutex_;

  volatile bool is_acquired_;
  LOCK_CALLBACK callback_func_;
  void* callback_param_;
};

struct LockCompletion {
 public:
  LockCompletion();
  ~LockCompletion();
  void SetLock(ZooKeeperLock* lock);
  bool Wait(int* zk_errno, const timeval* abs_time = NULL);
  void Signal(int err);

 private:
  ZooKeeperLock* lock_;
  int errno_;
  pthread_mutex_t mutex_;
  pthread_cond_t cond_;
};

}  // namespace zk
}  // namespace tera

#endif  // TERA_ZK_ZK_LOCK_H_
