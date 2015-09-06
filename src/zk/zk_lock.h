// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: likang01(com@baidu.com)

#ifndef  TERA_ZK_ZK_LOCK_H_
#define  TERA_ZK_ZK_LOCK_H_

#include <stdint.h>
#include <queue>
#include <string>
#include <sys/time.h>

namespace tera {
namespace zk {

struct SeqNode
{
    std::string name;
    int32_t seq;
};

class SeqNodeComp
{
public:
    bool operator() (const SeqNode & i, const SeqNode & j)
    {
        return i.seq >= j.seq;
    }
};

typedef void (*LOCK_CALLBACK)(const std::string& path, int err, void * param);

class ZooKeeperAdapter;

class ZooKeeperLock
{
public:
    ZooKeeperLock(ZooKeeperAdapter * adapter, const std::string& lock_path,
                  LOCK_CALLBACK func, void * param);
    ~ZooKeeperLock();
    bool BeginLock(int* zk_errno);
    bool CancelLock(int* zk_errno);
    bool Unlock(int* zk_errno);
    bool IsAcquired() {return m_is_acquired;}
    void OnWatchNodeDeleted(const std::string& path);

private:
    ZooKeeperAdapter * m_adapter;
    std::string m_lock_path;
    struct SeqNode m_self_node;
    std::priority_queue<SeqNode, std::vector<SeqNode>, SeqNodeComp> m_node_list;
    std::string m_watch_path;
    pthread_mutex_t m_mutex;

    volatile bool m_is_acquired;
    LOCK_CALLBACK m_callback_func;
    void * m_callback_param;
};

struct LockCompletion
{
public:
    LockCompletion();
    ~LockCompletion();
    void SetLock(ZooKeeperLock * lock);
    bool Wait(int* zk_errno, const timeval * abs_time = NULL);
    void Signal(int err);

private:
    ZooKeeperLock * m_lock;
    int m_errno;
    pthread_mutex_t m_mutex;
    pthread_cond_t m_cond;
};

} // namespace zk
} // namespace tera

#endif  // TERA_ZK_ZK_LOCK_H_
