// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: likang01(com@baidu.com)

#include "tera/zk/zk_lock.h"

#include "glog/logging.h"

#include "tera/zk/zk_util.h"
#include "tera/zk/zk_adapter.h"

namespace tera {
namespace zk {

ZooKeeperLock::ZooKeeperLock(ZooKeeperAdapter * adapter,
                             const std::string& lock_path, LOCK_CALLBACK func,
                             void * param)
    : m_adapter(adapter), m_lock_path(lock_path), m_is_acquired(false),
      m_callback_func(func), m_callback_param(param) {
    pthread_mutex_init(&m_mutex, NULL);
}

ZooKeeperLock::~ZooKeeperLock() {
    pthread_mutex_destroy(&m_mutex);
}

bool ZooKeeperLock::BeginLock(int* zk_errno) {
    // use session id as GUID
    // get session id
    int64_t session_id = -1;
    if (!m_adapter->GetSessionId(&session_id, zk_errno)) {
        SetZkAdapterCode(ZE_SESSION, zk_errno);
        return false;
    }
    char guid[17];
    sprintf(guid, "%016llx", session_id);
    LOG(INFO) << "lock GUID = " << guid;

    // get all lock nodes
    std::vector<std::string> child_list;
    std::vector<std::string> value_list;
    if (!m_adapter->ListChildren(m_lock_path, &child_list, &value_list,
                                 zk_errno)) {
        LOG(WARNING) << "list lock path fail : " << ZkErrnoToString(*zk_errno);
        return false;
    }

    // delete lock nodes with same GUID to avoid conflict
    *zk_errno = ZE_OK;
    std::vector<std::string>::iterator itor;
    for (itor = child_list.begin(); itor != child_list.end(); ++itor) {
        const std::string& name = *itor;
        if (name.size() > 16 && 0 == strncmp(name.c_str(), guid, 16)
            && name[16] == '#') {
            std::string child_path = m_lock_path + "/" + name;
            int zk_ret;
            if (!m_adapter->DeleteNode(child_path, &zk_ret)) {
                LOG(WARNING)<< "delete same GUID lock node fail : "
                    << ZkErrnoToString(*zk_errno);
                SetZkAdapterCode(zk_ret, zk_errno);
            }
        }
    }
    if (ZE_OK != *zk_errno) {
        return false;
    }

    // create lock node
    std::string lock_node_path = m_lock_path + "/" + guid + "#";
    std::string lock_node_data;
    m_adapter->GetId(&lock_node_data);
    std::string ret_path;
    if (!m_adapter->CreateSequentialEphemeralNode(lock_node_path, lock_node_data,
                                                  &ret_path, zk_errno)) {
        LOG(WARNING) << "create my lock node fail : " << ZkErrnoToString(*zk_errno);
        return false;
    }

    child_list.clear();
    value_list.clear();
    if (!m_adapter->ListChildren(m_lock_path, &child_list, &value_list,
                                 zk_errno)) {
        LOG(WARNING) << "list lock path fail : " << ZkErrnoToString(*zk_errno);
        return false;
    }
    if (child_list.size() == 0) {
        LOG(WARNING)<< "lock path is empty. where is my node?";
        SetZkAdapterCode(ZE_SYSTEM, zk_errno);
        return false;
    }

    const std::string& self_name = ret_path;
    int32_t self_seq_no = ZooKeeperUtil::GetSequenceNo(self_name);
    if (self_seq_no < 0) {
        LOG(WARNING) << "sequence node name is invalid";
        SetZkAdapterCode(ZE_SYSTEM, zk_errno);
        return false;
    }
    m_self_node.name = ZooKeeperUtil::GetNodeName(ret_path.c_str());
    m_self_node.seq = self_seq_no;

    for (itor = child_list.begin(); itor != child_list.end(); ++itor) {
        const std::string& name = *itor;
        int32_t seq_no = ZooKeeperUtil::GetSequenceNo(name);
        if (seq_no >= 0 && seq_no < self_seq_no) {
            struct SeqNode child = {name, seq_no};
            m_node_list.push(child);
        }
    }

    if (m_node_list.empty()) {
        LOG(INFO)<< "get lock success";
        m_is_acquired = true;
        m_callback_func(m_lock_path, ZE_OK, m_callback_param);
        SetZkAdapterCode(ZE_OK, zk_errno);
        return true;
    }

    //std::sort(m_node_list.begin(), m_node_list.end());

    do {
        m_watch_path = m_lock_path + "/" + m_node_list.top().name;
        bool is_exist;
        if (!m_adapter->CheckAndWatchExistForLock(m_watch_path, &is_exist,
                                                  zk_errno)) {
            return false;
        }
        if (is_exist) {
            SetZkAdapterCode(ZE_OK, zk_errno);
            return true;
        } else {
            m_node_list.pop();
        }
    } while (!m_node_list.empty());

    LOG(INFO) << "get lock success";
    m_is_acquired = true;
    m_callback_func(m_lock_path, ZE_OK, m_callback_param);
    SetZkAdapterCode(ZE_OK, zk_errno);
    return true;
}

bool ZooKeeperLock::CancelLock(int* zk_errno) {
    pthread_mutex_lock(&m_mutex);
    if (IsAcquired()) {
        pthread_mutex_unlock(&m_mutex);
        LOG(WARNING)<< "lock is acquired";
        SetZkAdapterCode(ZE_LOCK_ACQUIRED, zk_errno);
        return false;
    }

    pthread_mutex_unlock(&m_mutex);
    m_callback_func(m_lock_path, ZE_LOCK_CANCELED, m_callback_param);
    LOG(INFO)<< "unlock success";
    SetZkAdapterCode(ZE_OK, zk_errno);
    return true;
}

bool ZooKeeperLock::Unlock(int* zk_errno) {
    pthread_mutex_lock(&m_mutex);
    if (!IsAcquired()) {
        pthread_mutex_unlock(&m_mutex);
        LOG(WARNING) << "lock is not acquired";
        SetZkAdapterCode(ZE_LOCK_NOT_ACQUIRED, zk_errno);
        return false;
    }

    if (!m_adapter->DeleteNode(m_lock_path + "/" + m_self_node.name, zk_errno)) {
        pthread_mutex_unlock(&m_mutex);
        LOG(WARNING) << "unlock fail : " << ZkErrnoToString(*zk_errno);
        return false;
    }

    pthread_mutex_unlock(&m_mutex);
    LOG(INFO)<< "unlock success";
    SetZkAdapterCode(ZE_OK, zk_errno);
    return true;
}

void ZooKeeperLock::OnWatchNodeDeleted(const std::string& path) {
    pthread_mutex_lock(&m_mutex);
    if (IsAcquired()) {
        pthread_mutex_unlock(&m_mutex);
        return;
    }
    if (m_watch_path.compare(path) != 0) {
        pthread_mutex_unlock(&m_mutex);
        return;
    }
    LOG(INFO) << "node [" << path << "] is deleted";

    int zk_ret = ZE_OK;
    m_node_list.pop();
    while (!m_node_list.empty()) {
        m_watch_path = m_lock_path + "/" + m_node_list.top().name;
        bool is_exist;
        if (!m_adapter->CheckAndWatchExistForLock(m_watch_path, &is_exist,
                                                  &zk_ret)) {
            pthread_mutex_unlock(&m_mutex);
            m_callback_func(m_lock_path, zk_ret, m_callback_param);
            return;
        }
        if (is_exist) {
            pthread_mutex_unlock(&m_mutex);
            LOG(INFO) << "watch next node [" << m_watch_path << "]";
            return;
        } else {
            LOG(INFO) << "next node [" << m_watch_path << "] dead, skip";
            m_node_list.pop();
        }
    }

    m_is_acquired = true;
    LOG(INFO) << "get lock success";
    m_callback_func(m_lock_path, zk_ret, m_callback_param);
}

LockCompletion::LockCompletion()
    : m_lock(NULL), m_errno(ZE_OK) {
    pthread_mutex_init(&m_mutex, NULL);
    pthread_cond_init(&m_cond, NULL);
}

LockCompletion::~LockCompletion() {
    pthread_mutex_destroy(&m_mutex);
    pthread_cond_destroy(&m_cond);
}

void LockCompletion::SetLock(ZooKeeperLock * lock) {
    m_lock = lock;
}

bool LockCompletion::Wait(int* zk_errno, const timeval * end_time) {
    pthread_mutex_lock(&m_mutex);
    while (1) {
        if (m_lock->IsAcquired()) {
            pthread_mutex_unlock(&m_mutex);
            SetZkAdapterCode(ZE_OK, zk_errno);
            return true;
        } else if (m_errno != ZE_OK) {
            pthread_mutex_unlock(&m_mutex);
            SetZkAdapterCode(m_errno, zk_errno);
            return false;
        } else if (end_time != NULL) {
            struct timespec abs_time;
            abs_time.tv_sec = end_time->tv_sec;
            abs_time.tv_nsec = end_time->tv_usec * 1000;
            int err = pthread_cond_timedwait(&m_cond, &m_mutex, &abs_time);
            if (err == ETIMEDOUT && !m_lock->IsAcquired() && m_errno == ZE_OK) {
                pthread_mutex_unlock(&m_mutex);
                SetZkAdapterCode(ZE_LOCK_TIMEOUT, zk_errno);
                return false;
            }
        } else {
            pthread_cond_wait(&m_cond, &m_mutex);
        }
    }
}

void LockCompletion::Signal(int err) {
    pthread_mutex_lock(&m_mutex);
    m_errno = err;
    pthread_cond_signal(&m_cond);
    pthread_mutex_unlock(&m_mutex);
}

} // namespace zk
} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
