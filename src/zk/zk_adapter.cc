// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: likang01(com@baidu.com)

#include "zk/zk_adapter.h"

#include <errno.h>

#include <boost/bind.hpp>
#include <glog/logging.h>

#include "common/this_thread.h"

namespace tera {
namespace zk {

const int32_t kMaxNodeDataLen = 10240;

FILE* ZooKeeperAdapter::m_lib_log_output = NULL;
Mutex ZooKeeperAdapter::m_lib_log_mutex;

struct ZooKeeperWatch {
    pthread_mutex_t mutex;
    bool watch_value;
    bool watch_exist;
    bool watch_child;

    ZooKeeperWatch()
        : watch_value(false), watch_exist(false), watch_child(false) {
        pthread_mutex_init(&mutex, NULL);
    }

    ~ZooKeeperWatch() {
        pthread_mutex_destroy(&mutex);
    }
};

ZooKeeperAdapter::ZooKeeperAdapter()
    : m_handle(NULL), m_state(ZS_DISCONN), m_session_id(-1),
      m_state_cond(&m_state_mutex), m_session_timeout(0), m_session_timer_id(0),
      m_thread_pool(1) {
}

ZooKeeperAdapter::~ZooKeeperAdapter() {
    Finalize();
}

bool ZooKeeperAdapter::Init(const std::string& server_list,
                            const std::string& root_path,
                            uint32_t session_timeout,
                            const std::string& id,
                            int* zk_errno) {
    MutexLock mutex(&m_state_mutex);

    if (NULL != m_handle) {
        SetZkAdapterCode(ZE_INITED, zk_errno);
        return false;
    }

    m_server_list = server_list;
    m_root_path = root_path;
    if (*m_root_path.end() == '/') {
        m_root_path.resize(m_root_path.size() - 1);
    }
    m_id = id;

    m_handle = zookeeper_init((m_server_list + m_root_path).c_str(),
                              EventCallBack, session_timeout, NULL, this, 0);
    if (NULL == m_handle) {
        LOG(ERROR) << "zookeeper_init fail : " << zerror(errno);
        SetZkAdapterCode(ZE_SESSION, zk_errno);
        return false;
    }

    while (m_state == ZS_DISCONN || m_state == ZS_CONNECTING) {
        m_state_cond.Wait();
    }

    int code = ZE_OK;
    // succe
    if (m_state == ZS_CONNECTED) {
        pthread_rwlock_init(&m_watcher_lock, NULL);
        pthread_rwlock_init(&m_locks_lock, NULL);

        LOG(INFO) << "zookeeper_init success";
        SetZkAdapterCode(code, zk_errno);
        return true;
    }

    // fail
    if (m_state == ZS_TIMEOUT) {
        code = ZE_SESSION;
    } else if (m_state == ZS_AUTH) {
        code = ZE_AUTH;
    } else {
        code = ZE_UNKNOWN;
    }
    zookeeper_close(m_handle);
    m_handle = NULL;
    m_state = ZS_DISCONN;

    LOG(ERROR) << "zookeeper_init fail : " << ZkErrnoToString(code);
    SetZkAdapterCode(code, zk_errno);
    return false;
}

void ZooKeeperAdapter::Finalize() {
    zhandle_t* old_handle;
    {
        MutexLock mutex(&m_state_mutex);
        if (NULL == m_handle) {
            return;
        }
        old_handle = m_handle;
        m_handle = NULL;
    }
    int ret = zookeeper_close(old_handle);
    if (ret == ZOK) {
        LOG(INFO) << "zookeeper_close success";
    } else {
        LOG(ERROR) << "zookeeper_close fail : " << zerror(ret);
    }
    {
        MutexLock mutex(&m_state_mutex);
        pthread_rwlock_destroy(&m_locks_lock);
        pthread_rwlock_destroy(&m_watcher_lock);
        m_locks.clear();
        m_watchers.clear();
        m_state = ZS_DISCONN;
        m_thread_pool.CancelTask(m_session_timer_id);
        m_session_timer_id = 0;
        LOG(INFO) << "zookeeper_session_timeout_timer has gone, safe to finalize.";
    }
}

bool ZooKeeperAdapter::CreatePersistentNode(const std::string& path,
                                            const std::string& value,
                                            int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    return Create(path, value, 0, NULL, zk_errno);
}

bool ZooKeeperAdapter::CreateEphemeralNode(const std::string& path,
                                           const std::string& value,
                                           int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    return Create(path, value, ZOO_EPHEMERAL, NULL, zk_errno);
}

bool ZooKeeperAdapter::CreateSequentialEphemeralNode(const std::string& path,
                                                     const std::string& value,
                                                     std::string* ret_path,
                                                     int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    return Create(path, value, ZOO_EPHEMERAL | ZOO_SEQUENCE, ret_path, zk_errno);
}

bool ZooKeeperAdapter::Create(const std::string& path, const std::string& value,
                              int flag, std::string* ret_path, int* zk_errno) {
    m_state_mutex.AssertHeld();
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    int value_len = value.size();
    if (value_len == 0) {
        value_len = -1;
    }

    size_t root_path_len = m_root_path.size();
    size_t path_len = path.size();
    char * ret_path_buf = NULL;
    size_t ret_path_size = 0;
    if (ret_path != NULL) {
        ret_path_size = root_path_len + path_len + 11;
        ret_path_buf = new char[ret_path_size];
    }

    int ret = zoo_create(m_handle, path.c_str(), value.c_str(), value_len,
                         &ZOO_OPEN_ACL_UNSAFE, flag, ret_path_buf,
                         ret_path_size);
    if (ZOK == ret) {
        if (NULL != ret_path) {
            size_t ret_path_len = strlen(ret_path_buf);
            if (((flag & ZOO_SEQUENCE) == 1 &&
                ret_path_len == root_path_len + path_len + 10) ||
                ((flag & ZOO_SEQUENCE) == 0 &&
                ret_path_len == root_path_len + path_len)) {
                // compatible to zk 3.3.x
                *ret_path = ret_path_buf + root_path_len;
            } else {
                *ret_path = ret_path_buf;
            }
        }
        LOG(INFO) << "zoo_create success";
    } else {
        LOG(WARNING) << "zoo_create fail : " << zerror(ret);
    }

    if (NULL != ret_path_buf) {
        delete[] ret_path_buf;
    }

    switch (ret) {
        case ZOK:
            SetZkAdapterCode(ZE_OK, zk_errno);
            return true;
        case ZNONODE:
            SetZkAdapterCode(ZE_NO_PARENT, zk_errno);
            return false;
        case ZNODEEXISTS:
            SetZkAdapterCode(ZE_EXIST, zk_errno);
            return false;
        case ZNOAUTH:
            SetZkAdapterCode(ZE_AUTH, zk_errno);
            return false;
        case ZNOCHILDRENFOREPHEMERALS:
            SetZkAdapterCode(ZE_ENTITY_PARENT, zk_errno);
            return false;
        case ZBADARGUMENTS:
            SetZkAdapterCode(ZE_ARG, zk_errno);
            return false;
        case ZINVALIDSTATE:
            SetZkAdapterCode(ZE_SESSION, zk_errno);
            return false;
        case ZMARSHALLINGERROR:
            SetZkAdapterCode(ZE_SYSTEM, zk_errno);
            return false;
        default:
            SetZkAdapterCode(ZE_UNKNOWN, zk_errno);
            return false;
    }
}

bool ZooKeeperAdapter::DeleteNode(const std::string& path, int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    int ret = zoo_delete(m_handle, path.c_str(), -1);
    if (ZOK == ret) {
        LOG(INFO) << "zoo_delete success";
    } else {
        LOG(WARNING) << "zoo_delete fail : " << zerror(ret);
    }

    switch (ret) {
        case ZOK:
            SetZkAdapterCode(ZE_OK, zk_errno);
            return true;
        case ZNONODE:
            SetZkAdapterCode(ZE_NOT_EXIST, zk_errno);
            return false;
        case ZNOAUTH:
            SetZkAdapterCode(ZE_AUTH, zk_errno);
            return false;
        case ZBADVERSION: // impossible
            SetZkAdapterCode(ZE_UNKNOWN, zk_errno);
            return false;
        case ZNOTEMPTY:
            SetZkAdapterCode(ZE_HAS_CHILD, zk_errno);
            return false;
        case ZBADARGUMENTS:
            SetZkAdapterCode(ZE_ARG, zk_errno);
            return false;
        case ZINVALIDSTATE:
            SetZkAdapterCode(ZE_SESSION, zk_errno);
            return false;
        case ZMARSHALLINGERROR:
            SetZkAdapterCode(ZE_SYSTEM, zk_errno);
            return false;
        default:
            SetZkAdapterCode(ZE_UNKNOWN, zk_errno);
            return false;
    }
}

bool ZooKeeperAdapter::ReadNode(const std::string& path, std::string* value,
                                int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    int ret = GetWrapper(path, false, value);
    SetZkAdapterCode(ret, zk_errno);
    return (ZE_OK == ret);
}

bool ZooKeeperAdapter::ReadAndWatchNode(const std::string& path,
                                        std::string* value, int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    pthread_rwlock_wrlock(&m_watcher_lock);
    std::pair<WatcherMap::iterator, bool> insert_ret = m_watchers.insert(
        std::pair<std::string, ZooKeeperWatch*>(path, NULL));
    struct ZooKeeperWatch*& watch = insert_ret.first->second;
    if (NULL == watch) {
        watch = new ZooKeeperWatch;
    }
    pthread_mutex_lock(&watch->mutex);
    pthread_rwlock_unlock(&m_watcher_lock);

    bool is_watch = false;
    if (!watch->watch_value) {
        is_watch = true;
    } else {
        pthread_mutex_unlock(&watch->mutex);
        LOG(INFO) << "watch has been set before";
    }

    int ret = GetWrapper(path, is_watch, value);
    if (ZE_OK == ret) {
        if (is_watch) {
            watch->watch_value = true;
            pthread_mutex_unlock(&watch->mutex);
        }
        SetZkAdapterCode(ZE_OK, zk_errno);
        return true;
    } else {
        if (is_watch) {
            pthread_mutex_unlock(&watch->mutex);
        }
        SetZkAdapterCode(ret, zk_errno);
        return false;
    }
}

bool ZooKeeperAdapter::ListChildren(const std::string& path,
                                    std::vector<std::string>* child_list,
                                    std::vector<std::string>* value_list,
                                    int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    int ret = GetChildrenWrapper(path, false, child_list, value_list);
    SetZkAdapterCode(ret, zk_errno);
    return (ZE_OK == ret);
}

bool ZooKeeperAdapter::ListAndWatchChildren(const std::string& path,
                                            std::vector<std::string>* child_list,
                                            std::vector<std::string>* value_list,
                                            int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    pthread_rwlock_wrlock(&m_watcher_lock);
    std::pair<WatcherMap::iterator, bool> insert_ret = m_watchers.insert(
        std::pair<std::string, ZooKeeperWatch*>(path, NULL));
    struct ZooKeeperWatch*& watch = insert_ret.first->second;
    if (NULL == watch) {
        watch = new ZooKeeperWatch;
    }
    pthread_mutex_lock(&watch->mutex);
    pthread_rwlock_unlock(&m_watcher_lock);

    bool is_watch = false;
    if (!watch->watch_child) {
        is_watch = true;
    } else {
        pthread_mutex_unlock(&watch->mutex);
        LOG(INFO)<< "is_watch has been set before";
    }

    int ret = GetChildrenWrapper(path, is_watch, child_list, value_list);
    if (ZE_OK == ret) {
        if (is_watch) {
            watch->watch_child = true;
            pthread_mutex_unlock(&watch->mutex);
        }
        SetZkAdapterCode(ret, zk_errno);
        return true;
    } else {
        if (is_watch) {
            pthread_mutex_unlock(&watch->mutex);
        }
        SetZkAdapterCode(ret, zk_errno);
        return false;
    }
}

bool ZooKeeperAdapter::CheckExist(const std::string&path, bool* is_exist,
                                  int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    int ret = ExistsWrapper(path, false, is_exist);
    SetZkAdapterCode(ret, zk_errno);
    return (ZE_OK == ret);
}

bool ZooKeeperAdapter::CheckAndWatchExist(const std::string& path, bool* is_exist,
                                          int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    bool is_watch = false;
    pthread_rwlock_wrlock(&m_watcher_lock);
    std::pair<WatcherMap::iterator, bool> insert_ret = m_watchers.insert(
        std::pair<std::string, ZooKeeperWatch*>(path, NULL));
    struct ZooKeeperWatch*& watch = insert_ret.first->second;
    if (NULL == watch) {
        watch = new ZooKeeperWatch;
    }
    pthread_mutex_lock(&watch->mutex);
    pthread_rwlock_unlock(&m_watcher_lock);
    if (!watch->watch_exist) {
        is_watch = true;
    } else {
        pthread_mutex_unlock(&watch->mutex);
        LOG(INFO) << "is_watch has been set before";
    }

    int ret = ExistsWrapper(path, is_watch, is_exist);
    if (ZE_OK == ret) {
        if (is_watch) {
            watch->watch_exist = true;
            pthread_mutex_unlock(&watch->mutex);
        }
    } else {
        if (is_watch) {
            pthread_mutex_unlock(&watch->mutex);
        }
    }
    SetZkAdapterCode(ret, zk_errno);
    return (ZE_OK == ret);
}


bool ZooKeeperAdapter::CheckAndWatchExistForLock(const std::string& path,
                                                 bool* is_exist, int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    int ret = ExistsWrapperForLock(path, is_exist);
    SetZkAdapterCode(ret, zk_errno);
    return (ZE_OK == ret);
}

bool ZooKeeperAdapter::WriteNode(const std::string& path,
                                 const std::string& value, int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    int ret = zoo_set(m_handle, path.c_str(), value.c_str(), value.size(), -1);
    if (ZOK == ret) {
        LOG(INFO) << "zoo_set success";
    } else {
        LOG(WARNING) << "zoo_set fail : " << zerror(ret);
    }

    switch (ret) {
        case ZOK:
            SetZkAdapterCode(ZE_OK, zk_errno);
            return true;
        case ZNONODE:
            SetZkAdapterCode(ZE_NOT_EXIST, zk_errno);
            return false;
        case ZNOAUTH:
            SetZkAdapterCode(ZE_AUTH, zk_errno);
            return false;
        case ZBADVERSION:  // impossible
            SetZkAdapterCode(ZE_UNKNOWN, zk_errno);
            return false;
        case ZBADARGUMENTS:
            SetZkAdapterCode(ZE_ARG, zk_errno);
            return false;
        case ZINVALIDSTATE:
            SetZkAdapterCode(ZE_SESSION, zk_errno);
            return false;
        case ZMARSHALLINGERROR:
            SetZkAdapterCode(ZE_SYSTEM, zk_errno);
            return false;
        default:
            SetZkAdapterCode(ZE_UNKNOWN, zk_errno);
            return false;
    }
}

void ZooKeeperAdapter::EventCallBack(zhandle_t * zh, int type, int state,
                                     const char * node_path, void * watch_ctx) {
    VLOG(5) << "recv event: type=" << ZooTypeToString(type) << ", state="
        << ZooStateToString(state) << ", path=[" << node_path << "]";

    if (NULL == watch_ctx) {
        return;
    }
    ZooKeeperAdapter* zk_adapter = (ZooKeeperAdapter*)watch_ctx;

    MutexLock mutex(&zk_adapter->m_state_mutex);
    if (zh != zk_adapter->m_handle) {
        LOG(WARNING)<< "zhandle not match";
        return;
    }
    // m_handle is guaranteed (by zk lib) to be valid within callback func.
    // no need to check it.

    if (ZOO_SESSION_EVENT == type) {
        zk_adapter->SessionEventCallBack(state);
        return;
    }

    if (NULL == node_path) {
        LOG(WARNING) << "path is missing";
        return;
    }

    std::string path = node_path;
    if (!ZooKeeperUtil::IsValidPath(path)) {
        LOG(WARNING) << "path is invalid";
        return;
    }

    if (ZOO_CREATED_EVENT == type) {
        zk_adapter->CreateEventCallBack(path);
    } else if (ZOO_DELETED_EVENT == type) {
        zk_adapter->DeleteEventCallBack(path);
    } else if (ZOO_CHANGED_EVENT == type) {
        zk_adapter->ChangeEventCallBack(path);
    } else if (ZOO_CHILD_EVENT == type) {
        zk_adapter->ChildEventCallBack(path);
    } else if (ZOO_NOTWATCHING_EVENT == type) {
        zk_adapter->WatchLostEventCallBack(state, path);
    } else {
        LOG(WARNING) << "unknown event type : " << type;
    }
}

void ZooKeeperAdapter::CreateEventCallBack(std::string path) {
    VLOG(5) << "CreateEventCallBack: path=[" << path << "]";

    pthread_rwlock_wrlock(&m_watcher_lock);
    WatcherMap::iterator itor = m_watchers.find(path);
    if (itor == m_watchers.end()) {
        pthread_rwlock_unlock(&m_watcher_lock);
        LOG(INFO) << "watch not match";
        return;
    }

    ZooKeeperWatch * watch = itor->second;
    pthread_mutex_lock(&watch->mutex);
    pthread_rwlock_unlock(&m_watcher_lock);
    if (!watch->watch_exist) {
        pthread_mutex_unlock(&watch->mutex);
        LOG(WARNING) << "watch not match";
        return;
    }

    bool is_exist;
    int ret = ExistsWrapper(path, true, &is_exist);
    if (ZE_OK == ret) {
        pthread_mutex_unlock(&watch->mutex);
        m_state_mutex.Unlock();
        OnNodeCreated(path);
        if (!is_exist) {
            OnNodeDeleted(path);
        }
        m_state_mutex.Lock();
    } else {
        watch->watch_exist = false;
        pthread_mutex_unlock(&watch->mutex);
        TryCleanWatch(path);
        m_state_mutex.Unlock();
        OnWatchFailed(path, ZT_WATCH_EXIST, ret);
        m_state_mutex.Lock();
    }
}

void ZooKeeperAdapter::DeleteEventCallBack(std::string path) {
    VLOG(5) << "DeleteEventCallBack: path=[" << path << "]";

    pthread_rwlock_wrlock(&m_watcher_lock);
    WatcherMap::iterator itor = m_watchers.find(path);
    if (itor == m_watchers.end()) {
        pthread_rwlock_unlock(&m_watcher_lock);
        LOG(INFO) << "watch not match";
        return;
    }

    ZooKeeperWatch * watch = itor->second;
    pthread_mutex_lock(&watch->mutex);
    pthread_rwlock_unlock(&m_watcher_lock);

    if (!watch->watch_exist && !watch->watch_value && !watch->watch_child) {
        pthread_mutex_unlock(&watch->mutex);
        LOG(WARNING) << "watch not match";
        return;
    }

    bool is_watch_exist = watch->watch_exist;
    bool is_exist;
    int ret = ExistsWrapper(path, true, &is_exist);
    if (ZE_OK == ret) {
        watch->watch_value = false;
        watch->watch_child = false;
        pthread_mutex_unlock(&watch->mutex);
        if (!is_watch_exist) {
            TryCleanWatch(path);
        }
        m_state_mutex.Unlock();
        OnNodeDeleted(path);
        if (is_exist && is_watch_exist) {
            OnNodeCreated(path);
        }
        m_state_mutex.Lock();
    } else {
        watch->watch_exist = false;
        watch->watch_value = false;
        watch->watch_child = false;
        pthread_mutex_unlock(&watch->mutex);
        TryCleanWatch(path);
        m_state_mutex.Unlock();
        OnNodeDeleted(path);
        if (is_watch_exist) {
            OnWatchFailed(path, ZT_WATCH_EXIST, ret);
        }
        m_state_mutex.Lock();
    }
}

void ZooKeeperAdapter::ChangeEventCallBack(std::string path) {
    VLOG(5) << "ChangeEventCallBack: path=[" << path << "]";

    pthread_rwlock_wrlock(&m_watcher_lock);
    WatcherMap::iterator itor = m_watchers.find(path);
    if (itor == m_watchers.end()) {
        pthread_rwlock_unlock(&m_watcher_lock);
        LOG(INFO) << "watch not match";
        return;
    }

    ZooKeeperWatch * watch = itor->second;
    pthread_mutex_lock(&watch->mutex);
    pthread_rwlock_unlock(&m_watcher_lock);

    if (!watch->watch_value) {
        pthread_mutex_unlock(&watch->mutex);
        LOG(WARNING) << "watch not match";
        return;
    }

    std::string value;
    int ret = GetWrapper(path, true, &value);
    if (ZE_OK == ret) {
        pthread_mutex_unlock(&watch->mutex);
        m_state_mutex.Unlock();
        OnNodeValueChanged(path, value);
        m_state_mutex.Lock();
    } else if (ZE_NOT_EXIST == ret) {
        watch->watch_value = false;
        watch->watch_child = false;
        pthread_mutex_unlock(&watch->mutex);
        TryCleanWatch(path);
        m_state_mutex.Unlock();
        OnNodeDeleted(path);
        m_state_mutex.Lock();
    } else {
        watch->watch_value = false;
        pthread_mutex_unlock(&watch->mutex);
        TryCleanWatch(path);
        m_state_mutex.Unlock();
        OnWatchFailed(path, ZT_WATCH_VALUE, ret);
        m_state_mutex.Lock();
    }
}

void ZooKeeperAdapter::ChildEventCallBack(std::string path) {
    VLOG(5) << "ChildEventCallBack: path=[" << path << "]";

    pthread_rwlock_wrlock(&m_watcher_lock);
    WatcherMap::iterator itor = m_watchers.find(path);
    if (itor == m_watchers.end()) {
        pthread_rwlock_unlock(&m_watcher_lock);
        LOG(INFO) << "watch not match";
        return;
    }

    ZooKeeperWatch * watch = itor->second;
    pthread_mutex_lock(&watch->mutex);
    pthread_rwlock_unlock(&m_watcher_lock);

    if (!watch->watch_child) {
        pthread_mutex_unlock(&watch->mutex);
        LOG(WARNING) << "watch not match";
        return;
    }

    std::vector<std::string> child_list;
    std::vector<std::string> value_list;
    int ret = GetChildrenWrapper(path, true, &child_list, &value_list);
    if (ZE_OK == ret) {
        pthread_mutex_unlock(&watch->mutex);
        m_state_mutex.Unlock();
        OnChildrenChanged(path, child_list, value_list);
        m_state_mutex.Lock();
    } else if (ZE_NOT_EXIST == ret) {
        watch->watch_child = false;
        watch->watch_value = false;
        pthread_mutex_unlock(&watch->mutex);
        TryCleanWatch(path);
        m_state_mutex.Unlock();
        OnNodeDeleted(path);
        m_state_mutex.Lock();
    } else {
        watch->watch_child = false;
        pthread_mutex_unlock(&watch->mutex);
        TryCleanWatch(path);
        m_state_mutex.Unlock();
        OnWatchFailed(path, ZT_WATCH_CHILD, ret);
        m_state_mutex.Lock();
    }
}

void ZooKeeperAdapter::SessionTimeoutWrapper() {
    this->OnSessionTimeout();
    MutexLock mutex(&m_state_mutex);
    m_session_timer_id = 0;
}

void ZooKeeperAdapter::SessionEventCallBack(int state) {
    if (ZOO_CONNECTED_STATE == state) {
        if (ZS_CONNECTING == m_state) {
            if (!m_thread_pool.CancelTask(m_session_timer_id)) {
                LOG(WARNING) << "session timeout timer is triggered";
                return;
            }
            m_session_timer_id = 0;
        }
        const clientid_t *cid = zoo_client_id(m_handle);
        if (cid == NULL) {
            LOG(WARNING) << "zoo_client_id fail";
            return;
        }
        m_session_id = cid->client_id;
        m_state = ZS_CONNECTED;
        m_state_cond.Signal();
        m_session_timeout = zoo_recv_timeout(m_handle);
        LOG(INFO) << "connected to zk server, session timeout: "
            << m_session_timeout << " ms";
    } else if (ZOO_CONNECTING_STATE == state || ZOO_ASSOCIATING_STATE == state) {
        if (ZS_CONNECTED == m_state) {
            LOG(INFO) << "disconnect from zk server, enable timer: "
                << m_session_timeout << " ms";
            ThreadPool::Task task =
                boost::bind(&ZooKeeperAdapter::SessionTimeoutWrapper, this);
            m_session_timer_id = m_thread_pool.DelayTask(m_session_timeout, task);
        }
        m_session_id = -1;
        m_state = ZS_CONNECTING;
        m_state_cond.Signal();
    } else if (ZOO_AUTH_FAILED_STATE == state) {
        m_session_id = -1;
        m_state = ZS_AUTH;
        m_state_cond.Signal();
    } else if (ZOO_EXPIRED_SESSION_STATE == state) {
        m_session_id = -1;
        m_state = ZS_TIMEOUT;
        m_state_cond.Signal();
        m_state_mutex.Unlock();
        OnSessionTimeout();
        m_state_mutex.Lock();
    }
}

void ZooKeeperAdapter::WatchLostEventCallBack(int state, std::string path) {
    // shit...
}

bool ZooKeeperAdapter::SyncLock(const std::string& path, int* zk_errno,
                                int32_t timeout) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }
    bool ret_val;

    pthread_rwlock_wrlock(&m_locks_lock);
    std::pair<LockMap::iterator, bool> insert_ret = m_locks.insert(
        std::pair<std::string, ZooKeeperLock*>(path, NULL));
    if (!insert_ret.second) {
        ZooKeeperLock * lock = insert_ret.first->second;
        if (lock == NULL || !lock->IsAcquired()) {
            LOG(INFO) << "lock exists but is not acquired";
        } else {
            LOG(INFO) << "lock has been acquired";
        }
        pthread_rwlock_unlock(&m_locks_lock);
        SetZkAdapterCode(ZE_LOCK_EXIST, zk_errno);
        return false;
    }
    pthread_rwlock_unlock(&m_locks_lock);

    timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    end_time.tv_sec = start_time.tv_sec + timeout;
    end_time.tv_usec = start_time.tv_usec;

    LockCompletion * callback_param = new LockCompletion();
    ZooKeeperLock * lock = new ZooKeeperLock(this, path, SyncLockCallback,
                                             callback_param);
    callback_param->SetLock(lock);

    m_state_mutex.Unlock();
    if (!lock->BeginLock(zk_errno)) {
        m_state_mutex.Lock();
        delete callback_param;
        delete lock;
        pthread_rwlock_wrlock(&m_locks_lock);
        m_locks.erase(path);
        pthread_rwlock_unlock(&m_locks_lock);
        return false;
    }
    m_state_mutex.Lock();

    pthread_rwlock_wrlock(&m_locks_lock);
    m_locks[path] = lock;
    pthread_rwlock_unlock(&m_locks_lock);

    timeval now_time;
    gettimeofday(&now_time, NULL);
    if (timeout > 0 && (now_time.tv_sec > end_time.tv_sec
        || (now_time.tv_sec == end_time.tv_sec && now_time.tv_usec
            > end_time.tv_usec))) {
        if (lock->IsAcquired()) {
            SetZkAdapterCode(ZE_OK, zk_errno);
            return true;
        } else {
            SetZkAdapterCode(ZE_LOCK_TIMEOUT, zk_errno);
            return false;
        }
    }

    m_state_mutex.Unlock();
    if (timeout > 0) {
        ret_val = callback_param->Wait(zk_errno, &end_time);
    } else {
        ret_val = callback_param->Wait(zk_errno);
    }
    m_state_mutex.Lock();
    return ret_val;
}

bool ZooKeeperAdapter::AsyncLock(const std::string& path,
                                 LOCK_CALLBACK callback_func,
                                 void * callback_param, int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    pthread_rwlock_wrlock(&m_locks_lock);
    std::pair<LockMap::iterator, bool> insert_ret = m_locks.insert(
        std::pair<std::string, ZooKeeperLock*>(path, NULL));
    if (!insert_ret.second) {
        ZooKeeperLock * lock = insert_ret.first->second;
        if (lock == NULL || !lock->IsAcquired()) {
            LOG(INFO) << "lock exists but is not acquired";
        } else {
            LOG(INFO) << "lock has been acquired";
        }
        pthread_rwlock_unlock(&m_locks_lock);
        SetZkAdapterCode(ZE_LOCK_EXIST, zk_errno);
        return false;
    }
    pthread_rwlock_unlock(&m_locks_lock);
    ZooKeeperLock * lock = new ZooKeeperLock(this, path, callback_func,
                                             callback_param);
    m_state_mutex.Unlock();
    if (!lock->BeginLock(zk_errno)) {
        m_state_mutex.Lock();
        pthread_rwlock_wrlock(&m_locks_lock);
        m_locks.erase(path);
        pthread_rwlock_unlock(&m_locks_lock);
        delete lock;
        return false;
    } else {
        m_state_mutex.Lock();
        pthread_rwlock_wrlock(&m_locks_lock);
        m_locks[path] = lock;
        pthread_rwlock_unlock(&m_locks_lock);
        return true;
    }
}

void ZooKeeperAdapter::SyncLockCallback(const std::string& path, int err,
                                        void * param) {
    LockCompletion * comp = (LockCompletion *) param;
    comp->Signal(err);
}

bool ZooKeeperAdapter::CancelLock(const std::string& path, int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    pthread_rwlock_wrlock(&m_locks_lock);
    LockMap::iterator itor = m_locks.find(path);
    if (itor == m_locks.end()) {
        pthread_rwlock_unlock(&m_locks_lock);
        LOG(WARNING) << "lock not exist";
        SetZkAdapterCode(ZE_LOCK_NOT_EXIST, zk_errno);
        return false;
    }

    ZooKeeperLock * lock = itor->second;
    m_state_mutex.Unlock();
    if (!lock->CancelLock(zk_errno)) {
        m_state_mutex.Lock();
        delete lock;
        m_locks.erase(itor);
        pthread_rwlock_unlock(&m_locks_lock);
        return false;
    } else {
        m_state_mutex.Lock();
        pthread_rwlock_unlock(&m_locks_lock);
        return true;
    }
}

bool ZooKeeperAdapter::Unlock(const std::string& path, int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (!ZooKeeperUtil::IsValidPath(path)) {
        SetZkAdapterCode(ZE_ARG, zk_errno);
        return false;
    }
    if (NULL == m_handle) {
        SetZkAdapterCode(ZE_NOT_INIT, zk_errno);
        return false;
    }

    pthread_rwlock_wrlock(&m_locks_lock);
    LockMap::iterator itor = m_locks.find(path);
    if (itor == m_locks.end() || itor->second == NULL) {
        pthread_rwlock_unlock(&m_locks_lock);
        LOG(WARNING) << "lock not exist";
        SetZkAdapterCode(ZE_LOCK_NOT_EXIST, zk_errno);
        return false;
    }

    ZooKeeperLock * lock = itor->second;
    m_state_mutex.Unlock();
    if (lock->Unlock(zk_errno)) {
        m_state_mutex.Lock();
        delete lock;
        m_locks.erase(itor);
        pthread_rwlock_unlock(&m_locks_lock);
        return true;
    } else {
        m_state_mutex.Lock();
        pthread_rwlock_unlock(&m_locks_lock);
        return false;
    }
}

void ZooKeeperAdapter::GetId(std::string* id) {
    MutexLock mutex(&m_state_mutex);
    *id = m_id;
}

void ZooKeeperAdapter::TryCleanWatch(const std::string& path) {
    m_state_mutex.AssertHeld();
    pthread_rwlock_wrlock(&m_watcher_lock);
    WatcherMap::iterator itor = m_watchers.find(path);
    if (itor == m_watchers.end()) {
        pthread_rwlock_unlock(&m_watcher_lock);
        return;
    }

    ZooKeeperWatch * watch = itor->second;
    pthread_mutex_lock(&watch->mutex);
    if (!watch->watch_child && !watch->watch_exist && !watch->watch_value) {
        pthread_mutex_unlock(&watch->mutex);
        delete watch;
        m_watchers.erase(itor);
    } else {
        pthread_mutex_unlock(&watch->mutex);
    }
    pthread_rwlock_unlock(&m_watcher_lock);
}

void ZooKeeperAdapter::LockEventCallBack(zhandle_t * zh, int type, int state,
                                         const char * node_path, void * watch_ctx) {
    VLOG(5) << "recv lock event: type=" << ZooTypeToString(type) << ", state="
        << ZooStateToString(state) << ", path=[" << node_path << "]";

    if (ZOO_DELETED_EVENT != type) {
        LOG(WARNING) << "only allow DELETE_EVENT for lock";
        return;
    }

    if (NULL == watch_ctx) {
        return;
    }
    ZooKeeperAdapter* zk_adapter = (ZooKeeperAdapter*)watch_ctx;

    {
        MutexLock mutex(&zk_adapter->m_state_mutex);
        if (zh != zk_adapter->m_handle) {
            LOG(WARNING) << "zhandle not match";
            return;
        }
    }

    if (NULL == node_path) {
        LOG(WARNING) << "path is missing";
        return;
    }

    std::string path = node_path;
    if (!ZooKeeperUtil::IsValidPath(path)) {
        LOG(WARNING) << "path is invalid";
        return;
    }

    zk_adapter->LockEventCallBack(path);
}

void ZooKeeperAdapter::LockEventCallBack(std::string path) {
    VLOG(5) << "LockEventCallBack: path=[" << path << "]";
    MutexLock mutex(&m_state_mutex);

    std::string lock_path;
    ZooKeeperUtil::GetParentPath(path, &lock_path);

    pthread_rwlock_wrlock(&m_locks_lock);
    LockMap::iterator itor = m_locks.find(lock_path);
    if (itor == m_locks.end()) {
        pthread_rwlock_unlock(&m_locks_lock);
        LOG(WARNING) << "lock [" << lock_path << "] not exist";
        return;
    }
    ZooKeeperLock* lock = itor->second;
    if (lock == NULL) {
        pthread_rwlock_unlock(&m_locks_lock);
        return;
    }
    m_state_mutex.Unlock();
    lock->OnWatchNodeDeleted(path);
    m_state_mutex.Lock();
    pthread_rwlock_unlock(&m_locks_lock);
}

bool ZooKeeperAdapter::GetSessionId(int64_t* session_id, int* zk_errno) {
    MutexLock mutex(&m_state_mutex);
    if (ZS_CONNECTED == m_state) {
        *session_id = m_session_id;
        SetZkAdapterCode(ZE_OK, zk_errno);
        return true;
    }
    SetZkAdapterCode(ZE_SESSION, zk_errno);
    return false;
}

bool ZooKeeperAdapter::SetLibraryLogOutput(const std::string& file) {
    MutexLock mutex(&m_lib_log_mutex);
    FILE* new_log = fopen(file.c_str(), "a");
    if (NULL == new_log) {
        LOG(WARNING) << "fail to open file ["<< file << "]: " << strerror(errno);
        return false;
    }
    zoo_set_log_stream(new_log);
    if (NULL != m_lib_log_output) {
        fclose(m_lib_log_output);
    }
    m_lib_log_output = new_log;
    return true;
}

int ZooKeeperAdapter::ExistsWrapper(const std::string& path, bool is_watch,
                                    bool* is_exist) {
    m_state_mutex.AssertHeld();
    struct Stat stat;
    int ret = zoo_exists(m_handle, path.c_str(), is_watch, &stat);
    if (ZOK == ret) {
        *is_exist = true;
        LOG(INFO)<< "zoo_exists success";
    } else if (ZNONODE == ret) {
        *is_exist = false;
        LOG(INFO) << "zoo_exists success";
    } else {
        LOG(WARNING) << "zoo_exists fail : " << zerror(ret);
    }

    switch (ret) {
        case ZOK:
        case ZNONODE:
            return ZE_OK;
        case ZNOAUTH:
            return ZE_AUTH;
        case ZBADARGUMENTS:
            return ZE_ARG;
        case ZINVALIDSTATE:
            return ZE_SESSION;
        case ZMARSHALLINGERROR:
            return ZE_SYSTEM;
        default:
            return ZE_UNKNOWN;
    }
}

int ZooKeeperAdapter::ExistsWrapperForLock(const std::string& path,
                                           bool* is_exist) {
    m_state_mutex.AssertHeld();
    struct Stat stat;
    int ret = zoo_wexists(m_handle, path.c_str(), LockEventCallBack, this, &stat);
    if (ZOK == ret) {
        *is_exist = true;
        LOG(INFO)<< "zoo_exists success";
    } else if (ZNONODE == ret) {
        *is_exist = false;
        LOG(INFO) << "zoo_exists success";
    } else {
        LOG(WARNING) << "zoo_exists fail : " << zerror(ret);
    }

    switch (ret) {
        case ZOK:
        case ZNONODE:
            return ZE_OK;
        case ZNOAUTH:
            return ZE_AUTH;
        case ZBADARGUMENTS:
            return ZE_ARG;
        case ZINVALIDSTATE:
            return ZE_SESSION;
        case ZMARSHALLINGERROR:
            return ZE_SYSTEM;
        default:
            return ZE_UNKNOWN;
    }
}

int ZooKeeperAdapter::GetWrapper(const std::string& path, bool is_watch,
                                 std::string* value) {
    m_state_mutex.AssertHeld();
    char* buffer = new char[kMaxNodeDataLen];
    int buffer_len = kMaxNodeDataLen;
    int ret = zoo_get(m_handle, path.c_str(), is_watch, buffer, &buffer_len,
                      NULL);
    if (ZOK == ret) {
        if (buffer_len < 0) {
            buffer_len = 0;
        } else if (buffer_len >= kMaxNodeDataLen) {
            buffer_len = kMaxNodeDataLen - 1;
        }
        buffer[buffer_len] = '\0';
        *value = buffer;
        VLOG(10) << "zoo_get success";
    } else {
        LOG(WARNING) << "zoo_get fail : " << zerror(ret);
    }
    delete[] buffer;

    switch (ret) {
        case ZOK:
            return ZE_OK;
        case ZNONODE:
            return ZE_NOT_EXIST;
        case ZNOAUTH:
            return ZE_AUTH;
        case ZBADARGUMENTS:
            return ZE_ARG;
        case ZINVALIDSTATE:
            return ZE_SESSION;
        case ZMARSHALLINGERROR:
            return ZE_SYSTEM;
        default:
            return ZE_UNKNOWN;
    }
}

int ZooKeeperAdapter::GetChildrenWrapper(const std::string& path, bool is_watch,
                                         std::vector<std::string>* child_list,
                                         std::vector<std::string>* value_list) {
    m_state_mutex.AssertHeld();
    struct String_vector str_vec;
    allocate_String_vector(&str_vec, 0);
    int ret = zoo_get_children(m_handle, path.c_str(), is_watch, &str_vec);
    if (ZOK == ret) {
        child_list->clear();
        value_list->clear();
        for (int i = 0; i < str_vec.count; i++) {
            child_list->push_back(str_vec.data[i]);
            std::string child_path = path + '/' + str_vec.data[i];
            std::string value;
            int ret2 = GetWrapper(child_path, false, &value);
            if (ZE_OK != ret2) {
                value = "";
                LOG(WARNING) << "read node fail : " << ret2;
            }
            value_list->push_back(value);
        }
        LOG(INFO) << "zoo_get_children success";
    } else {
        LOG(WARNING) << "zoo_get_children fail : " << zerror(ret);
    }
    deallocate_String_vector(&str_vec);

    switch (ret) {
        case ZOK:
            return ZE_OK;
        case ZNONODE:
            return ZE_NOT_EXIST;
        case ZNOAUTH:
            return ZE_AUTH;
        case ZBADARGUMENTS:
            return ZE_ARG;
        case ZINVALIDSTATE:
            return ZE_SESSION;
        case ZMARSHALLINGERROR:
            return ZE_SYSTEM;
        default:
            return ZE_UNKNOWN;
    }
}

} // namespace zk
} // namespace tera
