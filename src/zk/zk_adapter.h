// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: likang01(com@baidu.com)

#ifndef  TERA_ZK_ZK_ADAPTER_H
#define  TERA_ZK_ZK_ADAPTER_H

#include <map>
#include <string>
#include <zookeeper/zookeeper.h>

#include "common/mutex.h"
#include "common/thread_pool.h"

#include "zk/zk_lock.h"
#include "zk/zk_util.h"

namespace tera {
namespace zk {

/*
 * CREATE_EVENT will be notified if and only if EXIST watch set
 * DELETE_EVENT will be notified if and only if EXIST/VALUE/CHILD watch set
 * CHANGE_EVENT will be notified if and only if VALUE watch set
 * CHILD_EVENT will be notified if and only if CHILD watch set
 *
 * EXIST_WATCH will be valid forever
 * VALUE_WATCH will be valid while node is present
 * CHILD_WATCH will be valid while node is present
 */

//typedef void (*LOCK_CALLBACK)(const char * path, int err, void * param);
struct ZooKeeperWatch;
class ZooKeeperAdapter {
public:
    ZooKeeperAdapter();
    virtual ~ZooKeeperAdapter();

    bool Init(const std::string& server_list, const std::string& root_path,
              uint32_t session_timeout, const std::string& id, int* zk_errno);
    void Finalize();
    bool GetSessionId(int64_t* session_id, int* zk_errno);

    // create
    bool CreatePersistentNode(const std::string& path, const std::string& value,
                              int* zk_errno);
    bool CreateEphemeralNode(const std::string& path, const std::string& value,
                             int* zk_errno);
    bool CreateSequentialEphemeralNode(const std::string& path,
                                       const std::string& value,
                                       std::string* ret_path, int* zk_errno);

    // delete
    bool DeleteNode(const std::string& path, int* zk_errno);

    // write
    bool WriteNode(const std::string& path, const std::string& value,
                   int* zk_errno);

    // read
    bool ReadNode(const std::string& path, std::string* value, int* zk_errno);
    bool ReadAndWatchNode(const std::string& path, std::string* value,
                          int* zk_errno);

    // exist
    bool CheckExist(const std::string&path, bool* is_exist, int* zk_errno);
    bool CheckAndWatchExist(const std::string& path, bool* is_exist,
                            int* zk_errno);
    bool CheckAndWatchExistForLock(const std::string& path, bool* is_exist,
                                   int* zk_errno);

    // list
    bool ListChildren(const std::string& path,
                      std::vector<std::string>* child_list,
                      std::vector<std::string>* value_list,
                      int* zk_errno);
    bool ListAndWatchChildren(const std::string& path,
                              std::vector<std::string>* child_list,
                              std::vector<std::string>* value_list,
                              int* zk_errno);

    // callback
    static void EventCallBack(zhandle_t* zh, int type, int state,
                              const char* path, void* watch_ctx);
    static void LockEventCallBack(zhandle_t* zh, int type, int state,
                                  const char* path, void* watch_ctx);

    // lock
    bool AsyncLock(const std::string& path, LOCK_CALLBACK func, void* param,
                   int* zk_errno);
    bool SyncLock(const std::string& path, int* zk_errno, int32_t timeout = -1);
    bool CancelLock(const std::string& path, int* zk_errno);
    bool Unlock(const std::string& path, int* zk_errno);
    static void SyncLockCallback(const std::string& path, int err, void* param);

    void GetId(std::string* id);
    static bool SetLibraryLogOutput(const std::string& file);

protected:
    bool Create(const std::string& path, const std::string& value, int flag,
                std::string* ret_path, int* zk_errno);

    void CreateEventCallBack(std::string path);
    void DeleteEventCallBack(std::string path);
    void ChangeEventCallBack(std::string path);
    void ChildEventCallBack(std::string path);
    void SessionEventCallBack(int state);
    void WatchLostEventCallBack(int state, std::string path);
    void LockEventCallBack(std::string path);

    void TryCleanWatch(const std::string& path);

    int Lock(const std::string& path, bool async, int32_t timeout = -1);

    virtual void OnChildrenChanged(const std::string& path,
                                   const std::vector<std::string>& name_list,
                                   const std::vector<std::string>& data_list) = 0;
    virtual void OnNodeValueChanged(const std::string& path,
                                    const std::string& value) = 0;
    virtual void OnNodeCreated(const std::string& path) = 0;
    virtual void OnNodeDeleted(const std::string& path) = 0;
    virtual void OnWatchFailed(const std::string& path, int watch_type,
                               int err) = 0;
    virtual void OnSessionTimeout() = 0;

    int ExistsWrapper(const std::string& path, bool is_watch, bool* is_exist);
    int ExistsWrapperForLock(const std::string& path, bool* is_exist);
    int GetChildrenWrapper(const std::string& path, bool is_watch,
                           std::vector<std::string>* child_list,
                           std::vector<std::string>* value_list);
    int GetWrapper(const std::string& path, bool is_watch, std::string* value);
    void SessionTimeoutWrapper();

private:
    static FILE* m_lib_log_output;

    // protected by m_state_mutex
    Mutex m_state_mutex;
    std::string m_id;
    std::string m_server_list;
    std::string m_root_path;
    zhandle_t * m_handle;
    volatile int m_state;
    volatile int64_t m_session_id;
    common::CondVar m_state_cond;
    uint32_t m_session_timeout;
    int64_t m_session_timer_id;
    ThreadPool m_thread_pool;

    // protected by m_watcher_lock
    typedef std::map<std::string, ZooKeeperWatch*> WatcherMap;
    WatcherMap m_watchers;
    pthread_rwlock_t m_watcher_lock;

    // protected by m_locks_lock
    typedef std::map<std::string, ZooKeeperLock*> LockMap;
    LockMap m_locks;
    pthread_rwlock_t m_locks_lock;
};

class ZooKeeperLightAdapter : public ZooKeeperAdapter {
private:
    bool ReadAndWatchNode(const std::string&, std::string*, int*) {return false;}
    bool CheckAndWatchExist(const std::string&, bool*, int*) {return false;}
    bool CheckAndWatchExistForLock(const std::string&, bool*, int*) {return false;}
    bool ListAndWatchChildren(const std::string&, std::vector<std::string>*,
                              std::vector<std::string>*, int*) {return false;}

    bool AsyncLock(const std::string&, LOCK_CALLBACK, void*, int*) {return false;}
    bool SyncLock(const std::string&, int*, int32_t = -1) {return false;}
    bool CancelLock(const std::string&, int*) {return false;}
    bool Unlock(const std::string&, int*) {return false;}

private:
    virtual void OnChildrenChanged(const std::string&,
                                   const std::vector<std::string>&,
                                   const std::vector<std::string>&) {}
    virtual void OnNodeValueChanged(const std::string&, const std::string&) {}
    virtual void OnNodeCreated(const std::string&) {}
    virtual void OnNodeDeleted(const std::string&) {}
    virtual void OnWatchFailed(const std::string&, int, int) {}
    virtual void OnSessionTimeout() {}
};

} // namespace zk
} // namespace tera

#endif  //TERA_ZK_ZK_ADAPTER_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
