// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: likang01(com@baidu.com)

#ifndef  TERA_ZK_ZK_ADAPTER_H_
#define  TERA_ZK_ZK_ADAPTER_H_

#include <map>
#include <string>
#include <zookeeper.h>

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

// typedef void (*LOCK_CALLBACK)(const char * path, int err, void * param);
struct ZooKeeperWatch;
class ZooKeeperAdapter {
public:
    ZooKeeperAdapter();
    virtual ~ZooKeeperAdapter();

    bool Init(const std::string& server_list, const std::string& root_path,
              uint32_t session_timeout, const std::string& id, int* zk_errno,
              int wait_timeout = -1); // default wait until zk server ready
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
    bool WatchZkLock(const std::string &path, int* zk_errno);
    virtual void OnZkLockDeleted() {}

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
    static FILE* lib_log_output_;
    static Mutex lib_log_mutex_;

    // protected by state_mutex_
    Mutex state_mutex_;
    std::string id_;
    std::string server_list_;
    std::string root_path_;
    zhandle_t * handle_;
    volatile int state_;
    volatile int64_t session_id_;
    common::CondVar state_cond_;
    uint32_t session_timeout_;
    int64_t session_timer_id_;
    ThreadPool thread_pool_;

    // protected by watcher_lock_
    typedef std::map<std::string, ZooKeeperWatch*> WatcherMap;
    WatcherMap watchers_;
    pthread_rwlock_t watcher_lock_;

    // protected by locks_lock_
    typedef std::map<std::string, ZooKeeperLock*> LockMap;
    LockMap locks_;
    pthread_rwlock_t locks_lock_;
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

#endif  // TERA_ZK_ZK_ADAPTER_H_
