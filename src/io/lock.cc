// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "io/lock.h"

#include "io/io_utils.h"

namespace tera {
namespace io {

LockManager::LockManager() {

}

LockManager::~LockManager() {

}

bool LockManager::Lock(const std::string& key, uint64_t id,
                       const std::string& annotation, StatusCode* status) {
    MutexLock l(&mutex_);

    LockIterator it = lock_map_.find(key);
    if (it == lock_map_.end()) {
        LockNode& lock_node = lock_map_[key];
        lock_node.id = id;
        lock_node.annotation = annotation;
        VLOG(10) << "lock key: " << key << " id: " << id
                 << " annotation: " << annotation;
        return true;
    }

    LockNode& ln = it->second;
    if (ln.id == id) {
        SetStatusCode(kLockDoubleLock, status);
    } else {
        SetStatusCode(kLockNotOwn, status);
    }
    VLOG(10) << "fail to lock key: " << key << " id: " << id
             << " annotation: " << annotation
             << " status: " << StatusCodeToString(*status);
    return false;
}

bool LockManager::Unlock(const std::string& key, uint64_t id,
                         std::string* annotation, StatusCode* status) {
    MutexLock l(&mutex_);

    LockIterator it;
    if (!GetLockNode(key, id, &it, status)) {
        VLOG(10) << "fail to unlock key: " << key << " id: " << id
                 << " status: " << StatusCodeToString(*status);
        return false;
    }

    if (annotation != NULL) {
        annotation->assign(it->second.annotation);
    }
    lock_map_.erase(it);
    VLOG(10) << "unlock key: " << key << " id: " << id
             << " annotation: " << it->second.annotation;
    return true;
}

bool LockManager::IsLocked(const std::string& key, uint64_t id,
                           std::string* annotation, StatusCode* status) {
    MutexLock l(&mutex_);

    LockIterator it;
    if (!GetLockNode(key, id, &it, status)) {
        return false;
    }

    LockNode& ln = it->second;
    if (annotation != NULL) {
        annotation->assign(ln.annotation);
    }
    return true;
}

bool LockManager::IsLocked(const std::string& key, uint64_t* id,
                           std::string* annotation, StatusCode* status) {
    MutexLock l(&mutex_);

    LockIterator it = lock_map_.find(key);
    if (it == lock_map_.end()) {
        SetStatusCode(kLockNotExist, status);
        return false;
    }

    LockNode& ln = it->second;
    if (id != NULL) {
        *id = ln.id;
    }
    if (annotation != NULL) {
        annotation->assign(ln.annotation);
    }
    return true;
}

bool LockManager::GetLockNode(const std::string& key, uint64_t id,
                              LockIterator* ret_it, StatusCode* status) {
    mutex_.AssertHeld();

    LockIterator it = lock_map_.find(key);
    if (it == lock_map_.end()) {
        VLOG(10) << "lock not exist, key: " << key << " id: " << id;
        SetStatusCode(kLockNotExist, status);
        return false;
    }

    LockNode& ln = it->second;
    if (ln.id != id) {
        VLOG(10) << "lock not own, key: " << key << " id: " << id
                 << ", real id: " << ln.id << ", annotation: " << ln.annotation;
        SetStatusCode(kLockNotOwn, status);
        return false;
    }

    *ret_it = it;
    return true;
}

} // namespace tabletnode
} // namespace tera
