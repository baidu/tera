// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_LOCK_MANAGER_H_
#define TERA_IO_LOCK_MANAGER_H_

#include "common/mutex.h"

#include "proto/status_code.pb.h"

namespace tera {
namespace io {

class LockManager {
    struct LockNode {
        uint64_t id;
        std::string annotation;
    };
public:
    LockManager();
    ~LockManager();

    // key is a unique name of the item you want to lock
    // id is used to authenticate
    // annotation is a hint for others to do cleanup work after the lock owner dead

    bool Lock(const std::string& key, uint64_t id,
              const std::string& annotation = "", StatusCode* status = NULL);

    bool Unlock(const std::string& key, uint64_t id,
                std::string* annotation = NULL, StatusCode* status = NULL);

    bool IsLocked(const std::string& key, uint64_t id,
                  std::string* annotation = NULL, StatusCode* status = NULL);

    bool IsLocked(const std::string& key, uint64_t* id = NULL,
                  std::string* annotation = NULL, StatusCode* status = NULL);

private:
    typedef std::map<std::string, LockNode> LockMap;
    typedef LockMap::iterator LockIterator;
    bool LockManager::GetLockNode(const std::string& key, uint64_t id,
                                  LockIterator* ret_it, StatusCode* status = NULL);

private:
    Mutex mutex_;
    LockMap lock_map_;
};

} // namespace io
} // namespace tera

#endif // TERA_IO_LOCK_MANAGER_H_
