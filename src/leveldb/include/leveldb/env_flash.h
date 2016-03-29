// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  __LEVELDB_ENV_FLASH_H_
#define  __LEVELDB_ENV_FLASH_H_

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <iostream>
#include "env.h"
#include "status.h"
#include "util/thread_pool.h"


namespace leveldb {

class FlashEnv : public EnvWrapper{
public:
    FlashEnv(Env* base_env);

    ~FlashEnv();

    virtual Status NewSequentialFile(const std::string& fname,
            SequentialFile** result);

    virtual Status NewRandomAccessFile(const std::string& fname,
            RandomAccessFile** result);

    virtual Status NewRandomAccessFile(const std::string& fname,
            uint64_t fsize, RandomAccessFile** result);

    virtual Status NewWritableFile(const std::string& fname,
            WritableFile** result);

    virtual bool FileExists(const std::string& fname);

    virtual Status GetChildren(const std::string& path, std::vector<std::string>* result);

    virtual Status DeleteFile(const std::string& fname);

    virtual Status CreateDir(const std::string& name);

    virtual Status DeleteDir(const std::string& name);

    virtual Status CopyFile(const std::string& from,
                            const std::string& to) {
        return Status::OK();
    }

    virtual Status GetFileSize(const std::string& fname, uint64_t* size);

    virtual Status RenameFile(const std::string& src, const std::string& target);

    virtual Status LockFile(const std::string& fname, FileLock** lock);

    virtual Status UnlockFile(FileLock* lock);

    virtual Env* CacheEnv() { return posix_env_; }

    Env* BaseEnv() { return dfs_env_; }

    /// flash path for local flash cache
    void SetFlashPath(const std::string& path, bool vanish_allowed);
    const std::string& FlashPath(const std::string& fname);
    const bool VanishAllowed() {
        return vanish_allowed_;
    }
    const std::vector<std::string>& GetFlashPaths() {
        return flash_paths_;
    }

    /// copy to local
    void SetIfForceReadFromCache(bool force);
    bool ForceReadFromCache();
    void SetUpdateFlashThreadNumber(int thread_num);

    bool FlashFileIdentical(const std::string& fname, uint64_t fsize);
    void ScheduleUpdateFlash(const std::string& fname, uint64_t fsize, int64_t priority);
    void UpdateFlashFile(const std::string& fname, uint64_t fsize);

private:
    Env* dfs_env_;
    Env* posix_env_;

    std::vector<std::string> flash_paths_;
    bool vanish_allowed_;

    bool force_read_from_cache_;
    ThreadPool update_flash_threads_;
    port::Mutex update_flash_mutex_;
    struct UpdateFlashTask {
        int64_t id;
        int64_t priority;
    };
    std::map<std::string, UpdateFlashTask> update_flash_waiting_files_;
    int64_t update_flash_retry_interval_millis_;
};

/// new flash env
Env* NewFlashEnv(Env* base_env);
}  // namespace leveldb

#endif  //__LEVELDB_ENV_FLASH_H_
