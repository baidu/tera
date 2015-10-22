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

    /// flash path for local flash cache
    static void SetFlashPath(const std::string& path, bool vanish_allowed);
    static const std::string& FlashPath(const std::string& fname);
    static const std::vector<std::string>& GetFlashPaths() {
        return flash_paths_;
    }

private:
    Env* dfs_env_;
    Env* posix_env_;
    static std::vector<std::string> flash_paths_;
    static bool vanish_allowed_;
};

/// new flash env
Env* NewFlashEnv(Env* base_env);
}  // namespace leveldb

#endif  //__LEVELDB_ENV_FLASH_H_
