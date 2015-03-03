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
    FlashEnv();

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

    virtual Status GetChildren(const std::string& path,
            std::vector<std::string>* result);

    virtual Status DeleteFile(const std::string& fname);

    virtual Status CreateDir(const std::string& name);

    virtual Status DeleteDir(const std::string& name);

    virtual Status ListDir(const std::string& name,
            std::vector<std::string>* result);

    virtual Status CopyFile(const std::string& from,
                            const std::string& to) {
        return Status::OK();
    }

    virtual Status GetFileSize(const std::string& fname, uint64_t* size);

    virtual Status RenameFile(const std::string& src, const std::string& target);

    virtual Status LockFile(const std::string& fname, FileLock** lock);

    virtual Status UnlockFile(FileLock* lock);

    /// flash path for local flash cache
    static void SetFlashPath(const std::string& path);
    static const std::string& FlashPath(const std::string& fname);
    static const std::vector<std::string>& GetFlashPaths() {
        return flash_paths_;
    }

private:
    Env* hdfs_env_;
    Env* posix_env_;
    static std::vector<std::string> flash_paths_;
};

/// default mem env
Env* EnvFlash();
/// new mem env
Env* NewFlashEnv();
}  // namespace leveldb


#endif  //__LEVELDB_ENV_FLASH_H_

/* vim: set ts=4 sw=4 sts=4 tw=100: */
