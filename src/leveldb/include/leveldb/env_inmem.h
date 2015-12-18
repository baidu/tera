// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  __LEVELDB_ENV_INMEM_H_
#define  __LEVELDB_ENV_INMEM_H_


#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <iostream>
#include "env.h"
#include "status.h"


namespace leveldb {

class InMemoryEnv : public EnvWrapper{
public:
    InMemoryEnv(Env* base_env);

    ~InMemoryEnv();

    virtual Status NewSequentialFile(const std::string& fname,
            SequentialFile** result);

    virtual Status NewRandomAccessFile(const std::string& fname,
            RandomAccessFile** result);

    virtual Status NewWritableFile(const std::string& fname,
            WritableFile** result);

    virtual bool FileExists(const std::string& fname);

    bool CheckDelete(const std::string& fname, std::vector<std::string>* flags);

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

    virtual Env* CacheEnv() { return mem_env_; }

private:
    Env* dfs_env_;
    Env* mem_env_;
};

/// new mem env
Env* NewInMemoryEnv(Env* base_env);
}  // namespace leveldb

#endif  //__LEVEL_ENV_INMEM_H_
