// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

// block-based cache env
namespace leveldb {

class FlashBlockCacheImpl;

class FlashBlockCacheEnv : public EnvWrapper {
public:
    explicit FlashBlockCacheEnv(Env* base);

    virtual ~FlashBlockCacheEnv();

    virtual Status FileExists(const std::string& fname);

    virtual Status GetChildren(const std::string& path,
                               std::vector<std::string>* result);

    virtual Status DeleteFile(const std::string& fname);

    virtual Status CreateDir(const std::string& name);

    virtual Status DeleteDir(const std::string& name);

    virtual Status CopyFile(const std::string& from,
                            const std::string& to);

    virtual Status GetFileSize(const std::string& fname, uint64_t* size);

    virtual Status RenameFile(const std::string& src, const std::string& target);

    virtual Status LockFile(const std::string& fname, FileLock** lock);

    virtual Status UnlockFile(FileLock* lock);

    virtual Status NewSequentialFile(const std::string& fname,
                                     SequentialFile** result); // never cache log

    // cache relatively
    virtual Status NewRandomAccessFile(const std::string& fname,
                                       RandomAccessFile** result,
                                       const EnvOptions& options); // cache Pread
    virtual Status NewRandomAccessFile(const std::string& fname,
                                       uint64_t fsize,
                                       RandomAccessFile** result,
                                       const EnvOptions& options); // cache Pread

    virtual Status NewWritableFile(const std::string& fname,
                                   WritableFile** result,
                                   const EnvOptions& options); // cache Append
    virtual Status LoadCache(const FlashBlockCacheOptions& opts, const std::string& cache_dir);

private:
    std::vector<FlashBlockCacheImpl*> caches_;
    Env* dfs_env_;
};

Env* NewFlashBlockCacheEnv(Env* base);

} // leveldb

