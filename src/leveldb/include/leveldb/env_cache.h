// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  STOREAGE_LEVELDB_UTIL_ENV_CACHE_H
#define  STOREAGE_LEVELDB_UTIL_ENV_CACHE_H

#include "leveldb/env.h"
#include "leveldb/status.h"

namespace leveldb {

class ThreeLevelCacheEnv : public EnvWrapper {
public:
    ThreeLevelCacheEnv();
    ~ThreeLevelCacheEnv();

    virtual Status NewSequentialFile(const std::string& fname,
            SequentialFile** result);

    virtual Status NewRandomAccessFile(const std::string& fname,
            RandomAccessFile** result);

    virtual Status NewWritableFile(const std::string& fname,
            WritableFile** result);

    virtual bool FileExists(const std::string& fname);

    virtual Status GetChildren(const std::string& path,
            std::vector<std::string>* result);

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

    static void SetCachePaths(const std::string& path);
    static void RemoveCachePaths();
    static const std::string& CachePath(const std::string& fname);
    static const std::vector<std::string>& GetCachePaths() {
        return cache_paths_;
    }

    virtual Env* CacheEnv() { return posix_env_; }

    // reset operation is for testing
    static void ResetMemCache();
    static void ResetDiskCache();

public:
    static uint32_t s_mem_cache_size_in_KB_;
    static uint32_t s_disk_cache_size_in_MB_;
    static uint32_t s_block_size_;
    static uint32_t s_disk_cache_file_num_;
    static std::string s_disk_cache_file_name_;

private:
    Env* dfs_env_;
    Env* posix_env_;

    static std::vector<std::string> cache_paths_;
};

Env* EnvThreeLevelCache();
Env* NewThreeLevelCacheEnv();

}  // namespace leveldb

#endif // STOREAGE_LEVELDB_UTIL_ENV_CACHE_H
