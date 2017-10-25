// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: caijieming@baidu.com

#ifndef  STOREAGE_LEVELDB_UTIL_BLOCK_CACHE_H_
#define  STOREAGE_LEVELDB_UTIL_BLOCK_CACHE_H_

#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {
/////////////////////////////////////////////
// Tcache
/////////////////////////////////////////////
extern uint64_t kBlockSize;
extern uint64_t kDataSetSize;
extern uint64_t kFidBatchNum;
extern uint64_t kCacheSize;
extern uint64_t kMetaBlockSize;
extern uint64_t kMetaTableSize;
extern uint64_t kWriteBufferSize;

struct BlockCacheOptions {
    Options opts;
    std::string cache_dir;
    uint64_t block_size;
    uint64_t dataset_size;
    uint64_t fid_batch_num;
    uint64_t cache_size;
    uint64_t dataset_num;
    uint64_t meta_block_cache_size;
    uint64_t meta_table_cache_size;
    uint64_t write_buffer_size;
    Env* env;
    Env* cache_env;

    BlockCacheOptions()
    : block_size(kBlockSize),
      dataset_size(kDataSetSize),
      fid_batch_num(kFidBatchNum),
      cache_size(kCacheSize),
      meta_block_cache_size(kMetaBlockSize),
      meta_table_cache_size(kMetaTableSize),
      write_buffer_size(kWriteBufferSize),
      env(NULL) {
          dataset_num = cache_size / dataset_size + 1;
    }
};

class BlockCacheImpl;

class BlockCacheEnv : public EnvWrapper {
public:
    BlockCacheEnv(Env* base);

    ~BlockCacheEnv();

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
                                       RandomAccessFile** result); // cache Pread
    virtual Status NewRandomAccessFile(const std::string& fname,
                                       uint64_t fsize,
                                       RandomAccessFile** result); // cache Pread

    virtual Status NewWritableFile(const std::string& fname,
                                   WritableFile** result); // cache Append
    virtual Status LoadCache(const BlockCacheOptions& opts, const std::string& cache_dir);

private:
    std::vector<BlockCacheImpl*> cache_vec_;
    Env* dfs_env_;
};

Env* NewBlockCacheEnv(Env* base);

} // leveldb
#endif

