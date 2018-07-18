// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <sstream>

#include "db/table_cache.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/env_flash_block_cache.h"
#include "leveldb/options.h"
#include "leveldb/statistics.h"
#include "leveldb/status.h"
#include "leveldb/table_utils.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/string_ext.h"
#include "util/thread_pool.h"

// We provide FlashBlockCacheImpl to manage each flash storage. Each flash's block-based cache
// devides into a set of block_set files, which managed by BlockSet. Each block_set devieds
// into a set of flash cache block, which managed by CacheBlock.

namespace leveldb {

static const std::string kMetaDBName = "meta_db";
static const std::string kKeyPrefixBlockSet = "BS#";
static const std::string kKeyPrefixFName = "FNAME#";
static const std::string kKeyFID = "FID#";
static const std::string kKeyConfCacheSize = "CONF#CACHE_SIZE";
static const std::string kKeyConfBlockSetSize = "CONF#BLOCKSET_SIZE";
static const std::string kKeyConfBlockSize = "CONF#BLOCK_SIZE";

// block state
extern uint64_t kCacheBlockValid;
extern uint64_t kCacheBlockLocked;
extern uint64_t kCacheBlockDfsRead;
extern uint64_t kCacheBlockCacheRead;
extern uint64_t kCacheBlockCacheFill;

struct CacheBlock {
    // file id alloced by FID alloctor, just like inode number
    uint64_t fid;

    // block offset number in user's file
    uint64_t block_idx;

    // block set's id
    uint64_t sid;

    // block offset number in block_set's file
    uint64_t offset_in_blockset;

    // block state bit, includes {valid, locked, dfs_read, cache_read, cache_write}
    volatile uint64_t state;

    port::Mutex mu;
    port::CondVar cv;

    // block data
    Slice data_block;
    bool data_block_alloc; // if true, delete data block by itself
    uint64_t data_block_refs;

    // handle of this CacheBlock node in the LRU of block
    LRUHandle* handle;

    // handle of the BlockSet node which this CacheBlock belongs to
    LRUHandle* bs_handle;

    Status s;

    CacheBlock()
    : fid(0),
      block_idx(0),
      sid(0xffffffffffffffff),
      offset_in_blockset(0xffffffffffffffff),
      state(0),
      cv(&mu),
      data_block_alloc(false),
      data_block_refs(0),
      handle(NULL),
      bs_handle(NULL) {
    }

    bool Test(uint64_t c_state) {
        mu.AssertHeld();
        return (state & c_state) == c_state;
    }

    void Clear(uint64_t c_state) {
        mu.AssertHeld();
        state &= ~c_state;
    }

    void Set(uint64_t c_state) {
        mu.AssertHeld();
        state |= c_state;
    }

    void WaitOnClear(uint64_t c_state) { // access in lock
        mu.AssertHeld();
        while (Test(c_state)) {
            cv.Wait();
        }
    }

    // access in cache lock
    void GetDataBlock(uint64_t block_size, Slice data) {
        if (data_block_refs == 0) { // first one alloc mem
            assert(data_block.size() == 0);
            assert(data_block_alloc == false);
            if (data.size() == 0) {
                char* buf = new char[block_size];
                data = Slice(buf, block_size);
                data_block_alloc = true;
            }
            data_block = data;
        }
        ++data_block_refs;
    }

    // access in cache lock
    void ReleaseDataBlock() {
        --data_block_refs;
        if (data_block_refs == 0) {
            if (data_block_alloc) {
                char* data = (char*)data_block.data();
                delete[] data;
                data_block_alloc = false;
            }
            data_block = Slice();
        }
    }

    // key of the CacheBlock in LRU
    std::string CacheKey() {
        std::string key;
        PutFixed64(&key, fid);
        PutFixed64(&key, block_idx);
        return key;
    }

    std::string EncodeDBKey() {
        std::string key = kKeyPrefixBlockSet;
        PutFixed64(&key, sid);
        PutFixed64(&key, offset_in_blockset);
        return key;
    }

    void DecodeDBKey(Slice lkey) {
        lkey.remove_prefix(kKeyPrefixBlockSet.size());// lkey = BS#, sid, offset
        sid = DecodeFixed64(lkey.data());
        lkey.remove_prefix(sizeof(uint64_t));
        offset_in_blockset = DecodeFixed64(lkey.data());
    }

    void DecodeDBValue(Slice record) {
        fid = DecodeFixed64(record.data());
        record.remove_prefix(sizeof(uint64_t));
        block_idx = DecodeFixed64(record.data());
        record.remove_prefix(sizeof(uint64_t));
        state = DecodeFixed64(record.data());
        return;
    }

    std::string EncodeDBValue() {
        std::string r;
        PutFixed64(&r, fid);
        PutFixed64(&r, block_idx);
        PutFixed64(&r, state);
        return r;
    }

    std::string ToString() {
        std::stringstream ss;
        ss << "CacheBlock(" << (uint64_t)this << "): fid: " << fid << ", block_idx: " << block_idx
           << ", sid: " << sid << ", offset_in_blockset: " << offset_in_blockset
           << ", state " << state << ", status " << s.ToString();
        return ss.str();
    }
};

struct BlockSet {
    // handle of this BlockSet node in the LRU of block set
    LRUHandle* handle;
    port::Mutex mu;
    // LRU of block
    Cache* block_cache;
    int fd;

    BlockSet(): handle(NULL), block_cache(NULL), fd(-1) {}

    // key of the BlockSet in LRU
    std::string CacheKey(uint64_t sid) {
        std::string key;
        PutFixed64(&key, sid);
        return key;
    }
};

class FlashBlockCacheImpl {
public:
    explicit FlashBlockCacheImpl(const FlashBlockCacheOptions& options);

    ~FlashBlockCacheImpl();

    std::string WorkPath() const;

    Status LoadCache(); // init cache

    static void BlockDeleter(const Slice& key, void* v);

    static void BGControlThreadFunc(void* arg);

    // alloc fid for sst file
    uint64_t FileId(const std::string& fname);

    // delete fid from cache
    Status DeleteFile(const std::string& fname);

    // alloc data block for sst file
    CacheBlock* GetAndAllocBlock(uint64_t fid, uint64_t block_idx);

    Status ReleaseBlock(CacheBlock* block, bool need_sync);

    Status FillCache(CacheBlock* block);

    Status ReadCache(CacheBlock* block);

    Status LogRecord(CacheBlock* block);

private:
    friend struct BlockSet;
    struct LockContent;

    Status LockAndPut(LockContent* lc);

    Status GetContentAfterWait(LockContent* lc);

    Status PutContentAfterLock(LockContent* lc);

    Status ReloadBlockSet(LockContent* lc);

    uint64_t AllocFileId(); // no more than fid_batch_num

    BlockSet* GetBlockSet(uint64_t sid);

    void BGControlThread();

public:
    FlashBlockCacheOptions options_;

    Statistics* stat_;

    Env* dfs_env_;

    ThreadPool bg_fill_;
    ThreadPool bg_read_;
    ThreadPool bg_dfs_read_;
    ThreadPool bg_flush_;
    ThreadPool bg_control_;

private:
    std::string work_path_;
    //Env* posix_env_;

    port::Mutex mu_;
    // key lock list
    struct Waiter {
        int wait_num; // protected by FlashBlockCacheImpl.mu_

        port::Mutex mu;
        port::CondVar cv;
        bool done;
        Waiter(): wait_num(0), cv(&mu), done(false) {}

        void Wait() {
            MutexLock l(&mu);
            while (!done) { cv.Wait(); }
        }

        void SignalAll() {
            MutexLock l(&mu);
            done = true;
            cv.SignalAll();
        }
    };
    typedef std::map<std::string, Waiter*> LockKeyMap;
    LockKeyMap lock_key_;

    uint64_t new_fid_;
    uint64_t prev_fid_;

    enum class LockKeyType {
        kDBKey = 0,
        kBlockSetKey = 1,
        kDeleteDBKey = 2,
    };
    struct LockContent {
        LockKeyType type;

        // DB key
        Slice db_lock_key;
        Slice db_lock_val;
        std::string* db_val;

        // block set id
        uint64_t sid;
        BlockSet* block_set;

        std::string Encode() {
            if (type == LockKeyType::kDBKey || type == LockKeyType::kDeleteDBKey) {
                return db_lock_key.ToString();
            } else if (type == LockKeyType::kBlockSetKey) {
                std::string key = kKeyPrefixBlockSet;
                PutFixed64(&key, sid);
                return key;
            }
            return "";
        }

        std::string KeyToString() {
            if (type == LockKeyType::kDBKey || type == LockKeyType::kDeleteDBKey) {
                return db_lock_key.ToString();
            } else if (type == LockKeyType::kBlockSetKey) {
                std::stringstream ss;
                ss << kKeyPrefixBlockSet << sid;
                return ss.str();
            } else {
                return "";
            }
        }

        std::string ValToString() {
            if (type == LockKeyType::kDBKey) {
                uint64_t val = DecodeFixed64(db_lock_val.data());
                std::stringstream ss;
                ss << val;
                return ss.str();
            }
            return "";
        }
    };

    std::string FNameDBKey(const std::string& fname) {
        std::string key = kKeyPrefixFName + fname;
        return key;
    }

    std::string FIDDBKey() {
        return kKeyFID;
    }

    // LRU of block set
    Cache* block_set_cache_;

    // store meta
    DB* meta_db_;
};

}  // namespace leveldb

