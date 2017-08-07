// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/block_cache.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <list>
#include <sstream>

#include "db/table_cache.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/table_utils.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/string_ext.h"
#include "util/thread_pool.h"

namespace leveldb {

/////////////////////////////////////////////
// Tcache
/////////////////////////////////////////////
uint64_t kBlockSize = 4096UL;
uint64_t kDataSetSize = 134217728UL;
uint64_t kFidBatchNum = 100000UL;
uint64_t kCacheSize = 350000000000UL;
uint64_t kMetaBlockSize = 2000UL;
uint64_t kMetaTableSize = 500UL;
uint64_t kWriteBufferSize = 1048576UL;

class BlockCacheWritableFile;
class BlockCacheRandomAccessFile;
class BlockCacheImpl;

// Each SSD will New a BlockCache
// block state
uint64_t kCacheBlockValid = 0x1;
uint64_t kCacheBlockLocked = 0x2;
uint64_t kCacheBlockDfsRead = 0x4;
uint64_t kCacheBlockCacheRead = 0x8;
uint64_t kCacheBlockCacheFill = 0x10;

struct CacheBlock {
    uint64_t fid;
    uint64_t block_idx;
    uint64_t sid;
    uint64_t cache_block_idx;
    volatile uint64_t state;
    port::CondVar cv;
    Slice data_block;
    bool data_block_alloc;
    uint64_t data_block_refs;
    LRUHandle* handle;
    Status s;

    CacheBlock(port::Mutex* mu)
    : fid(0),
      block_idx(0),
      sid(0xffffffffffffffff),
      cache_block_idx(0xffffffffffffffff),
      state(0),
      cv(mu),
      data_block_alloc(false),
      data_block_refs(0),
      handle(NULL) {
    }

    bool Test(uint64_t c_state) {
        return (state & c_state) == c_state;
    }

    void Clear(uint64_t c_state) {
        state &= ~c_state;
    }

    void Set(uint64_t c_state) {
        state |= c_state;
    }

    void WaitOnClear(uint64_t c_state) { // access in lock
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

    void DecodeFrom(Slice record) {
        fid = DecodeFixed64(record.data());
        record.remove_prefix(sizeof(uint64_t));
        block_idx = DecodeFixed64(record.data());
        record.remove_prefix(sizeof(uint64_t));
        state = DecodeFixed64(record.data());
        return;
    }

    const std::string Encode() {
        std::string r;
        PutFixed64(&r, fid);
        PutFixed64(&r, block_idx);
        PutFixed64(&r, state);
        return r;
    }

    const std::string ToString() {
        std::stringstream ss;
        ss << "CacheBlock(" << (uint64_t)this << "): fid: " << fid << ", block_idx: " << block_idx
           << ", sid: " << sid << ", cache_block_idx: " << cache_block_idx
           << ", state " << state << ", status " << s.ToString();
        return ss.str();
    }
};

struct DataSet {
    Cache* cache;
    int fd;
};

class BlockCacheImpl {
public:
    BlockCacheImpl(const BlockCacheOptions& options);

    ~BlockCacheImpl();

    const std::string& WorkPath();

    Status LoadCache(); // init cache

    Status NewWritableFile(const std::string& fname,
                           WritableFile** result);

    Status NewRandomAccessFile(const std::string& fname,
                               RandomAccessFile** result); // cache Pread
    static void BlockDeleter(const Slice& key, void* v);

private:
    friend struct DataSet;
    struct LockContent;

    Status LockAndPut(LockContent& lc);

    Status FillCache(CacheBlock* block);

    Status ReadCache(CacheBlock* block);

    uint64_t AllocFileId(); // no more than fid_batch_num

    uint64_t FileId(const std::string& fname);

    DataSet* GetDataSet(uint64_t sid);

    CacheBlock* GetAndAllocBlock(uint64_t fid, uint64_t block_idx);

    Status LogRecord(CacheBlock* block);

    Status ReleaseBlock(CacheBlock* block, bool need_sync);

private:
    friend class BlockCacheWritableFile;
    friend class BlockCacheRandomAccessFile;
    friend struct CacheBlock;

    BlockCacheOptions options_;
    std::string work_path_;
    Env* dfs_env_;
    //Env* posix_env_;

    port::Mutex mu_;
    // key lock list
    struct Waiter {
        port::CondVar cv;
        int wait_num;
        bool done;
        Waiter(port::Mutex* mu):cv(mu), wait_num(0), done(false) {}
    };
    typedef std::map<std::string, Waiter*> LockKeyMap;
    LockKeyMap lock_key_;

    uint64_t new_fid_;
    uint64_t prev_fid_;

    enum LockKeyType {
        kDBKey = 0,
        kDataSetKey = 1,
    };
    struct LockContent {
        int type;

        // DB key
        Slice db_lock_key;
        Slice db_lock_val;
        std::string* db_val;

        // data set id
        uint64_t sid;
        DataSet* data_set;

        const std::string Encode() {
            if (type == kDBKey) {
                return db_lock_key.ToString();
            } else if (type == kDataSetKey) {
                std::string key = "DS#";
                PutFixed64(&key, sid);
                return key;
            }
            return "";
        }

        const std::string KeyToString() {
            if (type == kDBKey) {
                return db_lock_key.ToString();
            } else if (type == kDataSetKey) {
                std::stringstream ss;
                ss << "DS#" << sid;
                return ss.str();
            } else {
                return "";
            }
        }

        const std::string ValToString() {
            if (type == kDBKey) {
                uint64_t val = DecodeFixed64(db_lock_val.data());
                std::stringstream ss;
                ss << val;
                return ss.str();
            }
            return "";
        }
    };
    typedef std::map<uint64_t, DataSet*> DataSetMap;
    DataSetMap data_set_map_;

    //WritableFile* logfile_;
    //log::Writer* log_;
    DB* db_; // store meta
    ThreadPool bg_fill_;
    ThreadPool bg_read_;
    ThreadPool bg_flush_;
};

// Must insure not init more than twice
Env* NewBlockCacheEnv(Env* base) {
    return new BlockCacheEnv(base);
}

BlockCacheEnv::BlockCacheEnv(Env* base)
  : EnvWrapper(NewPosixEnv()), dfs_env_(base) {
    //target()->SetBackgroundThreads(30);
}

BlockCacheEnv::~BlockCacheEnv() {}

Status BlockCacheEnv::FileExists(const std::string& fname) {
    return dfs_env_->FileExists(fname);
}

Status BlockCacheEnv::GetChildren(const std::string& path,
                                  std::vector<std::string>* result) {
    return dfs_env_->GetChildren(path, result);
}

Status BlockCacheEnv::DeleteFile(const std::string& fname) {
    return dfs_env_->DeleteFile(fname);
}

Status BlockCacheEnv::CreateDir(const std::string& name) {
    return dfs_env_->CreateDir(name);
}

Status BlockCacheEnv::DeleteDir(const std::string& name) {
    return dfs_env_->DeleteDir(name);
}

Status BlockCacheEnv::CopyFile(const std::string& from,
                               const std::string& to) {
    return dfs_env_->CopyFile(from, to);
}

Status BlockCacheEnv::GetFileSize(const std::string& fname, uint64_t* size) {
    return dfs_env_->GetFileSize(fname, size);
}

Status BlockCacheEnv::RenameFile(const std::string& src, const std::string& target) {
    return dfs_env_->RenameFile(src, target);
}

Status BlockCacheEnv::LockFile(const std::string& fname, FileLock** lock) {
    return dfs_env_->LockFile(fname, lock);
}

Status BlockCacheEnv::UnlockFile(FileLock* lock) {
    return dfs_env_->UnlockFile(lock);
}

Status BlockCacheEnv::LoadCache(const BlockCacheOptions& opts, const std::string& cache_dir) {
    BlockCacheOptions options = opts;
    options.cache_dir = cache_dir;
    options.env = dfs_env_;
    options.cache_env = this->target();
    BlockCacheImpl* cache = new BlockCacheImpl(options);
    Status s = cache->LoadCache();
    assert(s.ok());
    cache_vec_.push_back(cache); // no need lock
    return s;
}

Status BlockCacheEnv::NewSequentialFile(const std::string& fname,
                                        SequentialFile** result) {
    return dfs_env_->NewSequentialFile(fname, result);
}

Status BlockCacheEnv::NewWritableFile(const std::string& fname,
                                      WritableFile** result) {
    if (fname.rfind(".sst") != fname.size() - 4) {
        return dfs_env_->NewWritableFile(fname, result);
    }

    // cache sst file
    uint32_t hash = (Hash(fname.c_str(), fname.size(), 13)) % cache_vec_.size();
    BlockCacheImpl* cache = cache_vec_[hash];
    Status s = cache->NewWritableFile(fname, result);
    Log("[block_cache %s] open file write: %s, hash: %u, status: %s\n",
        cache->WorkPath().c_str(), fname.c_str(), hash, s.ToString().c_str());
    return s;
}

Status BlockCacheEnv::NewRandomAccessFile(const std::string& fname,
                                          RandomAccessFile** result) {
    uint32_t hash = (Hash(fname.c_str(), fname.size(), 13)) % cache_vec_.size();
    BlockCacheImpl* cache = cache_vec_[hash];
    Status s = cache->NewRandomAccessFile(fname, result);
    Log("[block_cache %s] open file read: %s, hash: %u, status: %s\n",
        cache->WorkPath().c_str(), fname.c_str(), hash, s.ToString().c_str());
    return s;
}

class BlockCacheWriteBuffer {
public:
    BlockCacheWriteBuffer(const std::string& path,
                          const std::string& file,
                          int block_size)
        : offset_(0),
        block_size_(block_size),
        block_idx_(0),
        tmp_storage_(NULL),
        path_(path),
        file_(file) {
    }

    ~BlockCacheWriteBuffer() {
        assert(block_list_.size() == 0);
    }

    uint32_t NumFullBlock() { // use for BGFlush
        MutexLock l(&mu_);
        if (block_list_.size() == 0) {
            return 0;
        } else if ((block_list_.back())->size() < block_size_) {
            return block_list_.size() - 1;
        } else {
            return block_list_.size();
        }
    }

    Status Append(const Slice& data) {
        MutexLock l(&mu_);
        if (tmp_storage_ == NULL) {
            tmp_storage_ = new std::string();
            tmp_storage_->resize(0);
            block_list_.push_back(tmp_storage_);
        }
        uint32_t begin = offset_ / block_size_;
        uint32_t end = (offset_ + data.size()) / block_size_;
        if (begin == end) { // in the same block
            tmp_storage_->append(data.data(), data.size());
        } else {
            uint32_t tmp_size = block_size_ - (offset_ % block_size_);
            tmp_storage_->append(data.data(), tmp_size);
            assert(tmp_storage_->size() == block_size_);
            Slice buf(data.data() + tmp_size, data.size() - tmp_size);
            for (uint32_t i = begin + 1; i <= end; ++i) {
                tmp_storage_ = new std::string();
                tmp_storage_->resize(0);
                block_list_.push_back(tmp_storage_);
                if (i < end) { // last block
                    tmp_storage_->append(buf.data(), block_size_);
                    buf.remove_prefix(block_size_);
                } else { // last block
                    tmp_storage_->append(buf.data(), buf.size());
                }
                Log("[%s] add tmp_storage %s: offset: %lu, buf_size: %lu\n",
                    path_.c_str(),
                    file_.c_str(),
                    offset_,
                    buf.size());
            }
        }
        offset_ += data.size();
        Log("[%s] add record: %s, begin: %u, end: %u, offset: %lu, data_size: %lu, block_size: %u\n",
            path_.c_str(),
            file_.c_str(),
            begin, end,
            offset_ - data.size() , data.size(), block_size_);
        return Status::OK();
    }

    std::string* PopFrontBlock(uint64_t* block_idx) {
        MutexLock l(&mu_);
        if (block_list_.size() == 0) {
            return NULL;
        }
        std::string* block = block_list_.front();
        assert(block->size() <= block_size_);
        if (block->size() != block_size_) {
            return NULL;
        }
        block_list_.pop_front();
        *block_idx = block_idx_;
        block_idx_++;
        return block;
    }

    std::string* PopBackBlock(uint64_t* block_idx) {
        MutexLock l(&mu_);
        if (block_list_.size() == 0) {
            return NULL;
        }
        std::string* block = block_list_.back();
        block_list_.pop_back();
        *block_idx = offset_ / block_size_;
        return block;
    }

    void ReleaseBlock(std::string* block) {
        delete block;
    }

private:
    port::Mutex mu_;
    uint64_t offset_;
    uint32_t block_size_;
    uint64_t block_idx_;
    std::string* tmp_storage_;
    std::list<std::string*> block_list_; // kBlockSize
    std::string path_;
    std::string file_;
};

class BlockCacheWritableFile : public WritableFile {
public:
    BlockCacheWritableFile(BlockCacheImpl* c, const std::string& fname, Status* s)
        : cache_(c),
          bg_cv_(&c->mu_),
          bg_block_flush_(0),
          pending_block_num_(0),
          write_buffer_(cache_->WorkPath(), fname, cache_->options_.block_size),
          fname_(fname) { // file open
        *s = cache_->dfs_env_->NewWritableFile(fname_, &dfs_file_);
        Log("[%s] dfs open: %s, block_size: %lu, status: %s\n",
            cache_->WorkPath().c_str(),
            fname.c_str(),
            cache_->options_.block_size,
            s->ToString().c_str());

        MutexLock lockgard(&cache_->mu_);
        fid_ = cache_->FileId(fname_);
        return;
    }

    ~BlockCacheWritableFile() {
        if (dfs_file_ != NULL) {
            Log("[%s] dfs close for release %s\n", cache_->WorkPath().c_str(), fname_.c_str());
            dfs_file_->Close();
            delete dfs_file_;
            dfs_file_ = NULL;
        }

        Log("[%s] begin release %s\n", cache_->WorkPath().c_str(), fname_.c_str());
        MutexLock lockgard(&cache_->mu_);
        uint64_t block_idx;
        std::string* block_data = write_buffer_.PopBackBlock(&block_idx);
        if (block_data != NULL) {
            FillCache(block_data, block_idx);
        }

        while (bg_block_flush_ > 0) {
            bg_cv_.Wait();
        }
        Log("[%s] end release %s\n", cache_->WorkPath().c_str(), fname_.c_str());
        return;
    }

    Status Append(const Slice& data) {
        Status s = dfs_file_->Append(data);
        if (!s.ok()) {
            Log("[%s] dfs append fail: %s, status: %s\n",
                cache_->WorkPath().c_str(),
                fname_.c_str(),
                s.ToString().c_str());
            return s;
        }
        write_buffer_.Append(data);

        MutexLock lockgard(&cache_->mu_);
        MaybeScheduleBGFlush();
        return Status::OK();
    }

    Status Close() {
        Log("[%s] begin close %s\n", cache_->WorkPath().c_str(), fname_.c_str());
        Status s = dfs_file_->Close();
        delete dfs_file_;
        dfs_file_ = NULL;

        MutexLock lockgard(&cache_->mu_);
        uint64_t block_idx;
        std::string* block_data = write_buffer_.PopBackBlock(&block_idx);
        if (block_data != NULL) {
            FillCache(block_data, block_idx);
        }

        while (bg_block_flush_ > 0) {
            bg_cv_.Wait();
        }
        Log("[%s] end close %s, status %s\n", cache_->WorkPath().c_str(), fname_.c_str(),
            s.ToString().c_str());
        return s;
    }

    Status Flush() {
        Log("[%s] dfs flush: %s\n", cache_->WorkPath().c_str(), fname_.c_str());
        return dfs_file_->Flush();
    }

    Status Sync() {
        Log("[%s] dfs sync: %s\n", cache_->WorkPath().c_str(), fname_.c_str());
        return dfs_file_->Sync();
    }

private:
    void MaybeScheduleBGFlush() {
        cache_->mu_.AssertHeld();
        //Log("[%s] Maybe schedule BGFlush: %s, bg_block_flush: %u, block_nr: %u\n",
        //    cache_->WorkPath().c_str(),
        //    fname_.c_str(),
        //    bg_block_flush_,
        //    write_buffer_.NumFullBlock());
        while (bg_block_flush_ < (write_buffer_.NumFullBlock() + pending_block_num_)) {
            bg_block_flush_++;
            cache_->bg_flush_.Schedule(&BlockCacheWritableFile::BGFlushFunc, this, 10);
        }
    }

    static void BGFlushFunc(void* arg) {
        reinterpret_cast<BlockCacheWritableFile*>(arg)->BGFlush();
    }
    void BGFlush() {
        Log("[%s] Begin BGFlush: %s\n", cache_->WorkPath().c_str(), fname_.c_str());
        MutexLock lockgard(&cache_->mu_);
        uint64_t block_idx;
        std::string* block_data = write_buffer_.PopFrontBlock(&block_idx);
        if (block_data != NULL) {
            pending_block_num_++;
            FillCache(block_data, block_idx);
            pending_block_num_--;
        }

        bg_block_flush_--;
        MaybeScheduleBGFlush();
        bg_cv_.Signal();
        return;
    }

    Status FillCache(std::string* block_data, uint64_t block_idx) {
        cache_->mu_.AssertHeld();
        Status s;
        uint64_t fid = fid_;
        CacheBlock* block = NULL;
        while ((block = cache_->GetAndAllocBlock(fid, block_idx)) == NULL) {
            Log("[%s] fill cache for write %s, fid %lu, block_idx %lu, wait 10ms after retry\n",
                cache_->WorkPath().c_str(), fname_.c_str(),
                fid, block_idx);
            port::CondVar cv(&cache_->mu_);
            cv.Wait(10); // timewait 10ms retry
        }
        block->state = 0;
        block->GetDataBlock(cache_->options_.block_size, Slice(*block_data));
        cache_->mu_.Unlock();

        // Do io without lock
        block->s = cache_->LogRecord(block);
        if (block->s.ok()) {
            block->s = cache_->FillCache(block);
        }

        cache_->mu_.Lock();
        if (block->s.ok()) {
            block->state = kCacheBlockValid;
        }
        s = cache_->ReleaseBlock(block, true);
        write_buffer_.ReleaseBlock(block_data);
        return s;
    }

private:
    BlockCacheImpl* cache_;
    //port::AtomicPointer shutting_down_;
    port::CondVar bg_cv_;          // Signalled when background work finishes
    WritableFile* dfs_file_;
    // protected by cache_.mu_
    uint32_t bg_block_flush_;
    uint32_t pending_block_num_;
    BlockCacheWriteBuffer write_buffer_;
    std::string fname_;
    uint64_t fid_;
};

class BlockCacheRandomAccessFile : public RandomAccessFile {
public:
    BlockCacheRandomAccessFile(BlockCacheImpl* c, const std::string& fname, Status* s)
    : cache_(c),
      fname_(fname) {
        *s = cache_->dfs_env_->NewRandomAccessFile(fname_, &dfs_file_);
        Log("[%s] dfs open for read: %s, block_size: %lu, status: %s\n",
            cache_->WorkPath().c_str(),
            fname.c_str(),
            cache_->options_.block_size,
            s->ToString().c_str());

        MutexLock lockgard(&cache_->mu_);
        fid_ = cache_->FileId(fname_);
        return;
    }

    ~BlockCacheRandomAccessFile() {
        delete dfs_file_;
        return;
    }

    Status Read(uint64_t offset, size_t n, Slice* result,
                char* scratch) const {
        Status s;
        uint64_t begin = offset / cache_->options_.block_size;
        uint64_t end = (offset + n) / cache_->options_.block_size;
        assert(begin <= end);
        uint64_t fid = fid_;
        std::vector<CacheBlock*> c_miss;
        std::vector<CacheBlock*> c_locked;
        std::vector<CacheBlock*> c_valid;
        std::vector<CacheBlock*> block_queue;

        Log("[%s] Begin Pread %s, size %lu, offset %lu, fid %lu, start_block %lu, end_block %lu"
            ", block_size %lu\n",
            cache_->WorkPath().c_str(), fname_.c_str(), n, offset, fid,
            begin, end, cache_->options_.block_size);

        MutexLock lockgard(&cache_->mu_);
        for (uint64_t block_idx = begin; block_idx <= end; ++block_idx) {
            CacheBlock* block = NULL;
            while ((block = cache_->GetAndAllocBlock(fid, block_idx)) == NULL) {
                Log("[%s] fill cache for read %s, fid %lu, block_idx %lu, wait 10ms after retry\n",
                    cache_->WorkPath().c_str(), fname_.c_str(),
                    fid, block_idx);
                port::CondVar cv(&cache_->mu_);
                cv.Wait(10); // timewait 10ms retry
            }
            assert(block->fid == fid && block->block_idx == block_idx);
            block->GetDataBlock(cache_->options_.block_size, Slice());
            block_queue.push_back(block); // sort by block_idx

            if (!block->Test(kCacheBlockLocked) &&
                block->Test(kCacheBlockValid)) {
                block->Set(kCacheBlockLocked | kCacheBlockCacheRead);
                c_valid.push_back(block);
            } else if (!block->Test(kCacheBlockLocked)) {
                block->Set(kCacheBlockLocked | kCacheBlockDfsRead);
                c_miss.push_back(block);
            } else {
                c_locked.push_back(block);
            }

            Log("[%s] Queue block: %s, refs %u, data_block_refs %lu, alloc %u\n",
                cache_->WorkPath().c_str(), block->ToString().c_str(),
                block->handle->refs, block->data_block_refs,
                block->data_block_alloc);
        }
        cache_->mu_.Unlock();

        // async read miss data
        for (uint32_t i = 0; i < c_miss.size(); ++i) {
            CacheBlock* block = c_miss[i];
            AsyncDfsReader* reader = new AsyncDfsReader;
            reader->file = const_cast<BlockCacheRandomAccessFile*>(this);
            reader->block = block;
            //Log("[%s] pread in miss list, %s\n",
            //    cache_->WorkPath().c_str(),
            //    block->ToString().c_str());
            cache_->bg_read_.Schedule(&BlockCacheRandomAccessFile::AsyncDfsRead, reader, 10);
        }

        // async read valid data
        for (uint32_t i = 0; i < c_valid.size(); ++i) {
            CacheBlock* block = c_valid[i];
            AsyncCacheReader* reader = new AsyncCacheReader;
            reader->file = const_cast<BlockCacheRandomAccessFile*>(this);
            reader->block = block;
            //Log("[%s] pread in valid list, %s\n",
            //    cache_->WorkPath().c_str(),
            //    block->ToString().c_str());
            cache_->bg_read_.Schedule(&BlockCacheRandomAccessFile::AsyncCacheRead, reader, 10);
        }

        // wait async cache read done
        for (uint32_t i = 0; i < c_valid.size(); ++i) {
            MutexLock lockgard(&cache_->mu_);
            CacheBlock* block = c_valid[i];
            block->WaitOnClear(kCacheBlockCacheRead);
            assert(block->Test(kCacheBlockValid));
            if (!block->s.ok() && s.ok()) {
                s = block->s; // degrade read
            }
            block->Clear(kCacheBlockLocked);
            block->cv.SignalAll();
            Log("[%s] cache read done, %s\n",
                cache_->WorkPath().c_str(),
                block->ToString().c_str());
        }

        // wait dfs read done and async cache file
        for (uint32_t i = 0; i < c_miss.size(); ++i) {
            MutexLock lockgard(&cache_->mu_);
            CacheBlock* block = c_miss[i];
            block->WaitOnClear(kCacheBlockDfsRead);
            block->Set(kCacheBlockCacheFill);
            if (!block->s.ok() && s.ok()) {
                s = block->s; // degrade read
            }
            Log("[%s] dfs read done, %s\n",
                cache_->WorkPath().c_str(),
                block->ToString().c_str());
        }

        for (uint32_t i = 0; i < c_miss.size(); ++i) {
            CacheBlock* block = c_miss[i];
            AsyncCacheWriter* writer = new AsyncCacheWriter;
            writer->file = const_cast<BlockCacheRandomAccessFile*>(this);
            writer->block = block;
            //Log("[%s] pread in miss list(fill cache), %s\n",
            //    cache_->WorkPath().c_str(),
            //    block->ToString().c_str());
            cache_->bg_fill_.Schedule(&BlockCacheRandomAccessFile::AsyncCacheWrite, writer, 10);
        }

        for (uint32_t i = 0; i < c_miss.size(); ++i) { // wait cache fill finish
            MutexLock lockgard(&cache_->mu_);
            CacheBlock* block = c_miss[i];
            block->WaitOnClear(kCacheBlockCacheFill);
            if (block->s.ok()) {
                block->Set(kCacheBlockValid);
            } else if (s.ok()) {
                s = block->s; // degrade read
            }
            block->Clear(kCacheBlockLocked);
            block->cv.SignalAll();
            Log("[%s] cache fill done, %s\n",
                cache_->WorkPath().c_str(),
                block->ToString().c_str());
        }

        // wait other async read finish
        for (uint32_t i = 0; i < c_locked.size(); ++i) {
            MutexLock lockgard(&cache_->mu_);
            CacheBlock* block = c_locked[i];
            block->WaitOnClear(kCacheBlockLocked);
            Log("[%s] wait locked done, %s\n",
                cache_->WorkPath().c_str(),
                block->ToString().c_str());
        }

        // fill user mem
        size_t msize = 0;
        for (uint64_t block_idx = begin; block_idx <= end; ++block_idx) {
            CacheBlock* block = block_queue[block_idx - begin];
            Slice data_block = block->data_block;
            if (block_idx == begin) {
                data_block.remove_prefix(offset % cache_->options_.block_size);
            }
            if (block_idx == end) {
                data_block.remove_suffix(cache_->options_.block_size - (n + offset) % cache_->options_.block_size);
            }
            memcpy(scratch + msize, data_block.data(), data_block.size());
            msize += data_block.size();
            Log("[%s] Fill user data, %s, fill_offset %lu, fill_size %lu, prefix %lu, suffix %lu, msize %lu, offset %lu\n",
                cache_->WorkPath().c_str(), fname_.c_str(),
                block_idx * cache_->options_.block_size + (block_idx == begin ? offset % cache_->options_.block_size: 0),
                data_block.size(),
                block_idx == begin ? offset % cache_->options_.block_size: 0,
                block_idx == end ? cache_->options_.block_size - (n + offset) % cache_->options_.block_size
                                 : cache_->options_.block_size,
                msize, offset);
        }
        assert(msize == n);
        *result = Slice(scratch, n);

        cache_->mu_.Lock();
        for (uint32_t i = 0; i < c_miss.size(); ++i) {
            CacheBlock* block = c_miss[i];
            //Log("[%s] wakeup for miss, %s\n", cache_->WorkPath().c_str(), block->ToString().c_str());
            cache_->ReleaseBlock(block, true);
        }
        for (uint32_t i = 0; i < c_valid.size(); ++i) {
            CacheBlock* block = c_valid[i];
            //Log("[%s] wakeup for valid, %s\n", cache_->WorkPath().c_str(), block->ToString().c_str());
            cache_->ReleaseBlock(block, false);
        }
        for (uint32_t i = 0; i < c_locked.size(); ++i) {
            CacheBlock* block = c_locked[i];
            //Log("[%s] wakeup for lock, %s\n", cache_->WorkPath().c_str(), block->ToString().c_str());
            cache_->ReleaseBlock(block, false);
        }

        if (!s.ok()) {
            s = dfs_file_->Read(offset, n, result, scratch);
            Log("[%s] Pread degrade %s, offset %lu, size %lu, status %s\n",
                cache_->WorkPath().c_str(), fname_.c_str(),
                offset, n, s.ToString().c_str());
        }

        Log("[%s] End Pread %s, size %lu, offset %lu, fid %lu, res %lu, status %s, start_block %lu, end_block %lu"
            ", block_size %lu\n",
            cache_->WorkPath().c_str(), fname_.c_str(), n, offset, fid,
            result->size(), s.ToString().c_str(),
            begin, end, cache_->options_.block_size);
        return s;
    }

private:
    struct AsyncDfsReader {
        BlockCacheRandomAccessFile* file;
        CacheBlock* block;
    };
    static void AsyncDfsRead(void* arg) {
        AsyncDfsReader* reader = (AsyncDfsReader*)arg;
        reader->file->HandleDfsRead(reader);
        delete reader;
        return;
    }
    void HandleDfsRead(AsyncDfsReader* reader) {
        Status s;
        CacheBlock* block = reader->block;
        char* scratch = (char*)(block->data_block.data());
        Slice result;
        uint64_t offset = block->block_idx * cache_->options_.block_size;
        size_t n = cache_->options_.block_size;
        block->s = dfs_file_->Read(offset, n, &result, scratch);
        Log("[%s] dfs read, %s"
            ", offset %lu, size %lu, status %s, res %lu\n",
            cache_->WorkPath().c_str(), block->ToString().c_str(),
            offset, n,
            block->s.ToString().c_str(), result.size());

        MutexLock lockgard(&cache_->mu_);
        block->Clear(kCacheBlockDfsRead);
        block->cv.SignalAll();
        return;
    }

    struct AsyncCacheReader {
        BlockCacheRandomAccessFile* file;
        CacheBlock* block;
    };
    static void AsyncCacheRead(void* arg) {
        AsyncCacheReader* reader = (AsyncCacheReader*)arg;
        reader->file->HandleCacheRead(reader);
        delete reader;
        return;
    }
    void HandleCacheRead(AsyncCacheReader* reader) {
        CacheBlock* block = reader->block;
        block->s = cache_->ReadCache(block);

        MutexLock lockgard(&cache_->mu_);
        block->Clear(kCacheBlockCacheRead);
        block->cv.SignalAll();
        //Log("[%s] async.cacheread signal, %s\n", cache_->WorkPath().c_str(),
        //    block->ToString().c_str());
        return;
    }

    struct AsyncCacheWriter {
        BlockCacheRandomAccessFile* file;
        CacheBlock* block;
    };
    static void AsyncCacheWrite(void* arg) {
        AsyncCacheWriter* writer = (AsyncCacheWriter*)arg;
        writer->file->HandleCacheWrite(writer);
        delete writer;
        return;
    }
    void HandleCacheWrite(AsyncCacheWriter* writer) {
        CacheBlock* block = writer->block;
        //Log("[%s] cache fill, %s\n",
        //    cache_->WorkPath().c_str(),
        //    block->ToString().c_str());
        block->s = cache_->LogRecord(block);
        if (block->s.ok()) {
            block->s = cache_->FillCache(block);
        }

        MutexLock lockgard(&cache_->mu_);
        block->Clear(kCacheBlockCacheFill);
        block->cv.SignalAll();
        return;
    }

private:
    BlockCacheImpl* cache_;
    RandomAccessFile* dfs_file_;
    std::string fname_;
    uint64_t fid_;
};

// Tcache impl
BlockCacheImpl::BlockCacheImpl(const BlockCacheOptions& options)
    : options_(options),
      dfs_env_(options.env),
      new_fid_(0),
      prev_fid_(0),
      db_(NULL) {
    bg_fill_.SetBackgroundThreads(30);
    bg_read_.SetBackgroundThreads(30);
    bg_flush_.SetBackgroundThreads(30);
}

BlockCacheImpl::~BlockCacheImpl() {}

Status BlockCacheImpl::NewWritableFile(const std::string& fname,
                                       WritableFile** result) {
    Status s;
    BlockCacheWritableFile* file = new BlockCacheWritableFile(this, fname, &s);
    *result = NULL;
    if (s.ok()) {
        *result = (WritableFile*)file;
    }
    return s;
}

Status BlockCacheImpl::NewRandomAccessFile(const std::string& fname,
                                           RandomAccessFile** result) {
    Status s;
    BlockCacheRandomAccessFile* file = new BlockCacheRandomAccessFile(this, fname, &s);
    *result = NULL;
    if (s.ok()) {
        *result = (RandomAccessFile*)file;
    }
    return s;
}

void BlockCacheImpl::BlockDeleter(const Slice& key, void* v) {
    CacheBlock* block = (CacheBlock*)v;
    Log("Evict blockcache: %s\n", block->ToString().c_str());
    delete block;
    return;
}

// if lock succ, put lock_val, else get newer value
Status BlockCacheImpl::LockAndPut(LockContent& lc) {
    mu_.AssertHeld();
    Status s;
    std::string key;
    if ((key = lc.Encode()) == "") {
        return Status::NotSupported("key type error");
    }
    //Log("[%s] trylock key: %s\n",
    //    this->WorkPath().c_str(),
    //    key.c_str());

    Waiter* w = NULL;
    LockKeyMap::iterator it = lock_key_.find(key);
    if (it != lock_key_.end()){
        w = it->second;
        w->wait_num ++;
        while (!w->done) {
            w->cv.Wait();
        }
        mu_.Unlock();

        if (lc.type == kDBKey) {
            ReadOptions r_opts;
            s = db_->Get(r_opts, key, lc.db_val);
            //Log("[%s] get lock key: %s, val: %s, status: %s\n",
            //    this->WorkPath().c_str(),
            //    key.c_str(),
            //    lc.db_val->c_str(),
            //    s.ToString().c_str());
        } else if (lc.type == kDataSetKey) {
            lc.data_set = data_set_map_[lc.sid];
            //Log("[%s] get dataset sid: %lu\n",
            //    this->WorkPath().c_str(),
            //    lc.sid);
        }

        mu_.Lock();
        if (--w->wait_num == 0) {
            // last thread wait for open
            lock_key_.erase(key);
            //Log("[%s] wait done %s, delete cv\n",
            //    this->WorkPath().c_str(),
            //    key.c_str());
            delete w;
        } else {
            //Log("[%s] wait done %s, not last\n",
            //    this->WorkPath().c_str(),
            //    key.c_str());
        }
    } else {
        w = new Waiter(&mu_);
        w->wait_num = 1;
        lock_key_[key] = w;
        mu_.Unlock();

        if (lc.type == kDBKey) {
            WriteOptions w_opts;
            s = db_->Put(w_opts, key, lc.db_lock_val);
            if (s.ok()) {
                lc.db_val->append(lc.db_lock_val.data(), lc.db_lock_val.size());
            }
            Log("[%s] Insert db key : %s, val %s, status %s\n",
                this->WorkPath().c_str(),
                lc.KeyToString().c_str(),
                lc.ValToString().c_str(),
                s.ToString().c_str());
        } else if (lc.type == kDataSetKey) {
            std::string end_ds = "DS#";
            PutFixed64(&end_ds, lc.sid + 1);
            lc.data_set = new DataSet;
            lc.data_set->cache = New2QCache((options_.dataset_size / options_.block_size) + 1);// number of blocks in DS
            std::string file = options_.cache_dir + "/" + Uint64ToString(lc.sid);
            lc.data_set->fd = open(file.c_str(), O_RDWR | O_CREAT, 0644);
            assert(lc.data_set->fd > 0);
            Log("[%s] New DataSet %s, file: %s, nr_block: %lu, fd: %d\n",
                this->WorkPath().c_str(),
                lc.KeyToString().c_str(),
                file.c_str(), (options_.dataset_size / options_.block_size) + 1,
                lc.data_set->fd);

            // reload hash lru
            ReadOptions s_opts;
            leveldb::Iterator* db_it = db_->NewIterator(s_opts);
            for (db_it->Seek(key);
                 db_it->Valid() && db_it->key().ToString() < end_ds;
                 db_it->Next()) {
                Slice lkey = db_it->key();
                lkey.remove_prefix(3 + sizeof(uint64_t));// remove DS#sid
                //Slice lval = db_it->value();

                CacheBlock* block = new CacheBlock(&mu_);
                block->DecodeFrom(db_it->value()); // get fid and block_idx
                std::string hkey;
                PutFixed64(&hkey, block->fid);
                PutFixed64(&hkey, block->block_idx);
                block->sid = lc.sid;
                block->cache_block_idx = DecodeFixed64(lkey.data());
                block->state = (block->Test(kCacheBlockValid)) ? kCacheBlockValid : 0;
                Log("[%s] Recovery %s, insert cacheblock into 2QLru, %s\n",
                    this->WorkPath().c_str(),
                    lc.KeyToString().c_str(),
                    block->ToString().c_str());
                LRUHandle* handle = (LRUHandle*)(lc.data_set->cache->Insert(hkey, block, 1, &BlockCacheImpl::BlockDeleter));
                assert(handle != NULL);
                handle->cache_id = block->cache_block_idx;
                block->handle = handle;
                lc.data_set->cache->Release((Cache::Handle*)handle);
            }
            delete db_it;

            mu_.Lock();
            data_set_map_[lc.sid] = lc.data_set;
            mu_.Unlock();
        }

        mu_.Lock();
        if (--w->wait_num == 0) {
            lock_key_.erase(key);
            //Log("[%s] put done %s, no wait thread\n",
            //    this->WorkPath().c_str(),
            //    key.c_str());
            delete w;
        } else {
            //Log("[%s] put done %s, signal all wait thread\n",
            //    this->WorkPath().c_str(),
            //    key.c_str());
            w->done = true;
            w->cv.SignalAll();
        }
    }
    return s;
}

const std::string& BlockCacheImpl::WorkPath() {
    return work_path_;
}

Status BlockCacheImpl::LoadCache() {
    // open meta file
    work_path_ = options_.cache_dir;
    std::string dbname = options_.cache_dir + "/meta/";
    options_.opts.env = options_.cache_env; // local write
    options_.opts.filter_policy = NewBloomFilterPolicy(10);
    options_.opts.block_cache = leveldb::NewLRUCache(options_.meta_block_cache_size * 1024UL * 1024);
    options_.opts.table_cache = new leveldb::TableCache(options_.meta_table_cache_size * 1024UL * 1024);
    options_.opts.write_buffer_size = options_.write_buffer_size;
    options_.opts.info_log = Logger::DefaultLogger();
    Log("[block_cache %s] open meta db: block_cache: %lu, table_cache: %lu\n",
        dbname.c_str(),
        options_.meta_block_cache_size,
        options_.meta_table_cache_size);
    Status s = DB::Open(options_.opts, dbname, &db_);
    assert(s.ok());

    // recover fid
    std::string key = "FID#";
    std::string val;
    ReadOptions r_opts;
    s = db_->Get(r_opts, key, &val);
    if (!s.ok()) {
        prev_fid_ = 0;
    } else {
        prev_fid_ = DecodeFixed64(val.c_str());
    }
    new_fid_ = prev_fid_ + options_.fid_batch_num;
    Log("[block_cache %s]: reuse block cache: prev_fid: %lu, new_fid: %lu\n",
        dbname.c_str(), prev_fid_, new_fid_);
    s = Status::OK();
    return s;
}

Status BlockCacheImpl::FillCache(CacheBlock* block) {
    MutexLock l(&mu_);
    uint64_t sid = block->sid;
    uint64_t cache_block_idx = block->cache_block_idx;
    int fd = (data_set_map_[sid])->fd;
    mu_.Unlock();

    // do io without lock
    ssize_t res = pwrite(fd, block->data_block.data(), block->data_block.size(),
                         cache_block_idx * options_.block_size);
    Log("[%s] cache fill: sid %lu, dataset.fd %d, datablock size %lu, cb_idx %lu, %s, res %ld\n",
        this->WorkPath().c_str(), sid, fd, block->data_block.size(),
        cache_block_idx,
        block->ToString().c_str(),
        res);

    mu_.Lock();
    if (res < 0) {
        return Status::Corruption("FillCache error");
    }
    return Status::OK();
}

Status BlockCacheImpl::ReadCache(CacheBlock* block) {
    MutexLock l(&mu_);
    uint64_t sid = block->sid;
    uint64_t cache_block_idx = block->cache_block_idx;
    int fd = (data_set_map_[sid])->fd;
    mu_.Unlock();

    // do io without lock
    ssize_t res = pread(fd, (char*)block->data_block.data(), block->data_block.size(),
                         cache_block_idx * options_.block_size);
    Log("[%s] cache read: sid %lu, dataset.fd %d, datablock size %lu, cb_idx %lu, %s, res %ld\n",
        this->WorkPath().c_str(), sid, fd, block->data_block.size(),
        cache_block_idx,
        block->ToString().c_str(),
        res);

    mu_.Lock();
    if (res < 0) {
        return Status::Corruption("ReadCache error");
    }
    return Status::OK();
}

uint64_t BlockCacheImpl::AllocFileId() { // no more than fid_batch_num
    mu_.AssertHeld();
    uint64_t fid = ++new_fid_;
    while (new_fid_ - prev_fid_ >= options_.fid_batch_num) {
        std::string key = "FID#";
        std::string lock_val;
        PutFixed64(&lock_val, new_fid_);
        std::string val;

        LockContent lc;
        lc.type = kDBKey;
        lc.db_lock_key = key;
        lc.db_lock_val = lock_val;
        lc.db_val = &val;
        Status s = LockAndPut(lc);
        if (s.ok()) {
            prev_fid_ = DecodeFixed64(val.c_str());
        }
        Log("[%s] alloc fid: key %s, new_fid: %lu, prev_fid: %lu\n",
            this->WorkPath().c_str(),
            key.c_str(),
            new_fid_,
            prev_fid_);
    }
    return fid;
}

uint64_t BlockCacheImpl::FileId(const std::string& fname) {
    mu_.AssertHeld();
    uint64_t fid = 0;
    std::string key = "FNAME#" + fname;
    mu_.Unlock();

    ReadOptions r_opts;
    std::string val;
    Status s = db_->Get(r_opts, key, &val);
    if (!s.ok()) { // not exist
        MutexLock l(&mu_);
        fid = AllocFileId();
        std::string v;
        PutFixed64(&val, fid);

        LockContent lc;
        lc.type = kDBKey;
        lc.db_lock_key = key;
        lc.db_lock_val = val;
        lc.db_val = &v;
        //Log("[%s] alloc fid: %lu, key: %s",
        //    this->WorkPath().c_str(),
        //    fid, key.c_str());
        s = LockAndPut(lc);
        assert(s.ok());
        fid = DecodeFixed64(v.c_str());
    } else { // fid in cache
        fid = DecodeFixed64(val.c_str());
    }
    Log("[%s] Fid: %lu, fname: %s\n",
        this->WorkPath().c_str(),
        fid, fname.c_str());

    mu_.Lock();
    return fid;
}

DataSet* BlockCacheImpl::GetDataSet(uint64_t sid) {
    mu_.AssertHeld();
    DataSet* set = NULL;

    DataSetMap::iterator it = data_set_map_.find(sid);
    if (it == data_set_map_.end()) {
        LockContent lc;
        lc.type = kDataSetKey;
        lc.sid = sid;
        lc.data_set = NULL;
        Status s = LockAndPut(lc);
        set = lc.data_set;
    } else {
        //Log("[%s] get dataset from memcache, sid %lu\n",
        //    this->WorkPath().c_str(), sid);
        set = it->second;
    }
    return set;
}

CacheBlock* BlockCacheImpl::GetAndAllocBlock(uint64_t fid, uint64_t block_idx) {
    mu_.AssertHeld();
    std::string key;
    PutFixed64(&key, fid);
    PutFixed64(&key, block_idx);
    uint32_t hash = Hash(key.c_str(), key.size(), 7);
    uint64_t sid = hash % options_.dataset_num;

    //Log("[%s] alloc block, try get dataset, fid: %lu, block_idx: %lu, hash: %u, sid %lu, dataset_num: %lu\n",
    //    this->WorkPath().c_str(), fid, block_idx, hash, sid, options_.dataset_num);
    CacheBlock* block = NULL;
    DataSet* ds = GetDataSet(sid); // get and alloc ds
    Cache* cache = ds->cache;
    LRUHandle* h = (LRUHandle*)cache->Lookup(key);
    if (h == NULL) {
        block = new CacheBlock(&mu_);
        h = (LRUHandle*)cache->Insert(key, block, 1, &BlockCacheImpl::BlockDeleter);
        if (h == NULL) {
            delete block;
            return NULL;
        }
        block->fid = fid;
        block->block_idx = block_idx;
        block->sid = sid;
        block->cache_block_idx = h->cache_id;
        block->handle = h;
        Log("[%s] Alloc Block: %s, sid %lu, fid %lu, block_idx %lu, hash %u, dataset_nr %lu\n",
            this->WorkPath().c_str(),
            block->ToString().c_str(),
            sid, fid, block_idx, hash, options_.dataset_num);
    } else {
        block = reinterpret_cast<CacheBlock*>(cache->Value((Cache::Handle*)h));
        //Log("[%s] get block from memcache, %s\n",
        //        this->WorkPath().c_str(), block->ToString().c_str());
    }
    return block;
}

Status BlockCacheImpl::LogRecord(CacheBlock* block) {
    std::string key = "DS#";
    PutFixed64(&key, block->sid);
    PutFixed64(&key, block->cache_block_idx);
    leveldb::WriteBatch batch;
    batch.Put(key, block->Encode());
    return db_->Write(leveldb::WriteOptions(), &batch);
}

Status BlockCacheImpl::ReleaseBlock(CacheBlock* block, bool need_sync) {
    mu_.AssertHeld();
    Status s;

    mu_.Unlock();
    if (need_sync) {
        // TODO: dump meta into memtable
        s = LogRecord(block);
    }

    mu_.Lock();
    LRUHandle* h = block->handle;
    DataSet* ds = GetDataSet(block->sid); // get and alloc ds
    block->ReleaseDataBlock();
    Log("[%s] release block: %s\n", this->WorkPath().c_str(), block->ToString().c_str());
    block->s = Status::OK(); // clear io status
    block->cv.SignalAll();
    ds->cache->Release((Cache::Handle*)h);
    return s;
}

}  // namespace leveldb

