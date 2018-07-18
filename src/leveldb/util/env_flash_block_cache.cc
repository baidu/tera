// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/env_flash_block_cache.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <sstream>
#include <unordered_map>

#include "common/counter.h"
#include "db/table_cache.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/statistics.h"
#include "leveldb/status.h"
#include "leveldb/table_utils.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/flash_block_cache_impl.h"
#include "util/flash_block_cache_write_buffer.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/string_ext.h"
#include "util/thread_pool.h"

namespace leveldb {

class FlashBlockCacheWritableFile : public WritableFile {
public:
    FlashBlockCacheWritableFile(FlashBlockCacheImpl* c, const std::string& fname, 
                                const EnvOptions& options, Status* s)
        : cache_(c),
          bg_cv_(&mu_),
          bg_block_flush_(0),
          pending_block_num_(0),
          write_buffer_(cache_->WorkPath(), fname, cache_->options_.block_size),
          fname_(fname),
          env_opt_(options) { // file open
        *s = cache_->dfs_env_->NewWritableFile(fname_, &dfs_file_, env_opt_);
        if (!s->ok()) {
            Log("[%s] dfs open: %s, block_size: %lu, status: %s\n",
                 cache_->WorkPath().c_str(),
                 fname.c_str(),
                 cache_->options_.block_size,
                 s->ToString().c_str());
        }
        bg_status_ = *s;
        fid_ = cache_->FileId(fname_);
    }

    virtual ~FlashBlockCacheWritableFile() { Close(); }

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

        MutexLock lockgard(&mu_);
        MaybeScheduleBGFlush();
        return s;
    }

    Status Close() {
        Status s, s1;
        if (dfs_file_ != nullptr) {
            s = dfs_file_->Close();
            delete dfs_file_;
            dfs_file_ = nullptr;
        }

        uint64_t block_idx;
        std::string* block_data = write_buffer_.PopBackBlock(&block_idx);
        if (block_data != nullptr) {
            s1 = FillCache(block_data, block_idx);
        }

        MutexLock lockgard(&mu_);
        while (bg_block_flush_ > 0) {
            bg_cv_.Wait();
        }
        if (bg_status_.ok()) {
            bg_status_ = s.ok() ? s1: s;
        }
        //Log("[%s] end close %s, status %s\n", cache_->WorkPath().c_str(), fname_.c_str(),
            //s.ToString().c_str());
        return bg_status_;
    }

    Status Flush() {
        //Log("[%s] dfs flush: %s\n", cache_->WorkPath().c_str(), fname_.c_str());
        return dfs_file_->Flush();
    }

    Status Sync() {
        //Log("[%s] dfs sync: %s\n", cache_->WorkPath().c_str(), fname_.c_str());
        return dfs_file_->Sync();
    }

private:
    void MaybeScheduleBGFlush() {
        mu_.AssertHeld();
        //Log("[%s] Maybe schedule BGFlush: %s, bg_block_flush: %u, block_nr: %u\n",
            //cache_->WorkPath().c_str(),
            //fname_.c_str(),
            //bg_block_flush_,
            //write_buffer_.NumFullBlock());
        while (bg_block_flush_ < (write_buffer_.NumFullBlock() + pending_block_num_)) {
            bg_block_flush_++;
            cache_->bg_flush_.Schedule(&FlashBlockCacheWritableFile::BGFlushFunc, this, 10);
        }
    }

    static void BGFlushFunc(void* arg) {
        reinterpret_cast<FlashBlockCacheWritableFile*>(arg)->BGFlush();
    }
    void BGFlush() {
        //Log("[%s] Begin BGFlush: %s\n", cache_->WorkPath().c_str(), fname_.c_str());
        Status s;
        MutexLock lockgard(&mu_);
        uint64_t block_idx;
        std::string* block_data = write_buffer_.PopFrontBlock(&block_idx);
        if (block_data != nullptr) {
            pending_block_num_++;
            mu_.Unlock();

            s = FillCache(block_data, block_idx);
            mu_.Lock();
            pending_block_num_--;
        }

        bg_status_ = bg_status_.ok() ? s: bg_status_;
        bg_block_flush_--;
        MaybeScheduleBGFlush();
        bg_cv_.Signal();
    }

    Status FillCache(std::string* block_data, uint64_t block_idx) {
        Status s;
        uint64_t fid = fid_;
        CacheBlock* block = nullptr;
        while ((block = cache_->GetAndAllocBlock(fid, block_idx)) == nullptr) {
            Log("[%s] fill cache for write %s, fid %lu, block_idx %lu, wait 10ms after retry\n",
                cache_->WorkPath().c_str(), fname_.c_str(),
                fid, block_idx);
            cache_->options_.cache_env->SleepForMicroseconds(10000);
        }

        block->mu.Lock();
        block->state = 0;
        block->GetDataBlock(cache_->options_.block_size, Slice(*block_data));
        block->mu.Unlock();

        // Do io without lock
        block->s = cache_->LogRecord(block);
        if (block->s.ok()) {
            block->s = cache_->FillCache(block);
            if (block->s.ok()) {
                MutexLock l(&block->mu);
                block->state = kCacheBlockValid;
            }
        }
        s = cache_->ReleaseBlock(block, true);
        write_buffer_.ReleaseBlock(block_data);
        return s;
    }

private:
    FlashBlockCacheImpl* cache_;
    //port::AtomicPointer shutting_down_;
    port::Mutex mu_;
    port::CondVar bg_cv_;          // Signalled when background work finishes
    Status bg_status_;
    WritableFile* dfs_file_;
    // protected by cache_.mu_
    uint32_t bg_block_flush_;
    uint32_t pending_block_num_;
    FlashBlockCacheWriteBuffer write_buffer_;
    std::string fname_;
    uint64_t fid_;
    EnvOptions env_opt_;
};

class FlashBlockCacheRandomAccessFile : public RandomAccessFile {
public:
    FlashBlockCacheRandomAccessFile(FlashBlockCacheImpl* c, const std::string& fname,
                                    uint64_t fsize, const EnvOptions& options, Status* s)
    : cache_(c),
      fname_(fname),
      fsize_(fsize),
      env_opt_(options) {
        *s = cache_->dfs_env_->NewRandomAccessFile(fname_, &dfs_file_, env_opt_);
        //Log("[%s] dfs open for read: %s, block_size: %lu, status: %s\n",
            //cache_->WorkPath().c_str(),
            //fname.c_str(),
            //cache_->options_.block_size,
            //s->ToString().c_str());

        fid_ = cache_->FileId(fname_);
    }

    virtual ~FlashBlockCacheRandomAccessFile() {
        delete dfs_file_;
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

        //Log("[%s] Begin Pread %s, size %lu, offset %lu, fid %lu, start_block %lu, end_block %lu"
            //", block_size %lu\n",
            //cache_->WorkPath().c_str(), fname_.c_str(), n, offset, fid,
            //begin, end, cache_->options_.block_size);

        uint64_t start_ts = cache_->options_.cache_env->NowMicros();
        for (uint64_t block_idx = begin; block_idx <= end; ++block_idx) {
            uint64_t get_block_ts = cache_->options_.cache_env->NowMicros();
            CacheBlock* block = nullptr;
            while ((block = cache_->GetAndAllocBlock(fid, block_idx)) == nullptr) {
                Log("[%s] fill cache for read %s, fid %lu, block_idx %lu, wait 10ms after retry\n",
                    cache_->WorkPath().c_str(), fname_.c_str(),
                    fid, block_idx);
                cache_->options_.cache_env->SleepForMicroseconds(10000);
            }

            block->mu.Lock();
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
            block->mu.Unlock();

            //Log("[%s] Queue block: %s, refs %u, data_block_refs %lu, alloc %u\n",
                //cache_->WorkPath().c_str(), block->ToString().c_str(),
                //block->handle->refs, block->data_block_refs,
                //block->data_block_alloc);
            cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_GET_BLOCK,
                                       cache_->options_.cache_env->NowMicros() - get_block_ts);
        }
        uint64_t queue_ts = cache_->options_.cache_env->NowMicros();
        cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_QUEUE, queue_ts - start_ts);
        cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_BLOCK_NR, end - begin + 1);

        // async read miss data
        for (uint32_t i = 0; i < c_miss.size(); ++i) {
            CacheBlock* block = c_miss[i];
            AsyncDfsReader* reader = new AsyncDfsReader;
            reader->file = const_cast<FlashBlockCacheRandomAccessFile*>(this);
            reader->block = block;
            //Log("[%s] pread in miss list, %s\n",
                //cache_->WorkPath().c_str(),
                //block->ToString().c_str());
            cache_->bg_dfs_read_.Schedule(&FlashBlockCacheRandomAccessFile::AsyncDfsRead, reader, 10);
        }
        //uint64_t miss_read_sched_ts = cache_->options_.cache_env->NowMicros();

        // async read valid data
        for (uint32_t i = 0; i < c_valid.size(); ++i) {
            CacheBlock* block = c_valid[i];
            AsyncCacheReader* reader = new AsyncCacheReader;
            reader->file = const_cast<FlashBlockCacheRandomAccessFile*>(this);
            reader->block = block;
            //Log("[%s] pread in valid list, %s\n",
                //cache_->WorkPath().c_str(),
                //block->ToString().c_str());
            cache_->bg_read_.Schedule(&FlashBlockCacheRandomAccessFile::AsyncCacheRead, reader, 10);
        }
        //uint64_t ssd_read_sched_ts = cache_->options_.cache_env->NowMicros();

        // wait async cache read done
        for (uint32_t i = 0; i < c_valid.size(); ++i) {
            CacheBlock* block = c_valid[i];
            block->mu.Lock();
            block->WaitOnClear(kCacheBlockCacheRead);
            assert(block->Test(kCacheBlockValid));
            if (!block->s.ok() && s.ok()) {
                s = block->s; // degrade read
            }
            block->Clear(kCacheBlockLocked);
            block->cv.SignalAll();
            block->mu.Unlock();
            //Log("[%s] cache read done, %s\n",
                //cache_->WorkPath().c_str(),
                //block->ToString().c_str());
        }
        uint64_t ssd_read_ts = cache_->options_.cache_env->NowMicros();
        cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_SSD_READ, ssd_read_ts - queue_ts);

        // wait dfs read done and async cache file
        for (uint32_t i = 0; i < c_miss.size(); ++i) {
            CacheBlock* block = c_miss[i];
            block->mu.Lock();
            block->WaitOnClear(kCacheBlockDfsRead);
            block->Set(kCacheBlockCacheFill);
            if (!block->s.ok() && s.ok()) {
                s = block->s; // degrade read
            }
            block->mu.Unlock();
            //Log("[%s] dfs read done, %s\n",
                //cache_->WorkPath().c_str(),
                //block->ToString().c_str());
        }
        uint64_t dfs_read_ts = cache_->options_.cache_env->NowMicros();
        cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_DFS_READ, dfs_read_ts - ssd_read_ts);

        for (uint32_t i = 0; i < c_miss.size(); ++i) {
            CacheBlock* block = c_miss[i];
            AsyncCacheWriter* writer = new AsyncCacheWriter;
            writer->file = const_cast<FlashBlockCacheRandomAccessFile*>(this);
            writer->block = block;
            //Log("[%s] pread in miss list(fill cache), %s\n",
                //cache_->WorkPath().c_str(),
                //block->ToString().c_str());
            cache_->bg_fill_.Schedule(&FlashBlockCacheRandomAccessFile::AsyncCacheWrite, writer, 10);
        }
        uint64_t ssd_write_sched_ts = cache_->options_.cache_env->NowMicros();
        //cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_SSD_WRITE_SCHED, ssd_write_sched_ts - dfs_read_ts);

        for (uint32_t i = 0; i < c_miss.size(); ++i) { // wait cache fill finish
            CacheBlock* block = c_miss[i];
            block->mu.Lock();
            block->WaitOnClear(kCacheBlockCacheFill);
            if (block->s.ok()) {
                block->Set(kCacheBlockValid);
            } else if (s.ok()) {
                s = block->s; // degrade read
            }
            block->Clear(kCacheBlockLocked);
            block->cv.SignalAll();
            block->mu.Unlock();
            //Log("[%s] cache fill done, %s\n",
                //cache_->WorkPath().c_str(),
                //block->ToString().c_str());
        }
        uint64_t ssd_write_ts = cache_->options_.cache_env->NowMicros();
        cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_SSD_WRITE, ssd_write_ts - ssd_write_sched_ts);

        // wait other async read finish
        for (uint32_t i = 0; i < c_locked.size(); ++i) {
            CacheBlock* block = c_locked[i];
            block->mu.Lock();
            block->WaitOnClear(kCacheBlockLocked);
            block->mu.Unlock();
            //Log("[%s] wait locked done, %s\n",
                //cache_->WorkPath().c_str(),
                //block->ToString().c_str());
        }
        uint64_t wait_unlock_ts = cache_->options_.cache_env->NowMicros();
        cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_WAIT_UNLOCK, wait_unlock_ts - ssd_write_ts);

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
            //Log("[%s] Fill user data, %s, fill_offset %lu, fill_size %lu, prefix %lu, suffix %lu, msize %lu, offset %lu\n",
                //cache_->WorkPath().c_str(), fname_.c_str(),
                //block_idx * cache_->options_.block_size + (block_idx == begin ? offset % cache_->options_.block_size: 0),
                //data_block.size(),
                //block_idx == begin ? offset % cache_->options_.block_size: 0,
                //block_idx == end ? cache_->options_.block_size - (n + offset) % cache_->options_.block_size
                                 //: cache_->options_.block_size,
                //msize, offset);
        }
        assert(msize == n);
        *result = Slice(scratch, n);
        uint64_t fill_user_data_ts = cache_->options_.cache_env->NowMicros();
        cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_FILL_USER_DATA, fill_user_data_ts - wait_unlock_ts);

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
        uint64_t release_cache_block_ts = cache_->options_.cache_env->NowMicros();
        cache_->stat_->MeasureTime(FLASH_BLOCK_CACHE_PREAD_RELEASE_BLOCK, release_cache_block_ts - fill_user_data_ts);

        if (!s.ok()) {
            s = dfs_file_->Read(offset, n, result, scratch);
            Log("[%s] Pread degrade %s, offset %lu, size %lu, status %s\n",
                cache_->WorkPath().c_str(), fname_.c_str(),
                offset, n, s.ToString().c_str());
        }
        //Log("[%s] Done Pread %s, size %lu, offset %lu, fid %lu, res %lu, status %s, start_block %lu, end_block %lu"
            //", block_size %lu\n",
            //cache_->WorkPath().c_str(), fname_.c_str(), n, offset, fid,
            //result->size(), s.ToString().c_str(),
            //begin, end, cache_->options_.block_size);
        return s;
    }

private:
    struct AsyncDfsReader {
        FlashBlockCacheRandomAccessFile* file;
        CacheBlock* block;
    };
    static void AsyncDfsRead(void* arg) {
        AsyncDfsReader* reader = (AsyncDfsReader*)arg;
        reader->file->HandleDfsRead(reader);
        delete reader;
    }
    void HandleDfsRead(AsyncDfsReader* reader) {
        Status s;
        CacheBlock* block = reader->block;
        char* scratch = (char*)(block->data_block.data());
        Slice result;
        uint64_t offset = block->block_idx * cache_->options_.block_size;
        size_t n = cache_->options_.block_size;
        block->s = dfs_file_->Read(offset, n, &result, scratch);
        if (!block->s.ok()) {
            Log("[%s] dfs read, %s"
                ", offset %lu, size %lu, status %s, res %lu\n",
                cache_->WorkPath().c_str(), block->ToString().c_str(),
                offset, n,
                block->s.ToString().c_str(), result.size());
        }

        block->mu.Lock();
        block->Clear(kCacheBlockDfsRead);
        block->cv.SignalAll();
        block->mu.Unlock();
    }

    struct AsyncCacheReader {
        FlashBlockCacheRandomAccessFile* file;
        CacheBlock* block;
    };
    // use use thread module to enhance sync io
    static void AsyncCacheRead(void* arg) {
        AsyncCacheReader* reader = (AsyncCacheReader*)arg;
        reader->file->HandleCacheRead(reader);
        delete reader;
    }
    void HandleCacheRead(AsyncCacheReader* reader) {
        CacheBlock* block = reader->block;
        block->s = cache_->ReadCache(block);

        block->mu.Lock();
        block->Clear(kCacheBlockCacheRead);
        block->cv.SignalAll();
        block->mu.Unlock();
        //Log("[%s] async.cacheread signal, %s\n", cache_->WorkPath().c_str(),
            //block->ToString().c_str());
    }

    struct AsyncCacheWriter {
        FlashBlockCacheRandomAccessFile* file;
        CacheBlock* block;
    };
    static void AsyncCacheWrite(void* arg) {
        AsyncCacheWriter* writer = (AsyncCacheWriter*)arg;
        writer->file->HandleCacheWrite(writer);
        delete writer;
    }
    void HandleCacheWrite(AsyncCacheWriter* writer) {
        CacheBlock* block = writer->block;
        //Log("[%s] cache fill, %s\n",
            //cache_->WorkPath().c_str(),
            //block->ToString().c_str());
        block->s = cache_->LogRecord(block);
        if (block->s.ok()) {
            block->s = cache_->FillCache(block);
        }

        block->mu.Lock();
        block->Clear(kCacheBlockCacheFill);
        block->cv.SignalAll();
        block->mu.Unlock();
    }

private:
    FlashBlockCacheImpl* cache_;
    RandomAccessFile* dfs_file_;
    std::string fname_;
    uint64_t fid_;
    uint64_t fsize_;
    EnvOptions env_opt_;
};

// Must insure not init more than twice
Env* NewFlashBlockCacheEnv(Env* base) {
    return new FlashBlockCacheEnv(base);
}

FlashBlockCacheEnv::FlashBlockCacheEnv(Env* base)
  : EnvWrapper(NewPosixEnv()), dfs_env_(base) {
    //target()->SetBackgroundThreads(30);
}

FlashBlockCacheEnv::~FlashBlockCacheEnv() {}

Status FlashBlockCacheEnv::FileExists(const std::string& fname) {
    return dfs_env_->FileExists(fname);
}

Status FlashBlockCacheEnv::GetChildren(const std::string& path,
                                  std::vector<std::string>* result) {
    return dfs_env_->GetChildren(path, result);
}

Status FlashBlockCacheEnv::DeleteFile(const std::string& fname) {
    if (fname.rfind(".sst") == fname.size() - 4) {
        uint32_t hash = (Hash(fname.c_str(), fname.size(), 13)) % caches_.size();
        FlashBlockCacheImpl* cache = caches_[hash];
        cache->DeleteFile(fname);
    }
    return dfs_env_->DeleteFile(fname);
}

Status FlashBlockCacheEnv::CreateDir(const std::string& name) {
    return dfs_env_->CreateDir(name);
}

Status FlashBlockCacheEnv::DeleteDir(const std::string& name) {
    return dfs_env_->DeleteDir(name);
}

Status FlashBlockCacheEnv::CopyFile(const std::string& from,
                               const std::string& to) {
    return dfs_env_->CopyFile(from, to);
}

Status FlashBlockCacheEnv::GetFileSize(const std::string& fname, uint64_t* size) {
    return dfs_env_->GetFileSize(fname, size);
}

Status FlashBlockCacheEnv::RenameFile(const std::string& src, const std::string& target) {
    return dfs_env_->RenameFile(src, target);
}

Status FlashBlockCacheEnv::LockFile(const std::string& fname, FileLock** lock) {
    return dfs_env_->LockFile(fname, lock);
}

Status FlashBlockCacheEnv::UnlockFile(FileLock* lock) {
    return dfs_env_->UnlockFile(lock);
}

Status FlashBlockCacheEnv::LoadCache(const FlashBlockCacheOptions& opts, const std::string& cache_dir) {
    FlashBlockCacheOptions options = opts;
    options.cache_dir = cache_dir;
    options.env = dfs_env_;
    options.cache_env = this->target();
    FlashBlockCacheImpl* cache = new FlashBlockCacheImpl(options);
    Status s = cache->LoadCache();
    caches_.push_back(cache); // no need lock
    return s;
}

Status FlashBlockCacheEnv::NewSequentialFile(const std::string& fname,
                                        SequentialFile** result) {
    return dfs_env_->NewSequentialFile(fname, result);
}

Status FlashBlockCacheEnv::NewWritableFile(const std::string& fname,
                                           WritableFile** result,
                                           const EnvOptions& options) {
    if (fname.rfind(".sst") != fname.size() - 4) {
        return dfs_env_->NewWritableFile(fname, result, options);
    }

    // cache sst file
    *result = nullptr;
    Status s;
    uint32_t hash = (Hash(fname.c_str(), fname.size(), 13)) % caches_.size();
    FlashBlockCacheImpl* cache = caches_[hash];
    FlashBlockCacheWritableFile* file = new FlashBlockCacheWritableFile(cache, fname, options, &s);
    if (s.ok()) {
        *result = (WritableFile*)file;
    } else {
        delete file;
        file = nullptr;
        *result = nullptr;
        Log("[flash_block_cache %s] open file write fail: %s, hash: %u, status: %s\n",
             cache->WorkPath().c_str(), fname.c_str(), hash, s.ToString().c_str());
    }
    return s;
}

Status FlashBlockCacheEnv::NewRandomAccessFile(const std::string& fname,
                                               RandomAccessFile** result,
                                               const EnvOptions&) {
    // never use it
    abort();
    return Status::OK();
}

Status FlashBlockCacheEnv::NewRandomAccessFile(const std::string& fname,
                                               uint64_t fsize,
                                               RandomAccessFile** result,
                                               const EnvOptions& options) {
    *result = nullptr;
    Status s;
    uint32_t hash = (Hash(fname.c_str(), fname.size(), 13)) % caches_.size();
    FlashBlockCacheImpl* cache = caches_[hash];
    FlashBlockCacheRandomAccessFile* file = new FlashBlockCacheRandomAccessFile(cache, fname, fsize, options, &s);
    if (s.ok()) {
        *result = (RandomAccessFile*)file;
    } else {
        delete file;
        file = nullptr;
        *result = nullptr;
        Log("[flash_block_cache %s] open file read fail: %s, hash: %u, status: %s, fsize %lu\n",
             cache->WorkPath().c_str(), fname.c_str(), hash, s.ToString().c_str(), fsize);
    }
    return s;
}

}  // namespace leveldb

