// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/env_cache.h"

#include <deque>
#include <errno.h>
#include <stdio.h>

#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/env_dfs.h"
#include "leveldb/slog.h"
#include "leveldb/status.h"
#include "leveldb/table_utils.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/hash.h"
// #include "util/md5.h" // just for debug, so not svn-ci
#include "util/mutexlock.h"
#include "util/string_ext.h"

namespace leveldb {

uint32_t ThreeLevelCacheEnv::s_mem_cache_size_in_KB_(256);
uint32_t ThreeLevelCacheEnv::s_disk_cache_size_in_MB_(1);
uint32_t ThreeLevelCacheEnv::s_block_size_(8 * 1024);
uint32_t ThreeLevelCacheEnv::s_disk_cache_file_num_(1);
std::string ThreeLevelCacheEnv::s_disk_cache_file_name_("tera.cache");

const char* paths[] = {"./cache_dir_1/", "./cache_dir_2/"};
std::vector<std::string> ThreeLevelCacheEnv::cache_paths_(paths, paths + 2);

static Status IOError(const std::string& context, int err_number) {
    return Status::IOError(context, strerror(err_number));
}

class LRU_BlockCache {
public:
    LRU_BlockCache(uint32_t block_size,
                   void (*deleter)(const Slice& key, void* value),
                   Cache* global_lru_cache =  NULL)
        : cache_(global_lru_cache), block_size_(block_size),
          created_own_cache_(false), deleter_(deleter) {
        if (cache_ == NULL) {
            cache_ = NewLRUCache(256 * 1024);
            created_own_cache_ = true;
        }
    }

    virtual ~LRU_BlockCache() {
        if (created_own_cache_) {
            delete cache_;
        }
    }

    Status WriteBlockCache(const std::string& fname, uint32_t block_no,
                           const Slice& data) {
        char *buf = NULL;
        uint32_t buf_size = 0;
        PackSlice(data, &buf, &buf_size);
        Cache::Handle* handle = cache_->Insert(HashKey(fname, block_no),
                                               buf, buf_size, deleter_);
        cache_->Release(handle);
        return Status::OK();
    }

    Status ReadBlockCache(const std::string& fname, uint32_t block_no,
                          uint64_t block_offset, uint64_t n,
                          Slice* result) {
        Cache::Handle* handle = cache_->Lookup(HashKey(fname, block_no));
        if (handle == NULL) {
            return Status::IOError("not loaded in mem-cache");
        }
        Slice block_slice = UnpackSlice((char*)cache_->Value(handle));
        if (n > block_slice.size()) {
            n = block_slice.size();
        }
        *result = Slice(block_slice.data() + block_offset, n);
        cache_->Release(handle);
        return Status::OK();
    }

    Status AppendCache(const Slice& data, const std::string& fname,
                       uint32_t start_block_no) {
        Slice block_slice;
        Status s = ReadBlockCache(fname, start_block_no, 0, block_size_, &block_slice);
        if (!s.ok()) {
            LDB_SLOG(TRACE, "not exit block #%d", start_block_no);
            s = Status::OK();
        }

        uint32_t fill_size = block_size_ - block_slice.size();
        if (fill_size > 0) {
            std::string new_block_str(block_slice.data(), block_slice.size());
            if (data.size() < fill_size) {
                fill_size = data.size();
            }
            new_block_str.append(data.data(), fill_size);
            s = WriteBlockCache(fname, start_block_no, new_block_str);
            if (!s.ok()) {
                LDB_SLOG(TRACE, "fail to append tail on block #%d. %s",
                     start_block_no, s.ToString().c_str());
                return s;
            }
        }
        int64_t rest_size = data.size() - fill_size;
        uint32_t start_pos = fill_size;
        while (rest_size > 0) {
            uint32_t avail = block_size_;
            if (avail > rest_size) {
                avail = rest_size;
            }
            s = WriteBlockCache(fname, ++start_block_no,
                                Slice(data.data() + start_pos, avail));
            if (!s.ok()) {
                LDB_SLOG(TRACE, "fail to append on block #%d. %s",
                     start_block_no, s.ToString().c_str());
                return s;
            }
            rest_size -= avail;
            start_pos += avail;
        }
        return s;
    }

    void DropBlockCache(const std::string& fname, uint32_t block_no) {
        cache_->Erase(HashKey(fname, block_no));
    }

    bool IsCacheLoaded(const std::string& fname, uint32_t block_no) {
        Cache::Handle* handle = cache_->Lookup(HashKey(fname, block_no));
        bool is_loaded = (handle != NULL);
        if (is_loaded) {
            cache_->Release(handle);
        }
        return is_loaded;
    }

    void ResetCache(uint32_t capacity) {
        // just for testing, not thread-secure support
        delete cache_;
        cache_ = NewLRUCache(capacity);
    }

protected:
    virtual Slice HashKey(const std::string& fname,
                          uint32_t block_no) {
        return fname + "###" + Uint64ToString(block_no);
    }

    void PackSlice(const Slice& slice, char** buf, uint32_t* buf_size) {
        uint32_t len = slice.size();
        *buf_size = len + sizeof(uint32_t);
        *buf = new char[*buf_size];
        memcpy(*buf, &len, sizeof(uint32_t));
        memcpy(*buf + sizeof(uint32_t), slice.data(), len);
    }

    static Slice UnpackSlice(const char* buf) {
        uint32_t len = 0;
        memcpy(&len, buf, sizeof(uint32_t));
        return Slice(buf + sizeof(uint32_t), len);
    }

    void PrintMD5(uint32_t block_no, const Slice& data,
                  const std::string& tips = "block") {
//         MD5 md5(data.ToString());
//         LDB_SLOG(INFO, "%s #%d md5: %s\n", tips.c_str(),
//                 block_no, md5.toString().c_str());
    }

protected:
    Cache* cache_;
    const uint32_t block_size_;
    bool created_own_cache_;

    void (*deleter_)(const Slice& key, void* value);
};

class LRU_MemCache : public LRU_BlockCache {
public:
    LRU_MemCache(uint32_t block_size,
                 Cache* global_lru_cache =  NULL)
        : LRU_BlockCache(block_size, &LRU_MemCache::Deleter, global_lru_cache) {}
    virtual ~LRU_MemCache() {}

    static void Deleter(const Slice& key, void* v) {
        LDB_SLOG(TRACE, "mem-cache: delete key: %s",
             key.ToString().c_str());
        delete[] (char*)v;
    }

    Status WriteBlock(const std::string& fname,
                      uint32_t block_no, const Slice& data) {
        return WriteBlockCache(fname, block_no, data);
    }

    Status ReadBlock(const std::string& fname, uint32_t block_no,
                     uint64_t block_offset, uint64_t n,
                     Slice* result) {
        return ReadBlockCache(fname, block_no, block_offset, n, result);
    }

    Status Append(const Slice& data, const std::string& fname,
                  uint32_t start_block_no) {
        return AppendCache(data, fname, start_block_no);
    }

    bool IsLoaded(const std::string& fname, uint32_t block_no) {
        return IsCacheLoaded(fname, block_no);
    }

    void Reset() {
        ResetCache((ThreeLevelCacheEnv::s_mem_cache_size_in_KB_ << 10));
    }

private:
    LRU_MemCache(const LRU_MemCache&);
    void operator=(const LRU_MemCache&);
};

class LRU_DiskCache;
static LRU_DiskCache* g_disk_cache;
static LRU_MemCache* g_mem_cache;

class LRU_DiskCache : public LRU_BlockCache {
public:
    LRU_DiskCache(const std::string& cache_fname, uint32_t block_size,
                  uint32_t cache_size_in_MB, uint32_t file_num = 1,
                  Cache* global_lru_cache = NULL)
        : LRU_BlockCache(block_size, &LRU_DiskCache::Deleter, global_lru_cache),
          fname_(cache_fname), blocks_num_(0), file_num_(file_num) {
        blocks_num_ = ((((uint64_t)cache_size_in_MB) << 20) + block_size - 1) / block_size;
        Status s = OpenFile();
        if (!s.ok()) {
            LDB_SLOG(FATAL, "fail to create cache file: %s, status: #%s",
                 cache_fname.c_str(), s.ToString().c_str());
        }
    }

    virtual ~LRU_DiskCache() {
        Close();
        delete[] fps_;
        CleanFile();
    }

    static void Deleter(const Slice& key, void* v) {
        uint32_t block = 0;
        uint32_t size = 0;;
        uint32_t crc32c = 0;
        Slice buf = UnpackSlice((char*)v);
        UnpackCacheSlice(buf, &block, &size, &crc32c);
        LDB_SLOG(TRACE, "disk-cache: delete key: %s [local block #%d]",
             key.ToString().c_str(), block);

        g_disk_cache->DropBlock(block);
        delete[] (char*)v;
    }

    Status WriteBlock(const std::string& fname, uint32_t block_no,
                      const Slice& data, bool force = false) {
        Status s;
        if (IsCacheLoaded(fname, block_no)) {
            Slice cache_slice;
            s = ReadBlockCache(fname, block_no, 0, block_size_, &cache_slice);
            if (!s.ok()) {
                DropBlockCache(fname, block_no);
            } else {
                uint32_t local_no = 0;
                uint32_t local_size = 0;
                uint32_t crc32c = 0;
                UnpackCacheSlice(cache_slice, &local_no, &local_size, &crc32c);
                if (crc32c == crc32c::Value(data.data(), data.size())) {
                    return Status::OK();
                }

                if (force) {
                    DropBlockCache(fname, block_no);
                } else {
                    return Status::IOError("can not overwrite existing blocke #"
                                           + Uint64ToString(block_no));
                }
            }
        }

        uint32_t free_block_no = blocks_num_ + 1;
        if (blocks_free_.size() > 0) {
            free_block_no = blocks_free_.front();
            blocks_free_.pop_front();
        }

        LDB_SLOG(TRACE, "write block #%d to disk-cache block #%d",
             block_no, free_block_no);
        s = WriteBlockLocal(free_block_no, data);
        if (!s.ok()) {
            LDB_SLOG(TRACE, "fail to write block to disk-cache: %s", s.ToString().c_str());
            return s;
        }
        Slice cache_slice = PackCacheSlice(free_block_no, data.size(),
                                           crc32c::Value(data.data(), data.size()));
        s = WriteBlockCache(fname, block_no, cache_slice);
        return s;
    }

    Status ReadBlock(const std::string& fname, uint32_t block_no,
                     uint64_t block_offset, uint64_t n,
                     Slice* result, char* scratch) {
        Slice cache_slice;
        Status s = ReadBlockCache(fname, block_no, 0, block_size_, &cache_slice);
        if (!s.ok()) {
            LDB_SLOG(TRACE, "fail to load meta, block #%d. %s",
                 block_no, s.ToString().c_str());
            return s;
        }

        uint32_t local_block_no = 0;
        uint32_t local_block_size = 0;
        uint32_t crc32c = 0;
        bool check_crc32 = false;
        if (UnpackCacheSlice(cache_slice, &local_block_no,
                             &local_block_size, &crc32c)) {
            check_crc32 = true;
        }
        if (block_offset + n > local_block_size) {
            return Status::IOError("offset beyond existing block size");
        }
        LDB_SLOG(TRACE, "read local block #%d for block #%d",
             local_block_no, block_no);
        Slice block_slice;
        s = ReadBlockLocal(local_block_no, local_block_size, &block_slice, scratch);
        if (!s.ok()) {
            LDB_SLOG(TRACE, "fail to load data from disk-cache, block #%d. %s",
                 local_block_no, s.ToString().c_str());
            return s;
        }
        if (check_crc32 && crc32c != crc32c::Value(block_slice.data(), block_slice.size())) {
            return Status::IOError("data corruption, local block #"
                                   + Uint64ToString(local_block_no));
        }
        *result = Slice(block_slice.data() + block_offset, n);
        return Status::OK();
    }

    Status DropBlock(uint32_t cache_block_no) {
        blocks_free_.push_front(cache_block_no);
        return Status::OK();
    }

    void DropBlock(const std::string& fname, uint32_t block_no) {
        Slice cache_slice;
        Status s = ReadBlockCache(fname, block_no, 0, block_size_, &cache_slice);
        if (!s.ok()) {
            return;
        }

        uint32_t local_block_no = 0;
        uint32_t local_block_size = 0;
        uint32_t crc32c = 0;
        UnpackCacheSlice(cache_slice, &local_block_no,
                         &local_block_size, &crc32c);
        LDB_SLOG(TRACE, "drop local block #%d for block #%d", local_block_no, block_no);
        DropBlockCache(fname, block_no);
        DropBlock(local_block_no);
    }

    Status Append(const std::string& fname,
                  const Slice& data, uint32_t start_block_no) {
        uint32_t start_pos = 0;
        int64_t rest_size = data.size();

        Slice cache_slice;
        Status s = ReadBlockCache(fname, start_block_no, 0, block_size_,
                                  &cache_slice);
        if (s.ok()) {
            uint32_t local_no = 0;
            uint32_t local_size = 0;
            uint32_t crc32c = 0;
            UnpackCacheSlice(cache_slice, &local_no, &local_size, &crc32c);
            if (local_size != block_size_) {
                char scratch[block_size_];
                Slice result;
                s = ReadBlockLocal(local_no, local_size, &result, scratch);
                if (s.ok() && crc32c == crc32c::Value(result.data(), result.size())) {
                    uint32_t write_size = block_size_ - local_size;
                    if (write_size > rest_size) {
                        write_size = rest_size;
                    }
                    memcpy(scratch + local_size, data.data(), write_size);
                    s = WriteBlock(fname, start_block_no,
                                   Slice(scratch, local_size + write_size), true);
                    if (!s.ok()) {
                        LDB_SLOG(TRACE, "fail to append local block #%d",
                             start_block_no);
                        return s;
                    }
                    start_pos = write_size;
                    rest_size = data.size() - start_pos;
                    start_block_no++;
                } else if (!s.ok()){
                    LDB_SLOG(ERROR, "fail to read exist data from disk-cache. %s",
                         s.ToString().c_str());
                    return s;
                }
            }
        } else {
            s = Status::OK();
        }
        while (rest_size > 0) {
            uint32_t avail = block_size_;
            if (avail > rest_size) {
                avail = rest_size;
            }
            s = WriteBlock(fname, start_block_no, Slice(data.data() + start_pos, avail));
            if (!s.ok()) {
                return Status::IOError("fail to append on block #"
                                       + Uint64ToString(start_block_no));

            }
            rest_size -= avail;
            start_pos += avail;
            start_block_no++;
        }
        return s;
    }

    bool IsLoaded(uint32_t block_no) {
        return IsCacheLoaded(fname_, block_no);
    }

    Status Flush() {
        for (uint32_t i = 0; i < file_num_; ++i) {
            if (fflush(fps_[i]) != 0) {
                return IOError(CacheName(fname_, i), errno);
            }
        }
        return Status::OK();
    }

    Status Sync() {
        return Flush();
    }

    Status Close() {
        for (uint32_t i = 0; i < file_num_; ++i) {
            fclose(fps_[i]);
        }
        return Status::OK();
    }

    void Reset() {
        // just for test
        ResetCache((blocks_num_ * sizeof(uint32_t) * 3));
    }

private:
    LRU_DiskCache(const LRU_DiskCache&);
    void operator=(const LRU_DiskCache&);

public:
    static Slice PackCacheSlice(uint32_t block_no, uint32_t block_size,
                                uint32_t crc32) {
        char* buf = new char[sizeof(uint32_t) * 3];
        memcpy(buf, &block_no, sizeof(uint32_t));
        memcpy(buf + sizeof(uint32_t), &block_size, sizeof(uint32_t));
        memcpy(buf + sizeof(uint32_t) * 2, &crc32, sizeof(uint32_t));
        return Slice(buf, sizeof(uint32_t) * 3);
    }

    static bool UnpackCacheSlice(const Slice& slice, uint32_t* block_no,
                                 uint32_t* block_size, uint32_t* crc32c) {
        if (slice.size() >= sizeof(uint32_t) * 3) {
            memcpy(block_no, slice.data(), sizeof(uint32_t));
            memcpy(block_size, slice.data() + sizeof(uint32_t), sizeof(uint32_t));
            memcpy(crc32c, slice.data() + sizeof(uint32_t) * 2, sizeof(uint32_t));
            return true;
        } else if (slice.size() >= sizeof(uint32_t) * 2) {
            memcpy(block_no, slice.data(), sizeof(uint32_t));
            memcpy(block_size, slice.data() + sizeof(uint32_t), sizeof(uint32_t));
        } else if (slice.size() >= sizeof(uint32_t)) {
            memcpy(block_no, slice.data(), sizeof(uint32_t));
        } else {
            assert(false);
        }
        return false;
    }

private:
    Status OpenFile() {
        fps_ = (FILE**)malloc(file_num_ * sizeof(FILE));
        for (uint32_t i = 0; i < file_num_; ++i) {
            std::string cache_file = CacheName(fname_, i);
            fps_[i] = fopen(cache_file.c_str(), "w+");
            if (fps_[i] == NULL) {
                return IOError(cache_file, errno);
            }
        }

        for (uint32_t i = 0; i < blocks_num_; ++i) {
            blocks_free_.push_back(i);
        }
        return Status::OK();
    }

    Status CleanFile() {
        for (uint32_t i = 0; i < file_num_; ++i) {
            std::string cache_file = CacheName(fname_, i);
            if (unlink(cache_file.c_str()) != 0) {
                return IOError(cache_file, errno);
            }
        }
        return Status::OK();
    }

    Status ReadBlockLocal(uint32_t block_no, uint32_t n, Slice* result, char* scratch) {
        uint32_t loc = LocateCache(block_no);
        if (fseek(fps_[loc], LocateBlockInCache(loc, block_no) * block_size_,
                  SEEK_SET) < 0) {
            return IOError(fname_, errno);
        }

        if (fread(scratch, sizeof(char), n, fps_[loc]) != n) {
            return IOError("fail to load block from disk-cache", errno);
        }
        *result = Slice(scratch, n);
        return Status::OK();
    }

    Status WriteBlockLocal(uint32_t block_no, const Slice& data,
                           bool force = false) {
        size_t len = data.size();
        if (len == 0) {
            return Status::OK();
        }

        if (len > block_size_) {
            len = block_size_;
        }
        uint32_t loc = LocateCache(block_no);
        if (fseek(fps_[loc], LocateBlockInCache(loc, block_no) * block_size_,
                  SEEK_SET) < 0) {
            return IOError(fname_, errno);
        }

        if (fwrite(data.data(), sizeof(char), len, fps_[loc]) == 0) {
            return IOError(fname_, errno);
        }
        return Status::OK();
    }

    std::string CacheName(const std::string& fname, uint32_t cache_no) {
        std::string cache_name = fname + "." + Uint64ToString(cache_no);
        const std::vector<std::string>& paths = ThreeLevelCacheEnv::GetCachePaths();
        return paths[cache_no % paths.size()] + cache_name;
    }

    uint32_t LocateCache(uint32_t block_no) {
        uint32_t i = 1;
        for (; i < file_num_; ++i) {
            if (block_no < i * blocks_num_ / file_num_) {
                break;
            }
        }
        return i - 1;
    }

    uint32_t LocateBlockInCache(uint32_t loc, uint32_t block_no) {
        return block_no - (loc * blocks_num_) / file_num_;
    }

private:
    FILE** fps_;
    std::string fname_;
    uint32_t blocks_num_;
    const uint32_t file_num_;

    std::deque<uint32_t> blocks_free_;
};

class DiskCacheReader {
public:
    DiskCacheReader(const std::string& fname_hdfs, Env* hdfs_env,
                    LRU_MemCache* mem_cache = NULL, LRU_DiskCache* disk_cache = NULL)
        : dfs_env_(hdfs_env), hdfs_file_(NULL), fname_hdfs_(fname_hdfs),
          size_(0), mem_cache_(mem_cache), disk_cache_(disk_cache),
          mem_cache_created_own_(false), disk_cache_created_own_(false),
          block_size_(ThreeLevelCacheEnv::s_block_size_) {
        if (!mem_cache_) {
            mem_cache_ = new LRU_MemCache(block_size_);
            mem_cache_created_own_ = true;
        }
        if (!disk_cache_) {
            disk_cache_ = new LRU_DiskCache(
                ThreeLevelCacheEnv::CachePath(fname_hdfs) + fname_hdfs + ".cache",
                block_size_, 10);
            disk_cache_created_own_ = true;
        }
        Status s = OpenFile();
        if (size_ > 0) {
            s = dfs_env_->NewRandomAccessFile(fname_hdfs_, &hdfs_file_);
        }
        assert(s.ok());
    }

    ~DiskCacheReader() {
        if (mem_cache_created_own_) {
            delete mem_cache_;
        }
        if (disk_cache_created_own_) {
            delete disk_cache_;
        }
    }

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) {
        if (offset > size_) {
            return Status::IOError("offset beyond file size. [offset: "
                                   + Uint64ToString(offset) + ", size: "
                                   + Uint64ToString(size_));
        }
        const uint64_t available = size_ - offset;
        if (n > available) {
            n = available;
        }
        if (n == 0) {
            *result = Slice();
            return Status::OK();
        }

        uint32_t block = offset / block_size_;
        uint32_t block_offset = offset % block_size_;

        if (n <= block_size_ - block_offset) {
            return ReadFromBlock(block, block_offset, n, result, scratch);
        }
        size_t bytes_to_copy = n;
        char* dst = scratch;
        char* buf = new char[block_size_];
        Status s;
        while (bytes_to_copy > 0) {
            size_t avail = block_size_ - block_offset;
            if (avail > bytes_to_copy) {
                avail = bytes_to_copy;
            }

            Slice block_slice;
            s = ReadFromBlock(block, block_offset, avail, &block_slice, buf);
            if (!s.ok()) {
                return s;
            }
            memcpy(dst, block_slice.data(), avail);

            bytes_to_copy -= avail;
            dst += avail;
            block++;
            block_offset = 0;
        }
        delete[] buf;
        *result = Slice(scratch, n);
        return Status::OK();
    }

private:
    Status OpenFile() {
        if (dfs_env_->FileExists(fname_hdfs_)) {
            Status s = dfs_env_->GetFileSize(fname_hdfs_, &size_);
            if (!s.ok()) {
                return Status::IOError("hdfs GetFileSize fail: ", fname_hdfs_);
            }
        }
        return Status::OK();
    }

    Status ReadFromBlock(uint32_t block, size_t block_offset, size_t n,
                         Slice* result, char* scratch) {
        LDB_SLOG(TRACE, "read [block: %d, block_offset: %d, size: %d]",
             block, block_offset, n);
        Status s;
        if (mem_cache_->IsLoaded(fname_hdfs_, block)) {
            s = mem_cache_->ReadBlock(fname_hdfs_, block, block_offset, n, result);
            if (s.ok()) {
                LDB_SLOG(TRACE, "hit mem-cache");
                return Status::OK();
            }
        }
        LDB_SLOG(TRACE, "fail to load from mem-cache, try disk-cache");
        Slice block_slice;
        s = LoadMissingFromLocal(block, &block_slice, scratch);
        if (s.ok()) {
            s = mem_cache_->WriteBlock(fname_hdfs_, block, block_slice);
            if (!s.ok()) {
                LDB_SLOG(WARNING, "fail to setup missing block to mem-cache: %s",
                     s.ToString().c_str());
            }
            *result = Slice(block_slice.data() + block_offset, n);
            LDB_SLOG(TRACE, "hit disk-cache");
            return Status::OK();
        }
        LDB_SLOG(TRACE, "fail to load from disk-cache, try remote base");
        s = LoadMissingFromRemote(block, &block_slice, scratch);
        if (s.ok()) {
            s = SetupMissingToLocal(block, block_slice);
            if (!s.ok()) {
                LDB_SLOG(WARNING, "fail to setup missing block to disk-cache: %s",
                     s.ToString().c_str());
            }
            s = mem_cache_->WriteBlock(fname_hdfs_, block, block_slice);
            if (!s.ok()) {
                LDB_SLOG(WARNING, "fail to setup missing block to mem-cache: %s",
                     s.ToString().c_str());
            }
            *result = Slice(block_slice.data() + block_offset, n);
        }
        return Status::OK();
    }

    Status LoadMissingFromRemote(uint32_t block_no, Slice* result, char* scratch) {
        Slice block_slice;
        uint64_t offset = block_no * block_size_;
        Status s = hdfs_file_->Read(offset, block_size_, &block_slice, scratch);
        if (s.ok()) {
            *result = Slice(scratch, block_size_);
        }
        return s;
    }

    Status LoadMissingFromLocal(uint32_t block_no, Slice* result, char* scratch) {
        return disk_cache_->ReadBlock(fname_hdfs_, block_no, 0, block_size_, result, scratch);
    }

    Status SetupMissingToLocal(uint32_t block_no, const Slice& data,
                               bool force = false) {
        return disk_cache_->WriteBlock(fname_hdfs_, block_no, data, force);
    }

private:
    DiskCacheReader(const DiskCacheReader&);
    void operator=(const DiskCacheReader&);

private:
    Env* dfs_env_;
    RandomAccessFile* hdfs_file_;
    std::string fname_hdfs_;
    uint64_t size_;

    LRU_MemCache* mem_cache_;
    LRU_DiskCache* disk_cache_;
    bool mem_cache_created_own_;
    bool disk_cache_created_own_;
    const uint32_t block_size_;
};

class DiskCacheWriter {
public:
    DiskCacheWriter(const std::string& fname_hdfs, Env* hdfs_env,
                    LRU_MemCache* mem_cache = NULL, LRU_DiskCache* disk_cache = NULL)
        : dfs_env_(hdfs_env), hdfs_file_(NULL), fname_hdfs_(fname_hdfs),
          size_(0), blocks_no_(0), mem_cache_(mem_cache), disk_cache_(disk_cache),
          mem_cache_created_own_(false), disk_cache_created_own_(false),
          block_size_(ThreeLevelCacheEnv::s_block_size_) {
        if (!mem_cache_) {
            mem_cache_ = new LRU_MemCache(block_size_);
            mem_cache_created_own_ = true;
        }
        if (!disk_cache_) {
            disk_cache_ = new LRU_DiskCache(
                ThreeLevelCacheEnv::CachePath(fname_hdfs) + fname_hdfs + ".cache",
                block_size_, 10);
            disk_cache_created_own_ = true;
        }
        Status s = OpenFile();
        s = dfs_env_->NewWritableFile(fname_hdfs_, &hdfs_file_);
        assert(s.ok());
    }

    ~DiskCacheWriter() {
        if (mem_cache_created_own_) {
            delete mem_cache_;
        }
        if (disk_cache_created_own_) {
            delete disk_cache_;
        }
    }

    Status Append(const Slice& data) {
        Status s = AppendToRemote(data);
#if 0 // append() not fill cache
        if (s.ok()) {
            s = AppendToLocal(data, blocks_no_);
            if (!s.ok()) {
                LDB_SLOG(WARNING, "fail to append to disk-cache. status: %s",
                     s.ToString().c_str());
            }
            s = AppendToMem(data, blocks_no_);
            if (!s.ok()) {
                LDB_SLOG(WARNING, "fail to append to mem-cache. status: %s",
                     s.ToString().c_str());
            }
        }
        return Status::OK();
#else
        return s;
#endif
    }

    Status Flush() {
        disk_cache_->Flush();
        return hdfs_file_->Flush();
    }

    Status Sync() {
        disk_cache_->Sync();
        return hdfs_file_->Sync();
    }

    Status Close() {
        disk_cache_->Sync();
        return hdfs_file_->Close();
    }

private:
    Status OpenFile() {
        if (dfs_env_->FileExists(fname_hdfs_)) {
            Status s = dfs_env_->GetFileSize(fname_hdfs_, &size_);
            if (!s.ok()) {
                return Status::IOError("hdfs GetFileSize fail: ", fname_hdfs_);
            }
            blocks_no_ = size_ / block_size_;
        }
        return Status::OK();
    }

    Status AppendToMem(const Slice& data, uint32_t cur_blocks_num) {
        return mem_cache_->Append(data, fname_hdfs_, cur_blocks_num);
    }

    Status AppendToLocal(const Slice& data, uint32_t cur_blocks_num) {
        return disk_cache_->Append(fname_hdfs_, data, cur_blocks_num);
    }

    Status AppendToRemote(const Slice& data) {
        Status s = hdfs_file_->Append(data);
        if (!s.ok()) {
            return s;
        }
        size_ += data.size();
        blocks_no_ = size_ / block_size_;
        return Status::OK();
    }

private:
    DiskCacheWriter(const DiskCacheWriter&);
    void operator=(const DiskCacheWriter&);

private:
    Env* dfs_env_;
    WritableFile* hdfs_file_;
    std::string fname_hdfs_;
    uint64_t size_;
    uint64_t blocks_no_;

    LRU_MemCache* mem_cache_;
    LRU_DiskCache* disk_cache_;
    bool mem_cache_created_own_;
    bool disk_cache_created_own_;
    const uint32_t block_size_;
};

class CacheSequentialFile: public SequentialFile {
private:
    DiskCacheReader* flash_file_;
    uint64_t offset_;

public:
    CacheSequentialFile(Env* hdfs_env, const std::string& fname)
        : flash_file_(NULL), offset_(0) {
        flash_file_ = new DiskCacheReader(fname, hdfs_env,
                                          g_mem_cache, g_disk_cache);
    }

    virtual ~CacheSequentialFile() {
        delete flash_file_;
    }

    virtual Status Read(size_t n, Slice* result, char* scratch) {
        Status s = flash_file_->Read(offset_, n, result, scratch);
        if (s.ok()) {
            offset_ += n;
        }
        return s;
    }

    virtual Status Skip(uint64_t n) {
        offset_ += n;
        return Status::OK();
    }

    bool isValid() {
        return (flash_file_ != NULL);
    }
};

class CacheRandomAccessFile : public RandomAccessFile {
private:
    DiskCacheReader* flash_file_;

public:
    CacheRandomAccessFile(Env* hdfs_env, const std::string& fname)
        : flash_file_(NULL) {
        flash_file_ = new DiskCacheReader(fname, hdfs_env, g_mem_cache, g_disk_cache);
    }
    ~CacheRandomAccessFile() {
        delete flash_file_;
    }
    Status Read(uint64_t offset, size_t n, Slice* result,
                char* scratch) const {
        return flash_file_->Read(offset, n, result, scratch);
    }
    bool isValid() {
        return flash_file_ != NULL;
    }
};


class CacheWritableFile: public WritableFile {
private:
    DiskCacheWriter* flash_file_;

public:
    CacheWritableFile(Env* hdfs_env, const std::string& fname)
        : flash_file_(NULL) {
        flash_file_ = new DiskCacheWriter(fname, hdfs_env, g_mem_cache, g_disk_cache);
    }
    virtual ~CacheWritableFile() {
        delete flash_file_;
    }
    virtual Status Append(const Slice& data) {
        return flash_file_->Append(data);
    }

    bool isValid() {
        return (flash_file_ != NULL);
    }

    virtual Status Flush() {
        return flash_file_->Flush();
    }

    virtual Status Sync() {
        return flash_file_->Sync();
    }

    virtual Status Close() {
        return flash_file_->Close();
    }
};

ThreeLevelCacheEnv::ThreeLevelCacheEnv() : EnvWrapper(Env::Default()) {
    dfs_env_ = EnvDfs();
    posix_env_ = Env::Default();
}

ThreeLevelCacheEnv::~ThreeLevelCacheEnv() {}

Status ThreeLevelCacheEnv::NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    CacheSequentialFile* f = new CacheSequentialFile(dfs_env_, fname);
    if (!f->isValid()) {
        delete f;
        *result = NULL;
        return IOError(fname, errno);
    }
    *result = f;
    return Status::OK();
}

Status ThreeLevelCacheEnv::NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    CacheRandomAccessFile* f = new CacheRandomAccessFile(dfs_env_, fname);
    if (f == NULL || !f->isValid()) {
        *result = NULL;
        delete f;
        return IOError(fname, errno);
    }
    *result = f;
    return Status::OK();
}

Status ThreeLevelCacheEnv::NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    CacheWritableFile* f = new CacheWritableFile(dfs_env_, fname);
    if (f == NULL || !f->isValid()) {
        *result = NULL;
        delete f;
        return IOError(fname, errno);
    }
    *result = f;
    return Status::OK();
}

bool ThreeLevelCacheEnv::FileExists(const std::string& fname) {
    return dfs_env_->FileExists(fname);
}

Status ThreeLevelCacheEnv::GetChildren(const std::string& path,
                                       std::vector<std::string>* result) {
    return dfs_env_->GetChildren(path, result);
}

Status ThreeLevelCacheEnv::DeleteFile(const std::string& fname) {
    // TODO(anqin): cache need to be removed too
    return dfs_env_->DeleteFile(fname);
}

Status ThreeLevelCacheEnv::CreateDir(const std::string& name) {
    return dfs_env_->CreateDir(name);
}

Status ThreeLevelCacheEnv::DeleteDir(const std::string& name) {
    posix_env_->DeleteDirRecursive(ThreeLevelCacheEnv::CachePath(name) + name);
    return dfs_env_->DeleteDir(name);
}

Status ThreeLevelCacheEnv::GetFileSize(const std::string& fname, uint64_t* size) {
    return dfs_env_->GetFileSize(fname, size);
}

Status ThreeLevelCacheEnv::RenameFile(const std::string& src,
                            const std::string& target) {
    // TODO(anqin): need to removed the dirty cache
    return dfs_env_->RenameFile(src, target);
}

Status ThreeLevelCacheEnv::LockFile(const std::string& fname, FileLock** lock) {
    *lock = NULL;
    return Status::OK();
}

Status ThreeLevelCacheEnv::UnlockFile(FileLock* lock) {
    return Status::OK();
}

void ThreeLevelCacheEnv::SetCachePaths(const std::string& paths) {
    LDB_SLOG(DEBUG, "set flash path: %s", paths.c_str());
    std::vector<std::string> path_list;
    SplitString(paths, ";", &path_list);
    cache_paths_.swap(path_list);

    Env* posix_env = Env::Default();
    for (uint32_t i = 0; i < cache_paths_.size(); ++i) {
        posix_env->CreateDir(cache_paths_[i]);
    }
}

void ThreeLevelCacheEnv::RemoveCachePaths() {
    Env* posix_env = Env::Default();
    for (uint32_t i = 0; i < cache_paths_.size(); ++i) {
        posix_env->DeleteDirRecursive(cache_paths_[i]);
    }
}

const std::string& ThreeLevelCacheEnv::CachePath(const std::string& fname) {
    if (cache_paths_.size() == 1) {
        return cache_paths_[0];
    }
    uint32_t hash = Hash(fname.c_str(), fname.size(), 13);
    return cache_paths_[hash % cache_paths_.size()];
}

void ThreeLevelCacheEnv::ResetMemCache() {
    g_mem_cache->Reset();
}

void ThreeLevelCacheEnv::ResetDiskCache() {
    g_disk_cache->Reset();
}

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* cache_env;

static Cache* g_disk_cache_meta;

static void InitGlobalCache() {
    g_mem_cache = new LRU_MemCache(ThreeLevelCacheEnv::s_block_size_,
                                   NewLRUCache(ThreeLevelCacheEnv::s_mem_cache_size_in_KB_ << 10));

    uint32_t blocks_num = ((((uint64_t)ThreeLevelCacheEnv::s_disk_cache_size_in_MB_) << 20)
        + ThreeLevelCacheEnv::s_block_size_ - 1) / ThreeLevelCacheEnv::s_block_size_;
    // meta value size is sizeof(uint32_t) * 3
    g_disk_cache_meta = NewLRUCache(blocks_num * sizeof(uint32_t) * 3);
    g_disk_cache = new LRU_DiskCache(ThreeLevelCacheEnv::s_disk_cache_file_name_,
        ThreeLevelCacheEnv::s_block_size_, ThreeLevelCacheEnv::s_disk_cache_size_in_MB_,
        ThreeLevelCacheEnv::s_disk_cache_file_num_, g_disk_cache_meta);
}

static void InitThreeLevelCacheEnv()
{
    InitGlobalCache();
    cache_env = new ThreeLevelCacheEnv();
}

Env* EnvThreeLevelCache() {
    pthread_once(&once, InitThreeLevelCacheEnv);
    return cache_env;
}

Env* NewThreeLevelCacheEnv() {
    return new ThreeLevelCacheEnv();
}

}  // namespace leveldb

