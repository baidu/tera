// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/flash_block_cache_impl.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <list>
#include <sstream>
#include <unordered_map>

#include "common/counter.h"
#include "db/table_cache.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/env_flash_block_cache.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/statistics.h"
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
::tera::Counter tera_flash_block_cache_evict_counter;

// Each SSD will New a BlockCache
// block state
uint64_t kCacheBlockValid = 0x1;
uint64_t kCacheBlockLocked = 0x2;
uint64_t kCacheBlockDfsRead = 0x4;
uint64_t kCacheBlockCacheRead = 0x8;
uint64_t kCacheBlockCacheFill = 0x10;

FlashBlockCacheImpl::FlashBlockCacheImpl(const FlashBlockCacheOptions& options)
    : options_(options),
      dfs_env_(options.env),
      new_fid_(0),
      prev_fid_(0),
      block_set_cache_(nullptr),
      meta_db_(nullptr) {
    bg_fill_.SetBackgroundThreads(30);
    bg_read_.SetBackgroundThreads(30);
    bg_dfs_read_.SetBackgroundThreads(30);
    bg_flush_.SetBackgroundThreads(30);
    bg_control_.SetBackgroundThreads(2);
    stat_ = CreateDBStatistics();
}

FlashBlockCacheImpl::~FlashBlockCacheImpl() {
    delete stat_;
}

void FlashBlockCacheImpl::BGControlThreadFunc(void* arg) {
    reinterpret_cast<FlashBlockCacheImpl*>(arg)->BGControlThread();
}

void FlashBlockCacheImpl::BGControlThread() {
    stat_->MeasureTime(FLASH_BLOCK_CACHE_EVICT_NR,
                       tera_flash_block_cache_evict_counter.Clear());

    Log("[%s] statistics: "
        "%s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s\n",
        this->WorkPath().c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_PREAD_QUEUE).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_PREAD_SSD_READ).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_PREAD_DFS_READ).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_PREAD_SSD_WRITE).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_PREAD_FILL_USER_DATA).c_str(),

        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_PREAD_RELEASE_BLOCK).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_LOCKMAP_BS_RELOAD_NR).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_PREAD_GET_BLOCK).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_PREAD_BLOCK_NR).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_GET_BLOCK_SET).c_str(),

        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_BS_LRU_LOOKUP).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_PREAD_WAIT_UNLOCK).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_ALLOC_FID).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_GET_FID).c_str(),
        stat_->GetBriefHistogramString(FLASH_BLOCK_CACHE_EVICT_NR).c_str());

    Log("[%s] statistics(meta): "
        "table_cache: %lf/%lu/%lu, "
        "block_cache: %lf/%lu/%lu\n",
        this->WorkPath().c_str(),
        options_.opts.table_cache->HitRate(true),
        options_.opts.table_cache->TableEntries(),
        options_.opts.table_cache->ByteSize(),
        options_.opts.block_cache->HitRate(true),
        options_.opts.block_cache->Entries(),
        options_.opts.block_cache->TotalCharge());

    // resched after 6s
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_PREAD_QUEUE);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_PREAD_SSD_READ);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_PREAD_DFS_READ);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_PREAD_SSD_WRITE);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_PREAD_FILL_USER_DATA);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_PREAD_RELEASE_BLOCK);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_LOCKMAP_BS_RELOAD_NR);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_PREAD_GET_BLOCK);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_PREAD_BLOCK_NR);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_GET_BLOCK_SET);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_BS_LRU_LOOKUP);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_PREAD_WAIT_UNLOCK);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_ALLOC_FID);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_GET_FID);
    stat_->ClearHistogram(FLASH_BLOCK_CACHE_EVICT_NR);
    bg_control_.Schedule(&FlashBlockCacheImpl::BGControlThreadFunc, this, 10, 6000);
}

void FlashBlockCacheImpl::BlockDeleter(const Slice& key, void* v) {
    CacheBlock* block = (CacheBlock*)v;
    //Log("Evict blockcache: %s\n", block->ToString().c_str());
    delete block;
    tera_flash_block_cache_evict_counter.Inc();
}

// if lock succ, put lock_val, else get newer value
Status FlashBlockCacheImpl::LockAndPut(LockContent* lc) {
    mu_.AssertHeld();
    Status s;
    std::string key;
    if ((key = lc->Encode()) == "") {
        return Status::NotSupported("key type error");
    }
    //Log("[%s] trylock key: %s\n",
        //this->WorkPath().c_str(),
        //key.c_str());

    Waiter* w = NULL;
    LockKeyMap::iterator it = lock_key_.find(key);
    if (it != lock_key_.end()) {
        w = it->second;
        w->wait_num ++;
        mu_.Unlock();
        w->Wait();

        s = GetContentAfterWait(lc);
        mu_.Lock();
        if (--w->wait_num == 0) {
            // last thread wait for open
            lock_key_.erase(key);
            //Log("[%s] wait done %s, delete cv\n",
                //this->WorkPath().c_str(),
                //key.c_str());
            delete w;
        } else {
            //Log("[%s] wait done %s, not last\n",
                //this->WorkPath().c_str(),
                //key.c_str());
        }
    } else {
        w = new Waiter;
        w->wait_num = 1;
        lock_key_[key] = w;
        mu_.Unlock();

        s = PutContentAfterLock(lc);
        mu_.Lock();
        if (--w->wait_num == 0) {
            lock_key_.erase(key);
            //Log("[%s] put done %s, no wait thread\n",
                //this->WorkPath().c_str(),
                //key.c_str());
            delete w;
        } else {
            mu_.Unlock();
            //Log("[%s] put done %s, signal all wait thread\n",
                //this->WorkPath().c_str(),
                //key.c_str());
            w->SignalAll();

            mu_.Lock();
        }
    }
    return s;
}

Status FlashBlockCacheImpl::GetContentAfterWait(LockContent* lc) {
    Status s;
    std::string key = lc->Encode();

    if (lc->type == LockKeyType::kDBKey) {
        ReadOptions r_opts;
        s = meta_db_->Get(r_opts, key, lc->db_val);
        //Log("[%s] get lock key: %s, val: %s, status: %s\n",
            //this->WorkPath().c_str(),
            //key.c_str(),
            //lc->db_val->c_str(),
            //s.ToString().c_str());
    } else if (lc->type == LockKeyType::kBlockSetKey) {
        std::string bs_key;
        PutFixed64(&bs_key, lc->sid);
        LRUHandle* bs_handle = (LRUHandle*)block_set_cache_->Lookup(bs_key);
        lc->block_set = reinterpret_cast<BlockSet*>(block_set_cache_->Value((Cache::Handle*)bs_handle));
        assert(bs_handle == lc->block_set->handle);
        //Log("[%s] get blockset sid: %lu\n",
            //this->WorkPath().c_str(),
            //lc->sid);
    }
    return s;
}

Status FlashBlockCacheImpl::PutContentAfterLock(LockContent* lc) {
    Status s;
    std::string key = lc->Encode();

    if (lc->type == LockKeyType::kDBKey) {
        WriteOptions w_opts;
        s = meta_db_->Put(w_opts, key, lc->db_lock_val);
        if (s.ok()) {
            lc->db_val->append(lc->db_lock_val.data(), lc->db_lock_val.size());
        }
        //Log("[%s] Insert db key : %s, val %s, status %s\n",
            //this->WorkPath().c_str(),
            //lc->KeyToString().c_str(),
            //lc->ValToString().c_str(),
            //s.ToString().c_str());
    } else if (lc->type == LockKeyType::kDeleteDBKey) {
        WriteOptions w_opts;
        s = meta_db_->Delete(w_opts, key);
        //Log("[%s] Delete db key : %s, val %s, status %s\n",
            //this->WorkPath().c_str(),
            //lc->KeyToString().c_str(),
            //lc->ValToString().c_str(),
            //s.ToString().c_str());
    } else if (lc->type == LockKeyType::kBlockSetKey) { // cannot double insert
        std::string bs_key;
        PutFixed64(&bs_key, lc->sid);
        LRUHandle* bs_handle = (LRUHandle*)block_set_cache_->Lookup(bs_key);
        if (bs_handle != NULL) {
            lc->block_set = reinterpret_cast<BlockSet*>(block_set_cache_->Value((Cache::Handle*)bs_handle));
            assert(bs_handle == lc->block_set->handle);
        } else {
            s = ReloadBlockSet(lc);
        }
    }
    return s;
}

Status FlashBlockCacheImpl::ReloadBlockSet(LockContent* lc) {
    Status s;
    std::string key = lc->Encode();

    lc->block_set = new BlockSet;
    lc->block_set->block_cache = NewBlockBasedCache(options_.blocks_per_set);// number of blocks in BS
    std::string file = options_.cache_dir + "/" + Uint64ToString(lc->sid);
    lc->block_set->fd = open(file.c_str(), O_RDWR | O_CREAT, 0644);
    assert(lc->block_set->fd > 0);
    Log("[%s] New BlockSet %s, file: %s, nr_block: %lu, fd: %d\n",
        this->WorkPath().c_str(),
        lc->KeyToString().c_str(),
        file.c_str(), options_.blocks_per_set,
        lc->block_set->fd);

    // reload hash lru
    uint64_t total_items = 0;
    ReadOptions s_opts;
    leveldb::Iterator* db_it = meta_db_->NewIterator(s_opts);
    for (db_it->Seek(key);
         db_it->Valid() && db_it->key().starts_with(kKeyPrefixBlockSet);
         db_it->Next()) {
        Slice lkey = db_it->key();
        CacheBlock* block = new CacheBlock;

        // decode key
        block->DecodeDBKey(lkey);
        if (block->sid != lc->sid) {
            delete block;
            block = nullptr;
            break;
        }
        total_items++;

        // decode value
        block->DecodeDBValue(db_it->value()); // get fid and block_idx
        block->state = (block->Test(kCacheBlockValid)) ? kCacheBlockValid : 0;
        //Log("[%s] Recovery %s, insert cacheblock into BlockBasedLRU, %s\n",
            //this->WorkPath().c_str(),
            //lc->KeyToString().c_str(),
            //block->ToString().c_str());

        LRUHandle* handle = (LRUHandle*)(lc->block_set->block_cache->Insert(block->CacheKey(), block, block->offset_in_blockset, &FlashBlockCacheImpl::BlockDeleter));
        assert((uint64_t)(lc->block_set->block_cache->Value((Cache::Handle*)handle)) == (uint64_t)block);
        assert(handle->cache_id == block->offset_in_blockset);
        block->handle = handle;
        lc->block_set->block_cache->Release((Cache::Handle*)handle);
    }
    delete db_it;
    stat_->MeasureTime(FLASH_BLOCK_CACHE_LOCKMAP_BS_RELOAD_NR, total_items);

    LRUHandle* bs_handle = (LRUHandle*)block_set_cache_->Insert(lc->block_set->CacheKey(lc->sid), lc->block_set, 1, NULL);
    assert(bs_handle != NULL);
    lc->block_set->handle = bs_handle;
    return s;
}

std::string FlashBlockCacheImpl::WorkPath() const {
    return work_path_;
}

Status FlashBlockCacheImpl::LoadCache() {
    // open meta db
    work_path_ = options_.cache_dir;
    std::string dbname = options_.cache_dir + "/" + kMetaDBName;
    options_.opts.env = options_.cache_env; // local write
    options_.opts.filter_policy = NewBloomFilterPolicy(10);
    options_.opts.block_cache = leveldb::NewLRUCache(options_.meta_block_cache_size * 1024UL * 1024);
    options_.opts.table_cache = new leveldb::TableCache(options_.meta_table_cache_size * 1024UL * 1024);
    options_.opts.write_buffer_size = options_.write_buffer_size;
    options_.opts.info_log = Logger::DefaultLogger();

    // give meta db's lg0 a separate PosixEnv including a seperate ThreadPool
    leveldb::LG_info* lg_info = new leveldb::LG_info(0);
    lg_info->env = NewPosixEnv();
    lg_info->env->SetBackgroundThreads(5);
    std::map<uint32_t, leveldb::LG_info*>* lg_info_list = new std::map<uint32_t, leveldb::LG_info*>;
    (*lg_info_list)[0] = lg_info;
    options_.opts.lg_info_list = lg_info_list; 

    Log("[flash_block_cache %s] open meta db: block_cache: %lu, table_cache: %lu, write_buffer: %lu",
        dbname.c_str(),
        options_.meta_block_cache_size,
        options_.meta_table_cache_size,
        options_.write_buffer_size);

    Status s = DB::Open(options_.opts, dbname, &meta_db_);
    assert(s.ok());

    // recover fid
    std::string key = FIDDBKey();
    std::string val;
    ReadOptions r_opts;
    s = meta_db_->Get(r_opts, key, &val);
    if (!s.ok()) {
        prev_fid_ = 0;
    } else {
        prev_fid_ = DecodeFixed64(val.c_str());
    }
    new_fid_ = prev_fid_ + options_.fid_batch_num;
    Log("[flash_block_cache %s]: recover fid: prev_fid: %lu, new_fid: %lu\n",
        dbname.c_str(), prev_fid_, new_fid_);

    // recover cache size
    key = kKeyConfCacheSize;
    val = "";
    s = meta_db_->Get(r_opts, key, &val);
    if (!s.ok() || options_.force_update_conf_enabled) {
        // first load or need force update, use conf from FLAG file,
        // and save conf to meta db
        if (!s.ok()) {
            // first load
            Log("[flash_block_cache %s]: cache size not exist in meta db, load from FLAG file, cache size: %lu\n", dbname.c_str(), options_.cache_size);
        } else {
            // force update conf
            Log("[flash_block_cache %s]: force update conf from FLAG file, cache size: %lu\n", dbname.c_str(), options_.cache_size);
        }
        val = "";
        PutFixed64(&val, options_.cache_size);
        leveldb::WriteBatch batch;
        batch.Put(key, val);
        s = meta_db_->Write(leveldb::WriteOptions(), &batch);
        if (s.ok()) {
            Log("[flash_block_cache %s]: save cache size success, cache size: %lu\n", dbname.c_str(), options_.cache_size);
        } else {
            Log("[flash_block_cache %s]: save cache size fail, cache size: %lu\n", dbname.c_str(), options_.cache_size);
        }
    } else {
        // load conf from meta db
        uint64_t cache_size = DecodeFixed64(val.c_str());
        if (cache_size != options_.cache_size) {
            Log("[flash_block_cache %s]: WARNING: ignore cache size in conf: %lu, set block_cache_force_update_conf_enabled true to update local db from conf file\n", dbname.c_str(), options_.cache_size);
        }
        options_.cache_size = cache_size;
        Log("[flash_block_cache %s]: recover cache size from meta db: %lu\n", dbname.c_str(), cache_size);
    }

    // recover blockset size
    key = kKeyConfBlockSetSize;
    val = "";
    s = meta_db_->Get(r_opts, key, &val);
    if (!s.ok() || options_.force_update_conf_enabled) {
        // first load or need force update, use conf from FLAG file,
        // and save conf to meta db
        if (!s.ok()) {
            // first load
            Log("[flash_block_cache %s]: blockset size not exist in meta db, load from FLAG file, blockset size: %lu\n", dbname.c_str(), options_.blockset_size);
        } else {
            // force update conf
            Log("[flash_block_cache %s]: force update conf from FLAG file, blockset size: %lu\n", dbname.c_str(), options_.blockset_size);
        }
        val = "";
        PutFixed64(&val, options_.blockset_size);
        leveldb::WriteBatch batch;
        batch.Put(key, val);
        s = meta_db_->Write(leveldb::WriteOptions(), &batch);
        if (s.ok()) {
            Log("[flash_block_cache %s]: save blockset size success, blockset size: %lu\n", dbname.c_str(), options_.blockset_size);
        } else {
            Log("[flash_block_cache %s]: save blockset size fail, blockset size: %lu\n", dbname.c_str(), options_.blockset_size);
        }
    } else {
        // load conf from meta db
        uint64_t blockset_size = DecodeFixed64(val.c_str());
        if (blockset_size != options_.blockset_size) {
            Log("[flash_block_cache %s]: WARNING: ignore blockset size in conf: %lu, set block_cache_force_update_conf_enabled true to update local db from conf file\n", dbname.c_str(), options_.blockset_size);
        }
        options_.blockset_size = blockset_size;
        Log("[flash_block_cache %s]: recover blockset size from meta db: %lu\n", dbname.c_str(), blockset_size);
    }

    // recover block size
    key = kKeyConfBlockSize;
    val = "";
    s = meta_db_->Get(r_opts, key, &val);
    if (!s.ok() || options_.force_update_conf_enabled) {
        // first load or need force update, use conf from FLAG file,
        // and save conf to meta db
        if (!s.ok()) {
            // first load
            Log("[flash_block_cache %s]: block size not exist in meta db, load from FLAG file, block size: %lu\n", dbname.c_str(), options_.block_size);
        } else {
            // force update conf
            Log("[flash_block_cache %s]: force update conf from FLAG file, block size: %lu\n", dbname.c_str(), options_.block_size);
        }
        val = "";
        PutFixed64(&val, options_.block_size);
        leveldb::WriteBatch batch;
        batch.Put(key, val);
        s = meta_db_->Write(leveldb::WriteOptions(), &batch);
        if (s.ok()) {
            Log("[flash_block_cache %s]: save block size success, block size: %lu\n", dbname.c_str(), options_.block_size);
        } else {
            Log("[flash_block_cache %s]: save block size fail, block size: %lu\n", dbname.c_str(), options_.block_size);
        }
    } else {
        // load conf from meta db
        uint64_t block_size = DecodeFixed64(val.c_str());
        if (block_size != options_.block_size) {
            Log("[flash_block_cache %s]: WARNING: ignore block size in conf: %lu, set block_cache_force_update_conf_enabled true to update local db from conf file\n", dbname.c_str(), options_.block_size);
        }
        options_.block_size = block_size;
        Log("[flash_block_cache %s]: recover block size from meta db: %lu\n", dbname.c_str(), block_size);
    }

    options_.blockset_num = options_.cache_size / options_.blockset_size + 1;
    Log("[flash_block_cache %s]: blockset num: %lu\n", dbname.c_str(), options_.blockset_num);

    options_.blocks_per_set = options_.blockset_size / options_.block_size + 1;
    Log("[flash_block_cache %s]: block num per blockset: %lu\n", dbname.c_str(), options_.blocks_per_set);

    block_set_cache_ = leveldb::NewBlockBasedCache(options_.blockset_num);

    bg_control_.Schedule(&FlashBlockCacheImpl::BGControlThreadFunc, this, 10, 6000);
    s = Status::OK();
    return s;
}

Status FlashBlockCacheImpl::FillCache(CacheBlock* block) {
    uint64_t offset_in_blockset = block->offset_in_blockset;
    BlockSet* bs = reinterpret_cast<BlockSet*>(block_set_cache_->Value((Cache::Handle*)block->bs_handle));
    int fd = bs->fd;

    // do io without lock
    ssize_t res = pwrite(fd, block->data_block.data(), block->data_block.size(),
                         offset_in_blockset * options_.block_size);

    if (res < 0) {
        Log("[%s] cache fill: sid %lu, blockset.fd %d, datablock size %lu, cb_idx %lu, %s, res %ld\n",
            this->WorkPath().c_str(), block->sid, fd, block->data_block.size(),
            offset_in_blockset,
            block->ToString().c_str(),
            res);
        return Status::Corruption("FillCache error");
    }
    return Status::OK();
}

Status FlashBlockCacheImpl::ReadCache(CacheBlock* block) {
    uint64_t offset_in_blockset = block->offset_in_blockset;
    BlockSet* bs = reinterpret_cast<BlockSet*>(block_set_cache_->Value((Cache::Handle*)block->bs_handle));
    int fd = bs->fd;

    // do io without lock
    ssize_t res = pread(fd, (char*)block->data_block.data(), block->data_block.size(),
                        offset_in_blockset * options_.block_size);
    if (res < 0) {
        Log("[%s] cache read: sid %lu, blockset.fd %d, datablock size %lu, cb_idx %lu, %s, res %ld\n",
            this->WorkPath().c_str(), block->sid, fd, block->data_block.size(),
            offset_in_blockset,
            block->ToString().c_str(),
            res);
        return Status::Corruption("ReadCache error");
    }
    return Status::OK();
}

uint64_t FlashBlockCacheImpl::AllocFileId() { // no more than fid_batch_num
    mu_.AssertHeld();
    uint64_t start_ts = options_.cache_env->NowMicros();
    uint64_t fid = ++new_fid_;
    while (new_fid_ - prev_fid_ >= options_.fid_batch_num) {
        std::string key = FIDDBKey();
        std::string lock_val;
        PutFixed64(&lock_val, new_fid_);
        std::string val;

        LockContent lc;
        lc.type = LockKeyType::kDBKey;
        lc.db_lock_key = key;
        lc.db_lock_val = lock_val;
        lc.db_val = &val;
        Status s = LockAndPut(&lc);
        if (s.ok()) {
            prev_fid_ = DecodeFixed64(val.c_str());
        }
        //Log("[%s] alloc fid: key %s, new_fid: %lu, prev_fid: %lu\n",
            //this->WorkPath().c_str(),
            //key.c_str(),
            //new_fid_,
            //prev_fid_);
    }
    stat_->MeasureTime(FLASH_BLOCK_CACHE_ALLOC_FID,
                       options_.cache_env->NowMicros() - start_ts);
    return fid;
}

uint64_t FlashBlockCacheImpl::FileId(const std::string& fname) {
    uint64_t fid = 0;
    std::string key = FNameDBKey(fname);
    uint64_t start_ts = options_.cache_env->NowMicros();
    ReadOptions r_opts;
    std::string val;

    Status s = meta_db_->Get(r_opts, key, &val);
    if (!s.ok()) { // not exist
        MutexLock l(&mu_);
        fid = AllocFileId();
        std::string v;
        PutFixed64(&val, fid);

        LockContent lc;
        lc.type = LockKeyType::kDBKey;
        lc.db_lock_key = key;
        lc.db_lock_val = val;
        lc.db_val = &v;
        //Log("[%s] alloc fid: %lu, key: %s",
            //this->WorkPath().c_str(),
            //fid, key.c_str());
        s = LockAndPut(&lc);
        assert(s.ok());
        fid = DecodeFixed64(v.c_str());
    } else { // fid in cache
        fid = DecodeFixed64(val.c_str());
    }

    //Log("[%s] Fid: %lu, fname: %s\n",
        //this->WorkPath().c_str(),
        //fid, fname.c_str());
    stat_->MeasureTime(FLASH_BLOCK_CACHE_GET_FID,
                       options_.cache_env->NowMicros() - start_ts);
    return fid;
}

Status FlashBlockCacheImpl::DeleteFile(const std::string& fname) {
    Status s;
    std::string key = FNameDBKey(fname);
    {
        MutexLock l(&mu_);
        LockContent lc;
        lc.type = LockKeyType::kDeleteDBKey;
        lc.db_lock_key = key;
        s = LockAndPut(&lc);
    }
    return s;
}

BlockSet* FlashBlockCacheImpl::GetBlockSet(uint64_t sid) {
    std::string key;
    PutFixed64(&key, sid);
    BlockSet* block_set = NULL;
    uint64_t start_ts = options_.cache_env->NowMicros();

    LRUHandle* h = (LRUHandle*)block_set_cache_->Lookup(key);
    if (h == NULL) {
        MutexLock l(&mu_);
        LockContent lc;
        lc.type = LockKeyType::kBlockSetKey;
        lc.sid = sid;
        lc.block_set = NULL;
        Status s = LockAndPut(&lc);
        block_set = lc.block_set;
    } else {
        //Log("[%s] get blockset from memcache, sid %lu\n",
            //this->WorkPath().c_str(), sid);
        block_set = reinterpret_cast<BlockSet*>(block_set_cache_->Value((Cache::Handle*)h));
        assert(block_set->handle == h);
    }
    stat_->MeasureTime(FLASH_BLOCK_CACHE_GET_BLOCK_SET,
                       options_.cache_env->NowMicros() - start_ts);
    return block_set;
}

CacheBlock* FlashBlockCacheImpl::GetAndAllocBlock(uint64_t fid, uint64_t block_idx) {
    std::string key;
    PutFixed64(&key, fid);
    PutFixed64(&key, block_idx);
    uint32_t hash = Hash(key.c_str(), key.size(), 7);
    uint64_t sid = hash % options_.blockset_num;

    //Log("[%s] alloc block, try get blockset, fid: %lu, block_idx: %lu, hash: %u, sid %lu, blockset_num: %lu\n",
        //this->WorkPath().c_str(), fid, block_idx, hash, sid, options_.blockset_num);
    CacheBlock* block = NULL;
    BlockSet* bs = GetBlockSet(sid); // get and alloc bs
    Cache* block_cache = bs->block_cache;

    uint64_t start_ts = options_.cache_env->NowMicros();
    bs->mu.Lock();
    LRUHandle* h = (LRUHandle*)block_cache->Lookup(key);
    if (h == NULL) {
        block = new CacheBlock;
        block->fid = fid;
        block->block_idx = block_idx;
        block->sid = sid;
        h = (LRUHandle*)block_cache->Insert(key, block, 0xffffffffffffffff, &FlashBlockCacheImpl::BlockDeleter);
        if (h != NULL) {
            assert((uint64_t)(block_cache->Value((Cache::Handle*)h)) == (uint64_t)block);
            block->offset_in_blockset = h->cache_id;
            block->handle = h;
            block->bs_handle = bs->handle;
            //Log("[%s] Alloc Block: %s, sid %lu, fid %lu, block_idx %lu, hash %u, usage: %lu/%lu\n",
                //this->WorkPath().c_str(),
                //block->ToString().c_str(),
                //sid, fid, block_idx, hash,
                //block_cache->TotalCharge(),
                //options_.blocks_per_set);
        } else {
            delete block;
            block = NULL;
            assert(0);
        }
    } else {
        block = reinterpret_cast<CacheBlock*>(block_cache->Value((Cache::Handle*)h));
        block->bs_handle = block->bs_handle == NULL? bs->handle: block->bs_handle;
    }
    bs->mu.Unlock();

    block_set_cache_->Release((Cache::Handle*)bs->handle);
    stat_->MeasureTime(FLASH_BLOCK_CACHE_BS_LRU_LOOKUP,
                       options_.cache_env->NowMicros() - start_ts);
    return block;
}

Status FlashBlockCacheImpl::LogRecord(CacheBlock* block) {
    leveldb::WriteBatch batch;
    batch.Put(block->EncodeDBKey(), block->EncodeDBValue());
    return meta_db_->Write(leveldb::WriteOptions(), &batch);
}

Status FlashBlockCacheImpl::ReleaseBlock(CacheBlock* block, bool need_sync) {
    Status s;
    if (need_sync) { // TODO: dump meta into memtable
        s = LogRecord(block);
    }

    block->mu.Lock();
    block->ReleaseDataBlock();
    block->s = Status::OK(); // clear io status
    block->cv.SignalAll();
    block->mu.Unlock();

    //Log("[%s] release block: %s\n", this->WorkPath().c_str(), block->ToString().c_str());
    LRUHandle* h = block->handle;
    BlockSet* bs = reinterpret_cast<BlockSet*>(block_set_cache_->Value((Cache::Handle*)block->bs_handle));
    bs->block_cache->Release((Cache::Handle*)h);
    return s;
}

}  // namespace leveldb

