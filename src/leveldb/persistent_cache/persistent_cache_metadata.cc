// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include "persistent_cache_metadata.h"
#include "persistent_cache_impl.h"
#include "leveldb/db.h"
#include "db/table_cache.h"
#include "leveldb/persistent_cache.h"

namespace leveldb {

bool PersistentCacheMetaData::AddCacheFile(CacheFile* file) {
  return cache_file_index_.Insert(file);
}

PersistentCacheMetaData::CacheFileWrapper PersistentCacheMetaData::Lookup(uint64_t cache_id) {
  CacheFile* ret = nullptr;
  CacheFile lookup_key(cache_id);
  bool ok = cache_file_index_.Find(&lookup_key, &ret);
  if (ok) {
    assert(ret->refs_);
    return CacheFileWrapper{ret, [](CacheFile* f) { --f->refs_; }};
  }
  return nullptr;
}

CacheFile* PersistentCacheMetaData::Evict() {
  CacheFile* evicted_file = nullptr;

  {
    std::lock_guard<std::mutex> _(evictable_files_lock_);
    if (!evictable_files_.empty()) {
      auto iter = evictable_files_.begin();
      while (iter != evictable_files_.end() && (*iter)->refs_) {
        ++iter;
      }
      if (iter != evictable_files_.end()) {
        evicted_file = *iter;
        evictable_files_.erase(iter);
        LEVELDB_LOG("Find evictable file for evict, current evictable files size: %lu\n",
                    evictable_files_.size());
      }
    }
  }

  if (!evicted_file) {
    using std::placeholders::_1;
    auto fn = std::bind(&PersistentCacheMetaData::RemoveFileInfo, this, _1);
    evicted_file = cache_file_index_.Evict(fn);
  }

  return evicted_file;
}

void PersistentCacheMetaData::Clear() {
  cache_file_index_.Clear([](CacheFile* arg) { delete arg; });
  cache_id_index_.Clear([](FileInfo* arg) { delete arg; });
}

FileInfo* PersistentCacheMetaData::InsertWithoutPutDb(const Slice& key, uint64_t cache_id) {
  std::unique_ptr<FileInfo> f_info(new FileInfo(key, cache_id));
  if (!cache_id_index_.Insert(f_info.get())) {
    LEVELDB_LOG("Insert key failed %s , cache_id %lu\n", key.ToString().c_str(), cache_id);
    return nullptr;
  } else {
    std::lock_guard<std::mutex> _(keys_lock_);
    keys_.emplace(key.ToString());
  }
  return f_info.release();
}

FileInfo* PersistentCacheMetaData::Insert(const Slice& key, CacheFile* cache_file) {
  auto finfo = InsertWithoutPutDb(key, cache_file->cacheid());
  if (finfo) {
    db_->Put({}, key, cache_file->Path());
  }
  return finfo;
}

bool PersistentCacheMetaData::Lookup(const Slice& key, uint64_t* cache_id) {
  FileInfo lookup_key(key, 0);
  FileInfo* info;
  RWMutex* rlock = nullptr;
  if (!cache_id_index_.Find(&lookup_key, &info, &rlock)) {
    return false;
  }

  ReadUnlock _(rlock);
  assert(info->key_ == key.ToString());
  if (cache_id) {
    *cache_id = info->cache_id_;
  }
  return true;
}

FileInfo* PersistentCacheMetaData::Remove(const Slice& key) {
  FileInfo lookup_key(key, 0);
  FileInfo* finfo = nullptr;
  cache_id_index_.Erase(&lookup_key, &finfo);
  {
    std::lock_guard<std::mutex> _(keys_lock_);
    keys_.erase(key.ToString());
  }
  db_->Delete({}, key);
  return finfo;
}

void PersistentCacheMetaData::RemoveFileInfo(CacheFile* f) {
  FileInfo* tmp = nullptr;
  auto f_info = f->Info();
  if (f_info) {
    auto status = cache_id_index_.Erase(f_info, &tmp);
    {
      std::lock_guard<std::mutex> _(keys_lock_);
      keys_.erase(f_info->key_);
    }
    assert(status);
    assert(tmp == f_info);
    db_->Delete({}, f_info->key_);
    LEVELDB_LOG("Remove file from persistent cache: %s\n", f_info->key_.c_str());
    delete f_info;
  }
  // assert(f_info);
}

Status PersistentCacheMetaData::Init(uint64_t* recovered_cache_id, PersistentCacheImpl* cache) {
  Options opt;
  opt.filter_policy = NewBloomFilterPolicy(10);
  opt.block_cache = leveldb::NewLRUCache(8UL * 1024 * 1024);
  opt.table_cache = new leveldb::TableCache(8UL * 1024 * 1024);
  opt.info_log = Logger::DefaultLogger();

  auto lg_info = new leveldb::LG_info(0);
  lg_info->env = NewPosixEnv();
  lg_info->env->SetBackgroundThreads(5);

  opt.lg_info_list = new std::map<uint32_t, leveldb::LG_info*>;
  opt.lg_info_list->insert(std::make_pair(0, lg_info));
  opt.use_file_lock = false;  // Single process access is guaranteed by ts port.

  DB* db;
  auto status = DB::Open(opt, cache->GetMetaPath(), &db);

  if (!status.ok()) {
    LEVELDB_LOG("Open persistent cache metadata failed, reason: %s\n", status.ToString().c_str());
    return status;
  }

  db_.reset(db);

  ReadOptions read_options(&opt);
  read_options.fill_cache = false;
  read_options.verify_checksums = true;
  std::unique_ptr<Iterator> iterator{db_->NewIterator(read_options)};
  iterator->SeekToFirst();
  uint64_t max_cache_id{0};

  //  db format(kv):
  //  key -> file_path
  //  when restore meta data from disk, we get k-v pair from leveldb
  //  where key is the user cache key, and value is file_path in system
  while (iterator->Valid()) {
    const Slice key = iterator->key();
    Slice val = iterator->value();
    assert(val.ends_with(".rc"));

    // path format : /xxx/yyy/zzz/k.cache_id.rc
    // try extract cache_id from path.
    std::string path = val.ToString();
    std::string cache_id_str = path.substr(0, path.size() - 3);
    auto last_point_pos = cache_id_str.find_last_of('.');
    assert(last_point_pos != std::string::npos);
    cache_id_str = cache_id_str.substr(last_point_pos + 1);
    uint64_t cache_id{0};

    try {
      cache_id = std::stoul(cache_id_str);
    } catch (...) {
      LEVELDB_LOG("[%s] Fail to recover cache_id key: %s, value: %s\n",
                  cache->GetCachePath().c_str(), key.ToString().c_str(), val.ToString().c_str());
      db->Delete({}, key);
      continue;
    };

    LEVELDB_LOG("[%s] Recover persistent key %s, value %s , cache id %lu\n",
                cache->GetCachePath().c_str(), key.ToString().c_str(), val.ToString().c_str(),
                cache_id);

    auto cache_file = Lookup(cache_id);
    assert(!cache_file);

    max_cache_id = std::max(cache_id, max_cache_id);

    std::unique_ptr<RandomAccessCacheFile> file{
        new RandomAccessCacheFile{cache_id, config_.env, config_.env_opt, path}};

    uint64_t file_size;
    Status s;

    // Get Cache File Size
    if (!(s = config_.env->GetFileSize(file->Path(), &file_size)).ok()) {
      LEVELDB_LOG("[%s] Get size for file %s failed, reason: %s.\n", cache->GetCachePath().c_str(),
                  file->Path().c_str(), s.ToString().c_str());
    }

    // Try open file for read
    if (s.ok() && !(s = file->Open()).ok()) {
      LEVELDB_LOG("[%s] Get cache file %s for read failed, reason: %s.\n",
                  cache->GetCachePath().c_str(), file->Path().c_str(), s.ToString().c_str());
    }

    // Try make space for insert cache file
    if (s.ok() && !(s = cache->MakeRoomForWrite(file_size)).ok()) {
      LEVELDB_LOG("[%s] Make room for cache file %s failed, reason: %s.\n",
                  cache->GetCachePath().c_str(), file->Path().c_str(), s.ToString().c_str());
    }

    // Insert file to meta data
    if (s.ok()) {
      auto success = AddCacheFile(file.get());
      assert(success);
      FileInfo* info = InsertWithoutPutDb(key, file->cacheid());
      assert(info);
      file->SetInfo(info);
      file.release();
    }

    if (!s.ok()) {
      db_->Delete({}, iterator->key());
    }
    iterator->Next();
  }
  *recovered_cache_id = (uint64_t)max_cache_id + 1;
  return Status::OK();
}

CacheFile* PersistentCacheMetaData::ForceEvict(const Slice& key) {
  auto f_info = Remove(key);
  if (!f_info) {
    return nullptr;
  }

  auto file = Lookup(f_info->cache_id_);
  delete f_info;

  if (!file) {
    return nullptr;
  } else {
    file->SetInfo(nullptr);
  }

  // All cache files should be found in cache_file_index.
  auto removed_file = cache_file_index_.Remove(file.get());
  assert(removed_file == file.get());
  file.reset();

  if (removed_file->refs_ == 0) {
    // No body use this file, return to persistent cache for delete.
    return removed_file;
  } else {
    {
      // Someone is using this file, so put it to evictable_files_, waiting for gc or evict.
      std::lock_guard<std::mutex> _(evictable_files_lock_);
      evictable_files_.emplace(removed_file);
    }
    return nullptr;
  }
}

void PersistentCacheMetaData::ForceEvict(CacheFile* file) {
  assert(file && file->refs_ == 1);
  file->SetInfo(nullptr);
  auto removed_file = cache_file_index_.Remove(file);
  assert(removed_file == file);
  {
    // This method is only called when write cache file failed.
    // So there must be one refs_ handle in CacheFile.
    // We temporarily add it to evictable_files_, and it will be removed in next gc.
    std::lock_guard<std::mutex> _(evictable_files_lock_);
    evictable_files_.emplace(removed_file);
  }
}

std::vector<std::unique_ptr<CacheFile>> PersistentCacheMetaData::CollectEvictableFiles() {
  std::vector<std::unique_ptr<CacheFile>> files;
  {
    std::lock_guard<std::mutex> _{evictable_files_lock_};
    auto iter = evictable_files_.begin();
    while (iter != evictable_files_.end()) {
      // Find all evictable files whose refs_ is zero.
      if ((*iter)->refs_ == 0) {
        files.emplace_back(*iter);
        evictable_files_.erase(iter++);
      } else {
        ++iter;
      }
    }
  }
  return std::move(files);
}
}  // namespace leveldb
