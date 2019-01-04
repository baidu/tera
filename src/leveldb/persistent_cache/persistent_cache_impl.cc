// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "persistent_cache_impl.h"
#include "persistent_cache_file.h"
#include "persistent_cache_metadata.h"

#include "util/logging.h"

namespace leveldb {
using SubscriberType = tera::Subscriber::SubscriberType;
using PersistentCacheMetricNames::kWriteThroughput;
using PersistentCacheMetricNames::kWriteCount;
using PersistentCacheMetricNames::kReadThroughput;
using PersistentCacheMetricNames::kCacheHits;
using PersistentCacheMetricNames::kCacheMisses;
using PersistentCacheMetricNames::kCacheErrors;
using PersistentCacheMetricNames::kCacheCapacity;
using PersistentCacheMetricNames::kFileEntries;
using PersistentCacheMetricNames::kCacheSize;
using PersistentCacheMetricNames::kMetaDataSize;

std::string PersistentCacheConfig::ToString() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  path: %s\n", path.c_str());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  use_direct_reads: %d\n", env_opt.use_direct_io_read);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  use_direct_writes: %d\n", env_opt.use_direct_io_write);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  cache_size: %lu\n", cache_size);
  ret.append(buffer);

  return ret;
}

bool PersistentCacheImpl::IsCacheFile(const std::string& file) {
  // check if the file has .rc suffix
  if (file.size() <= 3 || file.substr(file.size() - 3) != ".rc") {
    return false;
  }

  std::string prefix = file.substr(0, file.size() - 3);
  auto last_point_pos = prefix.find_last_of('.');
  if (last_point_pos == std::string::npos) {
    return false;
  }

  auto cache_id_str = prefix.substr(last_point_pos + 1);
  uint64_t cache_id{0};
  try {
    cache_id = std::stoul(cache_id_str);
  } catch (...) {
    return false;
  };

  auto cache_file = metadata_.Lookup(cache_id);
  if (!cache_file) {
    return false;
  }

  bool is_same_path = false;

  auto status = opt_.env->IsSamePath(file, cache_file->Path(), &is_same_path);
  if (!status.ok()) {
    LEVELDB_LOG("Error checking same path, path1 %s, path2 %s, reason %s, remove it.\n",
                file.c_str(), cache_file->Path().c_str(), status.ToString().c_str());
    return false;
  }

  return true;
}

PersistentCacheImpl::Statistics::Statistics()
    : write_throughput(kWriteThroughput, {SubscriberType::THROUGHPUT}),
      write_count(kWriteCount, {SubscriberType::QPS}),
      read_throughput(kReadThroughput, {SubscriberType::THROUGHPUT}),
      cache_hits(kCacheHits, {SubscriberType::QPS}),
      cache_misses(kCacheMisses, {SubscriberType::QPS}),
      cache_errors(kCacheErrors, {SubscriberType::QPS}),
      file_entries(kFileEntries, {SubscriberType::LATEST}, false) {}

PersistentCacheImpl::PersistentCacheImpl(const PersistentCacheConfig& opt,
                                         const std::shared_ptr<Statistics>& stats)
    : opt_(opt),
      metadata_(opt),
      size_(kCacheSize, "path:" + opt.path, {SubscriberType::LATEST}, false),
      capacity_(kCacheCapacity, "path:" + opt.path, {SubscriberType::LATEST}, false),
      metadata_size_(kMetaDataSize, "path:" + opt.path, {SubscriberType::LATEST}, false),
      stats_(stats) {
  capacity_.Set(opt.cache_size);
}

void PersistentCacheImpl::CleanupCacheFolder(const std::string& folder) {
  LEVELDB_LOG("Begin Cleanup Cache Folder %s.\n", folder.c_str());
  DoCleanupCacheFolder(folder);
}

void PersistentCacheImpl::DoCleanupCacheFolder(const std::string& folder) {
  std::vector<std::string> children;
  Status status = opt_.env->GetChildren(folder, &children);
  if (!status.ok()) {
    LEVELDB_LOG("Error getting files for %s. %s\n", folder.c_str(), status.ToString().c_str());
    return;
  }
  for (const auto& child : children) {
    auto path = folder + "/" + child;
    SystemFileType type;
    status = opt_.env->GetFileType(path, &type);
    if (!status.ok()) {
      LEVELDB_LOG("Get file type failed for %s, reason %s, remove it.\n", path.c_str(),
                  status.ToString().c_str());
      status = opt_.env->DeleteFile(path);
      if (status.ok()) {
        continue;
      }
      LEVELDB_LOG("Error deleting file %s. %s, try delete as dir\n", path.c_str(),
                  status.ToString().c_str());

      status = opt_.env->DeleteDirRecursive(path);
      if (status.ok()) {
        continue;
      }
      LEVELDB_LOG("Error deleting dir %s. %s\n", path.c_str(), status.ToString().c_str());
    }

    switch (type) {
      case SystemFileType::kRegularFile: {
        if (!IsCacheFile(path)) {
          // non cache file
          LEVELDB_LOG("Removing non-cache file %s.\n", path.c_str());
          status = opt_.env->DeleteFile(path);
          if (!status.ok()) {
            LEVELDB_LOG("Error deleting file %s. %s, try delete as dir\n", path.c_str(),
                        status.ToString().c_str());
          }
        }
        break;
      }
      case SystemFileType::kDir: {
        bool is_same_path = false;
        status = opt_.env->IsSamePath(path, GetMetaPath(), &is_same_path);
        if (!status.ok()) {
          LEVELDB_LOG("Error checking same path, path1 %s, path2 %s, reason %s, skip it.\n",
                      path.c_str(), GetMetaPath().c_str(), status.ToString().c_str());
          continue;
        }
        if (!is_same_path) {
          DoCleanupCacheFolder(path);
        }
        break;
      }
      default: { LEVELDB_LOG("Unknown file type for path: %s.\n", path.c_str()); }
    }
  }
}

Status PersistentCacheImpl::Open() {
  Status status;

  assert(!size_.Get());

  // Check the validity of the options
  status = opt_.ValidateSettings();
  assert(status.ok());
  if (!status.ok()) {
    LEVELDB_LOG("Invalid persistent cache options.\n");
    return status;
  }

  // Create base directory
  status = opt_.env->CreateDir(opt_.path);
  if (!status.ok()) {
    LEVELDB_LOG("Error creating directory %s. %s.\n", opt_.path.c_str(), status.ToString().c_str());
    return status;
  }

  // Create meta directory
  status = opt_.env->CreateDir(GetMetaPath());
  assert(status.ok());
  if (!status.ok()) {
    LEVELDB_LOG("Error creating directory %s. %s.\n", GetMetaPath().c_str(),
                status.ToString().c_str());
    return status;
  }

  status = metadata_.Init(&writer_cache_id_, this);
  if (!status.ok()) {
    LEVELDB_LOG("Init metadata failed, reason: %s.\n", status.ToString().c_str());
    return status;
  }
  metadata_size_.Set(metadata_.GetDBSize());
  if (opt_.transfer_flash_env_files) {
    PullEnvFlashFiles();
  }
  // Clean up non-cache file
  CleanupCacheFolder(GetCachePath());

  LEVELDB_LOG("Persistent Cache Init Done, writer_cache_id: %lu\n", writer_cache_id_);

  return Status::OK();
}

Status PersistentCacheImpl::Read(const Slice& key, size_t offset, size_t length, Slice* content,
                                 SstDataScratch* scratch) {
  uint64_t cache_id;
  bool ok = metadata_.Lookup(key, &cache_id);
  if (!ok) {
    stats_->cache_misses.Inc();
    return Status::NotFound("persistent cache: cache id not found");
  }

  auto file = metadata_.Lookup(cache_id);
  if (!file) {
    // this can happen because the file index and cache file index are
    // different, and the cache file might be removed between the two lookups
    stats_->cache_misses.Inc();
    return Status::NotFound("persistent cache: cache file not found");
  }

  assert(file->refs_);
  auto s = file->Read(offset, length, content, scratch);
  file.reset();

  if (!s.ok()) {
    stats_->cache_misses.Inc();
    stats_->cache_errors.Inc();
    return s;
  }

  stats_->read_throughput.Add(content->size());
  stats_->cache_hits.Inc();
  return Status::OK();
}

void PersistentCacheImpl::ForceEvict(const Slice& key) {
  std::unique_ptr<CacheFile> file{metadata_.ForceEvict(key)};
  if (file) {
    assert(!file->refs_);
    if (DeleteFileAndReleaseCache(file.get()).ok()) {
      LEVELDB_LOG("Remove force evicted cache file: %s.", file->Path().c_str());
    }
  }
}

void PersistentCacheImpl::ForceEvict(CacheFile* file) { metadata_.ForceEvict(file); }

Status PersistentCacheImpl::NewWriteableCacheFile(const std::string& path,
                                                  WriteableCacheFile** file) {
  std::lock_guard<std::mutex> _(lock_);
  auto real_path = GetCachePath() + path + "." + std::to_string(writer_cache_id_) + ".rc";
  std::unique_ptr<WriteableCacheFile> f(new WriteableCacheFile{
      writer_cache_id_, opt_.env, opt_.env_opt, real_path, opt_.write_retry_times, this});

  auto status = f->Create();

  if (!status.ok()) {
    return status;
  }

  LEVELDB_LOG("Created cache file %s\n", f->Path().c_str());

  ++writer_cache_id_;

  auto success = metadata_.AddCacheFile(f.get());
  assert(success);
  *file = f.release();
  return Status::OK();
}

Status PersistentCacheImpl::MakeRoomForWrite(int64_t size) {
  std::lock_guard<std::mutex> _(lock_);
  assert(size_.Get() <= opt_.cache_size);

  if (size + size_.Get() <= opt_.cache_size) {
    // there is enough space to write
    size_.Add(size);
    return Status::OK();
  }

  // there is not enough space to fit the requested data
  // we can clear some space by evicting cold data
  while (size + size_.Get() > opt_.cache_size) {
    std::unique_ptr<CacheFile> f(metadata_.Evict());
    if (!f) {
      // nothing is evictable
      return Status::IOError("No space for writing persistent cache.");
    }
    assert(!f->refs_);
    if (DeleteFileAndReleaseCache(f.get()).ok()) {
      LEVELDB_LOG("Remove evicted cache file: %lu.rc.", f->cacheid());
    }
  }

  size_.Add(size);
  assert(size_.Get() <= opt_.cache_size);
  return Status::OK();
}

bool PersistentCacheImpl::Insert(const Slice& key, CacheFile* file) {
  std::lock_guard<std::mutex> _(lock_);
  uint64_t cache_id;
  if (metadata_.Lookup(key, &cache_id)) {
    LEVELDB_LOG("File already exists, force evict it: %s\n", key.ToString().c_str());
    ForceEvict(key);
  }

  // Insert file to meta data;
  auto file_in_meta = metadata_.Lookup(file->cacheid());
  assert(file_in_meta.get() == file);

  auto f_info = metadata_.Insert(key, file);
  metadata_size_.Set(metadata_.GetDBSize());

  if (!f_info) {
    return false;
  }

  file->SetInfo(f_info);
  return true;
}

std::vector<std::string> PersistentCacheImpl::GetAllKeys() { return metadata_.GetAllKeys(); }

Status PersistentCacheImpl::DeleteFileAndReleaseCache(CacheFile* file) {
  uint64_t file_size;
  auto status = file->Delete(&file_size);
  if (!status.ok()) {
    // unable to delete file
    LEVELDB_LOG("Remove evicted cache file %lu.rc failed, reason: %s.", file->cacheid(),
                status.ToString().c_str());
    return status;
  }

  assert(file_size <= (uint64_t)size_.Get());
  size_.Sub(file_size);
  assert(size_.Get() >= 0);
  return status;
}

void PersistentCacheImpl::GarbageCollect() {
  std::vector<std::unique_ptr<CacheFile>> evictable_files = metadata_.CollectEvictableFiles();
  for (auto& file : evictable_files) {
    assert(!file->refs_);
    DeleteFileAndReleaseCache(file.get());
    LEVELDB_LOG("Remove evictable file in GC: %lu.rc.", file->cacheid());
  }
}

Status NewPersistentCache(const PersistentCacheConfig& config,
                          std::shared_ptr<PersistentCache>* cache) {
  if (!cache) {
    return Status::IOError("invalid argument cache");
  }
  std::shared_ptr<PersistentCacheImpl::Statistics> stats{new PersistentCacheImpl::Statistics};
  auto pcache = std::make_shared<PersistentCacheImpl>(config, stats);
  Status s = pcache->Open();

  if (!s.ok()) {
    return s;
  }

  *cache = pcache;
  return s;
}
}  // namespace leveldb
