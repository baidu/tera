// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include <functional>

#include "persistent_cache/persistent_cache_file.h"
#include "persistent_cache/persistent_cache_impl.h"
#include "table/format.h"

namespace leveldb {

//
// CacheFile
//
Status CacheFile::Delete(uint64_t* size) {
  Status status = env_->GetFileSize(Path(), size);
  if (!status.ok()) {
    return status;
  }
  return env_->DeleteFile(Path());
}

//
// RandomAccessFile
//
Status RandomAccessCacheFile::Open() { return OpenImpl(); }

Status RandomAccessCacheFile::OpenImpl() {
  RandomAccessFile* file;
  auto status = env_->NewRandomAccessFile(Path(), &file, env_opt_);
  if (!status.ok()) {
    LEVELDB_LOG("Error opening random access file %s. %s\n", Path().c_str(),
                status.ToString().c_str());
  } else {
    file_.reset(file);
  }

  return status;
}

Status RandomAccessCacheFile::Read(size_t offset, size_t length, Slice* val,
                                   SstDataScratch* scratch) {
  if (!file_) {
    LEVELDB_LOG("Not Open\n");
    return Status::IOError("File Not Open");
  }
  Status s = ReadSstFile(file_.get(), env_opt_.use_direct_io_read, offset, length, val, scratch);
  if (!s.ok()) {
    LEVELDB_LOG("Error reading from file %s. %s\n", Path().c_str(), s.ToString().c_str());
    return s;
  }
  return Status::OK();
}

//
// WriteableCacheFile
//
Status WriteableCacheFile::Create() {
  auto path = Path();
  auto dir_pos = path.find_last_of('/');
  Status s;
  if (dir_pos != std::string::npos) {
    s = env_->CreateDir(path.substr(0, dir_pos));
    if (!s.ok()) {
      LEVELDB_LOG("Create dir failed %s, reason %s.\n", path.substr(0, dir_pos).c_str(),
                  s.ToString().c_str());
      return s;
    }
  }
  WritableFile* file;
  s = env_->NewWritableFile(path, &file, env_opt_);
  file_.reset(file);
  if (!s.ok()) {
    LEVELDB_LOG("Unable to create file %s. %s\n", Path().c_str(), s.ToString().c_str());
    return s;
  }

  assert(!refs_);
  ++refs_;

  return s;
}

Status WriteableCacheFile::Append(const Slice& val) {
  uint64_t i = 0;

  Status s;
  while (!(s = cache_->MakeRoomForWrite(val.size())).ok()) {
    Env::Default()->SleepForMicroseconds(kRetryIntervalUs);
    if (i++ >= write_retry_times_) {
      cache_->GetStats()->cache_errors.Inc();
      return s;
    }
  }

  s = file_->Append(val);
  cache_->GetStats()->write_count.Inc();

  if (s.ok()) {
    cache_->GetStats()->write_throughput.Add(val.size());
  } else {
    cache_->GetStats()->cache_errors.Inc();
  }

  return s;
}

void WriteableCacheFile::Close(const Slice& key) {
  file_.reset();
  assert(refs_);
  // Our env abstraction do not allow reading from a file opened for appending
  // We need close the file and re-open it for reading
  if (!RandomAccessCacheFile::OpenImpl().ok() || !cache_->Insert(key, this)) {
    LEVELDB_LOG("Close cache file: %s -> %s failed.\n", Path().c_str(), key.ToString().c_str());
    cache_->ForceEvict(this);
  }
  LEVELDB_LOG("Close cache file: %s -> %s succeed.\n", Path().c_str(), key.ToString().c_str());

  if (InsertCallback) {
    InsertCallback(key);
  }

  --refs_;
}

void WriteableCacheFile::Abandon() {
  file_.reset();
  assert(refs_);
  LEVELDB_LOG("Abandon cache file: %s\n", Path().c_str());
  cache_->ForceEvict(this);
  --refs_;
}
}  // namespace leveldb
