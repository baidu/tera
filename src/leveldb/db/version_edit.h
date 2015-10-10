// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <map>
#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;
class VersionSetBuilder;

struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table
  bool smallest_fake;         // smallest is not real, have out-of-range keys
  bool largest_fake;          // largest is not real, have out-of-range keys
  bool being_compacted;       // true if the file is currently being compacted

  FileMetaData() :
      refs(0),
      allowed_seeks(1 << 30),
      file_size(0),
      smallest_fake(false),
      largest_fake(false),
      being_compacted(false) { }
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  uint64_t GetLastSequence() const {
    return last_sequence_;
  }
  uint64_t GetLogNumber() const {
    return log_number_;
  }
  uint64_t GetNextFileNumber() const {
    return next_file_number_;
  }
  std::string GetComparatorName() const {
    return comparator_;
  }

  bool HasNextFileNumber() const {
    return has_next_file_number_;
  }
  bool HasLastSequence() const {
    return has_last_sequence_;
  }
  bool HasLogNumber() const {
    return has_log_number_;
  }
  bool HasComparator() const {
    return has_comparator_;
  }
  bool HasFiles(std::vector<uint64_t>* deleted_files,
                std::vector<uint64_t>* added_files) {
      bool has_files = deleted_files_.size() > 0
          || new_files_.size() > 0;
    //  if (deleted_files && deleted_files_.size() > 0) {
    //    DeletedFileSet::iterator set_it = deleted_files_.begin();
    //    for (; set_it != deleted_files_.end(); ++set_it) {
    //        std::pair<int, uint64_t> pair = *set_it;
    //        deleted_files->push_back(pair.second);
    //    }
    //  }
    //  if (added_files && new_files_.size() > 0) {
    //    for (uint32_t i = 0; i < new_files_.size(); ++i) {
    //      FileMetaData& file = new_files_[i].second;
    //      added_files->push_back(file.number);
    //    }
    //  }
      return has_files;
  }

  void ModifyForMerge(std::map<uint64_t, uint64_t> num_map) {
    //if (num_map.size() == 0) {
    //    return;
    //}

    //// deleted file
    //DeletedFileSet deleted_files(deleted_files_);
    //deleted_files_.clear();
    //DeletedFileSet::iterator set_it = deleted_files.begin();
    //for (; set_it != deleted_files.end(); ++set_it) {
    //  std::pair<int, uint64_t> pair = *set_it;
    //  pair.second = num_map[pair.second];
    //  deleted_files_.insert(pair);
    //}
    //// new files
    //for (uint32_t i = 0; i < new_files_.size(); ++i) {
    //  FileMetaData& file = new_files_[i].second;
    //  file.number = num_map[file.number];
    //}
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  void AddFile(int level, const FileMetaData& f) {
    new_files_.push_back(std::make_pair(level, f));
    new_files_.back().second.being_compacted = false;
  }

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, int64_t number) {
    FileMetaData f;
    f.number = number;
    f.file_size = 0;
    f.smallest = InternalKey("", 0, kValueTypeForSeek);
    f.largest = InternalKey("", 0, kValueTypeForSeek);
    DeleteFile(level, f);
  }

  void DeleteFile(int level, const FileMetaData& f) {
    deleted_files_.push_back(std::make_pair(level, f));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;
  friend class VersionSetBuilder;

  typedef std::vector< std::pair<int, FileMetaData> > FileMetaSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;

  // Files in Version could be deleted by file number or file meta.
  // If deleted by file number, any file meta on this file would be deleted.
  // If deleted by file meta, only specified meta would be deleted and other
  // file metas on this file would be reserved.
  FileMetaSet deleted_files_;

  FileMetaSet new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
