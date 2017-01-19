// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/filename.h"
#include "db/version_set.h"
#include "util/coding.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed. max tag number = 4096, min tag number = 1
enum Tag {
  kComparator           = 1,
  kLogNumber            = 2,
  kNextFileNumber       = 3,
  kLastSequence         = 4,
  kCompactPointer       = 5,
  kDeletedFileForCompat = 6,
  kNewFileForCompat     = 7,
  // 8 was used for large value refs
  kPrevLogNumber        = 9,
  kNewFile              = 10,
  kDeletedFile          = 11,
  // no more than 4096
  kMaxTag               = 1 << 20,
};

void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    std::string str;
    PutLengthPrefixedSlice(&str, comparator_);

    PutVarint32(dst, str.size() + kMaxTag);
    PutVarint32(dst, kComparator);
    dst->append(str.data(), str.size());
  }
  if (has_log_number_) {
    std::string str;
    PutVarint64(&str, log_number_);

    PutVarint32(dst, str.size() + kMaxTag);
    PutVarint32(dst, kLogNumber);
    dst->append(str.data(), str.size());
  }
  if (has_prev_log_number_) {
    std::string str;
    PutVarint64(&str, prev_log_number_);

    PutVarint32(dst, str.size() + kMaxTag);
    PutVarint32(dst, kPrevLogNumber);
    dst->append(str.data(), str.size());
  }
  if (has_next_file_number_) {
    std::string str;
    PutVarint64(&str, next_file_number_);

    PutVarint32(dst, str.size() + kMaxTag);
    PutVarint32(dst, kNextFileNumber);
    dst->append(str.data(), str.size());
  }
  if (has_last_sequence_) {
    std::string str;
    PutVarint64(&str, last_sequence_);

    PutVarint32(dst, str.size() + kMaxTag);
    PutVarint32(dst, kLastSequence);
    dst->append(str.data(), str.size());
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    std::string str;
    PutVarint32(&str, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(&str, compact_pointers_[i].second.Encode());

    PutVarint32(dst, str.size() + kMaxTag);
    PutVarint32(dst, kCompactPointer);
    dst->append(str.data(), str.size());
  }

  for (size_t i = 0; i < deleted_files_.size(); i++) {
    std::string str;
    const FileMetaData& f = deleted_files_[i].second;
    PutVarint32(&str, deleted_files_[i].first);  // level
    PutVarint64(&str, f.number);
    PutVarint64(&str, f.file_size);
    PutLengthPrefixedSlice(&str, f.smallest.Encode());
    PutLengthPrefixedSlice(&str, f.largest.Encode());

    PutVarint32(dst, str.size() + kMaxTag);
    PutVarint32(dst, kDeletedFile);
    dst->append(str.data(), str.size());
  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    std::string str;
    const FileMetaData& f = new_files_[i].second;
    PutVarint32(&str, new_files_[i].first);  // level
    PutVarint64(&str, f.number);
    PutVarint64(&str, f.file_size);
    PutLengthPrefixedSlice(&str, f.smallest.Encode());
    PutLengthPrefixedSlice(&str, f.largest.Encode());
    if (f.smallest_fake) {
      PutVarint32(&str, 1);
    } else {
      PutVarint32(&str, 0);
    }
    if (f.largest_fake) {
      PutVarint32(&str, 1);
    } else {
      PutVarint32(&str, 0);
    }

    PutVarint32(dst, str.size() + kMaxTag);
    PutVarint32(dst, kNewFile);
    dst->append(str.data(), str.size());
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return true;
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) &&
      v < static_cast<uint32_t>(config::kNumLevels)) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  FileMetaData f;
  Slice str;
  InternalKey key;

  while (msg == NULL && GetVarint32(&input, &tag)) {
    uint32_t len = 0;
    if (tag > kMaxTag) {
        len = tag - kMaxTag;
        if (!GetVarint32(&input, &tag)) {
          break;
        }
    }
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) &&
            GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      case kDeletedFileForCompat:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &number)) {
          DeleteFile(level, number);
        } else {
          msg = "deleted file";
        }
        break;

      case kNewFileForCompat:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          new_files_.push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry for compat";
        }
        break;

      case kNewFile:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          uint32_t smallest_fake = 0;
          uint32_t largest_fake = 0;
          if (GetVarint32(&input, &smallest_fake) &&
              GetVarint32(&input, &largest_fake)) {
            if (smallest_fake == 0) {
              f.smallest_fake = false;
            } else {
              f.smallest_fake = true;
            }
            if (largest_fake == 0) {
              f.largest_fake = false;
            } else {
              f.largest_fake = true;
            }
            new_files_.push_back(std::make_pair(level, f));
          } else {
            msg = "new-file entry 1";
          }
        } else {
          msg = "new-file entry 2";
        }
        break;

      case kDeletedFile:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          deleted_files_.push_back(std::make_pair(level, f));
        } else {
          msg = "deleted file";
        }
        break;

      default: // tag not know, skip it.
        input.remove_prefix(len);
        fprintf(stderr, "VersionEdit, skip unknow tag %d, len %d\n", tag, len);
        break;
    }
  }

  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != NULL) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  uint64_t tablet_number = 0;
  uint64_t file_number = 0;
  for (FileMetaSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter) {
    ParseFullFileNumber(iter->second.number, &tablet_number, &file_number);
    r.append("\n  DeleteFile: level ");
    AppendNumberTo(&r, iter->first);
    r.append(" tablet ");
    AppendNumberTo(&r, tablet_number);
    r.append(" file ");
    AppendNumberTo(&r, file_number);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    ParseFullFileNumber(f.number, &tablet_number, &file_number);
    r.append("\n  AddFile: level ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" tablet ");
    AppendNumberTo(&r, tablet_number);
    r.append(" file ");
    AppendNumberTo(&r, file_number);
    r.append(" size ");
    AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb
