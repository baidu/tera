// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <iostream>
#include <algorithm>
#include <math.h>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "leveldb/table_utils.h"
#include "leveldb/compact_strategy.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(int64_t target_file_size) {
  return 10 * target_file_size;
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(int64_t target_file_size) {
  return 25 * target_file_size;
}

static double MaxBytesForLevel(int level, int sst_size) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  // double result = 40 * 1048576.0;  // Result for both level-0 and level-1
  double result = 5 * sst_size;  // Result for both level-0 and level-1
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(int level, int64_t target_file_size) {
  if (level == 2) {
    return 2 * target_file_size;
  } else if (level > 2) {
    return 8 * target_file_size;
  }
  return target_file_size;  // We could vary per level to reduce number of
                            // files?
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}
static int64_t TotalFileSizeNotBeingCompacted(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    if (!files[i]->being_compacted) {
      sum += files[i]->file_size;
    }
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        vset_->table_cache_->Evict(vset_->dbname_, f->number);
        delete f;
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files,
             const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs before all keys and is therefore never after *f
  return (user_key != NULL && ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

// all user key before f
static bool BeforeFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs after all keys and is therefore never before *f
  return (user_key != NULL && ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, const Comparator* ucmp,
                           bool disjoint_sorted_files,  // it means of level > 0
                           const std::vector<FileMetaData*>& files, const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) || BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != NULL) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>* flist,
                       const std::string& dbname, const ReadOptions& opts)
      : icmp_(icmp),
        flist_(flist),
        dbname_(dbname),
        index_(flist->size()),  // Marks as invalid
        read_single_row_(opts.read_single_row),
        row_start_key_(opts.row_start_key, kMaxSequenceNumber, kValueTypeForSeek),
        row_end_key_(opts.row_end_key, kMaxSequenceNumber, kValueTypeForSeek) {}
  virtual bool Valid() const {
    if (index_ >= flist_->size()) {
      return false;
    }
    FileMetaData* f = (*flist_)[index_];
    if (read_single_row_ &&
        (icmp_.InternalKeyComparator::Compare(f->largest.Encode(), row_start_key_.Encode()) < 0 ||
         icmp_.InternalKeyComparator::Compare(f->smallest.Encode(), row_end_key_.Encode()) >= 0)) {
      return false;
    }
    return true;
  }
  virtual void Seek(const Slice& target) { index_ = FindFile(icmp_, *flist_, target); }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() { index_ = flist_->empty() ? 0 : flist_->size() - 1; }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    FileMetaData* f = (*flist_)[index_];
    value_buf_.resize(28);
    EncodeFixed64((char*)value_buf_.data(), f->number);
    EncodeFixed64((char*)value_buf_.data() + 8, f->file_size);
    Slice smallest = f->smallest_fake ? f->smallest.Encode() : "";
    Slice largest = f->largest_fake ? f->largest.Encode() : "";
    EncodeFixed32((char*)value_buf_.data() + 16, smallest.size());
    EncodeFixed32((char*)value_buf_.data() + 20, largest.size());
    EncodeFixed32((char*)value_buf_.data() + 24, dbname_.size());
    value_buf_.append(smallest.ToString());
    value_buf_.append(largest.ToString());
    value_buf_.append(dbname_);
    return Slice(value_buf_);
  }
  virtual Status status() const { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  const std::string dbname_;
  uint32_t index_;
  bool read_single_row_;
  InternalKey row_start_key_;
  InternalKey row_end_key_;

  // Backing store for value().  Holds the file number and size.
  mutable std::string value_buf_;
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options, const Slice& file_value) {
  assert(options.db_opt);
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  int32_t ssize = DecodeFixed32(file_value.data() + 16);
  int32_t lsize = DecodeFixed32(file_value.data() + 20);
  int32_t dbname_size = DecodeFixed32(file_value.data() + 24);
  assert(ssize >= 0 && ssize < 65536 && lsize >= 0 && lsize < 65536 && dbname_size > 0 &&
         dbname_size < 1024);
  return cache->NewIterator(
      options, std::string(file_value.data() + 28 + ssize + lsize, dbname_size),
      DecodeFixed64(file_value.data()), DecodeFixed64(file_value.data() + 8),
      Slice(file_value.data() + 28, ssize), Slice(file_value.data() + 28 + ssize, lsize));
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options, int level) const {
  ReadOptions opts = options;
  opts.db_opt = vset_->options_;
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level], vset_->dbname_, opts), GetFileIterator,
      vset_->table_cache_, opts);
}

void Version::AddIterators(const ReadOptions& options, std::vector<Iterator*>* iters) {
  ReadOptions opts = options;
  opts.db_opt = vset_->options_;
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    Slice smallest = f->smallest_fake ? f->smallest.Encode() : "";
    Slice largest = f->largest_fake ? f->largest.Encode() : "";
    iters->emplace_back(vset_->table_cache_->NewIterator(opts, vset_->dbname_, f->number,
                                                         f->file_size, smallest, largest));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->emplace_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
  CompactStrategy* compact_strategy;
};
}
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        if (!s->compact_strategy || !s->compact_strategy->Drop(parsed_key.user_key, 0)) {
          s->value->assign(v.data(), v.size());
        } else {
          s->state = kDeleted;  // stop searching in other files.
        }
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) { return a->number > b->number; }

Status Version::Get(const ReadOptions& options, const LookupKey& k, std::string* value,
                    GetStats* stats) {
  ReadOptions opts = options;
  opts.db_opt = vset_->options_;
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;

  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  std::vector<FileMetaData*> tmp;
  FileMetaData* tmp2;
  for (int level = 0; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Get the list of files to search in this level
    FileMetaData* const* files = &files_[level][0];
    if (level == 0) {
      // Level-0 files may overlap each other.  Find all files that
      // overlap user_key and process them in order from newest to oldest.
      tmp.reserve(num_files);
      for (uint32_t i = 0; i < num_files; i++) {
        FileMetaData* f = files[i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
          tmp.push_back(f);
        }
      }
      if (tmp.empty()) continue;

      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      files = &tmp[0];
      num_files = tmp.size();
    } else {
      // Binary search to find earliest index whose largest key >= ikey.
      uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
      if (index >= num_files) {
        files = NULL;
        num_files = 0;
      } else {
        tmp2 = files[index];
        if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
          // All of "tmp2" is past any data for user_key
          files = NULL;
          num_files = 0;
        } else {
          files = &tmp2;
          num_files = 1;
        }
      }
    }

    for (uint32_t i = 0; i < num_files; ++i) {
      if (last_file_read != NULL && stats->seek_file == NULL) {
        // We have had more than one seek for this read.  Charge the 1st file.
        stats->seek_file = last_file_read;
        stats->seek_file_level = last_file_read_level;
      }

      FileMetaData* f = files[i];
      last_file_read = f;
      last_file_read_level = level;

      Saver saver;
      saver.state = kNotFound;
      saver.ucmp = ucmp;
      saver.user_key = user_key;
      saver.value = value;
      saver.compact_strategy = vset_->options_->enable_strategy_when_get
                                   ? vset_->options_->compact_strategy_factory->NewInstance()
                                   : NULL;
      s = vset_->table_cache_->Get(opts, vset_->dbname_, f->number, f->file_size, ikey, &saver,
                                   SaveValue);
      delete saver.compact_strategy;
      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          break;  // Keep searching in other files
        case kFound:
          return s;
        case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
    }
  }

  return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != NULL) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == NULL) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool VersionSet::ExpandInputsToCleanCut(int level, std::vector<FileMetaData*>* inputs) {
  // This isn't good compaction
  assert(!inputs->empty());

  if (level == 0) {
    return true;
  }

  InternalKey smallest, largest;

  // Keep expanding inputs until we are sure that there is a "clean cut"
  // boundary between the files in input and the surrounding files.
  // This will ensure that no parts of a key are lost during compaction.
  size_t old_size;
  do {
    old_size = inputs->size();
    GetRange(*inputs, &smallest, &largest);
    current_->GetOverlappingInputs(level, &smallest, &largest, inputs);
  } while (inputs->size() > old_size);

  // If, after the expansion, there are files that are already under
  // compaction, then we must drop/cancel this compaction.
  if (AreFilesInCompaction(*inputs)) {
    return false;
  }
  return true;
}

void Version::DEBUG_pick(const std::string& msg, int level, const std::vector<FileMetaData*>& files,
                         const InternalKey& smallest, const InternalKey& largest) const {
  std::string r = "";
  r.append("\nDEBUG_PICK####### level:");
  AppendNumberTo(&r, level);
  r.append(" range[");
  r.append(smallest.DebugString());
  r.append(", ");
  r.append(largest.DebugString());
  r.append("] \n");

  for (size_t i = 0; i < files.size(); i++) {
    r.append("NO.");
    AppendNumberTo(&r, static_cast<uint32_t>(files[i]->number & 0xffffffff));
    r.append(" f_size:");
    AppendNumberTo(&r, files[i]->file_size);
    r.append(" d_size:");
    AppendNumberTo(&r, files[i]->data_size);
    r.append(" in_comp:");
    r.append(files[i]->being_compacted ? "T" : "F");
    r.append(" [");
    r.append(files[i]->smallest.DebugString());
    r.append(" .. ");
    r.append(files[i]->largest.DebugString());
    r.append("]\n");
  }
  LEVELDB_LOG(vset_->options_->info_log, "[%s] %s %s", vset_->dbname_.c_str(), msg.c_str(),
              r.c_str());
}

void Version::GetCleanCutInputsWithinInterval(int level, const InternalKey* begin,
                                              const InternalKey* end,
                                              std::vector<FileMetaData*>* inputs) {
  if (files_[level].size() == 0) {
    // empty level, no inputs in it
    return;
  }
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  if (begin != nullptr && end != nullptr && level > 0) {
    // only when level > 0 files are sorted in same level
    BinarySearchOverlappingInputs(level, user_begin, user_end, true /* within interval */, inputs);
  }
}

void Version::BinarySearchOverlappingInputs(int level, const Slice& user_begin,
                                            const Slice& user_end, bool within_interval,
                                            std::vector<FileMetaData*>* inputs) {
  assert(level > 0);
  // use row key comparator
  CompactStrategy* strategy = vset_->options_->compact_strategy_factory->NewInstance();
  const Comparator* ucmp = strategy->RowKeyComparator();
  if (ucmp == NULL) {
    ucmp = vset_->icmp_.user_comparator();
  }

  // binary search overlap index and extend range
  int start_index, end_index;
  bool found = false;
  int mid = 0, min = 0, max = static_cast<int>(files_[level].size()) - 1;
  while (min <= max) {
    mid = (min + max) / 2;
    FileMetaData* f = files_[level][mid];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (within_interval) {
      if (ucmp->Compare(file_start, user_begin) < 0) {
        min = mid + 1;
      } else if (ucmp->Compare(file_limit, user_end) > 0) {
        max = mid - 1;
      } else {
        // found middle index within interval with range
        ExtendRangeWithinInterval(level, ucmp, user_begin, user_end, mid, &start_index, &end_index);
        found = true;
        break;
      }
    } else {
      if (ucmp->Compare(file_limit, user_begin) < 0) {
        min = mid + 1;
      } else if (ucmp->Compare(file_start, user_end) > 0) {
        max = mid - 1;
      } else {
        // found middle index overlap with range
        ExtendRangeOverlappingInterval(level, ucmp, user_begin, user_end, mid, &start_index,
                                       &end_index);
        found = true;
        break;
      }
    }
  }
  for (int i = start_index; found && i <= end_index; ++i) {
    inputs->push_back(files_[level][i]);
  }
  delete strategy;
}

void Version::ExtendRangeOverlappingInterval(int level, const Comparator* user_cmp,
                                             const Slice& user_begin, const Slice& user_end,
                                             unsigned int mid_index, int* start_index,
                                             int* end_index) {
  assert(mid_index < files_[level].size());
  *start_index = mid_index + 1;
  *end_index = mid_index;
  int file_count = 0;

  // Select start index, case patten:
  //          [               ]
  //      [f1 ] [f2]  [f3]
  // find from 'mid' to left, on this case f1 will be selected
  for (int i = mid_index; i >= 0; i--) {
    const FileMetaData* f = files_[level][i];
    const Slice file_limit = f->largest.user_key();
    // table use rowkey, kv/ttlkv use user_key
    if (user_cmp->Compare(file_limit, user_begin) >= 0) {
      *start_index = i;
      ++file_count;
    } else {
      break;
    }
  }
  // Select end index, case patten:
  //          [               ]
  //              [f3 ] [f4]  [f5]
  // find from 'mid+1' to right, on this case f5 will be selected
  for (unsigned int i = mid_index + 1; i < files_[level].size(); i++) {
    const FileMetaData* f = files_[level][i];
    const Slice file_start = f->smallest.user_key();
    // table use rowkey, kv/ttlkv use user_key
    if (user_cmp->Compare(file_start, user_end) <= 0) {
      ++file_count;
      *end_index = i;
    } else {
      break;
    }
  }
  assert(file_count == *end_index - *start_index + 1);
}

void Version::ExtendRangeWithinInterval(int level, const Comparator* user_cmp,
                                        const Slice& user_begin, const Slice& user_end,
                                        unsigned int mid_index, int* start_index, int* end_index) {
  assert(level > 0);

  ExtendRangeOverlappingInterval(level, user_cmp, user_begin, user_end, mid_index, start_index,
                                 end_index);
  int left = *start_index;
  int right = *end_index;
  // shrink from left to right
  while (left <= right) {
    const FileMetaData* f = files_[level][left];
    const Slice& first_key_in_range = f->smallest.user_key();
    if (user_cmp->Compare(first_key_in_range, user_begin) < 0) {
      left++;
      continue;
    }
    if (left > 0) {  // If not first file
      const FileMetaData* f_before = files_[level][left - 1];
      const Slice& last_key_before = f_before->largest.user_key();
      if (user_cmp->Compare(first_key_in_range, last_key_before) == 0) {
        // The first user key(row key) in range overlaps
        // with the previous file's last key
        left++;
        continue;
      }
    }
    break;
  }
  // shrink from right to left
  while (left <= right) {
    const FileMetaData* f = files_[level][right];
    const Slice& last_key_in_range = f->largest.user_key();
    if (user_cmp->Compare(last_key_in_range, user_end) > 0) {
      right--;
      continue;
    }
    if (right < static_cast<int>(files_[level].size()) - 1) {  // If not the last file
      const FileMetaData* f_next = files_[level][right + 1];
      const Slice& first_key_after = f_next->smallest.user_key();
      if (user_cmp->Compare(last_key_in_range, first_key_after) == 0) {
        // The last user key(row key) in range overlaps
        // with the next file's first key
        right--;
        continue;
      }
    }
    break;
  }

  *start_index = left;
  *end_index = right;
}

void VersionSet::SetupGrandparents(int level, const std::vector<FileMetaData*>& inputs,
                                   const std::vector<FileMetaData*>& output_inputs,
                                   std::vector<FileMetaData*>* grandparents) {
  InternalKey start, end;
  GetRange2(inputs, output_inputs, &start, &end);
  current_->GetOverlappingInputs(level + 2, &start, &end, grandparents);
}

void Version::GetCompactionScores(std::vector<std::pair<double, uint64_t>>* scores) const {
  // add level scores
  for (size_t i = 0; i < compaction_score_.size(); i++) {
    if (compaction_score_[i] >= 1) {
      scores->emplace_back(compaction_score_[i], 0);
    }
  }

  // add del score
  if (del_trigger_compact_ != NULL && !del_trigger_compact_->being_compacted &&
      del_trigger_compact_->del_percentage > vset_->options_->del_percentage) {
    scores->emplace_back((double)(del_trigger_compact_->del_percentage / 100.0), 0);
  }

  // add seek score
  if (file_to_compact_ != NULL && !file_to_compact_->being_compacted) {
    scores->emplace_back(0.1, 0);
  }

  // add ttl score
  if (ttl_trigger_compact_ != NULL && !ttl_trigger_compact_->being_compacted) {
    int64_t ttl = ttl_trigger_compact_->check_ttl_ts - vset_->env_->NowMicros();
    int64_t few_time_to_live = 900 * 1000000;  // 9 minutes
    if (ttl < few_time_to_live) {
      uint64_t wait_time = (ttl <= 0) ? 0 : (ttl / 1000 * 1000);
      scores->emplace_back((double)((ttl_trigger_compact_->ttl_percentage + 1) / 100.0), wait_time);
    }
  }
}

bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  // use row key comparator
  CompactStrategy* strategy = vset_->options_->compact_strategy_factory->NewInstance();
  const Comparator* ucmp = strategy->RowKeyComparator();
  if (ucmp == NULL) {
    ucmp = vset_->icmp_.user_comparator();
  }
  bool overlap = SomeFileOverlapsRange(vset_->icmp_, ucmp, (level > 0), files_[level],
                                       smallest_user_key, largest_user_key);
  delete strategy;
  return overlap;
}

int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > MaxGrandParentOverlapBytes(vset_->options_->sst_size)) {
        break;
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != NULL) {
    user_begin = begin->user_key();
  }
  if (end != NULL) {
    user_end = end->user_key();
  }

  // use row key comparator
  CompactStrategy* strategy = vset_->options_->compact_strategy_factory->NewInstance();
  const Comparator* user_cmp = strategy->RowKeyComparator();
  if (user_cmp == NULL) {
    user_cmp = vset_->icmp_.user_comparator();
  }
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != NULL && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != NULL && user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
  if (strategy != NULL) {
    delete strategy;
  }
  return;
}

void Version::GetApproximateSizes(uint64_t* size, uint64_t* size_under_level1) {
  uint64_t total_size = 0;
  // calculate file size under level1
  for (int level = 1; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      total_size += files[i]->data_size;
    }
  }
  if (size_under_level1) {
    *size_under_level1 = total_size;
  }

  // calculate level0
  const std::vector<FileMetaData*>& files = files_[0];
  for (size_t i = 0; i < files.size(); i++) {
    total_size += files[i]->data_size;
  }
  if (size) {
    *size = total_size;
  }
}

bool Version::FindSplitKey(double ratio, std::string* split_key) {
  assert(ratio >= 0 && ratio <= 1);
  uint64_t size_under_level1;
  GetApproximateSizes(NULL, &size_under_level1);

  uint64_t want_split_size = static_cast<uint64_t>(size_under_level1 * ratio);

  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  const FileMetaData* largest_file = NULL;
  size_t split_size = 0;
  size_t now_pos[config::kNumLevels] = {0};
  while (split_size < want_split_size) {
    largest_file = NULL;
    int step_level = -1;
    for (int level = 1; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = files_[level];
      if (now_pos[level] >= files.size()) {
        continue;
      }
      const FileMetaData* file = files[now_pos[level]];
      if (largest_file == NULL ||
          user_cmp->Compare(largest_file->largest.user_key(), file->largest.user_key()) > 0) {
        largest_file = file;
        step_level = level;
      }
    }
    // when leveldb is empty
    if (largest_file == NULL) {
      return false;
    }
    split_size += files_[step_level][now_pos[step_level]]->data_size;
    now_pos[step_level]++;
  }
  if (largest_file == NULL) {
    return false;
  }
  *split_key = largest_file->largest.user_key().ToString();
  return true;
}

bool Version::FindKeyRange(std::string* smallest_key, std::string* largest_key) {
  std::string sk, lk;
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* file = files[i];
      if (sk.empty() || user_cmp->Compare(file->smallest.user_key(), sk) < 0) {
        sk = file->smallest.user_key().ToString();
      }
      if (lk.empty() || user_cmp->Compare(file->largest.user_key(), lk) > 0) {
        lk = file->largest.user_key().ToString();
      }
    }
  }
  LEVELDB_LOG(vset_->options_->info_log, "[%s] find key range: [%s,%s].\n", vset_->dbname_.c_str(),
              sk.c_str(), lk.c_str());
  if (smallest_key) {
    *smallest_key = sk;
  }
  if (largest_key) {
    *largest_key = lk;
  }
  return true;
}

//  end of tera-specific

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->data_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSetBuilder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  typedef std::vector<FileMetaData> DeleteFileSet;
  struct LevelState {
    DeleteFileSet deleted_files;
    FileSet* added_files;
    const InternalKeyComparator* icmp;

    bool IsFileDeleted(const FileMetaData* f) {
      assert(icmp);
      DeleteFileSet::iterator fi = deleted_files.begin();
      for (; fi != deleted_files.end(); ++fi) {
        if (fi->number == f->number) {
          if (fi->file_size == 0) {
            // file is deleted by number, delete all file meta
            return true;
          } else if (icmp->Compare(fi->smallest, f->smallest) == 0 &&
                     icmp->Compare(fi->largest, f->largest) == 0) {
            // file is deleted by file meta, verify rest infos
            assert(fi->file_size == f->file_size);
            return true;
          }
        }
      }
      return false;
    }
    void DoNotDelete(const FileMetaData* f) {
      assert(icmp);
      DeleteFileSet::iterator fi = deleted_files.begin();
      while (fi != deleted_files.end()) {
        if (fi->number == f->number) {
          if (fi->file_size == 0) {
            // file is deleted by number, delete all file meta
            return;
          } else if (icmp->Compare(fi->smallest, f->smallest) == 0) {
            // file is deleted by file meta, revert it
            assert(icmp->Compare(fi->largest, f->largest) == 0);
            assert(fi->file_size == f->file_size);
            fi = deleted_files.erase(fi);
            continue;
          }
        }
        ++fi;
      }
    }
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  VersionSetBuilder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
      levels_[level].icmp = &vset_->icmp_;
    }
  }

  ~VersionSetBuilder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          vset_->table_cache_->Evict(vset_->dbname_, f->number);
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] = edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    const VersionEdit::FileMetaSet& del = edit->deleted_files_;
    for (VersionEdit::FileMetaSet::const_iterator iter = del.begin(); iter != del.end(); ++iter) {
      const int level = iter->first;
      FileMetaData f = iter->second;
      ModifyFileMeta(&f);
      levels_[level].deleted_files.push_back(f);
    }

    // Add new files
    VersionEdit::FileMetaSet::iterator it = edit->new_files_.begin();
    while (it != edit->new_files_.end()) {
      const int level = it->first;
      FileMetaData& f_new = it->second;
      if (!ModifyFileMeta(&f_new)) {
        // File is out-of-range, erase from edit
        it = edit->new_files_.erase(it);
        continue;
      }
      ++it;

      FileMetaData* f = new FileMetaData(f_new);
      f->refs = 1;
      f->being_compacted = false;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      uint64_t size_for_one_seek = 16384ULL * vset_->options_->seek_latency / 10000000;
      if (size_for_one_seek <= 0) {
        size_for_one_seek = 1;
      }
      f->allowed_seeks = (f->file_size / size_for_one_seek);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].DoNotDelete(f);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      for (FileSet::const_iterator added_iter = added->begin(); added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i - 1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) > 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(), this_begin.DebugString().c_str());
            abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].IsFileDeleted(f) > 0) {
      // File is deleted: do nothing
      return;
    }
    std::vector<FileMetaData*>* files = &v->files_[level];
    if (level > 0 && !files->empty()) {
      FileMetaData* f_old = (*files)[files->size() - 1];
      // Must not overlap
      assert(vset_->icmp_.Compare(f_old->largest, f->smallest) <= 0);
    }
    f->refs++;
    files->push_back(f);
  }
  // Modify key range
  // Return false if file out of tablet range
  bool ModifyFileMeta(FileMetaData* f) {
    // Try modify key range
    if (!vset_->db_key_start_.user_key().empty() &&
        vset_->icmp_.Compare(f->smallest, vset_->db_key_start_) < 0) {
      if (vset_->icmp_.Compare(f->largest, vset_->db_key_start_) > 0) {
        LEVELDB_LOG(vset_->options_->info_log, "[%s] reset file smallest key: %s, from %s to %s\n",
                    vset_->dbname_.c_str(), FileNumberDebugString(f->number).c_str(),
                    f->smallest.DebugString().c_str(), vset_->db_key_start_.DebugString().c_str());
        f->smallest = vset_->db_key_start_;
        f->smallest_fake = true;
        f->data_size = 0;
      } else {
        // file out of tablet range, skip it;
        return false;
      }
    }
    if (!vset_->db_key_end_.user_key().empty() &&
        vset_->icmp_.Compare(f->largest, vset_->db_key_end_) > 0) {
      if (vset_->icmp_.Compare(f->smallest, vset_->db_key_end_) < 0) {
        LEVELDB_LOG(vset_->options_->info_log, "[%s] reset file largest key: %s, from %s to %s\n",
                    vset_->dbname_.c_str(), FileNumberDebugString(f->number).c_str(),
                    f->largest.DebugString().c_str(), vset_->db_key_end_.DebugString().c_str());
        f->largest = vset_->db_key_end_;
        f->largest_fake = true;
        f->data_size = 0;
      } else {
        // file out of tablet range, skip it;
        return false;
      }
    }
    return true;
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      env_opt_(*options),
      table_cache_(table_cache),
      icmp_(*cmp),
      db_key_start_(options->key_start, kMaxSequenceNumber, kValueTypeForSeek),
      db_key_end_(options->key_end, kMaxSequenceNumber, kValueTypeForSeek),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      force_switch_manifest_(false),
      last_switch_manifest_(0),
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_size_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL) {
  last_switch_manifest_ = env_->NowMicros();
  level_size_counter_.resize(config::kNumLevels, 0);
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  // Debug
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      LEVELDB_LOG(options_->info_log, "[%s] finish : %08u %08u, level: %d, s: %d %s, l: %d %s\n",
                  dbname_.c_str(), static_cast<uint32_t>(files[i]->number >> 32 & 0x7fffffff),
                  static_cast<uint32_t>(files[i]->number & 0xffffffff), level,
                  files[i]->smallest_fake, files[i]->smallest.user_key().ToString().data(),
                  files[i]->largest_fake, files[i]->largest.user_key().ToString().data());
    }
  }
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);

  for (uint64_t i = 0; i < level_size_counter_.size(); ++i) {
    level_size_counter_[i] = TotalFileSize(v->files_[i]);
  }

  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

// multi thread safe
// Information kept for every waiting manifest writer
struct VersionSet::ManifestWriter {
  Status status;
  VersionEdit* edit = nullptr;
  bool done;
  port::CondVar cv;

  explicit ManifestWriter(port::Mutex* mu) : done(false), cv(mu) {}
};
void VersionSet::LogAndApplyHelper(VersionSetBuilder* builder, VersionEdit* edit) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);

  if (edit->HasLastSequence()) {
    LEVELDB_LOG(options_->info_log, "[%s] LogLastSequence %lu", dbname_.c_str(),
                edit->GetLastSequence());
    assert(edit->GetLastSequence() >= last_sequence_);
  } else {
    edit->SetLastSequence(last_sequence_);
  }
  builder->Apply(edit);
}

void VersionSet::GetCurrentLevelSize(std::vector<int64_t>* result) {
  result->clear();
  *result = level_size_counter_;
}

void VersionSet::MaybeSwitchManifest() {
  if (descriptor_log_ != NULL) {
    const uint64_t switch_interval = options_->manifest_switch_interval * 1000000UL;
    if (last_switch_manifest_ + switch_interval < env_->NowMicros() &&
        descriptor_size_ >= options_->manifest_switch_size << 20) {
      force_switch_manifest_ = true;
    }
  }
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  mu->AssertHeld();
  // multi write control, do not batch edit write, but multi thread safety
  ManifestWriter w(mu);
  w.edit = edit;
  manifest_writers_.push_back(&w);
  while (!w.done && &w != manifest_writers_.front()) {
    w.cv.Wait();
  }
  assert(manifest_writers_.front() == &w);

  // first manifest writer, batch edit
  Version* v = new Version(this);
  {
    VersionSetBuilder builder(this, current_);
    LogAndApplyHelper(&builder, w.edit);
    builder.SaveTo(v);
  }
  Finalize(v);  // recalculate new version score

  MaybeSwitchManifest();

  uint64_t manifest_file_num = manifest_file_number_;
  int retry_count = 0;
  Status s;
  std::string record;
  edit->EncodeTo(&record);
  // Unlock during expensive MANIFEST log write
  do {
    s = Status::OK();
    std::string new_manifest_file;
    // timeout cause switch or failure cause switch
    if (force_switch_manifest_) {
      manifest_file_number_ = NewFileNumber();
      last_switch_manifest_ = env_->NowMicros();
    }
    mu->Unlock();

    // close current manifest
    if (force_switch_manifest_) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      LEVELDB_LOG(options_->info_log, "[%s] force switch MANIFEST #%lu to #%lu", dbname_.c_str(),
                  manifest_file_num, manifest_file_number_);
      force_switch_manifest_ = false;
    }

    if (descriptor_log_ == NULL) {
      // Initialize new descriptor log file if necessary by creating
      // a temporary file that contains a snapshot of the current version.
      assert(descriptor_file_ == NULL);
      new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
      edit->SetNextFile(manifest_file_number_ + 1);
      s = env_->NewWritableFile(new_manifest_file, &descriptor_file_, env_opt_);
      if (s.ok()) {
        descriptor_log_ = new log::Writer(descriptor_file_);
        s = WriteSnapshot(descriptor_log_);
        if (!s.ok()) {
          LEVELDB_LOG(options_->info_log,
                      "[%s][dfs error] writesnapshot MANIFEST[%s] error, status[%s].\n",
                      dbname_.c_str(), new_manifest_file.c_str(), s.ToString().c_str());
        }
      } else {
        LEVELDB_LOG(options_->info_log, "[%s][dfs error] open MANIFEST[%s] error, status[%s].\n",
                    dbname_.c_str(), new_manifest_file.c_str(), s.ToString().c_str());
      }
    }

    // Write new record to MANIFEST log
    if (s.ok()) {
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
        if (!s.ok()) {
          LEVELDB_LOG(options_->info_log, "[%s][dfs error] MANIFEST sync error: %s\n",
                      dbname_.c_str(), s.ToString().c_str());
        }
      } else {
        LEVELDB_LOG(options_->info_log, "[%s][dfs error] AddRecord MANIFEST error: %s\n",
                    dbname_.c_str(), s.ToString().c_str());
      }
    }

    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
      if (s.ok()) {
        LEVELDB_LOG(options_->info_log, "[%s] set CURRENT #%lu to #%llu success\n", dbname_.c_str(),
                    manifest_file_num, static_cast<unsigned long long>(manifest_file_number_));
        manifest_file_num = manifest_file_number_;
      } else {
        LEVELDB_LOG(options_->info_log, "[%s][dfs error] set CURRENT #%lu to #%lu error: %s\n",
                    dbname_.c_str(), manifest_file_num, manifest_file_number_,
                    s.ToString().c_str());
      }
    }

    // switch manifest success, try delete obsolete file
    if (!new_manifest_file.empty() && s.ok()) {
      // manifest file set, keep latest 3 manifest files for backup
      std::set<std::string> manifest_set;
      std::vector<std::string> filenames;
      env_->GetChildren(dbname_, &filenames);

      uint64_t number;
      FileType type;
      for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
          bool keep = true;
          switch (type) {
            case kDescriptorFile:
              manifest_set.insert(filenames[i]);
              if (manifest_set.size() > 3) {
                std::set<std::string>::iterator it = manifest_set.begin();
                ParseFileName(*it, &number, &type);
                if (number < manifest_file_number_) {
                  // Keep my manifest file, and any newer incarnations'
                  // (in case there is a race that allows other incarnations)
                  filenames[i] = *it;
                  keep = false;
                  manifest_set.erase(it);
                }
              }
              break;
            case kTempFile:
              // Any temp files that are currently being written to must
              // be recorded in pending_outputs_, which is inserted into "live"
              keep = false;
              break;
            default:
              break;
          }

          if (!keep) {
            LEVELDB_LOG(options_->info_log, "[%s] version_set Delete type=%s #%lld, fname %s\n",
                        dbname_.c_str(), FileTypeToString(type),
                        static_cast<unsigned long long>(number), filenames[i].c_str());
            env_->DeleteFile(dbname_ + "/" + filenames[i]);
          }
        }
      }
    }
    // if MANIFEST or CURRENT file write error because of losting directory
    // lock,
    // do not try to switch manifest anymore
    if (!s.ok() && !s.IsIOPermissionDenied()) {
      force_switch_manifest_ = true;
      if (!new_manifest_file.empty()) {
        env_->DeleteFile(new_manifest_file);
      }
    }

    // retry until success
    if (force_switch_manifest_) {
      retry_count++;
      int sec = 1;
      for (int i = 1; i < retry_count && i < 4; i++) {
        sec *= 2;
      }
      LEVELDB_LOG(options_->info_log,
                  "[%s] Waiting after %d, LogAndApply sync error: %s, retry: %d", dbname_.c_str(),
                  sec, s.ToString().c_str(), retry_count);
      env_->SleepForMicroseconds(sec * 1000000);
    }

    mu->Lock();
  } while (force_switch_manifest_);  // bugfix issue=tera-10, dfs sync fail, but
                                     // eventually success, cause reload fail

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
    last_sequence_ = edit->GetLastSequence();
    descriptor_size_ += record.size();
  } else {
    delete v;
    force_switch_manifest_ = true;
    LEVELDB_LOG(options_->info_log, "[%s][dfs error] set force_switch_manifest", dbname_.c_str());
  }

  manifest_writers_.pop_front();
  if (!manifest_writers_.empty()) {
    manifest_writers_.front()->cv.Signal();
  }
  return s;
}

// reads contents of CURRENT and stores to *dscname
Status VersionSet::ReadCurrentFile(uint64_t tablet, std::string* dscname) {
  Status s;
  std::string pdbname;
  if (tablet > 0) {
    pdbname = RealDbName(dbname_, tablet);
  } else {
    pdbname = dbname_;
  }
  std::string current;
  s = ReadFileToString(env_, CurrentFileName(pdbname), &current);
  if (!s.ok()) {
    LEVELDB_LOG(options_->info_log, "[%s] read CURRENT failed: %s.", dbname_.c_str(),
                CurrentFileName(pdbname).c_str());
    s = env_->FileExists(CurrentFileName(pdbname));
    if (s.IsNotFound()) {
      // lost CURRENT
      if (options_->ignore_corruption_in_open) {
        // try to failover: find and use the newest mainfest
        LEVELDB_LOG(options_->info_log, "[%s] lost CURRENT but ignore_corruption_in_open: %s.",
                    dbname_.c_str(), CurrentFileName(pdbname).c_str());
      } else {
        LEVELDB_LOG(options_->info_log, "[%s] lost CURRENT and NOT ignore_corruption_in_open: %s.",
                    dbname_.c_str(), CurrentFileName(pdbname).c_str());
        return Status::Corruption("CURRENT lost", s.ToString());
      }
    } else if (s.IsTimeOut()) {
      LEVELDB_LOG(options_->info_log, "[%s]check exist CURRENT timeout: %s.", dbname_.c_str(),
                  CurrentFileName(pdbname).c_str());
      return Status::TimeOut("check exist CURRENT timeout");
    } else {
      // status of current is unknown
      LEVELDB_LOG(options_->info_log, "[%s]status of CURRENT is unknown: %s.", dbname_.c_str(),
                  CurrentFileName(pdbname).c_str());
      return Status::IOError("status of CURRENT is unknown");
    }
  } else if (current.empty() || current[current.size() - 1] != '\n') {
    LEVELDB_LOG(options_->info_log, "[%s] current file error: %s, content:\"%s\", size:%lu.",
                dbname_.c_str(), CurrentFileName(pdbname).c_str(), current.c_str(), current.size());
    if (options_->ignore_corruption_in_open) {
      ArchiveFile(env_, CurrentFileName(pdbname));
    } else if (current.size() == 0) {
      return Status::Corruption("CURRENT size = 0");
    } else {
      return Status::Corruption("CURRENT incomplete content");
    }
  } else {
    current.resize(current.size() - 1);
    *dscname = pdbname + "/" + current;
  }

  // if program runs to here, there are only 2 possibilities
  // 1). read CURRENT success
  // 2). lost CURRENT and ignore_corruption_in_open
  assert((s.ok()) || (options_->ignore_corruption_in_open && s.IsNotFound()));

  if (s.ok()) {
    // read CURRENT success
    Status manifest_status = env_->FileExists(*dscname);

    if (manifest_status.ok()) {
      // read CURRENT success and manifest exists
      // normal case
      return Status::OK();
    }

    if (manifest_status.IsTimeOut()) {
      return Status::TimeOut("MANIFSET check file exists timeout:", *dscname);
    }

    if (!manifest_status.IsNotFound()) {
      return Status::IOError("MANIFSET target manifest status is unknown",
                             manifest_status.ToString());
    }

    // lost manifest

    if (options_->ignore_corruption_in_open) {
      s = Status::Corruption("MANIFSET lost: ", *dscname);
    } else {
      return Status::Corruption("MANIFSET lost: ", *dscname);
    }
  } else {
    s = Status::Corruption("CURRENT lost", s.ToString());
  }

  // if program runs to here, there are only 2 possibilities
  // 1). lost CURRENT
  // 2). lost manifest
  assert(options_->ignore_corruption_in_open);

  // don't check manifest size because some dfs (eg. hdfs)
  // may return 0 even if the actual size is not 0
  // manifest is not ready, now recover the backup manifest
  std::vector<std::string> files;
  env_->GetChildren(pdbname, &files);
  std::set<std::string> manifest_set;
  for (size_t i = 0; i < files.size(); ++i) {
    uint64_t number;
    FileType type;
    if (ParseFileName(files[i], &number, &type)) {
      if (type == kDescriptorFile) {
        manifest_set.insert(files[i]);
      }
    }
  }
  if (manifest_set.size() < 1) {
    LEVELDB_LOG(options_->info_log, "[%s] none available manifest file", dbname_.c_str());
    ArchiveFile(env_, CurrentFileName(pdbname));
    return Status::Corruption("MANIFEST lost and haven't available manifest");
  }
  // select the largest manifest number
  std::set<std::string>::reverse_iterator it = manifest_set.rbegin();
  *dscname = pdbname + "/" + *it;
  LEVELDB_LOG(options_->info_log, "[%s] use backup manifest: %s", dbname_.c_str(),
              dscname->c_str());
  return Status::OK();
}

Status VersionSet::Recover() {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  // dscname.size==2 in tablet merging
  std::vector<std::string> dscname;
  Status s;
  size_t parent_size = options_->parent_tablets.size();
  std::string current;
  if (parent_size == 0) {
    LEVELDB_LOG(options_->info_log, "[%s] recover old or create new db.", dbname_.c_str());
    dscname.resize(1);
    s = ReadCurrentFile(0, &dscname[0]);
    if (!s.ok()) {
      LEVELDB_LOG(options_->info_log, "[%s] fail to read current.", dbname_.c_str());
      return s;
    }
  } else if (parent_size == 1) {
    LEVELDB_LOG(options_->info_log, "[%s] generated by splitting/merging, parent tablet: %llu",
                dbname_.c_str(), static_cast<unsigned long long>(options_->parent_tablets[0]));
    dscname.resize(1);
    s = ReadCurrentFile(options_->parent_tablets[0], &dscname[0]);
    if (!s.ok()) {
      LEVELDB_LOG(options_->info_log, "[%s] fail to read current (split/merge): %ld.",
                  dbname_.c_str(), options_->parent_tablets[0]);
      return s;
    }
  } else if (parent_size == 2) {
    LEVELDB_LOG(options_->info_log, "[%s] generated by merging, parent tablet: %llu, %llu",
                dbname_.c_str(), static_cast<unsigned long long>(options_->parent_tablets[0]),
                static_cast<unsigned long long>(options_->parent_tablets[1]));
    dscname.resize(2);
    // read first tablet CURRENT
    s = ReadCurrentFile(options_->parent_tablets[0], &dscname[0]);
    if (!s.ok()) {
      LEVELDB_LOG(options_->info_log, "[%s] fail to read current (merge0): %ld.", dbname_.c_str(),
                  options_->parent_tablets[0]);
      return s;
    }

    // read second tablet CURRENT
    s = ReadCurrentFile(options_->parent_tablets[1], &dscname[1]);
    if (!s.ok()) {
      LEVELDB_LOG(options_->info_log, "[%s] fail to read current (merge1): %ld.", dbname_.c_str(),
                  options_->parent_tablets[1]);
      return s;
    }
  } else {
    // never reached
    abort();
  }

  std::vector<SequentialFile*> files;
  files.resize(dscname.size());
  for (size_t i = 0; i < files.size(); ++i) {
    s = env_->NewSequentialFile(dscname[i], &files[i]);
    if (!s.ok()) {
      for (size_t j = 0; j < i; ++j) {
        delete files[j];
      }
      return s;
    }
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;

  for (size_t i = 0; i < files.size(); ++i) {
    VersionSetBuilder builder(this, current_);
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(files[i], &reporter, true /*checksum*/, 0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    int64_t record_num = 0;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      record_num++;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ && edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(edit.comparator_ + " does not match existing comparator ",
                                      icmp_.user_comparator()->Name());
          LEVELDB_LOG(options_->info_log, "[%s] %s\n", dbname_.c_str(), s.ToString().c_str());
        }
        if (files.size() > 1) {
          // clear compact_pointers if tablet is generated by merging.
          edit.compact_pointers_.clear();
        }
      } else {
        LEVELDB_LOG(options_->info_log, "[%s] Decode from manifest %s fail\n", dbname_.c_str(),
                    dscname[i].c_str());
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        if (log_number < edit.log_number_) {
          log_number = edit.log_number_;
        }
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        if (prev_log_number < edit.prev_log_number_) {
          prev_log_number = edit.prev_log_number_;
        }
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        if (next_file < edit.next_file_number_) {
          next_file = edit.next_file_number_;
        }
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        if (last_sequence < edit.last_sequence_) {
          last_sequence = edit.last_sequence_;
        }
        have_last_sequence = true;
      }
    }
    delete files[i];
    if (record_num == 0) {
      // empty manifest, delete it
      // ArchiveFile(env_, dscname[i]);
      return Status::Corruption("empty manifest:" + s.ToString());
    }
    if (s.ok()) {
      Version* v = new Version(this);
      builder.SaveTo(v);
      Finalize(v);
      AppendVersion(v);
      LEVELDB_LOG(options_->info_log, "[%s] recover manifest finish: %s\n", dbname_.c_str(),
                  dscname[i].c_str());
    } else {
      LEVELDB_LOG(options_->info_log, "[%s] recover manifest fail %s, %s\n", dbname_.c_str(),
                  dscname[i].c_str(), s.ToString().c_str());
      // ArchiveFile(env_, dscname[i]);
      return Status::Corruption("MANIFEST recover fail: " + s.ToString());
    }
  }

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("MANIFEST no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("MANIFEST no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("MANIFEST no last-sequence-number entry in descriptor");
      assert(0);
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    // Install recovered version
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;
  }

  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      FileMetaData* f = files[i];
      ModifyFileSize(f);
      // Debug
      LEVELDB_LOG(options_->info_log,
                  "[%s] recover: %s, level: %d, file_size %lu, data_size %lu, "
                  "del_p: %lu, check_ttl_ts %lu, ttl_p %lu, s: %d %s, l: %d %s\n",
                  dbname_.c_str(), FileNumberDebugString(f->number).c_str(), level, f->file_size,
                  f->data_size, f->del_percentage, f->check_ttl_ts, f->ttl_percentage,
                  f->smallest_fake, f->smallest.user_key().ToString().data(), f->largest_fake,
                  f->largest.user_key().ToString().data());
    }
  }

  LEVELDB_LOG(options_->info_log, "[%s] recover finish, key_start: %s, key_end: %s\n",
              dbname_.c_str(), db_key_start_.DebugString().data(),
              db_key_end_.DebugString().data());
  return s;
}

// Modify data_size of file meta
bool VersionSet::ModifyFileSize(FileMetaData* f) {
  if (f->data_size != 0) {
    return true;
  }
  // Try modify data_size in file meta
  // data_size = largest_key_offset - smallest_key_offset
  if (f->largest_fake || f->smallest_fake) {
    uint64_t s_offset = 0;
    uint64_t l_offset = f->file_size;
    Table* tableptr = NULL;
    Iterator* iter = table_cache_->NewIterator(ReadOptions(options_), dbname_, f->number,
                                               f->file_size, "", "", &tableptr);
    if (tableptr != NULL) {
      if (f->smallest_fake) {
        s_offset = tableptr->ApproximateOffsetOf(f->smallest.Encode());
      }
      if (f->largest_fake) {
        l_offset = tableptr->ApproximateOffsetOf(f->largest.Encode());
      }
    } else {
      LEVELDB_LOG(options_->info_log, "[%s] fail to reset file data_size: %s.\n", dbname_.c_str(),
                  FileNumberDebugString(f->number).c_str());
    }
    f->data_size = l_offset - s_offset;
    LEVELDB_LOG(options_->info_log, "[%s] reset file data_size: %s, from %llu to %llu\n, ",
                dbname_.c_str(), FileNumberDebugString(f->number).c_str(),
                static_cast<unsigned long long>(f->file_size),
                static_cast<unsigned long long>(f->data_size));
    delete iter;
  } else {  // for compatibility, we have not decoded f->data_size from MANIFEST
    f->data_size = f->file_size;
  }
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_del_level = -1;
  int best_del_idx = -1;
  int best_ttl_level = -1;
  int best_ttl_idx = -1;

  int base_level = -1;
  for (int level = config::kNumLevels - 1; level >= 0; level--) {
    double score = 0;
    if (level == 0 && level0_compactions_in_progress_.empty()) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      //
      // (3) More level0 files means write hotspot.
      // We give lower score to avoid too much level0 compaction.
      if (v->files_[level].size() <= (size_t)options_->slow_down_level0_score_limit) {
        score = v->files_[level].size() / static_cast<double>(config::kL0_CompactionTrigger);
      } else {
        score = sqrt(v->files_[level].size() / static_cast<double>(config::kL0_CompactionTrigger));
      }
    } else if (level > 0) {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSizeNotBeingCompacted(v->files_[level]);
      score = static_cast<double>(level_bytes) / MaxBytesForLevel(level, options_->sst_size);
    }

    // locate base level
    if (v->files_[level].size() > 0 && base_level < 0) {
      base_level = level;
    }

    if (level < config::kNumLevels - 1) {
      v->compaction_level_[level] = level;
      v->compaction_score_[level] = (score < 1.0) ? 0 : score;
    }

    for (size_t i = 0; i < v->files_[level].size(); i++) {
      FileMetaData* f = v->files_[level][i];
      // del compaction does not allow trigger by base level
      if ((!f->being_compacted) && (level > 0) && (level < base_level) &&
          (f->del_percentage > options_->del_percentage) &&
          (best_del_level < 0 ||
           v->files_[best_del_level][best_del_idx]->del_percentage < f->del_percentage)) {
        best_del_level = level;
        best_del_idx = i;
      }

      // ttl compaction can trigger in base level
      if ((!f->being_compacted) && (f->check_ttl_ts > 0) &&
          (best_ttl_level < 0 ||
           v->files_[best_ttl_level][best_ttl_idx]->check_ttl_ts > f->check_ttl_ts)) {
        best_ttl_level = level;
        best_ttl_idx = i;
      }
    }
  }

  // sort all the levels based on their score. Higher scores get listed
  // first. Use bubble sort because the number of entries are small.
  for (int i = 0; i < config::kNumLevels - 2; i++) {
    for (int j = i + 1; j < config::kNumLevels - 1; j++) {
      if (v->compaction_score_[i] < v->compaction_score_[j]) {
        int level = v->compaction_level_[i];
        double score = v->compaction_score_[i];
        v->compaction_level_[i] = v->compaction_level_[j];
        v->compaction_score_[i] = v->compaction_score_[j];
        v->compaction_level_[j] = level;
        v->compaction_score_[j] = score;
      }
    }
  }

  if (best_del_level >= 0) {
    v->del_trigger_compact_ = v->files_[best_del_level][best_del_idx];
    v->del_trigger_compact_level_ = best_del_level;
    LEVELDB_LOG(options_->info_log,
                "[%s] del_strategy(current), level %d, num #%lu, file_size "
                "%lu, del_p %lu\n",
                dbname_.c_str(), v->del_trigger_compact_level_,
                (v->del_trigger_compact_->number) & 0xffffffff, v->del_trigger_compact_->file_size,
                v->del_trigger_compact_->del_percentage);
  }

  if (best_ttl_level >= 0) {
    v->ttl_trigger_compact_ = v->files_[best_ttl_level][best_ttl_idx];
    v->ttl_trigger_compact_level_ = best_ttl_level;
    LEVELDB_LOG(options_->info_log,
                "[%s] ttl_strategy(current), level %d, num #%lu, file_size "
                "%lu, ttl_p %lu, check_ts %lu\n",
                dbname_.c_str(), v->ttl_trigger_compact_level_,
                (v->ttl_trigger_compact_->number) & 0xffffffff, v->ttl_trigger_compact_->file_size,
                v->ttl_trigger_compact_->ttl_percentage, v->ttl_trigger_compact_->check_ttl_ts);
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());
  edit.SetNextFile(next_file_number_);
  edit.SetLastSequence(last_sequence_);
  edit.SetLogNumber(log_number_);
  edit.SetPrevLogNumber(prev_log_number_);
  edit.SetStartKey(db_key_start_.user_key().ToString());
  edit.SetEndKey(db_key_end_.user_key().ToString());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      edit.AddFile(level, *files[i]);
    }
  }

  std::string record;
  edit.EncodeTo(&record);

  Status s = log->AddRecord(record);
  if (s.ok()) {
    descriptor_size_ = record.size();
  }
  return s;
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
           int(current_->files_[0].size()), int(current_->files_[1].size()),
           int(current_->files_[2].size()), int(current_->files_[3].size()),
           int(current_->files_[4].size()), int(current_->files_[5].size()),
           int(current_->files_[6].size()));
  return scratch->buffer;
}

// Return true iff the manifest contains the specified record.
bool VersionSet::ManifestContains(const std::string& record) const {
  std::string fname = DescriptorFileName(dbname_, manifest_file_number_);
  LEVELDB_LOG(options_->info_log, "[%s] ManifestContains: checking %s\n", dbname_.c_str(),
              fname.c_str());
  SequentialFile* file = NULL;
  Status s = env_->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    LEVELDB_LOG(options_->info_log, "[%s] ManifestContains: %s\n", dbname_.c_str(),
                s.ToString().c_str());
    return false;
  }
  log::Reader reader(file, NULL, true /*checksum*/, 0);
  Slice r;
  std::string scratch;
  bool result = false;
  while (reader.ReadRecord(&r, &scratch)) {
    if (r == Slice(record)) {
      result = true;
      break;
    }
  }
  delete file;
  LEVELDB_LOG(options_->info_log, "[%s] ManifestContains: result = %d\n", dbname_.c_str(),
              result ? 1 : 0);
  return result;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Slice smallest = files[i]->smallest_fake ? files[i]->smallest.Encode() : "";
        Slice largest = files[i]->largest_fake ? files[i]->largest.Encode() : "";
        Iterator* iter =
            table_cache_->NewIterator(ReadOptions(options_), dbname_, files[i]->number,
                                      files[i]->file_size, smallest, largest, &tableptr);
        if (tableptr != NULL) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_; v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

void VersionSet::AddLiveFiles(std::map<uint64_t, int>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_; v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        (*live)[files[i]->number] = level;
      }
    }
  }
}

void VersionSet::AddLiveFilesWithSize(std::map<uint64_t, uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_; v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        (*live)[files[i]->number] = files[i]->file_size;
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest, &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2, InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks || options_->verify_checksums_in_compaction;
  options.fill_cache = false;
  options.fill_persistent_cache = false;
  options.prefetch_scan = true;
  options.db_opt = options_;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator* [space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          Slice smallest = files[i]->smallest_fake ? files[i]->smallest.Encode() : "";
          Slice largest = files[i]->largest_fake ? files[i]->largest.Encode() : "";
          list[num++] = table_cache_->NewIterator(options, dbname_, files[i]->number,
                                                  files[i]->file_size, smallest, largest);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which], dbname_, options),
            GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

void VersionSet::PrintFilesInCompaction(const std::vector<FileMetaData*>& inputs) {
  char buf[30];
  std::string fstr = "file: ";
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (f->being_compacted) {
      snprintf(buf, sizeof(buf), "%lu ", f->number);
      fstr.append(buf);
      break;
    }
  }
  LEVELDB_LOG(options_->info_log, "[%s] test mark level [%s] bening compact.", dbname_.c_str(),
              fstr.c_str());
  return;
}

bool VersionSet::AreFilesInCompaction(const std::vector<FileMetaData*>& inputs) {
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (f->being_compacted) {
      return true;
    }
  }
  return false;
}

void VersionSet::PrintRangeInCompaction(const InternalKey* smallest, const InternalKey* largest,
                                        int level) {
  std::vector<FileMetaData*> inputs;
  assert(level < config::kNumLevels);
  current_->GetOverlappingInputs(level, smallest, largest, &inputs);
  PrintFilesInCompaction(inputs);
  return;
}

bool VersionSet::RangeInCompaction(const InternalKey* smallest, const InternalKey* largest,
                                   int level) {
  std::vector<FileMetaData*> inputs;
  assert(level < config::kNumLevels);
  current_->GetOverlappingInputs(level, smallest, largest, &inputs);
  return AreFilesInCompaction(inputs);
}

bool VersionSet::PickFutureCompaction(int level, std::vector<FileMetaData*>* inputs) {
  inputs->clear();
  std::vector<FileMetaData*> candidate;
  double low_level_score = 0;
  double high_level_score = 0;
  for (size_t li = 0; li < current_->compaction_score_.size(); li++) {
    if (current_->compaction_level_[li] == level) {
      low_level_score = current_->compaction_score_[li];
    } else if (current_->compaction_level_[li] == level + 1) {
      high_level_score = current_->compaction_score_[li];
    }
  }
  if (low_level_score < 1.0 || low_level_score <= high_level_score) {
    return false;
  }

  // file in level need compaction, pick file in next compaction
  for (size_t i = 0; i < current_->files_[level].size(); i++) {
    FileMetaData* f = current_->files_[level][i];
    if (f->being_compacted) {
      continue;
    }

    if (!compact_pointer_[level].empty() &&
        icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) <= 0) {
      candidate.push_back(f);
      continue;
    }

    inputs->push_back(f);
    break;
  }

  if (inputs->empty()) {
    FileMetaData* f = current_->files_[level][0];
    if (!f->being_compacted) {
      inputs->push_back(f);
    }
  }
  if (inputs->empty() && candidate.size() > 0) {
    inputs->push_back(candidate[candidate.size() - 1]);
  }
  return !inputs->empty();
}

bool VersionSet::IsOverlapInFileRange(FileMetaData* lf, FileMetaData* f) {
  if (lf == NULL || f == NULL) {
    return false;
  }
  if (icmp_.Compare(lf->largest.Encode(), f->smallest.Encode()) < 0 ||
      icmp_.Compare(f->largest.Encode(), lf->smallest.Encode()) < 0) {
    return false;
  }
  // LEVELDB_LOG(options_->info_log, "[%s] file range overlap, lfile #%d, [%s,
  // %s] being_compact %d, "
  //    "file #%d, [%s, %s] being_compact %d\n",
  //    dbname_.c_str(),
  //    static_cast<uint32_t>(lf->number & 0xffffffff),
  //    lf->smallest.Encode().ToString().c_str(),
  //    lf->largest.Encode().ToString().c_str(),
  //    lf->being_compacted,
  //    static_cast<uint32_t>(f->number & 0xffffffff),
  //    f->smallest.Encode().ToString().c_str(),
  //    f->largest.Encode().ToString().c_str(),
  //    f->being_compacted);
  return true;
}

// Note:
//  1) if f in level1 being compacted, level0 may be blocked;
//  2) compacting pointer may cause other f in the same level to be blocked.
bool VersionSet::PickCompactionBySize(int level, std::vector<FileMetaData*>* inputs) {
  // Pick low level file, which will be compact next time
  std::vector<FileMetaData*> low_level_inputs;
  PickFutureCompaction(level - 1, &low_level_inputs);
  FileMetaData* low_level_file = NULL;
  if (low_level_inputs.size() > 0) {
    low_level_file = low_level_inputs[0];
    // LEVELDB_LOG(options_->info_log, "[%s] PickCompactionBySize, low_level %d,
    // f[%s, %s] being_compact %d\n",
    //    dbname_.c_str(), level - 1,
    //    low_level_file->smallest.Encode().ToString().c_str(),
    //    low_level_file->largest.Encode().ToString().c_str(),
    //    low_level_file->being_compacted);
  }

  inputs->clear();
  std::vector<FileMetaData*> candidate;
  // Pick the first file that comes after compact_pointer_[level]
  for (size_t i = 0; i < current_->files_[level].size(); i++) {
    FileMetaData* f = current_->files_[level][i];
    if (f->being_compacted) {
      // LEVELDB_LOG(options_->info_log, "[%s] PickCompactionBySize, level %d,
      // f[%s, %s] being_compact %d\n",
      //    dbname_.c_str(), level,
      //    f->smallest.Encode().ToString().c_str(),
      //    f->largest.Encode().ToString().c_str(),
      //    f->being_compacted);
      continue;
    }
    if (!compact_pointer_[level].empty() &&
        icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) <= 0) {
      // LEVELDB_LOG(options_->info_log, "[%s] PickCompactionBySize, skip by
      // compact_pointer_[%d] %s, f[%s, %s] being_compacted %d\n",
      //    dbname_.c_str(), level, compact_pointer_[level].c_str(),
      //    f->smallest.Encode().ToString().c_str(),
      //    f->largest.Encode().ToString().c_str(),
      //    f->being_compacted);
      if (!RangeInCompaction(&f->smallest, &f->largest, level + 1) &&
          !IsOverlapInFileRange(low_level_file, f)) {
        candidate.push_back(f);
      }
      continue;
    }
    if (RangeInCompaction(&f->smallest, &f->largest, level + 1) ||
        IsOverlapInFileRange(low_level_file, f)) {
      // PrintRangeInCompaction(&f->smallest, &f->largest, level + 1);
      continue;
    }
    inputs->push_back(f);
    break;
  }
  if (inputs->empty()) {
    // Wrap-around to the beginning of the key space
    FileMetaData* f = current_->files_[level][0];
    if (!f->being_compacted && !RangeInCompaction(&f->smallest, &f->largest, level + 1) &&
        !IsOverlapInFileRange(low_level_file, f)) {
      inputs->push_back(f);
    }
    // LEVELDB_LOG(options_->info_log, "[%s] PickCompactBySize, wrap-arroud
    // level %d, f[%s, %s] being_compacted %d\n",
    //    dbname_.c_str(), level,
    //    f->smallest.Encode().ToString().c_str(),
    //    f->largest.Encode().ToString().c_str(),
    //    f->being_compacted);
    // PrintRangeInCompaction(&f->smallest, &f->largest, level + 1);
  }
  if (inputs->empty() && candidate.size() > 0) {
    inputs->push_back(candidate[candidate.size() - 1]);
  }
  return !inputs->empty();
}

void VersionSet::GetCompactionScores(std::vector<std::pair<double, uint64_t>>* scores) {
  current_->GetCompactionScores(scores);
}

Compaction* VersionSet::NewSubCompact(Compaction* compact) {
  Compaction* c = new Compaction(compact->level_);
  c->output_level_ = compact->output_level_;
  c->max_output_file_size_ = compact->max_output_file_size_;
  c->input_version_ = compact->input_version_;
  c->input_version_->Ref();  // make sure compacting version will not delete

  for (size_t i = 0; i < 2; i++) {
    for (size_t j = 0; j < compact->inputs_[i].size(); j++) {
      c->inputs_[i].push_back((compact->inputs_[i])[j]);
    }
  }

  for (size_t i = 0; i < compact->grandparents_.size(); i++) {
    c->grandparents_.push_back(compact->grandparents_[i]);
  }
  c->grandparent_index_ = compact->grandparent_index_;
  c->seen_key_ = compact->seen_key_;
  c->overlapped_bytes_ = compact->overlapped_bytes_;

  c->drop_lower_bound_ = compact->drop_lower_bound_;
  c->force_non_trivial_ = compact->force_non_trivial_;
  return c;
}

struct BoundaryComparator {
  BoundaryComparator(const Comparator* user_cmp) : ucmp(user_cmp) {}
  BoundaryComparator(const BoundaryComparator& key_cmp) = default;
  BoundaryComparator& operator=(const BoundaryComparator& key_cmp) = default;
  // retuen true if a < b
  bool operator()(const std::string& key_a, const std::string& key_b) {
    return ucmp->Compare(Slice(key_a), Slice(key_b)) < 0;
  }
  const Comparator* ucmp;
};

uint64_t VersionSet::GetApproximateSizeForBound(Compaction* compact, int input_index, int level,
                                                const InternalKey& ikey) {
  uint64_t result = 0;
  const std::vector<FileMetaData*>& files = compact->inputs_[input_index];
  for (size_t i = 0; i < files.size(); i++) {
    if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
      // Entire file is before "ikey", so just add the file size
      result += files[i]->file_size;
    } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
      // Entire file is after "ikey", so ignore
      if (level > 0) {
        // Files other than level 0 are sorted by meta->smallest, so
        // no further files in this level will contain data for
        // "ikey".
        break;
      }
    } else {
      // "ikey" falls in the range for this table.  Add the
      // approximate offset of "ikey" within the table.
      Table* tableptr;
      Slice smallest = files[i]->smallest_fake ? files[i]->smallest.Encode() : "";
      Slice largest = files[i]->largest_fake ? files[i]->largest.Encode() : "";
      Iterator* iter = table_cache_->NewIterator(ReadOptions(options_), dbname_, files[i]->number,
                                                 files[i]->file_size, smallest, largest, &tableptr);
      if (tableptr != NULL) {
        result += tableptr->ApproximateOffsetOf(ikey.Encode());
      }
      delete iter;
    }
  }
  return result;
}

void VersionSet::GenerateSubCompaction(Compaction* compact, std::vector<Compaction*>* compact_vec,
                                       port::Mutex* mu) {
  mu->AssertHeld();
  if (options_->max_sub_parallel_compaction <= 1) {
    Compaction* c = NewSubCompact(compact);
    compact_vec->push_back(c);
    return;
  }

  CompactStrategy* strategy = options_->compact_strategy_factory->NewInstance();
  const Comparator* ucmp = strategy->RowKeyComparator();
  if (ucmp == NULL) {
    ucmp = icmp_.user_comparator();
  }
  // generate candidate sub compaction split key
  std::set<std::string, BoundaryComparator> boundary(ucmp);
  std::string smallest_bound;
  std::string largest_bound;
  strategy->ExtractRowKey(compact->smallest_internal_key_.user_key(), &smallest_bound);
  strategy->ExtractRowKey(compact->largest_internal_key_.user_key(), &largest_bound);

  // boundary always seek keys in sst files, simply because
  // we should find complete row for split sub compaction.
  //
  // Important Remind : uncomplete row maybe lead to 'Tombstones' lost
  //                    at parallel sub compaction.
  // Bad Case Example :
  //   Sub No.1 Range: [r1:cf1:qu1, ..., r1:cfx:qu1, ..., r2:Tombstones, ...]
  //   Sub No.2 Range: [r2:cf1:qu1, ... ... ]
  //
  //   No.1 maybe del 'r2:Tombstones' and rubbish kv pairs after it, but at another
  //   thread No.2 can't discover these del opreators at No.1, and hold 'r2:cf1:qu1'
  for (int i = compact->level_; i < compact->output_level_; i++) {
    for (size_t j = 0; j < compact->inputs_[i - compact->level_].size(); j++) {
      FileMetaData* f = compact->inputs_[i - compact->level_][j];

      std::string file_smallest_row_key;
      std::string file_largest_row_key;
      strategy->ExtractRowKey(f->smallest.user_key(), &file_smallest_row_key);
      strategy->ExtractRowKey(f->largest.user_key(), &file_largest_row_key);
      boundary.insert(file_smallest_row_key);
      boundary.insert(file_largest_row_key);

      LEVELDB_LOG(options_->info_log,
                  "[%s] sub select : input level file , num #%lu, file_size %lu,"
                  " smallest:[%s -> %s], largest:[%s -> %s]\n",
                  dbname_.c_str(), (f->number) & 0xffffffff, f->file_size,
                  f->smallest.DebugString().c_str(), file_smallest_row_key.c_str(),
                  f->largest.DebugString().c_str(), file_largest_row_key.c_str());
    }
  }
  for (size_t j = 0; j < compact->inputs_[compact->output_level_ - compact->level_].size(); j++) {
    FileMetaData* f = compact->inputs_[compact->output_level_ - compact->level_][j];
    std::string file_smallest_row_key;
    std::string file_largest_row_key;
    strategy->ExtractRowKey(f->smallest.user_key(), &file_smallest_row_key);
    strategy->ExtractRowKey(f->largest.user_key(), &file_largest_row_key);
    boundary.insert(file_smallest_row_key);
    boundary.insert(file_largest_row_key);

    LEVELDB_LOG(options_->info_log,
                "[%s] sub select : output level file , num #%lu, file_size %lu,"
                " smallest:[%s -> %s], largest:[%s -> %s]\n",
                dbname_.c_str(), (f->number) & 0xffffffff, f->file_size,
                f->smallest.DebugString().c_str(), file_smallest_row_key.c_str(),
                f->largest.DebugString().c_str(), file_largest_row_key.c_str());
  }
  mu->Unlock();
  // generate sub compaction range by output file size
  uint64_t sum = 0, prev_sum = 0;
  std::set<std::string, BoundaryComparator>::iterator it = boundary.begin();
  while (it != boundary.end()) {
    sum = 0;
    const std::string& row_key = *it;
    // erase bound which smaller than smallest_bound or greater than largest_bound
    if (ucmp->Compare(Slice(row_key), Slice(smallest_bound)) <= 0 ||
        ucmp->Compare(Slice(row_key), Slice(largest_bound)) >= 0) {
      it = boundary.erase(it);
      continue;
    }

    InternalKey ikey(Slice(row_key), kMaxSequenceNumber, kValueTypeForSeek);
    LEVELDB_LOG(options_->info_log, "[%s] internalkey = %s\n", dbname_.c_str(),
                ikey.DebugString().c_str());
    for (int i = compact->level_; i <= compact->output_level_; i++) {
      sum += GetApproximateSizeForBound(compact, i - compact->level_, i, ikey);
    }
    LEVELDB_LOG(options_->info_log, "[%s] sum = %lu, prev_sum = %lu\n", dbname_.c_str(), sum,
                prev_sum);
    assert(sum >= prev_sum);
    // 0.9 sub compact size almost max_output_file_size_
    if (compact->max_output_file_size_ * 0.9 > sum - prev_sum) {
      it = boundary.erase(it);
    } else {
      ++it;
      prev_sum = sum;
    }
  }
  mu->Lock();

  // limit max sub compaction
  assert(options_->max_sub_parallel_compaction > 1);
  uint64_t avg_num = (boundary.size() + 1) / options_->max_sub_parallel_compaction + 1;
  it = boundary.begin();
  uint64_t i = 1;
  while (avg_num > 1 && it != boundary.end()) {
    if (i % avg_num != 0) {
      it = boundary.erase(it);
    } else {
      ++it;
    }
    i++;
  }

  // construct compaction
  if (boundary.size() == 0) {
    Compaction* c = NewSubCompact(compact);
    compact_vec->push_back(c);
  } else {
    std::set<std::string, BoundaryComparator>::iterator it = boundary.begin();
    std::string prev_key;
    while (true) {
      Compaction* c = NewSubCompact(compact);
      c->sub_compact_start_ = prev_key;
      InternalKey end_ikey(Slice(*it), kMaxSequenceNumber, kValueTypeForSeek);
      c->sub_compact_end_ = end_ikey.Encode().ToString();
      compact_vec->push_back(c);

      std::string start_ikey_str;
      if (prev_key == "") {
        start_ikey_str = "";
      } else {
        InternalKey start_ikey;
        start_ikey.DecodeFrom(Slice(c->sub_compact_start_));
        start_ikey_str = start_ikey.DebugString();
      }
      LEVELDB_LOG(options_->info_log, "[%s] sub select : range: [%s, %s]\n", dbname_.c_str(),
                  start_ikey_str.c_str(), end_ikey.DebugString().c_str());

      ++it;
      prev_key = c->sub_compact_end_;
      if (it == boundary.end()) {
        Compaction* c1 = NewSubCompact(compact);
        c1->sub_compact_start_ = prev_key;
        compact_vec->push_back(c1);

        InternalKey last_sub_start_ikey;
        last_sub_start_ikey.DecodeFrom(Slice(c1->sub_compact_start_));
        LEVELDB_LOG(options_->info_log, "[%s] sub select : range: [%s, ]\n", dbname_.c_str(),
                    last_sub_start_ikey.DebugString().c_str());
        break;
      }
    }
  }
  delete strategy;
}

void VersionSet::SetupSizeInitialFiles(int* input_level, std::vector<FileMetaData*>* inputs,
                                       bool* non_trivial) {
  assert(level0_compactions_in_progress_.size() <= 1);
  bool skipped_l0 = false;
  int level = -1;
  for (size_t li = 0; li < current_->compaction_score_.size(); li++) {
    double score = current_->compaction_score_[li];
    level = current_->compaction_level_[li];
    assert(li == 0 || score <= current_->compaction_score_[li - 1]);
    if (score >= 1) {
      assert(level >= 0 && level < config::kNumLevels - 1);
      if (skipped_l0 && level <= 1) {
        // level0 in progress and level 0 will not directly compact to level > 1
        continue;
      }
      if (level == 0 && !level0_compactions_in_progress_.empty()) {
        skipped_l0 = true;
        continue;
      }
      if (PickCompactionBySize(level, inputs)) {
        break;
      }
    }
  }
  *input_level = level;
}

void VersionSet::SetupSeekInitialFiles(int* input_level, std::vector<FileMetaData*>* inputs,
                                       bool* non_trivial) {
  int level = current_->file_to_compact_level_;
  assert(level >= 0 && level < config::kNumLevels - 1);
  FileMetaData* f = current_->file_to_compact_;
  if (!f->being_compacted && (level > 0 || level0_compactions_in_progress_.empty()) &&
      !RangeInCompaction(&f->smallest, &f->largest, level + 1)) {
    inputs->push_back(f);
    *input_level = level;
  }
}

void VersionSet::SetupDelInitialFiles(int* input_level, std::vector<FileMetaData*>* inputs,
                                      bool* non_trivial) {
  int level = current_->del_trigger_compact_level_;
  assert(level >= 0 && level < config::kNumLevels - 1);
  FileMetaData* f = current_->del_trigger_compact_;
  if (!f->being_compacted && (level > 0 || level0_compactions_in_progress_.empty()) &&
      !RangeInCompaction(&f->smallest, &f->largest, level + 1)) {
    inputs->push_back(f);
    *non_trivial = true;
    *input_level = level;
    LEVELDB_LOG(options_->info_log,
                "[%s] compact trigger by del stragety, level %d, num #%lu, "
                "file_size %lu, del_p %lu\n",
                dbname_.c_str(), current_->del_trigger_compact_level_,
                (current_->del_trigger_compact_->number) & 0xffffffff,
                current_->del_trigger_compact_->file_size,
                current_->del_trigger_compact_->del_percentage);
  }
}

void VersionSet::SetupTTLInitialFiles(int* input_level, std::vector<FileMetaData*>* inputs,
                                      bool* non_trivial) {
  int level = current_->ttl_trigger_compact_level_;
  assert(level >= 0 && level < config::kNumLevels);
  FileMetaData* f = current_->ttl_trigger_compact_;
  if (!f->being_compacted && (level > 0 || level0_compactions_in_progress_.empty()) &&
      (level + 1 == config::kNumLevels ||
       !RangeInCompaction(&f->smallest, &f->largest, level + 1))) {
    inputs->push_back(f);
    *non_trivial = true;
    *input_level = level;
    LEVELDB_LOG(options_->info_log,
                "[%s] compact trigger by ttl stragety, level %d, num #%lu, "
                "file_size %lu, ttl_p %lu, check_ts %lu\n",
                dbname_.c_str(), current_->ttl_trigger_compact_level_,
                (current_->ttl_trigger_compact_->number) & 0xffffffff,
                current_->ttl_trigger_compact_->file_size,
                current_->ttl_trigger_compact_->ttl_percentage,
                current_->ttl_trigger_compact_->check_ttl_ts);
  }
}

Compaction* VersionSet::PickCompaction() {
  int level = -1;
  std::vector<FileMetaData*> inputs;
  bool non_trivial = false;

  const bool size_compaction = (current_->compaction_score_[0] >= 1);
  if (size_compaction && inputs.empty()) {
    SetupSizeInitialFiles(&level, &inputs, &non_trivial);
  }
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if (seek_compaction && inputs.empty()) {
    SetupSeekInitialFiles(&level, &inputs, &non_trivial);
  }

  const bool del_compaction = (current_->del_trigger_compact_ != NULL);
  if (del_compaction && inputs.empty()) {
    SetupDelInitialFiles(&level, &inputs, &non_trivial);
  }

  const bool ttl_compaction = (current_->ttl_trigger_compact_ != NULL);
  if (ttl_compaction && inputs.empty()) {
    SetupTTLInitialFiles(&level, &inputs, &non_trivial);
  }

  if (inputs.empty()) {
    return NULL;
  }

  assert(level >= 0 && inputs.size() == 1);
  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    assert(level0_compactions_in_progress_.empty());
    InternalKey smallest, largest;
    GetRange(inputs, &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(level, &smallest, &largest, &inputs);
    GetRange(inputs, &smallest, &largest);
    if (RangeInCompaction(&smallest, &largest, level + 1)) {  // make sure level1 not in compaction
      LEVELDB_LOG(options_->info_log, "[%s] level1 in compacting, level0 conflict\n",
                  dbname_.c_str());
      return NULL;
    }
    assert(!inputs.empty());
    assert(!AreFilesInCompaction(inputs));
  }
  if (ExpandInputsToCleanCut(level, &inputs) == false) {
    return NULL;
  }
  int output_level = level < config::kNumLevels - 1 ? level + 1 : level;
  std::vector<FileMetaData*> output_level_inputs;
  if (!SetupOtherInputs(level, &inputs, output_level, &output_level_inputs)) {
    return NULL;
  }
  if (AreFilesInCompaction(inputs) || AreFilesInCompaction(output_level_inputs)) {
    return NULL;
  }
  // calc new range for final result
  InternalKey all_smallest, all_largest;
  GetRange2(inputs, output_level_inputs, &all_smallest, &all_largest);

  Compaction* c = new Compaction(level);
  c->SetNonTrivial(non_trivial);
  c->input_version_ = current_;
  c->input_version_->Ref();  // make sure compacting version will not delete
  c->set_output_level(output_level);
  c->max_output_file_size_ =
      MaxFileSizeForLevel(c->output_level(), current_->vset_->options_->sst_size);
  c->inputs_[0] = inputs;
  c->inputs_[1] = output_level_inputs;
  compact_pointer_[level] = all_largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, all_largest);
  c->smallest_internal_key_ = all_smallest;
  c->largest_internal_key_ = all_largest;

  if (level + 2 < config::kNumLevels) {
    SetupGrandparents(level, inputs, output_level_inputs, &c->grandparents_);
  }

  SetupCompactionBoundary(c);
  c->MarkBeingCompacted(true);
  if (level == 0) {
    level0_compactions_in_progress_.push_back(c);
  }
  Finalize(current_);  // reculate level score
  return c;
}

bool VersionSet::SetupOtherInputs(int level, std::vector<FileMetaData*>* level_inputs,
                                  int next_level, std::vector<FileMetaData*>* next_level_inputs) {
  assert(!level_inputs->empty());
  assert(next_level_inputs->empty());

  if (next_level == level) {
    return true;  // same level need not to find other inputs
  }

  InternalKey smallest, largest;

  // Step1: Expand next level
  GetRange(*level_inputs, &smallest, &largest);
  current_->GetOverlappingInputs(next_level, &smallest, &largest, next_level_inputs);
  if (AreFilesInCompaction(*next_level_inputs)) {
    return false;
  }
  if (!next_level_inputs->empty()) {
    if (!ExpandInputsToCleanCut(next_level, next_level_inputs)) {
      return false;
    }
  }
  // After expanded next level, we can get 4 cases, only case(3) or case(4) need
  // to try expand level. detail case parttens:
  // case(1.0) level      [        ]
  //           next_level               nothing founded
  //
  // case(1.1) level      [        ]
  //           next_level  [     ]      found smaller range at next level
  //
  // case(1.2) level      [        ]
  //           next_level [        ]    found same range at next level
  //
  // case(1.3) level      [        ]
  //           next_level     [       ] found part overlap range at next level
  //
  // case(1.4) level       [        ]
  //           next_level [          ]  found bigger range at next level

  // Step2: Try to expand level again
  if (!next_level_inputs->empty()) {
    const uint64_t next_level_inputs_size = TotalFileSize(*next_level_inputs);
    // case(2.1) level           [        ]
    //           next_level          [       ]
    //           all range       [           ]      found new start or limit
    //
    //
    // case(2.2) level       [        ]
    //           next_level [           ]
    //           all range  [           ]  nothing need to change
    InternalKey all_start, all_limit;
    GetRange2(*level_inputs, *next_level_inputs, &all_start, &all_limit);

    std::vector<FileMetaData*> level_expanded_inputs;

    bool try_expand_level_inputs = true;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &level_expanded_inputs);
    uint64_t level_expanded_inputs_size = 0;
    if (!ExpandInputsToCleanCut(level, &level_expanded_inputs)) {
      try_expand_level_inputs = false;
    } else {
      level_expanded_inputs_size = TotalFileSize(level_expanded_inputs);
    }

    bool expand_level = false;
    const uint64_t limit = ExpandedCompactionByteSizeLimit(options_->sst_size);
    if (try_expand_level_inputs  // expended level inputs all not being
                                 // compaction
        &&
        level_expanded_inputs.size() > level_inputs->size()  // expanded at level
        && next_level_inputs_size + level_expanded_inputs_size < limit &&
        !AreFilesInCompaction(level_expanded_inputs)) {
      InternalKey new_start, new_limit;
      GetRange(level_expanded_inputs, &new_start, &new_limit);
      std::vector<FileMetaData*> next_level_expanded_inputs;
      current_->GetOverlappingInputs(next_level, &new_start, &new_limit,
                                     &next_level_expanded_inputs);
      assert(!next_level_expanded_inputs.empty());
      if (!AreFilesInCompaction(next_level_expanded_inputs) &&
          ExpandInputsToCleanCut(next_level, &next_level_expanded_inputs) &&
          next_level_expanded_inputs.size() == next_level_inputs->size()) {
        expand_level = true;
      }
    }
    if (!expand_level) {
      current_->GetCleanCutInputsWithinInterval(level, &all_start, &all_limit,
                                                &level_expanded_inputs);
      level_expanded_inputs_size = TotalFileSize(level_expanded_inputs);
      if (level_expanded_inputs_size + next_level_inputs_size < limit &&
          level_expanded_inputs.size() > level_inputs->size() &&
          !AreFilesInCompaction(level_expanded_inputs)) {
        expand_level = true;
      }
    }
    if (expand_level) {
      // print debug info
      *level_inputs = level_expanded_inputs;
    }
  }
  return true;
}

void VersionSet::SetupCompactionBoundary(Compaction* c) {
  if (c->level_ == 0) {
    // compaction on level 0, do not drop delete mark
    return;
  }

  int base_level = config::kNumLevels - 1;
  while (base_level > 0) {
    if (current_->files_[base_level].size() != 0) {
      break;
    }
    base_level--;
  }
  if (base_level > c->output_level()) {
    // not base level
    return;
  }

  // consider case:
  // level:             sst: 100---------200  sst:200--------300
  // level + 1:         sst: 100------------------------250
  // if use 250 as lower_bound, then 200's del tag may miss
  // could not calculate input[1].large key.
  int input0_size = c->inputs_[0].size();
  FileMetaData* last_file = c->inputs_[0][input0_size - 1];
  c->set_drop_lower_bound(last_file->largest.user_key().ToString());
  return;
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin, const InternalKey* end,
                                     bool* manual_conflict) {
  *manual_conflict = false;
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return NULL;
  }

  // check level0 wether in compaction
  if (level == 0 && !level0_compactions_in_progress_.empty()) {
    *manual_conflict = true;
    return NULL;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(level, current_->vset_->options_->sst_size);
    uint64_t total = 0;
    for (size_t i = 0; i + 1 < inputs.size(); ++i) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  if (!ExpandInputsToCleanCut(level, &inputs)) {
    *manual_conflict = true;
    return NULL;
  }

  int output_level = level < config::kNumLevels - 1 ? level + 1 : level;
  std::vector<FileMetaData*> output_level_inputs;
  if (!SetupOtherInputs(level, &inputs, output_level, &output_level_inputs)) {
    *manual_conflict = true;
    return NULL;
  }
  if (AreFilesInCompaction(inputs) || AreFilesInCompaction(output_level_inputs)) {
    *manual_conflict = true;
    return NULL;
  }
  InternalKey all_smallest, all_largest;
  GetRange2(inputs, output_level_inputs, &all_smallest, &all_largest);

  std::vector<FileMetaData*> grandparents;

  Compaction* c = new Compaction(level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->max_output_file_size_ =
      MaxFileSizeForLevel(c->output_level(), current_->vset_->options_->sst_size);
  c->inputs_[0] = inputs;
  c->inputs_[1] = output_level_inputs;
  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = all_largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, all_largest);
  if (level + 2 < config::kNumLevels) {
    SetupGrandparents(level, inputs, output_level_inputs, &c->grandparents_);
  }

  c->smallest_internal_key_ = all_smallest;
  c->largest_internal_key_ = all_largest;

  SetupCompactionBoundary(c);
  c->MarkBeingCompacted(true);
  if (level == 0) {
    level0_compactions_in_progress_.push_back(c);
  }
  Finalize(current_);  // reculate level score
  return c;
}

void VersionSet::ReleaseCompaction(Compaction* c, Status& s) {
  c->MarkBeingCompacted(false);
  assert(level0_compactions_in_progress_.size() <= 1);
  if (c->level() == 0 && level0_compactions_in_progress_[0] == c) {
    level0_compactions_in_progress_.resize(0);
  }
  if (!s.ok()) {
    Finalize(current_);
  }
  return;
}

Compaction::Compaction(int level)
    : level_(level),
      output_level_(level + 1),
      max_output_file_size_(0),
      input_version_(NULL),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0),
      force_non_trivial_(false) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
}

void Compaction::SetNonTrivial(bool non_trivial) { force_non_trivial_ = non_trivial; }
bool Compaction::IsTrivialMove() const {
  if (force_non_trivial_) {
    return false;
  }
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          (TotalFileSize(grandparents_) <= MaxGrandParentOverlapBytes(max_output_file_size_)));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level_ + which, *inputs_[which][i]);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = output_level_ + 1; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size();) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &input_version_->vset_->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key, grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(max_output_file_size_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::MarkBeingCompacted(bool flag) {
  for (size_t i = 0; i < 2; i++) {
    for (size_t j = 0; j < inputs_[i].size(); j++) {
      assert(flag ? !inputs_[i][j]->being_compacted : inputs_[i][j]->being_compacted);
      inputs_[i][j]->being_compacted = flag;
    }
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}

}  // namespace leveldb
