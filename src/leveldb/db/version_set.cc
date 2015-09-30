// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <iostream>
#include <sstream>

#include <algorithm>
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
static int64_t MaxGrandParentOverlapBytes(int target_file_size) {
    return 10 * target_file_size;
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
#if 0
static int64_t ExpandedCompactionByteSizeLimit(int target_file_size) {
    return 25 * target_file_size;
}
#endif

static double MaxBytesForLevel(int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  double result = 40 * 1048576.0;  // Result for both level-0 and level-1
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(int level, int target_file_size) {
  if (level == 2) {
    return 2 * target_file_size;
  } else if(level > 2) {
    return 8 * target_file_size;
  }
  return target_file_size;  // We could vary per level to reduce number of files?
}

int64_t TotalNotBeingCompactedFileSize(const std::vector<FileMetaData*>& files) {
  int64_t bytes_no_compacting = 0;
  for (size_t i = 0; i < files.size(); i++) {
    if (!files[i]->being_compacted) {
       bytes_no_compacting += files[i]->file_size;
    }
  }
  return bytes_no_compacting;
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
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

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
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

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs before all keys and is therefore never after *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs after all keys and is therefore never before *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
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
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
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
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist,
                       const std::string& dbname)
      : icmp_(icmp),
        flist_(flist),
        dbname_(dbname),
        index_(flist->size()) {        // Marks as invalid
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
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
    EncodeFixed64((char*)value_buf_.data()+8, f->file_size);
    Slice smallest = f->smallest_fake ? f->smallest.Encode() : "";
    Slice largest = f->largest_fake ? f->largest.Encode() : "";
    EncodeFixed32((char*)value_buf_.data()+16, smallest.size());
    EncodeFixed32((char*)value_buf_.data()+20, largest.size());
    EncodeFixed32((char*)value_buf_.data()+24, dbname_.size());
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

  // Backing store for value().  Holds the file number and size.
  mutable std::string value_buf_;
};

static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) {
  assert(options.db_opt);
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  int32_t ssize = DecodeFixed32(file_value.data() + 16);
  int32_t lsize = DecodeFixed32(file_value.data() + 20);
  int32_t dbname_size = DecodeFixed32(file_value.data() + 24);
  assert(ssize >= 0 && ssize < 65536 &&
         lsize >= 0 && lsize < 65536 &&
         dbname_size > 0 && dbname_size < 1024);
  return cache->NewIterator(options,
                            std::string(file_value.data() + 28 + ssize + lsize,
                                        dbname_size),
                            DecodeFixed64(file_value.data()),
                            DecodeFixed64(file_value.data() + 8),
                            Slice(file_value.data() + 28, ssize),
                            Slice(file_value.data() + 28 + ssize, lsize));
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  ReadOptions opts = options;
  opts.db_opt = vset_->options_;
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level], vset_->dbname_),
      &GetFileIterator, vset_->table_cache_, opts);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  ReadOptions opts = options;
  opts.db_opt = vset_->options_;
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    Slice smallest = f->smallest_fake ? f->smallest.Encode() : "";
    Slice largest = f->largest_fake ? f->largest.Encode() : "";
    iters->push_back(vset_->table_cache_->NewIterator(
            opts, vset_->dbname_ , f->number,
            f->file_size, smallest, largest));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
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
          s->state = kDeleted; // stop searching in other files.
        }
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
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
      saver.compact_strategy = vset_->options_->enable_strategy_when_get ?
              vset_->options_->compact_strategy_factory->NewInstance() : NULL;
      s = vset_->table_cache_->Get(opts, vset_->dbname_, f->number,
                                   f->file_size, ikey, &saver, SaveValue);
      delete saver.compact_strategy;
      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          break;      // Keep searching in other files
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

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
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
void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs) {
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != NULL) {
    user_begin = begin->user_key();
  }
  if (end != NULL) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size(); ) {
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
}

// tera-specific
uint64_t Version::GetScopeSize(const Slice* smallest_user_key,
                               const Slice* largest_user_key) {
    // std::cerr << "start: [" << (smallest_user_key != NULL ? smallest_user_key->ToString() : "")
    //    << "], end: [" << (largest_user_key != NULL ? largest_user_key->ToString() : "") << "]" << std::endl;
    uint64_t total_size = 0;
    const Comparator* user_cmp = vset_->icmp_.user_comparator();
    for (int level = 1; level < config::kNumLevels; level++) {
        const std::vector<FileMetaData*>& files = files_[level];
        for (size_t i = 0; i < files.size(); i++) {
            FileMetaData* file = files[i];
            // std::cerr << "level: " << level << ", id: " << i
            //     << ", num: " << file->number
            //     << ", small: [" << file->smallest.user_key().ToString()
            //     << "], largest: [" << file->largest.user_key().ToString()
            //     << "], size: " << file->file_size << std::endl;
            if (BeforeFile(user_cmp, largest_user_key, file)
                || AfterFile(user_cmp, smallest_user_key, file)) {
                continue;
            }
            total_size += files[i]->file_size;
        }
    }

    return total_size;
}

bool Version::FindSplitKey(const Slice* smallest_user_key,
                           const Slice* largest_user_key,
                           double ratio,
                           std::string* split_key) {
    assert(ratio >= 0 && ratio <= 1);
    uint64_t total_size = GetScopeSize(smallest_user_key,
                                       largest_user_key);
    uint64_t want_split_size = static_cast<uint64_t>(total_size * ratio);

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
                user_cmp->Compare(largest_file->largest.user_key(),
                                  file->largest.user_key()) > 0) {
                largest_file = file;
                step_level = level;
            }
        }
        // when leveldb is empty
        if (largest_file == NULL) {
            return false;
        }
        split_size += files_[step_level][now_pos[step_level]]->file_size;
        now_pos[step_level] ++;
    }
    *split_key = largest_file->largest.user_key().ToString();
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
        fi++;
      }
    }
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  VersionSetBuilder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
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
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
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
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    const VersionEdit::FileMetaSet& del = edit->deleted_files_;
    for (VersionEdit::FileMetaSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      FileMetaData f = iter->second;
      ModifyFileMetaKeyRange(&f);
      levels_[level].deleted_files.push_back(f);
    }

    // Add new files
    VersionEdit::FileMetaSet::iterator it = edit->new_files_.begin();
    while (it != edit->new_files_.end()) {
      const int level = it->first;
      FileMetaData& f_new = it->second;
      if (!ModifyFileMetaKeyRange(&f_new)) {
        // File is out-of-range, erase from edit
        it = edit->new_files_.erase(it);
        continue;
      }
      it++;

      FileMetaData* f = new FileMetaData(f_new);
      f->refs = 1;

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
      uint64_t size_for_one_seek = 16384ULL  * vset_->options_->seek_latency / 10000000;
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
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
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
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) > 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(),
                    this_begin.DebugString().c_str());
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
      FileMetaData* f_old = (*files)[files->size()-1];
      // Must not overlap
      assert(vset_->icmp_.Compare(f_old->largest, f->smallest) <= 0);
    }
    f->refs++;
    files->push_back(f);
  }
  // modify key range of file meta
  // return false if file out of tablet range
  bool ModifyFileMetaKeyRange(FileMetaData* f) {
    if (!vset_->db_key_start_.user_key().empty() &&
        vset_->icmp_.Compare(f->smallest, vset_->db_key_start_) < 0) {
      if (vset_->icmp_.Compare(f->largest, vset_->db_key_start_) > 0) {
        Log(vset_->options_->info_log,
            "[%s] reset file smallest key: %s, from %s to %s\n",
            vset_->dbname_.c_str(),
            TableFileName(vset_->dbname_, f->number).c_str(),
            f->smallest.DebugString().c_str(),
            vset_->db_key_start_.DebugString().c_str());
        f->smallest = vset_->db_key_start_;
        f->smallest_fake = true;
      } else {
        // file out of tablet range, skip it;
        return false;
      }
    }
    if (!vset_->db_key_end_.user_key().empty() &&
        vset_->icmp_.Compare(f->largest, vset_->db_key_end_) > 0) {
      if (vset_->icmp_.Compare(f->smallest, vset_->db_key_end_) < 0) {
        Log(vset_->options_->info_log,
            "[%s] reset file largest key: %s, from %s to %s\n",
            vset_->dbname_.c_str(),
            TableFileName(vset_->dbname_, f->number).c_str(),
            f->largest.DebugString().c_str(),
            vset_->db_key_end_.DebugString().c_str());
        f->largest = vset_->db_key_end_;
        f->largest_fake = true;
      } else {
        // file out of tablet range, skip it;
        return false;
      }
    }
    return true;
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
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
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL),
      level0_being_compacted_(false) {
  last_switch_manifest_ = env_->NowMicros();
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  // Debug
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      Log(options_->info_log, "[%s] finish : %08u %08u, level: %d, s: %d %s, l: %d %s\n",
          dbname_.c_str(),
          static_cast<uint32_t>(files[i]->number >> 32 & 0x7fffffff),
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

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  // locked 
  ManifestWriter w(mu);
  manifest_writers_.push_back(&w);
  while (!w.done && &w != manifest_writers_.front()) {
    w.cv.Wait();
  }
  //Log(options_->info_log, "wait pass");
  if (w.done) {
    Log(options_->info_log, "already done");
    return w.status;
  }  

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
    Log(options_->info_log, "[%s] LogLastSequence %lu",
        dbname_.c_str(), edit->GetLastSequence());
  }

  if (edit->HasLastSequence()) {
    assert(edit->GetLastSequence() >= last_sequence_);
  } else {
    edit->SetLastSequence(last_sequence_);
  }

  Version* v = new Version(this);
  {
    VersionSetBuilder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v);

  const uint64_t switch_interval = options_->manifest_switch_interval * 1000000UL;
  if (descriptor_log_ != NULL &&
      last_switch_manifest_ + switch_interval < env_->NowMicros()) {
    force_switch_manifest_ = true;
  }
  if (force_switch_manifest_) {
    delete descriptor_log_;
    delete descriptor_file_;
    descriptor_log_ = NULL;
    descriptor_file_ = NULL;
    manifest_file_number_ = NewFileNumber();
    Log(options_->info_log, "[%s] force switch MANIFEST to %lu",
        dbname_.c_str(), manifest_file_number_);
    force_switch_manifest_ = false;
    last_switch_manifest_ = env_->NowMicros();
  }
  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == NULL) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "[%s] MANIFEST write: %s\n",
            dbname_.c_str(), s.ToString().c_str());
        if (ManifestContains(record)) {
          Log(options_->info_log,
              "[%s] MANIFEST contains log record despite error; advancing to new "
              "version to prevent mismatch between in-memory and logged state",
              dbname_.c_str());
          s = Status::OK();
        }
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
      if (s.ok()) {
          Log(options_->info_log, "[%s] set CURRENT to %llu\n",
              dbname_.c_str(), static_cast<unsigned long long>(manifest_file_number_));
      }
      // No need to double-check MANIFEST in case of error since it
      // will be discarded below.
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
    last_sequence_ = edit->GetLastSequence();
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
    force_switch_manifest_ = true;
    Log(options_->info_log, "[%s] set force_switch_manifest", dbname_.c_str());
  }

  while (true) {
    ManifestWriter* ready = manifest_writers_.front();
    manifest_writers_.pop_front();
    if (ready != &w) {
      ready->status = s;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == &w) break;
  }

  // Notify new head of write queue
  if (!manifest_writers_.empty()) {
    manifest_writers_.front()->cv.Signal();
  }
  return s;
}

Status VersionSet::ReadCurrentFile(uint64_t tablet, std::string* dscname ) {
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
    Log(options_->info_log, "[%s] current file lost: %s.",
        dbname_.c_str(), CurrentFileName(pdbname).c_str());
  } else if (current.empty() || current[current.size()-1] != '\n') {
    Log(options_->info_log, "[%s] current file error: %s, content:\"%s\", size:%lu.",
        dbname_.c_str(), CurrentFileName(pdbname).c_str(),
        current.c_str(), current.size());
    ArchiveFile(env_, CurrentFileName(pdbname));
  } else {
    current.resize(current.size() - 1);
    *dscname = pdbname + "/" + current;
  }

  // don't check manifest size because some dfs (eg. hdfs)
  // may return 0 even if the actual size is not 0
  if (!s.ok() || !env_->FileExists(*dscname)) {
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
      Log(options_->info_log, "[%s] none available manifest file.",
          dbname_.c_str());
      return Status::Corruption("DB has none available manifest file.");
    }
    // select the largest manifest number
    std::set<std::string>::reverse_iterator it = manifest_set.rbegin();
    *dscname = pdbname + "/" + *it;
    Log(options_->info_log, "[%s] use backup manifest: %s.",
        dbname_.c_str(), dscname->c_str());
    return Status::OK();
  }
  return s;
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
  if (parent_size  == 0) {
    Log(options_->info_log, "[%s] recover old or create new db.", dbname_.c_str());
    dscname.resize(1);
    s = ReadCurrentFile(0, &dscname[0]);
    if (!s.ok()) {
      Log(options_->info_log, "[%s] fail to read current.", dbname_.c_str());
      return s;
    }
  } else if (parent_size == 1) {
    Log(options_->info_log, "[%s] generated by splitting, parent tablet: %llu",
        dbname_.c_str(), static_cast<unsigned long long>(options_->parent_tablets[0]));
    dscname.resize(1);
    s = ReadCurrentFile(options_->parent_tablets[0], &dscname[0]);
    if (!s.ok()) {
      Log(options_->info_log, "[%s] fail to read current (split): %ld.",
          dbname_.c_str(), options_->parent_tablets[0]);
      return s;
    }
  } else if (parent_size == 2) {
    Log(options_->info_log, "[%s] generated by merging, parent tablet: %llu, %llu",
        dbname_.c_str(), static_cast<unsigned long long>(options_->parent_tablets[0]),
        static_cast<unsigned long long>(options_->parent_tablets[1]));
    dscname.resize(2);
    // read first tablet CURRENT
    s = ReadCurrentFile(options_->parent_tablets[0], &dscname[0]);
    if (!s.ok()) {
      Log(options_->info_log, "[%s] fail to read current (merge0): %ld.",
          dbname_.c_str(), options_->parent_tablets[0]);
      return s;
    }

    // read second tablet CURRENT
    s = ReadCurrentFile(options_->parent_tablets[1], &dscname[1]);
    if (!s.ok()) {
      Log(options_->info_log, "[%s] fail to read current (merge1): %ld.",
          dbname_.c_str(), options_->parent_tablets[1]);
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
    log::Reader reader(files[i], &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
          Log(options_->info_log, "[%s] %s\n", dbname_.c_str(), s.ToString().c_str());
        }
        if (files.size() > 1) {
          // clear compact_pointers if tablet is generated by merging.
          edit.compact_pointers_.clear();
        }
      } else {
        Log(options_->info_log, "[%s] Decode from manifest %s fail\n",
            dbname_.c_str(), dscname[i].c_str());
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
    if (s.ok()) {
      Version* v = new Version(this);
      builder.SaveTo(v);
      Finalize(v);
      AppendVersion(v);
      Log(options_->info_log, "[%s] recover manifest finish: %s\n",
          dbname_.c_str(), dscname[i].c_str());
    } else {
      Log(options_->info_log, "[%s] recover manifest fail %s, %s\n",
          dbname_.c_str(), dscname[i].c_str(), s.ToString().c_str());
      ArchiveFile(env_, dscname[i]);
      return Status::Corruption("recover manifest fail");
    }
  }

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
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

  Log(options_->info_log, "[%s] recover finish, key_start: %s, key_end: %s\n",
      dbname_.c_str(), db_key_start_.DebugString().data(), db_key_end_.DebugString().data());
  // Debug
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      Log(options_->info_log, "[%s] recover: %08u %08u, level: %d, s: %d %s, l: %d %s\n",
          dbname_.c_str(),
          static_cast<uint32_t>(files[i]->number >> 32 & 0x7fffffff),
          static_cast<uint32_t>(files[i]->number & 0xffffffff), level,
          files[i]->smallest_fake, files[i]->smallest.user_key().ToString().data(),
          files[i]->largest_fake, files[i]->largest.user_key().ToString().data());
    }
  }
  return s;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels-1; level++) {
    double score;
    if (level == 0) {
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
      score = v->files_[level].size() /
          static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
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
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer),
           "files[ %d %d %d %d %d %d %d ]",
           int(current_->files_[0].size()),
           int(current_->files_[1].size()),
           int(current_->files_[2].size()),
           int(current_->files_[3].size()),
           int(current_->files_[4].size()),
           int(current_->files_[5].size()),
           int(current_->files_[6].size()));
  return scratch->buffer;
}

// Return true iff the manifest contains the specified record.
bool VersionSet::ManifestContains(const std::string& record) const {
  std::string fname = DescriptorFileName(dbname_, manifest_file_number_);
  Log(options_->info_log, "[%s] ManifestContains: checking %s\n", dbname_.c_str(), fname.c_str());
  SequentialFile* file = NULL;
  Status s = env_->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    Log(options_->info_log, "[%s] ManifestContains: %s\n", dbname_.c_str(), s.ToString().c_str());
    return false;
  }
  log::Reader reader(file, NULL, true/*checksum*/, 0);
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
  Log(options_->info_log, "[%s] ManifestContains: result = %d\n",
    dbname_.c_str(), result ? 1 : 0);
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
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(options_), dbname_, files[i]->number, files[i]->file_size,
            smallest, largest, &tableptr);
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
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

void VersionSet::AddLiveFiles(std::map<uint64_t, int>* live) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        (*live)[files[i]->number] = level;
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
      current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     &overlaps);
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
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
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
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums =
      options_->paranoid_checks || options_->verify_checksums_in_compaction;
  options.fill_cache = false;
  options.db_opt = options_;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          Slice smallest = files[i]->smallest_fake ? files[i]->smallest.Encode() : "";
          Slice largest = files[i]->largest_fake ? files[i]->largest.Encode() : "";
          list[num++] = table_cache_->NewIterator(
              options, dbname_, files[i]->number, files[i]->file_size, smallest, largest);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which], dbname_),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickMultiThreadCompaction() {
  Compaction* c = NULL;
  int level;
  bool found_compaction = false;
  double score;
  std::multimap<double, int> amap; // (socre : level)
  ScoreMatrix(amap);
  std::multimap<double, int>::reverse_iterator rit;
  for (rit = amap.rbegin(); rit != amap.rend(); ++rit) {
    score = rit->first;
    level = rit->second;
    Log(options_->info_log, "scan %zd@%d, score:%lf", 
        current_->files_[level].size(), level, score);
    if (score < 1.0) {
      break;
    }
    c = new Compaction(level);
    if (PickCompactionOnLevel(c, level)) {
      // there are files on level & level+1 need to compacted
      found_compaction = true;
      break;
    }
    delete c;
  }

  if (!found_compaction) {
    return NULL;
  }
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->max_output_file_size_ =
      MaxFileSizeForLevel(level + 1, current_->vset_->options_->sst_size);

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    assert(level0_being_compacted_ == false);
    assert(!c->inputs_[0].empty());

    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());

    GetRange(c->inputs_[0], &smallest, &largest);
    if (RangeInCompaction(&smallest, &largest, level)) {
      Log(options_->info_log, "level-1 in compaction");
      return NULL;
    }
    level0_being_compacted_ = true;
  }

  SetupOtherInputs(c);
  std::ostringstream oss;
  for (size_t i = 0; i < c->inputs_[0].size(); i++) {
    oss << static_cast<uint32_t>(c->inputs_[0][i]->number & 0xffffffff) << " ";
  }
  oss << ", ";
  for (size_t i = 0; i < c->inputs_[1].size(); i++) {
    oss << static_cast<uint32_t>(c->inputs_[1][i]->number & 0xffffffff) << " ";
  }
  Log(options_->info_log, "pick %zd@%d + %zd@%d : %s", 
      c->inputs_[0].size(), c->level(),
      c->inputs_[1].size(), c->level()+1, oss.str().c_str());
  c->SetInputsFilesBeingCompacted(true);
  return c;
}

// return true if any one of a file in `files' is being compacted
bool VersionSet::FilesInCompaction(const std::vector<FileMetaData*>& files) {
    for (unsigned int i = 0; i < files.size(); i++) {
        if (files[i]->being_compacted) {
        //    Log(options_->info_log, "[pick] file in compact #%d", 
        //        static_cast<uint32_t>(files[i]->number & 0xffffffff));
            return true;
        }
    }
    return false;
}

// return true if range[smallest, largest] at `level' is being compacted
bool VersionSet::RangeInCompaction(const InternalKey* smallest, 
                                   const InternalKey* largest, 
                                   int level) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, smallest, largest, &inputs);
  return FilesInCompaction(inputs);
}

bool VersionSet::PickCompactionOnLevel(Compaction* c, int level) {
    // TODO(taocipian) concurrently compact level0 range
    if ((level == 0) && (level0_being_compacted_ == true)) {
      Log(options_->info_log, "level-0 being-compacted");
      return false;
    }
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (f->being_compacted) {
        // itself is being compacted
        continue;
      }
      if (RangeInCompaction(&f->smallest, &f->largest, level+1)) {
        // it's parent files are being compacted
        continue;
      }
      c->inputs_[0].push_back(f);
      break;
    }
    if (c->inputs_[0].empty()) {
      return false;
    }
    return true;
}

// calculate socre of every level, store result in `amap'
void VersionSet::ScoreMatrix(std::multimap<double, int>& amap) {
  int level;
  double score;
  for (level = 0; level < config::kNumLevels-1; level++) {
    if (level == 0) {
      int num = 0;
      for (size_t i = 0; i < current_->files_[level].size(); i++) {
        if (!current_->files_[level][i]->being_compacted) {
          num++;
        }
      }
      Log(options_->info_log, "level-0 all:%zd, not-compacted:%d", 
          current_->files_[level].size(), num);
      score = num / static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      const uint64_t level_bytes = TotalNotBeingCompactedFileSize(current_->files_[level]);
      score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
    }
    amap.insert(std::pair<double, int>(score, level));
  }
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    c = new Compaction(level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return NULL;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();
  c->max_output_file_size_ =
      MaxFileSizeForLevel(level + 1, current_->vset_->options_->sst_size);

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
#if 0 // disabled for multi-thread compaction: maybe cause level+1 conflicts
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
        ExpandedCompactionByteSizeLimit(options_->sst_size)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "[%s] Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            dbname_.c_str(),
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size),
            int(expanded0.size()),
            int(expanded1.size()),
            long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }
#endif

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  if (false) {
    Log(options_->info_log, "[%s] Compacting %d '%s' .. '%s'",
        dbname_.c_str(),
        level,
        smallest.DebugString().c_str(),
        largest.DebugString().c_str());
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);

  // tera-specific: calculate the smallest rowkey which overlap with file not
  // in this compaction.
  SetupCompactionBoundary(c);
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
  if (base_level > c->level_ + 1) {
    // not base level
    return;
  }

  // do not need calculate input[1].
  int input0_size = c->inputs_[0].size();
  FileMetaData* last_file = c->inputs_[0][input0_size - 1];
  c->set_drop_lower_bound(last_file->largest.user_key().ToString());
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
      return NULL;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit =
      MaxFileSizeForLevel(level, current_->vset_->options_->sst_size);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->max_output_file_size_ =
    MaxFileSizeForLevel(level + 1, current_->vset_->options_->sst_size);
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);

  c->SetInputsFilesBeingCompacted(true);
  level0_being_compacted_ = (level == 0 ? true : false);

  return c;
}

Compaction::Compaction(int level)
    : level_(level),
      max_output_file_size_(0),
      input_version_(NULL),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
}

void Compaction::SetInputsFilesBeingCompacted(bool new_mark) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      bool old_mark = inputs_[which][i]->being_compacted;
      if (old_mark == new_mark) {
        uint32_t file_no = static_cast<uint32_t>(inputs_[which][i]->number & 0xffffffff);
        fprintf(stderr, "file_no:#%08u being_compacted status:%d", file_no, new_mark);
        abort();
      }
      inputs_[which][i]->being_compacted = new_mark;
    }
  }
}

bool Compaction::IsTrivialMove() const {
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 &&
          num_input_files(1) == 0 &&
          (TotalFileSize(grandparents_) <=
          MaxGrandParentOverlapBytes(max_output_file_size_)));
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
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) {
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
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
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

void Compaction::ReleaseInputs() {
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}

}  // namespace leveldb
