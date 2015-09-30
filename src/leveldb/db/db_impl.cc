// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <iostream>

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_on_leveldb.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/compact_strategy.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "leveldb/table_utils.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& options, const std::string& dbname)
    : state_(kNotOpen), key_start_(options.key_start), key_end_(options.key_end),
      env_(options.env),
      internal_comparator_(options.comparator),
      internal_filter_policy_(options.filter_policy),
      options_(SanitizeOptions(
          dbname, &internal_comparator_, &internal_filter_policy_, options)),
      owns_info_log_(options_.info_log != options.info_log),
      owns_block_cache_(options_.block_cache != options.block_cache),
      dbname_(dbname),
      table_cache_(options_.table_cache),
      owns_table_cache_(options_.table_cache == NULL),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      writting_mem_cv_(&mutex_),
      is_writting_mem_(false),
      mem_(NewMemTable()),
      imm_(NULL), recover_mem_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      bound_log_size_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      bg_compaction_scheduled_count_(0),
      bg_compaction_score_(0),
      bg_schedule_id_(0),
      imm_dump_(false),
      unscheduled_compactions_(0),
      max_background_compactions_(10), // TODO(taocipian) configurable
      manual_compaction_(NULL),
      consecutive_compaction_errors_(0),
      flush_on_destroy_(false) {
  mem_->Ref();
  has_imm_.Release_Store(NULL);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  if (owns_table_cache_) {
    Log(options_.info_log, "[%s] create new table cache.", dbname_.c_str());
    const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
    table_cache_ = new TableCache(table_cache_size);
  }
  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);
}

Status DBImpl::Shutdown1() {
  assert(state_ == kOpened);
  state_ = kShutdown1;

  MutexLock l(&mutex_);
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok

  Log(options_.info_log, "[%s] wait bg compact finish", dbname_.c_str());
  ReScheduleAllCompactTask(kDumpMemTableUrgentScore);
  while (bg_compaction_scheduled_count_ > 0) {
    bg_cv_.Wait();
  }

  Status s;
  if (!options_.dump_mem_on_shutdown) {
    return s;
  }
  Log(options_.info_log, "[%s] fg compact mem table", dbname_.c_str());
  // there is no compact, so dump memtable is safe
  if (imm_ != NULL) {
    s = CompactMemTable();
  }
  if (s.ok()) {
    assert(imm_ == NULL);
    while (is_writting_mem_) {
        writting_mem_cv_.Wait();
    }
    imm_ = mem_;
    has_imm_.Release_Store(imm_);
    mem_ = NewMemTable();
    mem_->Ref();
    bound_log_size_ = 0;
    s = CompactMemTable();
  }
  return s;
}

Status DBImpl::Shutdown2() {
  assert(state_ == kShutdown1);
  state_ = kShutdown2;

  MutexLock l(&mutex_);
  Status s;
  if (!options_.dump_mem_on_shutdown) {
    return s;
  }
  Log(options_.info_log, "[%s] fg compact mem table", dbname_.c_str());
  assert(imm_ == NULL);
  imm_ = mem_;
  has_imm_.Release_Store(imm_);
  mem_ = NULL;
  bound_log_size_ = 0;
  return CompactMemTable();
}

DBImpl::~DBImpl() {
  if (state_ == kOpened) {
    Status s = Shutdown1();
    if (s.ok()) {
        Shutdown2();
    }
  }
  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  if (recover_mem_ != NULL) recover_mem_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  if (owns_table_cache_) {
    delete table_cache_;
  }
  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_block_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "[%s] Ignoring error %s",
        dbname_.c_str(), s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  // manifest file set, keep latest 3 manifest files for backup
  std::set<std::string> manifest_set;

  Log(options_.info_log, "[%s] try DeleteObsoleteFiles, total live file num: %llu\n",
      dbname_.c_str(), static_cast<unsigned long long>(live.size()));

  std::vector<std::string> filenames;
  mutex_.Unlock();
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  mutex_.Lock();
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          manifest_set.insert(filenames[i]);
          if (manifest_set.size() > 3) {
              std::set<std::string>::iterator it = manifest_set.begin();
              ParseFileName(*it, &number, &type);
              if (number < versions_->ManifestFileNumber()) {
                // Keep my manifest file, and any newer incarnations'
                // (in case there is a race that allows other incarnations)
                filenames[i] = *it;
                keep = false;
              }
          }
          break;
        case kTableFile:
          keep = (live.find(BuildFullFileNumber(dbname_, number)) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
        case kUnknown:
        default:
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(dbname_, BuildFullFileNumber(dbname_, number));
        }
        Log(options_.info_log, "[%s] Delete type=%s #%lld\n",
            dbname_.c_str(), FileTypeToString(type),
            static_cast<unsigned long long>(number));
        mutex_.Unlock();
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
        mutex_.Lock();
      }
    }
  }
}

bool DBImpl::IsDbExist() {
  if (env_->FileExists(CurrentFileName(dbname_))) {
    // db exist, ready to load, discard parent_tablets
    options_.parent_tablets.resize(0);
    return true;
  }

  // check MANIFEST. If exist, CURRENT file lost
  bool is_manifest_exist = false;
  std::vector<std::string> files;
  env_->GetChildren(dbname_, &files);
  for (size_t i = 0; i < files.size(); ++i) {
    uint64_t number;
    FileType type;
    if (ParseFileName(files[i], &number, &type) && type == kDescriptorFile) {
      is_manifest_exist = true;
    }
  }
  if (is_manifest_exist) {
    // CURRENT file lost, but MANIFEST exist, still open it
    options_.parent_tablets.resize(0);
    return true;
  }

  // CURRENT & MANIFEST not exist
  if (options_.parent_tablets.size() == 0) {
    // This is a new db
    return false;
  } else if (options_.parent_tablets.size() == 1) {
    // This is a new db generated by splitting
    std::string current =
        CurrentFileName(RealDbName(dbname_, options_.parent_tablets[0]));
    if (env_->FileExists(current)) {
      return true;
    } else {
      Log(options_.info_log, "[%s] all data(%ld) deleted, open a new db.",
          dbname_.c_str(),
          static_cast<long>(options_.parent_tablets[0]));
      return false;
    }
  } else if (options_.parent_tablets.size() == 2) {
    // This is a new db generated by merging
    std::string current0 =
        CurrentFileName(RealDbName(dbname_, options_.parent_tablets[0]));
    std::string current1 =
        CurrentFileName(RealDbName(dbname_, options_.parent_tablets[1]));
    if (env_->FileExists(current0) && env_->FileExists(current1)) {
      return true;
    } else {
      Log(options_.info_log, "[%s] all data(%ld, %ld) deleted, open a new db.",
          dbname_.c_str(),
          static_cast<long>(options_.parent_tablets[0]),
          static_cast<long>(options_.parent_tablets[1]));
      return false;
    }
  } else {
    assert(false);
  }

}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  // if (!env_->FileExists(CurrentFileName(dbname_))) {
  //    if (options_.parent_tablets.size() == 0) {
  if (!IsDbExist()) {
    s = NewDB();
    if (!s.ok()) {
      return s;
    }
  }

  Log(options_.info_log, "[%s] start VersionSet::Recover, last_seq= %llu",
      dbname_.c_str(), static_cast<unsigned long long>(versions_->LastSequence()));
  s = versions_->Recover();
  Log(options_.info_log, "[%s] end VersionSet::Recover last_seq= %llu",
      dbname_.c_str(), static_cast<unsigned long long>(versions_->LastSequence()));

  // check loss of sst files (fs exception)
  if (s.ok()) {
    std::map<uint64_t, int> expected;
    versions_->AddLiveFiles(&expected);

    // collect all tablets
    std::set<uint64_t> tablets;
    std::map<uint64_t, int>::iterator it_exp = expected.begin();
    for (; it_exp != expected.end(); ++it_exp) {
      uint64_t tablet;
      ParseFullFileNumber(it_exp->first, &tablet, NULL);
      tablets.insert(tablet);
    }

    std::set<uint64_t>::iterator it_tablet = tablets.begin();
    for (; it_tablet != tablets.end(); ++it_tablet) {
      std::string path = RealDbName(dbname_, *it_tablet);
      Log(options_.info_log, "[%s] GetChildren(%s)", dbname_.c_str(), path.c_str());
      std::vector<std::string> filenames;
      s = env_->GetChildren(path, &filenames);
      if (!s.ok()) {
        Log(options_.info_log, "[%s] GetChildren(%s) fail: %s",
            dbname_.c_str(), path.c_str(), s.ToString().c_str());
        continue;
      }
      uint64_t number;
      FileType type;
      for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type) && (type == kTableFile)) {
          expected.erase(BuildFullFileNumber(path, number));
        }
      }
    }
    if (!expected.empty()) {
      std::map<uint64_t, int>::iterator it = expected.begin();
      for (; it != expected.end(); ++it) {
        if (!env_->FileExists(TableFileName(dbname_, it->first))) {
          Log(options_.info_log, "[%s] file system lost files: %d.sst, delete it from versionset.",
              dbname_.c_str(), static_cast<int>(it->first));
          edit->DeleteFile(it->second, it->first);
        }
      }
    }
  }

  if (s.ok()) {
    state_ = kOpened;
  }
  return s;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = BuildFullFileNumber(dbname_, versions_->NewFileNumber());
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "[%s] Level-0 table #%u: started",
      dbname_.c_str(), (unsigned int) meta.number);

  uint64_t saved_size = 0;
  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta,
                   &saved_size);
    mutex_.Lock();
  }

  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "[%s] Level-0 table #%u: %lld (+ %lld ) bytes %s, %s",
      dbname_.c_str(), (unsigned int) meta.number,
      (unsigned long long) meta.file_size,
      (unsigned long long) saved_size,
      s.ToString().c_str(),
      versions_->LevelSummary(&tmp));
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

Status DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL && imm_dump_ == false);
  imm_dump_ = true;

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    // s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    if (imm_->GetLastSequence()) {
      edit.SetLastSequence(imm_->GetLastSequence());
    }
    Log(options_.info_log, "[%s] CompactMemTable SetLastSequence %lu",
        dbname_.c_str(), edit.GetLastSequence());
    s = versions_->LogAndApply(&edit, &mutex_);
    SchedulePendingCompaction();
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
  }

  assert(imm_dump_ == true);
  imm_dump_ = false;
  return s;
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done) {
    while (manual_compaction_ != NULL) {
      bg_cv_.Wait();
    }
    manual_compaction_ = &manual;
    MaybeScheduleCompaction();
    while (manual_compaction_ == &manual) {
      bg_cv_.Wait();
    }
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Log(options_.info_log, "[%s] CompactMemTable start", dbname_.c_str());
  Status s = Write(WriteOptions(), NULL);
  Log(options_.info_log, "[%s] CompactMemTable Write done", dbname_.c_str());
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    Log(options_.info_log, "[%s] CompactMemTable done", dbname_.c_str());
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

// tera-specific

bool DBImpl::FindSplitKey(const std::string& start_key,
                          const std::string& end_key,
                          double ratio,
                          std::string* split_key) {
    Slice start_slice(start_key);
    Slice end_slice(end_key);
    MutexLock l(&mutex_);
    return versions_->current()->FindSplitKey(start_key.empty()?NULL:&start_slice,
                                              end_key.empty()?NULL:&end_slice,
                                              ratio, split_key);
}

uint64_t DBImpl::GetScopeSize(const std::string& start_key,
                              const std::string& end_key,
                              std::vector<uint64_t>* lgsize) {
    Slice start_slice(start_key);
    Slice end_slice(end_key);
    MutexLock l(&mutex_);
    return versions_->current()->GetScopeSize(start_key.empty()?NULL:&start_slice,
                                              end_key.empty()?NULL:&end_slice);
}

bool DBImpl::MinorCompact() {
    Status s = TEST_CompactMemTable();
    return s.ok();
}

void DBImpl::AddInheritedLiveFiles(std::vector<std::set<uint64_t> >* live) {
  uint64_t tablet, lg;
  if (!ParseDbName(dbname_, NULL, &tablet, &lg)) {
    // have no tablet, return directly
    return;
  }
  assert(live && live->size() >= lg);

  std::set<uint64_t> live_all;
  {
    MutexLock l(&mutex_);
    versions_->AddLiveFiles(&live_all);
  }

  std::set<uint64_t>::iterator it;
  for (it = live_all.begin(); it != live_all.end(); ++it) {
    if (IsTableFileInherited(tablet, *it)) {
      (*live)[lg].insert(*it);
    }
  }
  //Log(options_.info_log, "[%s] finish collect inherited sst fils, %d totals",
  //    dbname_.c_str(), (*live)[lg].size());
}

Status DBImpl::RecoverInsertMem(WriteBatch* batch, VersionEdit* edit) {
    MutexLock lock(&mutex_);

    if (recover_mem_ == NULL) {
        recover_mem_ = NewMemTable();
        recover_mem_->Ref();
    }
    uint64_t log_sequence = WriteBatchInternal::Sequence(batch);
    uint64_t last_sequence = log_sequence + WriteBatchInternal::Count(batch) - 1;

    // if duplicate record, ignore
    if (log_sequence <= recover_mem_->GetLastSequence()) {
        assert (last_sequence <= recover_mem_->GetLastSequence());
        Log(options_.info_log, "[%s] duplicate record, ignore %lu ~ %lu",
            dbname_.c_str(), log_sequence, last_sequence);
        return Status::OK();
    }

    Status status = WriteBatchInternal::InsertInto(batch, recover_mem_);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
        return status;
    }
    if (recover_mem_->ApproximateMemoryUsage() > options_.write_buffer_size) {
        edit->SetLastSequence(recover_mem_->GetLastSequence());
        status = WriteLevel0Table(recover_mem_, edit, NULL);
        if (!status.ok()) {
            // Reflect errors immediately so that conditions like full
            // file-systems cause the DB::Open() to fail.
            return status;
        }
        recover_mem_->Unref();
        recover_mem_ = NULL;
    }
    return status;
}

Status DBImpl::RecoverLastDumpToLevel0(VersionEdit* edit) {
    MutexLock lock(&mutex_);
    Status status;
    if (recover_mem_ == NULL) {
        return status;
    }
    if (recover_mem_->GetLastSequence() > 0) {
        edit->SetLastSequence(recover_mem_->GetLastSequence());
        status = WriteLevel0Table(recover_mem_, edit, NULL);
    }
    recover_mem_->Unref();
    recover_mem_ = NULL;
    return status;
}

// end of tera-specific

#if 0
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else {
    double score = versions_->CompactionScore();
    if (manual_compaction_ != NULL) {
        score = kManualCompactScore;
    }
    if (imm_ != NULL) {
        score = kDumpMemTableScore;
    }
    if (score > 0) {
        if (bg_compaction_scheduled_ && score <= bg_compaction_score_) {
            // Already scheduled
        } else if (bg_compaction_scheduled_) {
            env_->ReSchedule(bg_schedule_id_, score);
            Log(options_.info_log, "[%s] ReSchedule Compact[%ld] score= %.2f",
                dbname_.c_str(), bg_schedule_id_, score);
            bg_compaction_score_ = score;
        } else {
            bg_schedule_id_ = env_->Schedule(&DBImpl::BGWork, this, score);
            Log(options_.info_log, "[%s] Schedule Compact[%ld] score= %.2f",
                dbname_.c_str(), bg_schedule_id_, score);
            bg_compaction_score_ = score;
            bg_compaction_scheduled_ = true;
        }
    } else {
      // No work to be done
    }
  }
}
#endif

void DBImpl::AddCompactTask(double score) {
  mutex_.AssertHeld();
  CompactTaskInfo *task = new CompactTaskInfo;
  task->db = this;
  task->id = env_->Schedule(&DBImpl::BGWork, task, score);
  compact_set_.insert(task);
  Log(options_.info_log, "[%s] add task:%ld", dbname_.c_str(), task->id);
  bg_compaction_scheduled_count_++;
}

void DBImpl::DeleteCompactTask(CompactTaskInfo *task) {
  mutex_.AssertHeld();
  std::set<CompactTaskInfo*>::iterator it = compact_set_.find(task);
  if (it == compact_set_.end()) {
    fprintf(stderr, "[%s] cannot found task id:%ld\n", dbname_.c_str(), task->id);
    abort();
  }
  Log(options_.info_log, "[%s] delete task:%ld", dbname_.c_str(), task->id);
  delete task;
  compact_set_.erase(task);
  bg_compaction_scheduled_count_--;
}

void DBImpl::ReScheduleAllCompactTask(double score) {
  mutex_.AssertHeld();
  std::set<CompactTaskInfo*>::iterator it;
  for (it = compact_set_.begin(); it != compact_set_.end(); ++it) {
    env_->ReSchedule((*it)->id, score);
    Log(options_.info_log, "[%s] reschedule task:%ld", dbname_.c_str(), (*it)->id);
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
    Log(options_.info_log, "[%s] being deleted, no more compaction", dbname_.c_str());
    return;
  }
  if ((bg_compaction_scheduled_count_ < max_background_compactions_ + 1)
      && ((imm_ != NULL) && (imm_dump_ == false))) {
    AddCompactTask(100.0);
  }
  while ((bg_compaction_scheduled_count_ < max_background_compactions_)
         && (unscheduled_compactions_ > 0)) {
    unscheduled_compactions_--;
    AddCompactTask(35.0);
  }
}

void DBImpl::BGWork(void* task) {
  CompactTaskInfo *c = reinterpret_cast<CompactTaskInfo*>(task);
  c->db->BackgroundCall(c);
}

void DBImpl::BackgroundCall(CompactTaskInfo* task) {
  Log(options_.info_log, "[%s] BackgroundCall, background-count:%d", 
          dbname_.c_str(), bg_compaction_scheduled_count_);
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_count_ > 0);
  if (!shutting_down_.Acquire_Load()) {
    Status s = BackgroundCompaction();
    if (s.ok()) {
      // Success
      consecutive_compaction_errors_ = 0;
    } else if (shutting_down_.Acquire_Load()) {
      // Error most likely due to shutdown; do not wait
    } else {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      Log(options_.info_log, "[%s] Waiting after background compaction error: %s, retry: %d",
          dbname_.c_str(), s.ToString().c_str(), consecutive_compaction_errors_);
      mutex_.Unlock();
      ++consecutive_compaction_errors_;
      if (consecutive_compaction_errors_ > 100000) {
          bg_error_ = s;
          consecutive_compaction_errors_ = 0;
      }
      int seconds_to_sleep = 1;
      for (int i = 0; i < 3 && i < consecutive_compaction_errors_ - 1; ++i) {
        seconds_to_sleep *= 2;
      }
      env_->SleepForMicroseconds(seconds_to_sleep * 1000000);
      mutex_.Lock();
    }
  }

  DeleteCompactTask(task);
  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

Status DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != NULL && imm_dump_ == false) {
    return CompactMemTable();
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "[%s] Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        dbname_.c_str(), m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickMultiThreadCompaction();
    if (c != NULL) {
      SchedulePendingCompaction();
    }
  }

  Status status;
  if (c == NULL) {
    Log(options_.info_log, "[debug] c is NULL");
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), *f);
    c->edit()->AddFile(c->level() + 1, *f);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    SchedulePendingCompaction();
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "[%s] Moved #%08u, %08u to level-%d %lld bytes %s: %s\n",
        dbname_.c_str(),
        static_cast<uint32_t>(f->number >> 32 & 0x7fffffff),  //tablet number
        static_cast<uint32_t>(f->number & 0xffffffff),        //sst number
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  if (c!=NULL && c->level() == 0) {
      Log(options_.info_log, "reset level0_being_compacted");
      versions_->SetLevel0BeingCompacted(false);
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "[%s] Compaction error: %s",
        dbname_.c_str(), status.ToString().c_str());
    if (options_.paranoid_checks && bg_error_.ok()) {
      bg_error_ = status;
    }
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
  return status;
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(BuildFullFileNumber(dbname_, out.number));
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(BuildFullFileNumber(dbname_, file_number));
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s;
  if (!options_.ignore_corruption_in_compaction) {
      s = input->status();
  }
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  const uint64_t saved_bytes = compact->builder->SavedSize();
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(&options_), dbname_,
                                               BuildFullFileNumber(dbname_, output_number),
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "[%s] Generated table #%llu: %lld keys, %lld (+ %lld ) bytes",
          dbname_.c_str(),
          (unsigned long long) output_number,
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes,
          (unsigned long long) saved_bytes);
    } else {
      Log(options_.info_log,
          "[%s] Verify new sst file fail #%llu",
          dbname_.c_str(), (unsigned long long) output_number);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "[%s] Compacted %d@%d + %d@%d files => %lld bytes",
      dbname_.c_str(),
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1, BuildFullFileNumber(dbname_, out.number),
        out.file_size, out.smallest, out.largest);
  }
  Status status = versions_->LogAndApply(compact->compaction->edit(), &mutex_);
  SchedulePendingCompaction();
  return status;
}

void DBImpl::SchedulePendingCompaction() {
   std::multimap<double, int> amap;
   versions_->ScoreMatrix(amap);
   std::multimap<double, int>::reverse_iterator rit = amap.rbegin();
   if ((rit != amap.rend()) && (rit->first >= 1.0)) {
     if ((rit->second == 0) && versions_->GetLevel0BeingCompacted()) {
       Log(options_.info_log, "[debug] no concurrently compact on level0");
     }
     unscheduled_compactions_++;
     MaybeScheduleCompaction();
     Log(options_.info_log, "[debug] another level:%d score:%lf", rit->second, rit->first);
   } else {
     Log(options_.info_log, "[debug] there is no more compaction to do");
   }
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "[%s] Compacting %d@%d + %d@%d files",
      dbname_.c_str(),
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = GetLastSequence(false);
  } else {
    compact->smallest_snapshot = *(snapshots_.begin());
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  CompactStrategy* compact_strategy = NULL;
  if (options_.compact_strategy_factory) {
     compact_strategy = options_.compact_strategy_factory->NewInstance();
  }
  Log(options_.info_log,  "[%s] Compact strategy: %s",
      dbname_.c_str(),
      compact_strategy->Name());

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      mutex_.Lock();
      if ((bg_compaction_scheduled_count_ < max_background_compactions_ + 1)
          && (imm_ != NULL && imm_dump_ == false)) {
        AddCompactTask(100.0);
      } else if (imm_ != NULL && imm_dump_ == false) {
        Log(options_.info_log, "[%s] thread pool is busy, sync dump memtable",
            dbname_.c_str());
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (RollbackDrop(ikey.sequence, rollbacks_)) {
        drop = true;
      } else if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 options_.drop_base_level_del_in_compaction &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      } else if (compact_strategy) {
        std::string lower_bound;
        if (options_.drop_base_level_del_in_compaction) {
            lower_bound = compact->compaction->drop_lower_bound();
        }
        drop = compact_strategy->Drop(ikey.user_key, ikey.sequence, lower_bound);
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    bool has_atom_merged = false;
    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);

      if (compact_strategy) {
          std::string merged_value;
          std::string merged_key;
          has_atom_merged = compact_strategy->MergeAtomicOPs(
              input, &merged_value, &merged_key);
          if (has_atom_merged) {
              Slice newValue(merged_value);
              compact->builder->Add(Slice(merged_key), newValue);
          }
      }

      if (!has_atom_merged) {
          compact->builder->Add(key, input->value());
      }
      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    if (!has_atom_merged) {
        input->Next();
    }
  }

  if (compact_strategy) {
    delete compact_strategy;
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok() && !input->status().ok()) {
      if (options_.ignore_corruption_in_compaction) {
          Log(options_.info_log, "[%s] ignore compaction error: %s",
              dbname_.c_str(), input->status().ToString().c_str());
      } else {
          status = input->status();
      }
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  VersionSet::LevelSummaryStorage tmp;
  double time_used = static_cast<double>(stats.micros);
  double compact_rate = (time_used == 0 ? -3.7 : compact->total_bytes/time_used);
  Log(options_.info_log,
      "[%s] compacted to: %s, cost time:%ld, rate:%lf", dbname_.c_str(), 
      versions_->LevelSummary(&tmp), stats.micros, compact_rate);
  return status;
}

struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = GetLastSequence(false);

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();
  mutex_.Unlock();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem->NewIterator());
  if (imm != NULL) {
    list.push_back(imm->NewIterator());
  }
  current->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());

  cleanup->mu = &mutex_;
  cleanup->mem = mem;
  cleanup->imm = imm;
  cleanup->version = current;
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  return NewInternalIterator(ReadOptions(), &ignored);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != kMaxSequenceNumber) {
    snapshot = options.snapshot;
  } else {
    snapshot = GetLastSequence(false);
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, options.rollbacks, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, options.rollbacks, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  Iterator* internal_iter = NewInternalIterator(options, &latest_snapshot);
  return NewDBIterator(
      &dbname_, env_, user_comparator(), internal_iter,
      (options.snapshot != kMaxSequenceNumber
       ? options.snapshot : latest_snapshot),
       options.rollbacks);
}

const uint64_t DBImpl::GetSnapshot(uint64_t last_sequence) {
  MutexLock l(&mutex_);
  snapshots_.insert(last_sequence);
  return last_sequence;
}

void DBImpl::ReleaseSnapshot(uint64_t sequence_number) {
  MutexLock l(&mutex_);
  std::multiset<uint64_t>::iterator it = snapshots_.find(sequence_number);
  assert(it != snapshots_.end());
  snapshots_.erase(it);
}

const uint64_t DBImpl::Rollback(uint64_t snapshot_seq, uint64_t rollback_point) {
  MutexLock l(&mutex_);
  assert(rollback_point >= snapshot_seq);
  rollbacks_[snapshot_seq] = rollback_point;
  return rollback_point;
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

bool DBImpl::BusyWrite() {
  MutexLock l(&mutex_);
  return (versions_->NumLevelFiles(0) >= options_.l0_slowdown_writes_trigger);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }


  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == NULL);

  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    uint64_t batch_sequence = WriteBatchInternal::Sequence(my_batch);
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, batch_sequence);
    assert(writers_.size() == 1);

    // Apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    is_writting_mem_ = true;

    mutex_.Unlock();
    status = WriteBatchInternal::InsertInto(updates, mem_);
    mutex_.Lock();

    if (updates == tmp_batch_) tmp_batch_->Clear();
    if (WriteBatchInternal::Count(updates) > 0) {
      mem_->SetNonEmpty();
    }
    if (mem_->Empty() && imm_ == NULL) {
      versions_->SetLastSequence(batch_sequence - 1);
    }
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  is_writting_mem_ = false;
  writting_mem_cv_.Signal();
  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *reuslt
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    } else {
      break;
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  Status s;
  assert(!writers_.empty());
  bool allow_delay = !force;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (shutting_down_.Acquire_Load()) {
      break;
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "[%s] Current memtable full; waiting...\n",
          dbname_.c_str());
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "[%s] Too many L0 files; waiting...\n",
          dbname_.c_str());
      bg_cv_.Wait();
    } else {
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = NewMemTable();
      mem_->Ref();
      bound_log_size_ = 0;
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

void DBImpl::AddBoundLogSize(uint64_t size) {
  {
    MutexLock lock(&mutex_);
    if (mem_->Empty()) {
      return;
    }
    bound_log_size_ += size;
    if (bound_log_size_ < options_.flush_triggered_log_size) {
      return;
    }
    if (imm_ != NULL) {
      Log(options_.info_log, "[%s] [TimeoutCompaction] imm_ != NULL", dbname_.c_str());
      return;
    }
  }
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    Log(options_.info_log, "[%s] [TimeoutCompaction] done %lu",
        dbname_.c_str(), size);
  } else {
    Log(options_.info_log, "[%s] [TimeoutCompaction] fail", dbname_.c_str());
  }
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || static_cast<int>(level) >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

uint64_t DBImpl::GetLastSequence(bool is_locked) {
  if (is_locked) {
      mutex_.Lock();
  }
  uint64_t retval;
  if (mem_->GetLastSequence() > 0) {
    retval = mem_->GetLastSequence();
  } else if (imm_ != NULL && imm_->GetLastSequence()) {
    retval = imm_->GetLastSequence();
  } else {
    retval = versions_->LastSequence();
  }
  if (is_locked) {
      mutex_.Unlock();
  }
  return retval;
}

MemTable* DBImpl::NewMemTable() const {
    if (!options_.use_memtable_on_leveldb) {
        return new MemTable(internal_comparator_,
                  options_.enable_strategy_when_get ? options_.compact_strategy_factory : NULL);
    } else {
        return new MemTableOnLevelDB(internal_comparator_,
                                     options_.compact_strategy_factory,
                                     options_.memtable_ldb_write_buffer_size,
                                     options_.memtable_ldb_block_size);
    }
}

uint64_t DBImpl::GetLastVerSequence() {
  MutexLock l(&mutex_);
  return versions_->LastSequence();
}

Iterator* DBImpl::NewInternalIterator() {
    SequenceNumber ignored;
    return NewInternalIterator(ReadOptions(), &ignored);
}

}  // namespace leveldb
