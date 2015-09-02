// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef STORAGE_LEVELDB_INCLUDE_DB_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_DB_TABLE_H_

#include <stdint.h>
#include <stdio.h>

#include <deque>
#include <vector>
#include <set>

#include "db/log_async_writer.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "leveldb/db.h"

namespace leveldb {

class DBImpl;
class MemTable;

class DBTable : public DB {
public:
    DBTable(const Options& options, const std::string& dbname);
    virtual ~DBTable();

    Status Init();
    virtual Status Shutdown1();
    virtual Status Shutdown2();

    // Set the database entry for "key" to "value".  Returns OK on success,
    // and a non-OK status on error.
    // Note: consider setting options.sync = true.
    virtual Status Put(const WriteOptions& options,
                       const Slice& key,
                       const Slice& value);

    // Remove the database entry (if any) for "key".  Returns OK on
    // success, and a non-OK status on error.  It is not an error if "key"
    // did not exist in the database.
    // Note: consider setting options.sync = true.
    virtual Status Delete(const WriteOptions& options, const Slice& key);

    // Is too busy to write.
    virtual bool BusyWrite();

    // Apply the specified updates to the database.
    // Returns OK on success, non-OK on failure.
    // Note: consider setting options.sync = true.
    virtual Status Write(const WriteOptions& options, WriteBatch* updates);

    // If the database contains an entry for "key" store the
    // corresponding value in *value and return OK.
    //
    // If there is no entry for "key" leave *value unchanged and return
    // a status for which Status::IsNotFound() returns true.
    //
    // May return some other Status on an error.
    virtual Status Get(const ReadOptions& options,
                       const Slice& key, std::string* value);

    // Return a heap-allocated iterator over the contents of the database.
    // The result of NewIterator() is initially invalid (caller must
    // call one of the Seek methods on the iterator before using it).
    //
    // Caller should delete the iterator when it is no longer needed.
    // The returned iterator should be deleted before this db is deleted.
    virtual Iterator* NewIterator(const ReadOptions& options);

    // Return a handle to the current DB state.  Iterators created with
    // this handle will all observe a stable snapshot of the current DB
    // state.  The caller must call ReleaseSnapshot(result) when the
    // snapshot is no longer needed.
    virtual const uint64_t GetSnapshot(uint64_t last_sequence = kMaxSequenceNumber);

    // Release a previously acquired snapshot.  The caller must not
    // use "snapshot" after this call.
    virtual void ReleaseSnapshot(uint64_t sequence_number);

    virtual const uint64_t Rollback(uint64_t snapshot_seq, uint64_t rollback_point = kMaxSequenceNumber);

    // DB implementations can export properties about their state
    // via this method.  If "property" is a valid property understood by this
    // DB implementation, fills "*value" with its current value and returns
    // true.  Otherwise returns false.
    //
    //
    // Valid property names include:
    //
    //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
    //     where <N> is an ASCII representation of a level number (e.g. "0").
    //  "leveldb.stats" - returns a multi-line string that describes statistics
    //     about the internal operation of the DB.
    //  "leveldb.sstables" - returns a multi-line string that describes all
    //     of the sstables that make up the db contents.
    virtual bool GetProperty(const Slice& property, std::string* value);

    // For each i in [0,n-1], store in "sizes[i]", the approximate
    // file system space used by keys in "[range[i].start .. range[i].limit)".
    //
    // Note that the returned sizes measure file system space usage, so
    // if the user data compresses by a factor of ten, the returned
    // sizes will be one-tenth the size of the corresponding user data size.
    //
    // The results may not include the sizes of recently written data.
    virtual void GetApproximateSizes(const Range* range, int n,
                                     uint64_t* sizes);

    // Compact the underlying storage for the key range [*begin,*end].
    // In particular, deleted and overwritten versions are discarded,
    // and the data is rearranged to reduce the cost of operations
    // needed to access the data.  This operation should typically only
    // be invoked by users who understand the underlying implementation.
    //
    // begin==NULL is treated as a key before all keys in the database.
    // end==NULL is treated as a key after all keys in the database.
    // Therefore the following call will compact the entire database:
    //    db->CompactRange(NULL, NULL);
    virtual void CompactRange(const Slice* begin, const Slice* end);

    // tera-specific
    virtual bool FindSplitKey(const std::string& start_key,
                              const std::string& end_key,
                              double ratio,
                              std::string* split_key);

    virtual uint64_t GetScopeSize(const std::string& start_key,
                                  const std::string& end_key,
                                  std::vector<uint64_t>* lgsize);

    virtual bool MinorCompact();

    // Add all sst files inherited from other tablets
    virtual void AddInheritedLiveFiles(std::vector<std::set<uint64_t> >* live);

    // for unit test
    Status TEST_CompactMemTable();
    void TEST_CompactRange(int level, const Slice* begin, const Slice* end);
    Iterator* TEST_NewInternalIterator();
    int64_t TEST_MaxNextLevelOverlappingBytes();

private:
    struct RecordWriter;
    WriteBatch* GroupWriteBatch(RecordWriter** last_writer);

    Status RecoverLogFile(uint64_t log_number, uint64_t recover_limit,
                          std::vector<VersionEdit*>* edit_list);
    void MaybeIgnoreError(Status* s) const;
    Status GatherLogFile(uint64_t begin_num,
                         std::vector<uint64_t>* logfiles);
    Status DeleteLogFile(const std::vector<uint64_t>& log_numbers);
    void DeleteObsoleteFiles(uint64_t seq_no = -1U);
    void ArchiveFile(const std::string& filepath);

    // return 0: switch log successed
    // return 1: cannot switch log right now
    // return 2: can switch but failed
    int SwitchLog(bool blocked_switch);
    void ScheduleGarbageClean(double score);
    static void GarbageCleanWrapper(void* db);
    void BackgroundGarbageClean();
    void GarbageClean();

private:
    State state_;
    std::vector<DBImpl*> lg_list_;
    port::Mutex mutex_;
    port::AtomicPointer shutting_down_;
    port::CondVar bg_cv_;
    port::CondVar bg_cv_timer_;
    port::CondVar bg_cv_sleeper_;
    const Options options_;
    const std::string dbname_;
    Env* const env_;
    bool created_own_lg_list_;
    bool created_own_info_log_;
    bool created_own_compact_strategy_;
    uint64_t commit_snapshot_;
    Status fatal_error_;

    WritableFile* logfile_;
    log::AsyncWriter* log_;
    bool force_switch_log_;
    uint64_t last_sequence_;
    size_t current_log_size_;

    std::deque<RecordWriter*> writers_;
    WriteBatch* tmp_batch_;

    // for GC schedule
    bool bg_schedule_gc_;
    int64_t bg_schedule_gc_id_;
    double bg_schedule_gc_score_;
    uint64_t force_clean_log_seq_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_DB_TABLE_H_
