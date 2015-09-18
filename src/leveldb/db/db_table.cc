// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db/db_table.h"

#include <algorithm>
#include <iostream>
#include <vector>
#include <stdio.h>

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/lg_compact_thread.h"
#include "db/log_reader.h"
#include "db/memtable.h"
#include "db/memtable.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/compact_strategy.h"
#include "leveldb/iterator.h"
#include "leveldb/lg_coding.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"
#include "leveldb/table_utils.h"
#include "table/merger.h"
#include "util/string_ext.h"

namespace leveldb {

struct DBTable::RecordWriter {
    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;

    explicit RecordWriter(port::Mutex* mu) : cv(mu) {}
};

Options InitDefaultOptions(const Options& options, const std::string& dbname) {
    Options opt = options;
    Status s = opt.env->CreateDir(dbname);
    if (!s.ok()) {
        std::cerr << "[" << dbname << "] fail to create dir: "
            << s.ToString() << std::endl;
    }
    if (opt.info_log == NULL) {
        opt.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
        s = opt.env->NewLogger(InfoLogFileName(dbname), &opt.info_log);
        if (!s.ok()) {
            // No place suitable for logging
            std::cerr << "[" << dbname << "] fail to init info log: "
                << s.ToString() << std::endl;
            opt.info_log = NULL;
        }
    }
    assert(s.ok());

    if (opt.exist_lg_list == NULL) {
        opt.exist_lg_list = new std::set<uint32_t>;
        opt.exist_lg_list->insert(0);
    }
    if (opt.compact_strategy_factory == NULL) {
        opt.compact_strategy_factory = new DummyCompactStrategyFactory();
    }
    return opt;
}

Options InitOptionsLG(const Options& options, uint32_t lg_id) {
    Options opt = options;
    if (options.lg_info_list == NULL) {
        return opt;
    }

    std::map<uint32_t, LG_info*>::iterator it = options.lg_info_list->find(lg_id);
    if (it == options.lg_info_list->end() || it->second == NULL) {
        return opt;
    }
    LG_info* lg_info = it->second;
    if (lg_info->env) {
        opt.env = lg_info->env;
    }
    opt.compression = lg_info->compression;
    opt.block_size = lg_info->block_size;
    opt.use_memtable_on_leveldb = lg_info->use_memtable_on_leveldb;
    opt.memtable_ldb_write_buffer_size = lg_info->memtable_ldb_write_buffer_size;
    opt.memtable_ldb_block_size = lg_info->memtable_ldb_block_size;
    opt.sst_size = lg_info->sst_size;
    return opt;
}

DBTable::DBTable(const Options& options, const std::string& dbname)
    : state_(kNotOpen), shutting_down_(NULL), bg_cv_(&mutex_),
      bg_cv_timer_(&mutex_), bg_cv_sleeper_(&mutex_),
      options_(InitDefaultOptions(options, dbname)),
      dbname_(dbname), env_(options.env),
      created_own_lg_list_(options_.exist_lg_list != options.exist_lg_list),
      created_own_info_log_(options_.info_log != options.info_log),
      created_own_compact_strategy_(options_.compact_strategy_factory != options.compact_strategy_factory),
      commit_snapshot_(kMaxSequenceNumber), logfile_(NULL), log_(NULL), force_switch_log_(false),
      last_sequence_(0), current_log_size_(0),
      tmp_batch_(new WriteBatch),
      bg_schedule_gc_(false), bg_schedule_gc_id_(0),
      bg_schedule_gc_score_(0), force_clean_log_seq_(0) {
}

Status DBTable::Shutdown1() {
    assert(state_ == kOpened);
    state_ = kShutdown1;

    Log(options_.info_log, "[%s] shutdown1 start", dbname_.c_str());
    shutting_down_.Release_Store(this);

    Status s;
    for (uint32_t i = 0; i < lg_list_.size(); ++i) {
        DBImpl* impl = lg_list_[i];
        if (impl) {
            Status impl_s = impl->Shutdown1();
            if (!impl_s.ok()) {
                s = impl_s;
            }
        }
    }

    Log(options_.info_log, "[%s] wait bg garbage clean finish", dbname_.c_str());
    mutex_.Lock();
    if (bg_schedule_gc_) {
        env_->ReSchedule(bg_schedule_gc_id_, kDeleteLogUrgentScore);
    }
    while (bg_schedule_gc_) {
        bg_cv_.Wait();
    }
    mutex_.Unlock();

    Log(options_.info_log, "[%s] fg garbage clean", dbname_.c_str());
    GarbageClean();

    Log(options_.info_log, "[%s] shutdown1 done", dbname_.c_str());
    return s;
}

Status DBTable::Shutdown2() {
    assert(state_ == kShutdown1);
    state_ = kShutdown2;

    Log(options_.info_log, "[%s] shutdown2 start", dbname_.c_str());

    Status s;
    for (uint32_t i = 0; i < lg_list_.size(); ++i) {
        DBImpl* impl = lg_list_[i];
        if (impl) {
            Status impl_s = impl->Shutdown2();
            if (!impl_s.ok()) {
                s = impl_s;
            }
        }
    }

    Log(options_.info_log, "[%s] stop async log", dbname_.c_str());
    if (log_) {
        log_->Stop(false);
    }

    if (s.ok() && options_.dump_mem_on_shutdown) {
        Log(options_.info_log, "[%s] gather all log file", dbname_.c_str());
        std::vector<uint64_t> logfiles;
        s = GatherLogFile(0, &logfiles);
        if (s.ok()) {
            Log(options_.info_log, "[%s] delete all log file", dbname_.c_str());
            s = DeleteLogFile(logfiles);
        }
    }

    Log(options_.info_log, "[%s] shutdown2 done", dbname_.c_str());
    return s;
}

DBTable::~DBTable() {
    // Shutdown1 must be called before delete.
    // Shutdown2 is both OK to be called or not.
    // But if Shutdown1 returns non-ok, Shutdown2 must NOT be called.
    if (state_ == kOpened) {
        Status s = Shutdown1();
        if (s.ok()) {
            Shutdown2();
        }
    }
    for (uint32_t i = 0; i < lg_list_.size(); ++i) {
        DBImpl* impl = lg_list_[i];
        if (impl) {
            delete impl;
        }
    }

    if (created_own_lg_list_) {
        delete options_.exist_lg_list;
    }

    if (created_own_compact_strategy_) {
        delete options_.compact_strategy_factory;
    }

    if (created_own_info_log_) {
        delete options_.info_log;
    }
    delete tmp_batch_;
}

Status DBTable::Init() {
    std::vector<VersionEdit*> lg_edits;
    Status s;
    Log(options_.info_log, "[%s] start Init()", dbname_.c_str());
    MutexLock lock(&mutex_);

    uint64_t min_log_sequence = kMaxSequenceNumber;
    std::vector<uint64_t> snapshot_sequence = options_.snapshots_sequence;
    std::map<uint64_t, uint64_t> rollbacks = options_.rollbacks;
    for (std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
         it != options_.exist_lg_list->end() && s.ok(); ++it) {
        uint32_t i = *it;
        DBImpl* impl = new DBImpl(InitOptionsLG(options_, i),
                                  dbname_ + "/" + Uint64ToString(i));
        lg_list_.push_back(impl);
        lg_edits.push_back(new VersionEdit);
        for (uint32_t i = 0; i < snapshot_sequence.size(); ++i) {
            impl->GetSnapshot(snapshot_sequence[i]);
        }
        std::map<uint64_t, uint64_t>::iterator rollback_it = rollbacks.begin();
        for (; rollback_it != rollbacks.end(); ++rollback_it) {
            impl->Rollback(rollback_it->first, rollback_it->second);
        }

        // recover SST
        Log(options_.info_log, "[%s] start Recover lg%d, last_seq= %lu",
            dbname_.c_str(), i, impl->GetLastSequence());
        s = impl->Recover(lg_edits[i]);
        Log(options_.info_log, "[%s] end Recover lg%d, last_seq= %lu",
            dbname_.c_str(), i, impl->GetLastSequence());
        if (s.ok()) {
            uint64_t last_seq = impl->GetLastSequence();

            Log(options_.info_log,
                "[%s] Recover lg %d last_log_seq= %lu", dbname_.c_str(), i, last_seq);
            if (min_log_sequence > last_seq) {
                min_log_sequence = last_seq;
            }
            if (last_sequence_ < last_seq) {
                last_sequence_ = last_seq;
            }
        } else {
            Log(options_.info_log, "[%s] fail to recover lg %d", dbname_.c_str(), i);
            break;
        }
    }
    if (!s.ok()) {
        Log(options_.info_log, "[%s] fail to recover table.", dbname_.c_str());
        for (uint32_t i = 0; i != lg_list_.size(); ++i) {
            delete lg_list_[i];
        }
        lg_list_.clear();
        return s;
    }

    Log(options_.info_log, "[%s] start GatherLogFile", dbname_.c_str());
    // recover log files
    std::vector<uint64_t> logfiles;
    s = GatherLogFile(min_log_sequence + 1, &logfiles);
    if (s.ok()) {
        for (uint32_t i = 0; i < logfiles.size(); ++i) {
            // If two log files have overlap sequence id, ignore records
            // from old log.
            uint64_t recover_limit = kMaxSequenceNumber;
            if (i < logfiles.size() - 1) {
                recover_limit = logfiles[i + 1];
            }
            s = RecoverLogFile(logfiles[i], recover_limit, &lg_edits);
            if (!s.ok()) {
                Log(options_.info_log, "[%s] Fail to RecoverLogFile %ld",
                    dbname_.c_str(), logfiles[i]);
            }
        }
    } else {
        Log(options_.info_log, "[%s] Fail to GatherLogFile", dbname_.c_str());
    }

    Log(options_.info_log, "[%s] start RecoverLogToLevel0Table", dbname_.c_str());
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        uint32_t i = *it;
        DBImpl* impl = lg_list_[i];
        s = impl->RecoverLastDumpToLevel0(lg_edits[i]);

        // LogAndApply to lg's manifest
        if (s.ok()) {
            MutexLock lock(&impl->mutex_);
            s = impl->versions_->LogAndApply(lg_edits[i], &impl->mutex_);
            if (s.ok()) {
                impl->DeleteObsoleteFiles();
                impl->MaybeScheduleCompaction();
            } else {
                Log(options_.info_log, "[%s] Fail to modify manifest of lg %d",
                    dbname_.c_str(),
                    i);
            }
        } else {
            Log(options_.info_log, "[%s] Fail to dump log to level 0", dbname_.c_str());
        }
        delete lg_edits[i];
    }

    if (s.ok()) {
        Log(options_.info_log, "[%s] start DeleteLogFile", dbname_.c_str());
        s = DeleteLogFile(logfiles);
    }

    if (s.ok() && !options_.disable_wal) {
        std::string log_file_name = LogHexFileName(dbname_, last_sequence_ + 1);
        s = options_.env->NewWritableFile(log_file_name, &logfile_);
        if (s.ok()) {
            //Log(options_.info_log, "[%s] open logfile %s",
            //    dbname_.c_str(), log_file_name.c_str());
            log_ = new log::AsyncWriter(logfile_, options_.log_async_mode);
        } else {
            Log(options_.info_log, "[%s] fail to open logfile %s",
                dbname_.c_str(), log_file_name.c_str());
        }
    }

    if (s.ok()) {
        state_ = kOpened;
        Log(options_.info_log, "[%s] custom compact strategy: %s, flush trigger %lu",
            dbname_.c_str(), options_.compact_strategy_factory->Name(),
            options_.flush_triggered_log_num);

        Log(options_.info_log, "[%s] Init() done, last_seq=%llu", dbname_.c_str(),
            static_cast<unsigned long long>(last_sequence_));
    } else {
        for (uint32_t i = 0; i != lg_list_.size(); ++i) {
            delete lg_list_[i];
        }
        lg_list_.clear();
    }
    return s;
}

Status DBTable::Put(const WriteOptions& options,
                    const Slice& key, const Slice& value) {
    return DB::Put(options, key, value);
}

Status DBTable::Delete(const WriteOptions& options, const Slice& key) {
    return DB::Delete(options, key);
}

bool DBTable::BusyWrite() {
    MutexLock l(&mutex_);
    for (std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
            it != options_.exist_lg_list->end(); ++it) {
        if (lg_list_[*it]->BusyWrite()) {
            return true;
        }
    }
    return false;
}

Status DBTable::Write(const WriteOptions& options, WriteBatch* my_batch) {
    RecordWriter w(&mutex_);
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

    // DB with fatal error is unwritable.
    Status s = fatal_error_;

    RecordWriter* last_writer = &w;
    WriteBatch* updates = NULL;
    if (s.ok()) {
        updates = GroupWriteBatch(&last_writer);
        WriteBatchInternal::SetSequence(updates, last_sequence_ + 1);
    }

    if (s.ok() && !options_.disable_wal && !options.disable_wal) {
        if (force_switch_log_ || current_log_size_ > options_.log_file_size) {
            mutex_.Unlock();
            if (SwitchLog(false) == 2) {
                s = Status::IOError(dbname_ + ": fail to open log: ", s.ToString());
            } else {
                force_switch_log_ = false;
            }
            mutex_.Lock();
        }
    }

    // dump to log
    if (s.ok() && !options_.disable_wal && !options.disable_wal) {
        mutex_.Unlock();

        Slice slice = WriteBatchInternal::Contents(updates);
        uint32_t wait_sec = options_.write_log_time_out;
        for (; ; wait_sec <<= 1) {
            // write a record into log
            log_->AddRecord(slice);
            s = log_->WaitDone(wait_sec);
            if (s.IsTimeOut()) {
                Log(options_.info_log, "[%s] AddRecord time out %lu",
                    dbname_.c_str(), current_log_size_);
                int ret = SwitchLog(true);
                if (ret == 0) {
                    continue;
                } else if (ret == 1) {
                    s = log_->WaitDone(-1);
                    if (!s.ok()) {
                        break;
                    }
                } else {
                    s = Status::IOError(dbname_ + ": fail to open log: ", s.ToString());
                    break;
                }
            }
            // do sync if needed
            if (!s.ok()) {
                s = Status::IOError(dbname_ + ": fail to write log: ", s.ToString());
                force_switch_log_ = true;
            } else {
                log_->Sync(options.sync);
                s = log_->WaitDone(wait_sec);
                if (s.IsTimeOut()) {
                    Log(options_.info_log, "[%s] Sync time out %lu",
                        dbname_.c_str(), current_log_size_);
                    int ret = SwitchLog(true);
                    if (ret == 0) {
                        continue;
                    } else if (ret == 1) {
                        s = log_->WaitDone(-1);
                        if (s.ok()) {
                            continue;
                        }
                    } else {
                        s = Status::IOError(dbname_ + ": fail to open log: ", s.ToString());
                        break;
                    }
                }
                if (!s.ok()) {
                    s = Status::IOError(dbname_ + ": fail to sync log: ", s.ToString());
                    force_switch_log_ = true;
                }
            }
            break;
        }
        mutex_.Lock();
    }
    if (s.ok()) {
        std::vector<WriteBatch*> lg_updates;
        lg_updates.resize(lg_list_.size());
        std::fill(lg_updates.begin(), lg_updates.end(), (WriteBatch*)0);
        bool created_new_wb = false;
        // kv version may not create snapshot
        for (uint32_t i = 0; i < lg_list_.size(); ++i) {
            lg_list_[i]->GetSnapshot(last_sequence_);
        }
        commit_snapshot_ = last_sequence_;
        if (lg_list_.size() > 1) {
            updates->SeperateLocalityGroup(&lg_updates);
            created_new_wb = true;
        } else {
            lg_updates[0] = updates;
        }
        mutex_.Unlock();
        //TODO: should be multi-thread distributed
        for (uint32_t i = 0; i < lg_updates.size(); ++i) {
            assert(lg_updates[i] != NULL);
            Status lg_s = lg_list_[i]->Write(WriteOptions(), lg_updates[i]);
            if (!lg_s.ok()) {
                // 这种情况下内存处于不一致状态
                Log(options_.info_log, "[%s] [Fatal] Write to lg%u fail",
                    dbname_.c_str(), i);
                s = lg_s;
                fatal_error_ = lg_s;
                break;
            }
        }
        mutex_.Lock();
        if (s.ok()) {
            for (uint32_t i = 0; i < lg_list_.size(); ++i) {
                lg_list_[i]->AddBoundLogSize(updates->DataSize());
            }
        }
        // Commit updates
        if (s.ok()) {
            for (uint32_t i = 0; i < lg_list_.size(); ++i) {
                lg_list_[i]->ReleaseSnapshot(commit_snapshot_);
            }
            commit_snapshot_ = last_sequence_ + WriteBatchInternal::Count(updates);
        }

        if (created_new_wb) {
            for (uint32_t i = 0; i < lg_updates.size(); ++i) {
                if (lg_updates[i] != NULL) {
                    delete lg_updates[i];
                    lg_updates[i] = NULL;
                }
            }
        }
    }

    // Update last_sequence
    if (updates) {
        last_sequence_ += WriteBatchInternal::Count(updates);
        current_log_size_ += WriteBatchInternal::ByteSize(updates);
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    while (true) {
        RecordWriter* ready = writers_.front();
        writers_.pop_front();
        if (ready != &w) {
            ready->status = s;
            ready->done = true;
            ready->cv.Signal();
        }
        if (ready == last_writer) break;
    }

    if (!writers_.empty()) {
        writers_.front()->cv.Signal();
    }

    return s;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBTable::GroupWriteBatch(RecordWriter** last_writer) {
    assert(!writers_.empty());
    RecordWriter* first = writers_.front();
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
    std::deque<RecordWriter*>::iterator iter = writers_.begin();
    ++iter;  // Advance past "first"
    for (; iter != writers_.end(); ++iter) {
      RecordWriter* w = *iter;
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
        assert(0);
      }
      *last_writer = w;
    }
    return result;
}

Status DBTable::Get(const ReadOptions& options,
                    const Slice& key, std::string* value) {
    uint32_t lg_id = 0;
    Slice real_key = key;
    if (!GetFixed32LGId(&real_key, &lg_id)) {
        lg_id = 0;
        real_key = key;
    }
    std::set<uint32_t>::iterator it = options_.exist_lg_list->find(lg_id);
    if (it == options_.exist_lg_list->end()) {
        return Status::InvalidArgument("lg_id invalid: " + Uint64ToString(lg_id));
    }
    ReadOptions new_options = options;
    MutexLock lock(&mutex_);
    if (options.snapshot != kMaxSequenceNumber) {
        new_options.snapshot = options.snapshot;
    } else if (commit_snapshot_ != kMaxSequenceNumber) {
        new_options.snapshot = commit_snapshot_;
    }
    mutex_.Unlock();
    Status s = lg_list_[lg_id]->Get(new_options, real_key, value);
    mutex_.Lock();
    return s;
}

Iterator* DBTable::NewIterator(const ReadOptions& options) {
    std::vector<Iterator*> list;
    ReadOptions new_options = options;
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    mutex_.Lock();
    if (options.snapshot != kMaxSequenceNumber) {
        new_options.snapshot = options.snapshot;
    } else if (commit_snapshot_ != kMaxSequenceNumber) {
        new_options.snapshot = commit_snapshot_;
    }
    mutex_.Unlock();
    it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        if (options.target_lgs) {
            std::set<uint32_t>::const_iterator found_it =
                options.target_lgs->find(*it);
            if (found_it == options.target_lgs->end()) {
                continue;
            }
        }
        list.push_back(lg_list_[*it]->NewIterator(new_options));
    }
    return NewMergingIterator(options_.comparator, &list[0], list.size());
}

const uint64_t DBTable::GetSnapshot(uint64_t last_sequence) {
    MutexLock lock(&mutex_);
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    uint64_t seq = last_sequence == kMaxSequenceNumber ? last_sequence_ : last_sequence;
    for (; it != options_.exist_lg_list->end(); ++it) {
        lg_list_[*it]->GetSnapshot(seq);
    }
    return seq;
}

void DBTable::ReleaseSnapshot(uint64_t sequence_number) {
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        lg_list_[*it]->ReleaseSnapshot(sequence_number);
    }
}

const uint64_t DBTable::Rollback(uint64_t snapshot_seq, uint64_t rollback_point) {
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    uint64_t rollback_seq = rollback_point == kMaxSequenceNumber ? last_sequence_ : rollback_point;;
    for (; it != options_.exist_lg_list->end(); ++it) {
        lg_list_[*it]->Rollback(snapshot_seq, rollback_seq);
    }
    return rollback_seq;
}

bool DBTable::GetProperty(const Slice& property, std::string* value) {
    bool ret = true;
    std::string ret_string;
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        std::string lg_value;
        bool lg_ret = lg_list_[*it]->GetProperty(property, &lg_value);
        if (lg_ret) {
            if (options_.exist_lg_list->size() > 1) {
                ret_string.append(Uint64ToString(*it) + ": {\n");
            }
            ret_string.append(lg_value);
            if (options_.exist_lg_list->size() > 1) {
                ret_string.append("\n}\n");
            }
        }
    }
    *value = ret_string;
    return ret;
}

void DBTable::GetApproximateSizes(const Range* range, int n,
                                  uint64_t* sizes) {
    for (int j = 0; j < n; ++j) {
        sizes[j] = 0;
    }

    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        uint32_t i = *it;
        uint64_t lg_sizes[config::kNumLevels] = {0};
        lg_list_[i]->GetApproximateSizes(range, n, lg_sizes);
        for (int j = 0; j < n; ++j) {
            sizes[j] += lg_sizes[j];
        }
    }
}

void DBTable::CompactRange(const Slice* begin, const Slice* end) {
    std::vector<LGCompactThread*> lg_threads;
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        LGCompactThread* thread = new LGCompactThread(*it, lg_list_[*it], begin, end);
        lg_threads.push_back(thread);
        thread->Start();
    }
    for (uint32_t i = 0; i < lg_threads.size(); ++i) {
        lg_threads[i]->Join();
        delete lg_threads[i];
    }
}

// @begin_num:  the 1st record(sequence number) should be recover
Status DBTable::GatherLogFile(uint64_t begin_num,
                              std::vector<uint64_t>* logfiles) {
    std::vector<std::string> files;
    Status s = env_->GetChildren(dbname_, &files);
    if (!s.ok()) {
        Log(options_.info_log, "[%s] GatherLogFile fail", dbname_.c_str());
        return s;
    }
    uint64_t number = 0;
    uint64_t last_number = 0;
    FileType type;
    for (uint32_t i = 0; i < files.size(); ++i) {
        type = kUnknown;
        number = 0;
        if (ParseFileName(files[i], &number, &type)
            && type == kLogFile && number >= begin_num) {
            logfiles->push_back(number);
        } else if (type == kLogFile && number > last_number) {
            last_number = number;
        }
    }
    std::sort(logfiles->begin(), logfiles->end());
    uint64_t first_log_num = logfiles->size() ? (*logfiles)[0] : 0;
    Log(options_.info_log, "[%s] begin_seq= %lu, first log num= %lu, last num=%lu, log_num=%lu\n",
        dbname_.c_str(), begin_num, first_log_num, last_number, logfiles->size());
    /* case 1
     *                                            @begin_num(not in sst)
     *       |->      records in sst            <-|->          records in log           <-|                
     *range: [start ------------------------------------------------------------------ end]
     *              ^         ^                     ^
     *              001.log   last_number.log       first_log_num.log
     */

    /* case 2
     *                                            @begin_num(not in sst)
     *       |->      records in sst            <-|->          records in log           <-|                
     *range: [start ------------------------------------------------------------------ end]
     *              ^         ^
     *              001.log   last_number.log
     */
    if ((last_number > 0 && first_log_num > begin_num)   // case 1
        || (logfiles->size() == 0 && last_number > 0)) { // case 2
        logfiles->push_back(last_number);
        Log(options_.info_log, "[%s] add log file #%lu", dbname_.c_str(), last_number);
    }
    std::sort(logfiles->begin(), logfiles->end());
    return s;
}

Status DBTable::RecoverLogFile(uint64_t log_number, uint64_t recover_limit,
                               std::vector<VersionEdit*>* edit_list) {
    struct LogReporter : public log::Reader::Reporter {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status;  // NULL if options_.paranoid_checks==false
        virtual void Corruption(size_t bytes, const Status& s) {
            Log(info_log, "%s%s: dropping %d bytes; %s",
                (this->status == NULL ? "(ignoring error) " : ""),
                fname, static_cast<int>(bytes), s.ToString().c_str());
            if (this->status != NULL && this->status->ok()) *this->status = s;
        }
    };

    mutex_.AssertHeld();

    // Open the log file
    std::string fname = LogHexFileName(dbname_, log_number);
    SequentialFile* file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok()) {
        MaybeIgnoreError(&status);
        return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : NULL);
    log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
    Log(options_.info_log, "[%s] Recovering log #%lx, sequence limit %lu",
        dbname_.c_str(), log_number, recover_limit);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    while (reader.ReadRecord(&record, &scratch) && status.ok()) {
        if (record.size() < 12) {
            reporter.Corruption(record.size(),
                                Status::Corruption("log record too small"));
            continue;
        }
        WriteBatchInternal::SetContents(&batch, record);
        uint64_t first_seq = WriteBatchInternal::Sequence(&batch);
        uint64_t last_seq = first_seq + WriteBatchInternal::Count(&batch) - 1;
        //Log(options_.info_log, "[%s] batch_seq= %lu, last_seq= %lu, count=%d",
        //    dbname_.c_str(), batch_seq, last_sequence_, WriteBatchInternal::Count(&batch));
        if (last_seq >= recover_limit) {
            Log(options_.info_log, "[%s] exceed limit %lu, ignore %lu ~ %lu",
                        dbname_.c_str(), recover_limit, first_seq, last_seq);
            continue;
        }

        if (last_seq > last_sequence_) {
            last_sequence_ = last_seq;
        }

        std::vector<WriteBatch*> lg_updates;
        lg_updates.resize(lg_list_.size());
        std::fill(lg_updates.begin(), lg_updates.end(), (WriteBatch*)0);
        bool created_new_wb = false;
        if (lg_list_.size() > 1) {
            status = batch.SeperateLocalityGroup(&lg_updates);
            created_new_wb = true;
            if (!status.ok()) {
                return status;
            }
        } else {
            lg_updates[0] = (&batch);
        }

        if (status.ok()) {
            //TODO: should be multi-thread distributed
            for (uint32_t i = 0; i < lg_updates.size(); ++i) {
                if (lg_updates[i] == NULL) {
                    continue;
                }
                if (last_seq <= lg_list_[i]->GetLastSequence()) {
                    continue;
                }
                uint64_t first = WriteBatchInternal::Sequence(lg_updates[i]);
                uint64_t last = first + WriteBatchInternal::Count(lg_updates[i]) - 1;
                // Log(options_.info_log, "[%s] recover log batch first= %lu, last= %lu\n",
                //     dbname_.c_str(), first, last);

                Status lg_s = lg_list_[i]->RecoverInsertMem(lg_updates[i], (*edit_list)[i]);
                if (!lg_s.ok()) {
                    Log(options_.info_log, "[%s] recover log fail batch first= %lu, last= %lu\n",
                        dbname_.c_str(), first, last);
                    status = lg_s;
                }
            }
        }

        if (created_new_wb) {
            for (uint32_t i = 0; i < lg_updates.size(); ++i) {
                if (lg_updates[i] != NULL) {
                    delete lg_updates[i];
                    lg_updates[i] = NULL;
                }
            }
        }
    }
    delete file;

    return status;
}

void DBTable::MaybeIgnoreError(Status* s) const {
    if (s->ok() || options_.paranoid_checks) {
        // No change needed
    } else {
        Log(options_.info_log, "[%s] Ignoring error %s",
            dbname_.c_str(), s->ToString().c_str());
        *s = Status::OK();
    }
}

Status DBTable::DeleteLogFile(const std::vector<uint64_t>& log_numbers) {
    Status s;
    for (uint32_t i = 0; i < log_numbers.size() && s.ok(); ++i) {
        uint64_t log_number = log_numbers[i];
        Log(options_.info_log, "[%s] Delete type=%s #%llu",
            dbname_.c_str(), FileTypeToString(kLogFile),
            static_cast<unsigned long long>(log_number));
        std::string fname = LogHexFileName(dbname_, log_number);
        s = env_->DeleteFile(fname);
        // The last log file must be deleted before write a new log
        // in case of record sequence_id overlap;
        // Fail to delete other log files may be accepted.
        if (i < log_numbers.size() - 1) {
            MaybeIgnoreError(&s);
        }
        if (!s.ok()) {
            Log(options_.info_log, "[%s] fail to delete logfile %llu: %s",
                dbname_.c_str(), static_cast<unsigned long long>(log_number),
                s.ToString().data());
        }
    }
    return s;
}

void DBTable::DeleteObsoleteFiles(uint64_t seq_no) {
    std::vector<std::string> filenames;
    env_->GetChildren(dbname_, &filenames);
    std::sort(filenames.begin(), filenames.end());
    Log(options_.info_log, "[%s] will delete obsolete file num: %u [seq < %llu]",
        dbname_.c_str(), static_cast<uint32_t>(filenames.size()),
        static_cast<unsigned long long>(seq_no));
    uint64_t number;
    FileType type;
    std::string last_file;
    for (size_t i = 0; i < filenames.size(); ++i) {
        bool deleted = false;
        if (ParseFileName(filenames[i], &number, &type)
            && type == kLogFile && number < seq_no) {
            deleted = true;
        }
        if (deleted) {
            Log(options_.info_log, "[%s] Delete type=%s #%llu",
                dbname_.c_str(), FileTypeToString(type),
                static_cast<unsigned long long>(number));
            if (!last_file.empty()) {
//                 ArchiveFile(dbname_ + "/" + last_file);
                env_->DeleteFile(dbname_ + "/" + last_file);
            }
            last_file = filenames[i];
        }
    }
}

void DBTable::ArchiveFile(const std::string& fname) {
    const char* slash = strrchr(fname.c_str(), '/');
    std::string new_dir;
    if (slash != NULL) {
        new_dir.assign(fname.data(), slash - fname.data());
    }
    new_dir.append("/lost");
    env_->CreateDir(new_dir);  // Ignore error
    std::string new_file = new_dir;
    new_file.append("/");
    new_file.append((slash == NULL) ? fname.c_str() : slash + 1);
    Status s = env_->RenameFile(fname, new_file);
    Log(options_.info_log, "[%s] Archiving %s: %s\n",
        dbname_.c_str(),
        fname.c_str(), s.ToString().c_str());
}

// tera-specific
bool DBTable::FindSplitKey(const std::string& start_key,
                           const std::string& end_key,
                           double ratio,
                           std::string* split_key) {
    // sort by lg size
    std::map<uint64_t, DBImpl*> size_of_lg;
    MutexLock l(&mutex_);
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        uint64_t size = lg_list_[*it]->GetScopeSize(start_key, end_key);
        size_of_lg[size] = lg_list_[*it];
    }
    std::map<uint64_t, DBImpl*>::reverse_iterator biggest_it =
        size_of_lg.rbegin();
    if (biggest_it == size_of_lg.rend()) {
        return false;
    }
    return biggest_it->second->FindSplitKey(start_key, end_key,
                                            ratio, split_key);
}

uint64_t DBTable::GetScopeSize(const std::string& start_key,
                               const std::string& end_key,
                               std::vector<uint64_t>* lgsize) {
    uint64_t size = 0;
    if (lgsize != NULL) {
        lgsize->clear();
    }
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        uint64_t lsize = lg_list_[*it]->GetScopeSize(start_key, end_key);
        size += lsize;
        if (lgsize != NULL) {
            lgsize->push_back(lsize);
        }
    }
    return size;
}

bool DBTable::MinorCompact() {
    bool ok = true;
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        bool ret = lg_list_[*it]->MinorCompact();
        ok = (ok && ret);
    }
    MutexLock lock(&mutex_);
    ScheduleGarbageClean(kDeleteLogScore);
    return ok;
}

void DBTable::AddInheritedLiveFiles(std::vector<std::set<uint64_t> >* live) {
    size_t lg_num = lg_list_.size();
    assert(live && live->size() == lg_num);
    {
        MutexLock l(&mutex_);
        for (size_t i = 0; i < lg_num; ++i) {
            lg_list_[i]->AddInheritedLiveFiles(live);
        }
    }
    //Log(options_.info_log, "[%s] finish collect inherited sst fils",
    //    dbname_.c_str());
}

// end of tera-specific

// for unit test
Status DBTable::TEST_CompactMemTable() {
    Status s;
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        s = lg_list_[*it]->TEST_CompactMemTable();
        if (!s.ok()) {
            return s;
        }
    }
    return Status::OK();
}

void DBTable::TEST_CompactRange(int level, const Slice* begin, const Slice* end) {
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        lg_list_[*it]->TEST_CompactRange(level, begin, end);
    }
}

Iterator* DBTable::TEST_NewInternalIterator() {
    std::vector<Iterator*> list;
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        list.push_back(lg_list_[*it]->TEST_NewInternalIterator());
    }
    return NewMergingIterator(options_.comparator, &list[0], list.size());
}

int64_t DBTable::TEST_MaxNextLevelOverlappingBytes() {
    return 0;
}

int DBTable::SwitchLog(bool blocked_switch) {
    if (!blocked_switch ||
        log::AsyncWriter::BlockLogNum() < options_.max_block_log_number) {
        if (current_log_size_ == 0) {
            last_sequence_++;
        }
        WritableFile* logfile = NULL;
        std::string log_file_name = LogHexFileName(dbname_, last_sequence_ + 1);
        Status s = env_->NewWritableFile(log_file_name, &logfile);
        if (s.ok()) {
            log_->Stop(blocked_switch);
            logfile_ = logfile;
            log_ = new log::AsyncWriter(logfile, options_.log_async_mode);
            current_log_size_ = 0;

            // protect bg thread cv
            mutex_.Lock();
            ScheduleGarbageClean(kDeleteLogScore);
            mutex_.Unlock();

            if (blocked_switch) {
                // if we switched log because it was blocked
                log::AsyncWriter::BlockLogNumInc();
                Log(options_.info_log, "[%s] SwitchLog", dbname_.c_str());
            }
            return 0;   // success
        } else {
            Log(options_.info_log, "[%s] fail to open logfile %s. SwitchLog failed",
                    dbname_.c_str(), log_file_name.c_str());
            if (!blocked_switch) {
                return 2;   // wanted to switch log but failed
            }
        }
    }
    return 1;   // cannot switch log right now
}

void DBTable::ScheduleGarbageClean(double score) {
    mutex_.AssertHeld();
    if (shutting_down_.Acquire_Load()) {
        return;
    }

    if (bg_schedule_gc_ && score <= bg_schedule_gc_score_) {
        return;
    } else if (bg_schedule_gc_) {
        Log(options_.info_log, "[%s] ReSchedule Garbage clean[%ld] score= %.2f",
            dbname_.c_str(), bg_schedule_gc_id_, score);
        env_->ReSchedule(bg_schedule_gc_id_, score);
        bg_schedule_gc_score_ = score;
    } else {
        bg_schedule_gc_id_ = env_->Schedule(&DBTable::GarbageCleanWrapper, this, score);
        bg_schedule_gc_score_ = score;
        bg_schedule_gc_ = true;
        Log(options_.info_log, "[%s] Schedule Garbage clean[%ld] score= %.2f",
            dbname_.c_str(), bg_schedule_gc_id_, score);
    }
}

void DBTable::GarbageCleanWrapper(void* db) {
    DBTable* db_table = reinterpret_cast<DBTable*>(db);
    db_table->BackgroundGarbageClean();
}

void DBTable::BackgroundGarbageClean() {
    if (!shutting_down_.Acquire_Load()) {
        GarbageClean();
    }
    MutexLock lock(&mutex_);
    bg_schedule_gc_ = false;
    bg_cv_.Signal();
}

void DBTable::GarbageClean() {
    uint64_t min_last_seq = -1U;
    bool found = false;
    std::set<uint32_t>::iterator it = options_.exist_lg_list->begin();
    for (; it != options_.exist_lg_list->end(); ++it) {
        assert(*it < lg_list_.size());
        DBImpl* impl = lg_list_[*it];
        uint64_t last_seq = impl->GetLastVerSequence();
        if (last_seq < min_last_seq) {
            min_last_seq = last_seq;
            found = true;
        }
    }
    if (force_clean_log_seq_ > min_last_seq) {
        Log(options_.info_log, "[%s] force_clean_log_seq_= %lu, min_last_seq= %lu",
            dbname_.c_str(), force_clean_log_seq_, min_last_seq);
        min_last_seq = force_clean_log_seq_;
        found = true;
    }

    if (found && min_last_seq > 0) {
        Log(options_.info_log, "[%s] delete obsolete file, seq_no below: %lu",
            dbname_.c_str(), min_last_seq);
        DeleteObsoleteFiles(min_last_seq);
    }
}

} // namespace leveldb
