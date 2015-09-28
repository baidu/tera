// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Junyi Sun (sunjunyi01@baidu.com)
// Description: memtable built on leveldb

#include "memtable_on_leveldb.h"

#include <stdlib.h>
#include <unistd.h>
#include "db/db_impl.h"
#include "db/write_batch_internal.h"
#include "leveldb/write_batch.h"

namespace leveldb {

MemTableOnLevelDB::MemTableOnLevelDB(const InternalKeyComparator& comparator,
                                     CompactStrategyFactory* compact_strategy_factory,
                                     size_t write_buffer_size,
                                     size_t block_size)
                                     : MemTable(comparator, compact_strategy_factory) {
    char memdb_name[1024] = { '\0' };
    snprintf(memdb_name, sizeof(memdb_name), "/%d/%llu", getpid(),
             (unsigned long long)this);
    leveldb::Options opts;
    opts.env = memenv_ = leveldb::NewMemEnv(GetBaseEnv());
    opts.compression = leveldb::kSnappyCompression;
    opts.write_buffer_size = write_buffer_size;
    opts.block_size = block_size;
    opts.compact_strategy_factory = compact_strategy_factory;
    opts.comparator = comparator.user_comparator();
    opts.dump_mem_on_shutdown = false;
    opts.drop_base_level_del_in_compaction = false;
    opts.info_log = NULL;

    DBImpl* db_impl = new DBImpl(opts, memdb_name);
    VersionEdit edit;
    Status s = db_impl->Recover(&edit);
    assert(s.ok());
    memdb_ = db_impl;
}

MemTableOnLevelDB::~MemTableOnLevelDB() {
    if (memdb_) {
        memdb_->Shutdown1();
        memdb_->Shutdown2();
        delete memdb_;
    }
    delete memenv_;

}

size_t MemTableOnLevelDB::ApproximateMemoryUsage() {
    uint64_t size = memdb_->GetScopeSize("", "");
    return size;
}

Iterator* MemTableOnLevelDB::NewIterator() {
    return memdb_->NewInternalIterator();
}

void MemTableOnLevelDB::Add(SequenceNumber seq,
                            ValueType type,
                            const Slice& key,
                            const Slice& value) {
    WriteBatch batch;
    if (type == kTypeValue) {
        batch.Put(key, value);
    } else if (type == kTypeDeletion) {
        batch.Delete(key);
    }
    WriteBatchInternal::SetSequence(&batch, seq);
    memdb_->Write(WriteOptions(), &batch);
    assert(last_seq_ < seq || seq == 0);
    last_seq_ = seq;
}

bool MemTableOnLevelDB::Get(const LookupKey& key,
                            std::string* value,
                            Status* s) {
    ReadOptions read_opt;
    ParsedInternalKey internal_key_data;
    ParseInternalKey(key.internal_key(), &internal_key_data);
    read_opt.snapshot = internal_key_data.sequence;
    *s = memdb_->Get(read_opt, key.user_key(), value);
    return s->ok();
}

const uint64_t MemTableOnLevelDB::GetSnapshot(uint64_t last_sequence) {
    return memdb_->GetSnapshot(last_sequence);
}

void MemTableOnLevelDB::ReleaseSnapshot(uint64_t sequence_number) {
    return memdb_->ReleaseSnapshot(sequence_number);
}

static pthread_once_t mem_base_env_once = PTHREAD_ONCE_INIT;
static Env* mem_base_env;
static void MakeMemBaseEnv() { mem_base_env = NewPosixEnv(); }

Env* MemTableOnLevelDB::GetBaseEnv() {
  pthread_once(&mem_base_env_once, MakeMemBaseEnv);
  return mem_base_env;
}

} //end namespace leveldb

