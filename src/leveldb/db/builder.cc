// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/compact_strategy.h"

namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta,
                  uint64_t* saved_size,
                  uint64_t smallest_snapshot) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }
    SequenceNumber snapshot = smallest_snapshot;

    CompactStrategy* compact_strategy = NULL;
    if (options.compact_strategy_factory) {
      compact_strategy = options.compact_strategy_factory->NewInstance();
      compact_strategy->SetSnapshot(snapshot);
    }

    // meta->smallest和meta->largest的范围可以向两侧伸长,但如果比实际范围小就是bug.
    // 这里暂且根据drop结果收窄largest而不收窄smallest,保证足够简单可靠.
    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (;iter->Valid();) {
      Slice key = iter->key();  // no-length-prefix-key

      const char* entry = key.data();
      Slice raw_key(entry, key.size() - 8);

      const uint64_t tag = DecodeFixed64(entry + key.size() - 8);
      const uint64_t sequence_id = tag >> 8;
      bool has_atom_merged = false;

      // type==kTypeValue, 且drop==true的记录可以被丢弃,
      // 其他记录均正常进入Memtable compact SST流程.
      if (static_cast<ValueType>(tag & 0xff) == kTypeValue && compact_strategy) {
        bool drop = compact_strategy->Drop(raw_key, sequence_id);
        if (drop) {
            iter->Next();
//             Log(options.info_log, "[Memtable Drop] sequence_id: %llu, raw_key: %s",
//                     sequence_id, entry);
            continue;   // drop it before build
        }
        else {
            std::string merged_value;
            std::string merged_key;
            has_atom_merged = compact_strategy->MergeAtomicOPs(iter, &merged_value,
                    &merged_key);
            if (has_atom_merged) {
                meta->largest.DecodeFrom(Slice(merged_key));
                builder->Add(Slice(merged_key), Slice(merged_value));
            }
        }
      }
      // 极端情况下, 整个Memtable全部被Drop(与strategy实现相关), 那么就不需要生成
      // SST了, 也不需要修改manifest, 当作什么也没发生过.
      if (!has_atom_merged) {
          meta->largest.DecodeFrom(key);
          builder->Add(key, iter->value());
          iter->Next();
      }
      //Log(options.info_log, "[Memtable Not Drop] sequence_id: %llu, raw_key: %s", sequence_id, entry);
    }

    if (compact_strategy) {
        delete compact_strategy;
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();
      *saved_size = 0;
      if (s.ok() && builder->NumEntries()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
        *saved_size = builder->SavedSize();
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (s.ok() && meta->file_size) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(&options),
                                              dbname,
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
