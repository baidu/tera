// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>

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
  int64_t del_num = 0;         // statistic: delete tag's percentage in sst
  std::vector<int64_t> ttls; // use for calculate timeout percentage
  int64_t entries = 0;
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

    ParsedInternalKey ikey;
    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (;iter->Valid();) {
      Slice key = iter->key();  // no-length-prefix-key
      assert(ParseInternalKey(key, &ikey));

      bool has_atom_merged = false;
      if (ikey.type == kTypeValue && compact_strategy && ikey.sequence <= snapshot) {
        bool drop = compact_strategy->Drop(ikey.user_key, ikey.sequence);
        if (drop) {
          iter->Next();
          // Log(options.info_log, "[Memtable Drop] sequence_id: %llu, raw_key: %s",
          //     ikey.sequence, ikey.user_key);
          continue;   // drop it before build
        } else {
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

      if (!has_atom_merged) {
        bool del_tag = false;
        int64_t ttl = -1;
        compact_strategy && compact_strategy->CheckTag(ikey.user_key, &del_tag, &ttl);
        if (ikey.type == kTypeDeletion || del_tag) {
          //Log(options_.info_log, "[%s] add del_tag %d, key_type %d\n",
          //    dbname_.c_str(), del_tag, ikey.type);
          del_num++;
        } else if (ttl > 0) { // del tag has not ttl
          //Log(options_.info_log, "[%s] add ttl_tag %ld\n",
          //    dbname_.c_str(), ttl);
          ttls.push_back(ttl);
        }

        meta->largest.DecodeFrom(key);
        builder->Add(key, iter->value());
        iter->Next();
      }
      // Log(options.info_log, "[Memtable Not Drop] sequence_id: %llu, raw_key: %s",
      //     ikey.sequence, ikey.user_key);
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

        // update ttl/del information
        entries = builder->NumEntries();
        std::sort(ttls.begin(), ttls.end());
        uint32_t idx = ttls.size() * options.ttl_percentage / 100 ;
        meta->del_percentage = del_num * 100 / entries; /* delete tag percentage */
        meta->check_ttl_ts = ((ttls.size() > 0) && (idx < ttls.size())) ? ttls[idx] : 0; /* sst's check ttl's time */
        meta->ttl_percentage = ((ttls.size() > 0) && (idx < ttls.size())) ? idx * 100 / ttls.size() : 0; /* ttl tag percentage */
        Log(options.info_log, "[%s] (mem dump) AddFile, number #%u, entries %ld, del_nr %lu"
                             ", ttl_nr %lu, del_p %lu, ttl_check_ts %lu, ttl_p %lu\n",
          dbname.c_str(),
          (unsigned int) meta->number,
          entries,
          del_num,
          ttls.size(),
          meta->del_percentage,
          meta->check_ttl_ts,
          meta->ttl_percentage);
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
