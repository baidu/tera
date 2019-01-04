// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define private public
#include "db/version_set.h"
#undef private

#include "db/dbformat.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "db/table_cache.h"
#include "leveldb/status.h"
#include "leveldb/compact_strategy.h"
#include "io/default_compact_strategy.h"
#include "io/ttlkv_compact_strategy.h"
#include "proto/test_helper.h"
#include <iostream>

namespace leveldb {

class FindFileTest {
 public:
  std::vector<FileMetaData*> files_;
  bool disjoint_sorted_files_;

  FindFileTest() : disjoint_sorted_files_(true) {}

  ~FindFileTest() {
    for (size_t i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest, SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData;
    f->number = files_.size() + 1;
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
    f->largest = InternalKey(largest, largest_seq, kTypeValue);
    files_.push_back(f);
  }

  int Find(const char* key) {
    InternalKey target(key, 100, kTypeValue);
    InternalKeyComparator cmp(BytewiseComparator());
    return FindFile(cmp, files_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != NULL ? smallest : "");
    Slice l(largest != NULL ? largest : "");
    return SomeFileOverlapsRange(cmp, cmp.user_comparator(), disjoint_sorted_files_, files_,
                                 (smallest != NULL ? &s : NULL), (largest != NULL ? &l : NULL));
  }
};

TEST(FindFileTest, Empty) {
  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(!Overlaps("a", "z"));
  ASSERT_TRUE(!Overlaps(NULL, "z"));
  ASSERT_TRUE(!Overlaps("a", NULL));
  ASSERT_TRUE(!Overlaps(NULL, NULL));
}

TEST(FindFileTest, Single) {
  Add("p", "q");
  ASSERT_EQ(0, Find("a"));
  ASSERT_EQ(0, Find("p"));
  ASSERT_EQ(0, Find("p1"));
  ASSERT_EQ(0, Find("q"));
  ASSERT_EQ(1, Find("q1"));
  ASSERT_EQ(1, Find("z"));

  ASSERT_TRUE(!Overlaps("a", "b"));
  ASSERT_TRUE(!Overlaps("z1", "z2"));
  ASSERT_TRUE(Overlaps("a", "p"));
  ASSERT_TRUE(Overlaps("a", "q"));
  ASSERT_TRUE(Overlaps("a", "z"));
  ASSERT_TRUE(Overlaps("p", "p1"));
  ASSERT_TRUE(Overlaps("p", "q"));
  ASSERT_TRUE(Overlaps("p", "z"));
  ASSERT_TRUE(Overlaps("p1", "p2"));
  ASSERT_TRUE(Overlaps("p1", "z"));
  ASSERT_TRUE(Overlaps("q", "q"));
  ASSERT_TRUE(Overlaps("q", "q1"));

  ASSERT_TRUE(!Overlaps(NULL, "j"));
  ASSERT_TRUE(!Overlaps("r", NULL));
  ASSERT_TRUE(Overlaps(NULL, "p"));
  ASSERT_TRUE(Overlaps(NULL, "p1"));
  ASSERT_TRUE(Overlaps("q", NULL));
  ASSERT_TRUE(Overlaps(NULL, NULL));
}

TEST(FindFileTest, Multiple) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_EQ(0, Find("100"));
  ASSERT_EQ(0, Find("150"));
  ASSERT_EQ(0, Find("151"));
  ASSERT_EQ(0, Find("199"));
  ASSERT_EQ(0, Find("200"));
  ASSERT_EQ(1, Find("201"));
  ASSERT_EQ(1, Find("249"));
  ASSERT_EQ(1, Find("250"));
  ASSERT_EQ(2, Find("251"));
  ASSERT_EQ(2, Find("299"));
  ASSERT_EQ(2, Find("300"));
  ASSERT_EQ(2, Find("349"));
  ASSERT_EQ(2, Find("350"));
  ASSERT_EQ(3, Find("351"));
  ASSERT_EQ(3, Find("400"));
  ASSERT_EQ(3, Find("450"));
  ASSERT_EQ(4, Find("451"));

  ASSERT_TRUE(!Overlaps("100", "149"));
  ASSERT_TRUE(!Overlaps("251", "299"));
  ASSERT_TRUE(!Overlaps("451", "500"));
  ASSERT_TRUE(!Overlaps("351", "399"));

  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
}

TEST(FindFileTest, MultipleNullBoundaries) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_TRUE(!Overlaps(NULL, "149"));
  ASSERT_TRUE(!Overlaps("451", NULL));
  ASSERT_TRUE(Overlaps(NULL, NULL));
  ASSERT_TRUE(Overlaps(NULL, "150"));
  ASSERT_TRUE(Overlaps(NULL, "199"));
  ASSERT_TRUE(Overlaps(NULL, "200"));
  ASSERT_TRUE(Overlaps(NULL, "201"));
  ASSERT_TRUE(Overlaps(NULL, "400"));
  ASSERT_TRUE(Overlaps(NULL, "800"));
  ASSERT_TRUE(Overlaps("100", NULL));
  ASSERT_TRUE(Overlaps("200", NULL));
  ASSERT_TRUE(Overlaps("449", NULL));
  ASSERT_TRUE(Overlaps("450", NULL));
}

TEST(FindFileTest, OverlapSequenceChecks) {
  Add("200", "200", 5000, 3000);
  ASSERT_TRUE(!Overlaps("199", "199"));
  ASSERT_TRUE(!Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST(FindFileTest, OverlappingFiles) {
  Add("150", "600");
  Add("400", "500");
  disjoint_sorted_files_ = false;
  ASSERT_TRUE(!Overlaps("100", "149"));
  ASSERT_TRUE(!Overlaps("601", "700"));
  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
  ASSERT_TRUE(Overlaps("450", "700"));
  ASSERT_TRUE(Overlaps("600", "700"));
}

class VersionSetTest {
 public:
  VersionSetTest() : icmp(opt.comparator), t_log_number(10), t_next_file(20), t_last_seq(100) {
    Logger* logger;
    Env::Default()->NewLogger("/tmp/db_test.log", LogOption::LogOptionBuilder().Build(), &logger);
    Env::Default()->SetLogger(logger);
    opt.info_log = logger;
    opt.compact_strategy_factory = new DummyCompactStrategyFactory();
    opt.env->DeleteDirRecursive("/tmp/db/test");
    opt.env->CreateDir("/tmp/db/test");
    t_vset = new VersionSet(std::string("/tmp/db/test"), &opt, new TableCache(10240), &icmp);
    t_vset->manifest_file_number_ = 100;
  }

  tera::TableSchema SetTableSchema() {
    tera::TableSchema table_s = tera::DefaultTableSchema();
    table_s.set_raw_key(raw_type);
    table_schema = table_s;
    return table_schema;
  }

  struct LightFileMeta {
    int level;
    InternalKey smallest;
    InternalKey largest;
    uint64_t file_size;
  };

  LightFileMeta BuildLightFileMeta(int level, uint64_t file_size, const std::string& row_key,
                                   const std::string& cf, const std::string& qu, int64_t ts,
                                   TeraKeyType type, const std::string& row_key1,
                                   const std::string& cf1, const std::string& qu1, int64_t ts1,
                                   TeraKeyType type1) {
    const leveldb::RawKeyOperator* opt = tera::GetRawKeyOperatorFromSchema(table_schema);
    std::string begin_key, end_key;
    opt->EncodeTeraKey(row_key, cf, qu, ts, type, &begin_key);
    opt->EncodeTeraKey(row_key1, cf1, qu1, ts1, type1, &end_key);
    InternalKey smallest(begin_key, 1, kTypeValue);
    InternalKey largest(end_key, 1, kTypeValue);
    LightFileMeta l;
    l.level = level;
    l.file_size = file_size;
    l.smallest = smallest;
    l.largest = largest;
    return l;
  }

  InternalKey BuildInternalKey(const std::string& row_key, const std::string& cf,
                               const std::string& qu, int64_t ts, TeraKeyType type) {
    const leveldb::RawKeyOperator* opt = tera::GetRawKeyOperatorFromSchema(table_schema);
    std::string begin_key;
    opt->EncodeTeraKey(row_key, cf, qu, ts, type, &begin_key);
    InternalKey smallest(begin_key, 1, kTypeValue);
    return smallest;
  }

  std::string GetInternalKeyStr0(const Slice& user_key, SequenceNumber s, ValueType t) {
    InternalKey ikey(user_key, s, t);
    return ikey.Encode().ToString();
  }

  std::string GetInternalKeyStr(const Slice& user_key, SequenceNumber s, ValueType t) {
    std::string key;
    const leveldb::RawKeyOperator* opt = tera::GetRawKeyOperatorFromSchema(table_schema);
    opt->EncodeTeraKey(user_key.ToString(), "", "", tera::kLatestTs, TKT_FORSEEK, &key);
    InternalKey ikey(Slice(key), s, t);
    return ikey.Encode().ToString();
  }

  void AddVersionToVersionSet(const std::vector<LightFileMeta>& version_metas) {
    VersionEdit edit;
    for (const auto& v : version_metas) {
      edit.AddFile(v.level, t_vset->NewFileNumber(), v.file_size, v.smallest, v.largest);
    }
    edit.SetComparatorName(leveldb::BytewiseComparator()->Name());
    t_mu.Lock();
    t_vset->LogAndApply(&edit, &t_mu);
    t_mu.Unlock();
  }

 public:
  Options opt;
  const InternalKeyComparator icmp;
  VersionSet* t_vset;
  uint64_t t_log_number;
  uint64_t t_next_file;
  uint64_t t_last_seq;
  port::Mutex t_mu;
  tera::RawKey raw_type;
  tera::TableSchema table_schema;
};

TEST(VersionSetTest, PickCompactionTest) {
  VersionEdit edit;

  edit.AddFile(0, t_vset->NewFileNumber(), 200, InternalKey("a0001", 1, kTypeValue),
               InternalKey("a0002", 1, kTypeDeletion));
  edit.AddFile(0, t_vset->NewFileNumber(), 200, InternalKey("a0003", 1, kTypeValue),
               InternalKey("a0004", 1, kTypeValue));
  edit.SetComparatorName(leveldb::BytewiseComparator()->Name());
  t_mu.Lock();
  t_vset->LogAndApply(&edit, &t_mu);
  t_mu.Unlock();
  Compaction* c = t_vset->PickCompaction();
  ASSERT_TRUE((uint64_t)t_vset->level0_compactions_in_progress_[0] == (uint64_t)c);

  VersionEdit edit1;
  edit1.AddFile(0, t_vset->NewFileNumber(), 200, InternalKey("a0005", 1, kTypeValue),
                InternalKey("a0006", 1, kTypeValue));
  edit1.SetComparatorName(leveldb::BytewiseComparator()->Name());
  t_mu.Lock();
  t_vset->LogAndApply(&edit1, &t_mu);
  t_mu.Unlock();
  ASSERT_TRUE(t_vset->PickCompaction() == NULL);
}

/* pick compaction files process test */

TEST(VersionSetTest, ExtendRangeOverlappingIntervalTest0) {
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      {2, InternalKey("a", 1, kTypeValue), InternalKey("b", 2, kTypeValue), 100},
  };
  AddVersionToVersionSet(version_metas);
  int start_index, end_index;
  const Comparator* user_cmp = t_vset->icmp_.user_comparator();
  // level 2 only one file
  t_vset->current_->ExtendRangeOverlappingInterval(2, user_cmp, "a", "b", 0, &start_index,
                                                   &end_index);
  ASSERT_EQ(0, start_index);
  ASSERT_EQ(0, end_index);
}

TEST(VersionSetTest, ExtendRangeOverlappingIntervalTest1) {
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      {1, InternalKey("a", 1, kTypeValue), InternalKey("b", 2, kTypeValue), 100},
      {1, InternalKey("b", 1, kTypeValue), InternalKey("c", 2, kTypeValue), 100},
      {1, InternalKey("c", 1, kTypeValue), InternalKey("d", 2, kTypeValue), 100},
      {1, InternalKey("d", 1, kTypeValue), InternalKey("e", 1, kTypeValue), 100},
  };
  AddVersionToVersionSet(version_metas);
  int start_index, end_index;
  const Comparator* user_cmp = t_vset->icmp_.user_comparator();
  // select from mid_index = 1
  t_vset->current_->ExtendRangeOverlappingInterval(1, user_cmp, "b", "c", 1, &start_index,
                                                   &end_index);
  ASSERT_EQ(0, start_index);
  ASSERT_EQ(2, end_index);
}

TEST(VersionSetTest, ExtendRangeOverlappingIntervalTest2) {
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      {1, InternalKey("a", 1, kTypeValue), InternalKey("aa", 2, kTypeValue), 100},
      {1, InternalKey("b", 1, kTypeValue), InternalKey("c", 2, kTypeValue), 100},
      {1, InternalKey("c1", 1, kTypeValue), InternalKey("d", 2, kTypeValue), 100},
      {1, InternalKey("d", 1, kTypeValue), InternalKey("e", 1, kTypeValue), 100},
  };
  AddVersionToVersionSet(version_metas);
  int start_index, end_index;
  const Comparator* user_cmp = t_vset->icmp_.user_comparator();
  // select from mid_index = 1
  t_vset->current_->ExtendRangeOverlappingInterval(1, user_cmp, "b", "c", 1, &start_index,
                                                   &end_index);
  ASSERT_EQ(1, start_index);
  ASSERT_EQ(1, end_index);
}

TEST(VersionSetTest, ExtendRangeWithinIntervalTest0) {
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      {1, InternalKey("a", 1, kTypeValue), InternalKey("aa", 2, kTypeValue), 100},
      {1, InternalKey("b", 1, kTypeValue), InternalKey("c", 2, kTypeValue), 100},
      {1, InternalKey("c1", 1, kTypeValue), InternalKey("d", 2, kTypeValue), 100},
      {1, InternalKey("d", 1, kTypeValue), InternalKey("e", 1, kTypeValue), 100},
  };
  AddVersionToVersionSet(version_metas);
  int start_index, end_index;
  const Comparator* user_cmp = t_vset->icmp_.user_comparator();
  // select from mid_index = 1
  t_vset->current_->ExtendRangeWithinInterval(1, user_cmp, "b", "c", 1, &start_index, &end_index);
  ASSERT_EQ(1, start_index);
  ASSERT_EQ(1, end_index);
}

TEST(VersionSetTest, ExtendRangeWithinIntervalTest1) {
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      {1, InternalKey("a", 1, kTypeValue), InternalKey("b", 2, kTypeValue), 100},
      {1, InternalKey("b", 1, kTypeValue), InternalKey("c", 2, kTypeValue), 100},
      {1, InternalKey("c", 1, kTypeValue), InternalKey("d", 2, kTypeValue), 100},
      {1, InternalKey("d", 1, kTypeValue), InternalKey("e", 1, kTypeValue), 100},
  };
  AddVersionToVersionSet(version_metas);
  int start_index, end_index;
  const Comparator* user_cmp = t_vset->icmp_.user_comparator();
  // select from mid_index = 1
  t_vset->current_->ExtendRangeWithinInterval(1, user_cmp, "b", "c", 1, &start_index, &end_index);
  ASSERT_EQ(3, start_index);
  ASSERT_EQ(2, end_index);
}

TEST(VersionSetTest, GetCleanCutInputsWithinInterval) {
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      {1, InternalKey("a", 1, kTypeValue), InternalKey("b", 2, kTypeValue), 100},
      {1, InternalKey("b", 1, kTypeValue), InternalKey("c", 2, kTypeValue), 100},
      {1, InternalKey("c", 1, kTypeValue), InternalKey("d", 2, kTypeValue), 100},
      {1, InternalKey("d", 1, kTypeValue), InternalKey("e", 1, kTypeValue), 100},
  };
  AddVersionToVersionSet(version_metas);
  // select from mid_index = 1
  InternalKey begin = InternalKey("b", 1, kTypeValue);
  InternalKey end = InternalKey("c", 2, kTypeValue);
  std::vector<FileMetaData*> inputs;
  // not found clean cut
  t_vset->current_->GetCleanCutInputsWithinInterval(1, &begin, &end, &inputs);
  ASSERT_EQ(0, inputs.size());
}

TEST(VersionSetTest, GetCleanCutInputsWithinInterval1) {
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      {1, InternalKey("a", 1, kTypeValue), InternalKey("aa", 2, kTypeValue), 100},
      {1, InternalKey("b", 1, kTypeValue), InternalKey("c", 2, kTypeValue), 100},
      {1, InternalKey("c1", 1, kTypeValue), InternalKey("d", 2, kTypeValue), 100},
      {1, InternalKey("d", 1, kTypeValue), InternalKey("e", 1, kTypeValue), 100},
  };
  AddVersionToVersionSet(version_metas);
  // select from mid_index = 1
  InternalKey begin = InternalKey("b", 1, kTypeValue);
  InternalKey end = InternalKey("c", 2, kTypeValue);
  std::vector<FileMetaData*> inputs;
  // not found clean cut
  t_vset->current_->GetCleanCutInputsWithinInterval(1, &begin, &end, &inputs);
  ASSERT_EQ(1, inputs.size());
}

TEST(VersionSetTest, GenerateSubCompaction0) {  // DummyCompactStrategyFactory
  opt.compact_strategy_factory = new DummyCompactStrategyFactory();
  std::vector<LightFileMeta> version_metas = {
      {1, InternalKey("a", 1, kTypeValue), InternalKey("z", 1, kTypeValue), 100},
      {2, InternalKey("b", 1, kTypeValue), InternalKey("c", 1, kTypeValue), 100},
      {2, InternalKey("d", 1, kTypeValue), InternalKey("e", 1, kTypeValue), 100},
      {2, InternalKey("f", 1, kTypeValue), InternalKey("g", 1, kTypeValue), 100},
      {2, InternalKey("h", 1, kTypeValue), InternalKey("i", 1, kTypeValue), 100},
  };
  AddVersionToVersionSet(version_metas);
  std::vector<FileMetaData*> inputs = t_vset->current_->files_[1];
  std::vector<FileMetaData*> output_inputs = t_vset->current_->files_[2];

  Compaction* c = new Compaction(1);  // level 1
  c->SetNonTrivial(true);
  c->input_version_ = t_vset->current_;
  c->input_version_->Ref();
  c->set_output_level(2);  // output level 2
  c->max_output_file_size_ = 200;
  c->inputs_[0] = inputs;
  c->inputs_[1] = output_inputs;
  c->smallest_internal_key_ = InternalKey("a", 1, kTypeValue);
  c->largest_internal_key_ = InternalKey("z", 1, kTypeValue);

  t_mu.Lock();
  std::vector<Compaction*> compaction_vec;
  t_vset->GenerateSubCompaction(c, &compaction_vec, &t_mu);
  t_mu.Unlock();
  // check
  ASSERT_EQ(2, compaction_vec.size());
  ASSERT_EQ((compaction_vec[0])->sub_compact_end_,
            GetInternalKeyStr0("f", kMaxSequenceNumber, kValueTypeForSeek));
  ASSERT_EQ((compaction_vec[1])->sub_compact_start_,
            GetInternalKeyStr0("f", kMaxSequenceNumber, kValueTypeForSeek));

  c->input_version_->Unref();
}

TEST(VersionSetTest, GenerateSubCompaction1) {  // DefaultCompactStrategyFactory
  opt.compact_strategy_factory =
      new tera::io::DefaultCompactStrategyFactory(tera::DefaultTableSchema());
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      BuildLightFileMeta(1, 100, "a", "cf", "qu", 123, TKT_VALUE, "z", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "b", "cf", "qu", 123, TKT_VALUE, "c", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "d", "cf", "qu", 123, TKT_VALUE, "e", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "f", "cf", "qu", 123, TKT_VALUE, "g", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "h", "cf", "qu", 123, TKT_VALUE, "i", "cf", "qu", 123, TKT_VALUE),
  };
  AddVersionToVersionSet(version_metas);
  std::vector<FileMetaData*> inputs = t_vset->current_->files_[1];
  std::vector<FileMetaData*> output_inputs = t_vset->current_->files_[2];

  Compaction* c = new Compaction(1);  // level 1
  c->SetNonTrivial(true);
  c->input_version_ = t_vset->current_;
  c->input_version_->Ref();
  c->set_output_level(2);  // output level 2
  c->max_output_file_size_ = 200;
  c->inputs_[0] = inputs;
  c->inputs_[1] = output_inputs;
  c->smallest_internal_key_ = BuildInternalKey("a", "cf", "qu", 123, TKT_VALUE);
  c->largest_internal_key_ = BuildInternalKey("z", "cf", "qu", 123, TKT_VALUE);

  t_mu.Lock();
  std::vector<Compaction*> compaction_vec;
  t_vset->GenerateSubCompaction(c, &compaction_vec, &t_mu);
  t_mu.Unlock();
  // check
  ASSERT_EQ(2, compaction_vec.size());
  ASSERT_EQ((compaction_vec[0])->sub_compact_end_,
            GetInternalKeyStr("f", kMaxSequenceNumber, kValueTypeForSeek));
  ASSERT_EQ((compaction_vec[1])->sub_compact_start_,
            GetInternalKeyStr("f", kMaxSequenceNumber, kValueTypeForSeek));

  c->input_version_->Unref();
}

TEST(VersionSetTest, GenerateSubCompaction2) {  // DefaultCompactStrategyFactory with del mark
  raw_type = tera::RawKey::Binary;
  opt.compact_strategy_factory = new tera::io::DefaultCompactStrategyFactory(SetTableSchema());
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      BuildLightFileMeta(1, 100, "a", "cf", "qu", 123, TKT_VALUE, "p", "", "", 123, TKT_DEL),
      BuildLightFileMeta(1, 100, "p", "cf", "qu2", 123, TKT_VALUE, "z", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "b", "cf", "qu", 123, TKT_VALUE, "c", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "d", "cf", "qu", 123, TKT_VALUE, "e", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "f", "cf", "qu", 123, TKT_VALUE, "g", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "h", "cf", "qu", 123, TKT_VALUE, "i", "cf", "qu", 123, TKT_VALUE),
  };
  AddVersionToVersionSet(version_metas);
  std::vector<FileMetaData*> inputs = t_vset->current_->files_[1];
  std::vector<FileMetaData*> output_inputs = t_vset->current_->files_[2];

  Compaction* c = new Compaction(1);  // level 1
  c->SetNonTrivial(true);
  c->input_version_ = t_vset->current_;
  c->input_version_->Ref();
  c->set_output_level(2);  // output level 2
  c->max_output_file_size_ = 200;
  c->inputs_[0] = inputs;
  c->inputs_[1] = output_inputs;
  c->smallest_internal_key_ = BuildInternalKey("a", "cf", "qu", 123, TKT_VALUE);
  c->largest_internal_key_ = BuildInternalKey("z", "cf", "qu", 123, TKT_VALUE);

  t_mu.Lock();
  std::vector<Compaction*> compaction_vec;
  t_vset->GenerateSubCompaction(c, &compaction_vec, &t_mu);
  t_mu.Unlock();
  // check
  ASSERT_EQ(3, compaction_vec.size());
  ASSERT_EQ((compaction_vec[0])->sub_compact_end_,
            GetInternalKeyStr("f", kMaxSequenceNumber, kValueTypeForSeek));
  ASSERT_EQ((compaction_vec[1])->sub_compact_start_,
            GetInternalKeyStr("f", kMaxSequenceNumber, kValueTypeForSeek));
  ASSERT_EQ((compaction_vec[1])->sub_compact_end_,
            GetInternalKeyStr("p", kMaxSequenceNumber, kValueTypeForSeek));
  ASSERT_EQ((compaction_vec[2])->sub_compact_start_,
            GetInternalKeyStr("p", kMaxSequenceNumber, kValueTypeForSeek));

  c->input_version_->Unref();
}

TEST(VersionSetTest, GenerateSubCompaction3) {  // DefaultCompactStrategyFactory with del mark
  raw_type = tera::RawKey::Readable;
  opt.compact_strategy_factory = new tera::io::DefaultCompactStrategyFactory(SetTableSchema());
  std::vector<LightFileMeta> version_metas = {
      /* level, smallest, largest, file_size */
      BuildLightFileMeta(1, 100, "a", "cf", "qu", 123, TKT_VALUE, "p", "", "", 123, TKT_DEL),
      BuildLightFileMeta(1, 100, "p", "cf", "qu2", 123, TKT_VALUE, "z", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "b", "cf", "qu", 123, TKT_VALUE, "c", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "d", "cf", "qu", 123, TKT_VALUE, "e", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "f", "cf", "qu", 123, TKT_VALUE, "g", "cf", "qu", 123, TKT_VALUE),
      BuildLightFileMeta(2, 100, "h", "cf", "qu", 123, TKT_VALUE, "i", "cf", "qu", 123, TKT_VALUE),
  };
  AddVersionToVersionSet(version_metas);
  std::vector<FileMetaData*> inputs = t_vset->current_->files_[1];
  std::vector<FileMetaData*> output_inputs = t_vset->current_->files_[2];

  Compaction* c = new Compaction(1);  // level 1
  c->SetNonTrivial(true);
  c->input_version_ = t_vset->current_;
  c->input_version_->Ref();
  c->set_output_level(2);  // output level 2
  c->max_output_file_size_ = 200;
  c->inputs_[0] = inputs;
  c->inputs_[1] = output_inputs;
  c->smallest_internal_key_ = BuildInternalKey("a", "cf", "qu", 123, TKT_VALUE);
  c->largest_internal_key_ = BuildInternalKey("z", "cf", "qu", 123, TKT_VALUE);

  t_mu.Lock();
  std::vector<Compaction*> compaction_vec;
  t_vset->GenerateSubCompaction(c, &compaction_vec, &t_mu);
  t_mu.Unlock();
  // check
  ASSERT_EQ(3, compaction_vec.size());
  ASSERT_EQ((compaction_vec[0])->sub_compact_end_,
            GetInternalKeyStr("f", kMaxSequenceNumber, kValueTypeForSeek));
  ASSERT_EQ((compaction_vec[1])->sub_compact_start_,
            GetInternalKeyStr("f", kMaxSequenceNumber, kValueTypeForSeek));
  ASSERT_EQ((compaction_vec[1])->sub_compact_end_,
            GetInternalKeyStr("p", kMaxSequenceNumber, kValueTypeForSeek));
  ASSERT_EQ((compaction_vec[2])->sub_compact_start_,
            GetInternalKeyStr("p", kMaxSequenceNumber, kValueTypeForSeek));

  c->input_version_->Unref();
}

TEST(VersionSetTest, OldDescriptorAndNewDescriptorCompatible) {
  VersionEdit edit;

  // old descriptor
  edit.SetComparatorName(leveldb::BytewiseComparator()->Name());

  t_mu.Lock();
  t_vset->LogAndApply(&edit, &t_mu);
  t_mu.Unlock();
  Status s = t_vset->Recover();
  ASSERT_TRUE(s.ok());

  // new descriptor
  edit.SetStartKey("user1");
  edit.SetEndKey("user100");
  t_mu.Lock();
  t_vset->LogAndApply(&edit, &t_mu);
  t_mu.Unlock();

  s = t_vset->Recover();
  ASSERT_TRUE(s.ok());
}

}  // namespace leveldb

int main(int argc, char** argv) { return leveldb::test::RunAllTests(); }
