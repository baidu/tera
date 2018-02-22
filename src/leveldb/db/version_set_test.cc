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
#include "leveldb/compact_strategy.h"

namespace leveldb {

class FindFileTest {
 public:
  std::vector<FileMetaData*> files_;
  bool disjoint_sorted_files_;

  FindFileTest() : disjoint_sorted_files_(true) { }

  ~FindFileTest() {
    for (size_t i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
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
                                 (smallest != NULL ? &s : NULL),
                                 (largest != NULL ? &l : NULL));
  }
};

TEST(FindFileTest, Empty) {
  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(! Overlaps("a", "z"));
  ASSERT_TRUE(! Overlaps(NULL, "z"));
  ASSERT_TRUE(! Overlaps("a", NULL));
  ASSERT_TRUE(! Overlaps(NULL, NULL));
}

TEST(FindFileTest, Single) {
  Add("p", "q");
  ASSERT_EQ(0, Find("a"));
  ASSERT_EQ(0, Find("p"));
  ASSERT_EQ(0, Find("p1"));
  ASSERT_EQ(0, Find("q"));
  ASSERT_EQ(1, Find("q1"));
  ASSERT_EQ(1, Find("z"));

  ASSERT_TRUE(! Overlaps("a", "b"));
  ASSERT_TRUE(! Overlaps("z1", "z2"));
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

  ASSERT_TRUE(! Overlaps(NULL, "j"));
  ASSERT_TRUE(! Overlaps("r", NULL));
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

  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("251", "299"));
  ASSERT_TRUE(! Overlaps("451", "500"));
  ASSERT_TRUE(! Overlaps("351", "399"));

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
  ASSERT_TRUE(! Overlaps(NULL, "149"));
  ASSERT_TRUE(! Overlaps("451", NULL));
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
  ASSERT_TRUE(! Overlaps("199", "199"));
  ASSERT_TRUE(! Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST(FindFileTest, OverlappingFiles) {
  Add("150", "600");
  Add("400", "500");
  disjoint_sorted_files_ = false;
  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("601", "700"));
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
    VersionSetTest ()
      : icmp(opt.comparator),
        t_log_number(10),
        t_next_file(20),
        t_last_seq(100) {
      opt.compact_strategy_factory = new DummyCompactStrategyFactory();
      opt.env->DeleteDirRecursive("/tmp/db/test");
      opt.env->CreateDir("/tmp/db/test");
      t_vset = new VersionSet(std::string("/tmp/db/test"), &opt, NULL, &icmp);
      t_vset->manifest_file_number_ = 100;
    }

public:
    Options opt;
    const InternalKeyComparator icmp;
    VersionSet* t_vset;
    uint64_t t_log_number;
    uint64_t t_next_file;
    uint64_t t_last_seq;
    port::Mutex t_mu;
};

TEST(VersionSetTest, PickCompactionTest) {
  VersionEdit edit;

  edit.AddFile(0, t_vset->NewFileNumber(), 200,
               InternalKey("a0001", 1, kTypeValue),
               InternalKey("a0002", 1, kTypeDeletion));
  edit.AddFile(0, t_vset->NewFileNumber(), 200,
               InternalKey("a0003", 1, kTypeValue),
               InternalKey("a0004", 1, kTypeValue));
  edit.SetComparatorName(leveldb::BytewiseComparator()->Name());
  t_mu.Lock();
  t_vset->LogAndApply(&edit, &t_mu);
  t_mu.Unlock();
  Compaction* c = t_vset->PickCompaction();
  ASSERT_TRUE((uint64_t)t_vset->level0_compactions_in_progress_[0] == (uint64_t)c);

  VersionEdit edit1;
  edit1.AddFile(0, t_vset->NewFileNumber(), 200,
               InternalKey("a0005", 1, kTypeValue),
               InternalKey("a0006", 1, kTypeValue));
  edit1.SetComparatorName(leveldb::BytewiseComparator()->Name());
  t_mu.Lock();
  t_vset->LogAndApply(&edit1, &t_mu);
  t_mu.Unlock();
  ASSERT_TRUE(t_vset->PickCompaction() == NULL);
}

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
