// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/testharness.h"
#include "db/db_impl.h"

namespace leveldb {

class EnvForVeriyDbInGetPropertyCase1 : public EnvWrapper {
 public:
  EnvForVeriyDbInGetPropertyCase1() : EnvWrapper(Env::Default()) {}

  virtual Status GetChildren(const std::string& dir, std::vector<std::string>* r) {
    return Status::OK();
  }
  virtual Status GetFileSize(const std::string& f, uint64_t* s) {
    *s = 789;
    return Status::OK();
  }
  void SetDBImpl(DBImpl* impl) { impl_ = impl; }

 public:
  DBImpl* impl_;
};

class EnvForVeriyDbInGetPropertyCase2 : public EnvWrapper {
 public:
  EnvForVeriyDbInGetPropertyCase2() : EnvWrapper(Env::Default()) {}

  virtual Status GetChildren(const std::string& dir, std::vector<std::string>* r) {
    r->push_back("456.sst");
    impl_->shutting_down_.Release_Store(impl_);
    return Status::OK();
  }
  virtual Status GetFileSize(const std::string& f, uint64_t* s) {
    *s = 789;
    return Status::OK();
  }
  void SetDBImpl(DBImpl* impl) { impl_ = impl; }

 public:
  DBImpl* impl_;
};

class EnvForVeriyDbInGetPropertyCase3 : public EnvWrapper {
 public:
  EnvForVeriyDbInGetPropertyCase3() : EnvWrapper(Env::Default()) {}

  virtual Status GetChildren(const std::string& dir, std::vector<std::string>* r) {
    r->push_back("456.sst");
    return Status::OK();
  }
  virtual Status GetFileSize(const std::string& f, uint64_t* s) {
    *s = 789;
    return Status::TimeOut("timeout");
  }
  void SetDBImpl(DBImpl* impl) { impl_ = impl; }

 public:
  DBImpl* impl_;
};

class DBImplTest {
 public:
  void init(DBImpl* impl) {
    Version* v = new Version(impl->versions_);
    FileMetaData* f = new FileMetaData;
    f->refs++;
    f->number = (1UL << 63 | 123UL << 32 | 456);
    f->file_size = 789;
    f->smallest = InternalKey("", 0, kTypeValue);
    f->largest = InternalKey("", 0, kTypeValue);
    (v->files_[0]).push_back(f);
    impl->versions_->AppendVersion(v);
  }
  DBImplTest() {}

  ~DBImplTest() {}
};

TEST(DBImplTest, VeriyDbInGetPropertyWhenShuttingDownCase1) {
  Options opt;
  EnvForVeriyDbInGetPropertyCase1* env = new EnvForVeriyDbInGetPropertyCase1;
  opt.env = env;
  DBImpl* impl = new DBImpl(opt, "test_table/tablet000123/1");
  env->SetDBImpl(impl);
  init(impl);
  std::string db_property_key = "leveldb.verify-db-integrity";
  std::string db_property_val;
  impl->shutting_down_.Release_Store(impl);
  ASSERT_EQ(impl->GetProperty(db_property_key, &db_property_val), false);
  delete opt.env;
  delete impl;
}

TEST(DBImplTest, VeriyDbInGetPropertyWhenShuttingDownCase2) {
  Options opt;
  EnvForVeriyDbInGetPropertyCase2* env = new EnvForVeriyDbInGetPropertyCase2;
  opt.env = env;
  DBImpl* impl = new DBImpl(opt, "test_table/tablet000123/1");
  env->SetDBImpl(impl);
  init(impl);
  std::string db_property_key = "leveldb.verify-db-integrity";
  std::string db_property_val;
  ASSERT_EQ(impl->GetProperty(db_property_key, &db_property_val), false);
  delete opt.env;
  delete impl;
}

TEST(DBImplTest, VeriyDbInGetPropertyWhenShuttingDownCase3) {
  Options opt;
  EnvForVeriyDbInGetPropertyCase3* env = new EnvForVeriyDbInGetPropertyCase3;
  opt.env = env;
  DBImpl* impl = new DBImpl(opt, "test_table/tablet000123/1");
  env->SetDBImpl(impl);
  init(impl);
  std::string db_property_key = "leveldb.verify-db-integrity";
  std::string db_property_val;
  ASSERT_EQ(impl->GetProperty(db_property_key, &db_property_val), false);
  delete opt.env;
  delete impl;
}

}  // namespace leveldb

int main(int argc, char** argv) { return leveldb::test::RunAllTests(); }
