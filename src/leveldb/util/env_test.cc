// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"
#include "leveldb/slog.h"
#include "port/port.h"
#include "util/string_ext.h"
#include "util/testharness.h"
#include "util/testutil.h"

#define private public
#include "util/env_posix.h"
#undef private

namespace leveldb {

static const int kDelayMicros = 100000;

class PosixWritableFileTest {
 public:
  Env* env_;
  PosixWritableFile* file_ = nullptr;
  const std::string kTmpFileName = "/tmp/posix_writable_test_file";

 public:
  PosixWritableFileTest() : env_(Env::Default()) {
    EnvOptions env_opt;
    env_opt.use_direct_io_write = true;
    env_opt.posix_write_buffer_size = 256 << 10;
    WritableFile* file;
    env_->NewWritableFile(kTmpFileName, &file, env_opt);
    if (file) {
      file_ = dynamic_cast<PosixWritableFile*>(file);
    }
  }

  ~PosixWritableFileTest() { env_->DeleteFile(kTmpFileName); }
};

TEST(PosixWritableFileTest, PosixWriteTest) {
  std::string content1((1 << 20) + 50, 'k');
  file_->Append(content1);
  uint64_t size(0);
  env_->GetFileSize(kTmpFileName, &size);
  ASSERT_EQ(size, 1 << 20);
  ASSERT_EQ(file_->pos_, 50);
  ASSERT_TRUE(file_->is_dio_);

  std::string content2((1 << 20) + 80, 't');
  file_->Append(content2);
  env_->GetFileSize(kTmpFileName, &size);
  ASSERT_EQ(size, 1 << 21);
  ASSERT_EQ(file_->pos_, 130);
  ASSERT_TRUE(file_->is_dio_);

  std::string content3(file_->align_size_ - 130, 'p');
  file_->Append(content3);
  env_->GetFileSize(kTmpFileName, &size);
  ASSERT_EQ(size, 1 << 21);
  ASSERT_EQ(file_->pos_, file_->align_size_);
  ASSERT_TRUE(file_->is_dio_);

  file_->Flush();
  env_->GetFileSize(kTmpFileName, &size);
  ASSERT_EQ(size, (1 << 21) + file_->align_size_);
  ASSERT_EQ(file_->pos_, 0);
  ASSERT_TRUE(file_->is_dio_);

  std::string content4(30, 'p');
  file_->Append(content4);
  env_->GetFileSize(kTmpFileName, &size);
  ASSERT_EQ(size, (1 << 21) + file_->align_size_);
  ASSERT_EQ(file_->pos_, 30);
  ASSERT_TRUE(file_->is_dio_);

  file_->Flush();
  env_->GetFileSize(kTmpFileName, &size);
  ASSERT_EQ(size, (1 << 21) + file_->align_size_ + 30);
  ASSERT_EQ(file_->pos_, 0);
  ASSERT_TRUE(!file_->is_dio_);

  ASSERT_TRUE(file_->Close().ok());
  RandomAccessFile* rfile;
  EnvOptions env_opt;
  env_opt.use_direct_io_read = true;
  env_->NewRandomAccessFile(kTmpFileName, &rfile, env_opt);
  Slice result;
  char* buf = new char[30 << 20];
  rfile->Read(0, 30 << 20, &result, buf);
  ASSERT_EQ(content1.size() + content2.size() + content3.size() + content4.size(), result.size());
  ASSERT_EQ(content1 + content2 + content3 + content4, result.ToString());

  delete[] buf;
}

class EnvPosixTest {
 private:
  port::Mutex mu_;
  std::string events_;

 public:
  Env* env_;
  EnvPosixTest() : env_(Env::Default()) {}
};

static void SetBool(void* ptr) {
  reinterpret_cast<port::AtomicPointer*>(ptr)->NoBarrier_Store(ptr);
}

TEST(EnvPosixTest, RunImmediately) {
  port::AtomicPointer called(NULL);
  env_->Schedule(&SetBool, &called);
  Env::Default()->SleepForMicroseconds(kDelayMicros);
  ASSERT_TRUE(called.NoBarrier_Load() != NULL);
}

TEST(EnvPosixTest, RunMany) {
  port::AtomicPointer last_id(NULL);

  struct CB {
    port::AtomicPointer* last_id_ptr;  // Pointer to shared slot
    uintptr_t id;                      // Order# for the execution of this callback

    CB(port::AtomicPointer* p, int i) : last_id_ptr(p), id(i) {}

    static void Run(void* v) {
      CB* cb = reinterpret_cast<CB*>(v);
      void* cur = cb->last_id_ptr->NoBarrier_Load();
      ASSERT_EQ(cb->id - 1, reinterpret_cast<uintptr_t>(cur));
      cb->last_id_ptr->Release_Store(reinterpret_cast<void*>(cb->id));
    }
  };

  Env* env = NewPosixEnv();
  env->SetBackgroundThreads(1);
  // Schedule in different order than start time
  CB cb1(&last_id, 1);
  CB cb2(&last_id, 2);
  CB cb3(&last_id, 3);
  CB cb4(&last_id, 4);
  env->Schedule(&CB::Run, &cb1);
  env->Schedule(&CB::Run, &cb2);
  env->Schedule(&CB::Run, &cb3);
  env->Schedule(&CB::Run, &cb4);

  Env::Default()->SleepForMicroseconds(kDelayMicros);
  void* cur = last_id.Acquire_Load();
  ASSERT_EQ(4u, reinterpret_cast<uintptr_t>(cur));
  delete env;
}

struct State {
  port::Mutex mu;
  int val;
  int num_running;
};

static void ThreadBody(void* arg) {
  State* s = reinterpret_cast<State*>(arg);
  s->mu.Lock();
  s->val += 1;
  s->num_running -= 1;
  s->mu.Unlock();
}

TEST(EnvPosixTest, StartThread) {
  State state;
  state.val = 0;
  state.num_running = 3;
  for (int i = 0; i < 3; i++) {
    env_->StartThread(&ThreadBody, &state);
  }
  while (true) {
    state.mu.Lock();
    int num = state.num_running;
    state.mu.Unlock();
    if (num == 0) {
      break;
    }
    Env::Default()->SleepForMicroseconds(kDelayMicros);
  }
  ASSERT_EQ(state.val, 3);
}

#define TEST_DATA_SIZE 384  // will cross buffer boundary
#define TEST_DATA_NUM 500
const std::string block_based_cache_paths = "./block_based_dir";
const std::string base_env_path = "./base_dir/";
// disable by anqin, because it needs HDFS env
#if 0

#define TEST_DATA_SIZE 384  // will cross buffer boundary
#define TEST_DATA_NUM 500

const std::string cache_paths = "./cache_dir_1/;./cache_dir_2/";
const std::string cache_name = "tera.test_cache";
const uint32_t mem_cache_size = 256; // should be bigger than 256

class MyCacheEnv {
public:
    MyCacheEnv()
        : env_(NULL), env_posix_(Env::Default()), tmpdir_("env_test_dir"),
          dbname_("dbtest"), raf_(NULL), sf_(NULL) {
        SplitString(cache_paths, ";", &cache_paths_);
        for (uint32_t i = 0; i < cache_paths_.size(); ++i) {
            env_posix_->CreateDir(cache_paths_[i]);
        }

        CacheEnv::SetCachePaths(cache_paths);
        CacheEnv::s_disk_cache_file_name_ = cache_name;
        CacheEnv::s_mem_cache_size_in_KB_ = mem_cache_size;
        env_ = EnvCache();

        LEVELDB_SET_LOG_LEVEL(DEBUG);
        SetupTestData();
    }

    ~MyCacheEnv() {
        DeleteRandomReader();
        DeleteSequentialReader();
        TearDownTestData();
        delete env_;

        for (uint32_t i = 0; i < cache_paths_.size(); ++i) {
            env_posix_->DeleteDir(cache_paths_[i]);
        }
    }

    void SetupTestData() {
        fprintf(stderr, "SetupTestData()\n");
        Status s = env_->CreateDir(tmpdir_);
        ASSERT_TRUE(s.ok()) << ": status = " << s.ToString();
        WritableFile* wf = NULL;
        s = env_->NewWritableFile(tmpdir_ + "/" + dbname_, &wf, EnvOptions());
        ASSERT_TRUE(s.ok());

        Random rnd(test::RandomSeed());
        uint32_t succ_num = 0;
        test_data.resize(TEST_DATA_NUM);
        for (uint32_t i = 0; i < TEST_DATA_NUM; ++i) {
            std::string key;
            test::RandomString(&rnd, TEST_DATA_SIZE, &key);
            s = wf->Append(key);
            if (s.ok()) {
                succ_num++;
            }
            test_data[i] = key;
        }
        ASSERT_EQ(succ_num, TEST_DATA_NUM);
        delete wf;
    }

    void TearDownTestData() {
        fprintf(stderr, "TearDownTestData()\n");
        Status s = env_->DeleteFile(tmpdir_ + "/"+ dbname_);
        s = env_->DeleteDir(tmpdir_);
        ASSERT_TRUE(s.ok());
    }

    void CreateRandomReader() {
        DeleteRandomReader();
        Status s = env_->NewRandomAccessFile(tmpdir_ + "/" + dbname_, &raf_);
        ASSERT_TRUE(s.ok());
    }

    void DeleteRandomReader() {
        if (raf_) {
            delete raf_;
            raf_ = NULL;
        }
    }

    void CreateSequentialReader() {
        DeleteSequentialReader();
        Status s = env_->NewSequentialFile(tmpdir_ + "/" + dbname_, &sf_);
        ASSERT_TRUE(s.ok());
    }

    void DeleteSequentialReader() {
        if (sf_) {
            delete sf_;
            sf_ = NULL;
        }
    }

    void CheckRandomRead(uint32_t idx, const std::string& tip = "") {
        if (idx > TEST_DATA_NUM) {
            return;
        }
        CreateRandomReader();
        if (idx == 362) {
            SLOG(DEBUG, "here");
        }

        Slice data;
        char scratch[8 * 1024];
        Status s = raf_->Read(idx * TEST_DATA_SIZE, TEST_DATA_SIZE,
                              &data, scratch);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(memcmp(data.data(), &test_data[idx][0], TEST_DATA_SIZE) == 0)
            << tip << ": data = " << data.data()
            << "\n vs. test_data[" << idx << "] = " << test_data[idx];
    }

    void CheckSequentialRead(uint32_t idx, const std::string& tip = "") {
        if (idx > TEST_DATA_NUM) {
            return;
        }
        if (sf_ == NULL) {
            CreateSequentialReader();
        }
        Slice data;
        char scratch[8 * 1024];
        Status s = sf_->Read(TEST_DATA_SIZE, &data, scratch);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(memcmp(data.data(), &test_data[idx][0], TEST_DATA_SIZE) == 0)
            << tip << ": data = " << data.data()
            << "\n vs. test_data[" << idx << "] = " << test_data[idx];
    }

protected:
    Env* env_;
    Env* env_posix_;
    std::string tmpdir_;
    std::string dbname_;
    RandomAccessFile* raf_;
    SequentialFile* sf_;

    std::vector<std::string> cache_paths_;
    std::vector<std::string> test_data;
};

MyCacheEnv my_cache;

class EnvCacheTest {};


TEST(EnvCacheTest, RandomRead_HitCache) {
    my_cache.CheckRandomRead(1, "no.1");
    my_cache.CheckRandomRead(1, "no.2");
}

TEST(EnvCacheTest, RandomRead_DataPreLoad) {
    Random rnd(test::RandomSeed());
    for (uint32_t i = 0; i < TEST_DATA_NUM; ++i) {
        uint32_t idx = rnd.Uniform(TEST_DATA_NUM);
        my_cache.CheckRandomRead(idx);
    }
}

TEST(EnvCacheTest, RandomRead_DataReload) {
    CacheEnv::ResetMemCache();
    my_cache.CreateRandomReader();
    Random rnd(test::RandomSeed());
    for (uint32_t i = 0; i < TEST_DATA_NUM; ++i) {
        uint32_t idx = rnd.Uniform(TEST_DATA_NUM);
        my_cache.CheckRandomRead(idx);
    }
    my_cache.DeleteRandomReader();
}

TEST(EnvCacheTest, SequenceRead) {
    my_cache.CreateSequentialReader();
    for (uint32_t i = 0; i < TEST_DATA_NUM; ++i) {
        my_cache.CheckSequentialRead(i);
    }
    my_cache.DeleteSequentialReader();
}

TEST(EnvCacheTest, ScheduleBlockIn) {}

TEST(EnvCacheTest, ScheduleBlockOut) {}

#endif

}  // namespace leveldb

int main(int argc, char** argv) { return leveldb::test::RunAllTests(); }
