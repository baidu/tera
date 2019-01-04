// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <fcntl.h>
#include <functional>
#include <iostream>
#include <string>
#include <sys/stat.h>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "common/file/file_path.h"
#include "common/log/log_cleaner.h"
#include "common/this_thread.h"
#include "utils/utils_cmd.h"

DECLARE_string(log_dir);
DECLARE_string(tera_log_prefix);
DECLARE_string(tera_leveldb_log_path);
DECLARE_int64(tera_info_log_clean_period_second);
DECLARE_int64(tera_info_log_expire_second);

using namespace std::placeholders;

namespace common {

static size_t g_touch_file_count = 0;
static size_t g_expect_clean_count = 0;
const static int64_t kTestLogExpireSecond = 5;

std::string TouchFile(const std::string &dir_path, const std::string &filename,
                      bool need_close = true) {
  std::string full_path = dir_path + "/" + filename;
  int fd = open(full_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
  if (need_close && fd > 0) {
    close(fd);
  }
  ++g_touch_file_count;
  return full_path;
}

void SetupTestEnv() {
  std::string leveldb_log_prefix = "leveldb.log";
  FLAGS_tera_leveldb_log_path = "./log/" + leveldb_log_prefix;
  // fake options, change log dir for cleaner
  FLAGS_log_dir = "./test_log";
  FLAGS_tera_log_prefix = "tera_test";
  FLAGS_tera_info_log_clean_period_second = 1;
  FLAGS_tera_info_log_expire_second = kTestLogExpireSecond;
  std::string other_prefix = "tera_other_prefix";

  // make test log dir, ignore failture
  mkdir(FLAGS_log_dir.c_str(), 0777);
  g_touch_file_count = 0;
  g_expect_clean_count = 0;

  // touch file unlinked
  std::string unlinked_info = FLAGS_tera_log_prefix + ".INFO.unlink";
  TouchFile(FLAGS_log_dir, unlinked_info);
  std::string unlinked_warn = FLAGS_tera_log_prefix + ".WARNING.unlink";
  TouchFile(FLAGS_log_dir, unlinked_warn);
  std::string unlinked_err = FLAGS_tera_log_prefix + ".stderr.unlink";
  TouchFile(FLAGS_log_dir, unlinked_err);
  g_expect_clean_count += 3;  // expect clean unlinked file

  // touch file linked
  std::string linked_info = FLAGS_tera_log_prefix + ".INFO.linked";
  std::string info_link_path = FLAGS_log_dir + "/" + FLAGS_tera_log_prefix + ".INFO";
  std::string linked_info_path = TouchFile(FLAGS_log_dir, linked_info);
  // link full path
  remove(info_link_path.c_str());
  symlink(linked_info_path.c_str(), info_link_path.c_str());
  ++g_touch_file_count;

  std::string linked_warn = FLAGS_tera_log_prefix + ".WARNING.linked";
  std::string warn_link_path = FLAGS_log_dir + "/" + FLAGS_tera_log_prefix + ".WARNING";
  TouchFile(FLAGS_log_dir, linked_warn);
  // link filename only
  remove(warn_link_path.c_str());
  symlink(linked_warn.c_str(), warn_link_path.c_str());
  ++g_touch_file_count;

  // touch file opened
  std::string opened_info = FLAGS_tera_log_prefix + ".INFO.opened";
  TouchFile(FLAGS_log_dir, opened_info, false);
  std::string opened_warn = FLAGS_tera_log_prefix + ".WARNING.opened";
  TouchFile(FLAGS_log_dir, opened_warn, false);
  std::string opened_err = FLAGS_tera_log_prefix + ".stderr.opened";
  TouchFile(FLAGS_log_dir, opened_err, false);

  // touch file not start with prefix
  std::string other_pre_info = other_prefix + ".INFO.otherpre";
  TouchFile(FLAGS_log_dir, other_pre_info);
  std::string other_pre_warn = other_prefix + ".WARNING.otherpre";
  TouchFile(FLAGS_log_dir, other_pre_warn);
  std::string other_pre_err = other_prefix + ".stderr.otherpre";
  TouchFile(FLAGS_log_dir, other_pre_err);

  // touch file start with leveldb_log_prefix and open one of them
  std::string ldb_pre_info = leveldb_log_prefix;
  TouchFile(FLAGS_log_dir, ldb_pre_info, false);
  std::string ldb_pre_info_lod = leveldb_log_prefix + ".old";
  TouchFile(FLAGS_log_dir, ldb_pre_info_lod);
  g_expect_clean_count++;  // expect clean leveldb_log_prefix.old
}

TEST(LogCleanerTest, InitialStatus) {
  // ensure stop firstly
  LogCleaner::StopCleaner();
  ASSERT_TRUE(LogCleaner::singleton_instance_ == NULL);
  SetupTestEnv();
  LogCleaner *cleaner = LogCleaner::GetInstance();

  ASSERT_FALSE(cleaner == NULL);
  ASSERT_FALSE(cleaner->IsRunning());
  ASSERT_TRUE(cleaner->CheckOptions());
  ASSERT_FALSE(cleaner->stop_);
}

TEST(LogCleanerTest, Basic) {
  SetupTestEnv();
  // get instance
  LogCleaner *cleaner = LogCleaner::GetInstance();
  ASSERT_FALSE(cleaner == NULL);

  // check log dir before clean
  std::vector<std::string> reserved_file_list;
  bool list_ret = ListCurrentDir(cleaner->info_log_dir_, &reserved_file_list);
  ASSERT_TRUE(list_ret);

  // print filelist before clean
  std::cout << "before clean. file count: " << reserved_file_list.size() << std::endl;
  for (size_t i = 0; i < reserved_file_list.size(); ++i) {
    std::cout << reserved_file_list[i] << std::endl;
  }
  ASSERT_EQ(reserved_file_list.size(), g_touch_file_count);

  // start and stop
  cleaner->Start();
  ASSERT_TRUE(cleaner->IsRunning());
  ASSERT_FALSE(cleaner->stop_);

  {
    // wait schedule clean first times
    MutexLock l(&(cleaner->mutex_), "log cleaner unittest");
    cleaner->bg_cond_.Wait();
  }

  // check clean result
  reserved_file_list.clear();
  list_ret = ListCurrentDir(cleaner->info_log_dir_, &reserved_file_list);
  ASSERT_TRUE(list_ret);
  // print filelist after clean
  std::cout << "first clean. expect clean nothing since not expire yet" << std::endl;
  EXPECT_EQ(reserved_file_list.size(), g_touch_file_count);

  {
    // wait schedule clean second times
    MutexLock l(&(cleaner->mutex_), "log cleaner unittest");
    cleaner->bg_cond_.Wait();
  }
  // check clean result
  reserved_file_list.clear();
  list_ret = ListCurrentDir(cleaner->info_log_dir_, &reserved_file_list);
  ASSERT_TRUE(list_ret);
  std::cout << "second clean. expect clean nothing since not expire yet" << std::endl;
  EXPECT_EQ(reserved_file_list.size(), g_touch_file_count);

  for (size_t i = 3; i < kTestLogExpireSecond + 5; ++i) {
    // wait schedule clean several times
    std::cout << "wait " << i << " times clean." << std::endl;
    MutexLock l(&(cleaner->mutex_), "log cleaner unittest");
    cleaner->bg_cond_.Wait();
  }
  // check clean result
  reserved_file_list.clear();
  list_ret = ListCurrentDir(cleaner->info_log_dir_, &reserved_file_list);
  ASSERT_TRUE(list_ret);
  std::cout << "after " << kTestLogExpireSecond << " times clean. expect clean "
            << g_expect_clean_count << " logs: " << std::endl;
  // print filelist after clean
  for (size_t i = 0; i < reserved_file_list.size(); ++i) {
    std::cout << reserved_file_list[i] << std::endl;
  }
  EXPECT_EQ(reserved_file_list.size(), g_touch_file_count - g_expect_clean_count);

  // stop cleaner
  cleaner->Stop();
  ASSERT_FALSE(cleaner->IsRunning());
  ASSERT_TRUE(cleaner->stop_);
  ASSERT_FALSE(cleaner == NULL);

  // destroy
  LogCleaner::StopCleaner();
  ASSERT_TRUE(LogCleaner::singleton_instance_ == NULL);
}

TEST(LogCleanerTest, MultiStartAndStop) {
  // ensure stop firstly
  LogCleaner::StopCleaner();
  ASSERT_TRUE(LogCleaner::singleton_instance_ == NULL);

  SetupTestEnv();
  // get instance
  LogCleaner *cleaner = LogCleaner::GetInstance();

  // stop while not start
  cleaner->Stop();
  ASSERT_FALSE(cleaner->IsRunning());
  ASSERT_TRUE(cleaner->stop_);

  // start three times
  cleaner->Start();
  ASSERT_TRUE(cleaner->IsRunning());
  cleaner->Start();
  ASSERT_TRUE(cleaner->IsRunning());
  cleaner->Start();
  ASSERT_TRUE(cleaner->IsRunning());

  {
    // wait schedule clean
    MutexLock l(&(cleaner->mutex_), "log cleaner unittest");
    cleaner->bg_cond_.Wait();
  }

  // stop twice
  cleaner->Stop();
  ASSERT_FALSE(cleaner->IsRunning());
  cleaner->Stop();
  ASSERT_FALSE(cleaner->IsRunning());

  // start again
  cleaner->Start();
  ASSERT_TRUE(cleaner->IsRunning());

  // stop and destroy
  LogCleaner::StopCleaner();
  ASSERT_TRUE(LogCleaner::singleton_instance_ == NULL);
}

}  // end namespace common
