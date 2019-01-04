// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_TEST_GLOBAL_TXN_TESTUTILS_H_
#define TERA_SDK_TEST_GLOBAL_TXN_TESTUTILS_H_

#include <string>

namespace tera {

class GlobalTxnTestHelper {
 public:
  GlobalTxnTestHelper(const std::string& conffile);
  ~GlobalTxnTestHelper() {}
  int64_t GetStartTs();
  int64_t GetPrewriteStartTs();
  int64_t GetCommitTs();
  void Wait(int64_t start_ts);
  void GetWait(int64_t start_ts);
  void LoadTxnConf();

 private:
  void ExitNow(int64_t start_ts, int position);
  void PrintLog(int64_t start_ts, const std::string& log_str, int64_t next_wait_time = -1);
  int pos_;
  size_t get_pos_;
  std::string conf_file_;
  int64_t start_ts_;
  int64_t prewrite_start_ts_;
  int64_t commit_ts_;
  int64_t ts_[8];
  std::vector<std::string> get_ts_list_;
  int64_t helper_create_time_;
};

}  // namespace tera

#endif  // TERA_SDK_TEST_GLOBAL_TXN_TESTUTILS_H_
