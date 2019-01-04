// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "common/base/string_ext.h"
#include "common/this_thread.h"
#include "sdk/test/global_txn_testutils.h"
#include "utils/config_utils.h"
#include "common/timer.h"

DEFINE_bool(tera_gtxn_test_opened, false, "for test gtxn opened");
DEFINE_bool(tera_gtxn_test_isolation_snapshot, true,
            "true means Snapshot, false means ReadCommitedSnapshot");
DEFINE_string(tera_gtxn_test_flagfile, "", "gtxn test flagfile");
DEFINE_int64(start_ts, 1, "start ts");
DEFINE_int64(begin_commit_ts, 0, "time to wait before begin commit");
DEFINE_int64(begin_prewrite_ts, 0, "time to wait before prewrite");
DEFINE_int64(end_prewrite_ts, 0, "time to wait from before prewrite");
DEFINE_int64(commit_ts, 1, "time to wait from end prewrite");
DEFINE_int64(begin_primary_commit_ts, 0, "time to wait before primary commit");
DEFINE_int64(end_primary_commit_ts, 0, "time to wait from primary commit");
DEFINE_int64(begin_other_commit_ts, 0, "time to wait before other commit");
DEFINE_string(get_wait_ts_list, "", "timestamp list for wait to get");

namespace tera {

constexpr int64_t kMillisPerSecond = 1000L;

GlobalTxnTestHelper::GlobalTxnTestHelper(const std::string& conffile)
    : pos_(0),
      get_pos_(0),
      conf_file_(conffile),
      start_ts_(0),
      prewrite_start_ts_(0),
      commit_ts_(0),
      helper_create_time_(get_millis()) {}

void GlobalTxnTestHelper::LoadTxnConf() {
  utils::LoadFlagFile(conf_file_);
  ts_[0] = FLAGS_start_ts;
  start_ts_ = FLAGS_start_ts;
  ts_[1] = FLAGS_begin_commit_ts;
  ts_[2] = FLAGS_begin_prewrite_ts;
  ts_[3] = FLAGS_end_prewrite_ts;
  ts_[4] = FLAGS_commit_ts;
  ts_[5] = FLAGS_begin_primary_commit_ts;
  ts_[6] = FLAGS_end_primary_commit_ts;
  ts_[7] = FLAGS_begin_other_commit_ts;
  VLOG(13) << "split get wait ts list begin...";
  SplitString(FLAGS_get_wait_ts_list, ",", &get_ts_list_);
  for (auto item : get_ts_list_) {
    VLOG(13) << item;
  }
  VLOG(13) << "split get wait ts list done";
  // if isolation_level == ReadCommitedSnapshot
  if (!FLAGS_tera_gtxn_test_isolation_snapshot) {
    prewrite_start_ts_ = FLAGS_start_ts + FLAGS_begin_commit_ts + FLAGS_begin_prewrite_ts;
  } else {
    prewrite_start_ts_ = start_ts_;
  }
  commit_ts_ = FLAGS_start_ts + FLAGS_begin_commit_ts + FLAGS_begin_prewrite_ts +
               FLAGS_end_prewrite_ts + FLAGS_commit_ts;
  if (commit_ts_ <= prewrite_start_ts_) {
    commit_ts_ = prewrite_start_ts_ + 1;
  }
  Wait(ts_[0]);
}

int64_t GlobalTxnTestHelper::GetStartTs() { return start_ts_; }

int64_t GlobalTxnTestHelper::GetPrewriteStartTs() { return prewrite_start_ts_; }

int64_t GlobalTxnTestHelper::GetCommitTs() { return commit_ts_; }

void GlobalTxnTestHelper::GetWait(int64_t start_ts) {
  if (get_ts_list_.size() == 0) {
    // don't wait
    VLOG(13) << "[gtxn_helper] [" << start_ts << "] will do get operater immediate";
  } else {
    // get operaters in 'get_ts_list' will wait by 'get_ts_list' set,
    // not in get_ts_list will immediate GET after the last 'get_ts_list' item
    // finished
    if (get_pos_ < get_ts_list_.size()) {
      int64_t now_millis = tera::get_millis();
      int64_t def_wait_time = stol(get_ts_list_[get_pos_]) * kMillisPerSecond;
      int64_t wait_time = helper_create_time_ + def_wait_time - now_millis;
      VLOG(13) << "get_pos_:" << get_pos_ << " now_millis:" << now_millis
               << " def_wait_time:" << def_wait_time << " size:" << get_ts_list_.size()
               << " wait_time:" << wait_time;
      if (wait_time > 0) {
        VLOG(13) << "[gtxn_helper] [" << start_ts << "] will do get operater(" << (get_pos_ + 1)
                 << ") after" << wait_time << " ms.";
        ThisThread::Sleep(wait_time);
      } else {
        VLOG(13) << "[gtxn_helper] [" << start_ts << "] will do get operater(" << (get_pos_ + 1)
                 << ") immediate";
      }
    } else {
      VLOG(13) << "[gtxn_helper] [" << start_ts << "] will do get operater(" << (get_pos_ + 1)
               << ") immediate";
    }
    get_pos_++;
  }
}

void GlobalTxnTestHelper::Wait(int64_t start_ts) {
  int wait_position = pos_++;
  int64_t* info = ts_;
  int64_t now_micros = tera::get_micros();
  if (wait_position == 0) {
    PrintLog(start_ts, "begin txn", info[wait_position + 1]);
  } else {
    if (info[wait_position] == -1) {
      ExitNow(start_ts, wait_position);
    }
    int64_t should_wait = info[wait_position] * 1000000L + info[wait_position - 1];
    if (should_wait - now_micros > 10) {
      ThisThread::Sleep((should_wait - now_micros) / 1000L);
    } else if (info[wait_position] == 0) {
      // nothing to do
    } else if (should_wait < now_micros) {
      LOG(ERROR) << "[gtxn_helper] [" << start_ts << "] txn run timeout, exited";
      _Exit(0);
    }
    switch (wait_position) {
      case 1:
        PrintLog(start_ts, "begin commit", info[wait_position + 1]);
        break;
      case 2:
        PrintLog(start_ts, "begin prewrite", info[wait_position + 1]);
        break;
      case 3:
        PrintLog(start_ts, "end prewrite", info[wait_position + 1]);
        break;
      case 4:
        PrintLog(start_ts, "begin real commit", info[wait_position + 1]);
        break;
      case 5:
        PrintLog(start_ts, "begin primary commit", info[wait_position + 1]);
        break;
      case 6:
        PrintLog(start_ts, "end primary commit", info[wait_position + 1]);
        break;
      case 7:
        PrintLog(start_ts, "begin other commit");
        break;
      default:
        LOG(ERROR) << "overflow position";
        _Exit(0);
    }
  }
  info[wait_position] = tera::get_micros();
  return;
}

void GlobalTxnTestHelper::ExitNow(int64_t start_ts, int position) {
  VLOG(13) << "[gtxn_helper] [" << start_ts << "] exit @ position=" << position;
  _Exit(0);  // for simulate test gtxn stop at anywhere
}

void GlobalTxnTestHelper::PrintLog(int64_t start_ts, const std::string& log_str,
                                   int64_t next_wait_time) {
  if (next_wait_time == -1) {
    VLOG(13) << "[gtxn_helper] [" << start_ts << "] " << log_str << ", txn will be done.";
  } else {
    VLOG(13) << "[gtxn_helper] [" << start_ts << "] " << log_str << ", next step will begin after ["
             << next_wait_time << "s]";
  }
}

}  // namespace tera
