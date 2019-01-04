// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <map>
#include <iostream>
#include <fstream>
#include <glog/logging.h>
#include "master/state_machine.h"
#include "proto/table_meta.pb.h"

namespace tera {
namespace master {

enum class TabletEvent {
  kLoadTablet,
  kUnLoadTablet,
  kFinishSplitTablet,
  kFinishMergeTablet,
  kUpdateMeta,    // update tablet info to meta table
  kTsLoadSucc,    // tabletnode load tablet succ
  kTsLoadFail,    // tabletnode load tablet fail
  kTsLoadBusy,    // tabletnode has reached its max load concurrency
  kTsUnloadBusy,  // tabletnode has reached its max unload cuncurrency
  kTsUnLoadSucc,  // tabletnode unload tablet succ
  kTsUnLoadFail,  // tabletnode unload tablet fail
  kTsOffline,     // tabletnode offline
  // tabletnode restarted, notice that we may apperceive TS_RESTART rather than
  // TS_OFFLINE because of the zk session timeout configuration
  kTsRestart,
  kWaitRpcResponse,  //  dispatch rpc
  kTabletLoadFail,   // tablet load failed finally exhausting all load attempts
  // a special event somewhat equivalent to TS_OFFLINE with consideration for
  // cache locality
  kTsDelayOffline,
  kTableDisable,  // enable table
  kTableEnable,   // disable table
  kEofEvent,      // end-marker event, some cleanup work maybe triggered by this
                  // event
};

std::ostream& operator<<(std::ostream& o, const TabletEvent& event);

class TabletStateMachine {
 public:
  TabletStateMachine(const TabletMeta::TabletStatus init_state);

  // return true and update curr_state_ to the associated PostState
  // corresponding to
  // <curr_state_, event> pair if there is a valid transition rule for
  // <curr_state_, event> pair,
  // else return false;
  bool DoStateTransition(const TabletEvent& event);

  TabletMeta::TabletStatus GetStatus() const;
  // update tablet's current status bypass TabletStateTransitionRule
  void SetStatus(const TabletMeta::TabletStatus status);
  // get tablet's readytime in us, return int64::max() if tablet not in
  // kTableReady status
  int64_t ReadyTime();

  typedef StateTransitionRules<TabletMeta::TabletStatus, TabletEvent>
      TabletStateTransitionRulesType;

 private:
  TabletMeta::TabletStatus curr_state_;
  // timestamp of tablet status changed to kTableReady, in us
  int64_t ready_time_;
  // TabletStatus transition rules
  const static TabletStateTransitionRulesType state_transitions_;
};
}
}
