// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <iostream>
#include <fstream>
#include "master/state_machine.h"
#include "proto/status_code.pb.h"

namespace tera {
namespace master {

enum MasterStatus {
  kIsSecondary = kMasterIsSecondary,
  kIsRunning = kMasterIsRunning,
  kOnRestore = kMasterOnRestore,
  kOnWait = kMasterOnWait,
  kIsReadonly = kMasterIsReadonly,
};

enum class MasterEvent {
  kGetMasterLock,
  kLostMasterLock,
  kNoAvailTs,
  kAvailTs,
  kMetaRestored,
  kEnterSafemode,
  kLeaveSafemode
};

std::ostream& operator<<(std::ostream& o, const MasterEvent event);

class MasterStateMachine {
 public:
  MasterStateMachine(MasterStatus init_status) : curr_status_(init_status) {}
  ~MasterStateMachine() {}

  bool DoStateTransition(const MasterEvent event, MasterStatus* status) {
    *status = curr_status_;
    return DoStateTransition(event);
  }

  bool DoStateTransition(const MasterEvent event) {
    MasterStatus post_status;
    if (!state_transitions_.DoStateTransition(curr_status_, event, &post_status)) {
      return false;
    }
    curr_status_ = post_status;
    return true;
  }

  MasterStatus GetState() const { return curr_status_; }

  typedef StateTransitionRules<MasterStatus, MasterEvent> MasterStateTransitionRulesType;

 private:
  MasterStatus curr_status_;
  const static MasterStateTransitionRulesType state_transitions_;
};
}
}
