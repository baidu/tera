// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/master_state_machine.h"

namespace tera {
namespace master {

static MasterStateMachine::MasterStateTransitionRulesType s_master_state_transition_rules;

const MasterStateMachine::MasterStateTransitionRulesType MasterStateMachine::state_transitions_(
    std::move(s_master_state_transition_rules.AddTransitionRule(kIsSecondary,
                                                                MasterEvent::kGetMasterLock,
                                                                kOnRestore)
                  .AddTransitionRule(kOnRestore, MasterEvent::kNoAvailTs, kOnWait)
                  .AddTransitionRule(kOnRestore, MasterEvent::kMetaRestored, kIsReadonly)
                  .AddTransitionRule(kOnRestore, MasterEvent::kLostMasterLock, kIsSecondary)
                  .AddTransitionRule(kOnWait, MasterEvent::kAvailTs, kOnRestore)
                  .AddTransitionRule(kOnWait, MasterEvent::kLostMasterLock, kIsSecondary)
                  .AddTransitionRule(kIsReadonly, MasterEvent::kLeaveSafemode, kIsRunning)
                  .AddTransitionRule(kIsReadonly, MasterEvent::kLostMasterLock, kIsSecondary)
                  .AddTransitionRule(kIsRunning, MasterEvent::kEnterSafemode, kIsReadonly)
                  .AddTransitionRule(kIsRunning, MasterEvent::kLostMasterLock, kIsSecondary)
                  .AddTransitionRule(kIsReadonly, MasterEvent::kLostMasterLock, kIsSecondary)));

std::ostream& operator<<(std::ostream& o, const MasterEvent event) {
  static const char* msg[] = {"MasterEvent::kGetMasterLock", "MasterEvent::kLostMasterLock",
                              "MasterEvent::kNoAvailTs",     "MasterEvent::kAvailTs",
                              "MasterEvent::kMetaRestored",  "MasterEvent::kEnterSafemode",
                              "MasterEvent::kLeaveSafemode", "MasterEvent::kUnknown"};
  static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
  typedef std::underlying_type<MasterEvent>::type UnderType;
  uint32_t index =
      static_cast<UnderType>(event) - static_cast<UnderType>(MasterEvent::kGetMasterLock);
  index = index < msg_size ? index : msg_size - 1;
  o << msg[index];
  return o;
}
}
}
