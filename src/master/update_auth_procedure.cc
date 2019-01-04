// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/update_auth_procedure.h"

namespace tera {
namespace master {

std::ostream& operator<<(std::ostream& o, const UpdateAuthPhase& phase) {
  static const char* msg[] = {"UpdateAuthPhase::kUpdateMeta", "UpdateAuthPhase::kEofPhase",
                              "UpdateAuthPhase::kUnknown"};
  static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
  using UnderType = std::underlying_type<UpdateAuthPhase>::type;
  uint32_t index =
      static_cast<UnderType>(phase) - static_cast<UnderType>(UpdateAuthPhase::kUpdateMeta);
  index = index < msg_size ? index : msg_size - 1;
  o << msg[index];
  return o;
}
}
}
