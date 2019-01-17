// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include "tera/error_code.h"
#include "access/helpers/access_utils.h"

namespace tera {
namespace auth {

class Identification {
 public:
  // Login to get the cred/password
  // Success will return true
  // otherwise return false
  virtual bool Login(IdentityInfo* const identity_info, ErrorCode* const error_code) = 0;
};
}
}
