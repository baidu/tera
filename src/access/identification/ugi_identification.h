// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include "access/identification/identification.h"

namespace tera {
namespace auth {
class UgiIdentification : public Identification {
 public:
  UgiIdentification();
  virtual ~UgiIdentification() {}
  virtual bool Login(IdentityInfo* const identity_info, ErrorCode* const error_code) override;
};
}
}
