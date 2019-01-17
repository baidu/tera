// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "access/giano/giano_verification.h"

namespace tera {
namespace auth {

GianoVerification::GianoVerification() {
}

bool GianoVerification::Verify(const IdentityInfo& identity_info, RoleList* roles) {
  return false;
}

} // auth
} // tera
