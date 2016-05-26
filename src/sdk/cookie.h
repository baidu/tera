// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_COOKIE_H_
#define TERA_SDK_COOKIE_H_

#include <string>

#include "proto/table_meta.pb.h"

namespace tera {
namespace sdk {

void DumpCookie(const std::string& cookie_file,
                const std::string& cookie_lock_file,
                const SdkCookie& cookie);
bool RestoreCookie(const std::string cookie_file,
                   bool delete_broken_cookie_file,
                   SdkCookie *cookie);

// helper
bool DumpCookieFile(const std::string& cookie_file);
bool FindKeyInCookieFile(const std::string& cookie_file, const std::string& key);

} // namespace sdk
} // namespace tera

#endif // TERA_SDK_COOKIE_H_
