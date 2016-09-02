// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Some tools are defined in this file.

#ifndef  TERA_UTILS_H_
#define  TERA_UTILS_H_

#include <stdint.h>
#include <string>

#pragma GCC visibility push(default)
namespace tera {

class CounterCoding {
public:
    // encode an int64 to a string for a counter cell
    static std::string EncodeCounter(int64_t counter);
    // decode an int64 from a string for a counter cell
    static bool DecodeCounter(const std::string& buf, int64_t* counter);
};
} // namespace tera
#pragma GCC visibility pop

#endif  // TERA_READER_H_
