// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/logging.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "util/string_ext.h"

namespace leveldb {

void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
  str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const Slice& value) {
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string EscapeString(const Slice& value) {
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}

bool ConsumeChar(Slice* in, char c) {
  if (!in->empty() && (*in)[0] == c) {
    in->remove_prefix(1);
    return true;
  } else {
    return false;
  }
}

bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
  if (in->size() > 1 && (*in)[0] == 'H') {
      return ConsumeHexDecimalNumber(in, val);
  }
  uint64_t v = 0;
  int digits = 0;
  while (!in->empty()) {
    char c = (*in)[0];
    if (c >= '0' && c <= '9') {
      ++digits;
      const int delta = (c - '0');
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
      if (v > kMaxUint64/10 ||
          (v == kMaxUint64/10 && delta > kMaxUint64%10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
      in->remove_prefix(1);
    } else {
      break;
    }
  }
  *val = v;
  return (digits > 0);
}

bool ConsumeHexDecimalNumber(Slice* in, uint64_t* val) {
    char c = (*in)[0];
    if (c != 'H') {
        return false;
    }
    in->remove_prefix(1);
    std::string hex_str = in->ToString();
    std::string log_num_str;
    SplitStringStart(hex_str, &log_num_str, NULL);
    if (log_num_str.empty()) {
        return false;
    }
    *val = StringToUint64(log_num_str, 16);
    in->remove_prefix(log_num_str.length());
    return true;
}

}  // namespace leveldb
