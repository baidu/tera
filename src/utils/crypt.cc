// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "crypt.h"

#include <cstdio>
#include <string>

namespace tera {

int32_t GetHashString(const std::string& str, uint32_t seed, std::string* result) {
  if (result == NULL) {
    return -1;
  }
  uint32_t hash = 0;
  if (GetHashNumber(str, seed, &hash) != 0) {
    return -1;
  }
  char hash_str[9];
  sprintf(hash_str, "%08x", hash);

  result->assign(hash_str, 8);
  return 0;
}

int32_t GetHashNumber(const std::string& str, uint32_t seed, uint32_t* result) {
  const char* data = str.c_str();
  size_t n = str.length();
  if (result == NULL) {
    return -1;
  }
  // Similar to murmur hash
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = seed ^ (n * m);

  // Pick up four bytes at a time
  while (data + 4 <= limit) {
    uint32_t w = *(uint32_t*)data;
    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  // Pick up remaining bytes
  switch (limit - data) {
    case 3:
      h += data[2] << 16;
    case 2:
      h += data[1] << 8;
    case 1:
      h += data[0];
      h *= m;
      h ^= (h >> r);
      break;
  }
  *result = h;
  return 0;
}

}  // namespace tera
