// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include "tera/hash.h"
#include <sstream>
#include <iomanip>

namespace tera {
/*
  Murmurhash from http://sites.google.com/site/murmurhash/
  All code is released to the public domain. For business purposes, Murmurhash
  is under the MIT license.
*/
static uint64_t MurmurHash64A(const void *key, int len, unsigned int seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t *data = (const uint64_t *)key;
  const uint64_t *end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char *data2 = (const unsigned char *)data;

  switch (len & 7) {
    case 7:
      h ^= ((uint64_t)data2[6]) << 48;  // fallthrough
    case 6:
      h ^= ((uint64_t)data2[5]) << 40;  // fallthrough
    case 5:
      h ^= ((uint64_t)data2[4]) << 32;  // fallthrough
    case 4:
      h ^= ((uint64_t)data2[3]) << 24;  // fallthrough
    case 3:
      h ^= ((uint64_t)data2[2]) << 16;  // fallthrough
    case 2:
      h ^= ((uint64_t)data2[1]) << 8;  // fallthrough
    case 1:
      h ^= ((uint64_t)data2[0]);
      h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

std::string MurmurHash(const std::string &user_key) {
  const static unsigned int kSeed = 823;
  std::stringstream ss;
  ss << std::setw(16) << std::setfill('0') << std::hex
     << MurmurHash64A(&user_key[0], user_key.size(), kSeed);
  return ss.str();
}
}
