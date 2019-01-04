// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include "sharded_persistent_cache_impl.h"
#include "leveldb/persistent_cache.h"

namespace leveldb {
Status NewShardedPersistentCache(const std::vector<PersistentCacheConfig> &configs,
                                 std::shared_ptr<PersistentCache> *cache) {
  if (!cache) {
    return Status::IOError("invalid argument cache");
  }

  auto pcache = std::make_shared<ShardedPersistentCacheImpl>(configs);
  Status s = pcache->Open();

  if (!s.ok()) {
    return s;
  }

  *cache = pcache;
  return s;
}
}  // namespace leveldb
