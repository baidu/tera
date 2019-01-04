// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_CONFIG_H_
#define STORAGE_LEVELDB_CONFIG_H_

namespace leveldb {
// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
static const int kL0_CompactionTrigger = 2;

// Soft limit on number of level-0 files.  We slow down writes at this point.
static const int kL0_SlowdownWritesTrigger = (1 << 30);  // 12;

// Maximum number of level-0 files.  We stop writes at this point.
static const int kL0_StopWritesTrigger = (1 << 30);  // 20;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemCompactLevel = 2;

}  // namespace config
}  // namespace leveldb

#endif