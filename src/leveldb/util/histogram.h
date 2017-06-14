// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
#define STORAGE_LEVELDB_UTIL_HISTOGRAM_H_

#include <string>
#include "port/port_posix.h"
#include "util/mutexlock.h"

namespace leveldb {

class Histogram {
 public:
  Histogram() { Clear(); }
  ~Histogram() { }

  void Clear();
  void Add(double value);
  void Merge(const Histogram& other);

  std::string ToString() const;

 private:
  mutable port::Mutex mutex_;
  double min_;
  double max_;
  double num_;
  double sum_;
  double sum_squares_;

  enum { kNumBuckets = 154 };
  static const double kBucketLimit[kNumBuckets];
  double buckets_[kNumBuckets];

 public:
  double Median() const;
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
