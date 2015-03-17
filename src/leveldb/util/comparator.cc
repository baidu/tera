// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/raw_key_operator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};

class TeraBinaryComparatorImpl : public Comparator {
 public:
  TeraBinaryComparatorImpl() : key_operator_(BinaryRawKeyOperator()){ }

  virtual const char* Name() const {
    return "tera.TeraBinaryComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return key_operator_->Compare(a, b);
  }

  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
      // TODO: this may waste storage space
  }

  virtual void FindShortSuccessor(std::string* key) const {
      // TODO: this may waste storage space
  }

 private:
  const RawKeyOperator* key_operator_;
};

class TeraTTLKvComparatorImpl : public Comparator {
public:
    TeraTTLKvComparatorImpl() :
            key_operator_(KvRawKeyOperator()) {
    }

    virtual const char* Name() const {
        return "tera.TeraTTLKvComparator";
    }

    virtual int Compare(const Slice& a, const Slice& b) const {
        Slice row_key_a, row_key_b;
        int64_t timestamp_a, timestamp_b;
        key_operator_->ExtractTeraKey(a, &row_key_a, NULL, NULL, &timestamp_a,
                NULL);
        key_operator_->ExtractTeraKey(b, &row_key_b, NULL, NULL, &timestamp_b,
                NULL);
        return row_key_a.compare(row_key_b);
    }

    virtual void FindShortestSeparator(std::string* start,
            const Slice& limit) const {
        // TODO: this may waste storage space
    }

    virtual void FindShortSuccessor(std::string* key) const {
        // TODO: this may waste storage space
    }

private:
    const RawKeyOperator* key_operator_;
};
}  // namespace

static port::OnceType once = LEVELDB_ONCE_INIT;
static const Comparator* bytewise;
static const Comparator* terabinary;
static const Comparator* terakv;

static void InitModule() {
    bytewise = new BytewiseComparatorImpl;
    terabinary = new TeraBinaryComparatorImpl;
    terakv = new TeraTTLKvComparatorImpl;
}

const Comparator* BytewiseComparator() {
    port::InitOnce(&once, InitModule);
    return bytewise;
}

const Comparator* TeraBinaryComparator() {
    port::InitOnce(&once, InitModule);
    return terabinary;
}

const Comparator* TeraTTLKvComparator() {
    port::InitOnce(&once, InitModule);
    return terakv;
}

}  // namespace leveldb
