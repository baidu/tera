// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include <functional>
#include <mutex>

#include "common/rwmutex.h"
#include "lrulist.h"
#include "persistent_cache/hash_table.h"
#include "util/random.h"

namespace leveldb {

// Evictable Hash Table
//
// Hash table index where least accessed (or one of the least accessed) elements
// can be evicted.
//
// Please note EvictableHashTable can only be created for pointer type objects
template <class T, class Hash, class Equal>
class EvictableHashTable : private HashTable<T*, Hash, Equal> {
 public:
  typedef HashTable<T*, Hash, Equal> hash_table;

  explicit EvictableHashTable(const size_t capacity = 1024 * 1024, const float load_factor = 2.0,
                              const uint32_t nlocks = 16)
      : HashTable<T*, Hash, Equal>(capacity, load_factor, nlocks),
        lru_lists_(new LRUList<T>[hash_table::nlocks_]) {
    assert(lru_lists_);
  }

  virtual ~EvictableHashTable() { AssertEmptyLRU(); }

  //
  // Insert given record to hash table (and LRU list)
  //
  bool Insert(T* t) {
    const uint64_t h = Hash()(t);
    typename hash_table::Bucket& bucket = GetBucket(h);
    LRUListType& lru = GetLRUList(h);
    RWMutex& lock = GetMutex(h);

    WriteLock _(&lock);
    if (hash_table::Insert(&bucket, t)) {
      lru.Push(t);
      lru.Touch(t);
      return true;
    }
    return false;
  }

  bool Find(T* t, T** ret) {
    const uint64_t h = Hash()(t);
    typename hash_table::Bucket& bucket = GetBucket(h);
    LRUListType& lru = GetLRUList(h);
    RWMutex& lock = GetMutex(h);

    ReadLock _(&lock);
    if (hash_table::Find(&bucket, t, ret)) {
      ++(*ret)->refs_;
      lru.Touch(*ret);
      return true;
    }
    return false;
  }

  T* Remove(T* t) {
    T* target = nullptr;
    const uint64_t h = Hash()(t);
    typename hash_table::Bucket& bucket = GetBucket(h);
    LRUListType& lru = GetLRUList(h);
    RWMutex& lock = GetMutex(h);

    WriteLock _(&lock);
    if (hash_table::Find(&bucket, t, &target)) {
      assert(t == target);
      lru.Unlink(target);
      T* tmp = nullptr;
      bool status = hash_table::Erase(&bucket, target, &tmp);
      assert(target == tmp);
      assert(status);
    }
    return target;
  }

  //
  // Evict one of the least recently used object
  //
  T* Evict(const std::function<void(T*)>& fn = nullptr) {
    uint32_t random = Random::GetTLSInstance()->Next();
    const size_t start_idx = random % hash_table::nlocks_;
    T* t = nullptr;

    for (size_t i = 0; !t && i < hash_table::nlocks_; ++i) {
      const size_t idx = (start_idx + i) % hash_table::nlocks_;

      WriteLock _(&hash_table::locks_[idx]);
      LRUListType& lru = lru_lists_[idx];
      if (!lru.IsEmpty() && (t = lru.Pop()) != nullptr) {
        assert(!t->refs_);
        // We got an item to evict, erase from the bucket
        const uint64_t h = Hash()(t);
        typename hash_table::Bucket& bucket = GetBucket(h);
        T* tmp = nullptr;
        bool status = hash_table::Erase(&bucket, t, &tmp);
        assert(t == tmp);
        assert(status);
        break;
      }
      assert(!t);
    }

    if (t && fn) {
      fn(t);
    }

    return t;
  }

  void Clear(void (*fn)(T*)) {
    for (uint32_t i = 0; i < hash_table::nbuckets_; ++i) {
      const uint32_t lock_idx = i % hash_table::nlocks_;
      WriteLock _(&hash_table::locks_[lock_idx]);
      auto& lru_list = lru_lists_[lock_idx];
      auto& bucket = hash_table::buckets_[i];
      for (auto* t : bucket.list_) {
        lru_list.Unlink(t);
        (*fn)(t);
      }
      bucket.list_.clear();
    }
    // make sure that all LRU lists are emptied
    AssertEmptyLRU();
  }

  void AssertEmptyLRU() {
    for (uint32_t i = 0; i < hash_table::nlocks_; ++i) {
      WriteLock _(&hash_table::locks_[i]);
      auto& lru_list = lru_lists_[i];
      assert(lru_list.IsEmpty());
    }
  }

  //
  // Fetch the mutex associated with a key
  // This call is used to hold the lock for a given data for extended period of
  // time.
  RWMutex* GetMutex(T* t) { return hash_table::GetMutex(t); }

 private:
  typedef LRUList<T> LRUListType;

  typename hash_table::Bucket& GetBucket(const uint64_t h) {
    const uint32_t bucket_idx = h % hash_table::nbuckets_;
    return hash_table::buckets_[bucket_idx];
  }

  LRUListType& GetLRUList(const uint64_t h) {
    const uint32_t bucket_idx = h % hash_table::nbuckets_;
    const uint32_t lock_idx = bucket_idx % hash_table::nlocks_;
    return lru_lists_[lock_idx];
  }

  RWMutex& GetMutex(const uint64_t h) {
    const uint32_t bucket_idx = h % hash_table::nbuckets_;
    const uint32_t lock_idx = bucket_idx % hash_table::nlocks_;
    return hash_table::locks_[lock_idx];
  }

  std::unique_ptr<LRUListType[]> lru_lists_;
};
}  // namespace leveldb
