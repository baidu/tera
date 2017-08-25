// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <cmath>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  size_t Entries();
  size_t TotalCharge();

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Unref(LRUHandle* e);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  port::Mutex mutex_;
  size_t usage_;
  size_t entries_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_;

  HandleTable table_;
};

LRUCache::LRUCache()
    : capacity_(0),
      usage_(0),
      entries_(0) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

LRUCache::~LRUCache() {
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle
    Unref(e);
    e = next;
  }
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {
    usage_ -= e->charge;
    entries_--;
    (*e->deleter)(e->key(), e->value);
    free(e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    e->refs++;
    LRU_Remove(e);
    LRU_Append(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache, one for the returned handle
  memcpy(e->key_data, key.data(), key.size());
  LRU_Append(e);
  usage_ += charge;
  entries_++;

  LRUHandle* old = table_.Insert(e);
  if (old != NULL) {
    LRU_Remove(old);
    Unref(old);
  }

  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    Unref(old);
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Remove(key, hash);
  if (e != NULL) {
    LRU_Remove(e);
    Unref(e);
  }
}

size_t LRUCache::Entries() {
  MutexLock l(&mutex_);
  return entries_;
}

size_t LRUCache::TotalCharge() {
  MutexLock l(&mutex_);
  return usage_;
}

class LRU2QCache: public Cache {
 public:
  explicit LRU2QCache(size_t capacity)
    : capacity_(capacity),
      usage_(0) {
     // Make empty circular linked list
    lru_.next = &lru_;
    lru_.prev = &lru_;
  }

  ~LRU2QCache() {}

  // Like Cache methods, but with an extra "hash" parameter.
  // Notice: insert if absent,if exist, return the old one.
  Cache::Handle* Insert(const Slice& key, void* value, size_t cache_id,
                        void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    MutexLock l(&mutex_);
    LRUHandle* e = NULL;
    e = (LRUHandle*)DoLookup(key, hash);
    if (e != NULL) {
        return reinterpret_cast<Cache::Handle*>(e);
    }

    if (usage_ < capacity_) { // cache not full
      e = reinterpret_cast<LRUHandle*>(
          malloc(sizeof(LRUHandle)-1 + key.size()));
      e->value = value;
      e->deleter = deleter;
      e->charge = 1;
      e->key_length = key.size();
      e->hash = hash;
      e->refs = 2;  // One from LRUCache, one for the returned handle
      e->cache_id = usage_;
      memcpy(e->key_data, key.data(), key.size());

      LRU_Append(e);
      assert(table_.Insert(e) == NULL);
      usage_++;
      return reinterpret_cast<Cache::Handle*>(e);
    }

    // cache full, reuse item
    LRUHandle* old = lru_.next;
    while (old != &lru_) {
      if (old->refs > 1) {
        old = old->next;
        continue;
      }
      e = reinterpret_cast<LRUHandle*>(
          malloc(sizeof(LRUHandle)-1 + key.size()));
      e->value = value;
      e->deleter = deleter;
      e->charge = 1;
      e->key_length = key.size();
      e->hash = hash;
      e->refs = 2;  // One from LRUCache, one for the returned handle
      e->cache_id = old->cache_id;
      memcpy(e->key_data, key.data(), key.size());

      LRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      Unref(old);

      LRU_Append(e);
      assert(table_.Insert(e) == NULL);
      return reinterpret_cast<Cache::Handle*>(e);
    }
    return NULL;
  }

  Cache::Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    MutexLock l(&mutex_);
    return DoLookup(key, hash);
  }

  void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    MutexLock l(&mutex_);
    LRUHandle* e = table_.Remove(key, hash);
    if (e != NULL) {
      LRU_Remove(e);
      Unref(e);
    }
  }

  void Release(Cache::Handle* handle) {
    MutexLock l(&mutex_);
    Unref(reinterpret_cast<LRUHandle*>(handle));
  }

  void* Value(Cache::Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }

  uint64_t NewId() {
    return 0;
  }

  double HitRate(bool force_clear = false) {
    return 99.9999;
  }

  size_t Entries() {
    MutexLock l(&mutex_);
    return usage_;
  }

  size_t TotalCharge() {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  Cache::Handle* DoLookup(const Slice& key, uint32_t hash) {
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != NULL) {
        e->refs++;
        LRU_Remove(e);
        LRU_Append(e);
    }
    return reinterpret_cast<Cache::Handle*>(e);
  }

  void LRU_Remove(LRUHandle* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
  }

  void LRU_Append(LRUHandle* e) {
    // Make "e" newest entry by inserting just before lru_
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
  }

  void Unref(LRUHandle* e) {
    assert(e->refs > 0);
    e->refs--;
    if (e->refs <= 0) {
      usage_ -= e->charge;
      (*e->deleter)(e->key(), e->value);
      free(e);
    }
  }

  inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  port::Mutex mutex_;
  size_t usage_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  //LRUHandle hot_lru_;
  //LRUHandle cold_lru_;
  LRUHandle lru_;

  HandleTable table_;
};

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;
  uint64_t hits_;
  uint64_t lookups_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0),
        hits_(0),
        lookups_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    Handle* h = shard_[Shard(hash)].Lookup(key, hash);
    MutexLock l(&id_mutex_);
    lookups_++;
    if (h != NULL) {
      hits_++;
    }
    return h;
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual double HitRate(bool force_clear) {
    MutexLock l(&id_mutex_);
    double ret;
    if (lookups_ > 0) {
      ret = (double)hits_ / (double)lookups_;
    } else {
      ret = NAN;
    }
    if (force_clear) {
      hits_ = 0;
      lookups_ = 0;
    }
    return ret;
  }
  virtual size_t Entries() {
    size_t entries = 0;
    for (int s = 0; s < kNumShards; s++) {
      entries += shard_[s].Entries();
    }
    return entries;
  }
  virtual size_t TotalCharge() {
    size_t total_charge = 0;
    for (int s = 0; s < kNumShards; s++) {
      total_charge += shard_[s].TotalCharge();
    }
    return total_charge;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

Cache* New2QCache(size_t capacity) {
  return new LRU2QCache(capacity);
}

}  // namespace leveldb
