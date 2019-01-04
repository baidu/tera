// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include <algorithm>
#include <vector>

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

class IterWrapper {
 public:
  IterWrapper() : iter_(NULL), valid_(false) {}
  explicit IterWrapper(Iterator* iter) : iter_(NULL) { Set(iter); }
  ~IterWrapper() {}
  Iterator* iter() const { return iter_; }

  void Set(Iterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == NULL) {
      valid_ = false;
    } else {
      Update();
    }
  }

  // Iterator interface methods
  bool Valid() const { return valid_; }
  Slice key() const {
    assert(Valid());
    return key_;
  }
  Slice value() const {
    assert(Valid());
    return iter_->value();
  }
  // Methods below require iter() != NULL
  Status status() const {
    assert(iter_);
    return iter_->status();
  }
  void Next() {
    assert(iter_);
    iter_->Next();
    Update();
  }
  void Prev() {
    assert(iter_);
    iter_->Prev();
    Update();
  }
  void Seek(const Slice& k) {
    assert(iter_);
    iter_->Seek(k);
    Update();
  }
  void SeekToFirst() {
    assert(iter_);
    iter_->SeekToFirst();
    Update();
  }
  void SeekToLast() {
    assert(iter_);
    iter_->SeekToLast();
    Update();
  }

 private:
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }
  }

  Iterator* iter_;
  bool valid_;
  Slice key_;
};
struct Greater {
  bool operator()(IterWrapper& it1, IterWrapper& it2) {
    if (!it1.Valid()) {
      // iterator 1 is not valid, regard it as the bigger one
      return true;
    }
    if (!it2.Valid()) {
      // iterator 2 is not valid, regard it as the bigger one
      return false;
    }
    return (comp->Compare(it1.key(), it2.key()) > 0);
  }

  Greater(const Comparator* comparator) : comp(comparator) {}

  const Comparator* comp;
};

struct Lesser {
  bool operator()(IterWrapper& it1, IterWrapper& it2) {
    if (!it1.Valid()) {
      // always regard it as the lesser one
      return true;
    }
    if (!it2.Valid()) {
      // always regard it as the lesser one
      return false;
    }
    return (comp->Compare(it1.key(), it2.key()) < 0);
  }

  Lesser(const Comparator* comparator) : comp(comparator) {}

  const Comparator* comp;
};

class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        current_(NULL),
        greater_(Greater(comparator)),
        lesser_(Lesser(comparator)),
        direction_(kForward) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  virtual ~MergingIterator() {
    for (size_t i = 0; i < children_.size(); i++) {
      delete children_[i].iter();
    }
  }

  virtual bool Valid() const { return (current_ != NULL); }

  virtual void SeekToFirst() {
    for (size_t i = 0; i < children_.size(); i++) {
      children_[i].SeekToFirst();
      if (!CheckIterStatus(children_[i])) {
        current_ = NULL;
        return;
      }
    }
    // make children as a min-heap
    make_heap(children_.begin(), children_.end(), greater_);
    FindSmallest();
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    for (size_t i = 0; i < children_.size(); i++) {
      children_[i].SeekToLast();
      if (!CheckIterStatus(children_[i])) {
        current_ = NULL;
        return;
      }
    }
    // make children as a max-heap
    make_heap(children_.begin(), children_.end(), lesser_);
    FindLargest();
    direction_ = kReverse;
  }

  virtual void Seek(const Slice& target) {
    for (size_t i = 0; i < children_.size(); i++) {
      children_[i].Seek(target);
      if (!CheckIterStatus(children_[i])) {
        current_ = NULL;
        return;
      }
    }
    // make children as a min-heap
    make_heap(children_.begin(), children_.end(), greater_);
    FindSmallest();
    direction_ = kForward;
  }

  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (size_t i = 0; i < children_.size(); i++) {
        IterWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() && comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
          if (!CheckIterStatus(*child)) {
            current_ = NULL;
            return;
          }
        }
      }

      // make children as a min-heap and pop the smallest one
      make_heap(children_.begin(), children_.end(), greater_);
      std::pop_heap(children_.begin(), children_.end(), greater_);
      current_ = &children_.back();
      direction_ = kForward;
    }

    current_->Next();
    if (!CheckIterStatus(*current_)) {
      current_ = NULL;
      return;
    }
    std::push_heap(children_.begin(), children_.end(), greater_);
    FindSmallest();
  }

  virtual void Prev() {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (size_t i = 0; i < children_.size(); i++) {
        IterWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
          if (!CheckIterStatus(*child)) {
            current_ = NULL;
            return;
          }
        }
      }

      // make children as a max-heap and pop the largest one
      make_heap(children_.begin(), children_.end(), lesser_);
      std::pop_heap(children_.begin(), children_.end(), lesser_);
      current_ = &children_.back();
      direction_ = kReverse;
    }

    current_->Prev();
    std::push_heap(children_.begin(), children_.end(), lesser_);
    if (!CheckIterStatus(*current_)) {
      current_ = NULL;
      return;
    }
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const {
    Status status;
    for (size_t i = 0; i < children_.size(); i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  bool CheckIterStatus(const IterWrapper& iter) { return iter.Valid() || iter.status().ok(); }

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  std::vector<IterWrapper> children_;
  IterWrapper* current_;
  const Greater greater_;
  const Lesser lesser_;

  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  std::pop_heap(children_.begin(), children_.end(), greater_);
  current_ = &children_.back();
  if (!current_->Valid()) {
    current_ = NULL;
  }
}

void MergingIterator::FindLargest() {
  std::pop_heap(children_.begin(), children_.end(), lesser_);
  current_ = &children_.back();
  if (!current_->Valid()) {
    current_ = NULL;
  }
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

}  // namespace leveldb
