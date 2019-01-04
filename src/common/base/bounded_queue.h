// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once
#include <numeric>
#include <deque>
#include <type_traits>
namespace common {

// A simple bounded queue based on std::queue
template <class T>
class BoundedQueue {
  using size_type = typename std::deque<T>::size_type;
  using value_type = typename std::deque<T>::value_type;
  using reference = typename std::deque<T>::reference;
  using const_reference = typename std::deque<T>::const_reference;

 public:
  explicit BoundedQueue(size_type limit) : limit_{limit} {}

  void Push(const value_type& v) {
    qu_.push_back(v);
    Drop();
  }

  void Push(value_type&& v) {
    qu_.push_back(std::move(v));
    Drop();
  }

  template <class... Args>
  void Emplace(Args&&... args) {
    qu_.emplace_back(std::forward<Args>(args)...);
    Drop();
  }

  reference Front() { return qu_.front(); }

  const_reference Front() const { return qu_.front(); }

  reference Back() { return qu_.back(); }

  const_reference Back() const { return qu_.back(); }

  bool Empty() const { return qu_.empty(); }

  void Pop() { qu_.pop_front(); }

  size_type Size() { return qu_.size(); }

  // tera specified
  value_type Sum() {
    static_assert(std::is_arithmetic<value_type>::value,
                  "Only arithmetic value is able to use Sum method");
    if (Empty()) {
      return 0;
    }
    return std::accumulate(std::begin(qu_), std::end(qu_), (value_type)0,
                           [](value_type x, const value_type& y) { return x + y; });
  }

  value_type Average() {
    static_assert(std::is_arithmetic<value_type>::value,
                  "Only arithmetic value is able to use Average method");
    if (Empty()) {
      return 0;
    }
    return Sum() / Size();
  }

 private:
  void Drop() {
    while (Size() > limit_) {
      Pop();
    }
  }
  std::deque<T> qu_;
  size_type limit_;
};
}
