// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "utils/rpc_timer_list.h"

namespace tera {

RpcTimerList::RpcTimerList()
    : head_(NULL), tail_(NULL), size_(0) {}

RpcTimerList::~RpcTimerList() {}

bool RpcTimerList::TopTime(int64_t* time) {
    MutexLock lock(&mutex_);
    if (NULL == head_) {
        return false;
    }
    *time = head_->time;
    return true;
}

void RpcTimerList::Push(RpcTimer* item) {
    MutexLock lock(&mutex_);
    item->prev = tail_;
    item->next = NULL;
    if (NULL != tail_) {
        tail_->next = item;
    }
    tail_ = item;
    if (NULL == head_) {
        head_ = item;
    }
    size_++;
}

void RpcTimerList::Erase(RpcTimer* item) {
    MutexLock lock(&mutex_);
    if (NULL != item->prev) {
        item->prev->next = item->next;
    }
    if (NULL != item->next) {
        item->next->prev = item->prev;
    }
    if (head_ == item) {
        head_ = item->next;
    }
    if (tail_ == item) {
        tail_ = item->prev;
    }
    item->prev = NULL;
    item->next = NULL;
    size_--;
}

size_t RpcTimerList::Size() {
    MutexLock lock(&mutex_);
    return size_;
}

RpcTimerList* RpcTimerList::Instance() {
    return s_instance;
}

RpcTimerList* RpcTimerList::s_instance = new RpcTimerList;

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
