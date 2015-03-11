// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera/utils/rpc_timer_list.h"

namespace tera {

RpcTimerList::RpcTimerList()
    : m_head(NULL), m_tail(NULL), m_size(0) {}

RpcTimerList::~RpcTimerList() {}

bool RpcTimerList::TopTime(int64_t* time) {
    MutexLock lock(&m_mutex);
    if (NULL == m_head) {
        return false;
    }
    *time = m_head->time;
    return true;
}

void RpcTimerList::Push(RpcTimer* item) {
    MutexLock lock(&m_mutex);
    item->prev = m_tail;
    item->next = NULL;
    if (NULL != m_tail) {
        m_tail->next = item;
    }
    m_tail = item;
    if (NULL == m_head) {
        m_head = item;
    }
    m_size++;
}

void RpcTimerList::Erase(RpcTimer* item) {
    MutexLock lock(&m_mutex);
    if (NULL != item->prev) {
        item->prev->next = item->next;
    }
    if (NULL != item->next) {
        item->next->prev = item->prev;
    }
    if (m_head == item) {
        m_head = item->next;
    }
    if (m_tail == item) {
        m_tail = item->prev;
    }
    item->prev = NULL;
    item->next = NULL;
    m_size--;
}

size_t RpcTimerList::Size() {
    MutexLock lock(&m_mutex);
    return m_size;
}

RpcTimerList* RpcTimerList::Instance() {
    return s_instance;
}

RpcTimerList* RpcTimerList::s_instance = new RpcTimerList;

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
