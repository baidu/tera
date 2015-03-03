// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/this_thread.h"

#include "tera/tera_entry.h"

namespace tera {

TeraEntry::TeraEntry()
    : m_started(false) {}

TeraEntry::~TeraEntry() {}

bool TeraEntry::Start() {
    if (ShouldStart()) {
        return StartServer();
    }
    return false;
}

bool TeraEntry::Run() {
    ThisThread::Sleep(2000);
    return true;
}

bool TeraEntry::Shutdown() {
    if (ShouldShutdown()) {
        ShutdownServer();
        return true;
    }
    return false;
}

bool TeraEntry::ShouldStart() {
    MutexLock lock(&m_mutex);
    if (m_started) {
        return false;
    }
    m_started = true;
    return true;
}

bool TeraEntry::ShouldShutdown() {
    MutexLock lock(&m_mutex);
    if (!m_started) {
        return false;
    }
    m_started = false;
    return true;
}

}  // namespace tera
