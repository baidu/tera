// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "tera_entry.h"
#include "common/this_thread.h"

namespace tera {

TeraEntry::TeraEntry()
    : started_(false) {}

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
    MutexLock lock(&mutex_);
    if (started_) {
        return false;
    }
    started_ = true;
    return true;
}

bool TeraEntry::ShouldShutdown() {
    MutexLock lock(&mutex_);
    if (!started_) {
        return false;
    }
    started_ = false;
    return true;
}

}  // namespace tera
