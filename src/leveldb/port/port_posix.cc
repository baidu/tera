// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/port_posix.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "util/logging.h"

#include <sys/time.h>
#include <unistd.h>

#ifdef USE_COMPRESS_EXT
#include "compress/lz4.h"
#include "compress/bmz_codec.h"

bmz::BmzCodec bmc;
#endif

namespace leveldb {
namespace port {

static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

Mutex::Mutex() {
    pthread_mutexattr_t attr;
    PthreadCall("init mutexattr", pthread_mutexattr_init(&attr));
    PthreadCall("set mutexattr", pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK));
    PthreadCall("init mutex", pthread_mutex_init(&mu_, &attr));
    PthreadCall("destroy mutexattr", pthread_mutexattr_destroy(&attr));
}

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() { PthreadCall("lock", pthread_mutex_lock(&mu_)); }

void Mutex::Unlock() { PthreadCall("unlock", pthread_mutex_unlock(&mu_)); }

CondVar::CondVar(Mutex* mu)
    : mu_(mu) {
    PthreadCall("init cv", pthread_cond_init(&cv_, NULL));
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() {
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
}

bool CondVar::Wait(int32_t wait_millisec) {
  assert(wait_millisec >= 0);
  struct timespec ts;
  struct timeval tp;
  gettimeofday(&tp, NULL);
  uint32_t usec = tp.tv_usec + wait_millisec * 1000;
  ts.tv_sec = tp.tv_sec + usec / 1000000;
  ts.tv_nsec = (usec % 1000000) * 1000;
  return (0 == pthread_cond_timedwait(&cv_, &mu_->mu_, &ts));
}

void CondVar::Signal() {
  PthreadCall("signal", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&cv_));
}

void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("once", pthread_once(once, initializer));
}

/////////// Compression Ext ///////////

bool Bmz_Compress(const char* input, size_t input_size,
                  std::string* output) {
#ifdef USE_COMPRESS_EXT
    size_t output_size = input_size * 2;
    output->resize(output_size);
    if (!bmc.Compress(input, input_size, &(*output)[0], &output_size)
        || output_size == 0 || output_size > input_size) {
        return false;
    }
    output->resize(output_size);
    return true;
#else
    return false;
#endif
}

bool Bmz_Uncompress(const char* input, size_t input_size,
                    char* output, size_t* output_size) {
#ifdef USE_COMPRESS_EXT
    return bmc.Uncompress(input, input_size, output, output_size);
#else
    return false;
#endif
}

bool Lz4_Compress(const char* input, size_t input_size,
                  std::string* output) {
#ifdef USE_COMPRESS_EXT
    output->resize(input_size * 2);
    size_t output_size = LZ4LevelDB_compress(input, &(*output)[0], input_size);
    if (output_size == 0 || output_size > input_size) {
        return false;
    }
    output->resize(output_size);
    return true;
#else
    return false;
#endif
}

bool Lz4_Uncompress(const char* input, size_t input_size,
                    char* output, size_t* output_size) {
#ifdef USE_COMPRESS_EXT
    size_t max_output_size = *output_size;
    *output_size = LZ4LevelDB_decompress_fast(
        input, output, input_size);
    return true;
#else
    return false;
#endif
}

//////////////////////////////

}  // namespace port
}  // namespace leveldb
