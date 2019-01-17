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

CondVar::CondVar(Mutex* mu) : mu_(mu) {
  // use monotonic clock
  PthreadCall("condattr init ", pthread_condattr_init(&attr_));
  PthreadCall("condattr setclock ", pthread_condattr_setclock(&attr_, CLOCK_MONOTONIC));
  PthreadCall("condvar init with attr", pthread_cond_init(&cond_, &attr_));
}

CondVar::~CondVar() {
  PthreadCall("condvar destroy", pthread_cond_destroy(&cond_));
  PthreadCall("condattr destroy", pthread_condattr_destroy(&attr_));
}

void CondVar::Wait() { PthreadCall("condvar wait", pthread_cond_wait(&cond_, &mu_->mu_)); }

// wait in ms
bool CondVar::Wait(int64_t wait_millisec) {
  // ref:
  // http://www.qnx.com/developers/docs/6.5.0SP1.update/com.qnx.doc.neutrino_lib_ref/p/pthread_cond_timedwait.html
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  int64_t nsec = ((int64_t)wait_millisec) * 1000000 + ts.tv_nsec;
  assert(nsec > 0 && wait_millisec >= 0);
  ts.tv_sec += nsec / 1000000000;
  ts.tv_nsec = nsec % 1000000000;
  int err = pthread_cond_timedwait(&cond_, &mu_->mu_, &ts);
  if (err == 0) {
    return true;
  } else if (err == ETIMEDOUT) {
    // The time specified by 'ts' to pthread_cond_timedwait() has passed.
    return false;
  } else {
    PthreadCall("condvar timedwait", err);
    return false;
  }
}

void CondVar::Signal() { PthreadCall("signal", pthread_cond_signal(&cond_)); }

void CondVar::SignalAll() { PthreadCall("broadcast", pthread_cond_broadcast(&cond_)); }

void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("once", pthread_once(once, initializer));
}

/////////// Compression Ext ///////////

bool Bmz_Compress(const char* input, size_t input_size, std::string* output) {
#ifdef USE_COMPRESS_EXT
  size_t output_size = input_size * 2;
  output->resize(output_size);
  if (!bmc.Compress(input, input_size, &(*output)[0], &output_size) || output_size == 0 ||
      output_size > input_size) {
    return false;
  }
  output->resize(output_size);
  return true;
#else
  return false;
#endif
}

bool Bmz_Uncompress(const char* input, size_t input_size, char* output, size_t* output_size) {
#ifdef USE_COMPRESS_EXT
  return bmc.Uncompress(input, input_size, output, output_size);
#else
  return false;
#endif
}

bool Lz4_Compress(const char* input, size_t input_size, std::string* output) {
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

bool Lz4_Uncompress(const char* input, size_t input_size, char* output, size_t* output_size) {
#ifdef USE_COMPRESS_EXT
  size_t max_output_size = *output_size;
  *output_size = LZ4LevelDB_decompress_fast(input, output, input_size);
  return true;
#else
  return false;
#endif
}

//////////////////////////////

}  // namespace port
}  // namespace leveldb
