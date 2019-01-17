// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/string_format.h"

size_t StringFormatAppendVA(std::string* dst, const char* format, va_list ap) {
  // First try with a small fixed size buffer
  char space[1024];
  // It's possible for methods that use a va_list to invalidate
  // the data in it upon use.  The fix is to make a copy
  // of the structure before using it and use that copy instead.
  va_list backup_ap;
  va_copy(backup_ap, ap);
  int result = vsnprintf(space, sizeof(space), format, backup_ap);
  va_end(backup_ap);
  if ((result >= 0) && (result < static_cast<int>(sizeof(space)))) {
    dst->append(space, result);
    return result;
  }
  // Repeatedly increase buffer size until it fits
  int length = sizeof(space);
  while (true) {
    if (result < 0) {
      // Older behavior: just try doubling the buffer size
      length *= 2;
    } else {
      // We need exactly "result+1" characters
      length = result + 1;
    }
    char* buf = new char[length];
    // Restore the va_list before we use it again
    va_copy(backup_ap, ap);
    result = vsnprintf(buf, length, format, backup_ap);
    va_end(backup_ap);
    if ((result >= 0) && (result < length)) {
      dst->append(buf, result);
      delete[] buf;
      break;
    }
    delete[] buf;
  }
  return result;
}

size_t StringFormatAppend(std::string* dst, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  size_t result = StringFormatAppendVA(dst, format, ap);
  va_end(ap);
  return result;
}

size_t StringFormatTo(std::string* dst, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  dst->clear();
  size_t result = StringFormatAppendVA(dst, format, ap);
  va_end(ap);
  return result;
}

std::string StringFormat(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  std::string result;
  StringFormatAppendVA(&result, format, ap);
  va_end(ap);
  return result;
}
