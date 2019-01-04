// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_BASE_STRING_FORMAT_H_
#define TERA_COMMON_BASE_STRING_FORMAT_H_

#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <string>

size_t StringFormatAppendVA(std::string* dst, const char* format, va_list ap);

size_t StringFormatAppend(std::string* dst, const char* format, ...);

size_t StringFormatTo(std::string* dst, const char* format, ...);

std::string StringFormat(const char* format, ...);

#endif  // TERA_COMMON_BASE_STRING_FORMAT_H_
