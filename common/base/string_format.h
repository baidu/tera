// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef COMMON_BASE_STRING_FORMAT_H
#define COMMON_BASE_STRING_FORMAT_H
#pragma once

#include <stdarg.h>
#include <stddef.h>
#include <string>


size_t StringFormatAppendVA(std::string* dst, const char* format, va_list ap);

size_t StringFormatAppend(std::string* dst, const char* format, ...);

size_t StringFormatTo(std::string* dst, const char* format, ...);

std::string StringFormat(const char* format, ...);


#endif // COMMON_BASE_STRING_FORMAT_H
