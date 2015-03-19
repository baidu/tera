// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_BASE_STRING_NUMBER_H_
#define TERA_COMMON_BASE_STRING_NUMBER_H_

#include <float.h>
#include <stdlib.h>
#include <string.h>

#include <string>

#include <stdint.h>
#include "common/base/ascii.h"

// namespace common {

/// -----------------------------------------------------------------------
/// @brief Parse the string from the first position. stop when error occurs.
/// @return true if part of the string can be converted to a valid number
/// @return false if the number exceeds limit or nothing is parsed.
/// @param str the string to parse
/// @param value store the parsed number if success
/// @param endptr not null, *endptr stores the address of the first invalid char
/// !!! If no invalid char is allowed, use function below: StringToNumber()
/// @param base specify the base to be used for the conversion
/// -----------------------------------------------------------------------

bool ParseNumber(const char* str, signed char* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, unsigned char* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, short* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, unsigned short* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, int* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, unsigned int* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, long* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, unsigned long* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, long long* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, unsigned long long* value, char** endptr, int base = 0);
bool ParseNumber(const char* str, float* value, char** endptr);
bool ParseNumber(const char* str, double* value, char** endptr);
bool ParseNumber(const char* str, long double* value, char** endptr);

/// ---------------------------------------------------------------
/// @brief interface for parsing string to number
/// ---------------------------------------------------------------
template <typename Type>
bool ParseNumber(const std::string& str, Type* value, char** endptr, int base)
{
    return ParseNumber(str.c_str(), value, endptr, base);
}

template <typename Type>
bool ParseNumber(const std::string& str, Type* value, char** endptr)
{
    return ParseNumber(str.c_str(), value, endptr);
}

/// ---------------------------------------------------------------
/// @brief interface for string to number
/// @return true if total string is successfully parsed.
/// ---------------------------------------------------------------
template <typename Type>
bool StringToNumber(const std::string& str, Type* value, int base)
{
//     STATIC_ASSERT(TypeTraits::IsInteger<Type>::Value, "Type must be integral type");
    char* endptr;
    bool ret = ParseNumber(str.c_str(), value, &endptr, base);
    return (ret && *endptr == '\0');
}

template <typename Type>
bool StringToNumber(const char* str, Type* value, int base)
{
//     STATIC_ASSERT(TypeTraits::IsInteger<Type>::Value, "Type must be integral type");
    char* endptr;
    bool ret = ParseNumber(str, value, &endptr, base);
    return (ret && *endptr == '\0');
}

template <typename Type>
bool StringToNumber(const std::string& str, Type* value)
{
    char* endptr;
    bool ret = ParseNumber(str.c_str(), value, &endptr);
    return (ret && *endptr == '\0');
}

template <typename Type>
bool StringToNumber(const char* str, Type* value)
{
    char* endptr;
    bool ret = ParseNumber(str, value, &endptr);
    return (ret && *endptr == '\0');
}

/// ---------------------------------------------------------------
/// @brief converting numbers  to buffer, buffer size should be big enough
/// ---------------------------------------------------------------
const int kMaxIntegerStringSize = 32;
const int kMaxDoubleStringSize = 32;
const int kMaxFloatStringSize = 24;
const int kMaxIntStringSize = kMaxIntegerStringSize;

/// @brief judge a number if it's nan
inline bool IsNaN(double value)
{
    return !(value > value) && !(value <= value);
}

/// @brief write number to buffer as string
/// @return end of result
/// @note without '\0' appended
/// private functions for common library, don't use them in your code
char* WriteDoubleToBuffer(double n, char* buffer);
char* WriteFloatToBuffer(float n, char* buffer);
char* WriteInt32ToBuffer(int32_t i, char* buffer);
char* WriteUInt32ToBuffer(uint32_t u, char* buffer);
char* WriteInt64ToBuffer(int64_t i, char* buffer);
char* WriteUInt64ToBuffer(uint64_t u64, char* buffer);

char* WriteIntegerToBuffer(int n, char* buffer);
char* WriteIntegerToBuffer(unsigned int n, char* buffer);
char* WriteIntegerToBuffer(long n, char* buffer);
char* WriteIntegerToBuffer(unsigned long n, char* buffer);
char* WriteIntegerToBuffer(long long n, char* buffer);
char* WriteIntegerToBuffer(unsigned long long n, char* buffer);

void AppendIntegerToString(int n, std::string* str);
void AppendIntegerToString(unsigned int n, std::string* str);
void AppendIntegerToString(long n, std::string* str);
void AppendIntegerToString(unsigned long n, std::string* str);
void AppendIntegerToString(long long n, std::string* str);
void AppendIntegerToString(unsigned long long n, std::string* str);

/// @brief convert number to hex string
/// buffer size should be at least [2 * sizeof(value) + 1]
char* WriteHexUInt16ToBuffer(uint16_t value, char* buffer);
char* WriteHexUInt32ToBuffer(uint32_t value, char* buffer);
char* WriteHexUInt64ToBuffer(uint64_t value, char* buffer);

/// @brief write number to buffer as string
/// @return start of buffer
/// @note with '\0' appended
char* DoubleToString(double n, char* buffer);
char* FloatToString(float n, char* buffer);
char* Int32ToString(int32_t i, char* buffer);
char* UInt32ToString(uint32_t u, char* buffer);
char* Int64ToString(int64_t i, char* buffer);
char* UInt64ToString(uint64_t u64, char* buffer);
char* IntegerToString(int n, char* buffer);
char* IntegerToString(unsigned int n, char* buffer);
char* IntegerToString(long n, char* buffer);
char* IntegerToString(unsigned long n, char* buffer);
char* IntegerToString(long long n, char* buffer);
char* IntegerToString(unsigned long long n, char* buffer);
char* UInt16ToHexString(uint16_t value, char* buffer);
char* UInt32ToHexString(uint32_t value, char* buffer);
char* UInt64ToHexString(uint64_t value, char* buffer);

/// @brief convert float number to string
std::string FloatToString(float n);

/// @brief convert float number to string
std::string DoubleToString(double n);

/// ---------------------------------------------------------------
/// @brief convert number to string, not so efficient but more convenient
/// ---------------------------------------------------------------
std::string IntegerToString(int n);
std::string IntegerToString(unsigned int n);
std::string IntegerToString(long n);
std::string IntegerToString(unsigned long n);
std::string IntegerToString(long long n);
std::string IntegerToString(unsigned long long n);

/// @brief convert number to hex string, not so efficient but more convenient
std::string UInt16ToHexString(uint16_t n);
std::string UInt32ToHexString(uint32_t n);
std::string UInt64ToHexString(uint64_t n);

/// @brief convert numeric type object to string
/// for generic programming code
inline std::string NumberToString(double n) { return DoubleToString(n); }
inline std::string NumberToString(float n) { return FloatToString(n); }
inline std::string NumberToString(int n) { return IntegerToString(n); }
inline std::string NumberToString(unsigned int n) { return IntegerToString(n); }
inline std::string NumberToString(long n) { return IntegerToString(n); }
inline std::string NumberToString(unsigned long n) { return IntegerToString(n); }
inline std::string NumberToString(long long n) { return IntegerToString(n); }
inline std::string NumberToString(unsigned long long n) { return IntegerToString(n); }

///////////////////////////////////////////////////////////////////////////////
// convert number to human readable string

/// Convert decimal number to human readable string, based on 1000.
/// @param n the number to be converted
/// @param unit the unit of the number, such as "m/s", "Hz", etc.
///             the unit can have an optional space(" ") prefix,
///             such as " bps", and then 10000 will be convert to "10 kbps"
/// @note this function support float number and negative number, keep 3
/// significant digits.
std::string FormatMeasure(double n, const char* unit = "");

/// Convert number to human readable string, based on 1024.
/// @param n the number to be converted
/// @param unit the unit of the number, such as "m/s"
///             the unit can have an optional space(" ") prefix,
///             such as " B", and then 10240 will be convert to "10 KiB"
/// this function support only integral value, keep 3 significant digits.
std::string FormatBinaryMeasure(int64_t n, const char* unit = "");

// } // namespace common

#endif // TERA_COMMON_BASE_STRING_STRING_NUMBER_H_
