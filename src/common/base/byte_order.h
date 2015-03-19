// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_BASE_BYTE_ORDER_H_
#define TERA_COMMON_BASE_BYTE_ORDER_H_

// GLOBAL_NOLINT(runtime/int)

#ifdef __unix__
#include <byteswap.h>
#include <endian.h>
#include <arpa/inet.h>
#endif

#ifdef _MSC_VER
#include <stdlib.h> // for _byteswap_*
#endif

/// define __LITTLE_ENDIAN
#ifndef __LITTLE_ENDIAN
#define __LITTLE_ENDIAN 1234
#endif

/// define __BIG_ENDIAN
#ifndef __BIG_ENDIAN
#define __BIG_ENDIAN 4321
#endif

/// define __BYTE_ORDER
#ifndef __BYTE_ORDER
#if defined(_M_IX86) || defined(_M_X64) || defined(__i386__) || defined(__x86_64__)
#define __BYTE_ORDER __LITTLE_ENDIAN
#else
#error unknown byte order
#endif
#endif

/// define LITTLE_ENDIAN
#ifndef LITTLE_ENDIAN
#define LITTLE_ENDIAN __LITTLE_ENDIAN
#endif

/// define BIG_ENDIAN
#ifndef BIG_ENDIAN
#define BIG_ENDIAN __BIG_ENDIAN
#endif

/// define BYTE_ORDER
#ifndef BYTE_ORDER
#define BYTE_ORDER __BYTE_ORDER
#endif

#ifdef _WIN32 // winsock APIs

#define BYTEORDER_WINSOCK_API_LINKAGE __declspec(dllimport)
#define BYTEORDER_WSAAPI __stdcall

extern "C" {

BYTEORDER_WINSOCK_API_LINKAGE
unsigned long
BYTEORDER_WSAAPI
htonl(
    unsigned long hostlong
);

BYTEORDER_WINSOCK_API_LINKAGE
unsigned short
BYTEORDER_WSAAPI
htons(
    unsigned short hostshort
);

BYTEORDER_WINSOCK_API_LINKAGE
unsigned long
BYTEORDER_WSAAPI
ntohl(
    unsigned long netlong
);

BYTEORDER_WINSOCK_API_LINKAGE
unsigned short
BYTEORDER_WSAAPI
ntohs(
    unsigned short netshort
);

} // extern "C"

#endif // _WIN32

#ifndef __linux__
# ifdef _MSC_VER
static unsigned short bswap_16(unsigned short x) // NOLINT(runtime/int)
{
    return _byteswap_ushort(x);
}
static unsigned int bswap_32(unsigned int x) // NOLINT(runtime/int)
{
    return _byteswap_ulong(x);
}
static unsigned long long bswap_64(unsigned long long x) // NOLINT(runtime/int)
{
    return _byteswap_uint64(x);
}
# else
static unsigned short bswap_16(unsigned short x) // NOLINT(runtime/int)
{
    return (((x >> 8) & 0xff) | ((x & 0xff) << 8));
}
static unsigned int bswap_32(unsigned int x) // NOLINT(runtime/int)
{
    return
        (((x & 0xff000000) >> 24) | ((x & 0x00ff0000) >>  8) |
        ((x & 0x0000ff00) <<  8) | ((x & 0x000000ff) << 24));
}
static unsigned long long bswap_64(unsigned long long x) // NOLINT(runtime/int)
{
    return
        (((x & 0xff00000000000000ull) >> 56)
        | ((x & 0x00ff000000000000ull) >> 40)
        | ((x & 0x0000ff0000000000ull) >> 24)
        | ((x & 0x000000ff00000000ull) >> 8)
        | ((x & 0x00000000ff000000ull) << 8)
        | ((x & 0x0000000000ff0000ull) << 24)
        | ((x & 0x000000000000ff00ull) << 40)
        | ((x & 0x00000000000000ffull) << 56));
}
# endif
#endif

#if BYTE_ORDER == LITTLE_ENDIAN
inline unsigned long long htonll(unsigned long long n) // NOLINT(runtime/int)
{
    return bswap_64(n);
}
#else
inline unsigned long long htonll(unsigned long long n) // NOLINT(runtime/int)
{
    return n;
}
#endif

inline unsigned long long ntohll(unsigned long long n) // NOLINT(runtime/int)
{
    return htonll(n);
}

// using as a strong namespace
struct ByteOrder
{
private:
    ByteOrder();
    ~ByteOrder();

public:
    static bool IsBigEndian()
    {
#if __linux__
        return __BYTE_ORDER == __BIG_ENDIAN;
#elif defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_IA64) || defined(_M_X64)
        // known little architectures
        return false;
#else // unknown
        int x = 1;
        return reinterpret_cast<unsigned char&>(x) == 0;
#endif
    }

    static bool IsLittleEndian()
    {
        return !IsBigEndian();
    }

    // one byte, NOP
    static char Swap(char value) { return value; }
    static signed char Swap(signed char value) { return value; }
    static unsigned char Swap(unsigned char value) { return value; }

    static short Swap(short value) { return bswap_16(value); } // NOLINT(runtime/int)
    static unsigned short Swap(unsigned short value) // NOLINT(runtime/int)
    { return bswap_16(value); }

    static int Swap(int value)  // NOLINT(runtime/int)
    { return bswap_32(value); }
    static unsigned int Swap(unsigned int value) // NOLINT(runtime/int)
    { return bswap_32(value); }

    static long Swap(long value) // NOLINT(runtime/int)
    {
        if (sizeof(value) == 4)
            return bswap_32(value);
        else
            return (long) bswap_64(value); // NOLINT(runtime/int)
    }

    static unsigned long Swap(unsigned long value) // NOLINT(runtime/int)
    {
        if (sizeof(value) == 4)
            return bswap_32(value);
        else
            return (unsigned long) bswap_64(value); // NOLINT(runtime/int)
    }

    static long long Swap(long long value) // NOLINT(runtime/int)
    {
        return (long long) bswap_64(value); // NOLINT(runtime/int)
    }
    static unsigned long long Swap(unsigned long long value) // NOLINT(runtime/int)
    {
        return bswap_64(value);
    }

    template <typename T>
    static void Swap(T* value)
    {
        *value = Swap(*value);
    }

    ///////////////////////////////////////////////////////////////////////////
    // float number can only be inplace swap

    static void Swap(float* value)
    {
        unsigned int *p = reinterpret_cast<unsigned int*>(value);
        Swap(p);
    }

    static void Swap(double* value)
    {
        unsigned long long *p = reinterpret_cast<unsigned long long*>(value); // NOLINT(runtime/int)
        Swap(p);
    }

    ///////////////////////////////////////////////////////////////////////////
    // local byte order to network byte order

    static char LocalToNet(char x) { return x; }
    static signed char LocalToNet(signed char x) { return x; }
    static unsigned char LocalToNet(unsigned char x) { return x; }

    static short LocalToNet(short x)
    {
        return htons(x);
    }
    static unsigned short LocalToNet(unsigned short x)
    {
        return htons(x);
    }
    static int LocalToNet(int x)
    {
        return htonl(x);
    }
    static unsigned int LocalToNet(unsigned int x)
    {
        return htonl(x);
    }

    static long LocalToNet(long x)
    {
        return (sizeof(x) == 4) ?
            htonl(x) : (long) htonll(static_cast<unsigned long long>(x));
    }
    static unsigned long LocalToNet(unsigned long x)
    {
        return (sizeof(x) == 4) ?
            htonl(x) : (unsigned long) htonll(static_cast<unsigned long long>(x));
    }
    static long long LocalToNet(long long x)
    {
        return htonll(static_cast<unsigned long long>(x));
    }
    static unsigned long long LocalToNet(unsigned long long x)
    {
        return htonll(static_cast<unsigned long long>(x));
    }

    template <typename T>
    static void LocalToNet(T* value)
    {
        *value = LocalToNet(*value);
    }

    static void LocalToNet(float* value)
    {
        if (IsLittleEndian())
            Swap(value);
    }

    static void LocalToNet(double* value)
    {
        if (IsLittleEndian())
            Swap(value);
    }

    ///////////////////////////////////////////////////////////////////////////
    // network byte order to local byte order
    static char NetToLocal(char x) { return x; }
    static signed char NetToLocal(signed char x) { return x; }
    static unsigned char NetToLocal(unsigned char x) { return x; }

    static short NetToLocal(short x)
    {
        return ntohs(x);
    }
    static unsigned short NetToLocal(unsigned short x)
    {
        return ntohs(x);
    }
    static int NetToLocal(int x)
    {
        return ntohl(x);
    }
    static unsigned int NetToLocal(unsigned int x)
    {
        return ntohl(x);
    }

    static long NetToLocal(long x)
    {
        return (sizeof(x) == 4) ? ntohl(x) : (long) ntohll(x);
    }
    static unsigned long NetToLocal(unsigned long x)
    {
        return (sizeof(x) == 4) ? ntohl(x) : (unsigned long) ntohll(x);
    }
    static long long NetToLocal(long long x)
    {
        return ntohll(x);
    }
    static unsigned long long NetToLocal(unsigned long long x)
    {
        return ntohll(x);
    }

    template <typename T>
    static void NetToLocal(T* value)
    {
        *value = NetToLocal(*value);
    }

    static void NetToLocal(float* value)
    {
        if (IsLittleEndian())
            Swap(value);
    }

    static void NetToLocal(double* value)
    {
        if (IsLittleEndian())
            Swap(value);
    }
};

#endif // TERA_COMMON_BASE_BYTE_ORDER_H_
