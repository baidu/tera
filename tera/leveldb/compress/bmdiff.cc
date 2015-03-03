// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stdint.h>
#include "compress/minilzo.h"
#include "compress/bmdiff.h"

#define BM_NPOS   ((size_t)-1)

/* Initial bases for computing Rabin Karp rolling hash */
#define BM_B1 257
#define BM_B2 277
#define BM_M1 0xffff
#define BM_M2 (0xffff - 4)

#define BM_MASK16 0xffff
#define BM_MASK24 0xffffff
#define BM_MASK32 0xffffffff
#define BM_MASKSZ BM_MASK32

/* Escape character
 * Don't change without understanding BM_DECODE_POS
 */
#define BM_ESC 0xfe
#define BM_MAX_LEN 0xfdffffffffffull /* 253TB block should be big enough */
#define BM_MAX_1B  0xfd
#define BM_MAX_2B  0xfdff
#define BM_MAX_3B  0xfdffff
#define BM_MAX_4B  0xfdfffffful
#define BM_MAX_5B  0xfdffffffffull

/* VInt limits */
#define BM_MAX_V1B 0x7f
#define BM_MAX_V2B 0x3fff
#define BM_MAX_V3B 0x1fffff
#define BM_MAX_V4B 0xfffffff
#define BM_MAX_V5B 0x7ffffffffull
#define BM_MAX_V6B 0x3ffffffffffull

namespace bmz {

inline uint64_t rolling_hash_mask(const uint8_t* buf, size_t len,
            uint64_t b, uint64_t m) {

    uint64_t    h = 0, p = 1;
    while(len--){
        h += p *buf[len];
        h &= m;
        p *= b;
        p &= m;
    }
    return h;
}


inline uint64_t pow_mask(uint64_t x, uint64_t n, uint64_t m) {

    if(n == 0) return 1;
    if(n == 1) return (x&m);

    uint64_t sqr = (x*x)&m;
    uint64_t pow = pow_mask(sqr, n/2, m);

    if(n & 1) return ((pow*x) & m);

    return (pow&m);
}

inline uint64_t update_hash_mask(uint64_t h, int32_t in, int32_t out,
                     uint64_t pow_n, uint32_t b, uint32_t m) {

    h *= b;
    h -= (out * pow_n)&m;
    h += in;
    return (h & m);
}


inline size_t bmz_hash_mask(const void *in, size_t in_len, size_t b) {
    return rolling_hash_mask((uint8_t *)in, in_len, b, BM_MASKSZ);
}

/* Some prime numbers as candidates for RK hash bases in case of adaptation */
static size_t s_primes[] = {
    3,    5,    7,   11,   13,   17,   19,   23,   29,
    31,   37,   41,   43,   47,   53,   59,   61,   67,   71,
    73,   79,   83,   89,   97,  101,  103,  107,  109,  113,
    127,  131,  137,  139,  149,  151,  157,  163,  167,  173,
    179,  181,  191,  193,  197,  199,  211,  223,  227,  229,
    233,  239,  241,  251,  257,  263,  269,  271,  277,  281,
    283,  293,  307,  311,  313,  317,  331,  337,  347,  349,
    353,  359,  367,  373,  379,  383,  389,  397,  401,  409,
    419,  421,  431,  433,  439,  443,  449,  457,  461,  463,
    467,  479,  487,  491,  499,  503,  509,  521,  523,  541,
    547,  557,  563,  569,  571,  577,  587,  593,  599,  601,
    607,  613,  617,  619,  631,  641,  643,  647,  653,  659,
    661,  673,  677,  683,  691,  701,  709,  719,  727,  733,
    739,  743,  751,  757,  761,  769,  773,  787,  797,  809,
    811,  821,  823,  827,  829,  839,  853,  857,  859,  863,
    877,  881,  883,  887,  907,  911,  919,  929,  937,  941,
    947,  953,  967,  971,  977,  983,  991,  997, 1009, 1013,
    1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069,
    1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151,
    1153, 1163, 1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223,
    1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291,
    1297, 1301, 1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373,
    1381, 1399, 1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451,
    1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493, 1499, 1511,
    1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579, 1583,
    1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657,
    1663, 1667, 1669, 1693, 1697, 1699, 1709, 1721, 1723, 1733,
    1741, 1747, 1753, 1759, 1777, 1783, 1787, 1789, 1801, 1811,
    1823, 1831, 1847, 1861, 1867, 1871, 1873, 1877, 1879, 1889,
    1901, 1907, 1913, 1931, 1933, 1949, 1951, 1973, 1979, 1987,
    1993, 1997, 1999, 2003, 2011, 2017, 2027, 2029, 2039, 2053,
    2063, 2069, 2081, 2083, 2087, 2089, 2099, 2111, 2113, 2129,
    2131, 2137, 2141, 2143, 2153, 2161, 2179, 2203, 2207, 2213,
    2221, 2237, 2239, 2243, 2251, 2267, 2269, 2273, 2281, 2287,
    2293, 2297, 2309, 2311, 2333, 2339, 2341, 2347, 2351, 2357,
    2371, 2377, 2381, 2383, 2389, 2393, 2399, 2411, 2417, 2423,
    2437, 2441, 2447, 2459, 2467, 2473, 2477, 2503, 2521, 2531,
    2539, 2543, 2549, 2551, 2557, 2579, 2591, 2593, 2609, 2617,
    2621, 2633, 2647, 2657, 2659, 2663, 2671, 2677, 2683, 2687,
    2689, 2693, 2699, 2707, 2711, 2713, 2719, 2729, 2731, 2741,
    2749, 2753, 2767, 2777, 2789, 2791, 2797, 2801, 2803, 2819,
    2833, 2837, 2843, 2851, 2857, 2861, 2879, 2887, 2897, 2903,
    2909, 2917, 2927, 2939, 2953, 2957, 2963, 2969, 2971, 2999,
    3001, 3011, 3019, 3023, 3037, 3041, 3049, 3061, 3067, 3079,
    3083, 3089, 3109, 3119, 3121, 3137, 3163, 3167, 3169, 3181,
    3187, 3191, 3203, 3209, 3217, 3221, 3229, 3251, 3253, 3257,
    3259, 3271, 3299, 3301, 3307, 3313, 3319, 3323, 3329, 3331,
    3343, 3347, 3359, 3361, 3371, 3373, 3389, 3391, 3407, 3413,
    3433, 3449, 3457, 3461, 3463, 3467, 3469, 3491, 3499, 3511,
    3517, 3527, 3529, 3533, 3539, 3541, 3547, 3557, 3559, 3571,
    3581, 3583, 3593, 3607, 3613, 3617, 3623, 3631, 3637, 3643,
    3659, 3671, 3673, 3677, 3691, 3697, 3701, 3709, 3719, 3727,
    3733, 3739, 3761, 3767, 3769, 3779, 3793, 3797, 3803, 3821,
    3823, 3833, 3847, 3851, 3853, 3863, 3877, 3881, 3889, 3907,
    3911, 3917, 3919, 3923, 3929, 3931, 3943, 3947, 3967, 3989,
    4001, 4003, 4007, 4013, 4019, 4021, 4027, 4049, 4051, 4057,
    4073, 4079, 4091, 4093, 4099, 4111, 4127, 4129, 4133, 4139,
    4153, 4157, 4159, 4177, 4201, 4211, 4217, 4219, 4229, 4231,
    4241, 4243, 4253, 4259, 4261, 4271, 4273, 4283, 4289, 4297,
    4327, 4337, 4339, 4349, 4357, 4363, 4373, 4391, 4397, 4409,
    4421, 4423, 4441, 4447, 4451, 4457, 4463, 4481, 4483, 4493,
    4507, 4513, 4517, 4519, 4523, 4547, 4549, 4561, 4567, 4583,
    4591, 4597, 4603, 4621, 4637, 4639, 4643, 4649, 4651, 4657,
    4663, 4673, 4679, 4691, 4703, 4721, 4723, 4729, 4733, 4751,
    4759, 4783, 4787, 4789, 4793, 4799, 4801, 4813, 4817, 4831,
    4861, 4871, 4877, 4889, 4903, 4909, 4919, 4931, 4933, 4937,
    4943, 4951, 4957, 4967, 4969, 4973, 4987, 4993, 4999, 5003,
    5009, 5011, 5021, 5023, 5039, 5051, 5059, 5077, 5081, 5087,
    5099, 5101, 5107, 5113, 5119, 5147, 5153, 5167, 5171, 5179,
    5189, 5197, 5209, 5227, 5231, 5233, 5237, 5261, 5273, 5279,
    5281, 5297, 5303, 5309, 5323, 5333, 5347, 5351, 5381, 5387,
    5393, 5399, 5407, 5413, 5417, 5419, 5431, 5437, 5441, 5443,
    5449, 5471, 5477, 5479, 5483, 5501, 5503, 5507, 5519, 5521,
    5527, 5531, 5557, 5563, 5569, 5573, 5581, 5591, 5623, 5639,
    5641, 5647, 5651, 5653, 5657, 5659, 5669, 5683, 5689, 5693,
    5701, 5711, 5717, 5737, 5741, 5743, 5749, 5779, 5783, 5791,
    5801, 5807, 5813, 5821, 5827, 5839, 5843, 5849, 5851, 5857,
    5861, 5867, 5869, 5879, 5881, 5897, 5903, 5923, 5927, 5939,
    5953, 5981, 5987, 6007, 6011, 6029, 6037, 6043, 6047, 6053,
    6067, 6073, 6079, 6089, 6091, 6101, 6113, 6121, 6131, 6133,
    6143, 6151, 6163, 6173, 6197, 6199, 6203, 6211, 6217, 6221,
    6229, 6247, 6257, 6263, 6269, 6271, 6277, 6287, 6299, 6301,
    6311, 6317, 6323, 6329, 6337, 6343, 6353, 6359, 6361, 6367,
    6373, 6379, 6389, 6397, 6421, 6427, 6449, 6451, 6469, 6473,
    6481, 6491, 6521, 6529, 6547, 6551, 6553, 6563, 6569, 6571,
    6577, 6581, 6599, 6607, 6619, 6637, 6653, 6659, 6661, 6673,
    6679, 6689, 6691, 6701, 6703, 6709, 6719, 6733, 6737, 6761,
    6763, 6779, 6781, 6791, 6793, 6803, 6823, 6827, 6829, 6833,
    6841, 6857, 6863, 6869, 6871, 6883, 6899, 6907, 6911, 6917,
    6947, 6949, 6959, 6961, 6967, 6971, 6977, 6983, 6991, 6997,
    7001, 7013, 7019, 7027, 7039, 7043, 7057, 7069, 7079, 7103,
    7109, 7121, 7127, 7129, 7151, 7159, 7177, 7187, 7193, 7207,
    7211, 7213, 7219, 7229, 7237, 7243, 7247, 7253, 7283, 7297,
    7307, 7309, 7321, 7331, 7333, 7349, 7351, 7369, 7393, 7411,
    7417, 7433, 7451, 7457, 7459, 7477, 7481, 7487, 7489, 7499,
    7507, 7517, 7523, 7529, 7537, 7541, 7547, 7549, 7559, 7561,
    7573, 7577, 7583, 7589, 7591, 7603, 7607, 7621, 7639, 7643,
    7649, 7669, 7673, 7681, 7687, 7691, 7699, 7703, 7717, 7723,
    7727, 7741, 7753, 7757, 7759, 7789, 7793, 7817, 7823, 7829,
    7841, 7853, 7867, 7873, 7877, 7879, 7883, 7901, 7907, 7919,
    7927, 7933, 7937, 7949, 7951, 7963, 7993, 8009, 8011, 8017
};

inline size_t random_prime(size_t excluded) {
    const  size_t n = sizeof(s_primes) / sizeof(size_t);
    size_t i = 0;

    /* Don't really care if rand/srand are not thread-safe for
     * predictable results, as long as it doesn't crash.
     */
    static int sranded = 0;

    if (!sranded) {
        srand(time(NULL));
        sranded = 1;
    }

    for (; i < n; ++i) {
        size_t s = s_primes[rand() % n];
        if (s != excluded) return s;
    }

    return 8111;
}

static size_t bm_lut_size(size_t n) {
    uint64_t    x = n;
    do {
        /* power of 2 for mask mod */
        x |= (x>>1);
        x |= (x>>2);
        x |= (x>>4);
        x |= (x>>8);
        x |= (x>>16);
        x |= (x>>32);
        ++x;

    } while( x < n * 8/5);   /*avoid too much probes */
    return x;
}

typedef struct {
    size_t  hash;
    size_t  pos;
} BmLutEntry;


typedef struct {
    size_t  size;
    size_t  collisions;
    size_t  adapts;
    size_t  probes;
    BmLutEntry   *entries;
} BmLut;


static void init_bmlut(BmLut *table, size_t n, void *work_mem) {
    BmLutEntry *p, *endp;

    table->probes = table->adapts = table->collisions = 0;
    table->size = bm_lut_size(n);
    table->entries = (BmLutEntry *) work_mem;

    for (p = table->entries, endp = p + table->size; p < endp; ++p) {
        p->pos = BM_NPOS;
    }
}

inline void bm_lookup(BmLut *table, size_t hash, BmLutEntry*& entry) {
    size_t  size = table->size;
    size_t  idx = hash&(size - 1);
    entry = table->entries + idx;
}

inline void bm_encode_int(uint8_t *&op,  uint64_t n,  int width) {
    switch (width) {
        case 1:
            *op++ = (uint8_t) n;
            break;
        case 2:
            *op++ = (uint8_t)(n >> 8);
            *op++ = (uint8_t)(n);
            break;
        case 3:
            *op++ = (uint8_t)(n >> 16);
            *op++ = (uint8_t)(n >> 8);
            *op++ = (uint8_t)(n);
            break;
        case 4:
            *op++ = (uint8_t)(n >> 24);
            *op++ = (uint8_t)(n >> 16);
            *op++ = (uint8_t)(n >> 8);
            *op++ = (uint8_t)(n);
            break;
        case 5:
            *op++ = (uint8_t)(n >> 32);
            *op++ = (uint8_t)(n >> 24);
            *op++ = (uint8_t)(n >> 16);
            *op++ = (uint8_t)(n >> 8);
            *op++ = (uint8_t)(n);
            break;
        case 6:
            *op++ = (uint8_t)(n >> 40);
            *op++ = (uint8_t)(n >> 32);
            *op++ = (uint8_t)(n >> 24);
            *op++ = (uint8_t)(n >> 16);
            *op++ = (uint8_t)(n >> 8);
            *op++ = (uint8_t)(n);
            break;
        default:
            ;
    }
}

inline void bm_encode_vint(uint8_t *&op,  uint64_t n, int width) {
    switch (width) {
        case 1:
            *op++ = (uint8_t)((n) & 0x7f);
            break;
        case 2:
            *op++ = (uint8_t)(n | 0x80);
            *op++ = (uint8_t)((n >> 7) & 0x7f);
            break;
        case 3:
            *op++ = (uint8_t)(n | 0x80);
            *op++ = (uint8_t)((n >> 7) | 0x80);
            *op++ = (uint8_t)((n >> 14) & 0x7f);
            break;
        case 4:
            *op++ = (uint8_t)(n | 0x80);
            *op++ = (uint8_t)((n >> 7) | 0x80);
            *op++ = (uint8_t)((n >> 14) | 0x80);
            *op++ = (uint8_t)((n >> 21) & 0x7f);
            break;
        case 5:
            *op++ = (uint8_t)(n | 0x80);
            *op++ = (uint8_t)((n >> 7) | 0x80);
            *op++ = (uint8_t)((n >> 14) | 0x80);
            *op++ = (uint8_t)((n >> 21) | 0x80);
            *op++ = (uint8_t)((n >> 28) & 0x7f);
            break;
        case 6:
            *op++ = (uint8_t)(n | 0x80);
            *op++ = (uint8_t)((n >> 7) | 0x80);
            *op++ = (uint8_t)((n >> 14) | 0x80);
            *op++ = (uint8_t)((n >> 21) | 0x80);
            *op++ = (uint8_t)((n >> 28) | 0x80);
            *op++ = (uint8_t)((n >> 35) & 0x7f);
            break;
        case 7:
            *op++ = (uint8_t)(n | 0x80);
            *op++ = (uint8_t)((n >> 7) | 0x80);
            *op++ = (uint8_t)((n >> 14) | 0x80);
            *op++ = (uint8_t)((n >> 21) | 0x80);
            *op++ = (uint8_t)((n >> 28) | 0x80);
            *op++ = (uint8_t)((n >> 35) | 0x80);
            *op++ = (uint8_t)((n >> 42) & 0x7f);
            break;
        default:
            ;
    }
}

inline void bm_decode_int(uint8_t  *&ip,  uint64_t &n, int width) {
    switch (width) {
        case 1:
            n = *ip++;
            break;
        case 2:
            n = (*ip++ << 8);
            n |= *ip++;
            break;
        case 3:
            n = (*ip++ << 16);
            n |= (*ip++ << 8);
            n |= *ip++;
            break;
        case 4:
            n = (*ip++ << 24);
            n |= (*ip++ << 16);
            n |= (*ip++ << 8);
            n |= *ip++;
            break;
        case 5:
            n = ((uint64_t)*ip++);
            n |= (*ip++ << 24);
            n |= (*ip++ << 16);
            n |= (*ip++ << 8);
            n |= *ip++;
            break;
        case 6:
            n = ((uint64_t)*ip++ << 40);
            n |= ((uint64_t)*ip++ << 32);
            n |= (*ip++ << 24);
            n |= (*ip++ << 16);
            n |= (*ip++ << 8);
            n |= *ip++;
        default:
            ;
    }
}

inline void bm_decode_vint(uint8_t  *&ip,  uint64_t &n) {
    n = (*ip++ & 0x7f);
    if (ip[-1] & 0x80) {
        n |= ((*ip++ & 0x7f) << 7);
    }
    else return;
    if (ip[-1] & 0x80) {

        n |= ((*ip++ & 0x7f) << 14);
    } else return;
    if (ip[-1] & 0x80) {

        n |= ((*ip++ & 0x7f) << 21);
    } else return;
    if (ip[-1] & 0x80) {

        n |= ((uint64_t)(*ip++ & 0x7f) << 28);
    } else return;
    if (ip[-1] & 0x80) {

        n |= ((uint64_t)(*ip++ & 0x7f) << 35);
    } else return;
    if (ip[-1] & 0x80) {
        n |= ((uint64_t)(*ip++ & 0x7f) << 42);
    }
}

inline int bm_int_width(uint64_t n) {
    return  (n > BM_MAX_5B ? 6 :
                (n > BM_MAX_4B ? 5 :
                 (n > BM_MAX_3B ? 4 :
                  (n > BM_MAX_2B ? 3 :
                   (n > BM_MAX_1B ? 2 : 1)))));
}

inline int bm_vint_width(uint64_t n) {
    return (n > BM_MAX_V6B ? 7 :
                (n > BM_MAX_V5B ? 6 :
                 (n > BM_MAX_V4B ? 5 :
                  (n > BM_MAX_V3B ? 4 :
                   (n > BM_MAX_V2B ? 3 :
                    (n > BM_MAX_V1B ? 2 : 1))))));
}

inline int bm_encode_pass(uint8_t *out, void *in, size_t in_len) {
    *out++ = 0;
    memcpy(out, in, in_len);
    return BM_E_OK;
}

inline void bm_encode_output(uint8_t *&op, uint8_t *&ip, uint8_t *ip_end) {
    while(ip < ip_end){
        if(*ip == BM_ESC) *op++ = BM_ESC;
        *op++ = *ip++;
    }
}

inline void bm_encode_delta(uint8_t *&op, uint64_t ipos,
            uint64_t pos,  uint64_t len) {

    //format "BM_ESC|POS(INT)|LEN(VIN)"
    *op++ = BM_ESC;
    bm_encode_int(op, pos, bm_int_width(ipos));
    bm_encode_vint(op, len, bm_vint_width(len));
}

void bm_hash_check_body(const void *src, size_t in_len, size_t fp_len,
            size_t b, size_t m) {

    size_t pow_n, hash;
    uint8_t *in = (uint8_t *)src, *ip = in, *in_end = in + in_len;

    int errs = 0;
    hash = rolling_hash_mask(in, fp_len, b, m);
    pow_n = pow_mask(b, fp_len, m);

    for(;ip < in_end - fp_len - 1; ++ip){

        size_t h0 = rolling_hash_mask(ip + 1, fp_len, b, m);
        hash = update_hash_mask(hash, ip[fp_len], *ip, pow_n, b,m);

        if(h0 != hash){
            ++errs;
        }
    }

    fprintf(stderr, "Error :%d\n", errs);
}

size_t bm_encode_worklen(size_t in_len, size_t fp_len) {
    return  bm_lut_size(in_len/fp_len)*sizeof(BmLutEntry);
}


size_t bm_decode_worklen(size_t out_len) {
    return 0;
}

inline int bm_memcmp(uint8_t* src, uint8_t* des, size_t fp_len) {
    for(size_t i=0; i < fp_len; i++) {
        if (src[i] != des[i]) {
            return src[i] - des[i];
        }
    }
    return 0;
}

int bm_encode_body(const void *src, size_t in_len, void *dst, size_t *out_len_p,
                    size_t fp_len,  void *work_mem) {
    size_t  pow_n;
    size_t  i = 0, last_i = i, end_i = in_len - fp_len;
    size_t  boundary = i;
    size_t  nfps, hash, pos;

    uint8_t *in = (uint8_t *)src, *out = (uint8_t *)dst;
    uint8_t *ip = in, *last_ip = ip, *in_end = in + in_len;
    uint8_t *op = out;

    BmLut lut = {0, 0, 0, 0, NULL};
    BmLutEntry *entry = NULL;

    //如果输入串小于fp，则不进行任何编码
    if(in_len <= fp_len){
        return bm_encode_pass(in, out, in_len);
    }

    nfps = in_len/fp_len;
    init_bmlut(&lut, nfps, work_mem);

    //init hash
    size_t b = random_prime(0);
    hash = rolling_hash_mask(in + i, fp_len, b, BM_MASKSZ);
    pow_n = pow_mask(b, fp_len, BM_MASKSZ);

    *op++ = 1; //default compress type

    for( ; i<= end_i; ++i){

        if(i > last_i){

            bm_lookup(&lut, hash, entry);
            pos = entry->pos;

            if(pos != BM_NPOS){

                uint8_t *ip0 = in + pos, *cp = in + i, *cp0 = cp;
                uint8_t *ip1 = ip0 + fp_len, *cp1 = cp0 + fp_len;

                if(memcmp(ip0, cp0, fp_len) == 0){
                    //we have a match
                    size_t  mlen = 0;

                    //greedy backward up to fp_len - 1
                    for(ip =ip0; ip > in && cp > last_ip
                                && mlen < fp_len -1 && *--ip == *--cp; ++mlen);
                    cp = cp0 - mlen;

                    //保留原文
                    bm_encode_output(op, last_ip, cp);

                    pos = ip0 - mlen -in;
                    mlen += fp_len;

                    //greedy forward as much as possible
                    for(ip = ip1, cp = cp1; cp < in_end && *ip++ == *cp++; ++mlen);
                    bm_encode_delta(op, last_ip-in, pos, mlen);

                    last_ip = cp0 - (ip0 - (in + pos)) + mlen;
                    if(last_ip == in_end) break;
                    last_i = last_ip - in;
                }
            }
        }

        if(i == boundary){
            if(i <= last_i) bm_lookup(&lut, hash, entry);
            entry->hash = hash;
            entry->pos = i;
            boundary += fp_len;
        }

        if(i < end_i){
            hash = update_hash_mask(hash, in[i + fp_len], in[i], pow_n, b, BM_MASKSZ);
        }
    }

    bm_encode_output(op, last_ip, in_end);
    *out_len_p = op - out;
    return BM_E_OK;
}

int bm_decode_body(const void *src, size_t in_len, void *dst, size_t *out_len_p)
{
    uint8_t *in = (uint8_t *)src, *out = (uint8_t *)dst;
    uint8_t *ip = in, *last_ip = ip + 1, *op = out, *cp;
    uint8_t *in_end = ip + in_len;
    int     remains;
    ///no compress type
    if(*ip++ == 0){
        memcpy(out, ip, --in_len);
        *out_len_p = in_len;
        return BM_E_OK;
    }

    while(ip < in_end){

        if(*ip == BM_ESC){

            uint64_t len = ip - last_ip, pos = 0;
            if(len){
                memcpy(op, last_ip, len);
                op += len;
                last_ip = ip;
            }

            if(ip[1] == BM_ESC){
                ++last_ip;
                ip += 2;
            } else {
                ++ip;
                bm_decode_int(ip, pos,bm_int_width(op -out));
                bm_decode_vint(ip, len);
                last_ip = ip;
                cp = out + pos;
                while(len--) *op++ = *cp++;
            }
        }
        else ++ip;
    }

    remains = in_end - last_ip;
    memcpy(op, last_ip, remains);
    *out_len_p = op - out + remains;
    return BM_E_OK;
}

} // namespace bmz
