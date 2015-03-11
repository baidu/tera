// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string.h>
#include <assert.h>
#include "compress/bmz_lite.h"
#include "compress/bmz_codec.h"

namespace bmz {

BmzCodec::BmzCodec()
    : m_workmem(NULL), m_workmem_size(0), m_fp_len(5) {
    assert(bmz_init() == BMZ_E_OK);
}

BmzCodec::~BmzCodec() {
    if (m_workmem) {
        free((void *)m_workmem);
    }

    m_workmem_size = 0;
}

bool BmzCodec::SetArgs(const char *name, const char *value) {
    if (0 == strcasecmp(name, "fp-len")) {
        m_fp_len = atoi(value);
        return true;
    }
    return false;
}

bool BmzCodec::Compress(const char *input,
                        size_t input_size,
                        char *output,
                        size_t *output_size) {
    size_t inlen = input_size;

    // compute the size of auxiliary buffer
    size_t worklen = bmz_pack_worklen(inlen, m_fp_len);

    if (m_workmem_size < worklen) {
        if (m_workmem) {
            free((void *)m_workmem);
        }

        m_workmem = (char *)malloc(worklen);
        m_workmem_size = worklen;
    }

    if (bmz_pack(input, inlen, output, output_size, m_fp_len, m_workmem) !=
        BMZ_E_OK) {
        return false;
    }

    return true;
}

bool BmzCodec::Uncompress(const char *input,
                          size_t input_size,
                          char *output,
                          size_t *output_size) {
    // 检测内存如果预分配的内存不够，则重新分配
    size_t worklen = bmz_unpack_worklen(*output_size);
    if (worklen > m_workmem_size) {
        if (m_workmem) {
            free((void *)m_workmem);
        }

        m_workmem = (char *)malloc(worklen);
        m_workmem_size = worklen;
    }

    if (bmz_unpack(input, input_size, output, output_size, m_workmem) !=
        BMZ_E_OK) {
        return false;
    }

    return true;
}

} // namespace bmz
