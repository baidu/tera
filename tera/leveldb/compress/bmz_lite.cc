// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stdint.h>

#include "compress/bmz_lite.h"
#include "compress/bmdiff.h"
#include "compress/minilzo.h"

namespace bmz {

size_t lz_encode_worklen(size_t in_len) {
    return  LZO1X_MEM_COMPRESS;
}

size_t bmz_pack_worklen(size_t in_len, size_t fp_len)
{
    size_t  lz_wlen = lz_encode_worklen(in_len) + 16;
    size_t  bm_wlen = bm_encode_worklen(in_len, fp_len);

    size_t  auxlen = bm_wlen > lz_wlen?bm_wlen:lz_wlen;

    return  in_len + auxlen + 1;
}

size_t bmz_unpack_worklen(size_t out_len)
{
    return out_len + 1;
}


int bmz_init()
{
    int ret = lzo_init();
    return ret == LZO_E_OK?BMZ_E_OK:BMZ_E_ERROR;
}

#define BM_ALIGN(_mem_, _n_)  (uint8_t *)(_mem_) + _n_ - (((size_t)(_mem_))%(_n_))

int bmz_pack(const void *in,  size_t in_len, void *out, size_t *out_len_p,
            size_t  fp_len, void *work_mem) {

    size_t  tlen = in_len + 1;
    uint8_t *dst = (uint8_t *)work_mem;
    uint8_t *aux_mem = dst + tlen;
    uint8_t *work_mem_aligned = BM_ALIGN(aux_mem, 8);

    ///进行bm压缩
    int ret = bm_encode_body(in, in_len, dst, &tlen, fp_len, work_mem_aligned);
    if(ret != BMZ_E_OK)  return ret;

    ///进行lzo压缩
    lzo_uint    olen = *out_len_p;
    ret = lzo1x_1_compress(dst, tlen, (uint8_t *)out, &olen, work_mem_aligned);
    *out_len_p = olen;
    return ret;
}

int bmz_unpack(const void *in, size_t in_len, void *out, size_t *out_len_p, void *work_mem)
{
    uint8_t *dst = (uint8_t *)work_mem;
    size_t  tlen = *out_len_p + 1;

    int ret;
    lzo_uint   lzo_tlen = tlen;
    ret = lzo1x_decompress((uint8_t *)in, in_len, dst, &lzo_tlen, NULL);
    tlen = lzo_tlen;

    if (ret != BMZ_E_OK) return ret;
    return bm_decode_body(dst, tlen, out, out_len_p);
}

} // namespace bmz
