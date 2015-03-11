// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef LEVELDB_COMPRESS_BMZ_LITE_H
#define LEVELDB_COMPRESS_BMZ_LITE_H

#include <stdlib.h>
#include <stdint.h>

#define BMZ_E_OK    (0)
#define BMZ_E_ERROR (-1)
#define BMZ_E_INPUT_OVERRUN   (-4)
#define BMZ_E_OUTPUT_OVERRUN  (-5)
#define BMZ_E_ERROR_FORMAT    (-6)

namespace bmz {

/**
 * @brief bmz初始化，主要初始化lzo
 *
 * @return 成功返回BMZ_E_OK
 */

int bmz_init();

/**
 * @brief 计算bmz编码所需要辅助内存大小
 *
 * @param in_len 输入长度
 * @param fp_len 探针长度
 *
 * @return 辅助内存长度
 */

size_t bmz_pack_worklen(size_t in_len, size_t fp_len);

/**
 * @brief 计算bmz解码所需要辅助内存大小
 *
 * @param in_len 输出长度
 *
 * @return 辅助内存长度
 */

size_t bmz_unpack_worklen(size_t out_len);

/**
 * @brief bmz编码
 *
 * @param in 源地址
 * @param in_len 源长度
 * @param out 目标地址
 * @param out_len_p 目标长度指针
 * @param fp_len 探针长度
 * @param work_mem 辅助内存,内存大小通过bmz_pack_worklen计算
 *
 * @return 成功返回BMZ_E_OK，失败返回相应的错误码
 */

int bmz_pack(const void *in,  size_t in_len, void *out, size_t *out_len_p, size_t	fp_len, void *work_mem);

/**
 * @brief bmz解码
 *
 * @param in 源地址
 * @param in_len 源长度
 * @param out 目标地址
 * @param out_len_p 目标长度指针
 * @param work_mem 辅助内存，内存大小通过bmz_unpack_worklen计算
 *
 * @return 成功返回BMZ_E_OK,失败返回相关的错误码
 */

int bmz_unpack(const void *in, size_t in_len, void *out, size_t	*out_len_p,  void *work_mem);

} // namespace bmz

#endif ///LEVELDB_COMPRESS_BMZ_LITE_H
