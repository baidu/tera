// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef LEVELDB_COMPRESS_BM_DIFF_H
#define LEVELDB_COMPRESS_BM_DIFF_H

#define BM_E_OK    0
#define BM_E_ERROR           (-1)
#define BM_E_INPUT_OVERRUN   (-4)
#define BM_E_OUTPUT_OVERRUN  (-5)
#define BM_E_ERROR_FORMAT    (-6)

namespace bmz {

/**
 * @brief 计算bm编码所需要的辅助内存大小
 *
 * @param in_len 输入长度
 * @param fp_len 探针长度
 *
 * @return 辅助内存大小
 */
size_t  bm_encode_worklen(size_t in_len, size_t fp_len);

/**
 * @brief 计算bm解码所需要辅助内存大小
 *
 * @param out_len 输出长度
 *
 * @return 辅助内存大小
 */
size_t bm_decode_worklen(size_t out_len);


/**
 * @brief bm压缩函数
 *
 * @param src 源地址
 * @param in_len 源长度
 * @param dst 目标地址
 * @param out_len_p 存储目标长度的整数地址，成功后长度值修改为实际的压缩后长度
 * @param fp_len 探针长度，长度为[fp_len, 2*fp_len]之间的公共串部分被压缩，长度大于2*fp_len则必定被压缩
 * @param work_mem 辅助内存，用于存储HashTable.其长度依赖fp_len和in_len
 *
 * @return 成功返回BMZ_E_OK,失败返回相应的错误码
 */

int bm_encode_body(const void *src, size_t in_len,
                   void *dst, size_t *out_len_p,
                   size_t fp_len,  void *work_mem);

/**
 * @brief bm解压函数
 *
 * @param src 源地址
 * @param in_len 源长度
 * @param dst 目标地址
 * @param out_len_p 目标长度指针,只有当目标长度大于源长度+1时才是最安全
 *
 * @return 成功后返回BMZ_E_OK,失败返回相应的错误码
 */

int bm_decode_body(const void *src, size_t in_len,
                   void *dst, size_t *out_len_p);

} // namespace bmz

#endif // LEVELDB_COMPRESS_BM_DIFF_H
