// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef LEVELDB_COMPRESSION_BMZ_CODEC_H
#define LEVELDB_COMPRESSION_BMZ_CODEC_H

#include <stdint.h>
#include <string>

namespace bmz {

class BmzCodec {
public:
    BmzCodec();
    virtual ~BmzCodec();

    virtual bool SetArgs(const char *name, const char *value);
    virtual std::string GetType() { return "COMPRESS:BMZ"; }

    virtual bool Compress(const char *input,
                          size_t input_size,
                          char *output,
                          size_t *output_size);

    virtual bool Uncompress(const char *input,
                            size_t input_size,
                            char *output,
                            size_t *output_size);

private:
    char *m_workmem;
    size_t m_workmem_size;
    size_t m_fp_len;
};

} // namespace bmz

#endif  // LEVELDB_COMPRESSION_BMZ_CODEC_H
