// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstring>
#include <stdio.h>
#include <malloc.h>
#include "table/format.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
#ifndef NDEBUG
  const size_t original_size = dst->size();
#endif
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::InvalidArgument("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

char* DirectIOAlign(RandomAccessFile* file, uint64_t offset, size_t len,
                    DirectIOArgs* direct_io_args) {
  assert(direct_io_args);
  assert(offset >= 0 && len >= 0);
  /* use this formula you will find the number Y, 
   * which Y is the first bigger number than X, at the same time Y is the power of 2.
   * Y = (X + (2^n - 1)) & (~(2^n - 1))
   * 
   * this function , accept len [is X] to find aligned_len [is Y + alignment], 
   * accept offset [is X] to find aligned_offset [is Y - alignment]
   * 
   * example: offset = 123 len = 610 - 123
   *         123              610
   *          [................]                   need buffer
   *
   * aligned_offset = 0, aligned_len = 1024
   *    [.................|.................]      alloc buffer x % alignment == 0
   *    x               x+512              x+1024  
   */
  size_t alignment = file->GetRequiredBufferAlignment();
  direct_io_args->aligned_len = alignment + (len % alignment > 0 ?
    (len + alignment - 1) & (~(alignment - 1)) : len);

  direct_io_args->aligned_offset = offset > 0 ? 
    ((offset + alignment - 1) & (~(alignment -1))) - alignment : 0;
  return (char*)memalign(alignment, direct_io_args->aligned_len);
}

void FreeBuf(char* buf, bool use_direct_io_read) {
  if (buf != NULL) {
    if (use_direct_io_read) {
      free(buf);
    } else {
      delete[] buf;
    }
    buf = NULL;
  }
}

Status ReadSstFile(RandomAccessFile* file,
                   bool use_direct_io_read,
                   uint64_t offset,
                   size_t len,
                   Slice* contents,
                   char** buf) {
  Status s;
  if (use_direct_io_read) {
    // calc and malloc align memory for direct io
    DirectIOArgs read_args;
    *buf = DirectIOAlign(file, offset, len, &read_args);
    if (*buf == NULL) {
      return Status::Corruption("direct io allgn failed");
    }
    //read to align buf
    s = file->Read(read_args.aligned_offset, read_args.aligned_len, contents, *buf);
    if (!s.ok()) {
      FreeBuf(*buf, use_direct_io_read);
      return s;
    }
    // reset 'contents' to actual block contents
    uint64_t align_offset = offset - read_args.aligned_offset;
    if (contents->size() >= align_offset + len) {
      contents->remove_prefix(align_offset);
      contents->remove_suffix(contents->size() - len);
    } else {
      FreeBuf(*buf, use_direct_io_read);
      return Status::Corruption("direct io read contents size invalid");
    }
  } else {
    *buf = new char[len];
    s = file->Read(offset, len, contents, *buf);
    if (!s.ok()) {
      FreeBuf(*buf, use_direct_io_read);
      return s;
    }
  }
  return s;
}

Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  size_t len = n + kBlockTrailerSize;
  uint64_t offset = handle.offset();
  Slice contents;
  char* buf = NULL;
  const Options& db_opt = *(options.db_opt);
  Status s = ReadSstFile(file, db_opt.use_direct_io_read, offset, len, &contents, &buf);
  if (!s.ok()) {
    return s;
  }
  s = ParseBlock(n, offset, options, contents, result);
  FreeBuf(buf, db_opt.use_direct_io_read);
  return s;
}

Status ParseBlock(size_t n,
                  size_t offset,
                  const ReadOptions& options,
                  Slice contents,
                  BlockContents* result) {

  if (contents.size() != n + kBlockTrailerSize) {
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      char err[128] = {'\0'};
      sprintf(err, "block checksum mismatch: crc %u, actual %u, offset %lu, size %lu",
              crc, actual, offset, n + kBlockTrailerSize);
      return Status::Corruption(Slice(err, strlen(err)));
    }
  }


  switch (data[n]) {
    case kNoCompression: {
      char* buf = new char[n];
      memcpy(buf, contents.data(), n);
      result->data = Slice(buf, n);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        return Status::Corruption("Snappy: corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] ubuf;
        return Status::Corruption("Snappy: corrupted compressed block contents");
      }
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    case kBmzCompression: {
        size_t uncompressed_size = 4 * 1024 * 2; // should be doubled block size, say > 4K * 2
        std::vector<char> uncompressed_buffer;
        uncompressed_buffer.resize(uncompressed_size);
        if (!port::Bmz_Uncompress(data, n, &uncompressed_buffer[0],
                                  &uncompressed_size)) {
            return Status::Corruption("Bmz: corrupted compressed block contents");
        }
        result->data = Slice(&uncompressed_buffer[0], uncompressed_size);
        result->heap_allocated = true;
        result->cachable = true;
        break;
    }
    case kLZ4Compression: {
        size_t uncompressed_size = 4 * 1024 * 2; // should be doubled block size, say > 4K * 2
        std::vector<char> uncompressed_buffer;
        uncompressed_buffer.resize(uncompressed_size);
        if (!port::Lz4_Uncompress(data, n, &uncompressed_buffer[0],
                                  &uncompressed_size)) {
            return Status::Corruption("LZ4: corrupted compressed block contents");
        }
        result->data = Slice(&uncompressed_buffer[0], uncompressed_size);
        result->heap_allocated = true;
        result->cachable = true;
        break;
    }
    default:
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb
