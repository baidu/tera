// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_MOCK_TABLET_IO_H_
#define TERA_IO_MOCK_TABLET_IO_H_

#include "io/tablet_io.h"

#include "gmock/gmock.h"

namespace tera {
namespace io {

class MockTabletIO : public TabletIO {
 public:
  MockTabletIO() : TabletIO("", "", "", 0, 0) {}
  MOCK_CONST_METHOD0(GetCompactStatus, CompactStatus());
  MOCK_CONST_METHOD0(GetSchema, TableSchema());
  MOCK_METHOD10(Load, bool(const TableSchema& schema, const std::string& key_start,
                           const std::string& key_end, const std::string& path,
                           const std::vector<uint64_t>& parent_tablets,
                           std::map<uint64_t, uint64_t> snapshots, leveldb::Logger* logger,
                           leveldb::Cache* block_cache, leveldb::TableCache* table_cache,
                           StatusCode* status));
  MOCK_METHOD1(Unload, bool(StatusCode* status));
  MOCK_METHOD2(Split, bool(std::string* split_key, StatusCode* status));
  MOCK_METHOD1(Compact, bool(StatusCode* status));
  MOCK_METHOD1(GetDataSize, int64_t(StatusCode* status));
  MOCK_METHOD3(GetDataSize, int64_t(const std::string& start_key, const std::string& end_key,
                                    StatusCode* status));
  MOCK_METHOD4(Read, bool(const leveldb::Slice& key, std::string* value, uint64_t snapshot_id,
                          StatusCode* status));
  MOCK_METHOD5(Read, bool(const KeyList& key_list, BytesList* value_list, uint32_t* success_num,
                          uint64_t snapshot_id, StatusCode* status));
  MOCK_METHOD5(ReadCells, bool(const RowReaderInfo& row_reader, RowResult* value_list,
                               uint64_t snapshot_id, StatusCode* status, int64_t timeout_ms));
  MOCK_METHOD5(Write, bool(std::vector<const RowMutationSequence*>* row_mutation_vec,
                           std::vector<StatusCode>* status_vec, bool is_instant,
                           WriteCallback callback, StatusCode* status));
  MOCK_METHOD6(Scan, bool(const ScanOption& option, KeyValueList* kv_list, uint32_t* read_row_count,
                          uint32_t* read_bytes, bool* complete, StatusCode* status));
  MOCK_METHOD3(ScanRows, bool(const ScanTabletRequest* request, ScanTabletResponse* response,
                              google::protobuf::Closure* done));
};

}  // namespace io
}  // namespace tera

#endif  // TERA_IO_MOCK_TABLET_IO_H_
