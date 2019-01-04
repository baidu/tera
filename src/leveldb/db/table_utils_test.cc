// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/table_utils.h"

#include "util/testharness.h"

namespace leveldb {

class TableUtilsTest {};

TEST(TableUtilsTest, HeadAndDumpManifest) {
  // the hex content of the manifest
  uint8_t content[] = {
      0x6e, 0x11, 0x5f, 0x7f, 0x2d, 0x00, 0x01, 0x9a, 0x80, 0x40, 0x01, 0x19, 0x74, 0x65,
      0x72, 0x61, 0x2e, 0x54, 0x65, 0x72, 0x61, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x43,
      0x6f, 0x6d, 0x70, 0x61, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x81, 0x80, 0x40, 0x02, 0x00,
      0x81, 0x80, 0x40, 0x03, 0x02, 0x81, 0x80, 0x40, 0x04, 0x00, 0x0a,
  };

  std::string manifest_file("./MANIFEST-000001");
  FILE* file = fopen(manifest_file.c_str(), "wb");
  size_t len = sizeof(content) / sizeof(uint8_t);
  fwrite(content, 1, len, file);
  fclose(file);

  leveldb::Env* env = leveldb::Env::Default();

  bool ret = false;
  ret = DumpFile(env, manifest_file);
  ASSERT_TRUE(ret);
  remove(manifest_file.c_str());
}
}

int main(int argc, char** argv) { return leveldb::test::RunAllTests(); }
