// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#define private public

#include "scan_impl.h"

#include "gtest/gtest.h"

using std::string;

namespace tera {

class ScanDescImplTest : public ::testing::Test, public ScanDescImpl {
 public:
  ScanDescImplTest() : ScanDescImpl("row") {
    CreateSchema();
    SetTableSchema(table_schema_);
  }

  ~ScanDescImplTest() {}

  void CreateSchema() {
    table_schema_.set_enable_hash(false);
    table_schema_.set_name("linkcache");
    LocalityGroupSchema* lg = table_schema_.add_locality_groups();
    lg->set_name("lg0");
    ColumnFamilySchema* cf = table_schema_.add_column_families();
    cf->set_name("cf0");
    cf->set_locality_group("lg0");
    cf->set_type("int32");

    cf = table_schema_.add_column_families();
    cf->set_name("cf1");
    cf->set_locality_group("lg0");
    cf->set_type("uint64");

    cf = table_schema_.add_column_families();
    cf->set_name("cf2");
    cf->set_locality_group("lg0");
    cf->set_type("binary");
  }

  const TableSchema& GetSchema() const { return table_schema_; }

 private:
  TableSchema table_schema_;
};
}  // namespace tera
