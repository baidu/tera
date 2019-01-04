// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: tianye15@baidu.com

#include <string>
#include <vector>

#include "sdk/sdk_utils.h"
#include "tera.h"
#include "sdk/scan_impl.h"
#include "gtest/gtest.h"
#include "mock_table.h"

using std::string;
using std::vector;

namespace tera {
class HashTableTest : public ::testing::Test {
 public:
  HashTableTest() {}
  ~HashTableTest() {}
  std::function<std::string(const std::string&)> hash_method;
  MockHashTable hash_table_;
};

TEST_F(HashTableTest, MutateReaderTest) {
  auto mu = dynamic_cast<RowMutationImpl*>(hash_table_.NewRowMutation("Happy Water"));
  EXPECT_EQ(mu->RowKey(), std::string("Happy Water"));
  EXPECT_EQ(mu->InternalRowKey(), hash_table_.hash_method_("Happy Water"));
  auto rd = dynamic_cast<RowReaderImpl*>(hash_table_.NewRowReader("Happy Water"));
  EXPECT_EQ(rd->RowKey(), std::string("Happy Water"));
  EXPECT_EQ(rd->InternalRowKey(), hash_table_.hash_method_("Happy Water"));
  mu->Reset("coffee");
  EXPECT_EQ(mu->RowKey(), std::string("coffee"));
  EXPECT_EQ(mu->InternalRowKey(), hash_table_.hash_method_("coffee"));
  hash_table_.is_hash_table_ = false;
  delete mu;
  delete rd;
  mu = dynamic_cast<RowMutationImpl*>(hash_table_.NewRowMutation("Happy Water"));
  EXPECT_EQ(mu->RowKey(), std::string("Happy Water"));
  EXPECT_EQ(mu->InternalRowKey(), std::string("Happy Water"));
  rd = dynamic_cast<RowReaderImpl*>(hash_table_.NewRowReader("Happy Water"));
  EXPECT_EQ(rd->RowKey(), std::string("Happy Water"));
  EXPECT_EQ(rd->InternalRowKey(), std::string("Happy Water"));
  delete mu;
  delete rd;
}

TEST_F(HashTableTest, ScanTest) {
  ScanDescriptor desc("coffee");
  desc.SetEnd("Water");
  ErrorCode err;
  auto stream_async_hash = dynamic_cast<ResultStreamImpl*>(hash_table_.Scan(desc, &err));
  auto* desc_impl = stream_async_hash->GetScanDesc();
  EXPECT_EQ(desc_impl->GetStartRowKey(), hash_table_.hash_method_("coffee"));
  EXPECT_EQ(desc_impl->GetEndRowKey(), hash_table_.hash_method_("Water"));
  hash_table_.is_hash_table_ = false;
  auto stream_async_normal = dynamic_cast<ResultStreamImpl*>(hash_table_.Scan(desc, &err));
  desc_impl = stream_async_normal->GetScanDesc();
  EXPECT_EQ(desc_impl->GetStartRowKey(), std::string("coffee"));
  EXPECT_EQ(desc_impl->GetEndRowKey(), std::string("Water"));
}

TEST_F(HashTableTest, GenerateHashDelimiters) {
  vector<string> delimiters;
  GenerateHashDelimiters(2, &delimiters);
  EXPECT_EQ(delimiters.size(), 1);
  EXPECT_EQ(delimiters[0], std::string{"7fffffffffffffff"});
  GenerateHashDelimiters(1, &delimiters);
  EXPECT_TRUE(delimiters.empty());
  GenerateHashDelimiters(-2029, &delimiters);
  EXPECT_TRUE(delimiters.empty());
  GenerateHashDelimiters(32, &delimiters);
  EXPECT_EQ(delimiters.size(), 31);
  EXPECT_EQ(std::stoul(delimiters[2], 0, 16) - std::stoul(delimiters[1], 0, 16),
            std::stoul(delimiters[1], 0, 16) - std::stoul(delimiters[0], 0, 16));
  EXPECT_EQ(
      0xFFFFFFFFFFFFFFFFul / (std::stoul(delimiters[2], 0, 16) - std::stoul(delimiters[1], 0, 16)),
      32);
}

TEST_F(HashTableTest, MurmurhashMethodTest) {
  EXPECT_EQ(hash_table_.GetHashMethod()("2UhGxUShBZr1ZBIJqz8g"),
            std::string{"91e5c5cc21866b182UhGxUShBZr1ZBIJqz8g"});
  EXPECT_EQ(hash_table_.GetHashMethod()("192zw9vc84dQzR2ptLCm"),
            std::string{"0bb8c3e200317607192zw9vc84dQzR2ptLCm"});
  EXPECT_EQ(hash_table_.GetHashMethod()("H4alYbi4yMfyNnDBtt7m"),
            std::string{"184b87380d6b1877H4alYbi4yMfyNnDBtt7m"});
  EXPECT_EQ(hash_table_.GetHashMethod()("96SIlaNgrQYyuVjt2Mbc"),
            std::string{"7e66efcac7a4de6996SIlaNgrQYyuVjt2Mbc"});
  EXPECT_EQ(hash_table_.GetHashMethod()("0FTkCOZmZcJRaacH9mLX"),
            std::string{"07832f92ed43e9740FTkCOZmZcJRaacH9mLX"});
  EXPECT_EQ(hash_table_.GetHashMethod()("wjwMOOh77asWARPKPZBl"),
            std::string{"ab159d1c102d398fwjwMOOh77asWARPKPZBl"});
  EXPECT_EQ(hash_table_.GetHashMethod()("KBnFAGwiJRBjrKE4hR9h"),
            std::string{"102bd69736f79785KBnFAGwiJRBjrKE4hR9h"});
  EXPECT_EQ(hash_table_.GetHashMethod()("76IfzK8E9y5IF41d0N4J"),
            std::string{"9de642f80be247c676IfzK8E9y5IF41d0N4J"});
  EXPECT_EQ(hash_table_.GetHashMethod()("lcrQ3nWyzfNE7TG7i4nl"),
            std::string{"ede00a2cbdcd1d63lcrQ3nWyzfNE7TG7i4nl"});
  EXPECT_EQ(hash_table_.GetHashMethod()("3G4XIeg6L963ws670jwe"),
            std::string{"9debe002db27ca393G4XIeg6L963ws670jwe"});
  EXPECT_EQ(hash_table_.GetHashMethod()("bhyKndJqNFLDCtmcg4g7"),
            std::string{"cc78b8dd79999489bhyKndJqNFLDCtmcg4g7"});
  EXPECT_EQ(hash_table_.GetHashMethod()("qwibdpkqfa5MOuwYiN4g"),
            std::string{"e2d20de35e14606eqwibdpkqfa5MOuwYiN4g"});
  EXPECT_EQ(hash_table_.GetHashMethod()("IoZl7jnqJKp4fYwaamAZ"),
            std::string{"9d635c9af1e44681IoZl7jnqJKp4fYwaamAZ"});
  EXPECT_EQ(hash_table_.GetHashMethod()("bRttFpTudIQk1OJv3oyQ"),
            std::string{"124538f8da7a6f84bRttFpTudIQk1OJv3oyQ"});
  EXPECT_EQ(hash_table_.GetHashMethod()("ap2C59zqWvRyRx2OUrs2"),
            std::string{"986eb90637360c00ap2C59zqWvRyRx2OUrs2"});
  EXPECT_EQ(hash_table_.GetHashMethod()("yIbxvdQdS8TBSD4tJXlT"),
            std::string{"87b9f235973943c2yIbxvdQdS8TBSD4tJXlT"});
  EXPECT_EQ(hash_table_.GetHashMethod()("Vh0Q2CxhXR0VRu4AtVG9"),
            std::string{"f322fa051195cdbdVh0Q2CxhXR0VRu4AtVG9"});
  EXPECT_EQ(hash_table_.GetHashMethod()("T5M7Hu3SuZoPGbN8nIHa"),
            std::string{"983bf8eee743fa1bT5M7Hu3SuZoPGbN8nIHa"});
  EXPECT_EQ(hash_table_.GetHashMethod()("Sb2CcBfN21pGlUmiSHhK"),
            std::string{"30cb6b5fb2525f05Sb2CcBfN21pGlUmiSHhK"});
  EXPECT_EQ(hash_table_.GetHashMethod()("qKHg43YHRyUblS9oUXBT"),
            std::string{"1d0c8442749505ceqKHg43YHRyUblS9oUXBT"});
  EXPECT_EQ(hash_table_.GetHashMethod()("w4XgzUg1gRectsjM4aEp"),
            std::string{"31642d6f4f6a2d67w4XgzUg1gRectsjM4aEp"});
  EXPECT_EQ(hash_table_.GetHashMethod()("0SbYixWcXXcHGNLN54c3"),
            std::string{"aa6cbe0341e42f830SbYixWcXXcHGNLN54c3"});
  EXPECT_EQ(hash_table_.GetHashMethod()("vZJ9zTljUr6QFku7EkCq"),
            std::string{"860600849ef6838fvZJ9zTljUr6QFku7EkCq"});
  EXPECT_EQ(hash_table_.GetHashMethod()("VarWftftOIB6bsGH9hOF"),
            std::string{"8e785c31f128a833VarWftftOIB6bsGH9hOF"});
  EXPECT_EQ(hash_table_.GetHashMethod()("V80PBkjgU7oJ8GYM7d7M"),
            std::string{"210dd5d1f5e839efV80PBkjgU7oJ8GYM7d7M"});
  EXPECT_EQ(hash_table_.GetHashMethod()("VJeTDjiO4kBxudvWAWIp"),
            std::string{"5e76614754cfcd5bVJeTDjiO4kBxudvWAWIp"});
  EXPECT_EQ(hash_table_.GetHashMethod()("AJUm9zqDsA4UQnxQ6SGh"),
            std::string{"f0c5c28cd56398f6AJUm9zqDsA4UQnxQ6SGh"});
  EXPECT_EQ(hash_table_.GetHashMethod()("g9lL6kWChRtRk85rUO98"),
            std::string{"272ba12c212a8844g9lL6kWChRtRk85rUO98"});
  EXPECT_EQ(hash_table_.GetHashMethod()("e5bv8EmZOR1UpBbN4Eh9"),
            std::string{"f189f6f85893524be5bv8EmZOR1UpBbN4Eh9"});
  EXPECT_EQ(hash_table_.GetHashMethod()("K3Ny3yiZJROTY15Imrca"),
            std::string{"fadeaf75c024c176K3Ny3yiZJROTY15Imrca"});
}
}  // namespace tera
