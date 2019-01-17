// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"
#include "util/testharness.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed. max tag number = 1<<20, min tag number = 1
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFileForCompat = 6,
  kNewFileForCompat = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9,
  kNewFile = 10,
  kDeletedFile = 11,
  kNewFileInfo = 12,
  kSstFileDataSize = 13,

  // no more than 1<<20
  kMaxTag = 1 << 20,
};
enum EditTestTag {
  kErrorTag = 200,
};

class VersionEditTest : public VersionEdit {
 public:
  VersionEditTest() : has_error_tag_(false) { Clear(); }
  void AddErrorTag(const std::string& str) {
    has_error_tag_ = true;
    error_code_ = str;
  }
  void EncodeToTestTag(std::string* dst) const {
    if (has_error_tag_) {
      std::string str;
      PutLengthPrefixedSlice(&str, error_code_);

      PutVarint32(dst, str.size() + kMaxTag);
      PutVarint32(dst, kErrorTag);
      dst->append(str.data(), str.size());
    }
  }
  void EncodeToOld(std::string* dst, bool with_sst, bool with_data_size) {
    DumpToOldFormat();
    if (has_comparator_) {
      PutVarint32(dst, kComparator);
      PutLengthPrefixedSlice(dst, comparator_);
    }
    if (has_log_number_) {
      PutVarint32(dst, kLogNumber);
      PutVarint64(dst, log_number_);
    }
    if (has_next_file_number_) {
      PutVarint32(dst, kNextFileNumber);
      PutVarint64(dst, next_file_number_);
    }
    if (has_last_sequence_) {
      PutVarint32(dst, kLastSequence);
      PutVarint64(dst, last_sequence_);
    }
    if (!with_sst) {
      return;
    }
    for (uint32_t i = 0; i < 5; i++) {
      FileMetaData f;
      f.number = 100 + i;
      f.file_size = 200 + i;
      f.data_size = f.file_size;
      f.smallest = InternalKey("apple", 300 + i, kTypeValue);
      f.largest = InternalKey("zookeeper", 400 + i, kTypeDeletion);
      EncodeSstFile(i, f, dst, with_data_size);
    }
  }

  void EncodeSstFile(uint32_t level, const FileMetaData& f, std::string* dst, bool with_data_size) {
    std::string str;
    PutVarint32(&str, level);  // level
    PutVarint64(&str, f.number);
    PutVarint64(&str, f.file_size);
    PutLengthPrefixedSlice(&str, f.smallest.Encode());
    PutLengthPrefixedSlice(&str, f.largest.Encode());
    PutVarint32(&str, 0);  // put f.smallest_fake
    PutVarint32(&str, 0);  // put f.largest_fake

    PutVarint32(dst, str.size() + kMaxTag);
    PutVarint32(dst, kNewFile);
    dst->append(str.data(), str.size());
    // record sst FileData
    if (with_data_size) {
      str.clear();
      PutVarint64(&str, f.data_size);
      PutVarint32(dst, str.size() + kMaxTag);
      PutVarint32(dst, kSstFileDataSize);
      dst->append(str.data(), str.size());
    }
  }

  void DumpToOldFormat() {
    has_comparator_ = HasComparator();
    comparator_ = GetComparatorName();

    has_log_number_ = HasLogNumber();
    log_number_ = GetLogNumber();

    has_next_file_number_ = HasNextFileNumber();
    next_file_number_ = GetNextFileNumber();

    has_last_sequence_ = HasLastSequence();
    last_sequence_ = GetLastSequence();
  }

 private:
  bool has_error_tag_;
  std::string error_code_;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;
};

static void TestEncodeDecode(const VersionEditTest& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEditTest parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2);
  ASSERT_EQ(encoded, encoded2);
}
static void CreateEditContent(VersionEditTest* edit) {
  for (int i = 0; i < 5; i++) {
    TestEncodeDecode(*edit);
    edit->AddFile(i, 100 + i, 200 + i, InternalKey("aoo", 300 + i, kTypeValue),
                  InternalKey("zoo", 400 + i, kTypeDeletion));
    edit->DeleteFile(i, 500 + i);
    edit->SetCompactPointer(i, InternalKey("x00", 600 + i, kTypeValue));
  }

  edit->SetComparatorName("test_nil_cmp");
  edit->SetLogNumber(700);
  edit->SetNextFile(800);
  edit->SetLastSequence(900);
  TestEncodeDecode(*edit);
}
static void CreateOldEncodedContent(VersionEditTest* edit, std::string* dst, bool with_sst,
                                    bool with_data_size) {
  edit->SetComparatorName("test_nil_cmp");
  edit->SetLogNumber(700);
  edit->SetNextFile(800);
  edit->SetLastSequence(900);
  TestEncodeDecode(*edit);
  edit->EncodeToOld(dst, with_sst, with_data_size);
}
static void CreateEditWithSstDetail(VersionEditTest* edit) {
  for (int i = 0; i < 5; i++) {
    TestEncodeDecode(*edit);
    FileMetaData f;
    f.number = 100 + i;
    f.file_size = 200 + i;
    f.data_size = f.file_size;
    f.smallest = InternalKey("apple", 300 + i, kTypeValue);
    f.largest = InternalKey("zookeeper", 400 + i, kTypeDeletion);
    f.del_percentage = 20 + i;
    f.ttl_percentage = 50 + i;
    f.check_ttl_ts = 1000000000 + i;
    edit->AddFile(i, f);
    edit->DeleteFile(i, 500 + i);
    edit->SetCompactPointer(i, InternalKey("x00", 600 + i, kTypeValue));
  }

  edit->SetComparatorName("test_nil_cmp");
  edit->SetLogNumber(700);
  edit->SetNextFile(800);
  edit->SetLastSequence(900);
  TestEncodeDecode(*edit);
}
TEST(VersionEditTest, EncodeFileInfoTag) {
  VersionEditTest edit;
  CreateEditWithSstDetail(&edit);
  fprintf(stderr, "%s\n", edit.DebugString().c_str());
}
TEST(VersionEditTest, OldFormatRead) {
  VersionEditTest edit;
  std::string c1, c3;
  CreateOldEncodedContent(&edit, &c1, false, false);
  edit.EncodeTo(&c3);  // dump into new format

  VersionEditTest parsed;
  Status s = parsed.DecodeFrom(c1);  // use new Decode to parse old format
  ASSERT_TRUE(s.ok()) << s.ToString();
  std::string c2;
  parsed.EncodeTo(&c2);

  ASSERT_EQ(c2, c3);
  fprintf(stderr, "%s\n", parsed.DebugString().c_str());
}

TEST(VersionEditTest, DecodeFormatWithoutSstFileDataSize) {
  VersionEditTest edit;
  std::string c1, c3;
  CreateOldEncodedContent(&edit, &c1, true, false);
  edit.EncodeTo(&c3);  // dump into new format

  VersionEditTest parsed;
  Status s = parsed.DecodeFrom(c1);  // use new Decode to parse old format
  ASSERT_TRUE(s.ok()) << s.ToString();
  std::string c2;
  parsed.EncodeTo(&c2);

  ASSERT_NE(c2, c3);
  fprintf(stderr, "%s\n", parsed.DebugString().c_str());
}

TEST(VersionEditTest, EncodeUnknowTag) {
  VersionEditTest edit;
  CreateEditContent(&edit);
  std::string err = "VersionEdit unknow tag";
  edit.AddErrorTag(err);

  // dump into c1
  std::string c1;
  edit.EncodeTo(&c1);
  edit.EncodeToTestTag(&c1);

  // skip unknow tag, dump others into c2
  std::string c2;
  VersionEditTest parsed;
  Status s = parsed.DecodeFrom(c1);
  ASSERT_TRUE(s.ok()) << s.ToString();
  TestEncodeDecode(parsed);
  parsed.EncodeTo(&c2);

  // check skip correctness
  VersionEditTest edit_src;
  CreateEditContent(&edit_src);
  std::string c3;
  edit_src.EncodeTo(&c3);
  ASSERT_EQ(c2, c3);
  fprintf(stderr, "%s\n", parsed.DebugString().c_str());
}
TEST(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;

  VersionEditTest edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(3, kBig + 300 + i, kBig + 400 + i, InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion));
    edit.DeleteFile(4, kBig + 700 + i);
    edit.SetCompactPointer(i, InternalKey("x", kBig + 900 + i, kTypeValue));
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}

}  // namespace leveldb

int main(int argc, char** argv) { return leveldb::test::RunAllTests(); }
