// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/file/recordio/record_io.h"

#include <string>

#include "common/file/file_stream.h"
#include "thirdparty/gtest/gtest.h"

#include "common/file/recordio/document.pb.h"


class RecordIOTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_reader.reset(new RecordReader());
        m_writer.reset(new RecordWriter());

        r1.set_docid(10);
        r1.mutable_links()->add_forward(20);
        r1.mutable_links()->add_forward(40);
        r1.mutable_links()->add_forward(60);
        r1.add_name();
        r1.mutable_name(0)->add_language();
        r1.mutable_name(0)->mutable_language(0)->set_code("en-us");
        r1.mutable_name(0)->mutable_language(0)->set_country("us");
        r1.mutable_name(0)->add_language();
        r1.mutable_name(0)->mutable_language(1)->set_code("en");
        r1.mutable_name(0)->set_url("http://A");
        r1.add_name();
        r1.mutable_name(1)->set_url("http://B");
        r1.add_name();
        r1.mutable_name(2)->add_language();
        r1.mutable_name(2)->mutable_language(0)->set_code("en-gb");
        r1.mutable_name(2)->mutable_language(0)->set_country("gb");

        r2.set_docid(20);
        r2.mutable_links()->add_backward(10);
        r2.mutable_links()->add_backward(30);
        r2.mutable_links()->add_forward(80);
        r2.add_name();
        r2.mutable_name(0)->set_url("http://C");

        r3.set_docid(30);
        r3.mutable_links()->add_backward(100);
        r3.add_name();
        r3.mutable_name(0)->set_url("http://D");
    }
    virtual void TearDown() {
        m_reader.reset();
        m_writer.reset();
    }

protected:
    scoped_ptr<RecordReader> m_reader;
    scoped_ptr<RecordWriter> m_writer;
    Document r1;
    Document r2;
    Document r3;
};

// TEST_F(RecordIOTest, TestMessage) {
//     FileStream file;
//     ASSERT_TRUE(file.Open("./test.dat", FILE_APPEND));

//     ASSERT_TRUE(m_writer->Reset(&file));

//     std::string r1_str = r1.SerializeAsString();
//     std::string r2_str = r2.SerializeAsString();
//     std::string r3_str = r3.SerializeAsString();

//     for (uint32_t i = 0; i < 20; ++i) {
//         ASSERT_TRUE(m_writer->WriteMessage(r1));
//         ASSERT_TRUE(m_writer->WriteMessage(r2));
//     }
//     ASSERT_TRUE(m_writer->WriteMessage(r3));

//     ASSERT_TRUE(file.Close());

//     ASSERT_TRUE(file.Open("./test.dat", FILE_READ));

//     ASSERT_TRUE(m_reader->Reset(&file));

//     ASSERT_EQ(1, m_reader->Next());
//     Document message;
//     ASSERT_TRUE(m_reader->ReadMessage(&message));
//     std::string content;
//     ASSERT_TRUE(message.SerializeToString(&content));
//     ASSERT_EQ(r3_str, content);

//     for (uint32_t i = 0; i < 40; ++i) {
//         ASSERT_EQ(1, m_reader->Next());

//         Document message;
//         ASSERT_TRUE(m_reader->ReadMessage(&message));

//         std::string content;
//         ASSERT_TRUE(message.SerializeToString(&content));
//         if (i % 2 == 0) {
//             ASSERT_EQ(r2_str, content);
//         } else {
//             ASSERT_EQ(r1_str, content);
//         }
//     }

//     ASSERT_EQ(0, m_reader->Next());
//     ASSERT_TRUE(file.Close());
// }

// TEST_F(RecordIOTest, TestNextMessage) {
//     FileStream file;
//     ASSERT_TRUE(file.Open("./test.dat",FILE_APPEND));

//     ASSERT_TRUE(m_writer->Reset(&file));

//     std::string r1_str = r1.SerializeAsString();
//     std::string r2_str = r2.SerializeAsString();
//     std::string r3_str = r3.SerializeAsString();

//     for (uint32_t i = 0; i < 20; ++i) {
//         ASSERT_TRUE(m_writer->WriteMessage(r1));
//         ASSERT_TRUE(m_writer->WriteMessage(r2));
//     }
//     ASSERT_TRUE(m_writer->WriteMessage(r3));

//     ASSERT_TRUE(file.Close());

//     ASSERT_TRUE(file.Open("./test.dat", FILE_READ));

//     ASSERT_TRUE(m_reader->Reset(&file));

//     Document message;
//     ASSERT_TRUE(m_reader->ReadNextMessage(&message));
//     std::string content;
//     ASSERT_TRUE(message.SerializeToString(&content));
//     ASSERT_EQ(r3_str, content);

//     for (uint32_t i = 0; i < 40; ++i) {
//         Document message;
//         ASSERT_TRUE(m_reader->ReadNextMessage(&message));

//         std::string content;
//         ASSERT_TRUE(message.SerializeToString(&content));
//         if (i % 2 == 0) {
//             ASSERT_EQ(r2_str, content);
//         } else {
//             ASSERT_EQ(r1_str, content);
//         }
//     }

//     ASSERT_TRUE(file.Close());
// }

TEST_F(RecordIOTest, TestNextMessageSequnce) {
    FileStream file;
    ASSERT_TRUE(file.Open("./test.dat",FILE_APPEND));

    ASSERT_TRUE(m_writer->Reset(&file));

    std::string r1_str = r1.SerializeAsString();
    std::string r2_str = r2.SerializeAsString();
    std::string r3_str = r3.SerializeAsString();

    ASSERT_TRUE(m_writer->WriteMessage(r1));
    ASSERT_TRUE(m_writer->WriteMessage(r2));
    ASSERT_TRUE(m_writer->WriteMessage(r3));

    ASSERT_TRUE(file.Close());

    LOG(ERROR) << "WRITE";
    ASSERT_TRUE(file.Open("./test.dat", FILE_READ));

    ASSERT_TRUE(m_reader->Reset(&file));

    Document message;
    ASSERT_TRUE(m_reader->ReadNextMessage(&message));
    std::string content;
    ASSERT_TRUE(message.SerializeToString(&content));
    ASSERT_EQ(r1_str, content);

    message.Clear();
    ASSERT_TRUE(m_reader->ReadNextMessage(&message));

    ASSERT_TRUE(message.SerializeToString(&content));
    ASSERT_EQ(r2_str, content);

    message.Clear();
    ASSERT_TRUE(m_reader->ReadNextMessage(&message));

    ASSERT_TRUE(message.SerializeToString(&content));
    ASSERT_EQ(r3_str, content);


    ASSERT_TRUE(file.Close());
}

