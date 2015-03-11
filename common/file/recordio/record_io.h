// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef COMMON_FILE_RECORDIO_RECORD_IO_H
#define COMMON_FILE_RECORDIO_RECORD_IO_H

#include <string>

#include <google/protobuf/message.h>
#include "common/base/scoped_ptr.h"
#include "common/file/file_stream.h"

class RecordWriter {
public:
    RecordWriter();
    ~RecordWriter();

    bool Reset(FileStream *file);
    bool WriteMessage(const ::google::protobuf::Message& message);
    bool WriteRecord(const char *data, uint32_t size);
    bool WriteRecord(const std::string& data);

private:
    bool Write(const char *data, uint32_t size);

private:
    FileStream* m_file;
};

class RecordReader {
public:
    RecordReader();
    ~RecordReader();

    bool Reset(FileStream *file);

    // for ok, return 1;
    // for no more data, return 0;
    // for error, return -1;
    int Next();

    bool ReadMessage(::google::protobuf::Message *message);
    bool ReadNextMessage(::google::protobuf::Message *message);
    bool ReadRecord(const char **data, uint32_t *size);
    bool ReadRecord(std::string *data);

private:
    bool Read(char *data, uint32_t size);

private:
    FileStream* m_file;
    scoped_array<char> m_buffer;
    uint32_t m_file_size;
    uint32_t m_buffer_size;
    uint32_t m_data_size;
};

#endif // COMMON_FILE_RECORDIO_RECORD_IO_H
