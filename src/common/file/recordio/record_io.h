// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_FILE_RECORDIO_RECORD_IO_H_
#define TERA_COMMON_FILE_RECORDIO_RECORD_IO_H_

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
    FileStream* file_;
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
    FileStream* file_;
    scoped_array<char> buffer_;
    uint32_t file_size_;
    uint32_t buffer_size_;
    uint32_t data_size_;
};

#endif // TERA_COMMON_FILE_RECORDIO_RECORD_IO_H_
