// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <glog/logging.h>

#include "common/file/recordio/record_io.h"

RecordWriter::RecordWriter() {}

RecordWriter::~RecordWriter() {}

bool RecordWriter::Reset(FileStream *file) {
    DCHECK(file != NULL);
    m_file = file;
    return true;
}

bool RecordWriter::WriteMessage(const ::google::protobuf::Message& message) {
    std::string output;
    if (!message.IsInitialized()) {
        LOG(WARNING) << "Missing required fields."
                     << message.InitializationErrorString();
        return false;
    }
    if (!message.AppendToString(&output)) {
        return false;
    }
    if (!WriteRecord(output.data(), output.size())) {
        return false;
    }
    return true;
}

bool RecordWriter::WriteRecord(const char *data, uint32_t size) {
    if (!Write(reinterpret_cast<char*>(&size), sizeof(size))) {
        return false;
    }
    if (!Write(data, size)) {
        return false;
    }
    return true;
}

bool RecordWriter::WriteRecord(const std::string& data) {
    return WriteRecord(data.data(), data.size());
}

bool RecordWriter::Write(const char *data, uint32_t size) {
    uint32_t write_size = 0;
    while (write_size < size) {
        int32_t ret = m_file->Write(data + write_size, size - write_size);
        if (ret == -1) {
            LOG(ERROR) << "RecordWriter error.";
            return false;
        }
        write_size += ret;
    }
    m_file->Flush();

    return true;
}


RecordReader::RecordReader()
    : m_buffer_size(1 * 1024 * 1024) {
    m_buffer.reset(new char[m_buffer_size]);
}

RecordReader::~RecordReader() {
}

bool RecordReader::Reset(FileStream *file) {
    DCHECK(file != NULL);
    m_file = file;
    if (-1 == m_file->Seek(0, SEEK_END)) {
        LOG(ERROR) << "RecordReader Reset error.";
        return false;
    }
    m_file_size = m_file->Tell();
    if (-1 == m_file->Seek(0, SEEK_SET)) {
        LOG(ERROR) << "RecordReader Reset error.";
        return false;
    }
    return true;
}

int RecordReader::Next() {
    // read size
    int64_t ret = m_file->Tell();
    if (ret == -1) {
        LOG(ERROR) << "Tell error.";
        return -1;
    }

    if (ret == m_file_size) {
        return 0;
    } else if (m_file_size - ret >= static_cast<int64_t>(sizeof(m_data_size))) { // NO_LINT
        if (!Read(reinterpret_cast<char*>(&m_data_size), sizeof(m_data_size))) {
            LOG(ERROR) << "Read size error.";
            return -1;
        }
    }

    // read data
    ret = m_file->Tell();
    if (ret == -1) {
        LOG(ERROR) << "Tell error.";
        return -1;
    }

    if (ret >= m_file_size && m_data_size != 0) {
        LOG(ERROR) << "read error.";
        return -1;
    } else if (m_file_size - ret >= m_data_size) { // NO_LINT
        if (m_data_size > m_buffer_size) {
            while (m_data_size > m_buffer_size) {
                m_buffer_size *= 2;
            }
            m_buffer.reset(new char[m_buffer_size]);
        }

        if (!Read(m_buffer.get(), m_data_size)) {
            LOG(ERROR) << "Read data error.";
            return -1;
        }
    } else {
        LOG(ERROR) << "m_data_size of current record is invalid: "
                   << m_data_size << " bigger than "
                   << (m_file_size - ret);
        return -1;
    }

    return 1;
}

bool RecordReader::ReadMessage(::google::protobuf::Message *message) {
    std::string str(m_buffer.get(), m_data_size);
    if (!message->ParseFromArray(m_buffer.get(), m_data_size)) {
        LOG(WARNING) << "Missing required fields.";
        return false;
    }
    return true;
}

bool RecordReader::ReadNextMessage(::google::protobuf::Message *message) {
    while (Next() == 1) {
        std::string str(m_buffer.get(), m_data_size);
        if (message->ParseFromArray(m_buffer.get(), m_data_size)) {
            return true;
        }
    }
    return false;
}

bool RecordReader::ReadRecord(const char **data, uint32_t *size) {
    *data = m_buffer.get();
    *size = m_data_size;
    return true;
}

bool RecordReader::ReadRecord(std::string *data) {
    data->assign(m_buffer.get());
    return true;
}

bool RecordReader::Read(char *data, uint32_t size) {
    // Read
    uint32_t read_size = 0;
    while (read_size < size) {
        int64_t ret = m_file->Read(data + read_size, size - read_size);
        if (ret == -1) {
            LOG(ERROR) << "Read error.";
            return false;
        }
        read_size += ret;
    }

    return true;
}
