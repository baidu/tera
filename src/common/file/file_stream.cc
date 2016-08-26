// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/file/file_stream.h"

#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/string_ext.h"

DECLARE_int32(file_op_retry_times);

FileStream::FileStream()
    : fp_(NULL) {}

bool FileStream::Open(const std::string& file_path, FileOpenMode flag,
                            FileErrorCode* error_code) {
    std::string open_mode = FileOpenModeToString(flag);
    fp_ = fopen(file_path.c_str(), open_mode.c_str());
    if (fp_ == NULL) {
        SetErrorCode(error_code, kFileErrOpenFail);
        return false;
    }

    SetErrorCode(error_code, kFileSuccess);
    return true;
}

bool FileStream::Close(FileErrorCode* error_code) {
    if (fp_ == NULL) {
        SetErrorCode(error_code, kFileErrNotOpen);
        return false;
    }

    Flush();

    if (fclose(fp_) != 0) {
        LOG(ERROR) << "fail to close file, errno: " << strerror(errno);
        SetErrorCode(error_code, kFileErrClose);
        return false;
    }
    return true;
}

int64_t FileStream::Read(void* buffer, int64_t buffer_size,
                         FileErrorCode* error_code) {
    if (fp_ == NULL) {
        SetErrorCode(error_code, kFileErrNotOpen);
        return -1;
    }
    if (!buffer || buffer_size <= 0) {
        SetErrorCode(error_code, kFileErrParameter);
        return -1;
    }

    int64_t read_bytes = fread(buffer, 1, static_cast<size_t>(buffer_size), fp_);
    bool success = true;
    if (read_bytes != buffer_size) {
        if (ferror(fp_)) success = false;
    }
    if (success) {
        SetErrorCode(error_code, kFileSuccess);
    } else {
        LOG(ERROR) << "error occurred in reader, errono:" << strerror(errno);
        SetErrorCode(error_code, kFileErrRead);
    }
    return success ? read_bytes : -1;
}

int64_t FileStream::Write(const void* buffer, int64_t buffer_size,
                          FileErrorCode* error_code) {
    if (fp_ == NULL) {
        SetErrorCode(error_code, kFileErrNotOpen);
        return -1;
    }
    if (!buffer || buffer_size <= 0) {
        SetErrorCode(error_code, kFileErrParameter);
        return -1;
    }

    int64_t total_size = 0;
    const char* byte_buf = static_cast<const char*>(buffer);
    for (int32_t retry = 0; retry < FLAGS_file_op_retry_times; ++retry) {
        size_t expect_size = static_cast<size_t>(buffer_size - total_size);
        if (expect_size == 0u) break;

        size_t ret_size = fwrite(byte_buf + total_size, 1, expect_size, fp_);
        total_size += static_cast<int64_t>(ret_size);

        if (ret_size < expect_size) {
            LOG(ERROR) << "write down enough bytes and fail, ["
                << "buffur_size = " << buffer_size
                << ", writen down total size = " << total_size
                << ", expect_size = " << expect_size
                << ", this writen size = " << ret_size
                << "], reason: " << strerror(errno);
            CHECK(ferror(fp_)) << "file writer is broken";
            if (errno != EINTR && ret_size == 0u) break;
        }
    }
    if (total_size == buffer_size) {
        SetErrorCode(error_code, kFileSuccess);
    } else {
        LOG(ERROR) << "error occurred in writter, errono:" << strerror(errno);
        SetErrorCode(error_code, kFileErrWrite);
    }
    return total_size > 0 ? total_size : -1;
}

bool FileStream::Flush() {
    if (fp_ ==  NULL) {
        return false;
    }
    return  (fflush(fp_) == 0) && (fsync(fileno(fp_)) == 0);
}

int64_t FileStream::Seek(int64_t offset, int32_t origin, FileErrorCode* error_code) {
    if (fp_ == NULL) {
        return -1;
    }
    if (fseeko(fp_, offset, origin) < 0) {
//         SetErrorCode(error_code, errono);
        return -1;
    }
    return Tell(error_code);
}

int64_t FileStream::Tell(FileErrorCode* error_code) {
    if (fp_ == NULL) {
        return -1;
    }
    int64_t ret = ftello(fp_);
    if (ret < 0) {
        // set error code
    } else {
        // set error code
    }
    return ret;
}

int64_t FileStream::GetSize(const std::string& file_path,
                            FileErrorCode* error_code) {
    int64_t file_size = -1;
    int32_t file_exist = access(file_path.c_str(), F_OK);
    if (file_exist != 0) {
        SetErrorCode(error_code, kFileErrNotExit);
        if (error_code == NULL) {
            LOG(ERROR) << "file " << file_path << " not exists";
        }
        return file_size;
    }

    struct stat stat_buf;
    if (stat(file_path.c_str(), &stat_buf) < 0) {
        file_size = -1;
//         SetErrorCode(error_code, errno);
        LOG(ERROR) << "stat error for " << file_path;
        return file_size;
    }

    if (S_ISDIR(stat_buf.st_mode)) {
        file_size = -1;
//         SetErrorCode(error_code, errno);
        LOG(ERROR) << "input file name " << file_path
            << " is a directory";
        return file_size;
    } else {
        SetErrorCode(error_code, kFileSuccess);
        file_size = static_cast<int64_t>(stat_buf.st_size);
    }
    return file_size;
}

int32_t FileStream::ReadLine(void* buffer, int32_t max_size) {
    if (fp_ == NULL) {
        return -1;
    }

    if (!buffer || max_size <= 0) return -1;
    char* read_buffer = static_cast<char*>(buffer);
    off64_t org_offest = ftello(fp_);
    if (org_offest < 0)
        return -1;
    char* readed_buffer = fgets(read_buffer, max_size, fp_);
    if (readed_buffer == NULL) {
        if (feof(fp_)) {
            return 0;
        } else {
            return -1;
        }
    } else {
        off64_t new_offset = ftello(fp_);
        if (new_offset < 0)
            return -1;
        return new_offset - org_offest;
    }
}

int32_t FileStream::ReadLine(std::string* result) {
    result->resize(0);

    while (true) {
        const int32_t kBufferSize = 4 * 1024;
        std::string buffer(kBufferSize, 0);
        int32_t bytes = ReadLine(StringAsArray(&buffer), kBufferSize);

        if (bytes < 0) {
            result->resize(0);
            return bytes;
        }
        if (bytes == 0) {
            return result->size();
        }
        if (bytes > 0) {
            buffer.resize(bytes);
            result->append(buffer);
            if (StringEndsWith(*result, "\n")) {
                return result->size();
            }
        }
    }
}

void FileStream::SetErrorCode(FileErrorCode* error_code, FileErrorCode code) {
    if (error_code) {
        *error_code = code;
    }
}

std::string FileStream::FileOpenModeToString(uint32_t flag) {
    std::string mode;
    if ((flag & FILE_READ) == FILE_READ) {
        mode += "r";
    } else if ((flag & FILE_WRITE) == FILE_WRITE) {
        mode += "w";
    } else if ((flag & FILE_APPEND) == FILE_APPEND) {
        mode += "a";
    }
    return mode;
}
