// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "cookie.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <iostream>

#include <glog/logging.h>

#include "common/file/file_path.h"
#include "common/file/file_stream.h"
#include "common/file/recordio/record_io.h"
#include "proto/table_meta.pb.h"
#include "utils/crypt.h"
#include "utils/string_util.h"

namespace tera {
namespace sdk {

/*
 * If there is overtime/legacy cookie-lock-file, then delete it.
 * Normally, process which created cookie-lock-file would delete it after dumped.
 * But if this process crashed before delete cookie-lock-file.
 * Another process could call this function to delete legacy cookie-lock-file.
 *
 * create_time = [time of cookie-lock-file creation]
 * timeout     = [input parameter `timeout_secondes']
 *
 * create_time + timeout > current_time :=> legacy cookie-lock-file
 */
static void DeleteLegacyCookieLockFile(const std::string& lock_file, int timeout_seconds) {
    struct stat lock_stat;
    int ret = stat(lock_file.c_str(), &lock_stat);
    if (ret == -1) {
        return;
    }
    time_t curr_time = time(NULL);
    if (((unsigned int)curr_time - lock_stat.st_atime) > timeout_seconds) {
        // It's a long time since creation of cookie-lock-file, dumping must has done.
        // So, deletes the cookie-lock-file is safe.
        int errno_saved = -1;
        if (unlink(lock_file.c_str()) == -1) {
            errno_saved = errno;
            LOG(INFO) << "[UTILS COOKIE] fail to delete cookie-lock-file: " << lock_file
                       << ". reason: " << strerror(errno_saved);
        }
    }
}

static void CloseAndRemoveCookieLockFile(int lock_fd, const std::string& cookie_lock_file) {
    if (lock_fd < 0) {
        return;
    }
    close(lock_fd);
    if (unlink(cookie_lock_file.c_str()) == -1) {
        int errno_saved = errno;
        LOG(INFO) << "[UTILS COOKIE] fail to delete cookie-lock-file: " << cookie_lock_file
                   << ". reason: " << strerror(errno_saved);
    }
}

static bool CalculateChecksumOfData(std::fstream& outfile, long size, std::string* hash_str) {
    // 100 MB, (100 * 1024 * 1024) / 250 = 419,430
    // cookie文件中，每个tablet的缓存大小约为100~200 Bytes，不妨计为250 Bytes，
    // 那么，100MB可以支持约40万个tablets
    const long MAX_SIZE = 100 * 1024 * 1024;

    if(size > MAX_SIZE || size <= 0) {
        LOG(INFO) << "[UTILS COOKIE] invalid size : " << size;
        return false;
    }
    if(hash_str == NULL) {
        LOG(INFO) << "[UTILS COOKIE] input argument `hash_str' is NULL";
        return false;
    }
    std::string data(size, '\0');
    outfile.read(const_cast<char*>(data.data()), size);
    if(outfile.fail()) {
        LOG(INFO) << "[UTILS COOKIE] fail to read cookie file";
        return false;
    }
    if (GetHashString(data, 0, hash_str) != 0) {
        return false;
    }
    return true;
}

static bool AppendChecksumToCookie(const std::string& cookie_file) {
    std::fstream outfile(cookie_file.c_str(), std::ios_base::in | std::ios_base::out);
    int errno_saved = errno;
    if(outfile.fail()) {
        LOG(INFO) << "[UTILS COOKIE] fail to open " << cookie_file.c_str()
            << " " << strerror(errno_saved);
        return false;
    }

    // get file size, in bytes
    outfile.seekp(0, std::ios_base::end);
    long file_size = outfile.tellp();
    if(file_size < HASH_STRING_LEN) {
        LOG(INFO) << "[UTILS COOKIE] invalid file size: " << file_size;
        return false;
    }

    // calculates checksum according to cookie file content
    outfile.seekp(0, std::ios_base::beg);
    std::string hash_str;
    if(!CalculateChecksumOfData(outfile, file_size, &hash_str)) {
        return false;
    }
    LOG(INFO) << "[UTILS COOKIE] file checksum: " << hash_str;

    // append checksum to the end of cookie file
    outfile.seekp(0, std::ios_base::end);
    outfile.write(hash_str.c_str(), hash_str.length());
    if(outfile.fail()) {
        LOG(INFO) << "[UTILS COOKIE] fail to append checksum";
        return false;
    }
    outfile.close();
    return true;
}

static bool AddOtherUserWritePermission(const std::string& cookie_file) {
    struct stat st;
    int ret = stat(cookie_file.c_str(), &st);
    if(ret != 0) {
        return false;
    }
    if((st.st_mode & S_IWOTH) == S_IWOTH) {
        // other user has write permission already
        return true;
    }
    return chmod(cookie_file.c_str(),
             S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH) == 0;
}

static bool IsCookieChecksumRight(const std::string& cookie_file) {
    std::fstream outfile(cookie_file.c_str(), std::ios_base::in | std::ios_base::out);
    int errno_saved = errno;
    if(outfile.fail()) {
        LOG(INFO) << "[UTILS COOKIE] fail to open " << cookie_file.c_str()
            << " " << strerror(errno_saved);
        return false;
    }

    // gets file size, in bytes
    outfile.seekp(0, std::ios_base::end);
    long file_size = outfile.tellp();
    if(file_size < HASH_STRING_LEN) {
        LOG(INFO) << "[UTILS COOKIE] invalid file size: " << file_size;
        return false;
    }

    // calculates checksum according to cookie file content
    std::string hash_str;
    outfile.seekp(0, std::ios_base::beg);
    if(!CalculateChecksumOfData(outfile, file_size - HASH_STRING_LEN, &hash_str)) {
        return false;
    }

    // gets checksum in cookie file
    char hash_str_saved[HASH_STRING_LEN + 1] = {'\0'};
    outfile.read(hash_str_saved, HASH_STRING_LEN);
    if(outfile.fail()) {
        int errno_saved = errno;
        LOG(INFO) << "[UTILS COOKIE] fail to get checksum: " << strerror(errno_saved);
        return false;
    }

    outfile.close();
    return strncmp(hash_str.c_str(), hash_str_saved, HASH_STRING_LEN) == 0;
}

bool RestoreCookie(const std::string cookie_file, bool delete_broken_cookie, SdkCookie *cookie) {
    if (!IsExist(cookie_file)) {
        // cookie file is not exist
        LOG(INFO) << "[UTILS COOKIE] cookie file not found";
        return false;
    }
    if(!IsCookieChecksumRight(cookie_file)) {
        if (!delete_broken_cookie) {
            // do nothing
        } else if (unlink(cookie_file.c_str()) == -1) {
            int errno_saved = errno;
            LOG(INFO) << "[UTILS COOKIE] fail to delete broken cookie file: " << cookie_file
                << ". reason: " << strerror(errno_saved);
        } else {
            LOG(INFO) << "[UTILS COOKIE] delete broken cookie file: " << cookie_file;
        }
        return false;
    }

    FileStream fs;
    if (!fs.Open(cookie_file, FILE_READ)) {
        LOG(INFO) << "[UTILS COOKIE] fail to open " << cookie_file;
        return false;
    }
    RecordReader record_reader;
    record_reader.Reset(&fs);
    if (!record_reader.ReadNextMessage(cookie)) {
        LOG(INFO) << "[UTILS COOKIE] fail to parse sdk cookie, file: " << cookie_file;
        return false;
    }
    fs.Close();
    return true;
}

void DumpCookie(const std::string& cookie_file,
                  const std::string& cookie_lock_file,
                  const SdkCookie& cookie) {
    int cookie_lock_file_timeout = 10; // in seconds
    DeleteLegacyCookieLockFile(cookie_lock_file, cookie_lock_file_timeout);
    int lock_fd = open(cookie_lock_file.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (lock_fd == -1) {
        int errno_saved = errno;
        if (errno != EEXIST) {
            LOG(INFO) << "[UTILS COOKIE] failed to create cookie-lock-file: " << cookie_lock_file
                      << ". reason: " << strerror(errno_saved);
        }
        return;
    }

    FileStream fs;
    if (!fs.Open(cookie_file, FILE_WRITE)) {
        LOG(INFO) << "[UTILS COOKIE] fail to open " << cookie_file;
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }
    RecordWriter record_writer;
    record_writer.Reset(&fs);
    if (!record_writer.WriteMessage(cookie)) {
        LOG(INFO) << "[UTILS COOKIE] fail to write cookie file " << cookie_file;
        fs.Close();
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }
    fs.Close();

    if(!AppendChecksumToCookie(cookie_file)) {
        LOG(INFO) << "[UTILS COOKIE] fail to append checksum to cookie file " << cookie_file;
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }
    if(!AddOtherUserWritePermission(cookie_file)) {
        LOG(INFO) << "[UTILS COOKIE] fail to chmod cookie file " << cookie_file;
        CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
        return;
    }

    CloseAndRemoveCookieLockFile(lock_fd, cookie_lock_file);
    LOG(INFO) << "[UTILS COOKIE] update local cookie success: " << cookie_file;
}

bool DumpCookieFile(const std::string& cookie_file) {
    SdkCookie cookie;
    if (!RestoreCookie(cookie_file, false, &cookie)) {
        std::cerr << "invalid cookie file" << std::endl;
        return false;
    }

    for (int i = 0; i < cookie.tablets_size(); ++i) {
        const TabletMeta& meta = cookie.tablets(i).meta();
        const std::string& start_key = meta.key_range().key_start();
        std::cout << meta.path()
            << " [" << DebugString(start_key)
            << " : " << DebugString(meta.key_range().key_end()) << "]" << std::endl;
    }
    return true;
}

bool FindKeyInCookieFile(const std::string& cookie_file, const std::string& key) {
    SdkCookie cookie;
    if (!RestoreCookie(cookie_file, false, &cookie)) {
        std::cerr << "invalid cookie file:" << std::endl;
        return false;
    }
    for (int i = 0; i < cookie.tablets_size(); ++i) {
        const TabletMeta& meta = cookie.tablets(i).meta();
        const std::string& start_key = meta.key_range().key_start();
        const std::string& end_key = meta.key_range().key_end();
        if ((key.compare(start_key) >= 0)
            && (key.compare(end_key) < 0 || end_key == "")) {
            std::cout << meta.path() << " @ " << meta.server_addr() << std::endl;
            return true;
        }
    }
    return false;
}

} // namespace sdk
} // namespace tera
