// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  __NFS_API_NFS_H_
#define  __NFS_API_NFS_H_

#include <dirent.h>
#include <stdint.h>
#include <stddef.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace comcfg {
struct ConfigUnit;
}

namespace nfs {

typedef int (*AssignNamespaceIdFunc)(const char* path, int max_namespaces);
void SetAssignNamespaceIdFunc(AssignNamespaceIdFunc func);

struct NFSFILE;
struct NFSDIR;
struct ClientConfig;

struct NfsOptions {
    // required
    const char *master_ip;
    uint16_t    master_port;
    const char *username;
    const char *password;
    // optional
    const char *interface;      // network adaptor
    uint32_t   timeout;         // ms
    uint32_t   ebusy_timeout;   // ms
    uint32_t   tranbuf_size;    // bytes
    uint32_t   tranbuf_num;     // num of above
    uint16_t   write_cache;
    uint16_t   read_cache;
    uint16_t   is_read_primary_only;

    NfsOptions() : master_ip(NULL), master_port(0), username(NULL), password(NULL),
                   interface(NULL), timeout(180000), ebusy_timeout(180000),
                   tranbuf_size(4096), tranbuf_num(65536),
                   write_cache(1), read_cache(1), is_read_primary_only(0) {
    }
};

/**
 * Length of any path should be less than or equals to NFS_MAX_FILEPATH_LEN,
 * or else, api will return error, and the nfs errno will be set as ENAMETOOLONG.
 * NFS_MAX_FILEPATH_LEN is defined as 4095 defaultly.
 * That means, the max length of path is 4095.
 *
 * Length of any name should be less than or equals to NFS_MAX_FILENAME_LEN,
 * or else, api will return error, and the nfs errno will be set as ENAMETOOLONG.
 * NFS_MAX_FILENAME_LEN is defined as 255 defaultly.
 * That means, the max length of name is 255.
 */

/**
 * @biref   Init kylin log, or else it will be disabled defaultly.
 */
void InitKylinLog(int loglevel);

/**
 * @biref   Set comlog log level, or else it will be set as 16 defaultly.
 */
void SetComlogLevel(int loglevel);

/**
 * @brief   Get last errno of NFS.
 * @return  nfs errno
 */
int GetErrno();

/**
 * @brief   Print str of last errno of NFS. May has prefix.
 */
void Perror(const char* s = NULL);

/**
 * @brief   Init nfs client system. This interface is process-level.
 * @param   mountpoint
 * @param   master ip
 * @param   master port
 * @param   username
 * @param   password
 * @param   cache switch
 * @param   ethernet device, default NULL means eth*
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - mountpoint is invalid or argument is invalid
 *   ENAMETOOLONG - mountpoint too long
 *   ENOTDIR - mountpoint is not directory
 *   ENOENT  - mountpoint not exist
 *   EACCES  - user has no permission on mountpoint
 *   EIO     - other error
 */
int Init(const char* mountpoint, const char* master_ip, uint16_t master_port,
        const char* username, const char* password, int cache = 1,
        const char* interface = NULL);

/**
 * @brief   Init nfs client system. This interface is process-level.
 * @param   mountpoint
 * @param   master ip
 * @param   master port
 * @param   username
 * @param   password
 * @param   espaddr
 * @param   host_id_base
 * @param   host_id_num
 * @param   cache switch
 * @param   ethernet device, default NULL means eth*
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - mountpoint is invalid or argument is invalid
 *   ENAMETOOLONG - mountpoint too long
 *   ENOTDIR - mountpoint is not directory
 *   ENOENT  - mountpoint not exist
 *   EACCES  - user has no permission on mountpoint
 *   EIO     - other error
 */
int Init(const char* mountpoint, const char* master_ip, uint16_t master_port,
        const char* username, const char* password, uint64_t espaddr,
        int host_id_base, int host_id_num, int cache = 1, const char* interface = NULL);

/**
 * @brief   Init nfs client system. This interface is process-level.
 * @param   mountpoint
 * @param   config with the format of ConfigUnit
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - mountpoint is invalid or config invalid
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - mountpoint is not directory
 *   ENOENT  - mountpoint not exist
 *   EACCES  - user has no permission on mountpoint
 *   EIO     - other error
 */
int Init(const char* mountpoint, const comcfg::ConfigUnit& config);

/**
 * @brief   Init nfs client system. This interface is process-level.
 * @param   mountpoint
 * @param   config with the format of struct ClientConfig
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - mountpoint is invalid or config invalid
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - mountpoint is not directory
 *   ENOENT  - mountpoint not exist
 *   EACCES  - user has no permission on mountpoint
 *   EIO     - other error
 */
int Init(const char* mountpoint, const ClientConfig& config);

/**
 * @brief   Init nfs client system. This interface is process-level.
 * @param   mountpoint
 * @param   config with the format of struct NfsOptions
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - mountpoint is invalid or config invalid
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - mountpoint is not directory
 *   ENOENT  - mountpoint not exist
 *   EACCES  - user has no permission on mountpoint
 *   EIO     - other error
 */
int Init(const char* mountpoint, const NfsOptions& options);

/**
 * @brief   Init nfs client system. This interface is process-level.
 * @param   mountpoint
 * @param   config file path
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - mountpoint is invalid or config file path is invalid
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - mountpoint is not directory
 *   ENOENT  - mountpoint not exist
 *   EACCES  - user has no permission on mountpoint
 *   EIO     - other error
 */
int Init(const char* mountpoint, const char* _config_file_path);

/**
 * @brief   Destroy nfs client system. This interface is process-level.
 * @param   mountpoint
 * @param   config with the format of struct ClientConfig
 * @return
 *    0     - will always successed
 */
int Destroy();

/**
 * @brief   Change current working directory. This interface is process-level.
 * @param   path
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - path is not directory
 *   ENOENT  - path not exist
 *   EIO     - other error
 */
//int Chdir(const char* path);

/**
 * @brief   Get current working directory. The returned cwd string will be normalized.
 * @param   buf
 * @param   size
 * @return
 *  >=0     - on success, return the size of bytes stored in buf
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - buf is invalid, or NFS not inited
 *   ERANGE  - buf is not enough to store path
 */
//ssize_t Getcwd(char* buf, size_t size);

/**
 * @brief   Check permission or existence of path.
 * @param   path
 * @param   mode (F_OK or R_OK or W_OK or X_OK or any ORed mode among them)
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - path's parent is not directory
 *   ENOENT  - path not exist
 *   EACCES  - path has not the permission
 *   EIO     - other error
 */
int Access(const char* path, int mode);

/**
 * @brief   Make a directory.
 * @param   path
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   EEXIST  - path already exist
 *   ENOENT  - path's parents not exist
 *   ENOTDIR - path's parents is not directory // TAKE CARE
 *   EIO     - other error
 */
int Mkdir(const char* path);

/**
 * @brief   Make a directory.
 * @param   path
 * @param   is_spread_all_namespaces: whether do mkdir for all federations
 * @param   is_pass_when_already_exist: whether pass and continue when some one federation ack EEXIST
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   EEXIST  - path already exist
 *   ENOENT  - path's parents not exist
 *   ENOTDIR - path's parents is not directory // TAKE CARE
 *   EIO     - other error
 */
int Mkdir(const char* path, bool is_spread_all_namespaces, bool is_pass_when_already_exist);

/**
 * @brief   Remove a directory.
 * @param   path
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - path is not directory
 *   ENOENT  - path not exist
 *   ENOTEMPTY - dir has children other than . and ..
 *   EIO     - other error
 */
int Rmdir(const char* path);

/**
 * @brief   Remove a directory.
 * @param   path
 * @param   is_spread_all_namespaces: whether do rmdir for all federations
 * @param   is_pass_when_already_exist: whether pass and continue when some one federation ack ENOENT
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - path is not directory
 *   ENOENT  - path not exist
 *   ENOTEMPTY - dir has children other than . and ..
 *   EIO     - other error
 */
int Rmdir(const char* path, bool is_spread_all_namespaces, bool is_pass_when_not_exist);

/**
 * @brief   Open a directory.
 * @param   path
 * @return  A ptr of struct NFSDIR.
 *          Do NOT use 'delete' or 'free()' to free ptr. Just only and MUST use 'Closedir()'.
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - path is not directory
 *   ENOENT  - path not exist
 *   EIO     - other error
 */
NFSDIR* Opendir(const char* path);

/**
 * @brief   Open a directory.
 * @param   path
 * @param   is_spread_all_namespaces: whether do opendir/readdir for all federations
 * @param   is_pass_when_already_exist: whether pass and continue when some one federation ack ENOENT
 * @return  A ptr of struct NFSDIR.
 *          Do NOT use 'delete' or 'free()' to free ptr. Just only and MUST use 'Closedir()'.
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOTDIR - path is not directory
 *   ENOENT  - path not exist
 *   EIO     - other error
 */
NFSDIR* Opendir(const char* path, bool is_spread_all_namespaces, bool is_pass_when_not_exist);

/**
 * @brief   Read next entry of the opened directory.
 *          Tips: You can get linux file type of the entry via marco
 *                DTTOIF(dirent->d_type), Which is also defined in dirent.h.
 * @param   dir
 * @return  A ptr of struct dirent.
 *   NOT-NULL - the ptr to next entry
 *   NULL     - read finished or occurs error
 *          Do NOT use 'delete' or 'free()' or any other method to free dirent pointer.
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF   - dir is invalid
 *   EIO     - other error
 */
struct ::dirent* Readdir(NFSDIR* dir);

/**
 * @brief   Close the opened directory.
 * @param   dir
 * @return
 *    0     - on success
 *   -1     - on error (can be ignored, but may occur memory-leak)
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - dir is invalid
 *   EIO    - other error
 */
int Closedir(NFSDIR* dir);

/**
 * @brief   Create a file. The default mode is 0666.
 * @param   path
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   EEXIST  - path already exist
 *   ENOENT  - path's parents not exist
 *   ENOSPC  - no space to create (in NFS, it means that exceeds quota)
 *   EIO     - other error
 */
int Create(const char* path);

/**
 * @brief   Unlink a file or symlink.
 * @param   path
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOENT  - path not exist
 *   EISDIR  - path is not a file or symlink
 *   EIO     - other error
 */
int Unlink(const char* path);

/**
 * @brief GetInode from NFSFILE
 *
 * @param file NFSFILE ptr
 *
 * @return   uint64_t inode
 */
uint64_t GetInode(const NFSFILE* file);

/**
 * @brief   Open a file stream.
 *          If mode is "w"/"a" and file not exist, file will be created automatically with 0666.
 *          If mode is "w" and file already exist, file will be truncated to 0 automatically.
 *          If mode is "a" and file already exist, file will not be truncated, and write stream ptr will be moved to the end of stream automatically.
 * @param   path
 * @param   mode - only "r" or "w" or "a"
 * @return  A stream ptr of struct NFSFILE
 *   NOT-NULL  - on success
 *   NULL      - on error
 *          Do NOT use 'delete' or 'free()' to free ptr. Just only and MUST use 'Close()'
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - path is invalid, or mode is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOENT  - path not exist when open with "r" mode
 *   EISDIR  - path is not a file
 *   ENOTDIR - path's parents is not directory
 *   EBUSY   - file has been opened with "w" or "a" by someone other (and I also want to open with "w" or "a")
 *   ENOSPC  - no space to create(if needed) (in NFS, it means that exceeds quota)
 *   EIO     - mode is "w" and file has been opened in "w" mode; or other error
 */
NFSFILE* Open(const char* path, const char* mode);

/**
 * @brief   Close a file stream.
 * @param   stream
 * @return
 *    0     - on success
 *   -1     - on error
 *          If file is opened with "r" mode, close will always return success.
 *          If file is opened with "w" mode, it perhaps will be complex:
 *             a) Use NFS system only, then close will always return success;
 *             b) Use NFS system along with RBS system, then close may return error.
 *                And libnfs will wait until all the blocks of the file have been committed or timeout.
 *                If some blocks commit failed or timeout, libnfs will return error, that means close failed.
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - stream is invalid
 *   EIO    - other error (only in RBS system)
 */
int Close(NFSFILE* stream);

/**
 * @brief   Read size bytes to the buf pointed by ptr from the file stream.
 *          Libnfs will assume that the offset is the finished offset you read last time.
 *          Actually, it is atomically, will not cause EAGAIN or EWOULDBLOCK.
 * @param   stream
 * @param   ptr
 * @param   size
 * @return
 *  >=0     - on success, return the actually read-size, if it is less than size, stream should be eof.
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - stream is invalid
 *   EINVAL - ptr is invalid
 *   EIO    - other error
 */
ssize_t Read(NFSFILE* stream, void* ptr, size_t size);

/**
 * @brief   Read size bytes to the buf pointed by ptr from the file stream, started with the offset.
 *          PRead will not change any state of stream, so, PRead is thread-safe.
 * @param   stream
 * @param   ptr
 * @param   size
 * @param   offset
 * @return
 *  >=0     - on success, return the actually read-size.
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - stream is invalid
 *   EINVAL - ptr is invalid
 *   EIO    - other error
 */
ssize_t PRead(NFSFILE* stream, void* ptr, size_t size, uint64_t offset);

/**
 * @brief   Write size bytes to the buf pointed by ptr from the file stream.
 *          Libnfs will assume that the offset is the finished offset you written last time.
 *          Actually, it is atomically, will not cause EAGAIN or EWOULDBLOCK.
 * @param   stream
 * @param   ptr
 * @param   size
 * @return
 *  >=0     - on success, return the actually written-size, it should be equals to size.
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - stream is invalid
 *   EINVAL - ptr is invalid
 *   EIO    - other error
 */
ssize_t Write(NFSFILE* stream, const void* ptr, size_t size);

/**
 * @brief   Write size bytes to the buf pointed by ptr from the file stream, started with the offset.
 *          PWrite will not change any state of stream.
 * @param   stream
 * @param   ptr
 * @param   size
 * @return
 *  >=0     - on success, return the actually written-size, it should be equals to size.
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - stream is invalid
 *   EINVAL - ptr is invalid or in-continuely write in RBS system
 *   EIO    - other error
 */
ssize_t PWrite(NFSFILE* stream, const void* ptr, size_t size, uint64_t offset);

/**
 * @brief   Sync the file stream.
 *          Currently, we do not support sync rbs write stream, but this api will return succ to avoid make caller troubled.
 *          Though, sync read stream (whether in nfs or rbs) will cause fail.
 * @param   stream
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - stream is invalid
 *   EINVAL - sync read stream
 *   EIO    - other error
 */
int Fsync(NFSFILE* stream);

/**
 * @brief   Tell the current pos of the file stream.
 * @param   stream
 * @return
 *  >=0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - stream is invalid
 */
int64_t Tell(NFSFILE* stream);

/**
 * @brief   Seek to the specificed pos of the file stream.
 * @param   stream
 * @param   offset
 * @return
 *    0     - on success
 *   -1     - on error
 *          If use NFS system only, seek will always return success (except the invalid arguments).
 *          If use NFS system along with RBS system, you can not seek the write stream backward, or else RBS may return other error.
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - stream is invalid
 *   EINVAL - seek the write stream backward in RBS system
 *   EIO    - other error in RBS system
 */
int Seek(NFSFILE* stream, uint64_t offset);

/**
 * @brief   Whether a read stream has arrived the end of file.
 * @param   stream
 * @return
 *    1     - on yes
 *    0     - on no
 * @errno   When error, the nfs errno will be set appropriately:
 *   EBADF  - stream is invalid
 *   EINVAL - ask a write stream whether eof
 */
int Eof(NFSFILE* stream);

/**
 * @brief   Rename oldpath to newpath.
 * @param   oldpath
 * @param   newpath
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL  - oldpath or newpath or both is(are) invalid
 *   EACCES  - user has no permission on oldpath or newpath
 *   ENAMETOOLONG - path too long
 *   ENOENT  - oldpath not exist
 *   EISDIR  - oldpath is not a directory while newpath is a existing directory
 *   ENOTEMPTY - oldpath is directory and newpath is directory and newpath contains entries other than . and ..
 *   EIO     - other error
 */
int Rename(const char* oldpath, const char* newpath);

/**
 * @brief   Stat the path.
 * @param   path
 * @param   stat
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL - path is invalid or stat is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOENT - path not exist
 *   EIO    - other error
 */
int Stat(const char* path, struct ::stat* stat);

/**
 * @brief   Truncate the file.
 *          Currently, NFS only support 0-size truncation.
 * @param   path
 * @param   size
 * @return
 *    0     - on success
 *   -1     - on error, if size is not 0, will return error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL - path is invalid or size is not 0
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   EISDIR - path is not file
 *   ENOENT - path not exist
 *   EIO    - other error
 */
int Truncate(const char* path, uint64_t size);

/**
 * @brief   Set the modify time of path.
 * @param   path
 * @param   mtime
 * @return
 *    0     - on success
 *   -1     - on error
 * @errno   When error, the nfs errno will be set appropriately:
 *   EINVAL - path is invalid
 *   EACCES  - user has no permission on the path
 *   ENAMETOOLONG - path too long
 *   ENOENT - path not exist
 *   EIO    - other error
 */
int SetModifyTime(const char* path, time_t mtime);

}

#endif  //__NFS_API_NFS_H_

