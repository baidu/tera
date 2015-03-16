#ifndef STORAGE_LEVELDB_UTIL_NFS_VERSION_H_
#define STORAGE_LEVELDB_UTIL_NFS_VERSION_H_

#ifdef __cplusplus
   extern  "C" {
#endif

extern const char kNfsSvnInfo[];
extern const char kNfsBuildType[];
extern const char kNfsBuildTime[];
extern const char kNfsBuilderName[];
extern const char kNfsHostName[];
extern const char kNfsCompiler[];
const char* PrintNfsVersion();

#ifdef __cplusplus
   }
#endif

#endif // STORAGE_LEVELDB_UTIL_NFS_VERSION_H_
