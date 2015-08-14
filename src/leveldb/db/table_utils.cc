// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/version_set.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/write_batch.h"
#include "util/logging.h"
#include "util/string_ext.h"
#include "util/mutexlock.h"
#include "leveldb/table_utils.h"

#include <iostream>
#include <map>

namespace leveldb {

void ArchiveFile(Env* env, const std::string& fname) {
  // Move into another directory.  E.g., for
  //    dir/foo
  // rename to
  //    dir/lost/foo
  const char* slash = strrchr(fname.c_str(), '/');
  std::string new_dir;
  if (slash != NULL) {
    new_dir.assign(fname.data(), slash - fname.data());
  }
  new_dir.append("/lost");
  env->CreateDir(new_dir);  // Ignore error
  std::string new_file = new_dir;
  new_file.append("/");
  new_file.append((slash == NULL) ? fname.c_str() : slash + 1);
  Status s = env->RenameFile(fname, new_file);
  fprintf(stderr, "Archiving %s: %s\n", fname.c_str(), s.ToString().c_str());
}

bool GuessType(const std::string& fname, FileType* type) {
  size_t pos = fname.rfind('/');
  std::string basename;
  if (pos == std::string::npos) {
    basename = fname;
  } else {
    basename = std::string(fname.data() + pos + 1, fname.size() - pos - 1);
  }
  uint64_t ignored;
  return ParseFileName(basename, &ignored, type);
}

// Notified when log reader encounters corruption.
class CorruptionReporter : public log::Reader::Reporter {
 public:
  virtual void Corruption(size_t bytes, const Status& status) {
    printf("corruption: %d bytes; %s\n",
            static_cast<int>(bytes),
            status.ToString().c_str());
  }
};

// Print contents of a log file. (*func)() is called on every record.
bool PrintLogContents(Env* env, const std::string& fname,
                      void (*func)(Slice)) {
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    return false;
  }
  CorruptionReporter reporter;
  log::Reader reader(file, &reporter, true, 0);
  Slice record;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch)) {
    printf("--- offset %llu; ",
           static_cast<unsigned long long>(reader.LastRecordOffset()));
    (*func)(record);
  }
  delete file;
  return true;
}

// Called on every item found in a WriteBatch.
class WriteBatchItemPrinter : public WriteBatch::Handler {
 public:
  uint64_t offset_;
  uint64_t sequence_;

  virtual void Put(const Slice& key, const Slice& value) {
    printf("  put '%s' '%s'\n",
           EscapeString(key).c_str(),
           EscapeString(value).c_str());
  }
  virtual void Delete(const Slice& key) {
    printf("  del '%s'\n",
           EscapeString(key).c_str());
  }
};


// Called on every log record (each one of which is a WriteBatch)
// found in a kLogFile.
static void WriteBatchPrinter(Slice record) {
  if (record.size() < 12) {
    printf("log record length %d is too small\n",
           static_cast<int>(record.size()));
    return;
  }
  WriteBatch batch;
  WriteBatchInternal::SetContents(&batch, record);
  printf("sequence %llu\n",
         static_cast<unsigned long long>(WriteBatchInternal::Sequence(&batch)));
  WriteBatchItemPrinter batch_item_printer;
  Status s = batch.Iterate(&batch_item_printer);
  if (!s.ok()) {
    printf("  error: %s\n", s.ToString().c_str());
  }
}

bool DumpLog(Env* env, const std::string& fname) {
  return PrintLogContents(env, fname, WriteBatchPrinter);
}

// Called on every log record (each one of which is a WriteBatch)
// found in a kDescriptorFile.
static void VersionEditPrinter(Slice record) {
  VersionEdit edit;
  Status s = edit.DecodeFrom(record);
  if (!s.ok()) {
    printf("%s\n", s.ToString().c_str());
    return;
  }
  printf("%s", edit.DebugString().c_str());
}

bool DumpDescriptor(Env* env, const std::string& fname) {
  return PrintLogContents(env, fname, VersionEditPrinter);
}

bool DumpTable(Env* env, const std::string& fname) {
  uint64_t file_size;
  RandomAccessFile* file = NULL;
  Table* table = NULL;
  Status s = env->GetFileSize(fname, &file_size);
  if (s.ok()) {
    s = env->NewRandomAccessFile(fname, &file);
  }
  if (s.ok()) {
    // We use the default comparator, which may or may not match the
    // comparator used in this database. However this should not cause
    // problems since we only use Table operations that do not require
    // any comparisons.  In particular, we do not call Seek or Prev.
    s = Table::Open(Options(), file, file_size, &table);
  }
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    delete table;
    delete file;
    return false;
  }

  ReadOptions ro;
  ro.fill_cache = false;
  Iterator* iter = table->NewIterator(ro);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey key;
    if (!ParseInternalKey(iter->key(), &key)) {
      printf("badkey '%s' => '%s'\n",
             EscapeString(iter->key()).c_str(),
             EscapeString(iter->value()).c_str());
    } else {
      char kbuf[20];
      const char* type;
      if (key.type == kTypeDeletion) {
        type = "del";
      } else if (key.type == kTypeValue) {
        type = "val";
      } else {
        snprintf(kbuf, sizeof(kbuf), "%d", static_cast<int>(key.type));
        type = kbuf;
      }
      printf("'%s' @ %8llu : %s => '%s'\n",
             EscapeString(key.user_key).c_str(),
             static_cast<unsigned long long>(key.sequence),
             type,
             EscapeString(iter->value()).c_str());
    }
  }
  s = iter->status();
  if (!s.ok()) {
    printf("iterator error: %s\n", s.ToString().c_str());
  }

  delete iter;
  delete table;
  delete file;
  return true;
}

bool DumpFile(Env* env, const std::string& fname) {
  FileType ftype;
  if (!GuessType(fname, &ftype)) {
    fprintf(stderr, "%s: unknown file type\n", fname.c_str());
    return false;
  }
  switch (ftype) {
    case kLogFile:         return DumpLog(env, fname);
    case kDescriptorFile:  return DumpDescriptor(env, fname);
    case kTableFile:       return DumpTable(env, fname);

    default: {
      fprintf(stderr, "%s: not a dump-able file type\n", fname.c_str());
      break;
    }
  }
  return false;
}

void DumpMergeFileMap(const std::string& filename,
                      const std::map<uint64_t, uint64_t>& file_map) {
    std::cout << "File[" << filename << "] map: ";
    std::map<uint64_t, uint64_t>::const_iterator it;
    for (it = file_map.begin(); it != file_map.end(); ++it) {
        std::cout << it->first << " -> " << it->second << ", ";
    }
    std::cout << std::endl;
}

void FindFileMap(const std::vector<uint64_t>& deleted_files,
                 const std::vector<uint64_t>& added_files,
                 uint64_t* last_file_no,
                 std::map<uint64_t, uint64_t>* file_map) {
    for (uint32_t i = 0; i < deleted_files.size(); ++i) {
        (*file_map)[deleted_files[i]] = *last_file_no + deleted_files[i];
        (*last_file_no)++;
    }
    for (uint32_t i = 0; i < added_files.size(); ++i) {
        (*file_map)[added_files[i]] = *last_file_no + added_files[i];
        (*last_file_no)++;
    }
}

bool CollectSSTFile(Env* env, const std::string& dir_path,
                    std::set<uint64_t>* sst_list) {
    std::vector<std::string> files;
    Status s = env->GetChildren(dir_path, &files, NULL);
    if (!s.ok()) {
        return false;
    }
    for (uint32_t i = 0; i < files.size(); ++i) {
        FileType ftype;
        if (!GuessType(files[i], &ftype)) {
            fprintf(stderr, "%s: unknown file type in CollectSSTFile()\n", files[i].c_str());
            continue;
        } else if (ftype == kTableFile) {
            std::vector<std::string> items;
            SplitString(files[i], ".", &items);
            sst_list->insert(StringToUint64(items[0]));
        }
    }
    return true;
}

bool BuildVersion(Env* env, VersionSet* version_set,
                  const std::string fn, uint64_t* last_seq,
                  uint64_t* last_file_no,
                  std::map<uint64_t, uint64_t>* file_map) {
    SequentialFile* file;
    Status s = env->NewSequentialFile(fn, &file);
    if (!s.ok()) {
        fprintf(stderr, "BuildVersion(): %s\n", s.ToString().c_str());
        return false;
    }
    std::string fn_dir;
    SplitStringEnd(fn, &fn_dir, NULL, "/");
    std::set<uint64_t> sst_list;
    CollectSSTFile(env, fn_dir, &sst_list);

    uint64_t file_last_seq = 0;
    uint64_t file_last_file_no = 0;
    std::vector<uint64_t> deleted_files;
    std::vector<uint64_t> added_files;
    port::Mutex mu;
    Slice record;
    VersionEdit base_edit;
    std::string scratch;
    CorruptionReporter reporter;
    log::Reader reader(file, &reporter, true, 0);
    while (reader.ReadRecord(&record, &scratch)) {
        VersionEdit edit;
        s = edit.DecodeFrom(record);

        if (!edit.HasFiles(&deleted_files, &added_files)) {
            continue;
        }
        for (uint32_t i = 0; i < deleted_files.size(); ++i) {
            sst_list.erase(deleted_files[i]);
        }
    }
    record.clear();
    log::Reader re_reader(file, &reporter, true, 0);
    while (re_reader.ReadRecord(&record, &scratch)) {
        base_edit.DecodeFilterFrom(record, sst_list);
    }
    if (base_edit.HasLogNumber()) {
        file_last_file_no = base_edit.GetLogNumber();
        base_edit.SetLogNumber(*last_file_no + file_last_file_no);
    }
    FindFileMap(deleted_files, added_files, last_file_no, file_map);

    if (base_edit.HasLastSequence()) {
        file_last_seq = base_edit.GetLastSequence();
        base_edit.SetLastSequence(*last_seq + file_last_seq);
    }
    if (base_edit.HasNextFileNumber()) {
        base_edit.SetNextFile(*last_file_no + file_last_file_no);
    }
    while (version_set->NewFileNumber() < *last_file_no + file_last_file_no);
    version_set->LogAndApply(&base_edit, &mu);

    *last_file_no += file_last_file_no;
    *last_seq += file_last_seq;

    delete file;
    return true;
}

bool MergeBoth(Env* env, Comparator* cmp,
               const std::string& merged_fn,
               const std::string& fn1, const std::string& fn2,
               std::map<uint64_t, uint64_t>* file_num_map) {
    std::cerr << "MergeBoth: " << fn1 << " & " << fn2
        << " => " << merged_fn
        << std::endl;
    Status s;
    WritableFile* final_file;
    s = env->NewWritableFile(merged_fn, &final_file);
    if (!s.ok()) {
        fprintf(stderr, "MergeBoth(): %s\n", s.ToString().c_str());
        return false;
    }
    delete final_file;
//     log::Writer writer(final_file);

    uint64_t last_seq = 0;
    uint64_t last_file_no = 0;
    InternalKeyComparator key_cmp(BytewiseComparator());
    Options options;
    options.env = env;
    VersionSet version_set(merged_fn, &options, NULL, &key_cmp);
    s = version_set.Recover();
    if (!s.ok()) {
        fprintf(stderr, "fail to recover: %s, status: %s\n",
                fn1.c_str(), s.ToString().c_str());
        return false;
    }

    last_seq = version_set.LastSequence();
    last_file_no = version_set.NewFileNumber();
    if (s.ok()) {
        fprintf(stderr, "current last seq: %llu, file_num: %llu\n",
                static_cast<unsigned long long>(last_seq),
                static_cast<unsigned long long>(last_file_no));
    }
    if (!BuildVersion(env, &version_set, fn2, &last_seq, &last_file_no,
                      file_num_map)) {
        fprintf(stderr, "fail to build version for %s\n", fn1.c_str());
        return false;
    }
    std::cerr << "Build version success" << std::endl;
    return true;
}

bool MergeTo(Env* env, const std::string& fname, log::Writer* writer,
             uint64_t *log_number, uint64_t *next_file_number,
             uint64_t *last_sequence,
             std::map<uint64_t, uint64_t>* file_num_map) {
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    return false;
  }
//   std::map<uint64_t, uint64_t> file_num_map;
  uint64_t file_last_seq = 0;
  Slice record;
  std::string scratch;
  CorruptionReporter reporter;
  log::Reader reader(file, &reporter, true, 0);
  while (reader.ReadRecord(&record, &scratch)) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if ((*log_number != 0) && (edit.GetLastSequence() == 0)) {
        continue;
    }
    if (edit.HasLogNumber() && edit.GetLogNumber() >= *log_number) {
        *log_number = edit.GetLogNumber();
    } else if (edit.HasLogNumber()) {
        *log_number = (*next_file_number);
        edit.SetLogNumber(*next_file_number);
    }
    if (edit.HasNextFileNumber() && edit.GetNextFileNumber() > *next_file_number) {
        *next_file_number = edit.GetNextFileNumber();
    } else if (edit.HasNextFileNumber()) {
        (*file_num_map)[edit.GetNextFileNumber() - 1] = *next_file_number;
        (*next_file_number)++;
        edit.SetNextFile(*next_file_number);
    }
//     if (edit.HasLastSequence() && edit.GetLastSequence() >= *last_sequence) {
//         file_last_seq = edit.GetLastSequence();
//     } else
    if (edit.HasLastSequence()) {
        file_last_seq = edit.GetLastSequence();
        edit.SetLastSequence(*last_sequence  + file_last_seq);
    }

    edit.ModifyForMerge(*file_num_map);
    std::string modified_record;
    edit.EncodeTo(&modified_record);
    s = writer->AddRecord(modified_record);
  }
  *last_sequence += file_last_seq;

  delete file;
  return s.ok();
}

bool MergeManifestFile(Env* env, const std::string& final_name,
                       char** files, int num,
                       std::map<std::string, std::map<uint64_t, uint64_t> >* file_maps) {
  FileType ftype;
  if (!GuessType(final_name, &ftype)) {
    fprintf(stderr, "%s: unknown file type in MergeManifestFile()\n", final_name.c_str());
    return false;
  } else if (ftype != kDescriptorFile) {
    fprintf(stderr, "%s: not descriptor file type\n", final_name.c_str());
    return false;
  }

  // open file for read
  WritableFile* final_file;
  Status s = env->NewWritableFile(final_name, &final_file);
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    return false;
  }
  log::Writer writer(final_file);

  uint64_t log_number = 0;
  uint64_t next_file_number = 0;
  uint64_t last_sequence = 0;
  bool ok = true;
  for (int i = 0; i < num; ++i) {
    std::map<uint64_t, uint64_t> file_num_map;
    ok &= MergeTo(env, files[i], &writer, &log_number,
                  &next_file_number, &last_sequence,
                  &file_num_map);
    (*file_maps)[files[i]] = file_num_map;

    std::cout << "log_number: " << log_number
        << ", next_file_number: " << next_file_number
        << ", last_sequence: " << last_sequence << std::endl;
    DumpMergeFileMap(files[i], file_num_map);
  }
  if (ok) {
      final_file->Sync();
  }
  delete final_file;
  return ok;
}

const std::string merge_file_prefix = "MERGEFROM";
const std::string merge_file_sep = "-";

bool IsTeraFlagFile(const std::string& file_path) {
    std::string file_name;
    SplitStringPath(file_path, NULL, &file_name);
    if (file_name.empty()) {
        file_name = file_path;
    }

    if (!file_name.empty() && StringStartWith(file_name, merge_file_prefix)) {
        return true;
    }
    return false;
}

std::string CombineMapFileName(const std::string& table_name,
                               uint64_t from_no, uint64_t to_no) {
    std::string from_no_str = Uint64ToString(from_no);
    std::string to_no_str = Uint64ToString(to_no);
    return merge_file_prefix + merge_file_sep +
        table_name + merge_file_sep + from_no_str
        + merge_file_sep + to_no_str;
}

bool ParseMapFileName(const std::string& path,
                      std::string* table_name,
                      uint64_t* from_no, uint64_t* to_no) {
    if (!StringStartWith(path, merge_file_prefix)) {
        return false;
    }
    std::vector<std::string> items;
    SplitString(path, merge_file_sep, &items);
    if (items.size() != 4) {
        return false;
    }

    if (table_name) {
        *table_name = items[1];
    }
    if (from_no) {
        *from_no = StringToUint64(items[2]);
    }
    if (to_no) {
        *to_no = StringToUint64(items[3]);
    }
    return true;
}

bool CleanFlagFile(Env* env, const std::string& path) {
    std::vector<std::string> files;
    Status s = env->GetChildren(path, &files, NULL);
    if (!s.ok()) {
        return false;
    }

    bool all_deleted = true;
    for (uint64_t i = 0; i < files.size(); ++i) {
        if (!StringStartWith(files[i], "bak.")
            && !StringStartWith(files[i], merge_file_prefix)
            && !StringEndsWith(files[i], ".merge") ) {
            continue;
        }
        s = env->DeleteFile(path + "/" + files[i]);
        if (!s.ok()) {
            std::cerr << "fail to clean flag file: " << files[i]
                << std::endl;
            all_deleted = false;
        }
    }
    return all_deleted;
}

bool DumpFileMapToDir(Env* env, const std::string& to_path,
                      const std::string& from_path,
                      const std::map<uint64_t, uint64_t>& file_map) {
    std::string dir;
    std::string table_name;
    SplitStringPath(from_path, &dir, &table_name);
    if (!StringStartWith(to_path, dir)) {
        return false;
    }
    std::map<uint64_t, uint64_t>::const_iterator it;
    for (it = file_map.begin(); it != file_map.end();  ++it) {
        std::string map_info = CombineMapFileName(table_name, it->first, it->second);
        WritableFile* file;
        Status s = env->NewWritableFile(to_path + "/" + map_info, &file);
        if (!s.ok()) {
            std::cerr << "fail to dump merge file map" << std::endl;
            return false;
        }
        file->Close();
        delete file;
    }
    return true;
}

bool GatherFileMapFromDir(Env* env, const std::string& dir_path,
                          std::string* table_name,
                          std::map<uint64_t, uint64_t>* file_map) {
    std::vector<std::string> files;
    Status s = env->GetChildren(dir_path, &files, NULL);
    if (!s.ok()) {
        return false;
    }

    for (uint32_t i = 0; i < files.size(); ++i) {
        std::map<uint64_t, uint64_t> single_map;
        if (!IsTeraFlagFile(files[i])) {
            continue;
        }

        std::string name;
        uint64_t from_no;
        uint64_t to_no;
        ParseMapFileName(files[i], &name, &from_no, &to_no);
        if (table_name->empty()) {
            *table_name = name;
        } else if (*table_name != name) {
            // filter the dirty file
            continue;
        }
        (*file_map)[from_no] = to_no;
    }
    return true;
}

bool BackupNotSSTFile(Env* env, const std::string& dir_path,
                      std::string* mf) {
    std::vector<std::string> files;
    Status s = env->GetChildren(dir_path, &files, NULL);
    if (!s.ok()) {
        return false;
    }

    bool found = false;
    for (uint32_t i = 0; i < files.size(); ++i) {
        FileType ftype;
        if (!GuessType(files[i], &ftype)) {
            fprintf(stderr, "%s: unknown file type in BackupNotSSTFile()\n", files[i].c_str());
            continue;
        } else if (ftype == kTableFile) {
            continue;
        } else if ((ftype == kDescriptorFile)
                   && (mf->empty() || *mf < files[i])) {
            *mf = files[i];
            found = true;
        } else if (ftype == kLogFile) {
            s = env->RenameFile(dir_path + "/" +  files[i],
                                dir_path + "/" + std::string("bak.") + files[i]);
            if (!s.ok()) {
                std::cerr << "fail to backup: " << files[i] << std::endl;
            }
        }
    }
    if (!mf->empty()) {
        s = env->RenameFile(dir_path + "/" +  *mf,
                            dir_path + "/" + *mf + std::string(".merge"));
        if (!s.ok()) {
            std::cerr << "fail to backup: " << *mf << std::endl;
            return false;
        }
        *mf += std::string(".merge");
    }
    return found;
}

bool RollbackBackupNotSst(Env* env, const std::string& dir_path) {
    std::vector<std::string> files;
    Status s = env->GetChildren(dir_path, &files, NULL);
    if (!s.ok()) {
        return false;
    }

    for (uint32_t i = 0; i < files.size(); ++i) {
        FileType ftype;
        if (!GuessType(files[i], &ftype)) {
            std::string org_file;
            if (StringStartWith(files[i], "bak.")) {
                SplitStringStart(files[i], NULL, &org_file);
            } else if (StringEndsWith(files[i], ".merge")) {
                std::string org_file;
                SplitStringEnd(files[i], &org_file, NULL);
            } else {
                fprintf(stderr, "%s: unknown file type in RollbackBackupNotSst()\n", files[i].c_str());
                continue;
            }
            s = env->RenameFile(dir_path + "/" + files[i],
                                dir_path + "/" + org_file);
        }
    }
    return true;
}

bool MoveMergedSst(Env* env, const std::string& from_path,
                   const std::string& to_path,
                   const std::map<uint64_t, uint64_t>& file_map) {
    std::vector<std::string> files;
    Status s = env->GetChildren(from_path, &files, NULL);
    if (!s.ok()) {
        return false;
    }
    for (uint32_t i = 0; i < files.size(); ++i) {
        FileType ftype;
        uint64_t file_num;
        if (!ParseFileName(files[i], &file_num, &ftype)
            || ftype != kTableFile) {
            continue;
        }
        std::map<uint64_t, uint64_t>::const_iterator it =
            file_map.find(file_num);
        if (it == file_map.end()) {
            std::cerr << "not found map for: " << files[i] << std::endl;
            continue;
        }
        char new_sst[100];
        snprintf(new_sst, sizeof(new_sst), "/%06llu.sst",
                 static_cast<unsigned long long>(it->second));
        std::string from_file = from_path + "/" + files[i];
        std::string to_file = to_path + new_sst;
        s = env->RenameFile(from_file, to_file);
        if (!s.ok()) {
            std::cerr << "fail to move: " << from_file
                << " to " << to_file << std::endl;
            return false;
        }
        std::cerr << "rename: " << from_file << " => "
            << to_file << std::endl;
    }
    return true;
}

bool MoveMergedSst(Env* env, const std::string& path) {
    std::cerr << "MoveMergedSst()" << std::endl;

    std::map<uint64_t, uint64_t> file_map;
    std::string merged_table;
    if (!GatherFileMapFromDir(env, path, &merged_table, &file_map)) {
        std::cerr << "fail to list dir: " << path << std::endl;
        return false;
    }

    std::string path_prefix;
    std::string cur_table;
    SplitStringPath(path, &path_prefix, &cur_table);
    std::string merged_table_path = path_prefix + "/" + merged_table;
    std::vector<std::string> files;
    Status s = env->GetChildren(merged_table_path, &files, NULL);
    if (!s.ok()) {
        return false;
    }
    for (uint32_t i = 0; i < files.size(); ++i) {
        FileType ftype;
        uint64_t file_num;
        if (!ParseFileName(files[i], &file_num, &ftype)
            || ftype != kTableFile) {
            continue;
        }
        std::map<uint64_t, uint64_t>::iterator it =
            file_map.find(file_num);
        if (it == file_map.end()) {
            std::cerr << "not found map for: " << files[i] << std::endl;
            continue;
        }
        std::string new_sst_name = TableFileName(cur_table, it->second);
        s = env->RenameFile(merged_table_path + "/" + files[i],
                            path_prefix + "/" + new_sst_name);
        if (!s.ok()) {
            std::cerr << "fail to move: " << files[i] << " from: "
                << merged_table << std::endl;
            return false;
        }
        std::cerr << "rename: " << merged_table_path + "/" + files[i]
            << " => " << path_prefix + "/" + new_sst_name << std::endl;

        std::string map_flag = CombineMapFileName(merged_table,
                                                  it->first, it->second);
        s = env->DeleteFile(path + "/" + map_flag);
        if (!s.ok()) {
            std::cerr << "fail to delete map flag: " << map_flag << std::endl;
            return false;
        }
    }
    return true;
}

bool RenameTable(Env* env, const std::string& from,
                 const std::string& to) {
    Status s = env->RenameFile(from, to);
    if (!s.ok()) {
        std::cerr << "fail to rename from " << from
            << " to " << to << std::endl;
        return false;
    }
    return true;
}

bool DeleteTable(Env* env, const std::string& name) {
    Status s = env->DeleteFile(name);
    if (!s.ok()) {
        std::cerr << "fail to delete tablet: " << name
            << std::endl;
        return false;
    }
    return true;
}

bool HandleDumpCommand(Env* env, char** files, int num) {
  bool ok = true;
  for (int i = 0; i < num; i++) {
    ok &= DumpFile(env, files[i]);
  }
  return ok;
}

bool HandleMergeCommnad(Env* env, char** files, int num) {
  if (num < 2) {
      return true;
  }

  std::map<std::string, std::map<uint64_t, uint64_t> > files_maps;
  return MergeManifestFile(env, files[0], files + 1, num - 1, &files_maps);
}


bool GetSplitPath(const std::string& path, std::string* split_path1,
                  std::string* split_path2) {
    if (path.empty()) {
        return false;
    }
    *split_path1 = path + "0";
    *split_path2 = path + "1";
    return true;
}

bool GetParentPath(const std::string& path, std::string* parent_path) {
    std::string path_without_lg;
    std::string lg_str;
    bool is_lg_path = GetDBpathAndLGid(path, &path_without_lg, &lg_str);
    size_t len = path_without_lg.size();
    if (len <= 1 || (path_without_lg[len - 1] != '0'
                     && path_without_lg[len - 1] != '1')) {
        return false;
    }
    parent_path->assign(path_without_lg, 0, len - 1);
    if (is_lg_path) {
        lg_str = std::string("/") + lg_str;
        parent_path->append(lg_str);
    }
    return true;
}

bool GetDBpathAndLGid(const std::string& path, std::string* db_path,
                      std::string* lg_id_str, uint64_t* lg_id) {
    std::string path_without_slash = path;
    while (path_without_slash[path_without_slash.size() - 1] == '/') {
        path_without_slash.resize(path_without_slash.size() - 1);
    }
    std::string id_str;
    uint32_t id = 0;
    SplitStringPath(path_without_slash, db_path, &id_str);
    if (!id_str.empty() && isdigit(id_str[0])) {
        if (lg_id) {
            *lg_id = id;
        }
        if (lg_id_str) {
            *lg_id_str = id_str;
        }
        return true;
    } else {
        *db_path = path;
    }
    return false;
}

std::string GetSplitGapNumString(const std::string& cur_path,
                                 const std::string& real_path) {
    std::string db_path_cur_dir;
    std::string db_path_real_dir;
    std::string db_path_cur_dir_lg;
    std::string db_path_real_dir_lg;

    SplitStringPath(cur_path, &db_path_cur_dir_lg, NULL);
    SplitStringPath(real_path, &db_path_real_dir_lg, NULL);
    GetDBpathAndLGid(db_path_cur_dir_lg, &db_path_cur_dir);
    GetDBpathAndLGid(db_path_real_dir_lg, &db_path_real_dir);

    std::string gap_num_str;
    if (db_path_cur_dir.length() > db_path_real_dir.length()) {
        gap_num_str = db_path_cur_dir.substr(db_path_real_dir.length());
    }
    return gap_num_str;
}

bool MigrateTableFile(Env* env, const std::string& dbpath,
                      const std::string& sst_file) {
    std::string cur_path = dbpath + "/" + sst_file;
    Status s = env->CopyFile(cur_path, cur_path);
    return s.ok();
}

}  // namespace leveldb
