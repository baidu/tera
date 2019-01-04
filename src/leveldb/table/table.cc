// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <malloc.h>

#include "db/dbformat.h"
#include "format.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/persistent_cache.h"
#include "leveldb/table.h"
#include "persistent_cache/persistent_cache_file.h"
#include "persistent_cache_helper.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "common/metric/metric_counter.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
    filter_block_size_total.Sub(filter_data_size);
  }

  Rep() : filter_data_size(0) {}

  Options options;
  Status status;
  RandomAccessFile* file;
  size_t fsize;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;
  uint64_t filter_data_size;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
  static tera::MetricCounter filter_block_size_total;
};
tera::MetricCounter Table::Rep::filter_block_size_total{
    "tera_filter_block_size", {tera::Subscriber::SubscriberType::LATEST}, false};

class TableIter : public Iterator {
 public:
  TableIter(Iterator* iter, const Comparator* comparator, const Slice& smallest,
            const Slice& largest)
      : iter_(iter),
        comparator_(comparator),
        smallest_(smallest.ToString()),
        largest_(largest.ToString()) {}

  virtual ~TableIter() { delete iter_; }

  virtual void Seek(const Slice& target) {
    if (smallest_.empty() && largest_.empty()) {
      iter_->Seek(target);
    } else if (!smallest_.empty() && largest_.empty()) {
      if (comparator_->Compare(smallest_, target) > 0) {
        // table file has smaller out-of-range keys
        iter_->Seek(smallest_);
      } else {
        iter_->Seek(target);
      }
    } else if (smallest_.empty() && !largest_.empty()) {
      if (comparator_->Compare(target, largest_) > 0) {
        // table file has larger out-of-range keys
        iter_->Seek(largest_);
      } else {
        iter_->Seek(target);
      }
    } else {
      if (comparator_->Compare(target, largest_) > 0) {
        // table file has larger out-of-range keys
        iter_->Seek(largest_);
      } else if (comparator_->Compare(smallest_, target) > 0) {
        // table file has smaller out-of-range keys
        iter_->Seek(smallest_);
      } else {
        iter_->Seek(target);
      }
    }
  }
  virtual void SeekToFirst() {
    if (smallest_.empty()) {
      iter_->SeekToFirst();
    } else {
      // table file has smaller out-of-range keys
      iter_->Seek(smallest_);
    }
  }
  virtual void SeekToLast() {
    if (largest_.empty()) {
      iter_->SeekToLast();
    } else {
      // table file has larger out-of-range keys
      iter_->Seek(largest_);
    }
  }
  virtual void Next() { iter_->Next(); }
  virtual void Prev() { iter_->Prev(); }
  virtual bool Valid() const {
    if (!iter_->Valid()) {
      return false;
    }
    if (!largest_.empty() && comparator_->Compare(iter_->key(), largest_) > 0) {
      return false;
    }
    if (!smallest_.empty() && comparator_->Compare(iter_->key(), smallest_) < 0) {
      return false;
    }
    return true;
  }
  virtual Slice key() const {
    assert(Valid());
    return iter_->key();
  }
  virtual Slice value() const {
    assert(Valid());
    return iter_->value();
  }
  virtual Status status() const { return iter_->status(); }

 private:
  Iterator* iter_;
  const Comparator* const comparator_;
  std::string smallest_;
  std::string largest_;
};

class IndexBlockIter : public Iterator {
 public:
  IndexBlockIter(const ReadOptions& opts, Block* index_block, FilterBlockReader* filter)
      : valid_(false),
        iter_(index_block->NewIterator(opts.db_opt->comparator)),
        comparator_(opts.db_opt->comparator),
        filter_(filter),
        read_single_row_(opts.read_single_row),
        row_start_key_(opts.row_start_key, kMaxSequenceNumber, kValueTypeForSeek),
        row_end_key_(opts.row_end_key, kMaxSequenceNumber, kValueTypeForSeek) {}
  virtual ~IndexBlockIter() { delete iter_; }
  virtual void Seek(const Slice& target) {
    iter_->Seek(target);
    SkipUnmatchedBlocksForward();
  }
  virtual void SeekToFirst() {
    iter_->SeekToFirst();
    SkipUnmatchedBlocksForward();
  }
  virtual void SeekToLast() {
    iter_->SeekToLast();
    SkipUnmatchedBlocksBackward();
  }
  virtual void Next() {
    iter_->Next();
    SkipUnmatchedBlocksForward();
  }
  virtual void Prev() {
    iter_->Prev();
    SkipUnmatchedBlocksBackward();
  }
  virtual bool Valid() const { return valid_; }
  virtual Slice key() const {
    assert(Valid());
    return iter_->key();
  }
  virtual Slice value() const {
    assert(Valid());
    return iter_->value();
  }
  virtual Status status() const { return iter_->status(); }

 private:
  void SkipUnmatchedBlocksForward() {
    valid_ = false;
    while (iter_->Valid()) {
      if (!read_single_row_) {
        valid_ = true;
        break;
      }
      if (!valid_index_key_.empty() && comparator_->Compare(iter_->key(), valid_index_key_) > 0) {
        // LEVELDB_LOG("bloomfilter: skip block by range");
        break;
      }
      if (comparator_->Compare(iter_->key(), row_end_key_.Encode()) >= 0 &&
          (valid_index_key_.empty() || comparator_->Compare(iter_->key(), valid_index_key_) < 0)) {
        valid_index_key_ = iter_->key().ToString();
      }
      if (CheckFilter()) {
        valid_ = true;
        // LEVELDB_LOG("bloomfilter: valid block");
        break;
      }
      // LEVELDB_LOG("bloomfilter: skip block by bloom");
      iter_->Next();
    }
  }
  void SkipUnmatchedBlocksBackward() {
    valid_ = false;
    while (iter_->Valid()) {
      if (!read_single_row_) {
        valid_ = true;
        break;
      }
      if (comparator_->Compare(iter_->key(), row_start_key_.Encode()) < 0) {
        // LEVELDB_LOG("bloomfilter: skip block by range");
        break;
      }
      if (comparator_->Compare(iter_->key(), row_end_key_.Encode()) >= 0 &&
          (valid_index_key_.empty() || comparator_->Compare(iter_->key(), valid_index_key_) < 0)) {
        valid_index_key_ = iter_->key().ToString();
      }
      if (CheckFilter()) {
        valid_ = true;
        // LEVELDB_LOG("bloomfilter: valid block");
        break;
      }
      // LEVELDB_LOG("bloomfilter: skip block by bloom");
      iter_->Prev();
    }
  }
  bool CheckFilter() {
    assert(iter_->Valid());
    Slice handle_value = iter_->value();
    BlockHandle handle;
    if (!read_single_row_ || filter_ == NULL || !handle.DecodeFrom(&handle_value).ok() ||
        filter_->KeyMayMatch(handle.offset(), row_start_key_.Encode())) {
      return true;
    }
    return false;
  }

 private:
  bool valid_;
  Iterator* iter_;
  const Comparator* comparator_;
  FilterBlockReader* filter_;
  bool read_single_row_;
  InternalKey row_start_key_;
  InternalKey row_end_key_;

  // smallest index key which is larger than row end key
  std::string valid_index_key_;
};

static void DeleteBlock(void* arg, void* ignored) { delete reinterpret_cast<Block*>(arg); }

class PrefetchBlockReader {
 public:
  PrefetchBlockReader(RandomAccessFile* file, size_t fsize) : file_(file), fsize_(fsize) {}

  Iterator* operator()(void* arg, const ReadOptions& options, const Slice& index_value) {
    assert(!arg);
    Block* block = NULL;
    BlockHandle handle;
    Slice input = index_value;
    Status s = handle.DecodeFrom(&input);

    if (s.ok()) {
      s = ReadBlock(handle, options, &block);
    }

    Iterator* iter;
    if (block != nullptr) {
      iter = block->NewIterator(options.db_opt->comparator);
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter = NewErrorIterator(s);
    }
    return iter;
  }

 private:
  Status ReadBlock(const BlockHandle& handle, const ReadOptions& options, Block** block) {
    assert(!*block);
    Slice block_slice;
    Status s;
    // Read file content, if missed, it will prefetch data from cache/dfs.
    if ((s = ReadFileContent(handle, options, &block_slice)).ok()) {
      BlockContents contents;
      s = ParseBlock(handle.size(), handle.offset(), options, block_slice, &contents);
      if (s.ok()) {
        *block = new Block(contents);
      } else if (prefetched_from_persistent_cache_) {
        // Parse Block failed and it's read from persistent cache.
        // May be an invalid cache file, try read directly from dfs.
        assert(options.db_opt->persistent_cache);
        SstDataScratch scratch;
        auto dfs_status = ReadDfsContent(handle, options, &scratch, &block_slice);
        if (dfs_status.ok()) {
          s = ParseBlock(handle.size(), handle.offset(), options, block_slice, &contents);
          if (s.ok()) {
            // Parse from dfs content successfully, so force evict the cache file.
            auto fname = file_->GetFileName();
            LEVELDB_LOG("Invalid cache data for %s, force evict it.", fname.c_str());
            Slice key{fname};
            key.remove_specified_prefix(options.db_opt->dfs_storage_path_prefix);
            options.db_opt->persistent_cache->ForceEvict(key);

            *block = new Block(contents);
          }
        } else {
          // Read from dfs failed, keep the last error.
          s = dfs_status;
        }
      }
    }
    return s;
  }

  Status ReadDfsContent(const BlockHandle& handle, const ReadOptions& options,
                        SstDataScratch* scratch, Slice* slice) {
    assert(slice);
    auto size = handle.size() + kBlockTrailerSize;
    auto offset = handle.offset();
    Status s;
    return ReadSstFile(file_, false, offset, size, slice, scratch);
  }

  Status ReadFileContent(const BlockHandle& handle, const ReadOptions& options, Slice* data) {
    assert(data);
    Status s;
    if (handle.offset() < prefetched_offset_) {
      s = PrefetchFileContents(handle, options);
    }
    if (!s.ok()) {
      return s;
    }

    auto relative_offset = handle.offset() - prefetched_offset_;
    auto size = handle.size() + kBlockTrailerSize;
    if (relative_offset + size > prefetched_data_.size()) {
      s = PrefetchFileContents(handle, options);
      if (s.ok()) {
        assert(handle.offset() == prefetched_offset_);
        relative_offset = 0;
      } else {
        return s;
      }
    }

    // No more check needed, because we successfully prefetch file contents.
    *data = Slice{prefetched_data_};
    data->remove_prefix(relative_offset);
    data->remove_suffix(prefetched_data_.size() - (relative_offset + size));
    assert(data->size() == handle.size() + kBlockTrailerSize);
    return Status::OK();
  }

  Status PrefetchFileContents(const BlockHandle& handle, const ReadOptions& options) {
    prefetched_offset_ = 0;
    prefetched_data_.clear();
    prefetched_from_persistent_cache_ = false;

    auto block_offset = handle.offset();
    if (fsize_ < block_offset) {
      prefetched_data_.clear();
      return Status::Corruption("truncated block read");
    }

    Slice contents;
    SstDataScratch val;
    auto block_size = handle.size() + kBlockTrailerSize;
    auto prefetch_size = std::max(block_size, options.prefetch_scan_size);
    prefetch_size = std::min(prefetch_size, fsize_ - block_offset);

    auto& p_cache = options.db_opt->persistent_cache;

    if (p_cache) {
      auto fname = file_->GetFileName();
      Slice key{fname};
      key.remove_specified_prefix(options.db_opt->dfs_storage_path_prefix);
      if (PersistentCacheHelper::TryReadFromPersistentCache(p_cache, key, block_offset,
                                                            prefetch_size, &contents, &val).ok()) {
        prefetched_data_.assign(contents.data(), contents.size());
        prefetched_from_persistent_cache_ = true;
      } else if (options.fill_persistent_cache) {
        PersistentCacheHelper::ScheduleCopyToLocal(options.db_opt->env, file_->GetFileName(),
                                                   fsize_, key.ToString(), p_cache);
      }
    }

    if (!prefetched_from_persistent_cache_) {
      auto s = ReadSstFile(file_, options.db_opt->use_direct_io_read, block_offset, prefetch_size,
                           &contents, &val);
      if (!s.ok()) {
        return s;
      }
      prefetched_data_.assign(contents.data(), contents.size());
    }

    if (prefetched_data_.size() < block_size) {
      prefetched_data_.clear();
      return Status::Corruption("truncated block read");
    }

    prefetched_offset_ = block_offset;
    return Status::OK();
  }

 private:
  RandomAccessFile* file_;
  size_t fsize_;
  std::string prefetched_data_;
  uint64_t prefetched_offset_ = 0;
  bool prefetched_from_persistent_cache_ = false;
};

Status Table::Open(const Options& options, RandomAccessFile* file, uint64_t size, Table** table) {
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  size_t len = Footer::kEncodedLength;
  uint64_t offset = size - Footer::kEncodedLength;
  Slice footer_input;
  SstDataScratch scratch;
  Status s;
  Footer footer;
  auto& p_cache = options.persistent_cache;

  if (p_cache) {
    Slice key{file->GetFileName()};
    key.remove_specified_prefix(options.dfs_storage_path_prefix);
    // Try Read From Persistent Cache
    s = PersistentCacheHelper::TryReadFromPersistentCache(p_cache, key, offset, len, &footer_input,
                                                          &scratch);
    if (s.ok()) {
      // Read Success
      s = footer.DecodeFrom(&footer_input);
      if (!s.ok()) {
        // Parse footer failed means this sst file is invalid, remove it.
        options.persistent_cache->ForceEvict(key);
      }
    }
  }

  if (!p_cache || !s.ok()) {
    // Disable Persistent Cache or
    // parse footer failed or
    // read file failed, just read from dfs.
    s = ReadSstFile(file, options.use_direct_io_read, offset, len, &footer_input, &scratch);
    if (!s.ok()) {
      return s;
    }
    s = footer.DecodeFrom(&footer_input);
    if (!s.ok()) {
      return s;
    }
  }

  // Read the index block
  ReadOptions opt(&options);
  opt.verify_checksums = true;
  BlockContents contents;
  Block* index_block = NULL;
  if (s.ok()) {
    s = ReadBlock(file, opt, footer.index_handle(), &contents);
    if (s.ok()) {
      index_block = new Block(contents);
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep;
    rep->fsize = size;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  } else {
    if (index_block) delete index_block;
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == NULL) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt(&(rep_->options));
  opt.verify_checksums = true;
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt(&(rep_->options));
  opt.verify_checksums = true;
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
    rep_->filter_data_size = block.data.size();
    rep_->filter_block_size_total.Add(rep_->filter_data_size);
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options, const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(), &DeleteCachedBlock);
          }

          if (table->rep_->options.persistent_cache && options.fill_persistent_cache &&
              !contents.read_from_persistent_cache) {
            std::string fname = table->rep_->file->GetFileName();
            Slice persistent_cache_key{fname};
            persistent_cache_key.remove_specified_prefix(options.db_opt->dfs_storage_path_prefix);
            PersistentCacheHelper::ScheduleCopyToLocal(
                table->rep_->options.env, table->rep_->file->GetFileName(), table->rep_->fsize,
                persistent_cache_key.ToString(), table->rep_->options.persistent_cache);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(options.db_opt->comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewIterator(options, Slice(), Slice());
}

void DeletePrefetchBlockReader(void* arg1, void*) {
  delete reinterpret_cast<PrefetchBlockReader*>(arg1);
}

Iterator* Table::NewIterator(const ReadOptions& options, const Slice& smallest,
                             const Slice& largest) const {
  if (options.prefetch_scan) {
    auto prefetch_block_reader = new PrefetchBlockReader(rep_->file, rep_->fsize);
    auto iter = new TableIter(
        NewTwoLevelIterator(new IndexBlockIter(options, rep_->index_block, rep_->filter),
                            *prefetch_block_reader, nullptr, options),
        options.db_opt->comparator, smallest, largest);
    iter->RegisterCleanup(&DeletePrefetchBlockReader, prefetch_block_reader, nullptr);
    return iter;
  } else {
    return new TableIter(
        NewTwoLevelIterator(new IndexBlockIter(options, rep_->index_block, rep_->filter),
                            Table::BlockReader, const_cast<Table*>(this), options),
        options.db_opt->comparator, smallest, largest);
  }
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(options.db_opt->comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        ParsedInternalKey ikey;
        ParseInternalKey(block_iter->key(), &ikey);
        if (!RollbackDrop(ikey.sequence, options.rollbacks)) {
          (*saver)(arg, block_iter->key(), block_iter->value());
        }
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

uint64_t Table::IndexBlockSize() const { return rep_->index_block->size(); }

}  // namespace leveldb
