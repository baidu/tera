// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <malloc.h>
#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "db/dbformat.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

class TableIter : public Iterator {
 public:
  TableIter(Iterator* iter,
            const Comparator* comparator,
            const Slice& smallest,
            const Slice& largest)
      : iter_(iter),
        comparator_(comparator),
        smallest_(smallest.ToString()),
        largest_(largest.ToString()) { }

  virtual ~TableIter() {
    delete iter_;
  }

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
  virtual void Next() {
    iter_->Next();
  }
  virtual void Prev() {
    iter_->Prev();
  }
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
  virtual Status status() const {
    return iter_->status();
  }

 private:
  Iterator* iter_;
  const Comparator* const comparator_;
  std::string smallest_;
  std::string largest_;
};

class IndexBlockIter : public Iterator {
 public:
    IndexBlockIter(const ReadOptions& opts,
                   Block* index_block,
                   FilterBlockReader* filter)
      : valid_(false),
        iter_(index_block->NewIterator(opts.db_opt->comparator)),
        comparator_(opts.db_opt->comparator),
        filter_(filter),
        read_single_row_(opts.read_single_row),
        row_start_key_(opts.row_start_key, kMaxSequenceNumber, kValueTypeForSeek),
        row_end_key_(opts.row_end_key, kMaxSequenceNumber, kValueTypeForSeek) {
  }
  virtual ~IndexBlockIter() {
    delete iter_;
  }
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
  virtual bool Valid() const {
    return valid_;
  }
  virtual Slice key() const {
    assert(Valid());
    return iter_->key();
  }
  virtual Slice value() const {
    assert(Valid());
    return iter_->value();
  }
  virtual Status status() const {
    return iter_->status();
  }

 private:
  void SkipUnmatchedBlocksForward() {
    valid_ = false;
    while (iter_->Valid()) {
      if (!read_single_row_) {
        valid_ = true;
        break;
      }
      if (!valid_index_key_.empty() && comparator_->Compare(iter_->key(), valid_index_key_) > 0) {
        //Log("bloomfilter: skip block by range");
        break;
      }
      if (comparator_->Compare(iter_->key(), row_end_key_.Encode()) >= 0 &&
          (valid_index_key_.empty() || comparator_->Compare(iter_->key(), valid_index_key_) < 0)) {
        valid_index_key_ = iter_->key().ToString();
      }
      if (CheckFilter()) {
        valid_ = true;
        //Log("bloomfilter: valid block");
        break;
      }
      //Log("bloomfilter: skip block by bloom");
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
        //Log("bloomfilter: skip block by range");
        break;
      }
      if (comparator_->Compare(iter_->key(), row_end_key_.Encode()) >= 0 &&
          (valid_index_key_.empty() || comparator_->Compare(iter_->key(), valid_index_key_) < 0)) {
        valid_index_key_ = iter_->key().ToString();
      }
      if (CheckFilter()) {
        valid_ = true;
        //Log("bloomfilter: valid block");
        break;
      }
      //Log("bloomfilter: skip block by bloom");
      iter_->Prev();
    }
  }
  bool CheckFilter() {
    assert(iter_->Valid());
    Slice handle_value = iter_->value();
    BlockHandle handle;
    if (!read_single_row_ ||
        filter_ == NULL ||
        !handle.DecodeFrom(&handle_value).ok() ||
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

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}


// This iterator is just used in long-scan cases, like compact and batch scan.
// It'll prefetch some continuous data blocks from SSD or dfs in a single read-operation
// for reducing iops and maximizing throughput.
class PrefetchScanIterator : public Iterator {
 public:
    PrefetchScanIterator(RandomAccessFile* file,
                         const ReadOptions& opt,
                         Iterator* index_block_iterator)
      : file_(file),
        index_block_iterator_(index_block_iterator),
        block_iterator_(NULL),
        handles_iterator_(handles_.end()),
        option_(opt) {
  }
  virtual ~PrefetchScanIterator() {
    delete index_block_iterator_;
    delete block_iterator_;
  }
  virtual void Seek(const Slice& target) {
    index_block_iterator_->Seek(target);
    if (index_block_iterator_->Valid()) {
      PrefetchBlockContent();
    }
    if (block_iterator_) {
      block_iterator_->Seek(target);
    }
  }
  virtual void SeekToFirst() {
    index_block_iterator_->SeekToFirst();
    if (index_block_iterator_->Valid()) {
      PrefetchBlockContent();
    }
  }
  virtual void SeekToLast() {
    index_block_iterator_->SeekToLast();
    if (index_block_iterator_->Valid()) {
      PrefetchBlockContent();
    }
    if (block_iterator_) {
      block_iterator_->SeekToLast();
    }
  }
  virtual void Next() {
    assert(Valid());
    /*
     * block_content_ is the file data we prefetched.
     * handles_ is a vector of block handles that record [offset, size] of each block in block_content_.
     * handles_iterator_ points to one block handle in handels_.
     * block_iterator_ is the iterator of the block currently in use.
     *
     * So when Next() is called, we first tried to call block_iterator_'s Next().
     * If it is valid, we don't need to do other things.
     * Otherwise, it means that we reach the end of current block.
     * Then, we try to load next block by moving handles_iterator_ to the next block handle,
     * and load this block from prefetched data (block_content_).
     * If handles_iterator_ points to the last block handle in handles_,
     * it means that we reach the end of prefetched blocks, so PrefetchBlockContent() is called to read new
     * blocks from file.
    */
    block_iterator_->Next();
    if (!block_iterator_->Valid()) {
      if (++handles_iterator_ != handles_.end()) {
        LoadBlockIterator();
      } else {
        PrefetchBlockContent();
      }
    }
  }

  virtual void Prev() {
    //This iterator should just be used in scan,
    //so prev() should never be called.
    assert(Valid());
    abort();
  }
  virtual bool Valid() const {
    return block_iterator_ && block_iterator_->Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return block_iterator_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return block_iterator_->value();
  }

  virtual Status status() const {
    if (!index_block_iterator_->status().ok()) {
      return index_block_iterator_->status();
    } else if (block_iterator_ && !block_iterator_->status().ok()) {
      return block_iterator_->status();
    } else {
      return status_;
    }
  }

 private:
  // Load the iterator of the block that handles_iterator_ currently points to.
  void LoadBlockIterator() {
    assert(handles_iterator_ != handles_.end());
    
    delete block_iterator_;
    block_iterator_ = NULL;
    //Caculate current block's offset in block_content_.
    uint64_t offset = handles_iterator_->offset() - handles_[0].offset();
    uint64_t size = handles_iterator_->size() + kBlockTrailerSize;

    assert(offset + size <= block_content_.size());
    Slice block_with_trailer(&block_content_[0] + offset, size);

    BlockContents contents;
    Block* block = NULL;
    //Parse block from block_with_trailer slice.
    Status s = ParseBlock(handles_iterator_->size(),
                          handles_iterator_->offset(),
                          option_,
                          block_with_trailer,
                          &contents);
    if (s.ok()) {
      block = new Block(contents);
    }

    if (block != NULL) {
      //Get current block's iterator.
      block_iterator_ = block->NewIterator(option_.db_opt->comparator);
      block_iterator_->RegisterCleanup(&DeleteBlock, block, NULL);
      block_iterator_->SeekToFirst();
    } else {
      block_iterator_ = NewErrorIterator(s);
    }
  }

  // Prefetch some continuous data blocks for reducing iops.
  // Their total size is less than tera_tabletnode_prefetch_scan_size.
  void PrefetchBlockContent() {
    handles_.clear();
    uint64_t current_size(0);
    while(index_block_iterator_->Valid() &&
          current_size < option_.prefetch_scan_size) {
      BlockHandle handle;
      Slice input = index_block_iterator_->value();
      status_ = handle.DecodeFrom(&input);
      if (!status_.ok()) {
        break;
      }
      current_size += handle.size() + kBlockTrailerSize;
      handles_.push_back(handle);
      index_block_iterator_->Next();
    }
    handles_iterator_ = handles_.end();

    if (status_.ok() && !handles_.empty()) {
      Options db_opt = *(option_.db_opt);
      uint64_t offset = handles_[0].offset();
      uint64_t blocks_size = handles_.back().offset() - offset;
      blocks_size += handles_.back().size() + kBlockTrailerSize;
      Slice contents;
      char* buf = NULL;
      status_ = ReadSstFile(file_, db_opt.use_direct_io_read, offset, blocks_size, &contents, &buf);
      if (!status_.ok()) {
        return;
      }
      block_content_.assign(contents.data(), contents.size());
      FreeBuf(buf, db_opt.use_direct_io_read);

      if (block_content_.size() != blocks_size) {
        status_ = Status::Corruption("truncated block read");
        return;
      }
      handles_iterator_ = handles_.begin();
      LoadBlockIterator();
    }
  }

private:

  Status status_;
  std::string block_content_;
  std::vector<BlockHandle> handles_;

  RandomAccessFile* file_;
  Iterator* index_block_iterator_;
  Iterator* block_iterator_;
  std::vector<BlockHandle>::iterator handles_iterator_;
  ReadOptions option_;
};


Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  size_t len = Footer::kEncodedLength;;
  uint64_t offset = size - Footer::kEncodedLength;
  Slice footer_input;
  char* buf = NULL;
  Status s = ReadSstFile(file, options.use_direct_io_read, offset, len, &footer_input, &buf);
  if (!s.ok()) {
    return s;
  }
  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  FreeBuf(buf, options.use_direct_io_read);
  if (!s.ok()) {
      return s;
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
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
}

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
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
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
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
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

Iterator* Table::NewIterator(const ReadOptions& options,
                             const Slice& smallest,
                             const Slice& largest) const {
  if (options.prefetch_scan) {
    return new TableIter(
            new PrefetchScanIterator(rep_->file, options, rep_->index_block->NewIterator(options.db_opt->comparator)),
            options.db_opt->comparator, smallest, largest);
  } else {
    return new TableIter(
        NewTwoLevelIterator(
            new IndexBlockIter(options, rep_->index_block, rep_->filter),
            &Table::BlockReader, const_cast<Table*>(this), options),
            options.db_opt->comparator, smallest, largest);
  }
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(options.db_opt->comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL &&
        handle.DecodeFrom(&handle_value).ok() &&
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
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
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

uint64_t Table::IndexBlockSize() const {
    return rep_->index_block->size();
}

}  // namespace leveldb
