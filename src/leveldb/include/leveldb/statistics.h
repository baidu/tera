// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
#define STORAGE_LEVELDB_INCLUDE_STATISTICS_H_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <vector>

namespace leveldb {

/**
 * Keep adding ticker's here.
 *  1. Any ticker should be added before TICKER_ENUM_MAX.
 *  2. Add a readable string in TickersNameMap below for the newly added ticker.
 */
enum Tickers : uint32_t {
  // total block cache misses
  // REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
  //                               BLOCK_CACHE_FILTER_MISS +
  //                               BLOCK_CACHE_DATA_MISS;
  BLOCK_CACHE_MISS = 0,
  // total block cache hit
  // REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
  //                              BLOCK_CACHE_FILTER_HIT +
  //                              BLOCK_CACHE_DATA_HIT;
  BLOCK_CACHE_HIT,
  // # of blocks added to block cache.
  BLOCK_CACHE_ADD,
  // # of failures when adding blocks to block cache.
  BLOCK_CACHE_ADD_FAILURES,
  // # of times cache miss when accessing index block from block cache.
  BLOCK_CACHE_INDEX_MISS,
  // # of times cache hit when accessing index block from block cache.
  BLOCK_CACHE_INDEX_HIT,
  // # of bytes of index blocks inserted into cache
  BLOCK_CACHE_INDEX_BYTES_INSERT,
  // # of bytes of index block erased from cache
  BLOCK_CACHE_INDEX_BYTES_EVICT,
  // # of times cache miss when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_MISS,
  // # of times cache hit when accessing filter block from block cache.
  BLOCK_CACHE_FILTER_HIT,
  // # of bytes of bloom filter blocks inserted into cache
  BLOCK_CACHE_FILTER_BYTES_INSERT,
  // # of bytes of bloom filter block erased from cache
  BLOCK_CACHE_FILTER_BYTES_EVICT,
  // # of times cache miss when accessing data block from block cache.
  BLOCK_CACHE_DATA_MISS,
  // # of times cache hit when accessing data block from block cache.
  BLOCK_CACHE_DATA_HIT,
  // # of bytes read from cache.
  BLOCK_CACHE_BYTES_READ,
  // # of bytes written into cache.
  BLOCK_CACHE_BYTES_WRITE,

  // # of times bloom filter has avoided file reads.
  BLOOM_FILTER_USEFUL,

  // # persistent cache hit
  PERSISTENT_CACHE_HIT,
  // # persistent cache miss
  PERSISTENT_CACHE_MISS,

  // # of memtable hits.
  MEMTABLE_HIT,
  // # of memtable misses.
  MEMTABLE_MISS,

  // # of Get() queries served by L0
  GET_HIT_L0,
  // # of Get() queries served by L1
  GET_HIT_L1,
  // # of Get() queries served by L2 and up
  GET_HIT_L2_AND_UP,

  /**
   * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
   * There are 3 reasons currently.
   * 覆盖写；删除；用户函数删除
   */
  COMPACTION_KEY_DROP_NEWER_ENTRY,  // key was written with a newer value.
  COMPACTION_KEY_DROP_OBSOLETE,     // The key is obsolete.
  COMPACTION_KEY_DROP_USER,  // user compaction function has dropped the key.

  // Number of keys written to the database via the Put and Write call's
  NUMBER_KEYS_WRITTEN,
  // Number of Keys read,
  NUMBER_KEYS_READ,
  // Number keys updated, if inplace update is enabled
  NUMBER_KEYS_UPDATED,
  // The number of uncompressed bytes issued by DB::Put(), DB::Delete(),
  // DB::Merge(), and DB::Write().
  BYTES_WRITTEN,
  // The number of uncompressed bytes read from DB::Get().  It could be
  // either from memtables, cache, or table files.
  // For the number of logical bytes read from DB::MultiGet(),
  // please use NUMBER_MULTIGET_BYTES_READ.
  BYTES_READ,
  // The number of calls to seek/next/prev
  NUMBER_DB_SEEK,
  NUMBER_DB_NEXT,
  NUMBER_DB_PREV,
  // The number of calls to seek/next/prev that returned data
  NUMBER_DB_SEEK_FOUND,
  NUMBER_DB_NEXT_FOUND,
  NUMBER_DB_PREV_FOUND,
  // The number of uncompressed bytes read from an iterator.
  // Includes size of key and value.
  ITER_BYTES_READ,
  NO_FILE_CLOSES,
  NO_FILE_OPENS,
  NO_FILE_ERRORS,
  // DEPRECATED Time system had to wait to do LO-L1 compactions
  STALL_L0_SLOWDOWN_MICROS,
  // DEPRECATED Time system had to wait to move memtable to L1.
  STALL_MEMTABLE_COMPACTION_MICROS,
  // DEPRECATED write throttle because of too many files in L0
  STALL_L0_NUM_FILES_MICROS,
  // Writer has to wait for compaction or flush to finish.
  STALL_MICROS,
  // The wait time for db mutex.
  // Disabled by default. To enable it set stats level to kAll
  DB_MUTEX_WAIT_MICROS,
  RATE_LIMIT_DELAY_MILLIS,
  NO_ITERATORS,  // number of iterators currently open

  // Number of MultiGet calls, keys read, and bytes read
  NUMBER_MULTIGET_CALLS,
  NUMBER_MULTIGET_KEYS_READ,
  NUMBER_MULTIGET_BYTES_READ,

  // Number of deletes records that were not required to be
  // written to storage because key does not exist
  NUMBER_FILTERED_DELETES,
  NUMBER_MERGE_FAILURES,
  SEQUENCE_NUMBER,

  // number of times bloom was checked before creating iterator on a
  // file, and the number of times the check was useful in avoiding
  // iterator creation (and thus likely IOPs).
  BLOOM_FILTER_PREFIX_CHECKED,
  BLOOM_FILTER_PREFIX_USEFUL,

  // Number of times we had to reseek inside an iteration to skip
  // over large number of keys with same userkey.
  NUMBER_OF_RESEEKS_IN_ITERATION,

  // Record the number of calls to GetUpadtesSince. Useful to keep track of
  // transaction log iterator refreshes
  GET_UPDATES_SINCE_CALLS,
  BLOCK_CACHE_COMPRESSED_MISS,  // miss in the compressed block cache
  BLOCK_CACHE_COMPRESSED_HIT,   // hit in the compressed block cache
  // Number of blocks added to comopressed block cache
  BLOCK_CACHE_COMPRESSED_ADD,
  // Number of failures when adding blocks to compressed block cache
  BLOCK_CACHE_COMPRESSED_ADD_FAILURES,
  WAL_FILE_SYNCED,  // Number of times WAL sync is done
  WAL_FILE_BYTES,   // Number of bytes written to WAL

  // Writes can be processed by requesting thread or by the thread at the
  // head of the writers queue.
  WRITE_DONE_BY_SELF,
  WRITE_DONE_BY_OTHER,  // Equivalent to writes done for others
  WRITE_TIMEDOUT,       // Number of writes ending up with timed-out.
  WRITE_WITH_WAL,       // Number of Write calls that request WAL
  COMPACT_READ_BYTES,   // Bytes read during compaction
  COMPACT_WRITE_BYTES,  // Bytes written during compaction
  FLUSH_WRITE_BYTES,    // Bytes written during flush

  // Number of table's properties loaded directly from file, without creating
  // table reader object.
  NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
  NUMBER_SUPERVERSION_ACQUIRES,
  NUMBER_SUPERVERSION_RELEASES,
  NUMBER_SUPERVERSION_CLEANUPS,
  NUMBER_BLOCK_NOT_COMPRESSED,
  MERGE_OPERATION_TOTAL_TIME,
  FILTER_OPERATION_TOTAL_TIME,

  // Row cache.
  ROW_CACHE_HIT,
  ROW_CACHE_MISS,

  TICKER_ENUM_MAX
};

// The order of items listed in  Tickers should be the same as
// the order listed in TickersNameMap
const std::vector<std::pair<Tickers, std::string> > TickersNameMap = {
    {BLOCK_CACHE_MISS, "leveldb.block.cache.miss"},
    {BLOCK_CACHE_HIT, "leveldb.block.cache.hit"},
    {BLOCK_CACHE_ADD, "leveldb.block.cache.add"},
    {BLOCK_CACHE_ADD_FAILURES, "leveldb.block.cache.add.failures"},
    {BLOCK_CACHE_INDEX_MISS, "leveldb.block.cache.index.miss"},
    {BLOCK_CACHE_INDEX_HIT, "leveldb.block.cache.index.hit"},
    {BLOCK_CACHE_INDEX_BYTES_INSERT, "leveldb.block.cache.index.bytes.insert"},
    {BLOCK_CACHE_INDEX_BYTES_EVICT, "leveldb.block.cache.index.bytes.evict"},
    {BLOCK_CACHE_FILTER_MISS, "leveldb.block.cache.filter.miss"},
    {BLOCK_CACHE_FILTER_HIT, "leveldb.block.cache.filter.hit"},
    {BLOCK_CACHE_FILTER_BYTES_INSERT,
     "leveldb.block.cache.filter.bytes.insert"},
    {BLOCK_CACHE_FILTER_BYTES_EVICT, "leveldb.block.cache.filter.bytes.evict"},
    {BLOCK_CACHE_DATA_MISS, "leveldb.block.cache.data.miss"},
    {BLOCK_CACHE_DATA_HIT, "leveldb.block.cache.data.hit"},
    {BLOCK_CACHE_BYTES_READ, "leveldb.block.cache.bytes.read"},
    {BLOCK_CACHE_BYTES_WRITE, "leveldb.block.cache.bytes.write"},
    {BLOOM_FILTER_USEFUL, "leveldb.bloom.filter.useful"},
    {MEMTABLE_HIT, "leveldb.memtable.hit"},
    {MEMTABLE_MISS, "leveldb.memtable.miss"},
    {GET_HIT_L0, "leveldb.l0.hit"},
    {GET_HIT_L1, "leveldb.l1.hit"},
    {GET_HIT_L2_AND_UP, "leveldb.l2andup.hit"},
    {COMPACTION_KEY_DROP_NEWER_ENTRY, "leveldb.compaction.key.drop.new"},
    {COMPACTION_KEY_DROP_OBSOLETE, "leveldb.compaction.key.drop.obsolete"},
    {COMPACTION_KEY_DROP_USER, "leveldb.compaction.key.drop.user"},
    {NUMBER_KEYS_WRITTEN, "leveldb.number.keys.written"},
    {NUMBER_KEYS_READ, "leveldb.number.keys.read"},
    {NUMBER_KEYS_UPDATED, "leveldb.number.keys.updated"},
    {BYTES_WRITTEN, "leveldb.bytes.written"},
    {BYTES_READ, "leveldb.bytes.read"},
    {NUMBER_DB_SEEK, "leveldb.number.db.seek"},
    {NUMBER_DB_NEXT, "leveldb.number.db.next"},
    {NUMBER_DB_PREV, "leveldb.number.db.prev"},
    {NUMBER_DB_SEEK_FOUND, "leveldb.number.db.seek.found"},
    {NUMBER_DB_NEXT_FOUND, "leveldb.number.db.next.found"},
    {NUMBER_DB_PREV_FOUND, "leveldb.number.db.prev.found"},
    {ITER_BYTES_READ, "leveldb.db.iter.bytes.read"},
    {NO_FILE_CLOSES, "leveldb.no.file.closes"},
    {NO_FILE_OPENS, "leveldb.no.file.opens"},
    {NO_FILE_ERRORS, "leveldb.no.file.errors"},
    {STALL_L0_SLOWDOWN_MICROS, "leveldb.l0.slowdown.micros"},
    {STALL_MEMTABLE_COMPACTION_MICROS, "leveldb.memtable.compaction.micros"},
    {STALL_L0_NUM_FILES_MICROS, "leveldb.l0.num.files.stall.micros"},
    {STALL_MICROS, "leveldb.stall.micros"},
    {DB_MUTEX_WAIT_MICROS, "leveldb.db.mutex.wait.micros"},
    {RATE_LIMIT_DELAY_MILLIS, "leveldb.rate.limit.delay.millis"},
    {NO_ITERATORS, "leveldb.num.iterators"},
    {NUMBER_MULTIGET_CALLS, "leveldb.number.multiget.get"},
    {NUMBER_MULTIGET_KEYS_READ, "leveldb.number.multiget.keys.read"},
    {NUMBER_MULTIGET_BYTES_READ, "leveldb.number.multiget.bytes.read"},
    {NUMBER_FILTERED_DELETES, "leveldb.number.deletes.filtered"},
    {NUMBER_MERGE_FAILURES, "leveldb.number.merge.failures"},
    {SEQUENCE_NUMBER, "leveldb.sequence.number"},
    {BLOOM_FILTER_PREFIX_CHECKED, "leveldb.bloom.filter.prefix.checked"},
    {BLOOM_FILTER_PREFIX_USEFUL, "leveldb.bloom.filter.prefix.useful"},
    {NUMBER_OF_RESEEKS_IN_ITERATION, "leveldb.number.reseeks.iteration"},
    {GET_UPDATES_SINCE_CALLS, "leveldb.getupdatessince.calls"},
    {BLOCK_CACHE_COMPRESSED_MISS, "leveldb.block.cachecompressed.miss"},
    {BLOCK_CACHE_COMPRESSED_HIT, "leveldb.block.cachecompressed.hit"},
    {BLOCK_CACHE_COMPRESSED_ADD, "leveldb.block.cachecompressed.add"},
    {BLOCK_CACHE_COMPRESSED_ADD_FAILURES,
     "leveldb.block.cachecompressed.add.failures"},
    {WAL_FILE_SYNCED, "leveldb.wal.synced"},
    {WAL_FILE_BYTES, "leveldb.wal.bytes"},
    {WRITE_DONE_BY_SELF, "leveldb.write.self"},
    {WRITE_DONE_BY_OTHER, "leveldb.write.other"},
    {WRITE_WITH_WAL, "leveldb.write.wal"},
    {FLUSH_WRITE_BYTES, "leveldb.flush.write.bytes"},
    {COMPACT_READ_BYTES, "leveldb.compact.read.bytes"},
    {COMPACT_WRITE_BYTES, "leveldb.compact.write.bytes"},
    {NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
     "leveldb.number.direct.load.table.properties"},
    {NUMBER_SUPERVERSION_ACQUIRES, "leveldb.number.superversion_acquires"},
    {NUMBER_SUPERVERSION_RELEASES, "leveldb.number.superversion_releases"},
    {NUMBER_SUPERVERSION_CLEANUPS, "leveldb.number.superversion_cleanups"},
    {NUMBER_BLOCK_NOT_COMPRESSED, "leveldb.number.block.not_compressed"},
    {MERGE_OPERATION_TOTAL_TIME, "leveldb.merge.operation.time.nanos"},
    {FILTER_OPERATION_TOTAL_TIME, "leveldb.filter.operation.time.nanos"},
    {ROW_CACHE_HIT, "leveldb.row.cache.hit"},
    {ROW_CACHE_MISS, "leveldb.row.cache.miss"},
};

/**
 * Keep adding histogram's here.
 * Any histogram whould have value less than HISTOGRAM_ENUM_MAX
 * Add a new Histogram by assigning it the current value of HISTOGRAM_ENUM_MAX
 * Add a string representation in HistogramsNameMap below
 * And increment HISTOGRAM_ENUM_MAX
 */
enum Histograms : uint32_t {
  DB_GET = 0,
  DB_WRITE,
  COMPACTION_TIME,
  SUBCOMPACTION_SETUP_TIME,
  TABLE_SYNC_MICROS,
  COMPACTION_OUTFILE_SYNC_MICROS,
  WAL_FILE_SYNC_MICROS,
  MANIFEST_FILE_SYNC_MICROS,
  // TIME SPENT IN IO DURING TABLE OPEN
  TABLE_OPEN_IO_MICROS,
  DB_MULTIGET,
  READ_BLOCK_COMPACTION_MICROS,
  READ_BLOCK_GET_MICROS,
  WRITE_RAW_BLOCK_MICROS,
  STALL_L0_SLOWDOWN_COUNT,
  STALL_MEMTABLE_COMPACTION_COUNT,
  STALL_L0_NUM_FILES_COUNT,
  HARD_RATE_LIMIT_DELAY_COUNT,
  SOFT_RATE_LIMIT_DELAY_COUNT,
  NUM_FILES_IN_SINGLE_COMPACTION,
  DB_SEEK,
  WRITE_STALL,
  SST_READ_MICROS,
  // The number of subcompactions actually scheduled during a compaction
  NUM_SUBCOMPACTIONS_SCHEDULED,
  // Value size distribution in each operation
  BYTES_PER_READ,
  BYTES_PER_WRITE,
  BYTES_PER_MULTIGET,
  // tera block cache spec
  TERA_BLOCK_CACHE_PREAD_QUEUE,
  TERA_BLOCK_CACHE_PREAD_SSD_READ,
  TERA_BLOCK_CACHE_PREAD_FILL_USER_DATA,
  TERA_BLOCK_CACHE_PREAD_RELEASE_BLOCK,
  HISTOGRAM_ENUM_MAX,  // TODO(ldemailly): enforce HistogramsNameMap match
};

const std::vector<std::pair<Histograms, std::string> > HistogramsNameMap = {
    {DB_GET, "leveldb.db.get.micros"},
    {DB_WRITE, "leveldb.db.write.micros"},
    {COMPACTION_TIME, "leveldb.compaction.times.micros"},
    {SUBCOMPACTION_SETUP_TIME, "leveldb.subcompaction.setup.times.micros"},
    {TABLE_SYNC_MICROS, "leveldb.table.sync.micros"},
    {COMPACTION_OUTFILE_SYNC_MICROS, "leveldb.compaction.outfile.sync.micros"},
    {WAL_FILE_SYNC_MICROS, "leveldb.wal.file.sync.micros"},
    {MANIFEST_FILE_SYNC_MICROS, "leveldb.manifest.file.sync.micros"},
    {TABLE_OPEN_IO_MICROS, "leveldb.table.open.io.micros"},
    {DB_MULTIGET, "leveldb.db.multiget.micros"},
    {READ_BLOCK_COMPACTION_MICROS, "leveldb.read.block.compaction.micros"},
    {READ_BLOCK_GET_MICROS, "leveldb.read.block.get.micros"},
    {WRITE_RAW_BLOCK_MICROS, "leveldb.write.raw.block.micros"},
    {STALL_L0_SLOWDOWN_COUNT, "leveldb.l0.slowdown.count"},
    {STALL_MEMTABLE_COMPACTION_COUNT, "leveldb.memtable.compaction.count"},
    {STALL_L0_NUM_FILES_COUNT, "leveldb.num.files.stall.count"},
    {HARD_RATE_LIMIT_DELAY_COUNT, "leveldb.hard.rate.limit.delay.count"},
    {SOFT_RATE_LIMIT_DELAY_COUNT, "leveldb.soft.rate.limit.delay.count"},
    {NUM_FILES_IN_SINGLE_COMPACTION, "leveldb.numfiles.in.singlecompaction"},
    {DB_SEEK, "leveldb.db.seek.micros"},
    {WRITE_STALL, "leveldb.db.write.stall"},
    {SST_READ_MICROS, "leveldb.sst.read.micros"},
    {NUM_SUBCOMPACTIONS_SCHEDULED, "leveldb.num.subcompactions.scheduled"},
    {BYTES_PER_READ, "leveldb.bytes.per.read"},
    {BYTES_PER_WRITE, "leveldb.bytes.per.write"},
    {BYTES_PER_MULTIGET, "leveldb.bytes.per.multiget"},
    {TERA_BLOCK_CACHE_PREAD_QUEUE, "tera.block_cache.pread_queue"},
    {TERA_BLOCK_CACHE_PREAD_SSD_READ, "tera.block_cache.pread_ssd_read"},
    {TERA_BLOCK_CACHE_PREAD_FILL_USER_DATA, "tera.block_cache.pread_fill_user_data"},
    {TERA_BLOCK_CACHE_PREAD_RELEASE_BLOCK, "tera.block_cache.pread_release_block"},
};

struct HistogramData {
  double median; // 中值
  double percentile95;
  double percentile99; // 99分为点
  double average;
  double standard_deviation;
};

// Analyze the performance of a db
class Statistics {
 public:
  virtual ~Statistics() {}

  virtual int64_t GetTickerCount(uint32_t ticker_type) = 0;
  virtual void RecordTick(uint32_t ticker_type, uint64_t count = 0) = 0;
  virtual void SetTickerCount(uint32_t ticker_type, uint64_t count) = 0;

  virtual void GetHistogramData(uint32_t type,
                                HistogramData* const data) = 0;
  virtual std::string GetBriefHistogramString(uint32_t type) { return ""; }
  virtual std::string GetHistogramString(uint32_t type) const { return ""; }
  virtual void MeasureTime(uint32_t histogram_type, uint64_t time) = 0;
  virtual void ClearHistogram(uint32_t type) = 0;

  // String representation of the statistic object.
  virtual std::string ToString() {
    // Do nothing by default
    return std::string("ToString(): not implemented");
  }
  virtual void ClearAll() = 0;
};

// Create a concrete DBStatistics object
Statistics* CreateDBStatistics();

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
