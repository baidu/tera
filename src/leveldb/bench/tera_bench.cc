// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include <stdlib.h>
#include "util/random.h"
#include "util/testutil.h"

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//
//   seq       -- output N values in sequential key order
//   random    -- output N values in random key order
static const char* FLAGS_benchmarks =
    "seq,"
    "random,"
    ;

// Number of key/values to place in database
static int FLAGS_num = 1000000;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Size of each key
static int FLAGS_key_size = 16;

// Size of each value
static int FLAGS_value_size = 100;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// Number of tablets
static int FLAGS_tablet_num = 1;

// CF list in format: "cf0:q1,cf1:q2..."
static std::string FLAGS_cf_list = "";

// Seq mode start key
static int FLAGS_start_key = 0;

// Value generator's seed
static int FLAGS_value_seed = 301;

// Key generator's seed
static int FLAGS_key_seed = 301;

// Key step, used in seq key generation
static int FLAGS_key_step = 1;

namespace leveldb {

// Helper for quickly generating random data.
namespace {
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(FLAGS_value_seed);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(int len) {
    if (pos_ + len > static_cast<int>(data_.size())) {
      pos_ = 0;
      assert(len < static_cast<int>(data_.size()));
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

}  // namespace

class Benchmark {
 private:
  int num_;
  int reads_;
  double start_;
  int64_t bytes_;
  std::string message_;
  RandomGenerator gen_;
  Random rand_;
  Random** tablet_rand_vector_;

  // State kept for progress messages
  int done_;
  int next_report_;     // When to report next

  void Start() {
    start_ = Env::Default()->NowMicros() * 1e-6;
    bytes_ = 0;
    message_.clear();
    done_ = 0;
    next_report_ = 100;
  }

  void Stop(const Slice& name) {
    double finish = Env::Default()->NowMicros() * 1e-6;

    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    if (bytes_ > 0) {
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / (finish - start_));
      if (!message_.empty()) {
        message_  = std::string(rate) + " " + message_;
      } else {
        message_ = rate;
      }
    }

    fprintf(stderr, "%-12s : %11.3f micros/op;%s%s\n",
            name.ToString().c_str(),
            (finish - start_) * 1e6 / done_,
            (message_.empty() ? "" : " "),
            message_.c_str());
    fflush(stderr);
  }

 public:
  enum Order {
    SEQUENTIAL,
    RANDOM
  };

  Benchmark()
  : num_(FLAGS_num),
    reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
    bytes_(0),
    rand_(FLAGS_key_seed) {
    tablet_rand_vector_ = new Random*[FLAGS_tablet_num];
    for (int i = 0; i < FLAGS_tablet_num; i++) {
      tablet_rand_vector_[i] = new Random(FLAGS_key_seed);
    }
  }

  ~Benchmark() {
    for (int i = 0; i < FLAGS_tablet_num; i++) {
      delete tablet_rand_vector_[i];
    }
    delete[] tablet_rand_vector_;
  }

  void Run() {
    // parse cf list
    std::vector<std::string> cfs;
    if (FLAGS_cf_list != "") {
      std::string buffer = FLAGS_cf_list;
      size_t start_index = 0;
      size_t end_index = buffer.find(",");
      while (end_index != std::string::npos) {
        cfs.push_back(buffer.substr(start_index, end_index - start_index));
        start_index = end_index + 1;
        end_index = buffer.find(",", start_index);
      }
      cfs.push_back(buffer.substr(start_index));
    }

    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      Start();

      bool known = true;
      if (name == Slice("seq")) {
        Output(SEQUENTIAL, num_, FLAGS_value_size, cfs);
      } else if (name == Slice("random")) {
        Output(RANDOM, num_, FLAGS_value_size, cfs);
      } else {
        known = false;
        if (name != Slice()) {  // No error message for empty name
          fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
        }
      }
      if (known) {
        Stop(name);
      }
    }
  }

 private:

  void Output(Order order, int num_entries, int value_size, std::vector<std::string>& cfs) {
    if (num_entries != num_) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", num_entries);
      message_ = msg;
    }
    char ts[10];
    snprintf(ts, sizeof(ts), "%d", FLAGS_value_seed);

    // Write to database
    int i = FLAGS_start_key;
    int end_key = i + num_entries * FLAGS_key_step;
    for (; i < end_key; i += FLAGS_key_step)
    {
      const int t = rand_.Next() % FLAGS_tablet_num;
      const int k = (order == SEQUENTIAL) ? i : (tablet_rand_vector_[t]->Next());
      char key[10000];
      snprintf(key, sizeof(key), "%06d%0*d", t, FLAGS_key_size - 6, k);
      bytes_ += value_size + strlen(key);
      if (cfs.empty() == true) {
        fprintf(stdout, "%s\t%s\n", key, gen_.Generate(value_size).ToString().c_str());
      } else {
        for (size_t j = 0; j < cfs.size(); ++j) {
          fprintf(stdout, "%s\t%s\t%s\t%s\n", key, gen_.Generate(value_size).ToString().c_str(), cfs[j].c_str(), ts);
        }
      }
    }
  }
};

}  // namespace leveldb

int main(int argc, char** argv) {
  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    char cf_list[1024];
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--key_size=%d%c", &n, &junk) == 1 && n < 10000) {
      FLAGS_key_size = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--tablet_num=%d%c", &n, &junk) == 1) {
      FLAGS_tablet_num = n;
    } else if (sscanf(argv[i], "--cf=%s", cf_list) == 1) {
      FLAGS_cf_list = std::string(cf_list);
    } else if (sscanf(argv[i], "--start_key=%d%c", &n, &junk) == 1) {
      FLAGS_start_key = n;
    } else if (sscanf(argv[i], "--value_seed=%d%c", &n, &junk) == 1) {
      FLAGS_value_seed = n;
    } else if (sscanf(argv[i], "--key_seed=%d%c", &n, &junk) == 1) {
      FLAGS_key_seed = n;
    } else if (sscanf(argv[i], "--key_step=%d%c", &n, &junk) == 1) {
      FLAGS_key_step = n;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  leveldb::Benchmark benchmark;
  benchmark.Run();
  return 0;
}
