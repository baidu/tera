// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/statistics.h"
#include <assert.h>
#include "util/histogram.h"
#include "../utils/counter.h"

namespace leveldb {

class StatisticsImpl : public Statistics {
public:
  StatisticsImpl() {}

  ~StatisticsImpl() {}

  virtual int64_t GetTickerCount(uint32_t ticker_type) {
    return counter_[ticker_type].Get();
  }

  virtual void RecordTick(uint32_t ticker_type, uint64_t count = 0) {
    counter_[ticker_type].Add(count);
  }

  virtual void SetTickerCount(uint32_t ticker_type, uint64_t count) {
    counter_[ticker_type].Set(count);
  }

  virtual void MeasureTime(uint32_t type, uint64_t time) {
    hist_[type].Add(time);
  }

  virtual void GetHistogramData(uint32_t type,
                                HistogramData* const data) {
    data->median = hist_[type].Median();
    data->percentile95 = hist_[type].Percentile(95);
    data->percentile99 = hist_[type].Percentile(99);
    data->average = hist_[type].Average();
    data->standard_deviation = hist_[type].StandardDeviation();
  }

  virtual std::string GetHistogramString(uint32_t type) const {
    return hist_[type].ToString();
  }

  virtual std::string GetBriefHistogramString(uint32_t type) {
    assert(HistogramsNameMap[type].first == type);

    std::string res;
    char buffer[200];
    HistogramData hData;
    GetHistogramData(type, &hData);
    snprintf(buffer,
            200,
            "%s statistics Percentiles :=> 50 : %f 95 : %f 99 : %f\n",
            HistogramsNameMap[type].second.c_str(),
            hData.median,
            hData.percentile95,
            hData.percentile99);
    res.append(buffer);
    res.shrink_to_fit();
    return res;
  }

  void ClearHistogram(uint32_t type) {
    hist_[type].Clear();
  }

  // String representation of the statistic object.
  virtual std::string ToString() {
    std::string res;
    res.reserve(20000);
    for (uint32_t i = 0; i < TickersNameMap.size(); i++) {
      char buffer[200];
      snprintf(buffer, 200, "%s COUNT : %lu\n",
               TickersNameMap[i].second.c_str(), GetTickerCount(TickersNameMap[i].first));
      res.append(buffer);
    }
    for (uint32_t i = 0; i < HistogramsNameMap.size(); i++) {
      char buffer[200];
      HistogramData hData;
      GetHistogramData(HistogramsNameMap[i].first, &hData);
      snprintf(buffer,
               200,
               "%s statistics Percentiles :=> 50 : %f 95 : %f 99 : %f\n",
               HistogramsNameMap[i].second.c_str(),
               hData.median,
               hData.percentile95,
               hData.percentile99);
      res.append(buffer);
    }
    res.shrink_to_fit();
    return res;
  }

  void ClearAll() {
    for (uint32_t i = 0; i < TICKER_ENUM_MAX; i++) {
      counter_[i].Clear();
    }
    for (uint32_t i = 0; i < HISTOGRAM_ENUM_MAX; i++) {
      hist_[i].Clear();
    }
  }

private:
  tera::Counter counter_[TICKER_ENUM_MAX];
  Histogram hist_[HISTOGRAM_ENUM_MAX];
};

Statistics* CreateDBStatistics() {
  return new StatisticsImpl;
}

} // namespace leveldb
