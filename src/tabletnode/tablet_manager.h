// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLET_MANAGER_H_
#define TERA_TABLETNODE_TABLET_MANAGER_H_

#include <map>
#include <string>
#include <vector>

#include "common/mutex.h"

#include "io/tablet_io.h"
#include "proto/status_code.pb.h"

namespace tera {
namespace tabletnode {

struct TabletRange {
  TabletRange(const std::string& name, const std::string& start, const std::string& end)
      : table_name(name), key_start(start), key_end(end) {}

  bool operator<(const TabletRange& other) const {
    int cmp_ret = table_name.compare(other.table_name);
    if (cmp_ret < 0) {
      return true;
    } else if (cmp_ret == 0) {
      return key_start < other.key_start;
    } else {
      return false;
    }
  }

  bool operator==(const TabletRange& other) const {
    return (table_name == other.table_name && key_start == other.key_start);
  }

  std::string table_name;
  std::string key_start;
  std::string key_end;
};

class TabletManager {
 public:
  TabletManager();
  virtual ~TabletManager();

  virtual bool AddTablet(const std::string& table_name, const std::string& table_path,
                         const std::string& key_start, const std::string& key_end, int64_t ctime,
                         uint64_t version, io::TabletIO** tablet_io, StatusCode* status = NULL);

  virtual bool RemoveTablet(const std::string& table_name, const std::string& key_start,
                            const std::string& key_end, StatusCode* status = NULL);

  virtual io::TabletIO* GetTablet(const std::string& table_name, const std::string& key_start,
                                  const std::string& key_end, StatusCode* status = NULL);

  virtual io::TabletIO* GetTablet(const std::string& table_name, const std::string& key,
                                  StatusCode* status = NULL);

  virtual void GetAllTabletMeta(std::vector<TabletMeta*>* tablet_meta_list);

  virtual void GetAllTablets(std::vector<io::TabletIO*>* taletio_list);

  virtual bool RemoveAllTablets(bool force = false, StatusCode* status = NULL);

  uint32_t Size();

 private:
  mutable Mutex mutex_;

  std::map<TabletRange, io::TabletIO*> tablet_list_;
};

}  // namespace tabletnode
}  // namespace tera

#endif  // TERA_TABLETNODE_TABLET_MANAGER_H_
