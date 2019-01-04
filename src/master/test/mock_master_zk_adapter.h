#include "master/master_zk_adapter.h"

namespace tera {
namespace master {
namespace test {

class TestZkAdapter : public MasterZkAdapter {
 public:
  TestZkAdapter() : MasterZkAdapter(nullptr, std::string("")) {}
  virtual bool UpdateRootTabletNode(const std::string& addr) { return true; }

  bool MarkSafeMode() { return true; }
};
}
}
}
