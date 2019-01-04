#include <limits>
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "master/table_state_machine.h"

namespace tera {
namespace master {
namespace test {

class TableStateMachineTest : public ::testing::Test {
 public:
  TableStateMachineTest() : state_mchine_(kTableEnable) {}
  virtual ~TableStateMachineTest() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 private:
  bool TransitFromState(const TableStatus& status, const TableEvent& event) {
    state_mchine_.SetStatus(status);
    return state_mchine_.DoStateTransition(event);
  }
  TableStateMachine state_mchine_;
};

TEST_F(TableStateMachineTest, LegalTransition) {
  EXPECT_TRUE(TransitFromState(kTableEnable, TableEvent::kDisableTable));
  EXPECT_EQ(state_mchine_.GetStatus(), kTableDisable);
  EXPECT_TRUE(TransitFromState(kTableDisable, TableEvent::kEnableTable));
  EXPECT_EQ(state_mchine_.GetStatus(), kTableEnable);
  EXPECT_TRUE(TransitFromState(kTableDisable, TableEvent::kDeleteTable));
  EXPECT_EQ(state_mchine_.GetStatus(), kTableDeleting);
  EXPECT_TRUE(TransitFromState(kTableDeleting, TableEvent::kDisableTable));
  EXPECT_EQ(state_mchine_.GetStatus(), kTableDisable);
}

TEST_F(TableStateMachineTest, IllegalTransition) {
  EXPECT_FALSE(TransitFromState(kTableEnable, TableEvent::kDeleteTable));
  EXPECT_EQ(state_mchine_.GetStatus(), kTableEnable);
  EXPECT_FALSE(TransitFromState(kTableDeleting, TableEvent::kEnableTable));
  EXPECT_EQ(state_mchine_.GetStatus(), kTableDeleting);
  std::cout << TableEvent::kEnableTable << std::endl;
}
}
}
}
