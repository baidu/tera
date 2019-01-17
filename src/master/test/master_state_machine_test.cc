#include <limits>
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "master/master_state_machine.h"

namespace tera {
namespace master {
namespace test {

class MasterStateMachineTest : public ::testing::Test {
 public:
  MasterStateMachineTest() : state_machine_(kIsSecondary) {}
  virtual ~MasterStateMachineTest() {}

  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 private:
  bool TransitFromState(const MasterStatus status, const MasterEvent event) {
    state_machine_.curr_status_ = status;
    return state_machine_.DoStateTransition(event);
  }

  MasterStateMachine state_machine_;
};

TEST_F(MasterStateMachineTest, LegalTransition) {
  EXPECT_TRUE(TransitFromState(kIsSecondary, MasterEvent::kGetMasterLock));
  EXPECT_EQ(state_machine_.GetState(), kOnRestore);
  EXPECT_TRUE(TransitFromState(kOnRestore, MasterEvent::kNoAvailTs));
  EXPECT_EQ(state_machine_.GetState(), kOnWait);
  EXPECT_TRUE(TransitFromState(kOnRestore, MasterEvent::kMetaRestored));
  EXPECT_EQ(state_machine_.GetState(), kIsReadonly);
  EXPECT_TRUE(TransitFromState(kOnRestore, MasterEvent::kLostMasterLock));
  EXPECT_EQ(state_machine_.GetState(), kIsSecondary);
  EXPECT_TRUE(TransitFromState(kOnWait, MasterEvent::kAvailTs));
  EXPECT_EQ(state_machine_.GetState(), kOnRestore);
  EXPECT_TRUE(TransitFromState(kOnWait, MasterEvent::kLostMasterLock));
  EXPECT_EQ(state_machine_.GetState(), kIsSecondary);
  EXPECT_TRUE(TransitFromState(kIsReadonly, MasterEvent::kLeaveSafemode));
  EXPECT_EQ(state_machine_.GetState(), kIsRunning);
  EXPECT_TRUE(TransitFromState(kIsReadonly, MasterEvent::kLostMasterLock));
  EXPECT_EQ(state_machine_.GetState(), kIsSecondary);
  EXPECT_TRUE(TransitFromState(kIsRunning, MasterEvent::kEnterSafemode));
  EXPECT_EQ(state_machine_.GetState(), kIsReadonly);
  EXPECT_TRUE(TransitFromState(kIsReadonly, MasterEvent::kLostMasterLock));
  EXPECT_EQ(state_machine_.GetState(), kIsSecondary);
}
}
}
}
