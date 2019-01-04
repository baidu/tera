#include <limits>
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "master/tabletnode_manager.h"

namespace tera {
namespace master {
namespace test {

class TSStateTransitionTest : public ::testing::Test {
 public:
  TSStateTransitionTest() : node_(new TabletNode("127.0.0.1:2000", "1234567890")) {}
  virtual ~TSStateTransitionTest() {}

  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 private:
  bool TransitFromState(const NodeState& state, const NodeEvent& event) {
    node_->state_ = state;
    return node_->DoStateTransition(event);
  }

  TabletNodePtr node_;
};

TEST_F(TSStateTransitionTest, LegalTransition) {
  EXPECT_TRUE(TransitFromState(kOffline, NodeEvent::kZkNodeCreated));
  EXPECT_EQ(node_->state_, kReady);
  EXPECT_TRUE(TransitFromState(kReady, NodeEvent::kZkSessionTimeout));
  EXPECT_EQ(node_->state_, kOffline);
  EXPECT_TRUE(TransitFromState(kReady, NodeEvent::kPrepareKickTs));
  EXPECT_EQ(node_->state_, kWaitKick);
  EXPECT_TRUE(TransitFromState(kWaitKick, NodeEvent::kCancelKickTs));
  EXPECT_EQ(node_->state_, kReady);
  EXPECT_TRUE(TransitFromState(kWaitKick, NodeEvent::kZkKickNodeCreated));
  EXPECT_EQ(node_->state_, kKicked);
  EXPECT_TRUE(TransitFromState(kWaitKick, NodeEvent::kZkSessionTimeout));
  EXPECT_EQ(node_->state_, kOffline);
  EXPECT_TRUE(TransitFromState(kKicked, NodeEvent::kZkSessionTimeout));
  EXPECT_EQ(node_->state_, kOffline);
}

TEST_F(TSStateTransitionTest, IllegalTransition) {
  EXPECT_FALSE(TransitFromState(kOffline, NodeEvent::kZkSessionTimeout));
  EXPECT_EQ(node_->state_, kOffline);
  EXPECT_FALSE(TransitFromState(kReady, NodeEvent::kZkNodeCreated));
  EXPECT_EQ(node_->state_, kReady);
  EXPECT_FALSE(TransitFromState(kReady, NodeEvent::kCancelKickTs));
  EXPECT_EQ(node_->state_, kReady);
  EXPECT_FALSE(TransitFromState(kWaitKick, NodeEvent::kPrepareKickTs));
  EXPECT_EQ(node_->state_, kWaitKick);
}
}
}
}
