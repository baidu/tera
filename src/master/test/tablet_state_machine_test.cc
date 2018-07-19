#include <limits>
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "master/tablet_state_machine.h"
#include "common/timer.h"
namespace tera {
namespace master {
namespace test {

class TabletStateMachineTest : public ::testing::Test {
public:
    TabletStateMachineTest() : state_machine_(TabletMeta::kTabletOffline) {}
    virtual ~TabletStateMachineTest() {}
    virtual void SetUp() {}
    virtual void TearDown() {}
    
    static void SetUpTestCase() {}
    static void TearDownTestCase() {}

private: 
    bool TransitFromState(const TabletMeta::TabletStatus& status, const TabletEvent& event) {
        state_machine_.SetStatus(status);
        return state_machine_.DoStateTransition(event);
    }
    TabletStateMachine state_machine_;
};

TEST_F(TabletStateMachineTest, LegalTransition) {
    // state transitioin from kTableOffLine
    state_machine_.SetStatus(TabletMeta::kTabletOffline);
    EXPECT_EQ(state_machine_.ReadyTime(), std::numeric_limits<int64_t>::max());
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletOffline, TabletEvent::kLoadTablet));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletLoading);
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletOffline, TabletEvent::kTsDelayOffline));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletDelayOffline);
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletOffline, TabletEvent::kTableDisable));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletDisable);
    // state transition from kTabletPending
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletDelayOffline, TabletEvent::kTsOffline));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletDelayOffline, TabletEvent::kTsRestart));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
    // state transtion from kTableOnLoad
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletLoading, TabletEvent::kTsLoadSucc));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletReady);
    EXPECT_NE(state_machine_.ReadyTime(), std::numeric_limits<int64_t>::max());
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletLoading, TabletEvent::kTsLoadFail));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletLoadFail);
    EXPECT_EQ(state_machine_.ReadyTime(), std::numeric_limits<int64_t>::max());
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletOffline, TabletEvent::kTabletLoadFail));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletLoadFail);
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletLoading, TabletEvent::kTsDelayOffline));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletDelayOffline);
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletLoading, TabletEvent::kTsOffline));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletLoading, TabletEvent::kTsRestart));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
    // state transition from kTableUnLoading
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletUnloading, TabletEvent::kTsUnLoadSucc));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletUnloading, TabletEvent::kTsUnLoadFail));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletUnloadFail);
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletUnloading, TabletEvent::kTsOffline));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
    // state transtion from kTableUnLoadFail
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletUnloadFail, TabletEvent::kTsOffline));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
    // state transtion from kTableReady
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletReady, TabletEvent::kUnLoadTablet));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletUnloading);
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletReady, TabletEvent::kTsOffline));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
    // state transition from kTabletDisable
    EXPECT_TRUE(TransitFromState(TabletMeta::kTabletDisable, TabletEvent::kTableEnable));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
}

TEST_F(TabletStateMachineTest, IllegalTransition) {
    state_machine_.SetStatus(TabletMeta::kTabletOffline);
    EXPECT_FALSE(state_machine_.DoStateTransition(TabletEvent::kTsLoadSucc));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletOffline);
    
    state_machine_.SetStatus(TabletMeta::kTabletReady);
    int64_t ready_time = state_machine_.ReadyTime();
    EXPECT_LE(get_micros() - ready_time, 1);
    EXPECT_FALSE(state_machine_.DoStateTransition(TabletEvent::kTableDisable));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletReady);
    EXPECT_EQ(ready_time, state_machine_.ReadyTime());

    EXPECT_FALSE(TransitFromState(TabletMeta::kTabletLoading, TabletEvent::kTsUnLoadSucc));
    EXPECT_EQ(state_machine_.GetStatus(), TabletMeta::kTabletLoading);

    std::cout << TabletEvent::kLoadTablet << std::endl;
}


}
}
}
