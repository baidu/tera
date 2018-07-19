#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "master/procedure_executor.h"

namespace tera {
namespace master {
namespace test {

class ProcedureExecutorTest : public ::testing::Test {
public:
    ProcedureExecutorTest() : executor_(new ProcedureExecutor){}
    virtual ~ProcedureExecutorTest() {}

    virtual void SetUp() {}
    virtual void TearDown() {}

    static void SetUpTestCase() {}
    static void TearDownTestCase() {}

private:
    std::shared_ptr<ProcedureExecutor> executor_;
};

class TestProcedure : public Procedure {
public:
    TestProcedure(std::string id) : id_(id), done_(false) {}
    virtual ~TestProcedure() {}
    std::string ProcId() const {return id_;}
    void RunNextStage() {
        std::cout << "id: " << id_ << std::endl;
        usleep(1000);
        done_ = true;
    }
    bool Done() {return done_;}
    
    std::string id_;
    bool done_;
};

TEST_F(ProcedureExecutorTest, StartStopProcedureExecutor) {
    EXPECT_FALSE(executor_->running_);
    EXPECT_TRUE(executor_->Start());
    EXPECT_TRUE(executor_->running_);
    EXPECT_FALSE(executor_->Start());
    EXPECT_TRUE(executor_->running_);
    executor_->Stop();
    EXPECT_FALSE(executor_->running_);
    executor_->Stop();
}

TEST_F(ProcedureExecutorTest, AddRemoveProcedures) {
    std::shared_ptr<Procedure> proc(new TestProcedure("TestProcedure1"));
    EXPECT_EQ(executor_->AddProcedure(proc), 0);
    // pretend Procedureexecutor_->is running by setting memeber field running_ to true
    executor_->running_ = true;
    EXPECT_GE(executor_->AddProcedure(proc), 0);
    EXPECT_EQ(executor_->procedures_.size(), 1);
    // add again, wil return 0
    EXPECT_EQ(executor_->AddProcedure(proc), 0);
    EXPECT_EQ(executor_->procedures_.size(), 1);
    EXPECT_TRUE(executor_->RemoveProcedure(proc->ProcId()));
    EXPECT_EQ(executor_->procedures_.size(), 0);
    EXPECT_FALSE(executor_->RemoveProcedure(proc->ProcId()));
    EXPECT_EQ(executor_->procedures_.size(), 0);
    executor_->running_ = false;
}

TEST_F(ProcedureExecutorTest, ScheduleProcedures) {
    std::shared_ptr<Procedure> proc1(new TestProcedure("TestProcedure1"));
    executor_->Start();
    EXPECT_TRUE(executor_->running_);
    EXPECT_FALSE(proc1->Done());
    executor_->AddProcedure(proc1);
    EXPECT_EQ(executor_->procedures_.size(), 1);
    usleep(50 * 1000);
    EXPECT_TRUE(proc1->Done());
    EXPECT_TRUE(executor_->procedures_.empty());
}

}
}
}
