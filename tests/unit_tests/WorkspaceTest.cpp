#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/WorkspaceView.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class WorkspaceTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";
        
        virtual void SetUp() override {
            drop(file_name);
        }

        virtual void TearDown() override {
            m_workspace.close();
            drop(file_name);
        }

    protected:
        Workspace m_workspace;
    };
    
    TEST_F( WorkspaceTest , testWorkspaceCanCreateNewFixture )
    {
        auto fixture = m_workspace.getFixture(prefix_name);
        ASSERT_NE(fixture, nullptr);
    }
    
    TEST_F( WorkspaceTest , testCanAccessLimitedStringPool )
    {
        auto fixture = m_workspace.getFixture(prefix_name);
        auto ptr = fixture->getLimitedStringPool().add("test");
        ASSERT_NE(ptr.m_value, 0);
    }
    
    TEST_F( WorkspaceTest , testFixtureCanBeAccessedByUUID )
    {        
        auto fixture = m_workspace.getFixture(prefix_name);
        auto fx2 = m_workspace.getFixture(fixture->getUUID());

        ASSERT_EQ(fixture, fx2);
    }
    
    struct [[gnu::packed]] o_TT: public o_fixed<o_TT> 
    {
        int a = 0;
        int b = 0;
        void testMethod() const {};
    };
    
    TEST_F( WorkspaceTest , testFixtureSnapshotCanBeTaken )
    {        
        std::uint64_t address = 0;
        // first transaction to create object
        {
            auto fixture = m_workspace.getFixture(prefix_name);
            v_object<o_TT> obj(*fixture);
            address = obj.getAddress();
            fixture->commit();            
        }

        // perform 10 object modifications in 10 transactions, take snapshot at 7th transaction
        db0::swine_ptr<Fixture> snap;
        std::unique_ptr<WorkspaceView> workspace_view;
        for (int i = 0; i < 10; ++i)
        {
            auto fixture = m_workspace.getFixture(prefix_name);
            v_object<o_TT> obj(fixture->myPtr(address));
            obj.modify().a = i + 1;

            if (i == 6) {
                // take snapshot
                workspace_view = std::make_unique<WorkspaceView>(m_workspace, fixture->getStateNum());
                snap = fixture->getSnapshot(*workspace_view, {});
            }

            fixture->commit();
        }
        
        // query the snapshot
        v_object<o_TT> obj(snap->myPtr(address));
        ASSERT_EQ(obj->a, 7);
    }

    TEST_F( WorkspaceTest , testFreeCanBePerformedBetweenTransactions )
    {
        std::uint64_t address = 0;
        auto fixture = m_workspace.getFixture(prefix_name);
        // first transaction to create object
        {
            v_object<o_TT> obj(*fixture);
            address = obj.getAddress();
            fixture->commit();            
        }
        
        fixture->getAllocator().free(address);
        fixture->commit();                
        ASSERT_ANY_THROW(fixture->getAllocator().getAllocSize(address));        
    }

    TEST_F( WorkspaceTest , testObjectInstanceCanBeModifiedBetweenTransactions )
    {
        auto fixture = m_workspace.getFixture(prefix_name);
        // create object and keep instance across multiple transactions
        v_object<o_TT> obj(*fixture);
        fixture->commit();
        ASSERT_EQ(obj->a, 0);
        obj.modify().a = 1;
        fixture->commit();
        ASSERT_EQ(obj->a, 1);
    }

    TEST_F( WorkspaceTest , testAllocFreeBetweenTransactionsIssue )
    {
        auto fixture = m_workspace.getFixture(prefix_name);
        std::vector<std::uint64_t> addresses;
        std::vector<std::size_t> allocs = {
            33, 28, 4
        };
        for (auto size: allocs) {
            addresses.push_back(fixture->getAllocator().alloc(size));
        }
        fixture->commit();
        fixture->getAllocator().alloc(8);
        ASSERT_NO_THROW(fixture->getAllocator().free(addresses.back()));
    }
    
    TEST_F( WorkspaceTest , testAllocFromTypeSlotThenFree )
    {
        auto fixture = m_workspace.getFixture(prefix_name);
        auto addr = fixture->getAllocator().alloc(100, Fixture::TYPE_SLOT_NUM);
        ASSERT_TRUE(addr);
        // get alloc size does not require providing slot number
        ASSERT_EQ(fixture->getAllocator().getAllocSize(addr), 100);

        // free the allocated address (no need to provide slot number here)
        fixture->getAllocator().free(addr);
        // make sure the address is no longer valid
        ASSERT_ANY_THROW(fixture->getAllocator().getAllocSize(addr));
    }

}
