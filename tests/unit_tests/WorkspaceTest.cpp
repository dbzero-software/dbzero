// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <mutex>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/workspace/WorkspaceView.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/PyLocks.hpp>

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
        
        void SetUp() override
        {
            if (!Py_IsInitialized()) {
                Py_InitializeEx(0);
            }
            // release GIL for the autocommit-thread (dead-locks otherwise)
            m_no_gil = std::make_unique<db0::python::WithGIL_Unlocked>();
            drop(file_name);
        }

        void TearDown() override
        {            
            m_workspace.close();
            m_no_gil = nullptr;
            drop(file_name);
        }
        
        PrefixName getPrefixName() const {
            return prefix_name;
        }

    protected:
        Workspace m_workspace;        
        std::unique_ptr<db0::python::WithGIL_Unlocked> m_no_gil;
    };
    
    TEST_F( WorkspaceTest , testWorkspaceCanCreateNewFixture )
    {
        auto fixture = m_workspace.getFixture(getPrefixName());
        ASSERT_NE(fixture, nullptr);
    }
    
    TEST_F( WorkspaceTest , testCanAccessLimitedStringPool )
    {
        auto fixture = m_workspace.getFixture(getPrefixName());
        auto ptr = fixture->getLimitedStringPool().addRef("test");
        ASSERT_NE(ptr.m_value, 0);
    }
    
    TEST_F( WorkspaceTest , testFixtureCanBeAccessedByUUID )
    {        
        auto fixture = m_workspace.getFixture(getPrefixName());
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
        Address address = {};
        // first transaction to create object
        {
            auto fixture = m_workspace.getFixture(getPrefixName());
            v_object<o_TT> obj(*fixture);
            address = obj.getAddress();
            fixture->commit();
        }

        // perform 10 object modifications in 10 transactions, take snapshot at 7th transaction
        db0::swine_ptr<Fixture> snap;
        std::shared_ptr<WorkspaceView> workspace_view;
        for (int i = 0; i < 10; ++i)
        {
            auto fixture = m_workspace.getFixture(getPrefixName());
            v_object<o_TT> obj(fixture->myPtr(address));
            obj.modify().a = i + 1;
            fixture->commit();
            
            if (i == 6) {
                // take snapshot
                workspace_view = m_workspace.getWorkspaceView(fixture->getStateNum());
                snap = fixture->getSnapshot(*workspace_view, {});
            }
        }
        
        // query the snapshot
        v_object<o_TT> obj(snap->myPtr(address));
        ASSERT_EQ(obj->a, 7);
    }
    
    TEST_F( WorkspaceTest , testFreeCanBePerformedBetweenTransactions )
    {        
        Address address = {};
        auto fixture = m_workspace.getFixture(getPrefixName());
        // first transaction to create object
        {
            v_object<o_TT> obj(*fixture);
            address = obj.getAddress();
            fixture->commit();
        }
        
        fixture->free(address);
        fixture->commit();
        ASSERT_ANY_THROW(fixture->getAllocator().getAllocSize(address));        
    }

    TEST_F( WorkspaceTest , testObjectInstanceCanBeModifiedBetweenTransactions )
    {        
        auto fixture = m_workspace.getFixture(getPrefixName());
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
        auto fixture = m_workspace.getFixture(getPrefixName());
        std::vector<Address> addresses;
        std::vector<std::size_t> allocs = {
            33, 28, 4
        };
        for (auto size: allocs) {
            addresses.push_back(fixture->alloc(size));
        }
        fixture->commit();
        fixture->alloc(8);
        ASSERT_NO_THROW(fixture->free(addresses.back()));
    }
    
    TEST_F( WorkspaceTest , testAllocFromTypeSlotThenFree )
    {        
        auto fixture = m_workspace.getFixture(getPrefixName());
        auto addr = fixture->alloc(100, Fixture::TYPE_SLOT_NUM);
        ASSERT_TRUE(addr);
        // get alloc size does not require providing slot number
        ASSERT_EQ(fixture->getAllocator().getAllocSize(addr), 100);

        // free the allocated address (no need to provide slot number here)
        fixture->free(addr);
        // must commit due to deferred free
        fixture->commit();
        // make sure the address is no longer valid
        ASSERT_ANY_THROW(fixture->getAllocator().getAllocSize(addr));
    }
    
    TEST_F( WorkspaceTest , testTimeTravelQueries )
    {        
        Address address = {};
        // First transaction to create a new object
        {
            auto memspace = m_workspace.getFixture(prefix_name);
            v_object<o_simple<int>> obj(*memspace, 999);
            address = obj.getAddress();            
            (*memspace).commit();
            m_workspace.close(getPrefixName());
        }
        
        // state_num + expected value
        std::vector<std::pair<std::uint64_t, int> > state_log;
        // perform 10 object modifications in 10 transactions
        for (int i = 0; i < 10; ++i) {
            auto memspace = m_workspace.getFixture(prefix_name);
            v_object<o_simple<int>> obj((*memspace).myPtr(address));
            obj.modify() = i + 1;
            state_log.emplace_back(memspace->getPrefix().getStateNum(), obj->value());
            (*memspace).commit();
            m_workspace.close(getPrefixName());
        }
        
        // now go back to specific transactions and validate object state
        for (auto &log: state_log) {
            auto memspace = m_workspace.getFixture(prefix_name, AccessType::READ_ONLY)->getSnapshot(m_workspace, log.first);
            v_object<o_simple<int>> obj((*memspace).myPtr(address));
            ASSERT_EQ(obj->value(), log.second);
            m_workspace.close(getPrefixName());
        }
    }
    
    TEST_F( WorkspaceTest , testTimeTravelWithPartialObjectModification )
    {                
        Address address = {};
        // first transaction to create object
        {
            auto memspace = m_workspace.getFixture(getPrefixName());
            v_object<o_TT> obj(*memspace);
            address = obj.getAddress();
            (*memspace).commit();
            m_workspace.close(getPrefixName());
        }
        
        // state_num + values
        std::vector<std::pair<std::uint64_t, std::pair<int, int> > > state_log;
        // perform 10 object modifications in 10 transactions
        for (int i = 0; i < 10; ++i) {
            auto memspace = m_workspace.getFixture(getPrefixName());
            v_object<o_TT> obj((*memspace).myPtr(address));
            // either modify a or b
            if (i % 2 == 0) {
                obj.modify().a = i + 1;
                state_log.emplace_back((*memspace).getPrefix().getStateNum(), std::pair<int, int>(i + 1, i));
            } else {
                obj.modify().b = i + 1;
                state_log.emplace_back((*memspace).getPrefix().getStateNum(), std::pair<int, int>(i, i + 1));
            }
            
            (*memspace).commit();
            m_workspace.close(getPrefixName());
        }
        
        // now, go back to specific transactions and validate object state
        // note that read-only mode is used to access transactions
        for (auto &log: state_log) {
            auto memspace = m_workspace.getFixture(getPrefixName(), AccessType::READ_ONLY)->getSnapshot(m_workspace, log.first);
            v_object<o_TT> obj((*memspace).myPtr(address));
            ASSERT_EQ(obj->a, log.second.first);
            ASSERT_EQ(obj->b, log.second.second);
            m_workspace.close(getPrefixName());
        }
    }
    
    // This test should be run using the massif tool to analyze memory usage
    TEST_F( WorkspaceTest , testMemoryUsageOverTime )
    {
        m_workspace.setCacheSize(1u << 20);
        auto fixture = m_workspace.getFixture(getPrefixName());
        for (int i = 0; i < 1000; ++i) {
            std::vector<db0::v_object<db0::o_binary> > objects;
            for (int j = 0; j < 250; ++j) {
                objects.emplace_back(*fixture, 1024);
            }
        }        
    }
    
    TEST_F( WorkspaceTest , testLockedSectionIDsAreReused )
    {        
        auto callback = [](const std::string &, std::uint64_t) {};
        auto id_0 = m_workspace.beginLocked();       
        auto id_1 = m_workspace.beginLocked();
        auto id_2 = m_workspace.beginLocked();
        
        // release in a different order
        m_workspace.endLocked(id_1, callback);
        m_workspace.endLocked(id_0, callback);
        m_workspace.endLocked(id_2, callback);

        // make sure the same IDs are reused in the same order
        ASSERT_EQ(m_workspace.beginLocked(), id_0);
        ASSERT_EQ(m_workspace.beginLocked(), id_1);
        ASSERT_EQ(m_workspace.beginLocked(), id_2);
    }

}
