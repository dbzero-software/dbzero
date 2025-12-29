// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <mutex>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class BaseWorkspaceTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        void SetUp() override {
            drop(file_name);
        }

        virtual void TearDown() override
        {            
            m_workspace.close();
            drop(file_name);
        }

    protected:
        BaseWorkspace m_workspace;        
    };

    TEST_F( BaseWorkspaceTest , testBaseWorkspaceCanCreateMemspace )
    {        
        auto memspace = m_workspace.getMemspace(prefix_name);
        ASSERT_TRUE(file_exists(file_name));        
    }

    TEST_F( BaseWorkspaceTest , testBaseWorkspaceCanHostVObjects )
    {        
        auto memspace = m_workspace.getMemspace(prefix_name);
        v_object<o_simple<int>> obj(memspace, 1);
        ASSERT_TRUE(obj.getAddress().getOffset() > 0);
    }

    TEST_F( BaseWorkspaceTest , testBaseWorkspaceCanPersistVObjects )
    {                
        Address address = {};
        {
            auto memspace = m_workspace.getMemspace(prefix_name);
            v_object<o_simple<int>> obj(memspace, 999);
            address = obj.getAddress();
            // data should be persisted here
            memspace.commit();
            m_workspace.close(prefix_name);
        }
        
        auto memspace = m_workspace.getMemspace(prefix_name);
        // access persisted object
        v_object<o_simple<int>> obj(memspace.myPtr(address));
        ASSERT_EQ(obj->value(), 999);
    }

    TEST_F( BaseWorkspaceTest , testBaseWorkspaceCanStoreMultipleTransactions )
    {                
        std::set<Address> addresses;
        // perform 2 transactions
        for (int i = 0; i < 2; ++i)
        { 
            auto memspace = m_workspace.getMemspace(prefix_name);
            v_object<o_simple<int>> obj(memspace, 999);
            addresses.insert(obj.getAddress());
            // data should be persisted here
            memspace.commit();
            m_workspace.close(prefix_name);
        }
        
        ASSERT_EQ(addresses.size(), 2);
        auto memspace = m_workspace.getMemspace(prefix_name);
        // validate persisted objects
        for (auto address : addresses)
        {
            v_object<o_simple<int>> obj(memspace.myPtr(address));
            ASSERT_EQ(obj->value(), 999);
        }
    }
    
    TEST_F( BaseWorkspaceTest , testSparseIndexCanReuseExpiredDataBlocks )
    {        
        std::set<Address> addresses;
        // perform 100 small transactions with disk commit of each                
        for (int i = 0; i < 100; ++i) {
            auto memspace = m_workspace.getMemspace(prefix_name);
            v_object<o_simple<int>> obj(memspace, 999);
            memspace.commit();
            m_workspace.close(prefix_name);
        }
        
        // open .db0 file directly
        // need to open as read/write to be able to estimate allocated size
        auto file_name = m_workspace.getPrefixCatalog().getFileName(prefix_name).string();
        BDevStorage storage(file_name, AccessType::READ_WRITE);
        // make sure the DramIO (sparse index + diff index storage) streams have allocated < 4 blocks
        auto &io = storage.getDramIO();
        ASSERT_LE((int)(io.getAllocatedSize() / io.getBlockSize()), 4);        
        storage.close();
    }
    
    TEST_F( BaseWorkspaceTest , testDB0FileCanBeOpenedInReadOnlyMode )
    {        
        std::set<Address> addresses;
        // perform a few small transactions with disk commit of each
        for (int i = 0; i < 10; ++i)
        {
            auto memspace = m_workspace.getMemspace(prefix_name);
            v_object<o_simple<int>> obj(memspace, 999);
            memspace.commit();
            m_workspace.close(prefix_name);
        }
        
        // open .db0 file directly as read-only
        auto file_name = m_workspace.getPrefixCatalog().getFileName(prefix_name).string();
        BDevStorage storage(file_name, AccessType::READ_ONLY);
    }
    
    struct [[gnu::packed]] o_TT: public o_fixed<o_TT> {
        int a = 0;
        int b = 0;
    };
    
    TEST_F( BaseWorkspaceTest , testBaseWorkspaceCanPersistVBVector )
    {        
        Address address = {};
        {
            auto memspace = m_workspace.getMemspace(prefix_name);
            db0::v_bvector<int> data_buf(memspace);
            const int item_count = 1;
            for (int index = 0;(index < item_count);++index) {
                data_buf.emplace_back(index);
            }
            
            address = data_buf.getAddress();
            // data should be persisted here
            memspace.commit();
            m_workspace.close(prefix_name);
        }
        
        auto memspace = m_workspace.getMemspace(prefix_name);
        // access & append persisted v_bvector instance
        db0::v_bvector<int> data_buf(memspace.myPtr(address));
        data_buf.push_back(100);
        ASSERT_EQ(data_buf.size(), 2);
        memspace.commit();
        m_workspace.close(prefix_name);
    }
    
    TEST_F( BaseWorkspaceTest , testBaseWorkspaceCanPersistDeallocation )
    {        
        Address address = {};
        std::size_t alloc_size = 0;

        // First transaction to create a new instance
        {
            auto memspace = m_workspace.getMemspace(prefix_name);
            v_object<o_TT> obj(memspace);
            address = obj.getAddress();
            alloc_size = memspace.getAllocator().getAllocSize(address);
            
            // data should be persisted here
            memspace.commit();
            m_workspace.close(prefix_name);
        }
        
        // Second transaction to destroy the instance
        {
            auto memspace = m_workspace.getMemspace(prefix_name);
            ASSERT_EQ(memspace.getAllocator().getAllocSize(address), alloc_size);
            v_object<o_TT> obj(memspace.myPtr(address));
            obj.destroy();
            memspace.commit();
            m_workspace.close(prefix_name);
        }
        
        auto memspace = m_workspace.getMemspace(prefix_name);
        // validate if address has been released
        ASSERT_ANY_THROW(memspace.getAllocator().getAllocSize(address));
        m_workspace.close(prefix_name);
    }
    
}
