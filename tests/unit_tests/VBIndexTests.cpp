#include <gtest/gtest.h>
#include <cstdlib>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/memory/BitSpace.hpp>
#include <utils/WorkspaceTest.hpp>

using namespace std;

namespace tests

{

    class VBIndexTests: public WorkspaceBaseTest
    {
    };
    
    TEST_F( VBIndexTests , testVBIndexCanBeCreatedOnDefaultMemspace )
    {            
        auto memspace = getMemspace();

        db0::v_bindex<std::uint64_t> cut(memspace, memspace.getPageSize());
        ASSERT_TRUE(cut.getAddress() != 0);
    }

    TEST_F( VBIndexTests , testVBIndexCanInsertOnDefaultMemspace )
    {        
        auto memspace = getMemspace();

        db0::v_bindex<std::uint64_t> cut(memspace, memspace.getPageSize());
        cut.insert(1);
        ASSERT_EQ(cut.size(), 1);
    }
    
    TEST_F( VBIndexTests , testVBIndexCanBePersisted )
    {        
        auto memspace = getMemspace();
        std::uint64_t address = 0;
        {
            db0::v_bindex<std::uint64_t> cut(memspace, memspace.getPageSize());
            cut.insert(1);
            ASSERT_EQ(cut.size(), 1u);
            address = cut.getAddress();
        }
        
        db0::v_bindex<std::uint64_t> cut(memspace.myPtr(address), memspace.getPageSize());
        ASSERT_EQ(cut.size(), 1u);
    }

    TEST_F( VBIndexTests , testVBIndexCanBeReverseIterated )
    {
        auto memspace = getMemspace();
        std::vector<std::uint64_t> values = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        db0::v_bindex<std::uint64_t> cut(memspace, memspace.getPageSize());
        for (auto value: values) {
            cut.insert(value);
        }

        auto it = cut.end();
        if (it != cut.begin()) {
            --it;
        }

        std::vector<std::uint64_t> rvalues;
        while (it != cut.end()) {
            rvalues.push_back(*it);
            if (it == cut.begin()) {
                it = cut.end();
            } else {
                --it;        
            }
        }
        ASSERT_EQ(rvalues, (std::vector<std::uint64_t> { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 }));
    }

}