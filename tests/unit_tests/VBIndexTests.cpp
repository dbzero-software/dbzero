#include <gtest/gtest.h>
#include <cstdlib>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/memory/BitSpace.hpp>
#include <dbzero/core/collections/full_text/key_value.hpp>
#include <dbzero/object_model/dict/Dict.hpp>

using namespace std;

namespace tests

{

    using Address = db0::Address;
    
    class VBIndexTests: public MemspaceTestBase
    {
    };
    
    TEST_F( VBIndexTests , testVBIndexCanBeCreatedOnDefaultMemspace )
    {            
        auto memspace = getMemspace();

        db0::v_bindex<std::uint64_t> cut(memspace, memspace.getPageSize());
        ASSERT_TRUE(cut.getAddress().isValid());
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
        Address address = {};
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

    TEST_F( VBIndexTests , testVBIndexBulkInsertUpdate )
    {
        using ItemT = db0::key_value<std::uint32_t, std::uint32_t>;
        auto memspace = getMemspace();
        std::vector<ItemT> values = { { 1, 0} , { 2, 0 }, { 3, 0 }, { 4, 0 }, { 5, 0 } };
        db0::v_bindex<ItemT> cut(memspace, memspace.getPageSize());
        for (auto value: values) {
            cut.insert(value);
        }

        std::vector<ItemT> values_2 = { { 1, 1} , { 2, 2 }, { 3, 1 }, { 6, 0 } };
        cut.bulkUpdate(values_2.begin(), values_2.end());

        auto it = cut.end();
        if (it != cut.begin()) {
            --it;
        }

        std::vector<ItemT> rvalues;
        while (it != cut.end()) {
            rvalues.push_back(*it);
            if (it == cut.begin()) {
                it = cut.end();
            } else {
                --it;        
            }
        }
        
        ASSERT_EQ(rvalues, (std::vector<ItemT> {{ 6, 0 }, { 5, 0 }, { 4, 0 }, { 3, 1 }, { 2, 2 }, { 1, 1 }}));
    }
    
    TEST_F( VBIndexTests , testVBIndexCopyConstructor )
    {
        auto memspace = getMemspace();
        std::vector<std::uint64_t> values = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        db0::v_bindex<std::uint64_t> cut(memspace, memspace.getPageSize());
        for (auto value: values) {
            cut.insert(value);
        }

        db0::v_bindex<std::uint64_t> copy(memspace, cut);
        auto it1 = cut.begin(), end1 = cut.end();
        auto it2 = copy.begin(), end2 = copy.end();
        while (it1 != end1) {
            ASSERT_TRUE(it2 != end2);
            ASSERT_EQ(*it1, *it2);
            ++it1;
            ++it2;
        }
    }
    
}

