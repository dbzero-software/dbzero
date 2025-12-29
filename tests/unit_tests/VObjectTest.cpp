// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/vspace/v_object.hpp>

namespace tests

{

	using namespace db0;
    
    struct [[gnu::packed]] o_test_data: db0::o_fixed<o_test_data>
    {
        std::uint32_t m_value;
        o_test_data(std::uint32_t value)
            : m_value(value)
        {
        }
    };

    class VObjectTest: public MemspaceTestBase
    {
    };

	TEST_F( VObjectTest , testVObjectCanBeCreatedOnMemspace )
	{
        auto memspace = getMemspace();
        v_object<o_test_data> cut(memspace, 123);
        ASSERT_TRUE(cut.getAddress().isValid());
	}
    
	TEST_F( VObjectTest , testVObjectCanBePersistedAndRetrievedFromMemspace )
	{
        auto memspace = getMemspace();
        Address address = {};
        {
            v_object<o_test_data> cut(memspace, 123);
            address = cut.getAddress();
        }

        v_object<o_test_data> cut(memspace.myPtr(address));
        ASSERT_EQ(cut->m_value, 123);
	}
    
	TEST_F( VObjectTest , testVObjectCanBeModified )
	{
        auto memspace = getMemspace();
        Address address = {};
        {
            v_object<o_test_data> cut(memspace, 123);
            cut.modify().m_value = 999;
            address = cut.getAddress();
        }

        v_object<o_test_data> cut(memspace.myPtr(address));
        ASSERT_EQ(cut->m_value, 999);
	}

	TEST_F( VObjectTest , testVObjectSpan )
	{
        auto memspace = getMemspace();
        v_object<o_binary> cut_1(memspace, 16);
        // small object should have span = 1 (i.e. spanning 1 DP)
        ASSERT_EQ(cut_1.span(), 1);
        v_object<o_binary> cut_2(memspace, 4120);
        // large object will span more tha 1DP
        ASSERT_EQ(cut_2.span(), 2);
	}

}