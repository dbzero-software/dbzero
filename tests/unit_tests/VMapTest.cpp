// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/collections/map/v_map.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>

namespace tests

{

	using namespace db0;

    class VMapTest: public MemspaceTestBase
    {
    };
	
	TEST_F( VMapTest , testDuplicateKeysAreRetainedIfInsertEqualUsed )
	{
        auto memspace = getMemspace();
		
		v_map<o_simple<int>, o_string> v_map(memspace);
		v_map.insert_equal(1, "one");
		v_map.insert_equal(1, "two");
		v_map.insert_equal(1, "three");
		ASSERT_EQ(v_map.size(), 3u);
	}

	TEST_F( VMapTest , testKeysAndCorrespondingValuesArePersisted )
	{
        auto memspace = getMemspace();

		v_map<o_simple<int>, o_string> v_map(memspace);
		v_map.insert_equal(5, "five");
		v_map.insert_equal(2, "two");
		v_map.insert_equal(4, "four");
		ASSERT_EQ(v_map.find(2)->second().toString(), "two");
		ASSERT_EQ(v_map.find(4)->second().toString(), "four");
		ASSERT_EQ(v_map.find(5)->second().toString(), "five");
	}

	TEST_F( VMapTest , testItemsInVMapAreOrganizedAscending )
	{
        auto memspace = getMemspace();
		
		v_map<o_simple<int>, o_string> v_map(memspace);
		v_map.insert_equal(5, "one");
		v_map.insert_equal(2, "some text");
		v_map.insert_equal(4, "some other text");
		ASSERT_EQ(3u, v_map.size() );
		int last = 0;
		auto it = v_map.begin();
		while (it!=v_map.end()) {
			int current = it->first();
			ASSERT_EQ(current >= last, true);
			++it;
		}
	}

	TEST_F( VMapTest , testItemCanBeFoundByKeyInitializer )
	{
        auto memspace = getMemspace();
		
		v_map<o_string, o_simple<int>, o_string::comp_t> v_map(memspace);
		v_map.insert_equal("one", 1);
		v_map.insert_equal("two", 2);
		v_map.insert_equal("eighteen", 18);
		auto it1 = v_map.find("two");
		ASSERT_FALSE( it1 == v_map.end() );
		auto it2 = v_map.find("eighteen");
		ASSERT_FALSE( it2 == v_map.end() );
		auto it3 = v_map.find("three");
		// item not found
		ASSERT_TRUE( it3 == v_map.end() );
	}

	TEST_F( VMapTest , testDuplicateKeysAreNotAddedWhenInsertUniqueUsed )
	{
        auto memspace = getMemspace();
		
		v_map<o_string, o_simple<int>, o_string::comp_t> v_map(memspace);
		v_map.insert_unique("one", 1);
		v_map.insert_unique("three", 2);
		v_map.insert_unique("two", 12);
		v_map.insert_unique("one", 8);
		v_map.insert_unique("two", 2);
		ASSERT_EQ(v_map.size(), 3u);
		std::stringstream _str;
		auto it = v_map.begin();
		while (it!=v_map.end())
		{
			_str << it->first().toString() << "=" << it->second().toString() << ",";
			++it;
		}
		ASSERT_EQ(std::string("one=1,three=2,two=12,"), _str.str());
	}

	TEST_F( VMapTest , testVMapInsertSpeed )
    {
        auto memspace = getMemspace();
		
		// collect random tokens first
		int node_count = 1000;
		std::set<std::string> sorted_tokens;
		std::list<std::string> tokens;
		for (int i=0; i < node_count;++i) {
			std::string str_token_key = db0::tests::randomToken(16, 42);
			tokens.push_back(str_token_key);
			sorted_tokens.insert(str_token_key);
		}

		// create new v_map
		using map_t = db0::v_map<o_string, o_simple<int>, o_string::comp_t>;
		map_t _map(memspace);
		// insert random tokens
		{
			// measure speed
        	auto start = std::chrono::high_resolution_clock::now();			
			int count = 0;
			auto it = sorted_tokens.begin();
			while (it != sorted_tokens.end()) {
				_map.insert_equal(it->c_str(), 0);
				++it;
				++count;
			}
			auto end = std::chrono::high_resolution_clock::now();
			auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
			std::cout << "VMap insert_equal speed: " << elapsed.count() << "ms" << std::endl;
			std::cout << "items / sec : " << (count * 1000.0) / elapsed.count() << std::endl;
		}

		// iterate whole collection
		auto it = _map.begin();
		while (it!=_map.end()) {
			++it;
		}
	}
	
	TEST_F( VMapTest , testVMapCanBePersisted )
	{
        auto memspace = getMemspace();
		Address address = {};

		{
			v_map<o_simple<int>, o_string> v_map(memspace);
			v_map.insert_equal(1, "one");
			v_map.insert_equal(1, "two");
			v_map.insert_equal(1, "three");
			address = v_map.getAddress();	
		}

		v_map<o_simple<int>, o_string> cut(memspace.myPtr(address));
		ASSERT_EQ(cut.size(), 3u);
	}

	TEST_F( VMapTest , testVMapIteratorCanBeConveredToAddress )
	{
        auto memspace = getMemspace();	
		
		v_map<o_simple<int>, o_string> v_map(memspace);
		v_map.insert_equal(1, "one");
		auto it = v_map.begin();
		auto addr = it.getAddress();
		ASSERT_TRUE(addr.isValid());
	}
	
	TEST_F( VMapTest , testVMapIteratorCanBeConstructedFromAddress )
	{
        auto memspace = getMemspace();		
		
		v_map<o_simple<int>, o_string> v_map(memspace);
		v_map.insert_equal(1, "one");
		auto it = v_map.begin();
		auto addr = it.getAddress();
		auto it_from_addr = v_map.beginFromAddress(addr);
		ASSERT_EQ(it, it_from_addr);
	}

	TEST_F( VMapTest , testVMapIteratorsRemainValidAfterUpdates )
	{
        auto memspace = getMemspace();

		v_map<o_simple<int>, o_string> v_map(memspace);
		v_map.insert_equal(2, "two");
		auto addr = v_map.begin().getAddress();
		v_map.insert_equal(1, "one");
		v_map.insert_equal(3, "three");
		auto it = v_map.beginFromAddress(addr);
		ASSERT_EQ(it->first(), 2);
		++it;
		ASSERT_EQ(it->first(), 3);
	}
	
}
