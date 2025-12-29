// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/vector/v_sorted_vector.hpp>
#include <dbzero/core/collections/full_text/FT_IndexIterator.hpp>

namespace tests 

{

    using namespace db0;

    class VSortedVectorTest: public MemspaceTestBase
    {
    public:
    };

    class VSortedVectorFixtureTest: public FixtureTestBase
    {
    public:
    };

	static void eraseVector(std::vector<int>& from, const std::vector<int>& to_erase)
	{
		for (int erased : to_erase) {
			auto it = std::find(from.begin(), from.end(), erased);
			if(it != from.end()) {
				from.erase(it);
			}
		}
	}

	TEST_F( VSortedVectorFixtureTest , testVSortedVectorBulkInsertReverseSorted )
    {
		auto fixture = m_workspace.getFixture("test-fixture");
		v_sorted_vector<int> data_buf(*fixture, 8);
		int insert_count = 100;
		while (insert_count-- > 0) {
			int count = 25;
			std::vector<int> data;
			while (count-- > 0) {
				data.push_back(rand() % 100);
			}
			// sort descending
			std::sort(data.begin(), data.end(), [](int i0, int i1) {
				return (i0 > i1);
			});
			data_buf.bulkInsertReverseSorted(data.begin(), data.size());
		}
		ASSERT_EQ(data_buf->m_size, 2500u);
		auto it0 = data_buf->begin();
		auto it1 = data_buf->begin();
		++it1;
		std::stringstream _str0;
		std::vector<int> erase_buf;
		while (it1!=data_buf->end()) {
			if (rand()%3==0) {
				erase_buf.push_back(*it0);
			}
			_str0 << *it0 << ",";
			ASSERT_EQ((*it0 <= *it1), true);
			++it0;
			++it1;
		}
		std::string str_dump = _str0.str();

		// test bulk erase sorted
		bool addr_changed = false;
		data_buf.bulkEraseSorted(erase_buf.begin(), erase_buf.end(), addr_changed);
		it0 = data_buf->begin();
		it1 = data_buf->begin();
		++it1;
		std::stringstream _str1;
		while (it1!=data_buf->end()) {
			_str1 << *it0 << ",";
			ASSERT_EQ((*it0 <= *it1), true);
			++it0;
			++it1;
		}
		str_dump = _str1.str();
	}

	TEST_F( VSortedVectorTest , testVSortedVectorJoin)
    {
		auto memspace = getMemspace();

		v_sorted_vector<int> data_buf(memspace, 8);
		int data[] = { 0, 1, 2, 3, 9, 26, 4, 19, 33, 912, 8, 55, 65, 99 };
		for (auto  i=0u; (i < sizeof(data)/sizeof(int)) ; ++i) {
			bool addr_changed = false;
			data_buf.insert(data[i],addr_changed);
		}
		// dump
		{
			std::stringstream _str;
			auto it = data_buf->begin(), end = data_buf->end();
			while (it!=end) {
				_str << *it;
				++it;
				if (it!=data_buf->end()) {
					_str << ",";
				}
			}
			std::string str_dump = _str.str();
			ASSERT_EQ(str_dump, "0,1,2,3,4,8,9,19,26,33,55,65,99,912");
		}
		// test join forward operator
		{
			auto it = data_buf->beginJoin(1);
			it.join(5, 1);
			ASSERT_EQ(*it,8);
			it.join(9, 1);
			ASSERT_EQ(*it,9);
			it.join(21, 1);
			ASSERT_EQ(*it,26);
			it.join(22, 1);
			ASSERT_EQ(*it,26);
			it.join(254, 1);
			ASSERT_EQ(*it,912);
			it.join(911, 1);
			ASSERT_EQ(*it,912);
			it.join(912, 1);
			ASSERT_EQ(*it,912);
			bool result = it.join(913, 1);
			ASSERT_EQ(result, false);
		}
		// test join backward operator
		{
			auto it = data_buf->beginJoin(-1);
			it.join(34, -1);
			ASSERT_EQ(*it,33);
			it.join(12, -1);
			ASSERT_EQ(*it,9);
			it.join(11, -1);
			ASSERT_EQ(*it,9);
			it.join(9, -1);
			ASSERT_EQ(*it,9);
			it.join(2, -1);
			ASSERT_EQ(*it,2);
			bool result = it.join(-1, -1);
			ASSERT_EQ(result, false);
		}
	}
	
	TEST_F( VSortedVectorTest , testVSortedVectorBulkErase) 
    {
		auto memspace = getMemspace();

		v_sorted_vector<int> data_buf(memspace, 8);
		std::vector<int> data{34, 63, 39, 53, 13, 25, 73, 1, 6, 61, 63, 55, 99, 8};
		data_buf.bulkInsert(data);
		std::sort(data.begin(), data.end());
		{
			std::vector<int> tested(data_buf.begin(), data_buf.end());
			ASSERT_EQ(tested, data);
		}
		{
			std::vector<int> erase{34, 39, 143, 13, -112, 73, 6, 55, -123, 8};
			ASSERT_EQ(data_buf.bulkErase(erase.begin(), erase.end()), 7);
			ASSERT_EQ(data_buf.size(), 7);
			eraseVector(data, erase);
			std::vector<int> tested(data_buf.begin(), data_buf.end());
			ASSERT_EQ(tested, data);
		}
		{
			std::vector<int> erase{-1, 63, 50,  53, 25, 13, 13, 1, 61, 8, 99, 1000, 63};
			ASSERT_EQ(data_buf.bulkErase(erase.begin(), erase.end()), 7);
			ASSERT_EQ(data_buf.size(), 0);
			eraseVector(data, erase);
			std::vector<int> tested(data_buf.begin(), data_buf.end());
			ASSERT_EQ(tested, data);
		}
		{
			std::vector<int> erase{-1, -100, 1, 4, 7, 9, 100, 54234, 511};
			ASSERT_EQ(data_buf.bulkErase(erase.begin(), erase.end()), 0);
			ASSERT_EQ(data_buf.size(), 0);
		}
	}

	TEST_F( VSortedVectorTest , testVSortedVectorBulkEraseSorted) 
    {
		auto memspace = getMemspace();

		v_sorted_vector<int> data_buf(memspace, 8);
		std::vector<int> data{3, 6, 7, 14, 35, 35, 55, 61, 62, 63, 63, 64, 67, 71, 80};
		data_buf.bulkInsert(data);
		{
			std::vector<int> tested(data_buf.begin(), data_buf.end());
			ASSERT_EQ(tested, data);
		}
		{
			std::vector<int> erase{1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15};
			bool dummy;
			ASSERT_EQ(data_buf.bulkEraseSorted(erase.begin(), erase.end(), dummy), 4);
			ASSERT_EQ(data_buf.size(), 11);
			eraseVector(data, erase);
			std::vector<int> tested(data_buf.begin(), data_buf.end());
			ASSERT_EQ(tested, data);
		}
		{
			std::vector<int> erase{35, 55, 60, 61, 62, 63, 64, 65};
			bool dummy;
			ASSERT_EQ(data_buf.bulkEraseSorted(erase.begin(), erase.end(), dummy), 6);
			ASSERT_EQ(data_buf.size(), 5);
			eraseVector(data, erase);
			std::vector<int> tested(data_buf.begin(), data_buf.end());
			ASSERT_EQ(tested, data);
		}
		{
			std::vector<int> erase{-100, 1, 53, 71, 165};
			bool dummy;
			ASSERT_EQ(data_buf.bulkEraseSorted(erase.begin(), erase.end(), dummy), 1);
			ASSERT_EQ(data_buf.size(), 4);
			eraseVector(data, erase);
			std::vector<int> tested(data_buf.begin(), data_buf.end());
			ASSERT_EQ(tested, data);
		}
	}

	TEST_F( VSortedVectorTest , testFTIndexIteratorCanBeCreatedOnVSortedVector )
    {
		auto memspace = getMemspace();

		v_sorted_vector<std::uint64_t> data_buf(memspace, 8);
		std::vector<std::uint64_t> data { 3, 6, 7, 14, 35, 35, 55 };
		data_buf.bulkInsert(data);
		
		// iterate with the FT_IndexIterator
		using IndexT = v_sorted_vector<std::uint64_t>;
		FT_IndexIterator<IndexT, std::uint64_t> cut(data_buf, -1);
		std::vector<std::uint64_t> result;
		while (!cut.isEnd()) {
			std::uint64_t next_value;
			cut.next(&next_value);
			result.push_back(next_value);
		}
		ASSERT_EQ(result, (std::vector<std::uint64_t> { 55, 35, 35, 14, 7, 6, 3 }));
	}
	
} 
