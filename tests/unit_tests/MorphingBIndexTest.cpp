// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <unordered_set>
#include <initializer_list>
#include <set>

#include <dbzero/core/collections/b_index/mb_index.hpp>

namespace tests

{

	using namespace db0;

	class MorphingBIndexTest: public MemspaceTestBase
	{
	public :
        using index_t = db0::MorphingBIndex<std::uint64_t>;
		
		void insertUnique(index_t &index, std::initializer_list<std::uint64_t> in)
		{
			std::unordered_set<std::uint64_t> data;
			for (std::uint64_t id: in) {
				data.insert(id);
			}
			index.bulkInsertUnique(data.begin(), data.end());
		}

		void erase(index_t &index, std::initializer_list<std::uint64_t> in) {
			std::unordered_set<std::uint64_t> data;
			for (std::uint64_t id: in) {
				data.insert(id);
			}
			index.bulkErase(data.begin(), data.end());
		}
	};
	
	TEST_F( MorphingBIndexTest , testNullInstanceCanBeCreated )
	{
		index_t cut;
		ASSERT_TRUE(cut.isNull());
	}

	TEST_F( MorphingBIndexTest , testCanBeCreatedAsEmpty )
	{
		auto memspace = getMemspace();

		index_t cut(memspace, bindex::type::empty);
		ASSERT_TRUE(cut.empty());
	}

	TEST_F( MorphingBIndexTest , testCanStoreSingleElementByMorphing )
	{
		auto memspace = getMemspace();

		index_t cut(memspace, bindex::type::empty);
		insertUnique(cut, { 1234 } );
		ASSERT_TRUE(!cut.empty());
		ASSERT_EQ(bindex::type::itty, cut.getIndexType());
	}

	TEST_F( MorphingBIndexTest , testCanStoreSingleElementAndThenRetrieveIt )
	{
        auto memspace = getMemspace();

		Address addr;
		bindex::type index_type;
		{
			index_t cut(memspace, bindex::type::empty);
			insertUnique(cut, { 1234 } );
			index_type = cut.getIndexType();
			addr = cut.getAddress();
		}
		// open existing and read contents
		index_t cut(memspace, addr, index_type);
		ASSERT_FALSE(cut.empty());
	}
	
	TEST_F( MorphingBIndexTest , testCanSwitchBetweenSixMorphologiesAsElementsAreAdded )
	{
        auto memspace = getMemspace();

		index_t cut(memspace, bindex::type::empty, 8);
		ASSERT_EQ(bindex::type::empty, cut.getIndexType()); // 0 elements = empty
		insertUnique(cut, { 1 });
		ASSERT_EQ(bindex::type::itty, cut.getIndexType()); // 1 element = itty_index
		insertUnique(cut, { 2 });
		ASSERT_EQ(bindex::type::array_2, cut.getIndexType()); // 2 elements = array_2
		insertUnique(cut, { 3 });
		ASSERT_EQ(bindex::type::array_3, cut.getIndexType()); // 3 elements = array_3
		insertUnique(cut, { 4 });
		ASSERT_EQ(bindex::type::array_4, cut.getIndexType()); // 4 elements = array_4
		insertUnique(cut, { 5 });
		ASSERT_EQ(bindex::type::sorted_vector, cut.getIndexType()); // 5 elements = sorted_vector
		insertUnique(cut, { 6 });
		ASSERT_EQ(bindex::type::sorted_vector, cut.getIndexType()); // 6 elements = sorted_vector
		insertUnique(cut, { 7 });
		ASSERT_EQ(bindex::type::sorted_vector, cut.getIndexType()); // 7 elements = sorted_vector
		insertUnique(cut, { 8 });
		ASSERT_EQ(bindex::type::sorted_vector, cut.getIndexType()); // 8 elements = sorted_vector
		unsigned int sv_limit = cut.getSortedVectorSizeLimit();
		log << "SV size limit:" << sv_limit << std::endl;
		unsigned int i = 9;
		// add elements to reach SV size limit
		while (i <= sv_limit)
		{
			insertUnique(cut, { i });
			++i;
		}
		insertUnique(cut, { i });
		ASSERT_EQ(bindex::type::bindex, cut.getIndexType()); // > SV limit elements = bindex
	}

	TEST_F( MorphingBIndexTest , testInsertUniqueWillIgnoreDuplicates )
	{
        auto memspace = getMemspace();

		index_t cut(memspace);
		insertUnique(cut, { 1, 2, 3 });
		ASSERT_EQ(3, cut.size());
		insertUnique(cut, { 2, 4, 8 });
		ASSERT_EQ(5, cut.size());
		insertUnique(cut, { 14, 1, 2, 5, 7, 3 });
		ASSERT_EQ(8, cut.size());
	}

	TEST_F( MorphingBIndexTest , testInsertCanSkipFewMorphologies )
	{
        auto memspace = getMemspace();

		index_t cut(memspace);
		ASSERT_EQ(bindex::type::empty, cut.getIndexType());
		// adding 4 elements will cause jump to "array_4" from "empty"
		insertUnique(cut, { 1, 2, 3, 4 });
		ASSERT_EQ(bindex::type::array_4, cut.getIndexType());
	}
	
	TEST_F( MorphingBIndexTest , testStorageSizeGrowsWithNumberOfElements )
	{
        auto memspace = getMemspace();

		index_t cut(memspace);
		std::uint64_t value = 0;
		// number of values written to index
		int count = 0;
		int max_count = 5000;
		std::uint64_t old_size = 0;
		while (count < max_count) {
			insertUnique(cut, { value++ });
			++count;
			auto size_of = cut.calculateStorageSize();
			if (size_of!=old_size) {
				log << size_of << std::endl;
			}
			log << "Size per item: " << (double)size_of / (double)count << std::endl;
			// actual data size
			auto data_size = count * sizeof(value);
			auto overhead = size_of - data_size;
			// overhead as percent of actual data
			log << "Overhead: " << (double)overhead / (double)data_size << std::endl;
			ASSERT_TRUE(size_of >= old_size);
			old_size = size_of;
		}
	}

	TEST_F( MorphingBIndexTest , testCanIterateOverAllElementsInserted )
	{
        auto memspace = getMemspace();

		index_t cut(memspace);
		insertUnique(cut, { 0, 123, 9, 11, 88 });
		auto it = cut.beginJoin(1);
		int count = 0;
		while (!it.is_end()) {
			log << *it << std::endl;
			++it;
			++count;
		}
		ASSERT_EQ(5, count);
	}

	TEST_F( MorphingBIndexTest , testCanBeCreatedDirectyAsIttyIndex )
	{
		auto memspace = getMemspace();
        db0::MorphingBIndex<std::uint64_t> cut(memspace, 123u, 48);
		auto it = cut.beginJoin(1);
		unsigned int count = 0;
		while (!it.is_end()) {
			ASSERT_EQ (123u, *it);
			++it;
			++count;
		}
		ASSERT_EQ( 1u, count);
	}

	TEST_F( MorphingBIndexTest , testBoolConversionCanTestForNullInstance ) {
		db0::MorphingBIndex<std::uint64_t> cut;
		ASSERT_FALSE (cut);
	}

	TEST_F( MorphingBIndexTest , testIteratorHasDefaultConstructor ) {
		ASSERT_NO_THROW( db0::MorphingBIndex<uint64_t>::joinable_const_iterator() );
	}

	TEST_F( MorphingBIndexTest , testAssignmentOperatorIsAllowed )
	{
        auto memspace = getMemspace();
        db0::MorphingBIndex<std::uint64_t> cut;
        db0::MorphingBIndex<std::uint64_t> i1(memspace, 123u);
        db0::MorphingBIndex<std::uint64_t> i2(memspace, 456u);

        cut = i2;

        auto it = cut.beginJoin(1);
        unsigned int count = 0;
        while (!it.is_end()) {
            ASSERT_EQ (456u, *it);
            ++it;
            ++count;
        }
        ASSERT_EQ( 1u, count);
	}

    TEST_F( MorphingBIndexTest , testIteratorAssignmentOperatorIsAllowed ) 
	{
        auto memspace = getMemspace();
        db0::MorphingBIndex<std::uint64_t> i1(memspace, 123u);

        db0::MorphingBIndex<std::uint64_t>::joinable_const_iterator cut;
        auto it = i1.beginJoin(1);

        cut = it;
        unsigned int count = 0;
        while (!cut.is_end()) {
            ASSERT_EQ (123u, *cut);
            ++cut;
            ++count;
        }

        ASSERT_EQ( 1u, count);
    }

	TEST_F( MorphingBIndexTest , testIteratorInstanceCanBeMovedAlongWithCollectionInstance ) {
		auto memspace = getMemspace();

        db0::MorphingBIndex<std::uint64_t> i2;
        db0::MorphingBIndex<std::uint64_t>::joinable_const_iterator it2;

        {
            db0::MorphingBIndex<std::uint64_t> i1(memspace, 123u);
            auto it1 = i1.beginJoin(1);

            i2 = i1;
            it2 = it1;
        }

		unsigned int count = 0;
		while (!it2.is_end()) {
			ASSERT_EQ (123u, *it2);
			++it2;
			++count;
		}

		ASSERT_EQ( 1u, count);
	}

	TEST_F( MorphingBIndexTest , testEraseSingleElement )
	{
		auto memspace = getMemspace();

		index_t cut(memspace);
		insertUnique(cut, { 0, 123, 9, 5 });
		ASSERT_EQ(bindex::type::array_4, cut.getIndexType());

		cut.erase(123);
		ASSERT_EQ(3, cut.size());
		ASSERT_EQ(bindex::type::array_3, cut.getIndexType());

		cut.erase(0);
		ASSERT_EQ(2, cut.size());
		ASSERT_EQ(bindex::type::array_2, cut.getIndexType());

		cut.erase(5);
		ASSERT_EQ(1u, cut.size());
		ASSERT_EQ(bindex::type::itty, cut.getIndexType());

		cut.erase(9);
		ASSERT_EQ(0, cut.size());
		ASSERT_EQ(bindex::type::empty, cut.getIndexType());
	}

    TEST_F( MorphingBIndexTest , testInsertUniqueElements )
    {
        auto memspace = getMemspace();

        index_t cut(memspace);
        cut.insert(0);
        cut.insert(123);
        cut.insert(91);
        cut.insert(5);
        ASSERT_EQ(bindex::type::array_4, cut.getIndexType());

        for (unsigned int i = 0; i < 100; ++i) {
			if (!cut.contains(i)) {
            	cut.insert(i);
			}
        }

        ASSERT_EQ(101u, cut.size());
    }

    TEST_F( MorphingBIndexTest , testMorphingBIndexIsCopyConsturctible )
    {
        auto memspace = getMemspace();

        index_t cut(memspace);
        cut.insert(0);
        cut.insert(123);
        cut.insert(91);
        cut.insert(5);

        index_t copy(cut);
        ASSERT_EQ( cut.size(), copy.size());
        ASSERT_EQ( cut.getIndexType(), copy.getIndexType());
    }

	TEST_F( MorphingBIndexTest , testClonedIteratorWillPreseveStateAndBounds )
	{
		auto memspace = getMemspace();

		index_t cut(memspace, bindex::type::empty, 8);
		insertUnique(cut, { 0, 123, 9, 5, 15, 19923, 312, 311, 540, 1119, 912, 919992, 0, 1, 2, 34, 567, 89  });
		auto it = cut.beginJoin(-1);
		it.limitBy(10);
		it.join(1000, -1);
		// copy (clone)
		int count = 0;
		decltype(it) it_clone(it);
		while (!it.is_end()) {
			ASSERT_TRUE(!it_clone.is_end());
			ASSERT_EQ(*it, *it_clone);
			--it;
			--it_clone;
			++count;
		}
		ASSERT_TRUE(it_clone.is_end());
		ASSERT_EQ(9, count);
	}

	TEST_F( MorphingBIndexTest , testIteratorCanBeClonedUsingCopyConstructor )
	{
        auto memspace = getMemspace();

		index_t cut(memspace, bindex::type::empty, 8);
		insertUnique(cut, { 0, 123, 9, 5, 15, 19923, 312, 311, 540, 1119, 912, 919992, 0, 1, 2, 34, 567, 89  });
		auto it = cut.beginJoin(-1);
		it.join(1000, -1);
		// copy (clone)
		int count = 0;
		decltype(it) it_clone(it);
		while(!it.is_end())
		{
			ASSERT_TRUE(!it_clone.is_end());
			ASSERT_TRUE(*it == *it_clone);
			--it;
			--it_clone;
			++count;
		}
		ASSERT_TRUE(it_clone.is_end());
		ASSERT_EQ(14, count);
	}

	TEST_F( MorphingBIndexTest , testBulkEraseWillChangeFixedSizeMorphologyToHoldProperNumberOfItems )
	{
        auto memspace = getMemspace();

		index_t cut(memspace);
		insertUnique(cut, { 0, 123, 9, 5 });
		ASSERT_EQ(bindex::type::array_4, cut.getIndexType());

		erase(cut, { 123 });
		ASSERT_EQ(3, cut.size());
		ASSERT_EQ(bindex::type::array_3, cut.getIndexType());

		erase(cut, { 0 });
		ASSERT_EQ(2, cut.size());
		ASSERT_EQ(bindex::type::array_2, cut.getIndexType());

		erase(cut, { 5 });
		ASSERT_EQ(1u, cut.size());
		ASSERT_EQ(bindex::type::itty, cut.getIndexType());

		erase(cut, { 9 });
		ASSERT_EQ(0, cut.size());
		ASSERT_EQ(bindex::type::empty, cut.getIndexType());
	}

    TEST_F( MorphingBIndexTest , testSequenceOfOperationsWhichTriggeredAssertion )
    {
        auto memspace = getMemspace();

        index_t cut(memspace, bindex::type::empty, 8);
        insertUnique(cut, { 1 });
        insertUnique(cut, { 2 });
        insertUnique(cut, { 3 });
        insertUnique(cut, { 4 });
        insertUnique(cut, { 5 });
        ASSERT_EQ (bindex::type::sorted_vector, cut.getIndexType());

        erase(cut, { 5 });
        erase(cut, { 4 });
        // still sorted vector after erase
        ASSERT_EQ (bindex::type::sorted_vector, cut.getIndexType());

        insertUnique(cut, { 4 });
        ASSERT_EQ (bindex::type::sorted_vector, cut.getIndexType());
        insertUnique(cut, { 5 });
        ASSERT_EQ (bindex::type::sorted_vector, cut.getIndexType());
    }

	TEST_F( MorphingBIndexTest , testBulkEraseWillRetainBIndexMorphology )
	{
        auto memspace = getMemspace();

		index_t cut(memspace, bindex::type::empty, 8);
		insertUnique(cut, { 0, 123, 9, 5, 15, 19923, 312, 311, 540, 1119  });
		unsigned int i = 500;
		// add elements to reach SV size limit
		while (cut.getIndexType()!=bindex::type::bindex)
		{
			insertUnique(cut, { i });
			++i;
		}
		auto size_before = cut.size();
		erase(cut, { 0, 19923, 311, 312, 540 });
		ASSERT_EQ(size_before - 5, cut.size());

		// morphology retained
		ASSERT_EQ(bindex::type::bindex, cut.getIndexType());
	}

	TEST_F( MorphingBIndexTest , testIteratingOverEmptyIndexAfterErase )
	{
		auto memspace = getMemspace();
		
		index_t cut(memspace);
		insertUnique(cut, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
		// erase all elements
		erase(cut, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
		auto it = cut.beginJoin(-1);
		int count = 0;
		while (!it.is_end()) {
			--it;
			++count;
		}
		ASSERT_EQ(0, count);
	}

} 
