// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <thread>
#include <cstdlib>
#include <utility>

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
    
using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class VBVectorTests: public MemspaceTestBase
    {
    };

    template <std::size_t N = 0> struct [[gnu::packed]] b_item
    {
        using KeyT = int;
        KeyT m_key = 0;
        std::array<uint32_t, N> m_value;

        b_item() = default;

        b_item(int key)
            : m_key(key)
        {
        }

        int key() const {
            return m_key;
        }

        void dump(std::ostream &os) const {
            os << m_key;
        }

        void operator=(int key) {
            this->m_key = key;
        }
    };
    
    template <typename T> void testSizeOfDataBlockDoesNotExceedSizeOfPage(VBVectorTests &self) 
    {
        auto memspace = self.getMemspace();
        db0::v_bvector<T> cut(memspace);

        using DataType = typename v_bvector<T>::DataBlockOverlaidType;
        const int item_count = 100000;
        for (int index = 0;(index < item_count);++index) 
        {
            if (index < 100 || index % 1000==0) 
            {
                cut.forAll([](const DataType &, std::pair<uint64_t, uint64_t> range) {
                    auto size = range.second - range.first;
                    ASSERT_TRUE(size * sizeof(T) <= 4096);
                });
            }
            cut.emplace_back();
        }
    }

    TEST_F( VBVectorTests , testElementsCanBeIterated ) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint64_t> cut(memspace);

        const unsigned int item_count = 6;
        for (unsigned int index = 0;index < item_count;++index) {
            cut.emplace_back();
        }

        unsigned int count = 0;
        auto it = cut.cbegin();
        while (it!=cut.cend()) {
            ++it;
            ++count;
        }
        ASSERT_EQ(item_count, count);
    }

    TEST_F( VBVectorTests , testDataCanBeWrittenToOneInstanceAndReadFromAnother )
    {
        // FIXME: this functionality is not supported yet
        /*
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint64_t> cut_1(memspace);
        // open same instance in 2 objects
        v_bvector<std::uint64_t> cut_2(memspace.myPtr(cut_1.getAddress()));

        const unsigned int item_count = 6;
        for (unsigned int index = 0;index < item_count;++index) {
            cut_1.emplace_back();
        }

        ASSERT_EQ(cut_2.size(), cut_1.size());
        unsigned int count = 0;
        auto it = cut_2.cbegin();
        while (it!=cut_2.cend()) {
            ++it;
            ++count;
        }
        ASSERT_EQ(item_count, count);
        */
    }

    TEST_F( VBVectorTests , testVBVectorIterator) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        // create virtualized b-vector object
        Address ptr_vector = {};
        {
            v_bvector<b_item<>, Address> cut(memspace);
            for (int i = 0;i < 512;++i) {
                cut.setItem(i, i);
            }
            ASSERT_EQ(512u, cut.size());
            ptr_vector = cut.getAddress();
        }
        // iterate part of existing data structure
        {
            v_bvector<b_item<>, Address> _bv(memspace.myPtr(ptr_vector));
            v_bvector<b_item<>, Address>::const_iterator it0 = _bv.begin(129);
            v_bvector<b_item<>, Address>::const_iterator it1 = _bv.begin(214);
            auto i = 129;
            while (i < 193) {
                ASSERT_EQ((*it0).m_key, i);
                ++it0;
                ++i;
            }
            i += 5;
            it0 += 5;
            // test diff operator
            ASSERT_EQ( (it1 - it0), (214 - static_cast<size_t>(i) ));
            while (it0!=it1) {
                ASSERT_EQ((*it0).m_key, i);
                ++it0;
                ++i;
            }
        }
    }

    TEST_F( VBVectorTests , LONG_testVBVectorInsertModifyAndPopBack) 
    {
        using item_type = b_item<>;
        using b_vector_type = v_bvector<item_type, Address>;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        // create b-vector object
        {
            b_vector_type cut(memspace);
            cut.setItem(0, 5);
            cut.setItem(3, 11);
            cut.setItem(12, 44);
            ASSERT_EQ(13u, cut.size() );
        }        
        Address ptr_b_vector = {};
        {
            b_vector_type cut(memspace);
            cut.setItem(28, 44);
            cut.setItem(0, 5);
            cut.setItem(2, 11);            
            ptr_b_vector = cut.getAddress();
        }
        // open & dump existing vector
        {
            b_vector_type _bv(memspace.myPtr(ptr_b_vector));
            std::stringstream _str;
            _bv.dump(_str);
            std::string str_dump = _str.str();
            ASSERT_EQ(str_dump,"5,0,11,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,44");
        }
        // stress test insert random values
        int ref_buf[20024];
        std::memset(ref_buf,0,sizeof(ref_buf));
        int size = 0;
        {
            b_vector_type cut(memspace);
            {
                int index = 6;
                int val = 15;
                ref_buf[index] = val;
                cut.setItem(index, val);
                size = std::max(size, index + 1);
            }
            int count = 15000;
            while ((count--) > 0) {
                int index = rand() % 20000;
                int val = rand() % 100;
                ref_buf[index] = val;
                cut.setItem(index, val);
                size = std::max(size,index + 1);
            }
            {
                int index = 20021;
                int val = 15;
                ref_buf[index] = val;
                cut.setItem(index, val);
                size = std::max(size,index + 1);
            }
            ptr_b_vector = cut.getAddress();
        }
        // validate content & size
        {
            b_vector_type _bv(memspace.myPtr(ptr_b_vector));
            std::stringstream _str,_str2;
            _bv.dump(_str);
            std::string str_dump = _str.str();
            for (int i=0;i<size;++i) {
                if (i!=0) {
                    _str2 << ",";
                }
                _str2 << ref_buf[i];
            }
            std::string str_dump2 = _str2.str();
            ASSERT_EQ(str_dump, str_dump2);
        }
        // pop_back from existing collection
        {
            b_vector_type _bv(memspace.myPtr(ptr_b_vector));
            int pop_count = (size / 2);
            size -= pop_count;
            _bv.pop_back(pop_count);
        }
        // validate content & size
        {
            b_vector_type _bv(memspace.myPtr(ptr_b_vector));
            std::stringstream _str,_str2,_str3;
            _bv.dump(_str);
            std::string str_dump = _str.str();
            for (int i=0;(i < size);++i) {
                if (i!=0) {
                    _str2 << ",";
                }
                _str2 << ref_buf[i];
            }
            // random access test
            {
                int count = 100;
                while ((count--) > 0) {
                    int index = rand() % size;
                    ASSERT_EQ(_bv[index].m_key, ref_buf[index]);
                }
            }
            ASSERT_EQ(str_dump,_str2.str());
            // pop back / leave 3 items
            _bv.pop_back(size - 3);
            ASSERT_EQ(_bv.height(), 1);
            ASSERT_EQ(_bv.getBClass(), 7);
            _bv.pop_back();
            ASSERT_EQ(_bv.getBClass(), 8);
            _bv.pop_back();
            ASSERT_EQ(_bv.getBClass(), 9);
            // pop_back all remaining items
            _bv.pop_back(1);
            ASSERT_EQ( 0u, _bv.size());
            ASSERT_TRUE(_bv->m_ptr_root.getOffset() == 0u);
        }
    }

    TEST_F( VBVectorTests , VBVectorInt8tTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::int8_t> v(memspace);
        v.growBy(2);
        for(int i = 0; i <= 2; ++i) {
            v.push_back(i);
        }
    }

    TEST_F( VBVectorTests , VBVectorPushAtBeginShorterTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);
        for (int i = 7; i <= 16; ++i) {
            v.push_back(i);
        }

        std::vector<uint32_t> to_insert {1,2,3,4,5,6};
        v.push_at(0, to_insert.begin(), to_insert.end());

        ASSERT_EQ(16, v.size());

        auto it = v.begin();
        for (auto i = 1; i <=16; ++i, ++it) {
            ASSERT_EQ(i, *it);
        }
    }

    TEST_F( VBVectorTests , VBVectorPushAtBeginLongerTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);
        for (int i = 13; i <= 16; ++i) {
            v.push_back(i);
        }

        std::vector<uint32_t> to_insert {1,2,3,4,5,6,7,8,9,10,11,12};
        v.push_at(0, to_insert.begin(), to_insert.end());

        ASSERT_EQ(16, v.size());

        auto it = v.begin();
        for (auto i = 1; i <=16; ++i, ++it) {
            ASSERT_EQ(i, *it);
        }
    }

    TEST_F( VBVectorTests , VBVectorPushAtMiddleShorterTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);

        for (int i = 1; i <= 3; ++i) {
            v.push_back(i);
        }

        for (int i = 11; i <= 16; ++i) {
            v.push_back(i);
        }

        std::vector<std::uint32_t> to_insert {4,5,6,7,8,9,10};
        v.push_at(3, to_insert.begin(), to_insert.end());

        ASSERT_EQ(16, v.size());
        auto it = v.begin();
        for (auto i = 1; i <=16; ++i, ++it) {
            ASSERT_EQ(i, *it);
        }
    }

    TEST_F( VBVectorTests , VBVectorPushAtMiddleLongerTest) 
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);

        for (int i = 1; i <= 4; ++i) {
            v.push_back(i);
        }

        for (int i = 14; i <= 16; ++i) {
            v.push_back(i);
        }

        std::vector<std::uint32_t> to_insert {5,6,7,8,9,10,11,12,13};
        v.push_at(4, to_insert.begin(), to_insert.end());

        ASSERT_EQ(16, v.size());
        auto it = v.begin();
        for (auto i = 1; i <=16; ++i, ++it) {
            ASSERT_EQ(i, *it);
        }
    }

    TEST_F( VBVectorTests , VBVectorPushAtEndTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);

        for (int i = 1; i <= 9; ++i) {
            v.push_back(i);
        }

        std::vector<std::uint32_t> to_insert {10,11,12,13,14,15,16};
        v.push_at(9, to_insert.begin(), to_insert.end());

        ASSERT_EQ(16, v.size());
        auto it = v.begin();
        for(auto i = 1; i <=16; ++i, ++it) {
            ASSERT_EQ(i, *it);
        }
    }
    
    TEST_F( VBVectorTests , VBVectorPushAtQuickTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);

        v.push_back(5606);
        v.push_back(4989);
        v.push_back(1762);
        v.push_back(9916);

        std::vector<uint32_t> to_insert {5033};
        v.push_at(0, to_insert.begin(), to_insert.end());

        ASSERT_EQ(5, v.size());
    }

    TEST_F( VBVectorTests , VBVectorPushAtIndexGreaterThanVectorLengthTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<uint32_t> v(memspace);
        for (int i = 1; i <= 5; ++i) {
            v.push_back(i);
        }

        std::vector<uint32_t> to_insert {6,7,8};
        ASSERT_THROW(v.push_at(6, to_insert.begin(), to_insert.end()), db0::InputException);
    }

    TEST_F( VBVectorTests , VBVectorRandomAccessTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<uint32_t> v(memspace);
        std::vector<uint32_t> expected;
        for (int i = 0; i < 50 ; ++i) {
            auto val = rand();
            v.push_back(val);
            expected.push_back(val);
        }

        for (int i = 0; i < 50; ++i) {
            ASSERT_EQ(expected[i], v[i]);
        }
    }

    TEST_F( VBVectorTests , VBVectorIteratorPlusPlusTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);
        std::vector<std::uint32_t> expected;
        for (int i = 0; i < 3 ; ++i) {
            auto val = rand();
            v.push_back(val);
            expected.push_back(val);
        }

        auto it = v.begin();

        ASSERT_EQ(expected[0], *it);
        ++it;
        ASSERT_EQ(expected[1], *it);
        ++it;
        ASSERT_EQ(expected[2], *it);
    }

    TEST_F( VBVectorTests , VBVectorIteratorMinusMinusTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);
        std::vector<std::uint32_t> expected;
        for (int i = 0; i < 3 ; ++i) {
            auto val = rand();
            v.push_back(val);
            expected.push_back(val);
        }

        auto it = v.begin(2);

        ASSERT_EQ(expected[2], *it);
        --it;
        ASSERT_EQ(expected[1], *it);
        --it;
        ASSERT_EQ(expected[0], *it);
    }

    TEST_F( VBVectorTests , VBVectorIteratorComparisonTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);
        std::vector<std::uint32_t> expected;
        for (int i = 0; i < 5 ; ++i) {
            auto val = rand();
            v.push_back(val);
            expected.push_back(val);
        }

        using iterator_t = decltype(v.begin());

        std::vector<iterator_t> itA {v.begin(), v.begin(1), v.begin(2), v.begin(3), v.begin(4)};
        std::vector<iterator_t> itB {v.begin(), v.begin(1), v.begin(2), v.begin(3), v.begin(4)};

        for(auto i = 0; i < 5; ++i)
        {
            ASSERT_TRUE(itA[i] == itB[i]);
            ASSERT_TRUE(itA[i] == itA[i]);
        }

        ASSERT_TRUE(v.begin(5) == v.end());

        for(auto i = 1; i < 5; ++i)
        {
            ASSERT_TRUE(itA[i - 1] < itA[i]);
            ASSERT_TRUE(itA[i] > itA[i - 1]);

            ASSERT_TRUE(itA[i - 1] <= itA[i]);
            ASSERT_TRUE(itA[i] >= itA[i - 1]);

            ASSERT_TRUE(itA[i - 1] != itA[i]);
        }
    }

    TEST_F( VBVectorTests , VBVectorIteratorPlusMinusEqualTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);
        std::vector<std::uint32_t> expected;
        for (int i = 0; i < 5 ; ++i) {
            auto val = rand();
            v.push_back(val);
            expected.push_back(val);
        }

        using iterator_t = decltype(v.begin());

        ASSERT_TRUE(v.begin(4) == v.begin() + 4);
        ASSERT_TRUE(v.begin(3) - 2 == v.begin(1));

        auto it = v.begin();
        it += 2;

        ASSERT_TRUE(it == v.begin(2));
        ASSERT_EQ(*it, *v.begin(2));

        it -= 1;

        ASSERT_TRUE(it == v.begin(1));
        ASSERT_EQ(*it, *v.begin(1));

        ASSERT_TRUE(v.begin() + 2 == v.begin() - (-2));

        ASSERT_EQ(3, v.begin(4) - v.begin(1));
    }

    TEST_F( VBVectorTests , VBVectorIteratorCopyingTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);
        std::vector<std::uint32_t> expected;
        for (int i = 0; i < 5 ; ++i) {
            auto val = rand();
            v.push_back(val);
            expected.push_back(val);
        }

        using iterator_t = decltype(v.begin());

        auto it1 = v.begin();
        auto it1_copy = it1;
        ASSERT_EQ(*it1, *it1_copy);
        ASSERT_TRUE(it1 == it1_copy);

        auto it2(it1);

        ASSERT_EQ(*it1, *it2);
        ASSERT_TRUE(it1 == it2);
    }

    TEST_F( VBVectorTests, VBVectorIteratorMovingTest) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<std::uint32_t> v(memspace);
        std::vector<std::uint32_t> expected;
        auto size = rand() % 10000 + 1;
        for (int i = 0; i < size ; ++i) {
            auto val = rand();
            v.push_back(val);
            expected.push_back(val);
        }

        using iterator_t = decltype(v.begin());

        for (int i = 0; i < 100; ++i) {
            auto begin_index = rand() % size;
            auto it = v.begin(begin_index);
            ASSERT_EQ(expected.at(begin_index), *it);
            for (int j = 0; j < 1000; j++) {
                auto new_index = rand() % size;
                it.moveTo(new_index);
                ASSERT_EQ(expected.at(new_index), *it);
            }
        }
    }

    TEST_F( VBVectorTests, testVBVectorCanBeCreatedWithCopyConstructor ) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<int> buf1(memspace);

        const int item_count = 100000;
        for (int index = 0;index < item_count;++index) {
            buf1.emplace_back(index);
        }

        // create second instance using copy constructor
        v_bvector<int> cut(memspace, buf1);
        ASSERT_EQ ( buf1.size(), cut.size() );
        // validate elements
        int index = 0;
        const auto &const_cut = cut;
        auto it = const_cut.begin(), end = const_cut.end();
        while (it!=end) {
            ASSERT_EQ ( index, *it );
            ++index;
            ++it;
        }
    }

    TEST_F( VBVectorTests, testSwapAndPopElementsByIndex ) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<int> buf(memspace);

        // elements to erase
        std::vector<std::uint64_t> element_numbers 
        {
            11,15,20,21,26,40,45,52,60,63,64,65,74,76,79,83,89,91,94,95,96,105,113,118,127,129,138,145,148,150,
            152,160,161,167,171,177,179,186, 187,195,198,205,208,218,219,220,233,236,243,246,251,252,255,261,262,
            263,264,267,270,281,285,293,296,303,310,321,322,330,336,337,341, 343,348,352,356,361,366,370,376,378,
            384,387,391,407,419,426,436,437,438,441,442,446,448,456,458,460,462,464,469,472,473,477,484,491, 499,
            509,510,512,515,517,520,524,529,533,545,546,549,580,582,586,587,591,592,600,602,604,605,606,613,615,
            618,619,622,625,626,628,629, 632,638,641,643,644,645,646,651,653,665,667,680,682,688,695,699,704,706,
            710,711,717,718,734,736,739,742,753,755,763,765,766,767,770,773,774,778,779,780,781,784,791,792,809,
            812,813,814,824,829,831,842,849,853,854,856,861,864,866,869,879,894,898,902,907,908,910,921,
            927,931,940,943,944,949,955,956,969,991,994,999
        };

        std::unordered_set<std::uint64_t> to_erase;
        std::unordered_set<std::uint64_t> unique_values;

        for (auto num: element_numbers) {
            to_erase.insert(num);
        }

        const int item_count = 1000;
        for (int index = 0;(index < item_count);++index) {
            buf.emplace_back(index);
            if (to_erase.find(index)==to_erase.end()) {
                unique_values.insert(index);
            }
        }

        buf.swapAndPop(element_numbers);

        // validate size after erase
        ASSERT_EQ ( unique_values.size(), buf.size() );
        auto it = buf.cbegin(), end = buf.cend();
        while (it!=end) {
            ASSERT_TRUE ( unique_values.find(*it)!=unique_values.end() );
            ++it;
        }
    }

    TEST_F( VBVectorTests, testEraseElementsByIndex ) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        v_bvector<int> buf(memspace);

        // elements to erase
        std::vector<std::uint64_t> element_numbers {
                1,2,3,4,5,6,7,8,9,10
        };

        std::unordered_set<std::uint64_t> to_erase;
        std::unordered_set<std::uint64_t> unique_values;

        for (auto num: element_numbers) {
            buf.emplace_back(num);
        }

        //erase last element
        buf.erase(9);
        ASSERT_EQ (  buf.size(),9);
        //check last element
        ASSERT_EQ ( buf[8], 9);

        //erase first element
        buf.erase(0);
        ASSERT_EQ (  buf.size(),8);
        ASSERT_EQ ( buf[0], 2);
        //erase element
        buf.erase(3);
        ASSERT_EQ (  buf.size(),7);
        // validate order
        std::vector<uint64_t> elements_order {
                2,3,4,6,7,8,9
        };
        for(unsigned int i = 0 ; i < buf.size();++i){
            ASSERT_EQ ( buf[i],elements_order[i]);
        }
    }

    TEST_F( VBVectorTests, testSwapAndPopByFunction ) 
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        db0::v_bvector<int> buf(memspace);

        auto select = [](const uint64_t &key) {
            return key % 7 == 0;
        };

        const int item_count = 100000;
        for (int index = 0;(index < item_count);++index) {
            buf.emplace_back(index);
        }

        buf.swapAndPop(select);
        auto it = buf.cbegin(), end = buf.cend();
        while (it!=end) {
            ASSERT_FALSE ( select(*it) );
            ++it;
        }
    }

    TEST_F( VBVectorTests, testSizeOfDataBlockDoesNotExceedSizeOfPage )
    {
        // test with various item sizes
        testSizeOfDataBlockDoesNotExceedSizeOfPage<std::uint64_t>(*this);
        testSizeOfDataBlockDoesNotExceedSizeOfPage<b_item<0> >(*this);
        testSizeOfDataBlockDoesNotExceedSizeOfPage<b_item<1> >(*this);
        testSizeOfDataBlockDoesNotExceedSizeOfPage<b_item<4> >(*this);
        testSizeOfDataBlockDoesNotExceedSizeOfPage<b_item<16> >(*this);
    }
    
    TEST_F( VBVectorTests, testVBVectorUseAfterClear )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        Address addr = {};
        {
            db0::v_bvector<int> cut(memspace);
            addr = cut.getAddress();
            cut.emplace_back(0);
            cut.clear();
            cut.emplace_back(1);
        }

        db0::v_bvector<int> cut(memspace.myPtr(addr));
        ASSERT_EQ(1u, cut.size());        
        for (auto value: cut) {
            ASSERT_EQ(1, value);
        } 
    }
    
    TEST_F( VBVectorTests, testVBVectorGrowBy1AfterDetach )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");

        db0::v_bvector<int> cut(memspace);
        for (int i = 0; i < 100; ++i) {
            cut.emplace_back(i);
            cut.detach();
        }

        ASSERT_EQ(100u, cut.size());
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(i, cut[i]);
        }
    }
    
    TEST_F( VBVectorTests, testVBVectorGrowBy1AfterInstanceRelease )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        Address addr = {};
        {
            db0::v_bvector<int> cut(memspace);
            addr = cut.getAddress();
        }

        for (int i = 0; i < 100; ++i) {
            db0::v_bvector<int> cut(memspace.myPtr(addr));
            cut.emplace_back(i);
            cut.detach();
        }

        db0::v_bvector<int> cut(memspace.myPtr(addr));
        ASSERT_EQ(100u, cut.size());        
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(i, cut[i]);
        }
    }
        
} 
