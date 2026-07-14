// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <algorithm>
#include <dbzero/core/utils/dary_heap.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    TEST( DHeapTests , testDHeapPushBinary )
    {
        std::vector<int> data =     { 3, 4, 5, 6, 7, 1, 2, 9, 123, 412, 5, 6 };
        std::vector<int> max_data = { 3, 4, 5, 6, 7, 7, 7, 9, 123, 412, 412, 412 };
        std::vector<int> heap(data.size());
        for (unsigned int i = 0; i < data.size(); ++i) {
            heap[i] = data[i];
            auto it = db0::dheap::push<2>(heap.begin(), heap.begin() + i + 1, std::less<int>());
            ASSERT_EQ(*it, data[i]);
            ASSERT_EQ(heap[0], max_data[i]);
            ASSERT_TRUE(std::is_heap(heap.begin(), heap.begin() + i + 1, std::less<int>()));
        }
    }

    TEST( DHeapTests , testDHeapEraseBinary )
    {
        std::vector<int> data = { 3, 4, 5, 6, 7, 1, 2, 9, 123, 412, 5, 6 };        
        std::vector<int> heap(data.size());        
        for (unsigned int i = 0; i < data.size(); ++i) {
            heap[i] = data[i];
            db0::dheap::push<2>(heap.begin(), heap.begin() + i + 1, std::less<int>());
        }
        
        auto end = heap.data() + heap.size();
        for (int i: data) {
            db0::dheap::erase<2>(heap.data(), end, std::find(heap.data(), end, i), std::less<int>());
            --end;
            ASSERT_TRUE(std::is_heap(heap.data(), end, std::less<int>()));
        }
    }

    TEST( DHeapTests , testDHeapPushReversedBinary )
    {
        std::vector<int> data =     { 3, 4, 5, 6, 7, 1, 2, 9, 123, 412, 5, 6 };
        std::vector<int> max_data = { 3, 4, 5, 6, 7, 7, 7, 9, 123, 412, 412, 412 };
        std::vector<int> heap(data.size());
        for (unsigned int i = 0; i < data.size(); ++i) {
            // push elements from back of the vector
            heap[heap.size() - 1 - i] = data[i];
            auto it = dheap::rpush<2>(heap.end() - 1, heap.end() - i - 2, std::less<int>());
            ASSERT_EQ(*it, data[i]);
            ASSERT_EQ(heap[heap.size() - 1], max_data[i]);            
        }
    }

    TEST( DHeapTests , testDHeapFlipsDuringSort )
    {
        std::vector<int> data = { 3, 4, 5, 6, 7, 1, 2, 9, 123, 412, 5, 6 };        
        std::vector<int> heap(data.size() + 10);
        for (unsigned int i = 0; i < data.size(); ++i) {
            heap[i] = data[i];
            db0::dheap::push<2>(heap.begin(), heap.begin() + i + 1, std::less<int>());
        }
        
        db0::dheap::sort<2>(heap.data(), heap.data() + data.size(), heap.data() + heap.size(), std::less<int>());
        // make sure data is reversely sorted
        std::sort(data.begin(), data.end(), std::greater<int>());
        for (unsigned int i = 0; i < data.size(); ++i) {
            ASSERT_EQ(heap[heap.size() - 1 - i], data[i]);
        }
    }
    
    TEST( DHeapTests , testDHeapDoubleFlip )
    {
        std::vector<int> data = { 3, 4, 5, 6, 7, 1, 2, 9, 123, 412, 5, 6 };        
        std::vector<int> heap(data.size() + 10);
        // create reversed heap
        for (unsigned int i = 0; i < data.size(); ++i) {
            heap[heap.size() - 1 - i] = data[i];
            db0::dheap::rpush<2>(heap.end() - 1, heap.end() - i - 2, std::less<int>());
        }
        
        // reverse sort
        db0::dheap::rsort<2>(heap.data() + heap.size() - 1, heap.data() + heap.size() - 1 - data.size(), heap.data() - 1, std::less<int>());
        // make sure data is reversely sorted
        std::sort(data.begin(), data.end(), std::greater<int>());
        for (unsigned int i = 0; i < data.size(); ++i) {
            ASSERT_EQ(heap[i], data[i]);
        }
    }

}
