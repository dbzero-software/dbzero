// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/memory/swine_ptr.hpp>

using namespace std;
using namespace db0;

namespace tests 

{

    class TestClass
    {
    public:
        TestClass() { ++instance_count; }

        TestClass(int arg)
        { 
            ++instance_count;
            last_arg = arg; 
        }

        ~TestClass() { --instance_count; }

        static int instance_count;
        static int last_arg;
    };

    int TestClass::instance_count = 0;
    int TestClass::last_arg = 0;

    class SwinePtrTest : public testing::Test
    {
    public:
        void SetUp() override 
        {
            TestClass::instance_count = 0;
            TestClass::last_arg = 0;
        }
    };
    
    TEST_F( SwinePtrTest, testMakeSwineCreatesNewInstance )
    {   
        auto cut = make_swine<TestClass>();
        ASSERT_EQ(1, TestClass::instance_count);
    }
    
    TEST_F( SwinePtrTest, testSwinePtrDeletesInstanceWithLastUnReference )
    {
        auto cut = make_swine<TestClass>();
        ASSERT_EQ(cut.use_count(), 1);
        auto ptr = cut;
        ASSERT_EQ(cut.use_count(), 2);
        ASSERT_EQ(1, TestClass::instance_count);
        cut = nullptr;
        ASSERT_EQ(ptr.use_count(), 1);
        ASSERT_EQ(1, TestClass::instance_count);
        ptr = nullptr;
        ASSERT_EQ(0, TestClass::instance_count);
    }
    
    TEST_F( SwinePtrTest, testSwineWeakPtrCanLockSwinePtr )
    {
        auto cut = make_swine<TestClass>();
        weak_swine_ptr<TestClass> weak_ptr(cut);
        {
            auto ptr = weak_ptr.lock();
            ASSERT_TRUE(ptr);
        }
        cut = nullptr;
        {
            auto ptr = weak_ptr.lock();
            ASSERT_FALSE(ptr);
        }
    }

    TEST_F( SwinePtrTest, testSwinePtrCanTakeWeakAsRawPtr )
    {
        auto cut = make_swine<TestClass>();
        cut.take_weak();
        // raw pointer can be taken as weak
        auto weak_ptr = cut.get();
        {
            auto ptr = swine_ptr<TestClass>::lock_weak(weak_ptr);
            ASSERT_TRUE(ptr);
        }
        cut = nullptr;
        {
            auto ptr = swine_ptr<TestClass>::lock_weak(weak_ptr);
            ASSERT_FALSE(ptr);
        }
        swine_ptr<TestClass>::release_weak(weak_ptr);
    }

}