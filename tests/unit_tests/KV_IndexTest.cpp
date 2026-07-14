// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <set>
#include <utils/TestBase.hpp>
#include <dbzero/object_model/object/KV_Index.hpp>

namespace tests

{

    using namespace db0;
    using namespace db0::object_model;
    
    class KV_IndexTest: public MemspaceTestBase
    {
    public:
    };

    TEST_F( KV_IndexTest , testKV_IndexCanBeCreatedWithSingleElement )
    {
        auto memspace = getMemspace();
        auto value = XValue(1, StorageClass::DATE, 12345);        
        KV_Index cut(memspace, value);
        ASSERT_EQ(cut.size(), 1);
        ASSERT_TRUE(cut.contains(value));
        ASSERT_FALSE(cut.contains(XValue(2, StorageClass::DATE, 12346)));
    }

} 
