// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/serialization/compose.hpp>
#include <utils/TestBase.hpp>

using namespace db0;
using namespace std;

namespace tests 

{

    class ComposeTests: public MemspaceTestBase
    {
    };

    TEST_F( ComposeTests , testComposeMeasure )
    {
        const char *str_value = "some-string-value";
        auto size = db0::o_compose<int, o_string>::measure(123, str_value);
        ASSERT_EQ(size, sizeof(int) + o_string::measure(str_value));
    }

    TEST_F( ComposeTests , testComposeIntAndString )
    {        
        const char *str_value = "some-string-value";
        auto memspace = getMemspace();
        db0::v_object<o_simple<int> > int_obj(memspace, 123);
        db0::v_object<o_string> string_obj(memspace, str_value);
        db0::v_object<db0::o_compose<int, o_string> > cut(memspace, 123, str_value);

        ASSERT_EQ(cut->m_first, *int_obj.getData());
        ASSERT_EQ(cut->second().toString(), string_obj->toString());
        ASSERT_EQ(cut->sizeOf(), int_obj->sizeOf() + string_obj->sizeOf());
    }
    
}