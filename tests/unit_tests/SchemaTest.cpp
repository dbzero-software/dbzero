// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/object_model/class/Schema.hpp>

using namespace std;
using namespace db0;
using namespace db0::object_model;
using namespace db0::tests;
    
namespace tests

{
    
    class SchemaTest: public MemspaceTestBase
    {
    };
    
    TEST_F( SchemaTest , testSchemaInstanceCreation )
    {   
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");             
        auto get_total = []() -> unsigned int {
            return 0;
        };

        ASSERT_NO_THROW( { auto cut = Schema(memspace, get_total); } );        
    }

    TEST_F( SchemaTest , testSchemaExceptionWhenUnknownFieldId )
    {           
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");             
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        ASSERT_ANY_THROW( { cut.getType(FieldID::fromIndex(0)); });
    }

    TEST_F( SchemaTest , testSchemaAddPrimaryTypeId )
    {           
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).first, SchemaTypeId::STRING);
    }
    
    TEST_F( SchemaTest , testSchemaAddSecondaryTypeId )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::INT);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).first, SchemaTypeId::STRING);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).second, SchemaTypeId::INT);
    }

    TEST_F( SchemaTest , testSchemaAddMoreTypeIds )
    {       
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        std::vector<SchemaTypeId> typeIds = {
            SchemaTypeId::STRING, SchemaTypeId::INT, SchemaTypeId::DATETIME,  SchemaTypeId::FLOAT,
            SchemaTypeId::DATETIME, SchemaTypeId::INT, SchemaTypeId::FLOAT, SchemaTypeId::INT, SchemaTypeId::FLOAT,
            SchemaTypeId::FLOAT
        };
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        for (auto typeId : typeIds) {
            cut.add(FieldID::fromIndex(0), typeId);
        }

        ASSERT_EQ(cut.getAllTypes(FieldID::fromIndex(0)), std::vector<SchemaTypeId>({
            SchemaTypeId::FLOAT, SchemaTypeId::INT, SchemaTypeId::DATETIME, SchemaTypeId::STRING
        }));
    }

    TEST_F( SchemaTest , testSchemaRemoveWithoutFlush )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::INT);
        cut.remove(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.remove(FieldID::fromIndex(0), SchemaTypeId::STRING);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).first, SchemaTypeId::INT);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).second, SchemaTypeId::UNDEFINED);
    }

    TEST_F( SchemaTest , testSchemaAddRemoveMultipleFieldIDs )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        std::vector<std::pair<unsigned int, SchemaTypeId>> fieldIds = {
            {0, SchemaTypeId::STRING}, {1, SchemaTypeId::INT}, {4, SchemaTypeId::DATETIME},
            {1, SchemaTypeId::FLOAT}, {0, SchemaTypeId::STRING}, {1, SchemaTypeId::INT},
            {4, SchemaTypeId::INT}, {0, SchemaTypeId::FLOAT}, {0, SchemaTypeId::DATETIME}
        };

        for (const auto &type_info : fieldIds) {
            cut.add(FieldID::fromIndex(type_info.first), type_info.second);
        }
        ASSERT_NE(cut.getType(FieldID::fromIndex(0)).first, SchemaTypeId::UNDEFINED);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(1)).first, SchemaTypeId::INT);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(1)).second, SchemaTypeId::FLOAT);
        ASSERT_NE(cut.getType(FieldID::fromIndex(4)).first, SchemaTypeId::UNDEFINED);
    }

    TEST_F( SchemaTest , testSchemaEvolutionUpdatePrimary )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.flush();
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).first, SchemaTypeId::STRING);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).second, SchemaTypeId::UNDEFINED);
    }
    
    TEST_F( SchemaTest , testSchemaEvolutionAddSecondary )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.flush();
        cut.add(FieldID::fromIndex(0), SchemaTypeId::INT);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).first, SchemaTypeId::STRING);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).second, SchemaTypeId::INT);
    }
    
    TEST_F( SchemaTest , testSchemaEvolutionUpdateSecondary )
    {       
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.add(FieldID::fromIndex(0), SchemaTypeId::STRING);
        cut.flush();
        cut.add(FieldID::fromIndex(0), SchemaTypeId::INT);
        cut.flush();
        cut.add(FieldID::fromIndex(0), SchemaTypeId::INT);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).first, SchemaTypeId::STRING);
        ASSERT_EQ(cut.getType(FieldID::fromIndex(0)).second, SchemaTypeId::INT);
    }
    
    TEST_F( SchemaTest , testSchemaEvolutionAddExtra )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        auto id0 = FieldID::fromIndex(0);
        cut.add(id0, SchemaTypeId::STRING);
        cut.add(id0, SchemaTypeId::STRING);
        cut.add(id0, SchemaTypeId::STRING);
        cut.add(id0, SchemaTypeId::INT);
        cut.add(id0, SchemaTypeId::INT);
        cut.flush();
        cut.add(id0, SchemaTypeId::FLOAT);
        ASSERT_EQ(cut.getAllTypes(id0), std::vector<SchemaTypeId>({
            SchemaTypeId::STRING, SchemaTypeId::INT, SchemaTypeId::FLOAT
        }));
    }

    TEST_F( SchemaTest , testSchemaEvolutionUpdateExtra )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1"); 
        auto get_total = []() -> unsigned int {
            return 0;
        };
             
        Schema cut(memspace, get_total);
        auto id0 = FieldID::fromIndex(0);
        cut.add(id0, SchemaTypeId::STRING);
        cut.add(id0, SchemaTypeId::STRING);
        cut.add(id0, SchemaTypeId::STRING);
        cut.add(id0, SchemaTypeId::STRING);
        cut.add(id0, SchemaTypeId::INT);
        cut.add(id0, SchemaTypeId::INT);
        cut.add(id0, SchemaTypeId::INT);
        cut.add(id0, SchemaTypeId::FLOAT);
        cut.flush();
        cut.add(id0, SchemaTypeId::FLOAT);
        cut.add(id0, SchemaTypeId::DATETIME);
        ASSERT_EQ(cut.getAllTypes(id0), std::vector<SchemaTypeId>({
            SchemaTypeId::STRING, SchemaTypeId::INT, SchemaTypeId::FLOAT, SchemaTypeId::DATETIME
        }));
    }
    
    TEST_F( SchemaTest , testSchemaEvolutionSwapPrimary )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        unsigned int total = 0;
        auto get_total = [&]() -> unsigned int {
            return total;
        };

        Schema cut(memspace, get_total);
        auto id0 = FieldID::fromIndex(0);
        cut.add(id0, SchemaTypeId::NONE);
        ++total;
        cut.add(id0, SchemaTypeId::NONE);
        ++total;
        cut.add(id0, SchemaTypeId::INT);
        ++total;
        cut.flush();
        cut.add(id0, SchemaTypeId::INT);
        ++total;
        cut.add(id0, SchemaTypeId::INT);
        ++total;
        ASSERT_EQ(cut.getType(id0).first, SchemaTypeId::INT);
    }

    TEST_F( SchemaTest , testSchemaEvolutionSwapSecondary )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        unsigned int total = 0;
        auto get_total = [&]() -> unsigned int {
            return total;
        };

        Schema cut(memspace, get_total);
        auto id0 = FieldID::fromIndex(0);
        cut.add(id0, SchemaTypeId::NONE);        
        cut.add(id0, SchemaTypeId::NONE);
        cut.add(id0, SchemaTypeId::NONE);
        cut.add(id0, SchemaTypeId::INT);
        total += 4;
        cut.flush();
        cut.add(id0, SchemaTypeId::FLOAT);        
        cut.add(id0, SchemaTypeId::FLOAT);
        total += 2;
        ASSERT_EQ(cut.getType(id0).second, SchemaTypeId::FLOAT);
    }

    TEST_F( SchemaTest , testSchemaEvolutionSwapPrimaryWithExtra )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        unsigned int total = 0;
        auto get_total = [&]() -> unsigned int {
            return total;
        };

        Schema cut(memspace, get_total);
        auto id0 = FieldID::fromIndex(0);
        cut.add(id0, SchemaTypeId::NONE);
        cut.add(id0, SchemaTypeId::NONE);
        cut.add(id0, SchemaTypeId::NONE);
        cut.add(id0, SchemaTypeId::INT);
        total += 4;
        cut.flush();
        cut.add(id0, SchemaTypeId::FLOAT);
        cut.add(id0, SchemaTypeId::FLOAT);
        cut.add(id0, SchemaTypeId::FLOAT);
        cut.add(id0, SchemaTypeId::FLOAT);
        total += 4;
        ASSERT_EQ(cut.getType(id0).first, SchemaTypeId::FLOAT);
        ASSERT_EQ(cut.getType(id0).second, SchemaTypeId::NONE);
    }

    TEST_F( SchemaTest , testSchemaEvolutionSwapPrimaryAndSecondaryWithExtra )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        unsigned int total = 0;
        auto get_total = [&]() -> unsigned int {
            return total;
        };

        Schema cut(memspace, get_total);
        auto id0 = FieldID::fromIndex(0);
        cut.add(id0, SchemaTypeId::NONE);        
        cut.add(id0, SchemaTypeId::NONE);
        cut.add(id0, SchemaTypeId::INT);
        total += 4;
        cut.flush();
        cut.add(id0, SchemaTypeId::FLOAT);
        cut.add(id0, SchemaTypeId::FLOAT);
        cut.remove(id0, SchemaTypeId::NONE);
        cut.remove(id0, SchemaTypeId::INT);
        cut.add(id0, SchemaTypeId::FLOAT);
        cut.add(id0, SchemaTypeId::DATETIME);
        cut.add(id0, SchemaTypeId::DATETIME);
        cut.add(id0, SchemaTypeId::FLOAT);
        total += 2;
        ASSERT_EQ(cut.getType(id0).first, SchemaTypeId::FLOAT);
        ASSERT_EQ(cut.getType(id0).second, SchemaTypeId::DATETIME);
    }

    TEST_F( SchemaTest , testSchemaEvolutionWithRemove )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        unsigned int total = 0;
        auto get_total = [&]() -> unsigned int {
            return total;
        };
        
        Schema cut(memspace, get_total);
        auto id0 = FieldID::fromIndex(0);
        cut.add(id0, SchemaTypeId::INT);
        cut.add(id0, SchemaTypeId::STRING);
        cut.add(id0, SchemaTypeId::STRING);
        total += 3;
        cut.flush();
        cut.remove(id0, SchemaTypeId::STRING);
        cut.remove(id0, SchemaTypeId::STRING);
        total -= 2;
        ASSERT_EQ(cut.getType(id0).first, SchemaTypeId::INT);
    }

    TEST_F( SchemaTest , testSchemaEvolutionIssue1 )
    {        
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        unsigned int total = 0;
        auto get_total = [&]() -> unsigned int {
            return total;
        };
        
        Schema cut(memspace, get_total);
        auto id0 = FieldID::fromIndex(0);
        cut.add(id0, SchemaTypeId::NONE);
        total += 1;
        cut.flush();
        cut.remove(id0, SchemaTypeId::NONE);        
        cut.add(id0, SchemaTypeId::STRING);
        cut.add(id0, SchemaTypeId::INT);
        total += 1;
        cut.flush();
        
        ASSERT_NE(cut.getType(id0).first, SchemaTypeId::NONE);
        ASSERT_NE(cut.getType(id0).second, SchemaTypeId::NONE);
    }
    
}
