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
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");             
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        ASSERT_ANY_THROW( { cut.getType(0); });
    }

    TEST_F( SchemaTest , testSchemaAddPrimaryTypeId )
    {   
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(0, TypeId::STRING);
        ASSERT_EQ(cut.getType(0).first, TypeId::STRING);
    }
    
    TEST_F( SchemaTest , testSchemaAddSecondaryTypeId )
    {
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::INTEGER);
        cut.add(0, TypeId::STRING);
        ASSERT_EQ(cut.getType(0).first, TypeId::STRING);
        ASSERT_EQ(cut.getType(0).second, TypeId::INTEGER);
    }

    TEST_F( SchemaTest , testSchemaAddMoreTypeIds )
    {   
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");             
        std::vector<TypeId> typeIds = {
            TypeId::STRING, TypeId::INTEGER, TypeId::DATETIME,  TypeId::FLOAT,
            TypeId::DATETIME, TypeId::INTEGER, TypeId::FLOAT, TypeId::INTEGER, TypeId::FLOAT, 
            TypeId::FLOAT
        };
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        for (auto typeId : typeIds) {
            cut.add(0, typeId);
        }

        ASSERT_EQ(cut.getAllTypes(0), std::vector<TypeId>({
            TypeId::FLOAT, TypeId::INTEGER, TypeId::DATETIME, TypeId::STRING
        }));
    }

    TEST_F( SchemaTest , testSchemaRemoveWithoutFlush )
    {
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::INTEGER);
        cut.remove(0, TypeId::STRING);
        cut.remove(0, TypeId::STRING);
        ASSERT_EQ(cut.getType(0).first, TypeId::INTEGER);
        ASSERT_EQ(cut.getType(0).second, TypeId::UNKNOWN);        
    }

    TEST_F( SchemaTest , testSchemaAddRemoveMultipleFieldIDs )
    {
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        std::vector<std::pair<unsigned int, TypeId>> fieldIds = {
            {0, TypeId::STRING}, {1, TypeId::INTEGER}, {4, TypeId::DATETIME},
            {1, TypeId::FLOAT}, {0, TypeId::STRING}, {1, TypeId::INTEGER},
            {4, TypeId::INTEGER}, {0, TypeId::FLOAT}, {0, TypeId::DATETIME}
        };

        for (const auto &type_info : fieldIds) {
            cut.add(type_info.first, type_info.second);
        }
        ASSERT_NE(cut.getType(0).first, TypeId::UNKNOWN);
        ASSERT_EQ(cut.getType(1).first, TypeId::INTEGER);
        ASSERT_EQ(cut.getType(1).second, TypeId::FLOAT);
        ASSERT_NE(cut.getType(4).first, TypeId::UNKNOWN);
    }

    TEST_F( SchemaTest , testSchemaEvolutionUpdatePrimary )
    {
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.flush();
        cut.add(0, TypeId::STRING);
        ASSERT_EQ(cut.getType(0).first, TypeId::STRING);
        ASSERT_EQ(cut.getType(0).second, TypeId::UNKNOWN);
    }

    TEST_F( SchemaTest , testSchemaEvolutionAddSecondary )
    {
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.flush();
        cut.add(0, TypeId::INTEGER);
        ASSERT_EQ(cut.getType(0).first, TypeId::STRING);
        ASSERT_EQ(cut.getType(0).second, TypeId::INTEGER);
    }

    TEST_F( SchemaTest , testSchemaEvolutionUpdateSecondary )
    {
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.flush();
        cut.add(0, TypeId::INTEGER);
        cut.flush();
        cut.add(0, TypeId::INTEGER);
        ASSERT_EQ(cut.getType(0).first, TypeId::STRING);
        ASSERT_EQ(cut.getType(0).second, TypeId::INTEGER);
    }
    
    TEST_F( SchemaTest , testSchemaEvolutionAddExtra )
    {
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        auto get_total = []() -> unsigned int {
            return 0;
        };

        Schema cut(memspace, get_total);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::INTEGER);
        cut.add(0, TypeId::INTEGER);
        cut.flush();
        cut.add(0, TypeId::FLOAT);
        ASSERT_EQ(cut.getAllTypes(0), std::vector<TypeId>({
            TypeId::STRING, TypeId::INTEGER, TypeId::FLOAT
        }));
    }

    TEST_F( SchemaTest , testSchemaEvolutionUpdateExtra )
    {
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1"); 
        auto get_total = []() -> unsigned int {
            return 0;
        };
             
        Schema cut(memspace, get_total);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::STRING);
        cut.add(0, TypeId::INTEGER);
        cut.add(0, TypeId::INTEGER);
        cut.add(0, TypeId::INTEGER);
        cut.add(0, TypeId::FLOAT);
        cut.flush();
        cut.add(0, TypeId::FLOAT);
        cut.add(0, TypeId::DATETIME);
        ASSERT_EQ(cut.getAllTypes(0), std::vector<TypeId>({
            TypeId::STRING, TypeId::INTEGER, TypeId::FLOAT, TypeId::DATETIME
        }));
    }
    
    TEST_F( SchemaTest , testSchemaEvolutionSwapPrimary )
    {
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        unsigned int total = 0;
        auto get_total = [&]() -> unsigned int {
            return total;
        };

        Schema cut(memspace, get_total);
        cut.add(0, TypeId::NONE);
        ++total;
        cut.add(0, TypeId::NONE);
        ++total;
        cut.add(0, TypeId::INTEGER);
        ++total;
        cut.flush();
        cut.add(0, TypeId::INTEGER);
        ++total;
        cut.add(0, TypeId::INTEGER);
        ++total;
        ASSERT_EQ(cut.getType(0).first, TypeId::INTEGER);
    }

}
