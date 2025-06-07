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
        ASSERT_NO_THROW( { auto cut = Schema(memspace); } );        
    }

    TEST_F( SchemaTest , testSchemaExceptionWhenUnknownFieldId)
    {   
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");             
        Schema cut(memspace);
        ASSERT_ANY_THROW( { cut.getType(0); });
    }
    
    TEST_F( SchemaTest , testSchemaAddPrimaryTypeId)
    {   
        using TypeId = Schema::TypeId;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");             
        Schema cut(memspace);
        cut.add(0, TypeId::STRING);
        ASSERT_EQ(cut.getType(0).first, TypeId::STRING);
    }

}
