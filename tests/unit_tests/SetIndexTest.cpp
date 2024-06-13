#include <gtest/gtest.h>
#include <set>
#include <utils/TestBase.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <tests/utils/utils.hpp>

namespace tests

{

    using namespace db0;
    using namespace db0::object_model;
    
    class SetIndexTest: public MemspaceTestBase
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override {
            db0::tests::drop(file_name);
            m_workspace.open(prefix_name,  AccessType::READ_WRITE, false);
        }

        virtual void TearDown() override {            
            m_workspace.close();
            db0::tests::drop(file_name);
        }

    protected:
        Workspace m_workspace;
    };

    TEST_F( SetIndexTest , testMorphing_example )
    {
        auto memspace = getMemspace();

        auto fixture = m_workspace.getMutableFixture();
        o_typed_item item(StorageClass::INT64, Value(1));
        SetIndex bindex(memspace,item);

        auto it = bindex.beginJoin(1);
        ASSERT_TRUE((*it).m_value == 1);

        auto typed_index = TypedIndex(bindex.getAddress(), bindex.getIndexType());

        auto index2 = typed_index.getIndex(memspace);
        auto it2 = index2.beginJoin(1);
        std::cerr << "IT 2 value " << (*it2).m_value.m_store << std::endl;
        ASSERT_TRUE((*it2).m_value == 1);

        
    }

} 
