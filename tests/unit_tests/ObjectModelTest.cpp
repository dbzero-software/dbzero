#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/object_model/class.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;
    
namespace tests

{
    
    class ObjectModelTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override 
        {
            drop(file_name);
        }

        virtual void TearDown() override 
        {
            drop(file_name);
        }
    };
    
    TEST_F( ObjectModelTest , testFixtureCanBeInitializedForTheObjectModel )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        // make sure all ObjectModel requisites are present
        ASSERT_NO_THROW ( fixture->get<ClassFactory>() );
        workspace.close();
    }

} 