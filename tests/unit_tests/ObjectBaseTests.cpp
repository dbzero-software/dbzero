#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/workspace/Workspace.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;

namespace tests

{

    class ObjectBaseTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override {
            drop(file_name);
        }
        
        virtual void TearDown() override {
            drop(file_name);
        }
    };
    
    TEST_F( ObjectBaseTest , testObjectBaseDerivedTypesMakeUniqueAlloc )
    {
        // List is derived from ObjectBase
        using List = db0::object_model::List;

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> buf(sizeof(List));
        auto &list = *List::makeNew((void*)buf.data(), fixture);
        auto addr = list.getAddress();
        // make sure the logical address differs from the physical one
        ASSERT_NE(addr, db0::getPhysicalAddress(addr));
        workspace.close();
    }

}