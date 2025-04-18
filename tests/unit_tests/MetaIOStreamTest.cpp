#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/core/storage/MetaIOStream.hpp>
#include <thread>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class MetaIOStreamTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override {            
            drop(file_name);
        }

        virtual void TearDown() override {            
            drop(file_name);
        }
    };
    
    TEST_F( MetaIOStreamTest , testMetaIOStreamCanBeCreated )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&]() { 
            return file.size(); 
        };
        
        MetaIOStream cut(file, 0, 4096, tail_function, {});
        ASSERT_TRUE(cut.eos());
        cut.close();        
    }
    
}