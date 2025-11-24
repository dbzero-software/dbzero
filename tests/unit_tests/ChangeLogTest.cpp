#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <thread>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class ChangeLogTest: public testing::Test
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
    
    TEST_F( ChangeLogTest , testChangeLogMeasureAndSizeOf )
    {
        std::vector<char> buf;
        // create default change log (i.e. null header)
        using ChangeLogT = o_change_log<>;

        // Test empty
        {            
            auto measured_size = ChangeLogT::measure(ChangeLogData());
            buf.resize(measured_size);
            ChangeLogT::__new(buf.data(), ChangeLogData());
            auto safe_size = ChangeLogT::safeSizeOf(buf.data());
            ASSERT_EQ(measured_size, safe_size);
        }

        // Test RLE compressed
        {
            std::vector<std::uint64_t> change_log = { 1, 2, 3, 4, 5 };
            ChangeLogData data(std::move(change_log), true, false, false);
            auto measured_size = ChangeLogT::measure(data);
            buf.resize(measured_size);
            ChangeLogT::__new(buf.data(), data);
            auto safe_size = ChangeLogT::safeSizeOf(buf.data());            
        }

        // Test uncompressed
        {
            std::vector<std::uint64_t> change_log = { 3, 4, 8 };
            ChangeLogData data(std::move(change_log), false, false, false);
            auto measured_size = ChangeLogT::measure(data);
            buf.resize(measured_size);
            ChangeLogT::__new(buf.data(), data);
            auto safe_size = ChangeLogT::safeSizeOf(buf.data());
            ASSERT_EQ(measured_size, safe_size);
        }                
    }
    
}
