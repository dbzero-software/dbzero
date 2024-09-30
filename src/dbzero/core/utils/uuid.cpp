#include "uuid.hpp"
#include <random>

namespace db0

{
    
    std::uint64_t make_UUID()
    {
        // FIXME: log
        return 1234u;
        
        /*
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<std::uint64_t> dis;        
        return dis(gen);
        */
    }
        
}