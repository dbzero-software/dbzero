#include "mu_store.hpp"
#include <cassert>

namespace db0

{

    o_mu_store::o_mu_store(std::size_t max_bytes)
        : m_capacity(max_bytes)
    {
        assert(max_bytes <= 0xFFFF);
    }

    std::size_t o_mu_store::measure(std::size_t max_bytes) 
    {
        assert(max_bytes <= 0xFFFF);
        return max_bytes;
    }

    std::size_t o_mu_store::sizeOf() const {
        return m_capacity;
    }
    
}