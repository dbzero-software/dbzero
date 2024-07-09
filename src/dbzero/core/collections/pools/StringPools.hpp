#pragma once

#include <dbzero/core/serialization/string.hpp>
#include "LimitedPool.hpp"
#include "RC_LimitedPool.hpp"
    
namespace db0::pools

{
    
    class RC_LimitedStringPool: public RC_LimitedPool<o_string, o_string::comp_t, std::uint32_t>
    {
    public:
        using super_t = RC_LimitedPool<o_string, o_string::comp_t, std::uint32_t>;
        
        RC_LimitedStringPool(const Memspace &pool_memspace, Memspace &);
        RC_LimitedStringPool(const Memspace &pool_memspace, mptr);

        /**
         * Convenience pointer/element ID type
        */
        struct [[gnu::packed]] PtrT
        {
            std::uint32_t m_value = 0;
            PtrT() = default;
            inline PtrT(std::uint32_t value) : m_value(value) {}

            inline operator bool() const 
            {
                return m_value != 0;
            }
        };
        
        PtrT add(const char *);
        PtrT add(const std::string &);
        // find existing or crate a new tag (if create set to true)
        PtrT get(const char *, bool create);
        PtrT get(const std::string &, bool create);

        std::string fetch(PtrT) const;
        
        /**
         * Convert pointer/element ID to actual memspace address
        */
        std::uint64_t toAddress(PtrT) const;
    };
    
}

namespace db0

{
    
    // limited pool string pointer type
    using LP_String = db0::pools::RC_LimitedStringPool::PtrT;

}
