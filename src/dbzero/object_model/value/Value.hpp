#pragma once

#include <cstdint>
#include <dbzero/core/metaprog/binary_cast.hpp>

namespace db0::object_model

{

    struct [[gnu::packed]] Value
    {
        Value() = default;
        inline Value(std::uint64_t value)
            : m_store(value)
        {
        }

        template <typename T> inline T cast() const
        {
            return db0::binary_cast<T, std::uint64_t>()(m_store);
        }

        bool operator==(const Value &other) const;
        
        std::uint64_t m_store = 0;
    };
       
}