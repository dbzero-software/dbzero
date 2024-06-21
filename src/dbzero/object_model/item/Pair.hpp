#pragma once
#include "Item.hpp"

namespace db0::object_model
{

    struct [[gnu::packed]] o_pair_item: public db0::o_fixed<o_pair_item>
    {
        o_typed_item m_first;
        o_typed_item m_second;

        o_pair_item() = default;

        inline o_pair_item(o_typed_item first, o_typed_item second)
            : m_first(first)
            , m_second(second)
        {
        }

        bool operator==(const o_pair_item & other) const{
            return m_first == other.m_first && m_second == other.m_second;
        }

        bool operator!=(const o_pair_item & other) const{
            return !(*this == other);
        }

        bool operator<(const o_pair_item & other) const{
            return m_first < other.m_first;
        }
    };


}