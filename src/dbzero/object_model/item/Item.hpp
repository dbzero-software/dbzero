#pragma once
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model

{


    struct [[gnu::packed]] o_typed_item: public db0::o_fixed<o_typed_item>
    {
        StorageClass m_storage_class;
        Value m_value;

        o_typed_item() = default;

        inline o_typed_item(StorageClass storage_class, Value value)
            : m_storage_class(storage_class)
            , m_value(value)
        {
        }

        bool operator==(const o_typed_item & other) const{
            return m_storage_class == other.m_storage_class && m_value == other.m_value;
        }
    };

    

}