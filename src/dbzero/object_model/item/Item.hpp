// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_typed_item: public db0::o_fixed<o_typed_item>
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

        bool operator!=(const o_typed_item & other) const{
            return !(*this == other);
        }

        bool operator<(const o_typed_item & other) const{
            return m_value.m_store < other.m_value.m_store;
        }
    };
DB0_PACKED_END

    template <typename ValueT>
DB0_PACKED_BEGIN
    union DB0_PACKED_ATTR ValueT_Address
    {
        std::uint64_t as_ptr = 0;
        ValueT as_value;
        
        ValueT_Address(){};

        ValueT_Address(Address address)
            : as_ptr(address.getValue())
        {
        }

        operator Address() const {
            return Address::fromValue(as_ptr);
        }
        
        operator bool () const {
            return as_ptr != 0;
        }
        
        // binary compare
        bool operator!=(const ValueT_Address &other) const {
            return memcmp(this, &other, sizeof(ValueT_Address)) != 0;
        }
    };
DB0_PACKED_END
    
    template <typename AddressT, typename IndexT>
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR TypedIndexAddr
    {
        AddressT m_index_address = {};
        bindex::type m_type = bindex::type::empty;

        TypedIndexAddr() = default;

        TypedIndexAddr(const IndexT &index)
            : m_index_address(index.getAddress())
            , m_type(index.getIndexType())
        {
        }

        TypedIndexAddr(AddressT index_addres, bindex::type type)
            : m_index_address(index_addres)
            , m_type(type)
        {
        }

        IndexT getIndex(Memspace &memspace) const {
            return { memspace, m_index_address, m_type };
        }        
    };
DB0_PACKED_END
    
    template<typename ItemT, typename AddressT>
    class CollectionIndex : public MorphingBIndex<ItemT, AddressT> 
    {
        using super_t = MorphingBIndex<ItemT, AddressT>;
    public:

        CollectionIndex() = default;

        CollectionIndex(Memspace &memspace)
            : super_t(memspace)
        {
        }
        
        CollectionIndex(Memspace &memspace, const ItemT & value)
            : super_t(memspace, value)
        {
        }

        CollectionIndex(const CollectionIndex & index)
            : super_t(index)
        {
        }
        
        CollectionIndex(Memspace& memspace, AddressT addr, bindex::type type)
            : super_t(memspace, addr, type)
        {
        }
    };
    
}