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

        bool operator!=(const o_typed_item & other) const{
            return !(*this == other);
        }

        bool operator<(const o_typed_item & other) const{
            return m_value.m_store < other.m_value.m_store;
        }
    };

     struct [[gnu::packed]] TypedItem_Ptr
    {
        std::uint64_t m_add;
    };

    template <typename ValueT>
    union [[gnu::packed]] ValueT_Address
    {
        TypedItem_Ptr as_ptr;
        ValueT as_value;
        
        ValueT_Address(){
            as_ptr.m_add = 0;
        };
        ValueT_Address(std::uint64_t addrs){
            as_ptr.m_add = addrs;
        }

        operator std::uint64_t() const{
            return as_ptr.m_add;
        }
        operator bool () const{
            return as_ptr.m_add != 0;
        }
        
        // binary compare
        bool operator!=(const ValueT_Address &other) const{
            return memcmp(this, &other, sizeof(ValueT_Address)) != 0;
        }
    };
    
       template <typename AddressT, typename IndexT>
    struct [[gnu::packed]] TypedIndex
    {
        AddressT m_index_address;
        bindex::type m_type;
        TypedIndex()
            : m_index_address(0), m_type(bindex::empty)
        {
        }


        TypedIndex(AddressT index_addres, bindex::type type)
            : m_index_address(index_addres), m_type(type)
        {
        }

        IndexT getIndex(Memspace & memspace){
            return IndexT(memspace, m_index_address, m_type);
        }
        
    };

     template<typename ItemT, typename AddressT>
    class CollectionIndex : public MorphingBIndex<ItemT, AddressT> {
        using super_t = MorphingBIndex<ItemT, AddressT>;
    public:

        CollectionIndex() = default;

        CollectionIndex(Memspace &memspace)
            : MorphingBIndex<ItemT, AddressT>(memspace)
        {
        }
        
        CollectionIndex(Memspace &memspace, const ItemT & value)
            : MorphingBIndex<ItemT, AddressT>(memspace, value)
        {
        }
        
        CollectionIndex(Memspace& memspace, AddressT addr, bindex::type type)
            : MorphingBIndex<ItemT, AddressT>(memspace, addr, type)
        {
        }
    };
    

}