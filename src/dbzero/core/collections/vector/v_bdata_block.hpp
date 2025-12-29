// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cstddef>
#include <dbzero/core/metaprog/tuple_utils.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

    /**
     * b_class size class (additional shift)
     */
DB0_PACKED_BEGIN
    template <typename ItemT, std::size_t b_class> struct DB0_PACKED_ATTR o_block_data
        : public o_base<o_block_data<ItemT, b_class>, 0, false >
    {
    protected:
        using super_t = o_base<o_block_data<ItemT, b_class>, 0, false >;
        friend super_t;
        
        o_block_data(std::uint32_t page_size_hint) {
            auto count = 0x1u << shift(page_size_hint);
            // initialize with default values
            std::uninitialized_fill(this->getData(), this->getData() + count, ItemT());            
        }

    public:
        using iterator = ItemT*;
        using const_iterator = const ItemT*;
        
        ItemT *getData() {
            return reinterpret_cast<ItemT *>(this);
        }
        
        const ItemT *getData() const {
            return reinterpret_cast<const ItemT *>(this);            
        }  

        static std::size_t measure(std::uint32_t page_size_hint) {
            return sizeof(ItemT) << shift(page_size_hint);
        } 

        template <class buf_t> static std::size_t safeSizeOf(buf_t buf) {
            throw std::runtime_error("o_block_data::safeSizeOf member not available");
        }

        inline const ItemT &getItem(std::size_t index) const {
            return getData()[index];
        }

        ItemT &modifyItem(std::size_t index) {
            return getData()[index];
        }

        static std::uint32_t shift(std::uint32_t page_size_hint)
        {
            std::uint32_t result = 0;
            std::size_t size = sizeof(ItemT);
            while ((size << 1) <= page_size_hint) {
                ++result;
                size <<= 1;
            }
            assert(result >= b_class);
            return (result - b_class);
        }

        static std::uint32_t mask(std::uint32_t page_size_hint)
        {
            std::uint32_t result = 0;
            std::uint32_t count = shift(page_size_hint);
            assert(count > 0);
            while (count > 0) {
                result <<= 1;
                result |= 0x01;
                --count;
            }
            return result;
        }
        
        // Pull begin (first item) iterator for write
        iterator begin() {
            return getData();
        }
        
        // Pull begin (first item) iterator for read
        const_iterator begin() const {
            return getData();
        }
    };
DB0_PACKED_END

    template <typename ItemT, std::size_t b_class> class v_bdata_block
        : public v_object<o_block_data<ItemT, b_class> > 
    {
    public :
        using super_t = v_object<o_block_data<ItemT, b_class> >;
        using ptr_t = typename super_t::ptr_t;
        using iterator = typename o_block_data<ItemT, b_class>::iterator;
        using const_iterator = typename o_block_data<ItemT, b_class>::const_iterator;

        v_bdata_block() = default;

        v_bdata_block(Memspace &memspace)
            : super_t(memspace, memspace.getPageSize())
        {
        }

        v_bdata_block(mptr ptr)
            : super_t(ptr)
        {            
        }
    };

    typedef void *(*NewVBDataBlockPtr)(Memspace &);
    typedef void *(*NewExistingVBDataBlockPtr)(mptr);
    typedef std::size_t (*GetBClassPtr)();

    struct DataBlockInterface 
    {
        // create new instance of v_bdata_block type
        NewVBDataBlockPtr createNewDataBlock;
        NewExistingVBDataBlockPtr createNewExistingDataBlock;
        GetBClassPtr getBClass;
    };
    
    template <typename ItemT, std::size_t b_class> void *createNewDataBlock(Memspace &memspace) 
    {
        return new v_bdata_block<ItemT, b_class>(memspace);
    }

    template <std::size_t b_class> size_t getBClass()
    {
        return b_class;
    }

    template <typename ItemT, std::size_t b_class> void *createNewExistingDataBlock(mptr ptr)
    {
        using data_type = o_block_data<ItemT, b_class>;
        return new v_bdata_block<ItemT, b_class>(ptr);
    }

    template <typename ItemT, std::size_t SIZE> class DataBlockInterfaceArray
        : public std::array<DataBlockInterface, SIZE> 
    {

        template <int N> DataBlockInterface getInterface() const
        {
            DataBlockInterface result;
            result.createNewDataBlock = createNewDataBlock<ItemT, N>;
            result.createNewExistingDataBlock = createNewExistingDataBlock<ItemT, N>;
            result.getBClass = getBClass<N>;
            return result;
        }

    public:
        template <int... Is> DataBlockInterfaceArray(db0::metaprog::seq<Is...>)
            : std::array<DataBlockInterface, SIZE> { (getInterface<Is>())... }
        {
        }

        DataBlockInterfaceArray()
            : DataBlockInterfaceArray(db0::metaprog::gen_seq<SIZE>())
        {
        }
    };

} 
