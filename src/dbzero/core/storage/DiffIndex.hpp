// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <utility>
#include <string>
#include "SparseIndex.hpp"
#include <dbzero/core/serialization/packed_int_pair.hpp>
#include <dbzero/core/serialization/packed_array.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "StorageFlags.hpp"

namespace db0

{


    // DiffIndex is a specialization of SparseIndexBase for storing
    // references do diff-pages
    // Each element consists of: page num (logical) / state num + physical page num + sequence of encoded: page num / state num
    // One element can encode variable number of state updates (transactions)
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR DI_Item: public SI_Item
    {
        using CompT = SI_ItemCompT;
        using EqualT = SI_ItemEqualT;
        using DiffBufT = std::array<unsigned char, 20>;
        using DiffArrayT = o_packed_array<o_packed_int_pair<std::uint32_t, std::uint64_t>, std::uint8_t, sizeof(DiffBufT)>;

        // the diff-data buffer to hold o_packed_array
        DiffBufT m_diff_data;

        DI_Item() {
            DiffArrayT::__new(m_diff_data.data());
        }

        DI_Item(std::uint64_t page_num, std::uint32_t state_num)
            : SI_Item(page_num, state_num)            
        {                
            DiffArrayT::__new(m_diff_data.data());
        }

        DI_Item(std::uint64_t page_num, std::uint32_t state_num, std::uint64_t storage_page_num)
            : SI_Item(page_num, state_num, storage_page_num)            
        {                
            DiffArrayT::__new(m_diff_data.data());
        }

        DI_Item(const SI_Item &, const DiffBufT &);
        
        // the iterator over diff-items
        class ConstIterator
        {
        public:
            ConstIterator() = default;
            ConstIterator(std::uint32_t base_state_num, std::uint64_t base_page_num,
                typename DiffArrayT::ConstIterator current, typename DiffArrayT::ConstIterator end);
            
            bool next(std::uint32_t &state_num, std::uint64_t &storage_page_num);
            // retrieve the next state number only
            bool next(std::uint32_t &state_num);

            // invalidate the iterator
            void reset();

            operator bool () const {
                return m_current;
            }
            
        private:
            // base page num & state num are required for decoding
            std::uint32_t m_base_state_num = 0;
            std::uint64_t m_base_storage_page_num = 0;
            typename DiffArrayT::ConstIterator m_current;
            typename DiffArrayT::ConstIterator m_end;            
        };
                
        ConstIterator beginDiff() const;

        // @return the largest state number such that state <= state_num
        std::uint32_t findLower(std::uint32_t state_num) const;
        
        // @return the smallest state number such that state >= state_num
        std::uint32_t findUpper(std::uint32_t state_num) const;
    };
DB0_PACKED_END    

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR DI_CompressedItem: public SI_CompressedItem
    {
        using CompT = SI_CompressedItemCompT;
        using EqualT = SI_CompressedItemEqualT;
        using DiffBufT = typename DI_Item::DiffBufT;
        using DiffArrayT = typename DI_Item::DiffArrayT;
        
        // the diff-data buffer to hold o_packed_array
        DiffBufT m_diff_data;
        
        // construct DI-compressed item relative to the specific page number - i.e. first_page_num
        DI_CompressedItem(std::uint32_t first_page_num, const DI_Item &);
        // construct DI-compressed item for comparison purposes (incomplete)
        DI_CompressedItem(std::uint32_t first_page_num, std::uint64_t page_num, std::uint32_t state_num);
        
        // uncompress relative to a specific page number
        DI_Item uncompress(std::uint32_t first_page_num) const;

        // check if the item can be appended to diff-data
        // and prepare the appended values (i.e. make them relative)
        bool beginAppend(std::uint32_t &state_num, std::uint64_t &storage_page_num) const;
        // append relative values
        void append(std::uint32_t state_num, std::uint64_t storage_page_num);        
    };
DB0_PACKED_END    

    class DiffIndex: protected SparseIndexBase<DI_Item, DI_CompressedItem>
    {
    public:
        using super_t = SparseIndexBase<DI_Item, DI_CompressedItem>;        
        using PageNumT = typename super_t::PageNumT;
        using StateNumT = typename super_t::StateNumT;
        
        DiffIndex(std::size_t node_size, std::vector<std::uint64_t> *change_log_ptr = nullptr);
        DiffIndex(DRAM_Pair, AccessType, Address, std::vector<std::uint64_t> *change_log_ptr = nullptr, StorageFlags = {});
        
        struct tag_create {};
        DiffIndex(tag_create, DRAM_Pair, std::vector<std::uint64_t> *change_log_ptr = nullptr);
        
        // Either insert into a new item or extend the existing one
        // @param overflow flag indicating if the stored page has 
        // been written witn an "overflow" - in which case actually 2 DPs were written
        void insert(PageNumT page_num, StateNumT state_num, PageNumT storage_page_num, bool overflow = false);
        
        bool empty() const;
        std::size_t size() const;
        
        // Find mutation of page_num where state >= state_num
        DI_Item findUpper(PageNumT page_num, StateNumT state_num) const;
        // Find mutation ID of page_num where state <= state_num
        StateNumT findLower(PageNumT page_num, StateNumT state_num) const;
        
        std::optional<PageNumT> getNextStoragePageNum() const;
        
        StateNumT getMaxStateNum() const;

        Address getIndexAddress() const;

        void commit();
        
        void refresh();
    };

}
