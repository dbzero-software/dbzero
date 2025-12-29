// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "DiffIndex.hpp"

namespace db0

{
    
    DI_Item::DI_Item(const SI_Item &si_item, const DiffBufT &diff_data)
        : SI_Item(si_item)
        , m_diff_data(diff_data)
    {    
    }
    
    DI_Item::ConstIterator::ConstIterator(std::uint32_t base_state_num, std::uint64_t base_storage_page_num,
        typename DiffArrayT::ConstIterator current, typename DiffArrayT::ConstIterator end)
        : m_base_state_num(base_state_num)
        , m_base_storage_page_num(base_storage_page_num)
        , m_current(current)
        , m_end(end)        
    {
    }

    bool DI_Item::ConstIterator::next(std::uint32_t &state_num, std::uint64_t &storage_page_num)
    {
        assert(m_current);
        if (m_current == m_end) {
            return false;
        }

        // contains relative state number / storage page number
        auto result_pair = (*m_current).value();
        state_num = m_base_state_num + result_pair.first;            
        storage_page_num = m_base_storage_page_num + result_pair.second;
        ++m_current;
        return true;
    }

    bool DI_Item::ConstIterator::next(std::uint32_t &state_num)
    {
        assert(m_current);
        if (m_current == m_end) {
            return false;
        }

        // contains relative state number / storage page number
        auto result_pair = (*m_current).value();
        state_num = m_base_state_num + result_pair.first;        
        ++m_current;
        return true;
    }

    void DI_Item::ConstIterator::reset() {
        m_current = nullptr;
    }

    DI_Item::ConstIterator DI_Item::beginDiff() const
    {
        const auto &diff_array = DiffArrayT::__const_ref(m_diff_data.data());
        return ConstIterator(m_state_num, m_storage_page_num, diff_array.begin(), diff_array.end());
    }
    
    std::uint32_t DI_Item::findLower(std::uint32_t state_num) const
    {
        std::uint32_t result = 0;
        if (m_state_num > state_num) {
            return result;
        }
        result = m_state_num;
        std::uint32_t next_state_num = 0;
        auto it = beginDiff();
        while (result != state_num && it.next(next_state_num)) {
            if (next_state_num > state_num) {
                break;
            }
            result = next_state_num;
        }
        return result;
    }

    std::uint32_t DI_Item::findUpper(std::uint32_t state_num) const
    {        
        if (m_state_num >= state_num) {
            return m_state_num;
        }
        auto it = beginDiff();
        std::uint32_t next_state_num;
        while (it.next(next_state_num)) {
            if (next_state_num >= state_num) {
                return next_state_num;
            }            
        }
        // all elements are less than state_num
        return 0;
    }

    DI_CompressedItem::DI_CompressedItem(std::uint32_t first_page_num, const DI_Item &item)
        : SI_CompressedItem(first_page_num, item)
        , m_diff_data(item.m_diff_data)
    {
    }
    
    DI_CompressedItem::DI_CompressedItem(std::uint32_t first_page_num, std::uint64_t page_num, std::uint32_t state_num)
        : SI_CompressedItem(first_page_num, page_num, state_num)
        // zero-initialize fundamental type
        , m_diff_data {}
    {         
    }
    
    DI_Item DI_CompressedItem::uncompress(std::uint32_t first_page_num) const {
        return DI_Item(SI_CompressedItem::uncompress(first_page_num), this->m_diff_data);
    }
    
    bool DI_CompressedItem::beginAppend(std::uint32_t &state_num, std::uint64_t &storage_page_num) const
    {
        auto base_state_num = SI_CompressedItem::getStateNum();
        auto base_storage_page_num = SI_CompressedItem::getStoragePageNum();
        // state & storage page numbers should be non-decreasing values
        assert(state_num >= base_state_num);
        assert(storage_page_num >= base_storage_page_num);
        if (!DiffArrayT::__const_ref(m_diff_data.data()).
            canEmplaceBack(state_num - base_state_num, storage_page_num - base_storage_page_num)) 
        {
            return false;
        }
        state_num -= base_state_num;
        storage_page_num -= base_storage_page_num;
        return true;
    }
    
    void DI_CompressedItem::append(std::uint32_t state_num, std::uint64_t storage_page_num) {
        DiffArrayT::__ref(m_diff_data.data()).emplaceBack(state_num, storage_page_num);
    }

    DiffIndex::DiffIndex(std::size_t node_size, std::vector<std::uint64_t> *change_log_ptr)
        : SparseIndexBase(node_size, change_log_ptr)
    {
    }
    
    DiffIndex::DiffIndex(DRAM_Pair dram_pair, AccessType access_type, Address address, 
        std::vector<std::uint64_t> *change_log_ptr, StorageFlags flags)
        : SparseIndexBase(dram_pair, access_type, address, change_log_ptr, flags)
    {
    }
    
    DiffIndex::DiffIndex(tag_create, DRAM_Pair dram_pair, std::vector<std::uint64_t> *change_log_ptr)
        : SparseIndexBase(typename super_t::tag_create{}, dram_pair, change_log_ptr)
    {
    }

    bool DiffIndex::empty() const {
        return super_t::empty();
    }
    
    std::size_t DiffIndex::size() const {
        return super_t::size();
    }
    
    void DiffIndex::insert(PageNumT page_num, StateNumT state_num, PageNumT storage_page_num, bool overflow)
    {
        // try locating existing item first
        typename super_t::ConstNodeIterator node;
        auto item_ptr = super_t::lowerEqualBound(page_num, state_num, node);
        auto relative_state_num = state_num;
        auto relative_storage_page_num = storage_page_num;
        if (item_ptr && node->header().getPageNum(*item_ptr) == page_num && item_ptr->beginAppend(relative_state_num, relative_storage_page_num)) {
            // NOTE: relative_state_num & relative_storage_page_num get converted from absolute to relative values
            db0::modifyMember(node, *item_ptr).append(relative_state_num, relative_storage_page_num);
            // collect the change-log
            this->update(page_num, state_num, storage_page_num + (overflow ? 1 : 0));
        } else {
            // create new item (with no history of updates)
            super_t::emplace(page_num, state_num, storage_page_num);
            // we also need to account for the overflow
            if (overflow) {
                this->update(storage_page_num + 1);
            }
        }
    }
    
    DI_Item DiffIndex::findUpper(PageNumT page_num, StateNumT state_num) const
    {
        auto it = super_t::findLower(page_num, state_num);
        if (!it.isEnd()) {
            auto item = it.second->header().uncompress(*it.get());
            if (item.m_page_num == page_num && item.findUpper(state_num)) {
                return item;
            }
        }
        return super_t::findUpper(page_num, state_num);
    }
    
    Address DiffIndex::getIndexAddress() const {
        return super_t::getIndexAddress();
    }
    
    std::optional<typename DiffIndex::PageNumT> DiffIndex::getNextStoragePageNum() const {
        return super_t::getNextStoragePageNum();
    }
    
    typename DiffIndex::StateNumT DiffIndex::getMaxStateNum() const {
        return super_t::getMaxStateNum();
    }
    
    void DiffIndex::refresh() {
        super_t::refresh();
    }
    
    void DiffIndex::commit() {
        super_t::commit();
    }

    DiffIndex::StateNumT DiffIndex::findLower(PageNumT page_num, StateNumT state_num) const
    {        
        auto item = super_t::lookup(page_num, state_num);
        if (!item) {
            return 0;
        }
        return item.findLower(state_num);        
    }

}