#include "DiffIndex.hpp"

namespace db0

{
    
    DI_Item::DI_Item(const SI_Item &si_item, const DiffBufT &diff_data)
        : SI_Item(si_item)
        , m_diff_data(diff_data)
    {    
    }

    DI_CompressedItem::DI_CompressedItem(std::uint32_t first_page_num, const DI_Item &item)
        : SI_CompressedItem(first_page_num, item)
        , m_diff_data(item.m_diff_data)
    {
    }
    
    DI_CompressedItem::DI_CompressedItem(std::uint32_t first_page_num, std::uint64_t page_num, std::uint32_t state_num)
        : SI_CompressedItem(first_page_num, page_num, state_num)
    {        
    }

    DI_Item DI_CompressedItem::uncompress(std::uint32_t first_page_num) const {
        return DI_Item(SI_CompressedItem::uncompress(first_page_num), m_diff_data);
    }

    bool DI_CompressedItem::canAppend(std::uint32_t state_num, std::uint64_t storage_page_num) const {
        return DiffArrayT::__const_ref(m_diff_data.data()).canEmplaceBack(state_num, storage_page_num);
    }

    void DI_CompressedItem::append(std::uint32_t state_num, std::uint64_t storage_page_num) {
        DiffArrayT::__ref(m_diff_data.data()).emplaceBack(state_num, storage_page_num);
    }

    DiffIndex::DiffIndex(std::size_t node_size)
        : SparseIndexBase(node_size)
    {
    }

    DiffIndex::DiffIndex(DRAM_Pair dram_pair, AccessType access_type)
        : SparseIndexBase(dram_pair, access_type)
    {
    }
    
    std::size_t DiffIndex::size() const {
        return super_t::size();
    }
    
    void DiffIndex::insert(PageNumT page_num, StateNumT state_num, PageNumT storage_page_num)
    {
        // try locating existing item first
        typename super_t::ConstNodeIterator node;
        auto item_ptr = super_t::lowerEqualBound(page_num, state_num, node);
        if (item_ptr && node->header().getPageNum(*item_ptr) == page_num && item_ptr->canAppend(state_num, storage_page_num)) {
            // extend the existing item
            auto item_index = node->indexOf(item_ptr);
            // NOTE: item_ptr gets invalidated by "modify"
            node.modify().at(item_index).append(state_num, storage_page_num);
        } else {
            // create new item
            super_t::emplace(page_num, state_num, storage_page_num);
        }
    }

}