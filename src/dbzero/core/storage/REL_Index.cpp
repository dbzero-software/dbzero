#include "REL_Index.hpp"
#include <dbzero/core/memory/utils.hpp>

namespace db0

{
    
    bool REL_ItemCompT::operator()(const REL_Item &lhs, const REL_Item &rhs) const {
        return lhs.m_rel_page_num < rhs.m_rel_page_num;
    }

    bool REL_ItemEqualT::operator()(const REL_Item &lhs, const REL_Item &rhs) const {
        return lhs.m_rel_page_num == rhs.m_rel_page_num;
    }   

    REL_Index::REL_Index(Memspace &memspace, std::size_t node_capacity, AccessType access_type)
        : super_t(memspace, node_capacity, access_type)
    {        
    }
    
    bool REL_Item::operator==(const REL_Item &other) const {
        return m_rel_page_num == other.m_rel_page_num;
    }

    bool REL_CompressedItemCompT::operator()(const REL_CompressedItem &lhs, const REL_CompressedItem &rhs) const {
        // compressed page numbers are comparable
        return lhs.m_compressed_rel_page_num < rhs.m_compressed_rel_page_num;
    }
    
    bool REL_CompressedItemEqualT::operator()(const REL_CompressedItem &lhs, const REL_CompressedItem &rhs) const {
        return lhs.m_compressed_rel_page_num == rhs.m_compressed_rel_page_num;
    }
    
    REL_CompressedItem::REL_CompressedItem(std::uint32_t first_rel_page_num, const REL_Item &item)
        : m_storage_page_num(item.m_storage_page_num)
    {
        // check if can fit
        assert(first_rel_page_num == (item.m_rel_page_num >> 32));
        // compress by taking low 32 bits only
        m_compressed_rel_page_num = static_cast<std::uint32_t>(item.m_rel_page_num & 0xFFFFFFFF);
    }
    
    REL_CompressedItem::REL_CompressedItem(std::uint32_t first_rel_page_num, std::uint64_t rel_page_num, std::uint64_t storage_page_num)
        : m_storage_page_num(storage_page_num)
    {
        // check if can fit
        assert(first_rel_page_num == (rel_page_num >> 32));
        // compress by taking low 32 bits only
        m_compressed_rel_page_num = static_cast<std::uint32_t>(rel_page_num & 0xFFFFFFFF);
    }
    
    REL_Item REL_CompressedItem::uncompress(std::uint32_t first_rel_page_num) const 
    {
        std::uint64_t full_rel_page_num = (static_cast<std::uint64_t>(first_rel_page_num) << 32) | static_cast<std::uint64_t>(m_compressed_rel_page_num);
        return { full_rel_page_num, m_storage_page_num };
    }

    std::string REL_CompressedItem::toString() const {
        return "REL_CompressedItem{ rel_page_num=" + std::to_string(m_compressed_rel_page_num)
            + ", storage_page_num=" + std::to_string(m_storage_page_num) + " }";
    }

    REL_Index::REL_Index(mptr ptr, std::size_t node_capacity, AccessType access_type)
        : super_t(ptr, node_capacity, access_type)
    {        
    }
    
    db0::Address REL_Index::getAddress() const {
        return super_t::getAddress();
    }
    
    void REL_Index::detach() const {
        super_t::detach();
    }
    
    void REL_Index::commit() const {
        super_t::commit();
    }

    void REL_Index::add(std::uint64_t start_rel_page_num, std::uint64_t start_storage_page_num) {
        super_t::insert({ start_rel_page_num, start_storage_page_num });
    }
    
}