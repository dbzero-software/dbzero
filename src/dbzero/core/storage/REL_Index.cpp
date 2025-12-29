// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "REL_Index.hpp"
#include <dbzero/core/memory/utils.hpp>

namespace db0

{
    
    std::string REL_Item::toString() const 
    {
        std::stringstream ss;        
        ss << "REL_Item(" << m_rel_page_num << ", " << m_storage_page_num << ")";
        return ss.str();
    }

    bool REL_ItemCompT::operator()(const REL_Item &item, REL_StoragePageNum storage_page_num) const {
        return item.m_storage_page_num < storage_page_num.m_value;
    }
    
    bool REL_ItemCompT::operator()(REL_StoragePageNum storage_page_num, const REL_Item &item) const {
        return storage_page_num.m_value < item.m_storage_page_num;
    }

    bool REL_ItemCompT::operator()(const REL_Item &lhs, const REL_Item &rhs) const {
        return lhs.m_rel_page_num < rhs.m_rel_page_num;
    }

    bool REL_ItemCompT::operator()(const REL_Item &lhs, std::uint64_t rhs) const {
        return lhs.m_rel_page_num < rhs;
    }

    bool REL_ItemCompT::operator()(std::uint64_t lhs, const REL_Item &rhs) const {
        return lhs < rhs.m_rel_page_num;
    }

    bool REL_ItemEqualT::operator()(const REL_Item &lhs, const REL_Item &rhs) const {
        return lhs.m_rel_page_num == rhs.m_rel_page_num;
    }

    bool REL_ItemEqualT::operator()(const REL_Item &lhs, std::uint64_t rhs) const {
        return lhs.m_rel_page_num == rhs;
    }
    
    bool REL_ItemEqualT::operator()(std::uint64_t lhs, const REL_Item &rhs) const {
        return lhs == rhs.m_rel_page_num;
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

    bool REL_CompressedItemCompT::operator()(const REL_CompressedItem &item, REL_StoragePageNum storage_page_num) const {
        return item.m_storage_page_num < storage_page_num.m_value;
    }

    bool REL_CompressedItemCompT::operator()(REL_StoragePageNum storage_page_num, const REL_CompressedItem &item) const {
        return storage_page_num.m_value < item.m_storage_page_num;
    }

    REL_CompressedItem::REL_CompressedItem(std::uint32_t first_rel_page_num, const REL_Item &item)
        : m_storage_page_num(item.m_storage_page_num)
        , m_flags(item.m_flags)
    {
        // check if can fit
        assert(first_rel_page_num == (item.m_rel_page_num >> 32));
        // compress by taking low 32 bits only
        m_compressed_rel_page_num = static_cast<std::uint32_t>(item.m_rel_page_num & 0xFFFFFFFF);
    }
    
    REL_CompressedItem::REL_CompressedItem(std::uint32_t first_rel_page_num, std::uint64_t rel_page_num,
        std::uint64_t storage_page_num, REL_Flags flags)
        : m_storage_page_num(storage_page_num)
        , m_flags(flags)
    {
        // check if can fit
        assert(first_rel_page_num == (rel_page_num >> 32));
        // compress by taking low 32 bits only
        m_compressed_rel_page_num = static_cast<std::uint32_t>(rel_page_num & 0xFFFFFFFF);
    }

    REL_Item REL_CompressedItem::uncompress(std::uint32_t first_rel_page_num) const 
    {
        std::uint64_t full_rel_page_num = (static_cast<std::uint64_t>(first_rel_page_num) << 32) | static_cast<std::uint64_t>(m_compressed_rel_page_num);
        return { full_rel_page_num, m_storage_page_num, m_flags };
    }

    std::string REL_CompressedItem::toString() const {
        return "REL_CompressedItem{ rel_page_num=" + std::to_string(m_compressed_rel_page_num)
            + ", storage_page_num=" + std::to_string(m_storage_page_num) + " }";
    }

    REL_IndexTypes::CompressedItemT
    REL_IndexTypes::BlockHeader::compressFirst(const ItemT &item)
    {
        m_first_page_num = item.m_rel_page_num >> 32;
        return CompressedItemT(m_first_page_num, item);
    }
    
    REL_IndexTypes::CompressedItemT
    REL_IndexTypes::BlockHeader::compress(const ItemT &item) const
    {
        // ensure can fit
        assert(m_first_page_num == (item.m_rel_page_num >> 32));
        return CompressedItemT(m_first_page_num, item);
    }
    
    REL_IndexTypes::CompressedItemT
    REL_IndexTypes::BlockHeader::compress(std::uint64_t rel_page_num) const {
        // ensure can fit
        assert(m_first_page_num == (rel_page_num >> 32));
        return CompressedItemT(m_first_page_num, rel_page_num, 0);
    }

    REL_IndexTypes::ItemT
    REL_IndexTypes::BlockHeader::uncompress(const CompressedItemT &item) const {
        return item.uncompress(m_first_page_num);
    }
    
    bool REL_IndexTypes::BlockHeader::canFit(const ItemT &item) const {
        return m_first_page_num == (item.m_rel_page_num >> 32);
    }
    
    bool REL_IndexTypes::BlockHeader::canFit(std::uint64_t rel_page_num) const {
        return m_first_page_num == (rel_page_num >> 32);
    }
    
    bool REL_IndexTypes::BlockHeader::canFit(REL_StoragePageNum) const {
        return true;
    }

    REL_StoragePageNum REL_IndexTypes::BlockHeader::compress(REL_StoragePageNum storage_page_num) const {
        return storage_page_num;
    }
    
    std::string REL_IndexTypes::BlockHeader::toString(const CompressedItemT &item) const 
    {
        auto full_item = uncompress(item);
        std::stringstream ss;
        ss << full_item;        
        return ss.str();
    }

    std::string REL_IndexTypes::BlockHeader::toString() const {
        return "BlockHeader{ first_page_num=" + std::to_string(m_first_page_num) + " }";
    }

    REL_Index::REL_Index(mptr ptr, std::size_t node_capacity, AccessType access_type)
        : super_t(ptr, node_capacity, access_type)
        , m_storage_page_num(this->treeHeader().m_storage_page_num)
        , m_rel_page_num(this->treeHeader().m_rel_page_num)
        , m_max_rel_page_num(this->treeHeader().m_max_rel_page_num)        
    {        
    }
    
    db0::Address REL_Index::getAddress() const {
        return super_t::getAddress();
    }
    
    void REL_Index::detach() const {
        super_t::detach();
    }
    
    void REL_Index::commit() const
    {
        // flush locally cached value
        auto &self = const_cast<REL_Index&>(*this);
        self.modifyTreeHeader().m_storage_page_num = m_storage_page_num;
        self.modifyTreeHeader().m_rel_page_num = m_rel_page_num;
        self.modifyTreeHeader().m_max_rel_page_num = m_max_rel_page_num;        
        super_t::commit();
    }
    
    std::uint64_t REL_Index::assignRelative(std::uint64_t storage_page_num, bool is_first_in_step)
    {
        assert(storage_page_num >= m_storage_page_num);
        // prevent adding a duplicate mapping (e.g. might be called multiple times after appendDiff)
        if (is_first_in_step && (storage_page_num != m_storage_page_num)) {
            super_t::insert({ ++m_max_rel_page_num, storage_page_num });
            assert(storage_page_num > m_storage_page_num);
            m_storage_page_num = storage_page_num;
            m_rel_page_num = m_max_rel_page_num;
        }
        
        auto result = m_rel_page_num + (storage_page_num - m_storage_page_num);
        if (result > m_max_rel_page_num) {
            m_max_rel_page_num = result;
        }
        
        return result;
    }
    
    void REL_Index::addMapping(std::uint64_t storage_page_num, std::uint64_t rel_page_num, std::uint32_t count)
    {
        assert(count > 0);
        assert(storage_page_num >= m_storage_page_num);
        assert(rel_page_num >= m_max_rel_page_num);

        m_max_rel_page_num = rel_page_num + count - 1;
        if (!this->empty()) {
            // check if the mapping is already valid
            if (storage_page_num - m_storage_page_num == rel_page_num - m_rel_page_num) {
                // mapping already valid, no need for registering another entry
                return;
            }
        }
        
        // register the new mapping
        super_t::insert({ rel_page_num, storage_page_num });
        m_rel_page_num = rel_page_num;
        m_storage_page_num = storage_page_num;
    }
    
    void REL_Index::refresh()
    {
        detach();
        m_storage_page_num = this->treeHeader().m_storage_page_num;
        m_rel_page_num = this->treeHeader().m_rel_page_num;
        m_max_rel_page_num = this->treeHeader().m_max_rel_page_num;        
    }
    
    std::uint64_t REL_Index::getAbsolute(std::uint64_t rel_page_num) const
    {
        auto result = super_t::lower_equal_bound(rel_page_num);
        if (!result) {
            THROWF(db0::InternalException) << "REL_Index: page lookup failed on: " << rel_page_num;
        }
        // translate to absolute storage page number
        return result->m_storage_page_num + (rel_page_num - result->m_rel_page_num);        
    }
    
    std::uint64_t REL_Index::getRelative(std::uint64_t storage_page_num) const
    {
        // Query using an alternative comparator
        // - by storage page num only (which is stored preserving the same order as relative page num)        
        auto result = super_t::lower_equal_bound(REL_StoragePageNum { storage_page_num});
        if (!result) {
            THROWF(db0::InternalException) << "REL_Index: page lookup failed on: " << storage_page_num;
        }
        // translate to relative page number        
        return result->m_rel_page_num + (storage_page_num - result->m_storage_page_num);
    }
    
    std::uint64_t REL_Index::size() const {
        return super_t::size();
    }
    
    REL_Index::const_iterator REL_Index::cbegin() const {
        return super_t::cbegin();
    }

}

namespace std

{

    ostream &operator<<(ostream &os, const db0::REL_Item &item) {
        return os << item.toString();
    }
    
}