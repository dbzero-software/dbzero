// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SparseIndex.hpp"
#include <cassert>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    bool SI_Item::operator==(const SI_Item &other) const
    {
        return m_page_num == other.m_page_num && m_state_num == other.m_state_num
            && m_storage_page_num == other.m_storage_page_num;
    }
    
    std::string SI_Item::toString() const
    {
        std::stringstream ss;        
        ss << "SI_Item(" << m_page_num << ", " << m_state_num << ", " << m_storage_page_num << ")";
        return ss.str();
    }

    bool SI_ItemCompT::operator()(const SI_Item &lhs, const SI_Item &rhs) const
    {
        if (lhs.m_page_num < rhs.m_page_num) {
            return true;
        }
        if (lhs.m_page_num > rhs.m_page_num) {
            return false;
        }
        return lhs.m_state_num < rhs.m_state_num;
    }

    bool SI_ItemCompT::operator()(const SI_Item &lhs, std::pair<std::uint64_t, std::uint32_t> rhs) const
    {
        if (lhs.m_page_num < rhs.first) {
            return true;
        }
        if (lhs.m_page_num > rhs.first) {
            return false;
        }
        return lhs.m_state_num < rhs.second;
    }

    bool SI_ItemCompT::operator()(std::pair<std::uint64_t, std::uint32_t> lhs, const SI_Item &rhs) const
    {        
        if (lhs.first < rhs.m_page_num) {
            return true;
        }
        if (lhs.first > rhs.m_page_num) {
            return false;
        }
        return lhs.second < rhs.m_state_num;
    }

    bool SI_ItemEqualT::operator()(const SI_Item &lhs, const SI_Item &rhs) const {
        return lhs.m_page_num == rhs.m_page_num && lhs.m_state_num == rhs.m_state_num;
    }

    bool SI_ItemEqualT::operator()(const SI_Item &lhs, std::pair<std::uint64_t, std::uint32_t> rhs) const {
        return lhs.m_page_num == rhs.first && lhs.m_state_num == rhs.second;
    }

    bool SI_ItemEqualT::operator()(std::pair<std::uint64_t, std::uint32_t> lhs, const SI_Item &rhs) const {
        return lhs.first == rhs.m_page_num && lhs.second == rhs.m_state_num;
    }

    bool SI_CompressedItemCompT::operator()(const SI_CompressedItem &lhs, const SI_CompressedItem &rhs) const {
        return lhs.getKey() < rhs.getKey();
    }

    bool SI_CompressedItemEqualT::operator()(const SI_CompressedItem &lhs, const SI_CompressedItem &rhs) const {
        return lhs.getKey() == rhs.getKey();
    }

    SI_CompressedItem::SI_CompressedItem(std::uint32_t first_page_num, const SI_Item &item)
        : SI_CompressedItem(first_page_num, item.m_page_num, item.m_state_num)
    {
        if (item.m_storage_page_num & 0xFFFFFF0000000000) {
            THROWF(InputException) << "storage page number " << item.m_storage_page_num << " is too large";
        }
        // take 8 high bits from the storage page num
        m_high_bits |= item.m_storage_page_num >> 32;
        // take low 32 bits from the storage page num
        m_low_bits = item.m_storage_page_num & 0xFFFFFFFF;
    }

    SI_CompressedItem::SI_CompressedItem(std::uint32_t first_page_num, std::uint64_t page_num, std::uint32_t state_num)
        : m_low_bits(0)
    {
        assert(first_page_num == (page_num >> 24));
        m_high_bits = page_num & 0b111111111111111111111111;
        m_high_bits <<= 40;
        m_high_bits |= static_cast<std::uint64_t>(state_num) << 8;        
    }
    
    std::uint64_t SI_CompressedItem::getStoragePageNum() const
    {
        std::uint64_t result = m_high_bits & 0xFF;
        result <<= 32;
        result |= m_low_bits;
        return result;
    }
    
    SI_Item SI_CompressedItem::uncompress(std::uint32_t first_page_num) const
    {
        return {
            this->getPageNum(first_page_num),
            this->getStateNum(), 
            this->getStoragePageNum()            
        };
    }

    std::string SI_CompressedItem::toString() const
    {
        std::stringstream ss;        
        ss << "CompressedItem(" << getCompressedPageNum() << ", " << getStateNum() << ", " << getStoragePageNum() << ")";
        return ss.str();
    }

}

namespace std

{

    ostream &operator<<(ostream &os, const db0::SI_Item &item) {
        return os << item.toString();
    }
    
}