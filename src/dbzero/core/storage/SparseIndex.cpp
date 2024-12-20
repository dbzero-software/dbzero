#include "SparseIndex.hpp"

namespace db0

{

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

    std::uint64_t SI_CompressedItem::getStoragePageNum() const
    {
        std::uint64_t result = m_high_bits & 0xFF;
        result <<= 32;
        result |= m_low_bits;
        return result;
    }
    
}