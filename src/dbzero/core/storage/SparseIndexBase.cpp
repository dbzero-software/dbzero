#include "SparseIndex.hpp"
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>

namespace db0

{
    
    SparseIndex::SparseIndex(std::size_t node_size)
        : m_dram_space(DRAMSpace::create(node_size, [this](DRAM_Pair dram_pair) {
            this->m_dram_prefix = dram_pair.first;
            this->m_dram_allocator = dram_pair.second;
        }))
        , m_index(m_dram_space, node_size, AccessType::READ_WRITE)
    {
        m_index.modifyTreeHeader().m_next_page_num = m_next_page_num;
        m_index.modifyTreeHeader().m_max_state_num = m_max_state_num;
    }
    
    SparseIndex::SparseIndex(DRAM_Pair dram_pair, AccessType access_type)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_index(openIndex(access_type))
        , m_next_page_num(m_index.treeHeader().m_next_page_num)
        , m_max_state_num(m_index.treeHeader().m_max_state_num)
    {
    }
    
    bool SparseIndex::Item::operator==(const Item &other) const
    {
        return m_page_num == other.m_page_num && m_state_num == other.m_state_num
            && m_storage_page_num == other.m_storage_page_num && m_page_type == other.m_page_type;
    }
    
    SparseIndex::IndexT SparseIndex::openIndex(AccessType access_type)
    {
        if (m_dram_prefix->empty()) {
            // create new on top of existing DRAMSpace
            return IndexT(m_dram_space, m_dram_prefix->getPageSize(), access_type);
        } else {
            // open existing under first addres assigned to the DRAMSpace
            auto address = m_dram_allocator->firstAlloc();            
            return IndexT(m_dram_space.myPtr(address), m_dram_prefix->getPageSize(), access_type);
        }
    }
    
    const DRAM_Prefix &SparseIndex::getDRAMPrefix() const {
        return *m_dram_prefix;
    }

    SparseIndex::CompressedItem SparseIndex::BlockHeader::compressFirst(const Item &item) {
        m_first_page_num = item.m_page_num >> 23;
        return compress(item);
    }

    SparseIndex::CompressedItem SparseIndex::BlockHeader::compress(const Item &item) const
    {
        assert(m_first_page_num == (item.m_page_num >> 23));
        CompressedItem result;
        result.m_high_bits = item.m_page_num & 0x7FFFFF;
        result.m_high_bits <<= 41;
        result.m_high_bits |= static_cast<std::uint64_t>(item.m_state_num) << 9;
        result.m_high_bits |= static_cast<std::uint64_t>(item.m_page_type) << 8;
        if (item.m_storage_page_num & 0xFFFFFF0000000000) {
            THROWF(InputException) << "storage page number " << item.m_storage_page_num << " is too large";
        }
        // take 8 high bits from the storage page num
        result.m_high_bits |= item.m_storage_page_num >> 32;
        // take low 32 bits from the storage page num
        result.m_low_bits = item.m_storage_page_num & 0xFFFFFFFF;
        return result;
    }

    /*
    SparseIndex::CompressedItem SparseIndex::BlockHeader::compress(std::pair<std::uint64_t, std::int32_t> item) const
    {
        assert(m_first_page_num == (item.first >> 23));
        CompressedItem result;
        result.m_high_bits = item.first & 0x7FFFFF;
        result.m_high_bits <<= 41;
        result.m_high_bits |= static_cast<std::uint64_t>(item.second) << 9;
        return result;
    }
    */

    SparseIndex::Item SparseIndex::BlockHeader::uncompress(CompressedItem item) const 
    {
        return { 
            item.getPageNum() | (static_cast<std::uint64_t>(m_first_page_num) << 23), 
            item.getStateNum(), 
            item.getStoragePageNum(), 
            item.getPageType() 
        };
    }

    bool SparseIndex::BlockHeader::canFit(const Item &item) const {
        return m_first_page_num == (item.m_page_num >> 23);
    }

    SparseIndex::PageType SparseIndex::CompressedItem::getPageType() const {
        return static_cast<PageType>((m_high_bits >> 8) & 0x1);
    }
        
    std::uint64_t SparseIndex::CompressedItem::getStoragePageNum() const
    {
        std::uint64_t result = m_high_bits & 0xFF;
        result <<= 32;
        result |= m_low_bits;
        return result;
    }
    
    SparseIndex::Item SparseIndex::lookup(std::uint64_t page_num, std::uint32_t state_num) const {
        return lookup(std::make_pair(page_num, state_num));
    }
    
    SparseIndex::Item SparseIndex::lookup(std::pair<std::uint64_t, std::uint32_t> page_and_state) const
    {
        auto result = m_index.lower_equal_bound(page_and_state);
        if (!result || result->m_page_num != page_and_state.first) {
            return {};
        }
        return *result;
    }
    
    SparseIndex::Item SparseIndex::lookup(const Item &item) const
    {
        auto result = m_index.lower_equal_bound(item);
        if (!result || result->m_page_num != item.m_page_num) {
            return {};
        }
        return *result;
    }
    
    std::uint64_t SparseIndex::getNextStoragePageNum() const {
        return m_next_page_num;
    }

    std::uint32_t SparseIndex::getMaxStateNum() const {
        return m_max_state_num;
    }
    
    void SparseIndex::refresh()
    {
        m_next_page_num = m_index.treeHeader().m_next_page_num;
        m_max_state_num = m_index.treeHeader().m_max_state_num;
        m_index.detach();
    }
    
    const o_change_log &SparseIndex::extractChangeLog(ChangeLogIOStream &changelog_io)
    {        
        assert(!m_change_log.empty());
        // sort change log but keep the 1st item (the state number) at its place
        if (!m_change_log.empty()) {
            std::sort(m_change_log.begin() + 1, m_change_log.end());
        }
        
        ChangeLogData cl_data;
        auto it = m_change_log.begin(), end = m_change_log.end();
        // first item is the state number
        cl_data.m_rle_builder.append(*(it++), true);
        // the first page number (second item) must be added even if it is identical as the state number
        if (it != end) {
            cl_data.m_rle_builder.append(*(it++), true);
        }
        // all remaining items add with deduplication
        for (; it != end; ++it) {
            cl_data.m_rle_builder.append(*it, false);
        }
        
        // RLE encode, no duplicates
        auto &result = changelog_io.appendChangeLog(std::move(cl_data));
        m_change_log.clear();
        return result;
    }
    
    std::string SparseIndex::CompressedItem::toString() const
    {
        std::stringstream ss;
        std::string str_page_type = getPageType() == PageType::FIXED ? "FIXED" : "MUTABLE";
        ss << "CompressedItem(" << getPageNum() << ", " << getStateNum() << ", " << getStoragePageNum() << ", " << str_page_type << ")";
        return ss.str();
    }

    std::string SparseIndex::BlockHeader::toString(const CompressedItem &item) const {
        return item.toString();
    }
    
    std::size_t SparseIndex::getChangeLogSize() const {
        return m_change_log.empty() ? 0 : m_change_log.size() - 1;
    }

    std::size_t SparseIndex::size() const {
        return m_index.size();        
    }
    
}