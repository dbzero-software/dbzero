#include "SparsePair.hpp"

namespace db0

{
    
    SparsePair::SparsePair(std::size_t node_size)
        : m_sparse_index(node_size, &m_change_log)
        , m_diff_index(node_size, &m_change_log)
    {
    }
    
    SparsePair::SparsePair(DRAM_Pair dram_pair, AccessType access_type)
        : m_sparse_index(dram_pair, access_type, 0, &m_change_log)
        , m_diff_index(dram_pair, access_type, m_sparse_index.getExtraData(), &m_change_log)
    {        
    }
    
    SparsePair::SparsePair(tag_create, DRAM_Pair dram_pair)
        : m_sparse_index(SparseIndex::tag_create(), dram_pair, &m_change_log)
        , m_diff_index(DiffIndex::tag_create(), dram_pair, &m_change_log)
    {
        // store the diff-index's address as extra data in the sparse index
        m_sparse_index.setExtraData(m_diff_index.getIndexAddress());
    }
        
    typename SparsePair::PageNumT SparsePair::getNextStoragePageNum() const {
        return std::max(m_sparse_index.getNextStoragePageNum(), m_diff_index.getNextStoragePageNum());
    }

    typename SparsePair::StateNumT SparsePair::getMaxStateNum() const {
        return std::max(m_sparse_index.getMaxStateNum(), m_diff_index.getMaxStateNum());
    }

    void SparsePair::refresh()
    {
        m_sparse_index.refresh();
        m_diff_index.refresh();
    }
    
    std::size_t SparsePair::size() const {
        return m_sparse_index.size() + m_diff_index.size();
    }
    
    const o_change_log &SparsePair::extractChangeLog(ChangeLogIOStream &changelog_io)
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

    std::size_t SparsePair::getChangeLogSize() const {
        return m_change_log.empty() ? 0 : m_change_log.size() - 1;
    }

}