#include "SparsePair.hpp"

namespace db0

{
    
    SparsePair::SparsePair(std::size_t node_size)
        : m_sparse_index(node_size, &m_change_log)
        , m_diff_index(node_size, &m_change_log)
    {
    }
    
    SparsePair::SparsePair(DRAM_Pair dram_pair, AccessType access_type)
        : m_sparse_index(dram_pair, access_type, {}, &m_change_log)
        , m_diff_index(dram_pair, access_type, Address::fromOffset(m_sparse_index.getExtraData()), &m_change_log)
    {
    }
    
    SparsePair::SparsePair(tag_create, DRAM_Pair dram_pair)
        : m_sparse_index(SparseIndex::tag_create(), dram_pair, &m_change_log)
        , m_diff_index(DiffIndex::tag_create(), dram_pair, &m_change_log)
    {
        // store the diff-index's address as extra data in the sparse index
        m_sparse_index.setExtraData(m_diff_index.getIndexAddress().getOffset());
    }

    SparsePair::~SparsePair()
    {
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
    
    const SparsePair::DP_ChangeLogT &SparsePair::extractChangeLog(DP_ChangeLogStreamT &changelog_io, 
        std::uint64_t end_storage_page_num)
    {
        std::sort(m_change_log.begin(), m_change_log.end());        
        ChangeLogData cl_data;
        // add page numbers with deduplication
        for (auto page_num : m_change_log) {
            cl_data.m_rle_builder.append(page_num, false);            
        }
        
        // RLE encode, no duplicates        
        auto &result = changelog_io.appendChangeLog(
            std::move(cl_data), this->getMaxStateNum(), end_storage_page_num
        );
        m_change_log.clear();
        return result;
    }
    
    std::size_t SparsePair::getChangeLogSize() const {
        return m_change_log.size();
    }

    void SparsePair::commit()
    {
        m_sparse_index.commit();
        m_diff_index.commit();
    }
    
}