#include "SparsePair.hpp"

namespace db0

{
        
    SparsePair::SparsePair(DRAM_Pair dram_pair, AccessType access_type)
        : m_sparse_index(dram_pair, access_type)        
        , m_diff_index(dram_pair, access_type, m_sparse_index.getExtraData())
    {        
    }
    
    SparsePair::SparsePair(tag_create, DRAM_Pair dram_pair)
        : m_sparse_index(SparseIndex::tag_create(), dram_pair)
        , m_diff_index(DiffIndex::tag_create(), dram_pair)
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

}