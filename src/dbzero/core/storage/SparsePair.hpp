#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include "SparseIndex.hpp"
#include "DiffIndex.hpp"

namespace db0

{

    // The SparsePair combines SparseIndex and DiffIndex
    class SparsePair
    {
    public:
        using PageNumT = SparseIndex::PageNumT;
        using StateNumT = SparseIndex::StateNumT;
        using tag_create = SparseIndex::tag_create;
        
        SparsePair(DRAM_Pair, AccessType);
        SparsePair(tag_create, DRAM_Pair);

        inline SparseIndex &getSparseIndex() {
            return m_sparse_index;
        }
        
        inline const SparseIndex &getSparseIndex() const {
            return m_sparse_index;
        }
        
        inline DiffIndex &getDiffIndex() {
            return m_diff_index;
        }
        
        inline const DiffIndex &getDiffIndex() const {
            return m_diff_index;
        }        

        // combine from both underlyig indexes
        PageNumT getNextStoragePageNum() const;
        
        // combine from both underlyig indexes
        StateNumT getMaxStateNum() const;
        
        std::size_t size() const;

        void refresh();

    private:
        SparseIndex m_sparse_index;
        DiffIndex m_diff_index;
    };
    
}