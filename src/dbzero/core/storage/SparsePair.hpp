#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include "SparseIndex.hpp"
#include "DiffIndex.hpp"

namespace db0

{

    class ChangeLogIOStream;

    // The SparsePair combines SparseIndex and DiffIndex
    class SparsePair
    {
    public:
        using PageNumT = SparseIndex::PageNumT;
        using StateNumT = SparseIndex::StateNumT;
        using tag_create = SparseIndex::tag_create;
        
        SparsePair(std::size_t node_size);
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
        
        /**
         * Write internally managed change log into a specific stream 
         * and then clean the internal change log
        */
        const o_change_log &extractChangeLog(ChangeLogIOStream &);
        
        std::size_t getChangeLogSize() const;

    private:
        // change log contains the list of updates (modified items / page numbers)
        // first element is the state number
        std::vector<std::uint64_t> m_change_log;
        SparseIndex m_sparse_index;
        DiffIndex m_diff_index;
    };
    
}