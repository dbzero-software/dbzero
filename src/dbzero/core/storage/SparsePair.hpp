// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include "SparseIndex.hpp"
#include "DiffIndex.hpp"
#include "BaseStorage.hpp"
#include "ChangeLogIOStream.hpp"
#include "StorageFlags.hpp"

namespace db0

{
    
    // The SparsePair combines SparseIndex and DiffIndex
    class SparsePair
    {
    public:
        using PageNumT = SparseIndex::PageNumT;
        using StateNumT = SparseIndex::StateNumT;
        using tag_create = SparseIndex::tag_create;
        using DP_ChangeLogT = BaseStorage::DP_ChangeLogT;
        using DP_ChangeLogStreamT = db0::ChangeLogIOStream<DP_ChangeLogT>;
        
        SparsePair(std::size_t node_size);        
        SparsePair(DRAM_Pair, AccessType, StorageFlags = {});
        SparsePair(tag_create, DRAM_Pair);
        
        ~SparsePair();
        
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
        std::optional<PageNumT> getNextStoragePageNum() const;
        
        // combine from both underlyig indexes
        StateNumT getMaxStateNum() const;
        
        bool empty() const;
        std::size_t size() const;

        void refresh();
        
        /**
         * Write internally managed change log into a specific stream 
         * and then clean the internal change log
        */
        const DP_ChangeLogT &extractChangeLog(DP_ChangeLogStreamT &, std::uint64_t end_storage_page_num);
        
        std::size_t getChangeLogSize() const;
        
        void commit();
        
    private:
        // Change log contains the list of updates (modified items / page numbers)        
        std::vector<std::uint64_t> m_change_log;
        SparseIndex m_sparse_index;
        DiffIndex m_diff_index;
        
        static Address getDiffIndexAddress(const SparseIndex &, StorageFlags);
    };
    
}