#pragma once

#include "SparseIndex.hpp"
#include "DiffIndex.hpp"

namespace db0

{

    // The SparseIndexQuery allows retrieving a DP location
    // as a combination of full-DP + optional multiple diff-DPs
    // it combines the use of SparseIndex and DiffIndex
    class SparseIndexQuery
    {
    public:
        SparseIndexQuery(const SparseIndex &, const DiffIndex &, std::uint64_t page_num, std::uint32_t state_num);

        // NOTE: the first returned storage page num will be full-DP
        // @return 0 if no associated DP found
        inline std::uint64_t first() const {
            return m_full_dp.m_storage_page_num;
        }
        
        // and the subsequent ones - diff-DPs until false is returned
        bool next(std::uint32_t &state_num, std::uint64_t &storage_page_num);

    private:
        using DiffArrayT = DI_Item::DiffArrayT;
        const std::uint32_t m_query_state_num;
        std::uint32_t m_state_num;
        const SI_Item m_full_dp;
        const DiffIndex &m_diff_index;
        DI_Item m_diff_dp;
        typename DI_Item::ConstIterator m_diff_it;        
    };
    
}   
