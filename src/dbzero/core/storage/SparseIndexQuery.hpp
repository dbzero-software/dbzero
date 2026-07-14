// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "SparseIndex.hpp"
#include "DiffIndex.hpp"
#include <dbzero/core/memory/config.hpp>

namespace db0

{

    // The SparseIndexQuery allows retrieving a DP location
    // as a combination of full-DP + optional multiple diff-DPs
    // it combines the use of SparseIndex and DiffIndex
    class SparseIndexQuery
    {
    public:
        SparseIndexQuery(const SparseIndex &, const DiffIndex &, std::uint64_t page_num, StateNumType state_num);
        
        inline StateNumType firstStateNum() const {
            return m_full_dp.m_state_num;
        }

        // NOTE: the first returned storage page num will be full-DP
        // @return 0 if no associated DP found
        inline std::uint64_t first() const 
        {
            m_state_num = m_full_dp.m_state_num;
            return m_full_dp.m_storage_page_num;
        }
        
        inline std::uint64_t first(StateNumType &state_num) const
        {
            state_num = m_full_dp.m_state_num;
            m_state_num = state_num;
            return m_full_dp.m_storage_page_num;
        }
        
        // and the subsequent ones - diff-DPs until false is returned
        bool next(StateNumType &state_num, std::uint64_t &storage_page_num);
        
        // Check if the total number of query results (first + next) is less than the given value
        bool lessThan(unsigned int) const;

        // Check if the total number of query results still obtainable with the next() call is less than the given value
        bool leftLessThan(unsigned int) const;

        // check if the query yields any results (first or next)
        bool empty() const;

    private:
        using DiffArrayT = DI_Item::DiffArrayT;
        const std::uint64_t m_query_page_num;
        const StateNumType m_query_state_num;
        mutable StateNumType m_state_num = 0;
        const SI_Item m_full_dp;
        const DiffIndex &m_diff_index;
        DI_Item m_diff_dp;
        typename DI_Item::ConstIterator m_diff_it;
        bool m_non_empty = true;
        
        // Common implemetation part for lessThan and leftLessThan
        bool lessThanFrom(unsigned int size, DI_Item &, typename DI_Item::ConstIterator &, 
            StateNumType &last_state_num) const;
    };
    
    // Try identifying the state number (but not larger than state_num) swhen a specific page was modified
    bool tryFindMutation(const SparseIndex &, const DiffIndex &, std::uint64_t page_num, StateNumType state_num,
        StateNumType &mutation_id);
    
}   
