// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SparseIndexQuery.hpp"

namespace db0

{

    SparseIndexQuery::SparseIndexQuery(const SparseIndex &sparse_index, const DiffIndex &diff_index,
        std::uint64_t page_num, StateNumType state_num)
        : m_query_page_num(page_num)
        , m_query_state_num(state_num)
        // will be initialized with 0 if not found
        , m_full_dp(sparse_index.lookup(page_num, state_num))
        , m_diff_index(diff_index)
    {
        assert(m_full_dp.m_state_num <= state_num && "SparseIndexQuery: full-DP state number must be <= query state number");
        if (m_full_dp && m_full_dp.m_state_num < state_num) {
            m_diff_dp = m_diff_index.findUpper(page_num, m_full_dp.m_state_num + 1);
        } else {
            // in case updates start from the diff-DP
            m_diff_dp = m_diff_index.findUpper(page_num, 1);
            if (!m_full_dp && (!m_diff_dp || m_diff_dp.m_state_num > m_query_state_num)) {
                m_non_empty = false;
            }
        }
    }
    
    bool SparseIndexQuery::empty() const {
        return !m_non_empty || lessThan(1);
    }
    
    bool SparseIndexQuery::next(StateNumType &state_num, std::uint64_t &storage_page_num)
    {
        // unable to iterate past the queried state number
        if (m_state_num >= m_query_state_num) {
            return false;
        }
        
        for (;;) {
            if (!m_diff_dp || m_diff_dp.m_state_num > m_query_state_num) {
                return false;
            }
            if (m_diff_it) {
                for (;;) {
                    if (m_diff_it.next(state_num, storage_page_num)) {
                        if (state_num <= m_state_num) {
                            // must position after the full-DP item
                            continue;
                        }
                    } else {
                        m_diff_it.reset();
                        // try locating the next diff-DP
                        m_diff_dp = m_diff_index.findUpper(m_diff_dp.m_page_num, m_state_num + 1);
                        break;
                    }
                    if (state_num > m_query_state_num) {
                        // end of iteration since the queried state number was reached
                        return false;
                    }
                    m_state_num = state_num;
                    return true;
                }                
            } else {
                m_diff_it = m_diff_dp.beginDiff();
                // must position after the full-DP item
                if (m_diff_dp.m_state_num <= m_state_num) {
                    continue;
                }
                // retrieve the first diff-item
                storage_page_num = m_diff_dp.m_storage_page_num;
                state_num = m_diff_dp.m_state_num;
                m_state_num = state_num;
                return true;
            }        
        }
    }
    
    bool SparseIndexQuery::lessThan(unsigned int size) const
    {
        assert(size > 0 && "SparseIndexQuery::lessThan: size must be > 0");        
        if (m_full_dp) {
            --size;
        }

        if (size == 0) {
            return false;
        }

        DI_Item diff_dp;
        if (m_full_dp && m_full_dp.m_state_num < m_query_state_num) {
            diff_dp = m_diff_index.findUpper(m_query_page_num, m_full_dp.m_state_num + 1);
        } else {
            // in case updates start from the diff-DP
            diff_dp = m_diff_index.findUpper(m_query_page_num, 1);
        }
        
        typename DI_Item::ConstIterator diff_it;
        StateNumType last_state_num = 0;
        return lessThanFrom(size, diff_dp, diff_it, last_state_num);
    }
    
    bool SparseIndexQuery::leftLessThan(unsigned int size) const
    {
        assert(size > 0 && "SparseIndexQuery::lessThan: size must be > 0");
        auto diff_dp = m_diff_dp;
        auto diff_it = m_diff_it;
        auto last_state_num = m_state_num;
        return lessThanFrom(size, diff_dp, diff_it, last_state_num);
    }
    
    bool SparseIndexQuery::lessThanFrom(unsigned int size, DI_Item &diff_dp, typename DI_Item::ConstIterator &diff_it,
        StateNumType &last_state_num) const
    {
        assert(size > 0 && "SparseIndexQuery::lessThan: size must be > 0");
        // unable to iterate past the queried state number
        if (last_state_num >= m_query_state_num) {
            return true;
        }
        
        StateNumType state_num = 0;
        while (size > 0) {
            if (!diff_dp || diff_dp.m_state_num > m_query_state_num) {
                return true;
            }
            if (diff_it) {
                for (;;) {
                    if (diff_it.next(state_num)) {
                        if (state_num <= last_state_num) {
                            // must position after the full-DP item
                            continue;
                        }
                    } else {
                        diff_it.reset();
                        // try locating the next diff-DP
                        diff_dp = m_diff_index.findUpper(diff_dp.m_page_num, last_state_num + 1);
                        break;
                    }
                    if (state_num > m_query_state_num) {
                        // end of iteration since the queried state number was reached
                        return true;
                    }
                    last_state_num = state_num;
                    --size;
                    break;
                }
            } else {
                diff_it = diff_dp.beginDiff();
                // must position after the full-DP item
                if (diff_dp.m_state_num <= last_state_num) {
                    continue;
                }
                // retrieve the first diff-item                
                state_num = diff_dp.m_state_num;
                last_state_num = state_num;
                --size;
            }
        }

        return false;
    }

    bool tryFindMutation(const SparseIndex &sparse_index, const DiffIndex &diff_index, std::uint64_t page_num,
        StateNumType state_num, StateNumType &mutation_id)
    {
        // query the diff index first
        mutation_id = diff_index.findLower(page_num, state_num);
        auto item  = sparse_index.lookup(page_num, state_num);
        if (!item) {
            // mutation only exists in the diff index
            return mutation_id != 0;
        }
        // take max from the sparse index and diff index
        mutation_id = std::max((StateNumType)item.m_state_num, mutation_id);
        return true;
    }
    
}