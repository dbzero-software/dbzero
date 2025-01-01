#include "SparseIndexQuery.hpp"

namespace db0

{

    SparseIndexQuery::SparseIndexQuery(const SparseIndex &sparse_index, const DiffIndex &diff_index,
        std::uint64_t page_num, std::uint32_t state_num)
        : m_query_state_num(state_num)        
        // will be initialized with 0 if not found
        , m_full_dp(sparse_index.lookup(page_num, state_num))
        , m_diff_index(diff_index)
    {
        if (m_full_dp && m_full_dp.m_state_num < state_num) {
            m_diff_dp = m_diff_index.findUpper(page_num, m_full_dp.m_state_num + 1);
        } else {
            // in case updates start from the diff-DP
            m_diff_dp = m_diff_index.findUpper(page_num, state_num);
        }
    }
    
    bool SparseIndexQuery::empty() const
    {
        if (m_full_dp) {
            return false;
        }
        if (!m_diff_dp || m_diff_dp.m_state_num > m_query_state_num) {
            return true;
        }
        if (m_state_num >= m_query_state_num) {
            return true;
        }
        return false;
    }

    bool SparseIndexQuery::next(std::uint32_t &state_num, std::uint64_t &storage_page_num)
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
                if (!m_diff_it.next(state_num, storage_page_num)) {
                    m_diff_it.reset();
                    // try locating the next diff-DP 
                    m_diff_dp = m_diff_index.findUpper(m_diff_dp.m_page_num, m_state_num + 1);
                    continue;
                }
                // end of iteration since the queried state number was reached
                if (state_num > m_query_state_num) {
                    return false;
                }
                // must position after the full-DP item
                if (state_num < m_full_dp.m_state_num) {
                    continue;
                }
                m_state_num = state_num;
            } else {
                // retrieve the first diff-item
                storage_page_num = m_diff_dp.m_storage_page_num;
                state_num = m_diff_dp.m_state_num;
                m_state_num = state_num;
                m_diff_it = m_diff_dp.beginDiff();
                // must position after the full-DP item
                if (m_diff_dp.m_state_num < m_full_dp.m_state_num) {
                    continue;
                }
            }
            return true;
        }
    }

    bool SparseIndexQuery::lessThan(unsigned int size) const
    {
        // unable to iterate past the queried state number
        if (m_state_num >= m_query_state_num) {
            return true;
        }
        auto diff_dp = m_diff_dp;
        auto diff_it = m_diff_it;
        auto last_state_num = m_state_num;
        std::uint32_t state_num = 0;
        while (size > 0) {
            if (!diff_dp || diff_dp.m_state_num > m_query_state_num) {
                return true;
            }
            if (diff_it) {
                if (!diff_it.next(state_num)) {
                    diff_it.reset();
                    // try locating the next diff-DP
                    diff_dp = m_diff_index.findUpper(diff_dp.m_page_num, last_state_num + 1);                    
                    continue;
                }
                // end of iteration since the queried state number was reached
                if (state_num > m_query_state_num) {
                    return true;
                }
                // must position after the full-DP item
                if (state_num < m_full_dp.m_state_num) {
                    continue;
                }
                last_state_num = state_num;
            } else {
                // retrieve the first diff-item
                state_num = m_diff_dp.m_state_num;
                last_state_num = state_num;
                diff_it = diff_dp.beginDiff();
                // must position after the full-DP item
                if (diff_dp.m_state_num < m_full_dp.m_state_num) {
                    continue;
                }
            }
            --size;         
        }
        return false;
    }
    
    bool tryFindMutation(const SparseIndex &sparse_index, const DiffIndex &diff_index, std::uint64_t page_num,
        std::uint64_t state_num, std::uint64_t &mutation_id)
    {
        // query the diff index first
        mutation_id = diff_index.findLower(page_num, state_num);
        auto item  = sparse_index.lookup(page_num, state_num);
        if (!item) {
            // mutation only exists in the diff index
            return mutation_id != 0;
        }
        // take max from the sparse index and diff index
        mutation_id = std::max((std::uint64_t)item.m_state_num, mutation_id);
        return true;
    }
    
}