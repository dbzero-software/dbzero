#include "SparseIndexQuery.hpp"

namespace db0

{

    SparseIndexQuery::SparseIndexQuery(const SparseIndex &sparse_index, const DiffIndex &diff_index,
        std::uint64_t page_num, std::uint32_t state_num)
        : m_query_state_num(state_num)
        , m_state_num(state_num)
        // will be initialized with 0 if not found
        , m_full_dp(sparse_index.lookup(page_num, state_num))
        , m_diff_index(diff_index)
    {
        if (m_full_dp && m_full_dp.m_state_num < state_num) {
            m_diff_dp = m_diff_index.findUpper(page_num, m_full_dp.m_state_num);
        }
    }
    
    bool SparseIndexQuery::next(std::uint32_t &state_num, std::uint64_t &storage_page_num)
    {
        for (;;) {
            if (!m_diff_dp || m_diff_dp.m_state_num > m_query_state_num) {
                return false;
            }
            if (m_diff_it) {
                if (!m_diff_it.next(state_num, storage_page_num)) {
                    m_diff_it.reset();
                    // try locating the next diff-DP
                    m_diff_dp = m_diff_index.findUpper(m_diff_dp.m_page_num, m_state_num);
                    continue;
                }
                // end of iteration since the queried state number was reached
                if (state_num > m_query_state_num) {
                    return false;
                }
                m_state_num = state_num;
            } else {
                // retrieve the first diff-item
                storage_page_num = m_diff_dp.m_storage_page_num;
                state_num = m_diff_dp.m_state_num;
                m_state_num = state_num;
                m_diff_it = m_diff_dp.beginDiff();
            }
            return true;
        }
    }
    
}