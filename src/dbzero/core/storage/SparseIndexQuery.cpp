#include "SparseIndexQuery.hpp"

namespace db0

{

    SparseIndexQuery::SparseIndexQuery(const SparseIndex &sparse_index, const DiffIndex &diff_index,
        std::uint64_t page_num, std::uint32_t state_num)
        // will be initialized with 0 if not found
        : m_full_dp(sparse_index.lookup(page_num, state_num))
        , m_diff_index(diff_index)
    {
        /**
        if (m_full_dp) {
            m_diff_dp = m_diff_index.lookup(page_num, m_full_dp.m_state_num);
        }*/
    }

}