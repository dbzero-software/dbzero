// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SparsePair.hpp"
#include <dbzero/core/dram/DRAMSpace.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/utils.hpp>

namespace db0

{
    namespace
    {
        template <typename SparseIndexT>
        class SparsePairUniquePageRangeIterator
        {
        public:
            using PageNumT = typename SparseIndexT::PageNumT;
            using SparseIteratorT = decltype(std::declval<const SparseIndexT &>().sortedBeginFrom(SI_Item()));
            using DiffIteratorT = decltype(std::declval<const DiffIndex &>().sortedBeginFrom(DI_Item()));

            SparsePairUniquePageRangeIterator(const SparseIndexT &sparse_index, const DiffIndex &diff_index,
                PageNumT first_page_num, PageNumT end_page_num)
                : m_sparse_it(sparse_index.sortedBeginFrom(SI_Item(first_page_num, 0)))
                , m_diff_it(diff_index.sortedBeginFrom(DI_Item(first_page_num, 0)))
                , m_end_page_num(end_page_num)
            {
                m_sparse_page_num = currentPageFrom(m_sparse_it);
                m_diff_page_num = currentPageFrom(m_diff_it);
                m_current = fromRange(selectCurrent());
            }

            bool is_end() const {
                return !m_current;
            }

            PageNumT operator*() const {
                assert(m_current);
                return *m_current;
            }

            SparsePairUniquePageRangeIterator &operator++()
            {
                assert(m_current);
                advancePast(*m_current);
                m_current = fromRange(selectCurrent());
                return *this;
            }

        private:
            SparseIteratorT m_sparse_it;
            DiffIteratorT m_diff_it;
            PageNumT m_end_page_num = 0;
            std::optional<PageNumT> m_sparse_page_num;
            std::optional<PageNumT> m_diff_page_num;
            std::optional<PageNumT> m_current;

            template <typename IteratorT>
            std::optional<PageNumT> currentPageFrom(const IteratorT &it) const
            {
                if (it.is_end()) {
                    return std::nullopt;
                }

                auto item = *it;
                PageNumT page_num = item.m_page_num;
                return page_num;
            }

            void advancePast(PageNumT page_num)
            {
                if (m_sparse_page_num && *m_sparse_page_num <= page_num) {
                    m_sparse_page_num = detail::advancePageIteratorPast(m_sparse_it, page_num);
                }
                if (m_diff_page_num && *m_diff_page_num <= page_num) {
                    m_diff_page_num = detail::advancePageIteratorPast(m_diff_it, page_num);
                }
            }

            std::optional<PageNumT> fromRange(std::optional<PageNumT> page_num) const
            {
                if (page_num && *page_num < m_end_page_num) {
                    return page_num;
                }
                return std::nullopt;
            }

            std::optional<PageNumT> selectCurrent() const
            {
                if (!m_diff_page_num) {
                    return m_sparse_page_num;
                } else if (!m_sparse_page_num) {
                    return m_diff_page_num;
                }
                // both available, return the smaller one
                return *m_sparse_page_num < *m_diff_page_num ? m_sparse_page_num : m_diff_page_num;
            }
        };
    }
    
    template <typename ConfigT>
    SparsePairBase<ConfigT>::SparsePairBase(DRAM_Pair dram_pair, AccessType access_type, Address root_address,
        StorageFlags flags, Allocator::SlotId slot_num, ChangeLogT *change_log)
        : m_change_log(change_log ? change_log : &m_owned_change_log)
        , m_dram_space(DRAMSpace::create(dram_pair))
        // sparse index locate at the slot's root address
        , m_sparse_index(dram_pair, access_type, root_address,
            m_change_log, flags, slot_num)
        , m_diff_index(dram_pair, access_type, getDiffIndexAddress(m_sparse_index),
            m_change_log, flags, slot_num)
    {
    }
    
    template <typename ConfigT>
    SparsePairBase<ConfigT>::SparsePairBase(tag_create, DRAM_Pair dram_pair, Allocator::SlotId slot_num,
        ChangeLogT *change_log)        
        : m_change_log(change_log ? change_log : &m_owned_change_log)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_sparse_index(typename SparseIndexT::tag_create(), dram_pair, m_change_log, slot_num)
        , m_diff_index(DiffIndex::tag_create(), dram_pair, m_change_log, slot_num)
    {
        // validate SparseIndex address
        assert(m_sparse_index.getIndexAddress() == dram_pair.second->firstAlloc(slot_num));
        // write in the Sparse Index header
        storeDiffIndexAddresses();
    }
    
    template <typename ConfigT>
    std::optional<typename SparsePairBase<ConfigT>::PageNumT> SparsePairBase<ConfigT>::getNextStoragePageNum() const
    {
        if constexpr (ConfigT::has_storage_root_metadata) {
            return m_sparse_index.mixIn().getNextStoragePageNum();
        } else {
            return std::nullopt;
        }
    }

    template <typename ConfigT>
    typename SparsePairBase<ConfigT>::StateNumT SparsePairBase<ConfigT>::getMaxStateNum() const
    {
        if constexpr (ConfigT::has_storage_root_metadata) {
            return m_sparse_index.mixIn().getMaxStateNum();
        } else {
            return 0;
        }
    }

    template <typename ConfigT>
    Address SparsePairBase<ConfigT>::getAddress() const
    {
        return m_sparse_index.getIndexAddress();
    }

    template <typename ConfigT>
    void SparsePairBase<ConfigT>::recordMaxStateNum(StateNumT state_num)
    {
        if constexpr (ConfigT::has_storage_root_metadata) {
            m_sparse_index.modifyMixIn().recordMaxStateNum(state_num);
        } else {
            (void)state_num;
        }
    }

    template <typename ConfigT>
    void SparsePairBase<ConfigT>::recordNextStoragePageNum(PageNumT next_page_num)
    {
        if constexpr (ConfigT::has_storage_root_metadata) {
            m_sparse_index.modifyMixIn().recordNextStoragePageNum(next_page_num);
        } else {
            (void)next_page_num;
        }
    }

    template <typename ConfigT>
    void SparsePairBase<ConfigT>::refresh()
    {
        m_sparse_index.refresh();
        m_diff_index.refresh();
    }
    
    template <typename ConfigT>
    void SparsePairBase<ConfigT>::detach() const
    {
        m_sparse_index.detach();
        m_diff_index.detach();    
    }

    template <typename ConfigT>
    std::size_t SparsePairBase<ConfigT>::size() const
    {
        return m_sparse_index.size() + m_diff_index.size();
    }

    template <typename ConfigT>
    bool SparsePairBase<ConfigT>::empty() const
    {
        return m_sparse_index.empty() && m_diff_index.empty();
    }

    template <typename ConfigT>
    void SparsePairBase<ConfigT>::commit() const
    {
        m_sparse_index.commit();
        m_diff_index.commit();
    }

    template <typename ConfigT>
    void SparsePairBase<ConfigT>::forUniquePageRange(PageNumT first_page_num, PageNumT end_page_num,
        std::function<void(PageNumT)> callback) const
    {
        if (first_page_num >= end_page_num) {
            return;
        }

        SparsePairUniquePageRangeIterator<SparseIndexT> it(
            m_sparse_index, m_diff_index, first_page_num, end_page_num);
        while (!it.is_end()) {
            callback(*it);
            ++it;
        }
    }
    
    template <typename ConfigT>
    std::size_t SparsePairBase<ConfigT>::getChangeLogSize() const
    {
        return m_change_log ? m_change_log->size() : 0;
    }
    
    template <typename ConfigT>
    Address SparsePairBase<ConfigT>::getDiffIndexAddress(
        const SparseIndexT &sparse_index)
    {
        return Address::fromOffset(sparse_index.mixIn().getExtraData());
    }

    template <typename ConfigT>
    void SparsePairBase<ConfigT>::storeDiffIndexAddresses()
    {
        m_sparse_index.modifyMixIn().setExtraData(m_diff_index.getIndexAddress().getOffset());        
    }
    
    template <typename ConfigT>
    typename SparsePairBase<ConfigT>::ChangeLogT SparsePairBase<ConfigT>::extractChangeLogPages()
    {
        if (m_change_log != &m_owned_change_log) {
            THROWF(db0::InternalException) << "extractChangeLogPages is only supported for SparsePair instances with owned change log";
        }
        ChangeLogT page_nums;
        page_nums.swap(m_owned_change_log);
        return page_nums;
    }

    template class SparsePairBase<RootSparsePairConfig>;
    template class SparsePairBase<PlainSparsePairConfig>;

}
