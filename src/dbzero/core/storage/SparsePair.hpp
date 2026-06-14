// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/Types.hpp>
#include "SparsePairFwd.hpp"
#include "SparseIndex.hpp"
#include "DiffIndex.hpp"
#include "BaseStorage.hpp"
#include "ChangeLogIOStream.hpp"
#include "StorageFlags.hpp"
#include <dbzero/core/dram/MS_Address.hpp>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <functional>
#include <optional>
#include <type_traits>
#include <utility>

namespace db0

{
    namespace detail
    {
        template <typename IteratorT, typename PageNumT>
        std::optional<PageNumT> advancePageIteratorPast(IteratorT &it, PageNumT page_num)
        {
            while (!it.is_end()) {
                auto item = *it;
                PageNumT item_page_num = item.m_page_num;
                if (item_page_num > page_num) {
                    return item_page_num;
                }
                ++it;
            }
            return std::nullopt;
        }
    }

    struct RootSparsePairConfig
    {
        using SparseIndexT = RootSparseIndex;        
        static constexpr bool has_storage_root_metadata = true;
    };

    struct PlainSparsePairConfig
    {
        using SparseIndexT = PlainSparseIndex;        
        static constexpr bool has_storage_root_metadata = false;
    };

    /**
     * Combines SparseIndex and DiffIndex.
     *
     * The root configuration stores storage-level high-water metadata in the
     * sparse-index root mix-in. The plain configuration is used by
     * SparsePairManager and keeps that sparse-index mix-in empty; it only adds a
         * tiny pair header so the paired sparse/diff index addresses can be opened.
     */
    template <typename ConfigT> class SparsePairBase
    {
    public:
        using Config = ConfigT;
        using SlotId = Allocator::SlotId;
        using SparseIndexT = typename ConfigT::SparseIndexT;
        using PageNumT = typename SparseIndexT::PageNumT;
        using StateNumT = typename SparseIndexT::StateNumT;
        using tag_create = typename SparseIndexT::tag_create;

        using ChangeLogT = std::vector<std::uint64_t>;
        using ChangeLogEntryT = std::uint64_t;
        
        SparsePairBase(DRAM_Pair, AccessType, Address, StorageFlags = {}, SlotId slot_num = 0,
            ChangeLogT *change_log = nullptr);
        SparsePairBase(tag_create, DRAM_Pair, SlotId slot_num = 0, ChangeLogT *change_log = nullptr);

        inline SparseIndexT &getSparseIndex() {
            return m_sparse_index;
        }

        inline const SparseIndexT &getSparseIndex() const {
            return m_sparse_index;
        }

        inline DiffIndex &getDiffIndex() {
            return m_diff_index;
        }

        inline const DiffIndex &getDiffIndex() const {
            return m_diff_index;
        }

        std::optional<PageNumT> getNextStoragePageNum() const;
        
        StateNumT getMaxStateNum() const;

        Address getAddress() const;

        void recordMaxStateNum(StateNumT state_num);

        void recordNextStoragePageNum(PageNumT);

        bool empty() const;
        std::size_t size() const;

        void refresh();

        void detach() const;
        
        void commit() const;

        void forUniquePageRange(PageNumT first_page_num, PageNumT end_page_num,
            std::function<void(PageNumT)> callback) const;
        
        std::size_t getChangeLogSize() const;

        // only supported with owned change log
        ChangeLogT extractChangeLogPages();

    private:
        // owned change log used only for non-managed root instances
        ChangeLogT m_owned_change_log;
        ChangeLogT *m_change_log;
        Memspace m_dram_space;
        // Sparse Index is created at the root address (or the slot's first address)
        // and in its header it stores the address of the diff index
        SparseIndexT m_sparse_index;
        DiffIndex m_diff_index;
        
        static Address getDiffIndexAddress(const SparseIndexT &);
        void storeDiffIndexAddresses();
    };

    extern template class SparsePairBase<RootSparsePairConfig>;
    extern template class SparsePairBase<PlainSparsePairConfig>;

}
