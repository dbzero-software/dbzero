// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <array>
#include <cassert>
#include <cstdint>
#include <optional>
#include <utility>

namespace db0
{

DB0_PACKED_BEGIN
    // Persisted tree-level metadata for the root storage index.
    struct DB0_PACKED_ATTR o_storage_root_metadata: o_fixed_versioned<o_storage_root_metadata>
    {
        // page_io stream positioning variable
        std::uint64_t m_next_page_num = 0;
        std::uint32_t m_max_state_num = 0;
        // The extra-data slot currently stores the paired diff-index address.
        std::uint64_t m_extra_data = 0;
        // reserved for future use
        std::array<std::uint64_t, 3> m_reserved = {0, 0, 0};
    };
DB0_PACKED_END

DB0_PACKED_BEGIN
    // Persisted tree-level metadata for the Plain Storage Index
    struct DB0_PACKED_ATTR o_plain_metadata: o_fixed_versioned<o_plain_metadata>
    {
        // The extra-data slot currently stores the paired diff-index address.
        std::uint64_t m_extra_data = 0;
        // reserved for future use
        std::array<std::uint64_t, 2> m_reserved = {0, 0};
    };
DB0_PACKED_END

    template <typename BaseT> class MetadataAPI
    {
    public:
        using PageNumT = typename BaseT::PageNumT;
        using StateNumT = typename BaseT::StateNumT;
        using SI_ItemT = typename BaseT::SI_ItemT;
        using SI_CompressedItemT = typename BaseT::SI_CompressedItemT;
        using tag_create = typename BaseT::tag_create;

        explicit MetadataAPI(BaseT &base)
            : m_base(&base)
        {
            refresh();
        }
        
        void setExtraData(std::uint64_t data)
        {
            m_base->m_index.modifyTreeHeader().m_extra_data = data;
        }

        std::uint64_t getExtraData() const
        {                
            return m_base->m_index.treeHeader().m_extra_data;
        }

        void refresh() {}

    protected:
        BaseT *m_base;
    };
    
    template <typename BaseT> 
    class StorageRootMetadataAPI: public MetadataAPI<BaseT>
    {
    public:
        using PageNumT = typename BaseT::PageNumT;
        using StateNumT = typename BaseT::StateNumT;
        using SI_ItemT = typename BaseT::SI_ItemT;
        using SI_CompressedItemT = typename BaseT::SI_CompressedItemT;
        using tag_create = typename BaseT::tag_create;

        explicit StorageRootMetadataAPI(BaseT &base)
            : MetadataAPI<BaseT>(base)
        {
            this->refresh();
        }

        void refresh()
        {
            auto &header = this->m_base->m_index.treeHeader();
            m_next_page_num = header.m_next_page_num;            
            m_max_state_num = header.m_max_state_num;
        }

        std::optional<PageNumT> getNextStoragePageNum() const
        {
            if (m_next_page_num == 0) {
                return std::nullopt;
            }
            return m_next_page_num;
        }

        StateNumT getMaxStateNum() const
        {
            return m_max_state_num;
        }

        void recordMaxStateNum(StateNumT state_num)
        {
            if (state_num >= m_max_state_num && state_num != 0) {
                m_max_state_num = state_num;
                this->m_base->m_index.modifyTreeHeader().m_max_state_num = state_num;
            }
        }

        void recordNextStoragePageNum(PageNumT next_page_num)
        {
            if (next_page_num > m_next_page_num) {
                m_next_page_num = next_page_num;
                this->m_base->m_index.modifyTreeHeader().m_next_page_num = next_page_num;
            }
        }

    private:        
        PageNumT m_next_page_num = 0;
        StateNumT m_max_state_num = 0;
    };
    
    struct StorageRootMetadataMixin
    {
        using OverlayT = o_storage_root_metadata;
        template <typename BaseT> using ApiT = StorageRootMetadataAPI<BaseT>;
    };

    struct PlainMetadataMixin
    {
        using OverlayT = o_plain_metadata;
        template <typename BaseT> using ApiT = MetadataAPI<BaseT>;
    };

    struct EmptyMixin
    {
        using OverlayT = o_plain_metadata;
        template <typename BaseT> using ApiT = MetadataAPI<BaseT>;
    };

}
