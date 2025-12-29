// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <utility>
#include <string>
#include "SparseIndexBase.hpp"
#include <dbzero/core/compiler_attributes.hpp>
    
namespace db0

{
    
    struct SI_Item;
    struct SI_CompressedItem;

    struct SI_ItemCompT
    {
        bool operator()(const SI_Item &, const SI_Item &) const;

        bool operator()(const SI_Item &, std::pair<std::uint64_t, std::uint32_t>) const;

        bool operator()(std::pair<std::uint64_t, std::uint32_t>, const SI_Item &) const;
    };

    struct SI_ItemEqualT
    {
        bool operator()(const SI_Item &, const SI_Item &) const;

        bool operator()(const SI_Item &, std::pair<std::uint64_t, std::uint32_t>) const;

        bool operator()(std::pair<std::uint64_t, std::uint32_t>, const SI_Item &) const;
    };

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR SI_Item
    {
        using CompT = SI_ItemCompT;
        using EqualT = SI_ItemEqualT;

        // the logical page number
        std::uint64_t m_page_num = 0;
        // the state number (>0 for valid items)
        std::uint32_t m_state_num = 0;
        // the physical / storage page number (possibly relative!)
        std::uint64_t m_storage_page_num = 0;
        
        SI_Item() = default;

        SI_Item(std::uint64_t page_num, std::uint32_t state_num)
            : m_page_num(page_num)
            , m_state_num(state_num) 
        {                
        }
        
        SI_Item(std::uint64_t page_num, std::uint32_t state_num, std::uint64_t storage_page_num)
            : m_page_num(page_num)
            , m_state_num(state_num)
            , m_storage_page_num(storage_page_num)        
        {                
        }

        bool operator==(const SI_Item &) const;

        inline operator bool() const {
            return m_state_num;
        }
        
        std::string toString() const;
    };
DB0_PACKED_END

    struct SI_CompressedItemCompT
    {
        bool operator()(const SI_CompressedItem &, const SI_CompressedItem &) const;
    };

    struct SI_CompressedItemEqualT
    {
        bool operator()(const SI_CompressedItem &, const SI_CompressedItem &) const;
    };
    
    // Compressed items are actual in-memory representation
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR SI_CompressedItem
    {
        using CompT = SI_CompressedItemCompT;
        using EqualT = SI_CompressedItemEqualT;

        // construct SI-compressed item relative to the specific page number - i.e. first_page_num
        SI_CompressedItem(std::uint32_t first_page_num, const SI_Item &);
        // construct SI-compressed item for comparison purposes (incomplete)
        SI_CompressedItem(std::uint32_t first_page_num, std::uint64_t page_num, std::uint32_t state_num);

        // high bits include (in this order)
        // 1. relative logical page number (24 bits)
        // 2. state number (32 bits)
        // 3. physical page number (8 highest bits)
        std::uint64_t m_high_bits;
        // low bits = physical page number (lower 32 bits)
        std::uint32_t m_low_bits;

        inline std::uint64_t getCompressedPageNum() const {
            return m_high_bits >> 40;
        }

        inline std::uint32_t getStateNum() const {
            return (m_high_bits >> 8) & 0xFFFFFFFF;
        }

        // get page_num + state_num for comparisons
        inline std::uint64_t getKey() const {
            return m_high_bits >> 8;
        }
        
        // retrieve physical (storage) page number
        std::uint64_t getStoragePageNum() const;
        
        // uncompress relative to a specific page number
        SI_Item uncompress(std::uint32_t first_page_num) const;

        inline std::uint64_t getPageNum(std::uint32_t first_page_num) const {
            return this->getCompressedPageNum() | (static_cast<std::uint64_t>(first_page_num) << 24);
        }

        std::string toString() const;
    };
DB0_PACKED_END

    using SparseIndex = SparseIndexBase<SI_Item, SI_CompressedItem>;

}

namespace std

{
    
    ostream &operator<<(ostream &, const db0::SI_Item &);

}