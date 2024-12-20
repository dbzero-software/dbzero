#pragma once

#include <cstdint>
#include <utility>
#include <string>

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

    struct [[gnu::packed]] SI_Item
    {
        using CompT = SI_ItemCompT;
        using EqualT = SI_ItemEqualT;

        // the logical page number
        std::uint64_t m_page_num = 0;
        // the state number (>0 for valid items)
        std::uint32_t m_state_num = 0;
        // the physical / storage page number
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
    };
    
    struct SI_CompressedItemCompT
    {
        bool operator()(const SI_CompressedItem &, const SI_CompressedItem &) const;
    };

    struct SI_CompressedItemEqualT
    {
        bool operator()(const SI_CompressedItem &, const SI_CompressedItem &) const;
    };

    // Compressed items are actual in-memory representation
    struct [[gnu::packed]] SI_CompressedItem
    {
        using CompT = SI_CompressedItemCompT;
        using EqualT = SI_CompressedItemEqualT;

        // high bits include (in this order)
        // 1. relative logical page number (24 bits)
        // 2. state number (32 bits)        
        // 3. physical page number (8 highest bits)
        std::uint64_t m_high_bits = 0;
        // low bits = physical page number (lower 32 bits)
        std::uint32_t m_low_bits = 0;

        inline std::uint64_t getPageNum() const {
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

        std::string toString() const;
    };
    
}