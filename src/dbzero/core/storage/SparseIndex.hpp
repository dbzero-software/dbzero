#pragma once

#include <dbzero/core/dram/DRAMSpace.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_CompressedLookupTree.hpp>
#include <dbzero/core/collections/rle/RLE_Sequence.hpp>
#include "ChangeLogIOStream.hpp"

namespace db0

{
    
    class DRAM_Prefix;
    class DRAM_Allocator;
    class ChangeLogIOStream;

    /**
     * The in-memory sparse index implementation
     * it utilizes DRAMSpace (in-memory) for storage and SGB_Tree as the data structure
    */
    class SparseIndex
    {
    public:

        enum class PageType: short unsigned int
        {
            // a fixed content full data page
            FIXED = 0,
            // a multiple-state page to which data can be appended
            MUTABLE = 1
        };

        /**
         * Create empty as read/write
         * @param node_size size of a single in-memory data block / node
        */
        SparseIndex(std::size_t node_size);
        
        /**
         * Create pre-populated with existing data (e.g. after reading from disk)
         * open either for read or read/write
        */
        SparseIndex(DRAM_Pair, AccessType);
        
        struct [[gnu::packed]] Item
        {
            // the logical page number
            std::uint64_t m_page_num = 0;
            // the state number (>0 for valid items)
            std::uint32_t m_state_num = 0;
            // the physical / storage page number
            std::uint64_t m_storage_page_num = 0;
            PageType m_page_type = PageType::FIXED;

            Item() = default;

            Item(std::uint64_t page_num, std::uint32_t state_num)
                : m_page_num(page_num)
                , m_state_num(state_num) 
            {                
            }
            
            Item(std::uint64_t page_num, std::uint32_t state_num, std::uint64_t storage_page_num, PageType page_type)
                : m_page_num(page_num)
                , m_state_num(state_num)
                , m_storage_page_num(storage_page_num)
                , m_page_type(page_type)
            {                
            }

            bool operator==(const Item &) const;

            inline operator bool() const {
                return m_state_num;
            }
        };
        
        struct ItemCompT
        {
            bool operator()(const Item &, const Item &) const;

            bool operator()(const Item &, std::pair<std::uint64_t, std::uint32_t>) const;

            bool operator()(std::pair<std::uint64_t, std::uint32_t>, const Item &) const;
        };
        
        struct ItemEqualT
        {
            bool operator()(const Item &, const Item &) const;

            bool operator()(const Item &, std::pair<std::uint64_t, std::uint32_t>) const;

            bool operator()(std::pair<std::uint64_t, std::uint32_t>, const Item &) const;
        };

        void insert(const Item &item)
        {
            m_index.insert(item);

            // update tree header if necessary
            if (item.m_storage_page_num >= m_next_page_num) {
                m_next_page_num = item.m_storage_page_num + 1;
                m_index.modifyTreeHeader().m_next_page_num = m_next_page_num;
            }
            if (item.m_state_num > m_max_state_num) {
                m_max_state_num = item.m_state_num;
                m_index.modifyTreeHeader().m_max_state_num = m_max_state_num;
            }
            // put the currently generated state number as the first element in the change-log
            if (m_change_log.empty()) {
                m_change_log.push_back(m_max_state_num);
            }
            m_change_log.push_back(item.m_page_num);
        }

        template <typename... Args> void emplace(Args&&... args) {
            insert(Item(std::forward<Args>(args)...));
        }
        
        /**
         * Note that 'lookup' may fail in presence of duplicate items, the behavior is undefined
         * @return false item if not found
        */
        Item lookup(const Item &item) const;

        Item lookup(std::uint64_t page_num, std::uint32_t state_num) const;

        Item lookup(std::pair<std::uint64_t, std::uint32_t> page_and_state) const;

        const DRAM_Prefix &getDRAMPrefix() const;

        /**
         * Get next storage page number expected to be assigned
        */
        std::uint64_t getNextStoragePageNum() const;
        
        /**
         * Get the maximum used state number
        */
        std::uint32_t getMaxStateNum() const;
        
        struct DeltaItem
        {
            // the state number
            std::uint32_t m_state_num;
            // logical data page number
            std::uint32_t m_page_num;
        };
        
        /**
         * Refresh cache after underlying DRAM has been updated
        */
        void refresh();

        /**
         * Write internally managed change log into a specific stream 
         * and then clean the internal change log
        */
        const o_change_log &extractChangeLog(ChangeLogIOStream &changelog_io);
        
        void forAll(std::function<void(const Item &)> callback) const {
            m_index.forAll(callback);
        }
                        
        std::size_t getChangeLogSize() const;

    private:
        
        // Compressed items are actual in-memory representation
        struct [[gnu::packed]] CompressedItem
        {
            // high bits include (in this order)
            // 1. logical page number (23 bits)
            // 2. state number (32 bits)
            // 3. page type (1 bit)
            // 4. physical page number (8 highest bits)
            std::uint64_t m_high_bits;
            // low bits = physical page number (lower 32 bits)
            std::uint32_t m_low_bits;

            inline std::uint64_t getPageNum() const {
                return m_high_bits >> 41;
            }

            inline std::uint32_t getStateNum() const {
                return (m_high_bits >> 9) & 0xFFFFFFFF;
            }

            // get page_num + state_num for comparisons
            inline std::uint64_t getKey() const {
                return m_high_bits >> 9;
            }

            PageType getPageType() const;
            
            // retrieve physical (storage) page number
            std::uint64_t getStoragePageNum() const;

            std::string toString() const;
        };
        
        struct CompressedItemCompT
        {
            bool operator()(CompressedItem, CompressedItem) const;
        };

        struct CompressedItemEqualT
        {
            bool operator()(CompressedItem, CompressedItem) const;
        };
        
        struct BlockHeader
        {
            // number of the 1st page in a data block / node
            std::uint32_t m_first_page_num;

            CompressedItem compressFirst(const Item &);

            CompressedItem compress(std::pair<std::uint64_t, std::int32_t>) const;

            CompressedItem compress(const Item &) const;

            Item uncompress(CompressedItem) const;

            bool canFit(const Item &) const;

            std::string toString(const CompressedItem &) const;
        };

        std::shared_ptr<DRAM_Prefix> m_dram_prefix;
        std::shared_ptr<DRAM_Allocator> m_dram_allocator;
        Memspace m_dram_space;

        // tree-level header type
        struct [[gnu::packed]] o_sparse_index_header: o_fixed<o_sparse_index_header> {
            std::uint64_t m_next_page_num = 0;
            std::uint32_t m_max_state_num = 0;
        };

        // DRAM space deployed sparse index (in-memory)
        using IndexT = SGB_CompressedLookupTree<
            Item, CompressedItem, BlockHeader,
            ItemCompT, CompressedItemCompT, ItemEqualT, CompressedItemEqualT,
            o_sparse_index_header>;
        
        IndexT m_index;
        // copied from tree header (cached)
        std::uint64_t m_next_page_num = 0;
        std::uint32_t m_max_state_num = 0;
        // change log contains the list of updates (modified items / page numbers)
        // first element is the state number
        std::vector<std::uint64_t> m_change_log;
        
        IndexT openIndex(AccessType);
    };

}
