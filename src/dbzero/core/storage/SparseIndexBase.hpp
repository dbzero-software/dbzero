#pragma once

#include <dbzero/core/dram/DRAMSpace.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_CompressedLookupTree.hpp>
#include <dbzero/core/collections/rle/RLE_Sequence.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include "ChangeLogIOStream.hpp"

namespace db0

{
    
    class DRAM_Prefix;
    class DRAM_Allocator;
    class ChangeLogIOStream;
    
    /**
     * The in-memory sparse index implementation
     * it utilizes DRAMSpace (in-memory) for storage and SGB_Tree as the data structure
     * @tparam KeyT the key type (logical page number + state number)
     * @tparam ItemT the (uncompressed item type) for operations
     * @tparam CompressedItemT the compressed item type for storage
    */
    template <typename ItemT, typename CompressedItemT> class SparseIndexBase
    {
    public:
        using SI_ItemT = ItemT;
        using SI_CompressedItemT = CompressedItemT;
        using PageNumT = std::uint64_t;
        using StateNumT = std::uint32_t;
        using ItemCompT = typename ItemT::CompT;
        using ItemEqualT = typename ItemT::EqualT;
        using CompressedItemCompT = typename CompressedItemT::CompT;
        using CompressedItemEqualT = typename CompressedItemT::EqualT;

        /**
         * Create empty as read/write
         * @param node_size size of a single in-memory data block / node
        */
        SparseIndexBase(std::size_t node_size);
        
        /**
         * Create pre-populated with existing data (e.g. after reading from disk)
         * open either for read or read/write
        */
        SparseIndexBase(DRAM_Pair, AccessType);

        void insert(const ItemT &item);

        template <typename... Args> void emplace(Args&&... args) {
            insert(ItemT(std::forward<Args>(args)...));
        }
        
        /**
         * Note that 'lookup' may fail in presence of duplicate items, the behavior is undefined
         * @return false item if not found
        */
        ItemT lookup(const ItemT &item) const;
        
        ItemT lookup(PageNumT page_num, StateNumT state_num) const;
        
        ItemT lookup(std::pair<PageNumT, StateNumT> page_and_state) const;

        // Locate the item with equal page_num and state number >= state_num
        ItemT findUpper(PageNumT, StateNumT) const;

        const DRAM_Prefix &getDRAMPrefix() const;

        /**
         * Get next storage page number expected to be assigned
        */
        PageNumT getNextStoragePageNum() const;
        
        /**
         * Get the maximum used state number
        */
        StateNumT getMaxStateNum() const;
                
        /**
         * Refresh cache after underlying DRAM has been updated
        */
        void refresh();

        /**
         * Write internally managed change log into a specific stream 
         * and then clean the internal change log
        */
        const o_change_log &extractChangeLog(ChangeLogIOStream &changelog_io);
        
        void forAll(std::function<void(const ItemT &)> callback) const {
            m_index.forAll(callback);
        }
        
        std::size_t getChangeLogSize() const;
        
        // Get the total number of data page descriptors stored in the index
        std::size_t size() const;

    protected:

        struct BlockHeader
        {
            // number of the 1st page in a data block / node (high order bits)
            std::uint32_t m_first_page_num = 0;

            CompressedItemT compressFirst(const ItemT &);

            // Compress the key part only for lookup purposes
            CompressedItemT compress(std::pair<PageNumT, StateNumT>) const;

            CompressedItemT compress(const ItemT &) const;

            ItemT uncompress(const CompressedItemT &) const;

            // From a compressed item, retrieve the (logical) page number only
            PageNumT getPageNum(const CompressedItemT &) const;

            bool canFit(const ItemT &) const;

            std::string toString(const CompressedItemT &) const;
        };

        // tree-level header type
        struct [[gnu::packed]] o_sparse_index_header: o_fixed<o_sparse_index_header> {
            PageNumT m_next_page_num = 0;
            StateNumT m_max_state_num = 0;
        };

        // DRAM space deployed sparse index (in-memory)
        using IndexT = SGB_CompressedLookupTree<
            ItemT, CompressedItemT, BlockHeader,
            ItemCompT, CompressedItemCompT, ItemEqualT, CompressedItemEqualT,
            o_sparse_index_header>;
        
        using ConstNodeIterator = typename IndexT::sg_tree_const_iterator;

        const CompressedItemT *lowerEqualBound(PageNumT, StateNumT, ConstNodeIterator &) const;

    private:
        std::shared_ptr<DRAM_Prefix> m_dram_prefix;
        std::shared_ptr<DRAM_Allocator> m_dram_allocator;
        Memspace m_dram_space;
        // the actual index
        IndexT m_index;
        // copied from tree header (cached)
        PageNumT m_next_page_num = 0;
        StateNumT m_max_state_num = 0;
        // change log contains the list of updates (modified items / page numbers)
        // first element is the state number
        std::vector<std::uint64_t> m_change_log;
        
        IndexT openIndex(AccessType);
    };

    template <typename ItemT, typename CompressedItemT>
    SparseIndexBase<ItemT, CompressedItemT>::SparseIndexBase(std::size_t node_size)
        : m_dram_space(DRAMSpace::create(node_size, [this](DRAM_Pair dram_pair) {
            this->m_dram_prefix = dram_pair.first;
            this->m_dram_allocator = dram_pair.second;
        }))
        , m_index(m_dram_space, node_size, AccessType::READ_WRITE)
    {
        m_index.modifyTreeHeader().m_next_page_num = m_next_page_num;
        m_index.modifyTreeHeader().m_max_state_num = m_max_state_num;
    }

    template <typename ItemT, typename CompressedItemT>
    SparseIndexBase<ItemT, CompressedItemT>::SparseIndexBase(DRAM_Pair dram_pair, AccessType access_type)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_index(openIndex(access_type))
        , m_next_page_num(m_index.treeHeader().m_next_page_num)
        , m_max_state_num(m_index.treeHeader().m_max_state_num)
    {
    }
    
    template <typename ItemT, typename CompressedItemT>
    void SparseIndexBase<ItemT, CompressedItemT>::insert(const ItemT &item)
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

    template <typename ItemT, typename CompressedItemT>
    typename SparseIndexBase<ItemT, CompressedItemT>::IndexT
    SparseIndexBase<ItemT, CompressedItemT>::openIndex(AccessType access_type)
    {
        if (m_dram_prefix->empty()) {
            // create new on top of existing DRAMSpace
            return IndexT(m_dram_space, m_dram_prefix->getPageSize(), access_type);
        } else {
            // open existing under first addres assigned to the DRAMSpace
            auto address = m_dram_allocator->firstAlloc();            
            return IndexT(m_dram_space.myPtr(address), m_dram_prefix->getPageSize(), access_type);
        }
    }

    template <typename ItemT, typename CompressedItemT>
    const DRAM_Prefix &SparseIndexBase<ItemT, CompressedItemT>::getDRAMPrefix() const {
        return *m_dram_prefix;
    }

    template <typename ItemT, typename CompressedItemT>
    CompressedItemT SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::compressFirst(const ItemT &item) 
    {
        m_first_page_num = item.m_page_num >> 24;
        return CompressedItemT(m_first_page_num, item);
    }

    template <typename ItemT, typename CompressedItemT>
    CompressedItemT SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::compress(const ItemT &item) const
    {
        assert(m_first_page_num == (item.m_page_num >> 24));
        return CompressedItemT(m_first_page_num, item);
    }

    template <typename ItemT, typename CompressedItemT>
    CompressedItemT SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::compress(std::pair<PageNumT, StateNumT> item) const
    {
        assert(m_first_page_num == (item.first >> 24));
        return CompressedItemT(m_first_page_num, item.first, item.second);
    }
    
    template <typename ItemT, typename CompressedItemT>
    ItemT SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::uncompress(const CompressedItemT &item) const {
        return item.uncompress(this->m_first_page_num);
    }

    template <typename ItemT, typename CompressedItemT>
    typename SparseIndexBase<ItemT, CompressedItemT>::PageNumT 
    SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::getPageNum(const CompressedItemT &item) const {
        return item.getPageNum(this->m_first_page_num);
    }

    template <typename ItemT, typename CompressedItemT>
    bool SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::canFit(const ItemT &item) const {
        return this->m_first_page_num == (item.m_page_num >> 24);
    }

    template <typename ItemT, typename CompressedItemT>
    ItemT SparseIndexBase<ItemT, CompressedItemT>::lookup(PageNumT page_num, StateNumT state_num) const {
        return lookup(std::make_pair(page_num, state_num));
    }
    
    template <typename ItemT, typename CompressedItemT>
    ItemT SparseIndexBase<ItemT, CompressedItemT>::lookup(std::pair<PageNumT, StateNumT> page_and_state) const
    {
        auto result = m_index.lower_equal_bound(page_and_state);
        if (!result || result->m_page_num != page_and_state.first) {
            return {};
        }
        return *result;
    }
    
    template <typename ItemT, typename CompressedItemT>
    ItemT SparseIndexBase<ItemT, CompressedItemT>::lookup(const ItemT &item) const
    {
        auto result = m_index.lower_equal_bound(item);
        if (!result || result->m_page_num != item.m_page_num) {
            return {};
        }
        return *result;
    }
    
    template <typename ItemT, typename CompressedItemT>
    typename SparseIndexBase<ItemT, CompressedItemT>::PageNumT 
    SparseIndexBase<ItemT, CompressedItemT>::getNextStoragePageNum() const {
        return m_next_page_num;
    }

    template <typename ItemT, typename CompressedItemT>
    typename SparseIndexBase<ItemT, CompressedItemT>::StateNumT
    SparseIndexBase<ItemT, CompressedItemT>::getMaxStateNum() const {
        return m_max_state_num;
    }
    
    template <typename ItemT, typename CompressedItemT>
    void SparseIndexBase<ItemT, CompressedItemT>::refresh()
    {
        m_next_page_num = m_index.treeHeader().m_next_page_num;
        m_max_state_num = m_index.treeHeader().m_max_state_num;
        m_index.detach();
    }
    
    template <typename ItemT, typename CompressedItemT>
    const o_change_log &SparseIndexBase<ItemT, CompressedItemT>::extractChangeLog(ChangeLogIOStream &changelog_io)
    {        
        assert(!m_change_log.empty());
        // sort change log but keep the 1st item (the state number) at its place
        if (!m_change_log.empty()) {
            std::sort(m_change_log.begin() + 1, m_change_log.end());
        }
        
        ChangeLogData cl_data;
        auto it = m_change_log.begin(), end = m_change_log.end();
        // first item is the state number
        cl_data.m_rle_builder.append(*(it++), true);
        // the first page number (second item) must be added even if it is identical as the state number
        if (it != end) {
            cl_data.m_rle_builder.append(*(it++), true);
        }
        // all remaining items add with deduplication
        for (; it != end; ++it) {
            cl_data.m_rle_builder.append(*it, false);
        }
        
        // RLE encode, no duplicates
        auto &result = changelog_io.appendChangeLog(std::move(cl_data));
        m_change_log.clear();
        return result;
    }
    
    template <typename ItemT, typename CompressedItemT>
    std::string SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::toString(const CompressedItemT &item) const {
        return item.toString();
    }
    
    template <typename ItemT, typename CompressedItemT>
    std::size_t SparseIndexBase<ItemT, CompressedItemT>::getChangeLogSize() const {
        return m_change_log.empty() ? 0 : m_change_log.size() - 1;
    }
    
    template <typename ItemT, typename CompressedItemT>
    std::size_t SparseIndexBase<ItemT, CompressedItemT>::size() const {
        return m_index.size();
    }

    template <typename ItemT, typename CompressedItemT>
    const CompressedItemT *SparseIndexBase<ItemT, CompressedItemT>::lowerEqualBound(
        PageNumT page_num, StateNumT state_num, ConstNodeIterator &node) const
    {
        return m_index.lower_equal_bound(std::make_pair(page_num, state_num), node);
    }
    
    template <typename ItemT, typename CompressedItemT>
    ItemT SparseIndexBase<ItemT, CompressedItemT>::findUpper(PageNumT page_num, StateNumT state_num) const
    {
        auto result = m_index.upper_equal_bound(std::make_pair(page_num, state_num));
        if (!result || result->m_page_num != page_num) {
            return {};
        }
        return *result;
    }

}       
