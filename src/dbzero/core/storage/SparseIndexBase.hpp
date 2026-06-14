// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/dram/DRAMSpace.hpp>
#include "StorageRootMetadata.hpp"

namespace db0
{
    // Forward declarations for operator<< to be used in SGB_LookupTree.hpp
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT> class SparseIndexBase;
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    std::ostream &operator<<(std::ostream &os, const typename db0::SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader &header);
}

#include <dbzero/core/collections/SGB_Tree/SGB_CompressedLookupTree.hpp>
#include <dbzero/core/collections/rle/RLE_Sequence.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/dram/MS_Address.hpp>
#include <limits>
#include <new>
#include <optional>

namespace db0

{
    
    class DRAM_Prefix;
    class DRAM_Allocator;    

    /**
     * The in-memory sparse index implementation
     * it utilizes DRAMSpace (in-memory) for storage and SGB_Tree as the data structure
     * @tparam KeyT the key type (logical page number + state number)
     * @tparam ItemT the (uncompressed item type) for operations
     * @tparam CompressedItemT the compressed item type for storage
    */
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT = StorageRootMetadataMixin>
    class SparseIndexBase
    {
    public:
        using SI_ItemT = ItemT;
        using SI_CompressedItemT = CompressedItemT;
        using MixInT = SparseIndexMixinT;
        using TreeHeaderMixinT = typename SparseIndexMixinT::OverlayT;
        using MixInAPIT = typename SparseIndexMixinT::template ApiT<SparseIndexBase>;
        using PageNumT = std::uint64_t;
        using StateNumT = std::uint32_t;
        using ItemCompT = typename ItemT::CompT;
        using ItemEqualT = typename ItemT::EqualT;
        using CompressedItemCompT = typename CompressedItemT::CompT;
        using CompressedItemEqualT = typename CompressedItemT::EqualT;
        using SlotId = Allocator::SlotId;
        
        // Create a new empty sparse index
        struct tag_create {};
        SparseIndexBase(tag_create, DRAM_Pair, std::vector<std::uint64_t> *change_log_ptr = nullptr,
            SlotId slot_num = 0);
        
        /**
         * Create pre-populated with existing data (e.g. after reading from disk)
         * open either for read or read/write
         * @param address pass 0 to use the first assigned address
        */
        SparseIndexBase(DRAM_Pair, AccessType, Address, std::vector<std::uint64_t> *change_log_ptr = nullptr,
            StorageFlags= {}, SlotId slot_num = 0);
        
        void insert(const ItemT &item);

        template <typename... Args> void emplace(Args&&... args) {
            insert(ItemT(std::forward<Args>(args)...));
        }

        /**
         * Replace older descriptors for a page with a descriptor for the supplied state.
         *
         * This is intended for compaction-style rewrites that publish a new full-DP
         * as the only remaining descriptor for a logical page.
         */
        void update(PageNumT page_num, StateNumT state_num, std::uint64_t storage_page_num);

        /**
         * Erase a single descriptor identified by an exact key.
         *
         * @param page_num logical page number of the descriptor to erase
         * @param state_num state number of the descriptor to erase
         * @return true if a descriptor was erased, false if no exact descriptor exists
         */
        bool erase(PageNumT page_num, StateNumT state_num);

        /**
         * Erase descriptors for a page in the half-open state range [first_state_num, last_state_num).
         *
         * @param page_num logical page number whose descriptors should be erased
         * @param first_state_num optional inclusive lower state bound; if empty, erase from the first state on page_num
         * @param last_state_num optional exclusive upper state bound; if empty, erase through the last state on page_num
         * @return number of descriptors erased
         */
        std::size_t eraseRange(PageNumT page_num, std::optional<StateNumT> first_state_num = {},
            std::optional<StateNumT> last_state_num = {});

        /**
         * Erase descriptors for a page with state numbers below state_num.
         *
         * @param page_num logical page number whose descriptors should be erased
         * @param state_num exclusive upper state bound
         * @return number of descriptors erased
         */
        std::size_t eraseBelow(PageNumT page_num, StateNumT state_num);

        /**
         * Erase all descriptors while preserving tree-header mix-in data.
         */
        void clear();
        
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
         * Refresh cache after underlying DRAM has been updated
        */
        void refresh();

        void detach() const;
                
        void forAll(std::function<void(const ItemT &)> callback) const {
            m_index.forAll(callback);
        }

        void forPageRange(PageNumT first_page_num, PageNumT last_page_num,
            std::function<void(const ItemT &)> callback) const;
        // Iterate over unique pages only (ignoring entries for different state numbers)
        void forUniquePageRange(PageNumT first_page_num, PageNumT last_page_num,
            std::function<void(const ItemT &)> callback) const;            
        
        auto cbegin() const {
            return m_index.cbegin();
        }

        auto sortedBeginFrom(const ItemT &first) const {
            return m_index.sortedBeginFrom(first);
        }
        
        bool empty() const;

        // Get the total number of data page descriptors stored in the index
        std::size_t size() const;

        void commit() const;

        bool operator!() const;

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

            bool canFit(std::pair<PageNumT, StateNumT>) const;
            bool canFit(const ItemT &) const;

            std::string toString(const CompressedItemT &) const;
            std::string toString() const;
        };
        
        Address getIndexAddress() const;

        /**
         * Access metadata colocated with the index root page.
         *
         * The mix-in is intentionally embedded in the sparse-index tree header
         * rather than stored in a separate object: small collections and limited
         * updates often dirty the root page anyway, so colocating tiny metadata
         * avoids forcing an additional dirty metadata page.
         */
        const MixInAPIT &mixIn() const;

        /**
         * Mutating access to the colocated metadata API.
         *
         * Use this when the storage owner updates root-level metadata that is
         * logically separate from sparse-index descriptor operations but shares
         * the root page to reduce write amplification for small updates.
         */
        MixInAPIT &modifyMixIn();

    protected:
        template <typename ConfigT> friend class SparsePairBase;
        template <typename BaseT> friend class MetadataAPI;
        template <typename BaseT> friend class StorageRootMetadataAPI;

        // DRAM space deployed sparse index (in-memory)
        using IndexT = SGB_CompressedLookupTree<
            ItemT, CompressedItemT, BlockHeader,
            ItemCompT, CompressedItemCompT, ItemEqualT, CompressedItemEqualT,
            TreeHeaderMixinT>;
        
        using ConstNodeIterator = typename IndexT::sg_tree_const_iterator;
        using ConstItemIterator = typename IndexT::ConstItemIterator;

        const CompressedItemT *lowerEqualBound(PageNumT, StateNumT, ConstNodeIterator &) const;

        ConstItemIterator findLower(PageNumT, StateNumT) const;
        
        void recordChange(PageNumT page_num);
        
    private:
        std::shared_ptr<DRAM_Prefix> m_dram_prefix;
        std::shared_ptr<DRAM_Allocator> m_dram_allocator;
        Memspace m_dram_space;
        const AccessType m_access_type;
        // slot ID is required to properly allocate SparseIndex nodes        
        SlotId m_slot_num = 0;
        // the actual index
        IndexT m_index;
        MixInAPIT m_mixin_api;
        // change log contains the list of updates (modified items / page numbers)
        std::vector<std::uint64_t> *m_change_log_ptr = nullptr;
        
        IndexT openIndex(Address, AccessType access_type, StorageFlags);
        IndexT createIndex();
    };
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::SparseIndexBase(DRAM_Pair dram_pair, AccessType access_type, Address address,
        std::vector<std::uint64_t> *change_log_ptr, StorageFlags flags, SlotId slot_num)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_access_type(access_type)
        , m_slot_num(slot_num)
        , m_index(openIndex(address, access_type, flags))
        , m_mixin_api(*this)
        , m_change_log_ptr(change_log_ptr)
    {
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::SparseIndexBase(tag_create, DRAM_Pair dram_pair,
        std::vector<std::uint64_t> *change_log_ptr, SlotId slot_num)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_access_type(AccessType::READ_WRITE)
        , m_slot_num(slot_num)
        , m_index(createIndex())
        , m_mixin_api(*this)
        , m_change_log_ptr(change_log_ptr)
    {
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    void SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::recordChange(PageNumT page_num)
    {
        if (m_change_log_ptr) {
            m_change_log_ptr->push_back(MS_Address::encode(m_slot_num, page_num));
        }
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    void SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::update(PageNumT page_num, StateNumT state_num,
        std::uint64_t storage_page_num)
    {
        this->eraseBelow(page_num, state_num);
        m_index.insert(ItemT(page_num, state_num, storage_page_num));
        this->recordChange(page_num);
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    void SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::insert(const ItemT &item)
    {
        m_index.insert(item);
        this->recordChange(item.m_page_num);
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    void SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::forPageRange(PageNumT first_page_num, PageNumT end_page_num,
        std::function<void(const ItemT &)> callback) const
    {
        m_index.forRange(
            ItemT(first_page_num, 0),
            ItemT(end_page_num, 0),
            std::move(callback)
        );
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    void SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::forUniquePageRange(PageNumT first_page_num, PageNumT end_page_num,
        std::function<void(const ItemT &)> callback) const
    {
        std::optional<PageNumT> last_page_num;
        // NOTE: since forRange iterates in ascending order we can de-duplicate pages 
        // on the fly by tracking the last seen page number
        m_index.forRange(
            ItemT(first_page_num, 0),
            ItemT(end_page_num, 0),
            [&](const ItemT &item) {
                if (!last_page_num || item.m_page_num != *last_page_num) {
                    callback(item);
                    last_page_num = item.m_page_num;
                }
            }
        );
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    bool SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::erase(PageNumT page_num, StateNumT state_num)
    {
        if (!m_index.erase_equal(std::make_pair(page_num, state_num))) {
            return false;
        }
        return true;
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    std::size_t SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::eraseBelow(PageNumT page_num, StateNumT state_num)
    {
        return eraseRange(page_num, {}, state_num);
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    std::size_t SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::eraseRange(PageNumT page_num,
        std::optional<StateNumT> first_state_num, std::optional<StateNumT> last_state_num)
    {
        auto first = ItemT(page_num, first_state_num.value_or(0));
        if (last_state_num) {
            return m_index.erase_range(first, ItemT(page_num, *last_state_num));
        }
        if (page_num != std::numeric_limits<PageNumT>::max()) {
            return m_index.erase_range(first, ItemT(page_num + 1, 0));
        }

        auto removed = m_index.erase_range(first, ItemT(page_num, std::numeric_limits<StateNumT>::max()));
        removed += m_index.erase_equal(std::make_pair(page_num, std::numeric_limits<StateNumT>::max())) ? 1 : 0;
        return removed;
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    void SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::clear()
    {
        m_index.clear();
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    typename SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::IndexT
    SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::openIndex(Address address, AccessType access_type, StorageFlags flags)
    {
        assert((!m_dram_prefix->empty() || flags[StorageFlagOption::NO_LOAD])
            && "SparseIndexBase::openIndex: DRAM prefix is empty"
        );
        // NOTE: Index NOT opened if NO_LOAD flag is set
        if (flags[StorageFlagOption::NO_LOAD]) {
            return {};
        } else {
            // Use the first address if no specified
            // this is the default address where the SparseIndex is located
            if (!address) {
                address = m_dram_allocator->firstAlloc(m_slot_num);
            }            
            return IndexT(m_dram_space.myPtr(address), m_dram_prefix->getPageSize(), access_type,
                {}, {}, {}, IndexT::DEFAULT_SORT_THRESHOLD, m_slot_num);
        }
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    typename SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::IndexT
    SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::createIndex() 
    {
        // Sparse Index is created at the root address (or the slot's first address)
        return IndexT(m_dram_space, m_dram_prefix->getPageSize(), AccessType::READ_WRITE,
            {}, {}, {}, IndexT::DEFAULT_SORT_THRESHOLD, m_slot_num);
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    const DRAM_Prefix &SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::getDRAMPrefix() const {
        return *m_dram_prefix;
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    CompressedItemT SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader::compressFirst(const ItemT &item) 
    {
        m_first_page_num = item.m_page_num >> 24;
        return CompressedItemT(m_first_page_num, item);
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    CompressedItemT SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader::compress(const ItemT &item) const
    {
        assert(m_first_page_num == (item.m_page_num >> 24));
        return CompressedItemT(m_first_page_num, item);
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    CompressedItemT SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader::compress(std::pair<PageNumT, StateNumT> item) const
    {
        assert(m_first_page_num == (item.first >> 24));
        return CompressedItemT(m_first_page_num, item.first, item.second);
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    ItemT SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader::uncompress(const CompressedItemT &item) const {
        return item.uncompress(this->m_first_page_num);
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    typename SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::PageNumT 
    SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader::getPageNum(const CompressedItemT &item) const {
        return item.getPageNum(this->m_first_page_num);
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    bool SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader::canFit(const ItemT &item) const {
        return this->m_first_page_num == (item.m_page_num >> 24);
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    bool SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader::canFit(std::pair<PageNumT, StateNumT> item) const 
    {
        return this->m_first_page_num == (item.first >> 24);
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    ItemT SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::lookup(PageNumT page_num, StateNumT state_num) const {
        return lookup(std::make_pair(page_num, state_num));
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    ItemT SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::lookup(std::pair<PageNumT, StateNumT> page_and_state) const
    {
        auto result = m_index.lower_equal_bound(page_and_state);
        if (!result || result->m_page_num != page_and_state.first) {
            return {};
        }
        return *result;
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    ItemT SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::lookup(const ItemT &item) const
    {
        auto result = m_index.lower_equal_bound(item);
        if (!result || result->m_page_num != item.m_page_num) {
            return {};
        }
        return *result;
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    void SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::refresh()
    {
        assert(!!m_index && "SparseIndexBase::refresh: index is not open");
        m_index.detach();
        m_mixin_api.refresh();
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    void SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::detach() const
    {
        m_index.detach();        
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    std::string SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader::toString(const CompressedItemT &item) const {
        return item.toString();
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    std::string SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::BlockHeader::toString() const 
    {
        std::stringstream _str;
        _str << "BlockHeader { first_page_num: " << m_first_page_num << " }";
        return _str.str();
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    bool SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::empty() const {
        return m_index.empty();
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    std::size_t SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::size() const {
        return m_index.size();
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    const CompressedItemT *SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::lowerEqualBound(
        PageNumT page_num, StateNumT state_num, ConstNodeIterator &node) const
    {
        return m_index.lower_equal_bound(std::make_pair(page_num, state_num), node);
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    ItemT SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::findUpper(PageNumT page_num, StateNumT state_num) const
    {
        auto result = m_index.upper_equal_bound(std::make_pair(page_num, state_num));
        if (!result || result->m_page_num != page_num) {
            return {};
        }
        return *result;
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    Address SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::getIndexAddress() const {
        return m_index.getAddress();
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    const typename SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::MixInAPIT &
    SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::mixIn() const {
        return m_mixin_api;
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    typename SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::MixInAPIT &
    SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::modifyMixIn() {
        return m_mixin_api;
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    typename SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::ConstItemIterator    
    SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::findLower(PageNumT page_num, StateNumT state_num) const {
        return m_index.findLower(std::make_pair(page_num, state_num));
    }

    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    void SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::commit() const {
        m_index.commit();        
    }
    
    template <typename ItemT, typename CompressedItemT, typename SparseIndexMixinT>
    bool SparseIndexBase<ItemT, CompressedItemT, SparseIndexMixinT>::operator!() const {
        return !m_index;
    }

}
