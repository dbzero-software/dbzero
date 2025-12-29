// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/dram/DRAMSpace.hpp>

namespace db0
{
    // Forward declarations for operator<< to be used in SGB_LookupTree.hpp
    template <typename ItemT, typename CompressedItemT> class SparseIndexBase;
    
    template <typename ItemT, typename CompressedItemT>
    std::ostream &operator<<(std::ostream &os, const typename db0::SparseIndexBase<ItemT, CompressedItemT>::BlockHeader &header);
}

#include <dbzero/core/collections/SGB_Tree/SGB_CompressedLookupTree.hpp>
#include <dbzero/core/collections/rle/RLE_Sequence.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/compiler_attributes.hpp>

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
        SparseIndexBase(std::size_t node_size, std::vector<std::uint64_t> *change_log_ptr = nullptr);
        
        /**
         * Create pre-populated with existing data (e.g. after reading from disk)
         * open either for read or read/write
         * @param address pass 0 to use the first assigned address
        */
        SparseIndexBase(DRAM_Pair, AccessType, Address address = {}, 
            std::vector<std::uint64_t> *change_log_ptr = nullptr, StorageFlags= {});
        
        // Create a new empty sparse index
        struct tag_create {};
        SparseIndexBase(tag_create, DRAM_Pair, std::vector<std::uint64_t> *change_log_ptr = nullptr);
        
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
        std::optional<PageNumT> getNextStoragePageNum() const;
        
        /**
         * Get the maximum used state number
        */
        StateNumT getMaxStateNum() const;
        
        /**
         * Refresh cache after underlying DRAM has been updated
        */
        void refresh();
                
        void forAll(std::function<void(const ItemT &)> callback) const {
            m_index.forAll(callback);
        }
        
        bool empty() const;

        // Get the total number of data page descriptors stored in the index
        std::size_t size() const;

        void commit();

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

    protected:
        friend class SparsePair;

DB0_PACKED_BEGIN
        // tree-level header type
        struct DB0_PACKED_ATTR o_sparse_index_header: o_fixed_versioned<o_sparse_index_header>
        {
            PageNumT m_next_page_num = 0;
            StateNumT m_max_state_num = 0;
            // the extra-data slot currently used to store reference to the dff-index
            std::uint64_t m_extra_data = 0;
            // reserved space for future use
            std::array<std::uint64_t, 4> m_reserved = {0, 0, 0, 0};
        };
DB0_PACKED_END

        // DRAM space deployed sparse index (in-memory)
        using IndexT = SGB_CompressedLookupTree<
            ItemT, CompressedItemT, BlockHeader,
            ItemCompT, CompressedItemCompT, ItemEqualT, CompressedItemEqualT,
            o_sparse_index_header>;
        
        using ConstNodeIterator = typename IndexT::sg_tree_const_iterator;
        using ConstItemIterator = typename IndexT::ConstItemIterator;

        const CompressedItemT *lowerEqualBound(PageNumT, StateNumT, ConstNodeIterator &) const;

        ConstItemIterator findLower(PageNumT, StateNumT) const;
        
        void setExtraData(std::uint64_t);

        std::uint64_t getExtraData() const;

        void update(std::uint64_t max_storage_page_num);
        void update(PageNumT page_num, StateNumT state_num, std::uint64_t max_storage_page_num);
        
    private:
        std::shared_ptr<DRAM_Prefix> m_dram_prefix;
        std::shared_ptr<DRAM_Allocator> m_dram_allocator;
        Memspace m_dram_space;
        const AccessType m_access_type;
        // the actual index
        IndexT m_index;
        // copied from tree header (cached)
        PageNumT m_next_page_num = 0;
        StateNumT m_max_state_num = 0;
        // change log contains the list of updates (modified items / page numbers)qweqwe
        // first element is the state number
        std::vector<std::uint64_t> *m_change_log_ptr = nullptr;
        
        IndexT openIndex(Address, AccessType access_type, StorageFlags);
        IndexT createIndex();
    };
    
    template <typename ItemT, typename CompressedItemT>
    SparseIndexBase<ItemT, CompressedItemT>::SparseIndexBase(std::size_t node_size, std::vector<std::uint64_t> *change_log_ptr)
        : m_dram_space(DRAMSpace::create(node_size, [this](DRAM_Pair dram_pair) {
            this->m_dram_prefix = dram_pair.first;
            this->m_dram_allocator = dram_pair.second;
        }))
        , m_access_type(AccessType::READ_WRITE)
        , m_index(m_dram_space, node_size, AccessType::READ_WRITE)
        , m_change_log_ptr(change_log_ptr)
    {
    }

    template <typename ItemT, typename CompressedItemT>
    SparseIndexBase<ItemT, CompressedItemT>::SparseIndexBase(DRAM_Pair dram_pair, AccessType access_type, Address address,
        std::vector<std::uint64_t> *change_log_ptr, StorageFlags flags)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_access_type(access_type)
        , m_index(openIndex(address, access_type, flags))
        // NOTE: index may NOT be loaded
        , m_next_page_num(!!m_index ? m_index.treeHeader().m_next_page_num : 0)
        , m_max_state_num(!!m_index ? m_index.treeHeader().m_max_state_num : 0)
        , m_change_log_ptr(change_log_ptr)
    {
    }

    template <typename ItemT, typename CompressedItemT>
    SparseIndexBase<ItemT, CompressedItemT>::SparseIndexBase(tag_create, DRAM_Pair dram_pair, std::vector<std::uint64_t> *change_log_ptr)
        : m_dram_prefix(dram_pair.first)
        , m_dram_allocator(dram_pair.second)
        , m_dram_space(DRAMSpace::create(dram_pair))
        , m_access_type(AccessType::READ_WRITE)
        , m_index(createIndex())
        , m_next_page_num(m_index.treeHeader().m_next_page_num)
        , m_max_state_num(m_index.treeHeader().m_max_state_num)
        , m_change_log_ptr(change_log_ptr)
    {
    }

    template <typename ItemT, typename CompressedItemT>
    void SparseIndexBase<ItemT, CompressedItemT>::update(std::uint64_t max_storage_page_num)
    {   
        // update tree header if necessary
        if (max_storage_page_num >= m_next_page_num) {
            m_next_page_num = max_storage_page_num + 1;
            m_index.modifyTreeHeader().m_next_page_num = m_next_page_num;
        }
    }
    
    template <typename ItemT, typename CompressedItemT>
    void SparseIndexBase<ItemT, CompressedItemT>::update(PageNumT page_num, StateNumT state_num, std::uint64_t max_storage_page_num)
    {
        // update tree header if necessary
        this->update(max_storage_page_num);
        if (state_num > m_max_state_num) {
            m_max_state_num = state_num;
            m_index.modifyTreeHeader().m_max_state_num = state_num;
        }
        // put the currently generated state number as the first element in the change-log
        if (m_change_log_ptr) {
            m_change_log_ptr->push_back(page_num);
        }
    }
    
    template <typename ItemT, typename CompressedItemT>
    void SparseIndexBase<ItemT, CompressedItemT>::insert(const ItemT &item)
    {
        m_index.insert(item);
        this->update(item.m_page_num, item.m_state_num, item.m_storage_page_num);
    }
    
    template <typename ItemT, typename CompressedItemT>
    typename SparseIndexBase<ItemT, CompressedItemT>::IndexT
    SparseIndexBase<ItemT, CompressedItemT>::openIndex(Address address, AccessType access_type, StorageFlags flags)
    {
        assert((!m_dram_prefix->empty() || flags[StorageOptions::NO_LOAD])
            && "SparseIndexBase::openIndex: DRAM prefix is empty"
        );
        // NOTE: Index NOT opened if NO_LOAD flag is set
        if (flags[StorageOptions::NO_LOAD]) {
            return {};
        } else {
            if (!address.isValid()) {
                address = m_dram_allocator->firstAlloc();
            }
            return IndexT(m_dram_space.myPtr(address), m_dram_prefix->getPageSize(), access_type);
        }
    }
    
    template <typename ItemT, typename CompressedItemT>
    typename SparseIndexBase<ItemT, CompressedItemT>::IndexT
    SparseIndexBase<ItemT, CompressedItemT>::createIndex() {
        return IndexT(m_dram_space, m_dram_prefix->getPageSize(), AccessType::READ_WRITE);  
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
    bool SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::canFit(std::pair<PageNumT, StateNumT> item) const 
    {
        return this->m_first_page_num == (item.first >> 24);
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
    std::optional<typename SparseIndexBase<ItemT, CompressedItemT>::PageNumT> 
    SparseIndexBase<ItemT, CompressedItemT>::getNextStoragePageNum() const 
    {
        if (this->empty() ) {
            return std::nullopt;
        }
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
        m_index.detach();
        m_next_page_num = m_index.treeHeader().m_next_page_num;
        m_max_state_num = m_index.treeHeader().m_max_state_num;        
    }
    
    template <typename ItemT, typename CompressedItemT>
    std::string SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::toString(const CompressedItemT &item) const {
        return item.toString();
    }
    
    template <typename ItemT, typename CompressedItemT>
    std::string SparseIndexBase<ItemT, CompressedItemT>::BlockHeader::toString() const 
    {
        std::stringstream _str;
        _str << "BlockHeader { first_page_num: " << m_first_page_num << " }";
        return _str.str();
    }
    
    template <typename ItemT, typename CompressedItemT>
    bool SparseIndexBase<ItemT, CompressedItemT>::empty() const {
        return m_index.empty();
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
    
    template <typename ItemT, typename CompressedItemT>
    void SparseIndexBase<ItemT, CompressedItemT>::setExtraData(std::uint64_t data) {
        m_index.modifyTreeHeader().m_extra_data = data;
    }

    template <typename ItemT, typename CompressedItemT>
    std::uint64_t SparseIndexBase<ItemT, CompressedItemT>::getExtraData() const {
        return m_index.treeHeader().m_extra_data;
    }
    
    template <typename ItemT, typename CompressedItemT>
    Address SparseIndexBase<ItemT, CompressedItemT>::getIndexAddress() const {
        return m_index.getAddress();
    }
    
    template <typename ItemT, typename CompressedItemT>
    typename SparseIndexBase<ItemT, CompressedItemT>::ConstItemIterator    
    SparseIndexBase<ItemT, CompressedItemT>::findLower(PageNumT page_num, StateNumT state_num) const {
        return m_index.findLower(std::make_pair(page_num, state_num));
    }

    template <typename ItemT, typename CompressedItemT>
    void SparseIndexBase<ItemT, CompressedItemT>::commit() {
        m_index.commit();        
    }
    
    template <typename ItemT, typename CompressedItemT>
    bool SparseIndexBase<ItemT, CompressedItemT>::operator!() const {
        return !m_index;
    }
    
}