// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/unbound_array.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/memory/Memspace.hpp>

namespace db0

{

    /**
     * A limited vector has the following properties / limitations:
     * - performs full-page allocations only
     * - stores only a single indexing block (one data page) with references to data blocks
     * - can store a limited number of elements
     * - size of the structure is only knonw up to a whole block
     * - first element in the root block stores the total number of allocated blocks
     * 
     * The main use case of the LimitedVector in dbzero is holding instance IDs on a per slab basis
     */
    template <typename ValueT> class LimitedVector
    {
    public:
        LimitedVector(Memspace &, std::size_t page_size, ValueT value_limit = std::numeric_limits<ValueT>::max());
        LimitedVector(mptr, std::size_t page_size, ValueT value_limit = std::numeric_limits<ValueT>::max());

        // atomically try incrementing (or creating) value at a specific index
        // return false in case of a numeric overflow (value is not incremented)
        bool atomicInc(std::size_t index, ValueT &value);

        ValueT get(std::size_t index) const;
        void set(std::size_t index, ValueT value);

        Address getAddress() const;

        void detach() const;

        void commit() const;

        // reserve space for a given number of elements
        void reserve(std::size_t size);

        // Get total storage space occupied by the structure
        std::size_t getDataSize() const;
        
        // calculate the number of required data pages to hold specific capacity (i.e. elements)
        static constexpr std::uint32_t DP_REQ(std::size_t capacity, std::size_t page_size) {
            return (capacity * sizeof(ValueT) - 1) / page_size + 2;
        }
        
    private:
        Memspace &m_memspace;
        const std::size_t m_page_size;
        const ValueT m_value_limit;
        // a root block with pointers to data blocks
        db0::v_object<o_unbound_array<std::uint64_t> > m_root;
        // cache of actual data blocks
        mutable std::vector<db0::v_object<o_unbound_array<ValueT> > > m_cache;

        std::size_t getMaxBlockCount() const;
        std::size_t getBlockSize() const;

        // @retrun block number / position in block
        std::pair<std::uint32_t, std::uint32_t> getBlockPosition(std::size_t index) const;

        // retrieve existing block
        const db0::v_object<o_unbound_array<ValueT> > &getExistingBlock(std::uint32_t block_num) const;

        // retrieve existing or create new block
        db0::v_object<o_unbound_array<ValueT> > &getBlock(std::uint32_t block_num);
    };
    
    template <typename ValueT> LimitedVector<ValueT>::LimitedVector(Memspace &memspace, std::size_t page_size, ValueT value_limit)
        : m_memspace(memspace)
        , m_page_size(page_size)
        , m_value_limit(value_limit)
        , m_root(memspace, getMaxBlockCount(), 0)
    {
    }
    
    template <typename ValueT> LimitedVector<ValueT>::LimitedVector(mptr ptr, std::size_t page_size, ValueT value_limit)
        : m_memspace(ptr.m_memspace)
        , m_page_size(page_size)
        , m_value_limit(value_limit)
        , m_root(ptr)
    {
    }

    template <typename ValueT> std::size_t LimitedVector<ValueT>::getMaxBlockCount() const
    {
        if ((m_page_size % sizeof(std::uint64_t)) != 0) {
            THROWF(db0::InternalException) << "LimitedVector: page size must be a multiple of address size: " << m_page_size << " % " << sizeof(ValueT);
        }
        return m_page_size / sizeof(std::uint64_t);
    }

    template <typename ValueT> std::size_t LimitedVector<ValueT>::getBlockSize() const
    {
        if ((m_page_size % sizeof(ValueT)) != 0) {
            THROWF(db0::InternalException) << "LimitedVector: page size must be a multiple of value size: " << m_page_size << " % " << sizeof(ValueT);
        }
        return m_page_size / sizeof(ValueT);
    }

    template <typename ValueT> Address LimitedVector<ValueT>::getAddress() const {
        return m_root.getAddress();
    }

    template <typename ValueT> ValueT LimitedVector<ValueT>::get(std::size_t index) const
    {
        auto [block_num, block_pos] = getBlockPosition(index);
        return getExistingBlock(block_num)->get(block_pos);
    }

    template <typename ValueT> std::pair<std::uint32_t, std::uint32_t>
    LimitedVector<ValueT>::getBlockPosition(std::size_t index) const
    {
        auto block_size = m_page_size / sizeof(ValueT);
        return std::make_pair(index / block_size, index % block_size);
    }

    template <typename ValueT>
    const db0::v_object<o_unbound_array<ValueT> > &LimitedVector<ValueT>::getExistingBlock(std::uint32_t block_num) const
    {
        assert(block_num < getMaxBlockCount());
        // return from cache if block is already loaded
        if (block_num >= m_cache.size() || !m_cache[block_num]) {
            auto block_addr = Address::fromOffset(m_root->get(block_num + 1));
            if (!block_addr.isValid()) {
                THROWF(db0::InternalException) << "LimitedVector: block " << block_num << " not allocated";
            }
            if (block_num >= m_cache.size()) {
                m_cache.resize(block_num + 1);
            }
            // open existing data block
            m_cache[block_num] = db0::v_object<o_unbound_array<ValueT> >(m_memspace.myPtr(block_addr));
        }
        // pull from cache
        return m_cache[block_num];
    }
    
    template <typename ValueT>
    db0::v_object<o_unbound_array<ValueT> > &LimitedVector<ValueT>::getBlock(std::uint32_t block_num)
    {
        assert(block_num < getMaxBlockCount());
        if (block_num >= m_cache.size() || !m_cache[block_num]) {
            if (block_num >= m_cache.size()) {
                m_cache.resize(block_num + 1);
            }
            // data block is expected to occupy a full page
            assert(o_unbound_array<ValueT>::measure(getBlockSize()) == m_page_size);            
            // allocate new data block or pull existing
            auto block_addr = Address::fromOffset(m_root->get(block_num + 1));
            if (block_addr) {
                m_cache[block_num] = db0::v_object<o_unbound_array<ValueT> >(m_memspace.myPtr(block_addr));
            } else {                
                m_cache[block_num] = db0::v_object<o_unbound_array<ValueT> >(m_memspace, getBlockSize(), ValueT());
                m_root.modify()[block_num + 1] = m_cache[block_num].getAddress();
                // first element in the root block stores the total number of allocated blocks
                ++m_root.modify()[0];
            }
        }
        return m_cache[block_num];
    }
    
    template <typename ValueT> void LimitedVector<ValueT>::set(std::size_t index, ValueT value)
    {
        auto [block_num, block_pos] = getBlockPosition(index);
        getBlock(block_num).modify()[block_pos] = value;
    }
    
    template <typename ValueT> void LimitedVector<ValueT>::detach() const
    {
        m_root.detach();
        m_cache.clear();
    }
    
    template <typename ValueT> void LimitedVector<ValueT>::commit() const
    {
        m_root.commit();
        for (auto &block: m_cache) {
            if (!!block) {
                block.commit();
            }
        }
    }
    
    template <typename ValueT> bool LimitedVector<ValueT>::atomicInc(std::size_t index, ValueT &value)
    {
        auto [block_num, block_pos] = getBlockPosition(index);
        auto &block = getBlock(block_num);
        if (block->get(block_pos) == m_value_limit) {
            // unable to increment, limit reached
            return false;
        }
        value = ++block.modify()[block_pos];
        return true;
    }

    template <typename ValueT> void LimitedVector<ValueT>::reserve(std::size_t size)
    {
        auto [block_num, block_pos] = getBlockPosition(size);
        // create missing blocks from the range
        // blocks are not added to cache
        for (std::size_t i = 0; i <= block_num; ++i) {
            if (!m_root->get(block_num + 1)) {
                auto new_block = db0::v_object<o_unbound_array<ValueT> >(m_memspace, getBlockSize(), ValueT());
                m_root.modify()[block_num] = new_block.getAddress();
                ++m_root.modify()[0];
            }
        }
    }
    
    template <typename ValueT> std::size_t LimitedVector<ValueT>::getDataSize() const {
        // root block + data blocks
        return ((*m_root.getData())[0] + 1) * m_page_size;
    }
    
}