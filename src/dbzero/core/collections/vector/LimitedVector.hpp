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

        std::uint64_t getAddress() const;

        void detach() const;

        void commit() const;

    private:
        Memspace &m_memspace;
        const std::size_t m_page_size;
        const ValueT m_value_limit;
        // a root block with pointers to data blocks
        db0::v_object<o_unbound_array<std::uint64_t> > m_root;
        // cache of actual data blocks
        mutable std::vector<db0::v_object<o_unbound_array<std::uint64_t> > > m_cache;

        std::size_t getMaxBlockCount() const;
        std::size_t getBlockSize() const;

        // @retrun block number / position in block
        std::pair<std::uint32_t, std::uint32_t> getBlockPosition(std::size_t index) const;

        // retrieve existing block
        const db0::v_object<o_unbound_array<std::uint64_t> > &getExistingBlock(std::uint32_t block_num) const;

        // retrieve existing or create new block
        db0::v_object<o_unbound_array<std::uint64_t> > &getBlock(std::uint32_t block_num);
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

    template <typename ValueT> std::uint64_t LimitedVector<ValueT>::getAddress() const {
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
    const db0::v_object<o_unbound_array<std::uint64_t> > &LimitedVector<ValueT>::getExistingBlock(std::uint32_t block_num) const
    {
        assert(block_num < getMaxBlockCount());
        // return from cache if block is already loaded
        if (block_num >= m_cache.size() || !m_cache[block_num]) {
            auto block_addr = m_root->get(block_num);
            if (!block_addr) {
                THROWF(db0::InternalException) << "LimitedVector: block " << block_num << " not allocated";
            }
            if (block_num >= m_cache.size()) {
                m_cache.resize(block_num + 1);
            }
            // open existing data block
            m_cache[block_num] = db0::v_object<o_unbound_array<std::uint64_t> >(m_memspace.myPtr(block_addr));
        }
        // pull from cache
        return m_cache[block_num];
    }

    template <typename ValueT>
    db0::v_object<o_unbound_array<std::uint64_t> > &LimitedVector<ValueT>::getBlock(std::uint32_t block_num)
    {
        assert(block_num < getMaxBlockCount());
        if (block_num >= m_cache.size() || !m_cache[block_num]) {
            if (block_num >= m_cache.size()) {
                m_cache.resize(block_num + 1);
            }
            // allocate new data block
            m_cache[block_num] = db0::v_object<o_unbound_array<std::uint64_t> >(m_memspace, getBlockSize(), ValueT());
            m_root.modify()[block_num] = m_cache[block_num].getAddress();
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
        for (auto &block: m_cache) {
            if (block) {
                block.detach();
            }
        }
    }

    template <typename ValueT> void LimitedVector<ValueT>::commit() const
    {
        m_root.commit();
        for (auto &block: m_cache) {
            if (block) {
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

}