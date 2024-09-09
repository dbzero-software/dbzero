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
     * 
     * The main use case of the LimitedVector in dbzero is holding instance IDs on a per slab basis
     */
    template <typename ValueT> class LimitedVector
    {
    public:
        LimitedVector(Memspace &, std::size_t page_size);
        LimitedVector(mptr, std::size_t page_size);

        // atomically try incrementing (or creating) value at a specific index
        // return false in case of a numeric overflow (value is not incremented)
        bool atomicInc(std::size_t index, ValueT &value);

        ValueT get(std::size_t index) const;
        void set(std::size_t index, ValueT value);

        std::uint64_t getAddress() const;

        void detach() const;

    private:
        Memspace &m_memspace;
        const std::size_t m_page_size;
        // a root block with pointers to data blocks
        db0::v_object<o_unbound_array<std::uint64_t> > m_root;
        // cache of actual data blocks
        std::vector<db0::v_object<o_unbound_array<std::uint64_t> > > m_cache;

        std::size_t getMaxBlockCount() const;
    };

    template <typename ValueT> LimitedVector<ValueT>::LimitedVector(Memspace &memspace, std::size_t page_size)
        : m_memspace(memspace)
        , m_page_size(page_size)
        , m_root(memspace, getMaxBlockCount(), 0)
    {
    }
    
    template <typename ValueT> LimitedVector<ValueT>::LimitedVector(mptr ptr, std::size_t page_size)
        : m_memspace(ptr.m_memspace)
        , m_page_size(page_size)
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

    template <typename ValueT> std::uint64_t LimitedVector<ValueT>::getAddress() const {
        return m_root.getAddress();
    }
    
}