// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once 

#include <cstdint>
#include <limits>
#include <dbzero/core/memory/Memspace.hpp>

namespace db0::pools

{

    /**
     * The LimitedPool is a pool of identical type objects with a capacity limited by the underlying memspace
     * it simply uses the underlying allocator to create / delete objects
     * @tparam T the (overlaid) type of object to be stored in the pool
     * @tparam AddressT the type of address returned by the pool
    */
    template <typename T, typename AddressT = std::uint32_t> class LimitedPool
    {
    public:
        LimitedPool(const Memspace &);
        LimitedPool(LimitedPool const &);

        /**
         * Adds a new object to the pool and returns its address
        */
        template <typename... Args> AddressT add(Args&&... args);
        
        /**
         * Fetch object from the pool by its address
         * fetch is performed as the call operation if provided arguments over T instance
         * @param address the pool element's address
         * @param lock the MemLock persistency buffer
        */
        template <typename ResultT> ResultT fetch(AddressT address, MemLock &lock) const;

        void erase(AddressT address);

        // Check if the address is a pointential token's address
        // i.e. is the address within the pool's range
        // NOTE: the param address may be of different type than AddressT (higher range)
        bool isTokenAddr(Address address) const;

        void close();

    private:
        Memspace m_memspace;
    };
    
    template <typename T, typename AddressT> LimitedPool<T, AddressT>::LimitedPool(const Memspace &memspace)
        : m_memspace(memspace)
    {
    }

    template <typename T, typename AddressT> LimitedPool<T, AddressT>::LimitedPool(LimitedPool const &other)
        : m_memspace(other.m_memspace)
    {
    }
    
    template <typename T, typename AddressT> template <typename... Args> AddressT LimitedPool<T, AddressT>::add(Args&&... args)
    {
        auto size_of = T::measure(std::forward<Args>(args)...);
        auto address = m_memspace.alloc(size_of);
        assert(address <= std::numeric_limits<AddressT>::max());
        auto ptr = m_memspace.getPrefix().mapRange(address, size_of, { AccessOptions::write });
        T::__new(ptr.modify(), std::forward<Args>(args)...);
        
        return static_cast<AddressT>(address);
    }
    
    template <typename T, typename AddressT> template <typename ResultT> 
    ResultT LimitedPool<T, AddressT>::fetch(AddressT address, MemLock &lock) const
    {
        // FIXME: mapRangeWeak optimization should be implemented
        auto size = m_memspace.getAllocator().getAllocSize(Address::fromOffset(address));
        lock = m_memspace.getPrefix().mapRange(address, size, { AccessOptions::read });

        // cast to result type, persistency buffer managed by the caller
        return T::__const_ref(lock);
    }
    
    template <typename T, typename AddressT> void LimitedPool<T, AddressT>::erase(AddressT address) {
        m_memspace.free(Address::fromOffset(address));
    }
    
    template <typename T, typename AddressT> void LimitedPool<T, AddressT>::close() {
        m_memspace = {};
    }
    
    template <typename T, typename AddressT> bool LimitedPool<T, AddressT>::isTokenAddr(Address address) const {
        return m_memspace.getAllocator().inRange(address);
    }
    
}