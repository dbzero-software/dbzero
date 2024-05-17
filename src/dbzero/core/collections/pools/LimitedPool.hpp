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
        */
        template <typename ResultT> ResultT fetch(AddressT address) const;

        void erase(AddressT address);

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
        auto address = m_memspace.getAllocator().alloc(size_of);
        assert(address <= std::numeric_limits<AddressT>::max());
        auto ptr = m_memspace.getPrefix().mapRange(address, size_of, { AccessOptions::create, AccessOptions::write });
        T::__new(ptr.modify(), std::forward<Args>(args)...);        
        return static_cast<AddressT>(address);
    }

    template <typename T, typename AddressT> template <typename ResultT> ResultT LimitedPool<T, AddressT>::fetch(AddressT address) const
    {
        // FIXME: mapRangeWeak optimization should be implemented
        auto size = m_memspace.getAllocator().getAllocSize(address);
        auto ptr = m_memspace.getPrefix().mapRange(address, size, { AccessOptions::read });
        // cast to result type
        return T::__const_ref(ptr);
    }
    
    template <typename T, typename AddressT> void LimitedPool<T, AddressT>::erase(AddressT address) {
        m_memspace.getAllocator().free(address);
    }
    
    template <typename T, typename AddressT> void LimitedPool<T, AddressT>::close() {
        m_memspace = {};
    }
    
}