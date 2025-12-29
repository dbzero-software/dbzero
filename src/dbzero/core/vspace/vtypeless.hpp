// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cstdlib>
#include <atomic>
#include <optional>
#include <dbzero/core/memory/Allocator.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/mptr.hpp>
#include <dbzero/core/threading/ROWO_Mutex.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/metaprog/type_traits.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include "MappedAddress.hpp"
#include "safe_buf_t.hpp"

namespace db0
    
{

    class vtypeless
    {
    protected:     
        using ResourceReadMutexT = ROWO_Mutex<
            std::uint16_t,
            db0::RESOURCE_AVAILABLE_FOR_READ,
            db0::RESOURCE_AVAILABLE_FOR_READ,
            db0::RESOURCE_LOCK >;

        using ResourceReadWriteMutexT = ROWO_Mutex<
            std::uint16_t,
            db0::RESOURCE_AVAILABLE_FOR_WRITE,
            db0::RESOURCE_AVAILABLE_FOR_RW,
            db0::RESOURCE_LOCK >;

        // detach checks either R/W flags and clears both of them
        using ResourceDetachMutexT = ROWO_Mutex<
            std::uint16_t,
            db0::RESOURCE_AVAILABLE_FOR_RW,
            db0::RESOURCE_AVAILABLE_FOR_RW,
            db0::RESOURCE_LOCK >;

        /**
         * Within-prefix address of this object
        */
        Address m_address = {};
        Memspace *m_memspace_ptr = nullptr;
        mutable std::atomic<std::uint16_t> m_resource_flags = 0;
        // initial access flags (e.g. read / write / create)
        FlagSet<AccessOptions> m_access_mode;
        // NOTE: cached size may speed-up updates but also is relevant for existing vptr's reinterpret casts
        mutable std::optional<std::uint32_t> m_cached_size;

        // Memory mapped range corresponding to this object
        mutable MemLock m_mem_lock;

    public:
        vtypeless() = default;
        
        vtypeless(Memspace &, Address address, FlagSet<AccessOptions>);

        /**
         * Create mem-locked with specific flags (e.g. read/ write)
        */
        vtypeless(Memspace &, Address address, MemLock &&, std::uint16_t resource_flags,
            FlagSet<AccessOptions>);

        vtypeless(const vtypeless& other);
        vtypeless(vtypeless&&);
        
        /**
         * @param access_mode additional flags / modes to use
        */
        inline vtypeless(mptr ptr, FlagSet<AccessOptions> access_mode = {})
            : m_address(ptr.m_address)
            , m_memspace_ptr(&ptr.m_memspace.get())
            , m_access_mode(ptr.m_access_mode | access_mode)
        {
            assertFlags();
        }
        
        inline FlagSet<AccessOptions> getAccessMode() const {
            return m_access_mode;
        }
        
        vtypeless &operator=(const vtypeless &other);
        void operator=(vtypeless &&);
        
        /**
         * Instance compare
         */
        inline bool operator==(const vtypeless &ptr) const {
            return (m_memspace_ptr == ptr.m_memspace_ptr && m_address == ptr.m_address);
        }

        inline bool operator!=(const vtypeless &ptr) const {
            return (m_memspace_ptr != ptr.m_memspace_ptr || m_address != ptr.m_address);
        }

        inline bool isNull() const {
            return !m_address.isValid();
        }
        
        bool operator!() const {
            return !m_address.isValid();
        }

        inline Address getAddress() const {
            return m_address;
        }

        inline Memspace &getMemspace() const {
            assert(m_memspace_ptr);
            return *m_memspace_ptr;
        }

        inline Memspace *getMemspacePtr() const {
            return m_memspace_ptr;
        }
        
        inline bool isNoCache() const {
            return m_access_mode[AccessOptions::no_cache];
        }
        
        // Get use count of the underlying lock
        unsigned int use_count() const;
        
        /**
         * Check if the underlying resource is available in local memory
        */
        bool isAttached() const;

        /**
         * Detach underlying resource lock (i.e. mark resource as not available in local memory)
        */
        void detach() const;
        
        /**
         * Commit by marking the write as final.
         * The subsequent modify() will need to refresh the underlying lock
        */
        void commit() const;
        
        /**
         * Cast to a specific concrete type
         * @return pointer which may be null if the underlying lock does not exist
        */
        template <typename T> const T *castTo() const {
            return reinterpret_cast<const T*>(m_mem_lock.m_buffer);
        }
        
    private:

        inline void assertFlags()
        {
            // read / write / create flags are disallowed since they're assigned dynamically
            assert(!m_access_mode[AccessOptions::read]);
            assert(!m_access_mode[AccessOptions::write]);
        }
    };
    
}
