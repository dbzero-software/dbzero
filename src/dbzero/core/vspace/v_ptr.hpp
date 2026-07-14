// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "vtypeless.hpp"

namespace db0
    
{

    template <typename T, std::uint32_t SLOT_NUM = 0, unsigned char REALM_ID = 0>
    class v_object;
    
    /**
     * virtual pointer to object of ContainerT
     */
    template <typename ContainerT, std::uint32_t SLOT_NUM = 0, unsigned char REALM_ID = 0>
    class v_ptr : public vtypeless
    {
    public :
        using container_t = ContainerT;
        using self_t = v_ptr<ContainerT, SLOT_NUM, REALM_ID>;

        inline v_ptr() = default;

        inline v_ptr(Memspace &memspace, Address address, FlagSet<AccessOptions> access_mode = {})
            : vtypeless(memspace, address, access_mode)
        {
        }

        inline v_ptr(Memspace &memspace, Address address, MemLock &&lock, std::uint16_t resource_flags,
            FlagSet<AccessOptions> access_mode = {})
            : vtypeless(memspace, address, std::move(lock), resource_flags, access_mode)
        {
        }
        
        v_ptr(mptr ptr)
            : vtypeless(ptr)
        {
        }

        v_ptr(mptr ptr, FlagSet<AccessOptions> access_mode)
            : vtypeless(ptr, access_mode)
        {
        }

        // Explicit upcast from typeless
        explicit v_ptr(const vtypeless &ptr)
            : vtypeless(ptr)
        {
        }
        
        void destroy()
        {
            assert(m_memspace_ptr);
            // container's destroy
            (*this)->destroy(*m_memspace_ptr);
            m_mem_lock.release();
            m_memspace_ptr->free(m_address);
            this->m_address = {};
            this->m_resource_flags = 0;
            this->m_cached_size.reset();
        }
        
        ContainerT &modify()
        {
            assert(m_memspace_ptr);
            // access resource for read-write
            while (!ResourceReadWriteMutexT::__ref(m_resource_flags).get()) {
                ResourceReadWriteMutexT::WriteOnlyLock lock(m_resource_flags);
                if (lock.isLocked()) {
                    // release the MemLock first to avoid or reduce CoWs
                    // otherwise mapRange might need to manage multiple lock versions
                    m_mem_lock.release();
                    // lock for +write
                    // note that lock is getting updated, possibly copy-on-write is being performed
                    // NOTE: must extract physical address for mapRange                    
                    m_mem_lock = m_memspace_ptr->getPrefix().mapRange(
                        m_address.getOffset(), this->getSize(), m_access_mode | AccessOptions::write | AccessOptions::read
                    );
                    // by calling MemLock::modify we mark the object's associated range as modified
                    m_mem_lock.modify();
                    // collect as a modified instance for commit speedup
                    m_memspace_ptr->collectModified(this);
                    lock.commit_set();
                    break;
                }
            }
            // this is to notify dirty-callbacks if needed
            return *reinterpret_cast<ContainerT*>(m_mem_lock.m_buffer);
        }
        
        void modify(std::size_t offset, std::size_t size)
        {
            auto &ref = modify();
            m_mem_lock.modify((std::byte*)&ref + offset, size);
        }
        
        // Check if the underlying resource is available as mutable
        // i.e. was already access for read/write
        bool isModified() const {
            return ResourceReadWriteMutexT::__ref(m_resource_flags).get();            
        }
                
        const ContainerT *getData() const
        {
            assureInitialized();            
            return reinterpret_cast<const ContainerT*>(m_mem_lock.m_buffer);
        }
        
        inline const ContainerT *operator->() const {
            return this->getData();
        }
        
        // Get the underlying mapped range (for mutation)        
        MemLock modifyMappedRange()
        {
            modify();
            return this->m_mem_lock;
        }
        
    protected:
        
        const ContainerT &safeConstRef(std::size_t size_of = 0) const
        {
            if (!size_of) {
                size_of = this->getSize();
            }
            assureInitialized(size_of);
            return ContainerT::__safe_const_ref(
                safe_buf_t((std::byte*)m_mem_lock.m_buffer, (std::byte*)m_mem_lock.m_buffer + size_of)
            );
        }

    private:
        
        static inline unsigned char getLocality(FlagSet<AccessOptions> access_mode) {
            // NOTE: use locality = 1 for no_cache allocations, 0 otherwise (undefined)
            return access_mode[AccessOptions::no_cache] ? 1 : 0;
        }

        void assureInitialized() const
        {
            assert(m_memspace_ptr);
            // access the resource for read (or check if the read or read/write access has already been gained)
            while (!ResourceReadMutexT::__ref(m_resource_flags).get()) {
                ResourceReadMutexT::WriteOnlyLock lock(m_resource_flags);
                if (lock.isLocked()) {
                    // NOTE: must extract physical address for mapRange
                    m_mem_lock = m_memspace_ptr->getPrefix().mapRange(
                        m_address.getOffset(), this->getSize(), m_access_mode | AccessOptions::read
                    );
                    lock.commit_set();
                    break;
                }
            }
            assert(m_mem_lock.m_buffer);
        }
        
        // version with known size-of (pre-retrieved from the allocator)
        // we made it as a separate implementation for potential performance gains
        void assureInitialized(std::size_t size_of) const
        {
            assert(m_memspace_ptr);
            // access the resource for read (or check if the read or read/write access has already been gained)
            while (!ResourceReadMutexT::__ref(m_resource_flags).get()) {
                ResourceReadMutexT::WriteOnlyLock lock(m_resource_flags);
                if (lock.isLocked()) {
                    // NOTE: must extract physical address for mapRange
                    m_mem_lock = m_memspace_ptr->getPrefix().mapRange(
                        m_address.getOffset(), size_of, m_access_mode | AccessOptions::read
                    );
                    lock.commit_set();
                    break;
                }
            }
            assert(m_mem_lock.m_buffer);
        }
        
        // Resolve the instance size
        std::uint32_t fetchSize() const
        {
            assert(m_memspace_ptr);
            if constexpr(metaprog::has_constant_size<ContainerT>::value) {
                // fixed size type
                return ContainerT::measure();
            }
            else if constexpr(metaprog::has_fixed_header<ContainerT>::value) {
                v_object<typename ContainerT::fixed_header_type, SLOT_NUM, REALM_ID> header(mptr{*m_memspace_ptr, m_address});
                return header.getData()->getOBaseSize();
            }
            
            // retrieve from allocator (slowest)
            return m_memspace_ptr->getAllocator().getAllocSize(m_address, REALM_ID);
        }
        
        // Get from cache or fetch size
        std::uint32_t getSize() const
        {
            if (!m_cached_size) {
                m_cached_size = fetchSize();
            }
            return *m_cached_size;            
        }
    };  

}
