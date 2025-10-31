#pragma once

#include <cstdint>
#include <cstdlib>
#include <atomic>
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

    template <typename T, std::uint32_t SLOT_NUM = 0, unsigned char REALM_ID = 0>
    class v_object;

    class vtypeless
    {
    protected :        
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
        
        /**
         * Memory mapped range corresponding to this object
        */
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

        inline operator bool() const {
            return m_address.isValid();
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

        /**
         * Get use count of the underlying lock
        */
        unsigned int use_count() const;

        /**
         * Check if the underlying resource is available in local memory
        */
        bool isAttached() const;

        /**
         * Detach underlying resource lock (i.e. mark resource as not available in local memory)
        */
        void detach();
        
        /**
         * Commit by marking the write as final.
         * The subsequent modify() will need to refresh the underlying lock
        */
        void commit();
        
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
                        m_address.getOffset(), this->getSize(), m_access_mode | AccessOptions::write | AccessOptions::read);
                    // by calling MemLock::modify we mark the object's associated range as modified
                    m_mem_lock.modify();
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
        
        const ContainerT *get() const
        {
            assureInitialized();            
            return reinterpret_cast<const ContainerT*>(m_mem_lock.m_buffer);
        }

        const ContainerT *getData() const
        {
            assureInitialized();            
            return reinterpret_cast<const ContainerT*>(m_mem_lock.m_buffer);
        }
        
        inline const ContainerT *operator->() const {
            return get();
        }
        
        static self_t makeNew(Memspace &memspace, std::size_t size, FlagSet<AccessOptions> access_mode = {})
        {
            // read not allowed for instance creation
            assert(!access_mode[AccessOptions::read]);
            auto address = memspace.alloc(size, SLOT_NUM, REALM_ID);
            // lock for create & write
            // NOTE: must extract physical address for mapRange
            auto mem_lock = memspace.getPrefix().mapRange(address, size, access_mode | AccessOptions::write);
            // mark the entire writable area as modified
            mem_lock.modify();
            // mark as available for both write & read
            return self_t(
                memspace, address, std::move(mem_lock),
                db0::RESOURCE_AVAILABLE_FOR_READ | db0::RESOURCE_AVAILABLE_FOR_WRITE, access_mode
            );
        }
        
        // Create a new instance using allocUnique functionality
        static self_t makeNewUnique(Memspace &memspace, std::uint16_t &instance_id, std::size_t size, 
            FlagSet<AccessOptions> access_mode = {})
        {
            // read not allowed for instance creation
            assert(!access_mode[AccessOptions::read]);
            auto unique_address = memspace.allocUnique(size, SLOT_NUM, REALM_ID);
            instance_id = unique_address.getInstanceId();
            // lock for create & write
            // NOTE: must extract physical address for mapRange
            auto mem_lock = memspace.getPrefix().mapRange(
                unique_address.getOffset(), size, access_mode | AccessOptions::write
            );
            // mark the entire writable area as modified
            mem_lock.modify();
            // mark as available for both write & read
            return self_t(
                memspace, unique_address, std::move(mem_lock),
                db0::RESOURCE_AVAILABLE_FOR_READ | db0::RESOURCE_AVAILABLE_FOR_WRITE, access_mode
            );
        }
        
        /**
         * Create a new instance from the mapped address
         * @param memspace the memspace to use
         * @param mapped_addr the mapped address
         * @param access_mode additional access mode flags
        */   
        static self_t makeNew(Memspace &memspace, MappedAddress &&mapped_addr, FlagSet<AccessOptions> access_mode = {})
        {            
            // mark the entire writable area as modified
            mapped_addr.m_mem_lock.modify();
            return self_t(memspace, mapped_addr.m_address,
                std::move(mapped_addr.m_mem_lock),
                // mark as available for read & write
                db0::RESOURCE_AVAILABLE_FOR_READ | db0::RESOURCE_AVAILABLE_FOR_WRITE, access_mode
            );            
        }
        
        /**
         * Get the underlying mapped range (for mutation)
        */
        MemLock modifyMappedRange()
        {
            modify();
            return this->m_mem_lock;
        }
        
    private:
        
        void assureInitialized() const
        {
            assert(m_memspace_ptr);
            // access the resource for read (or check if the read or read/write access has already been gained)
            while (!ResourceReadMutexT::__ref(m_resource_flags).get()) {
                ResourceReadMutexT::WriteOnlyLock lock(m_resource_flags);
                if (lock.isLocked()) {
                    // NOTE: must extract physical address for mapRange
                    m_mem_lock = m_memspace_ptr->getPrefix().mapRange(
                        m_address.getOffset(), this->getSize(), m_access_mode | AccessOptions::read);
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
                        m_address.getOffset(), size_of, m_access_mode | AccessOptions::read);
                    lock.commit_set();
                    break;
                }
            }
            assert(m_mem_lock.m_buffer);
        }
        
        /**
         * Resolve the instance size
         */
        std::size_t getSize() const
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
    };

}
