#pragma once

#include <cstdint>
#include <cstdlib>
#include <atomic>
#include <dbzero/core/memory/Allocator.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/mptr.hpp>
#include <dbzero/core/threading/ROWO_Mutex.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/vspace/vs_buf_t.hpp>
#include <dbzero/core/metaprog/type_traits.hpp>
#include "MappedAddress.hpp"

namespace db0
    
{

    template <typename T, std::uint32_t SLOT_NUM = 0>
    class v_object;

    struct [[gnu::packed]] vso_null_t
    {
    };

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
        std::uint64_t m_address = 0;
        Memspace *m_memspace_ptr = nullptr;
        mutable std::atomic<std::uint16_t> m_resource_flags = 0;
        // initial access flags (e.g. read / write / create)
        mutable FlagSet<AccessOptions> m_access_mode;
        
        /**
         * Memory mapped range corresponding to this object
        */
        mutable MemLock m_mem_lock;

    public:
        vtypeless() = default;
        
        vtypeless(Memspace &, std::uint64_t address, FlagSet<AccessOptions>);

        /**
         * Create mem-locked with specific flags (e.g. read/ write)
        */
        vtypeless(Memspace &, std::uint64_t address, MemLock &&, FlagSet<AccessOptions> access_mode);

        vtypeless(const vtypeless& other);
        
        /**
         * @param access_mode additional flags / modes to use
        */
        inline vtypeless(mptr ptr, FlagSet<AccessOptions> access_mode = {})
            : m_address(ptr.m_address)
            , m_memspace_ptr(&ptr.m_memspace.get())
            , m_access_mode(ptr.m_access_mode | access_mode | AccessOptions::read)
        {
            // FIXME: log
            std::cout << "New vptr" << this << " flags: " << m_resource_flags.load() << std::endl;
        }
                
        FlagSet<AccessOptions> getAccessMode() const;
        
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
            return (m_address == 0);
        }

        inline operator bool() const {
            return m_address != 0;
        }

        inline std::uint64_t getAddress() const {
            return m_address;
        }

        inline Memspace &getMemspace() const {
            assert(m_memspace_ptr);
            return *m_memspace_ptr;
        }

        inline Memspace *getMemspacePtr() const {
            return m_memspace_ptr;
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
    };
    
    /**
     * virtual pointer to object of ContainerT
     */
    template <typename ContainerT, std::uint32_t SLOT_NUM = 0>
    class v_ptr : public vtypeless
    {
    public :
        using container_t = ContainerT;

        inline v_ptr() = default;

        inline v_ptr(Memspace &memspace, std::uint64_t address, FlagSet<AccessOptions> access_mode = {})
            : vtypeless(memspace, address, access_mode)
        {
        }

        inline v_ptr(Memspace &memspace, std::uint64_t address, MemLock &&lock, FlagSet<AccessOptions> access_mode)
            : vtypeless(memspace, address, std::move(lock), access_mode)
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
            this->m_address = 0;
            this->m_resource_flags = 0;
        }
        
        ContainerT &modify()
        {
            assert(m_memspace_ptr);
            // access resource for read-write
            while (!ResourceReadWriteMutexT::__ref(m_resource_flags).get()) {
                // FIXME: log
                std::cout << "Resource flags (before lock): " << m_resource_flags.load() << std::endl;                
                ResourceReadWriteMutexT::WriteOnlyLock lock(m_resource_flags);
                if (lock.isLocked()) {
                    // lock for +write
                    // note that lock is getting updated, possibly copy-on-write is being performed
                    // NOTE: must extract physical address for mapRange
                    m_mem_lock = m_memspace_ptr->getPrefix().mapRange(
                        getPhysicalAddress(m_address), this->getSize(), m_access_mode | AccessOptions::write | AccessOptions::read);
                    lock.commit_set();
                }
            }            
            // FIXME: log
            std::cout << "Resource flags (before lock): " << m_resource_flags.load() << std::endl;
            return *reinterpret_cast<ContainerT*>(m_mem_lock.modify());           
        }

        const ContainerT& safeRef() const 
        {
            assureInitialized();
            return ContainerT::__safe_ref(vs_buf_t(m_mem_lock.m_buffer, m_mem_lock.m_buffer + this->getSize()));
        }

        inline const ContainerT *get() const
        {
            assureInitialized();
            assert(m_mem_lock.m_buffer);
            return reinterpret_cast<const ContainerT*>(m_mem_lock.m_buffer);
        }

        inline const ContainerT *operator->() const {
            return get();
        }
        
        static v_ptr<ContainerT> makeNew(Memspace &memspace, std::size_t size, FlagSet<AccessOptions> access_mode)
        {
            auto address = memspace.alloc(size, SLOT_NUM, access_mode[AccessOptions::unique]);
            // lock for create & write
            // NOTE: must extract physical address for mapRange
            auto mem_lock = memspace.getPrefix().mapRange(
                getPhysicalAddress(address), size, access_mode | AccessOptions::write | AccessOptions::create
            );
            // mark as available for both write & read
            return v_ptr<ContainerT>(
                memspace, address, std::move(mem_lock), access_mode | AccessOptions::read | AccessOptions::write
            );
        }
        
        static v_ptr<ContainerT> makeNew(Memspace &memspace, MappedAddress &&mapped_addr, FlagSet<AccessOptions> access_mode) {
            return v_ptr<ContainerT>(memspace, mapped_addr.m_address, std::move(mapped_addr.m_mem_lock), access_mode);
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
                // FIXME: log
                std::cout << "Resource flags (before lock): " << m_resource_flags.load() << std::endl;                
                ResourceReadMutexT::WriteOnlyLock lock(m_resource_flags);
                if (lock.isLocked()) {
                    // NOTE: must extract physical address for mapRange
                    m_mem_lock = m_memspace_ptr->getPrefix().mapRange(
                        getPhysicalAddress(m_address), this->getSize(), m_access_mode | AccessOptions::read);
                    lock.commit_set();
                }
            }
            // FIXME: log
            std::cout << "Resource flags (after lock): " << m_resource_flags.load() << std::endl;             
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
                v_object<typename ContainerT::fixed_header_type, SLOT_NUM> header(mptr{*m_memspace_ptr, m_address, AccessOptions::read});
                return header.getData()->getOBaseSize();
            }
            
            // retrieve from allocator (slowest)
            return m_memspace_ptr->getAllocator().getAllocSize(m_address);
        }

        static void printTypeName() {
            std::cout << typeid(ContainerT).name() << "locks:" << std::endl;
        }

    };

}
