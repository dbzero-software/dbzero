#pragma once

#include "vtypeless.hpp"
#include <dbzero/core/metaprog/int_seq.hpp>
#include <dbzero/core/metaprog/last_type_is.hpp>

namespace db0
    
{

    struct tag_verified {};

    /**
     * Base class for vspace-mapped objects
     * @tparam T container object type
     */
    template <typename T, std::uint32_t SLOT_NUM, unsigned char REALM_ID>
    class v_object: public v_typeless
    {
    public:
        using ContainerT = T;

        v_object() = default;
        
        v_object(mptr ptr, FlagSet<AccessOptions> access_mode = {})
            : vtypeless(ptr, access_mode)
        {
        }
        
        // Construct a verified instance - i.e. backed by a valid db0 address with a known size
        v_object(db0::tag_verified, mptr ptr, std::size_t size_of = 0, FlagSet<AccessOptions> access_mode = {})
            : vtypeless(ptr, access_mode)
        {
            this->safeConstRef(size_of);
        }
        
        v_object(const v_object &other)
            : vtypeless(other)
        {
        }
        
        static constexpr unsigned char getRealmID() {
            return REALM_ID;
        }

    private:
        template<typename Tuple, std::size_t...I, std::size_t N=std::tuple_size<Tuple>::value-1>
        v_object(Memspace &memspace, Tuple&& t, int_seq<std::size_t, I...>)
        {
            initNew(
                memspace,
                ContainerT::measure(std::get<I>(std::forward<Tuple>(t))...),
                std::get<N>(std::forward<Tuple>(t)) );            
            
            ContainerT::__new(reinterpret_cast<std::byte*>(&this->modify()), std::get<I>(std::forward<Tuple>(t))...);
        }
        
        /// Pre-locked constructor
        struct tag_prelocked {};
        template<typename Tuple, std::size_t...I, std::size_t N=std::tuple_size<Tuple>::value-1>
        v_object(Memspace &memspace, tag_prelocked, Tuple&& t, int_seq<std::size_t, I...>)            
        {
            intiNew(memspace, std::move(std::get<N>(std::forward<Tuple>(t))));
            // placement new syntax
            ContainerT::__new(reinterpret_cast<std::byte*>(&this->modify()), std::get<I>(std::forward<Tuple>(t))...);
        }
        
        template<typename Tuple, std::size_t...I, std::size_t N=std::tuple_size<Tuple>::value-1>
        void init(Memspace &memspace, Tuple&& t, int_seq<std::size_t, I...>)
        {
            initNew(
                memspace, 
                ContainerT::measure(std::get<I>(std::forward<Tuple>(t))...),
                // access options (the last argument)
                std::get<N>(std::forward<Tuple>(t)) 
            );
            ContainerT::__new(reinterpret_cast<std::byte*>(&this->modify()), std::get<I>(std::forward<Tuple>(t))...);
        }

        template<typename Tuple, std::size_t...I, std::size_t N=std::tuple_size<Tuple>::value-1>        
        std::uint16_t initUnique(Memspace &memspace, Tuple&& t, int_seq<std::size_t, I...>)
        {
            std::uint16_t instance_id;
            initNewUnique(
                memspace, 
                instance_id, 
                ContainerT::measure(std::get<I>(std::forward<Tuple>(t))...),
                // access options (the last argument)
                std::get<N>(std::forward<Tuple>(t)) 
            );
            ContainerT::__new(reinterpret_cast<std::byte*>(&this->modify()), std::get<I>(std::forward<Tuple>(t))...);
            return instance_id;
        }

    public:
        /**
         * Allocating constructor with flags
         */
        template<typename... Args, last_type_is_t<FlagSet<AccessOptions>, Args...>* = nullptr>
        v_object(Memspace &memspace, Args&&... args)
            : v_object(memspace, std::forward_as_tuple(std::forward<Args>(args)...), make_int_seq_t<std::size_t, sizeof...(args)-1>())
        {
        }

        /**
         * Pre-locked allocating constructor
         * this constructor allows to pass the address and the mapped range for the instance being created
         */
        template<typename... Args, last_type_is_t<MappedAddress, Args...>* = nullptr>
        v_object(Memspace &memspace, Args&&... args)
            : v_object(memspace, tag_prelocked(), std::forward_as_tuple(std::forward<Args>(args)...), make_int_seq_t<std::size_t, sizeof...(args)-1>())
        {
        }
        
        // Standard allocating constructor
        template<typename... Args, last_type_is_not_t<FlagSet<AccessOptions>, Args...>* = nullptr, last_type_is_not_t<MappedAddress, Args...>* = nullptr>
        v_object(Memspace &memspace, Args&&... args)
            : v_object(memspace, std::forward<Args>(args)..., FlagSet<AccessOptions> {})
        {
        }
        
        // Create a new dbzero instance in the given memory space
        template<typename... Args, last_type_is_t<FlagSet<AccessOptions>, Args...>* = nullptr>
        void init(Memspace &memspace, Args&&... args) {
            init(memspace, std::forward_as_tuple(std::forward<Args>(args)...), make_int_seq_t<std::size_t, sizeof...(args)-1>());
        }

        template<typename... Args, last_type_is_not_t<FlagSet<AccessOptions>, Args...>* = nullptr>
        void init(Memspace &memspace, Args&&... args) {
            init(memspace, std::forward<Args>(args)..., FlagSet<AccessOptions> {});
        }

        // Create new instance assigned unique address
        // @return instance id
        template<typename... Args, last_type_is_t<FlagSet<AccessOptions>, Args...>* = nullptr>
        std::uint16_t initUnique(Memspace &memspace, Args&&... args) {
            return initUnique(memspace, std::forward_as_tuple(std::forward<Args>(args)...), make_int_seq_t<std::size_t, sizeof...(args)-1>());
        }
        
        template<typename... Args, last_type_is_not_t<FlagSet<AccessOptions>, Args...>* = nullptr>
        std::uint16_t initUnique(Memspace &memspace, Args&&... args) {
            return initUnique(memspace, std::forward<Args>(args)..., FlagSet<AccessOptions> {});        
        }
        
        v_object(v_object &&other)
            : vtypeless(std::move(other))
        {            
        }
        
        template<typename... Args>
        static std::uint64_t makeNew(Memspace &memspace, Args&&... args)
        {
            v_object new_object(memspace, std::forward<Args>(args)...);
            return new_object.getAddress();
        }
                
        // Readonly data access operator
        inline const ContainerT *operator->() const {
            return this->getData();
        }

        const ContainerT *getData() const
        {
            assureInitialized();            
            return reinterpret_cast<const ContainerT*>(m_mem_lock.m_buffer);
        }

        // Reference data container for read
        inline const ContainerT &const_ref() const {
            return *this->getData();
        }

        // Reference data container for update
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
        
        void destroy()
        {
            if (m_address.isValid()) {
                assert(m_memspace_ptr);
                // container's destroy
                (*this)->destroy(*m_memspace_ptr);
                m_mem_lock.release();
                m_memspace_ptr->free(m_address);
                this->m_address = {};
                this->m_resource_flags = 0;
                this->m_cached_size.reset();
            }
        }
        
        mptr myPtr(Address address, FlagSet<AccessOptions> access_mode = {}) const {
            return this->getMemspace().myPtr(address, access_mode);
        }
        
        /* FIXME: 
        void commit() const
        {
            // NOTE: this operation assumes that only one v_object instance pointing to the same address exists
            // otherwise modifications done to one instance will not be visible to the other instances
            // this assumption holds true for dbzero objects but if unable to fulfill in the future,
            // it must be changed to "this->detach()"

            v_this.commit();
        }
        */
        
        // Calculate the number of DPs spanned by this object
        // NOTE: even small objects may span more than 1 DP if are positioned on a boundary
        // however allocators typically will avoid such situations
        unsigned int span() const
        {
            auto first_dp = this->getMemspace().getPageNum(this->m_address);
            auto last_dp = this->getMemspace().getPageNum(this->m_address + (*this)->sizeOf());
            return last_dp - first_dp + 1;
        }
        
        // Check if the underlying resource is available as mutable
        // i.e. was already access for read/write
        bool isModified() const {
            return ResourceReadWriteMutexT::__ref(m_resource_flags).get();            
        }

        // Get the underlying mapped range (for mutation)
        MemLock modifyMappedRange()
        {
            modify();
            return this->m_mem_lock;
        }

    private:

        // Create a new instance
        void initNew(Memspace &memspace, std::size_t size, FlagSet<AccessOptions> access_mode = {})
        {
            // read not allowed for instance creation
            assert(!access_mode[AccessOptions::read]);
            this->m_memspace_ptr = &memspace;
            this->m_address = memspace.alloc(size, SLOT_NUM, REALM_ID, getLocality(access_mode));
            // lock for create & write
            // NOTE: must extract physical address for mapRange
            this->m_mem_lock = memspace.getPrefix().mapRange(
                m_address, size, access_mode | AccessOptions::write
            );
            // mark the entire writable area as modified
            this->m_mem_lock.modify();
            this->m_resource_flags = db0::RESOURCE_AVAILABLE_FOR_READ | db0::RESOURCE_AVAILABLE_FOR_WRITE;
            this->m_access_mode = access_mode;
        }
        
        // Create a new instance using allocUnique functionality
        void initNewUnique(Memspace &memspace, std::uint16_t &instance_id, std::size_t size, 
            FlagSet<AccessOptions> access_mode = {})
        {
            // read not allowed for instance creation
            assert(!access_mode[AccessOptions::read]);
            this->m_memspace_ptr = &memspace;
            auto unique_address = memspace.allocUnique(size, SLOT_NUM, REALM_ID, getLocality(access_mode));
            instance_id = unique_address.getInstanceId();
            // lock for create & write
            // NOTE: must extract physical address for mapRange
            this->m_address = unique_address;
            this->m_mem_lock = memspace.getPrefix().mapRange(
                unique_address.getOffset(), size, access_mode | AccessOptions::write
            );
            // mark the entire writable area as modified
            this->m_mem_lock.modify();
            // mark as available for both write & read
            this->m_resource_flags = db0::RESOURCE_AVAILABLE_FOR_READ | db0::RESOURCE_AVAILABLE_FOR_WRITE;
            this->m_access_mode = access_mode;            
        }
        
        /**
         * Create a new instance from the mapped address
         * @param memspace the memspace to use
         * @param mapped_addr the mapped address
         * @param access_mode additional access mode flags
        */   
        void initNew(Memspace &memspace, MappedAddress &&mapped_addr, FlagSet<AccessOptions> access_mode = {})
        {
            this->m_memspace_ptr = &memspace;
            // mark the entire writable area as modified
            mapped_addr.m_mem_lock.modify();
            this->m_address = mapped_addr.m_address;
            this->m_mem_lock = std::move(mapped_addr.m_mem_lock);
            // mark as available for read & write
            this->m_resource_flags = db0::RESOURCE_AVAILABLE_FOR_READ | db0::RESOURCE_AVAILABLE_FOR_WRITE;
            this->m_access_mode = access_mode;
        }
            
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

    // Utility function to safely mutate a v_object's fixed-size member
    template <typename T, typename MemberT>
    MemberT &modifyMember(T &obj, const MemberT &member) 
    {
        assert((std::byte*)&member >= (std::byte*)obj.getData());
        assert((std::byte*)&member + sizeof(MemberT) <= (std::byte*)obj.getData() + obj->sizeOf());
        auto offset = (std::byte*)&member - (std::byte*)obj.getData();
        return *(MemberT*)((std::byte*)(&obj.modify()) + offset);
    }

}