// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "v_ptr.hpp"
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
    class v_object: public v_ptr<T, SLOT_NUM, REALM_ID>
    {
    public:
        using ContainerT = T;
        // for compatiblility with intrusive containers (e.g. v_sgtree)
        using ptr_t = v_ptr<ContainerT, SLOT_NUM, REALM_ID>;

        v_object() = default;
        
        v_object(const ptr_t &ptr)
            : ptr_t(ptr)
        {
        }        
        
        v_object(mptr ptr, FlagSet<AccessOptions> access_mode = {})
            : ptr_t(ptr, access_mode)
        {
        }
        
        // Construct a verified instance - i.e. backed by a valid db0 address with a known size
        v_object(db0::tag_verified, mptr ptr, std::size_t size_of = 0, FlagSet<AccessOptions> access_mode = {})
            : ptr_t(ptr, access_mode)
        {
            ptr_t::safeConstRef(size_of);
        }
        
        v_object(const v_object &other)
            : ptr_t(other)
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
                std::get<N>(std::forward<Tuple>(t))
            );            
            ContainerT::__new(reinterpret_cast<std::byte*>(&this->modify()), std::get<I>(std::forward<Tuple>(t))...);
        }
        
        /// Pre-locked constructor
        struct tag_prelocked {};
        template<typename Tuple, std::size_t...I, std::size_t N=std::tuple_size<Tuple>::value-1>
        v_object(Memspace &memspace, tag_prelocked, Tuple&& t, int_seq<std::size_t, I...>)            
        {
            initNew(memspace, std::move(std::get<N>(std::forward<Tuple>(t))));
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
            : ptr_t(std::move(other))
        {            
        }
        
        template<typename... Args>
        static std::uint64_t makeNew(Memspace &memspace, Args&&... args)
        {
            v_object new_object(memspace, std::forward<Args>(args)...);
            return new_object.getAddress();
        }
        
        // Reference data container for read
        inline const ContainerT &const_ref() const {
            return *this->getData();
        }
        
        mptr myPtr(Address address, FlagSet<AccessOptions> access_mode = {}) const {
            return this->getMemspace().myPtr(address, access_mode);
        }
        
        // Calculate the number of DPs spanned by this object
        // NOTE: even small objects may span more than 1 DP if are positioned on a boundary
        // however allocators typically will avoid such situations
        unsigned int span() const
        {
            auto first_dp = this->getMemspace().getPageNum(this->m_address);
            auto last_dp = this->getMemspace().getPageNum(this->m_address + (*this)->sizeOf());
            return last_dp - first_dp + 1;
        }
        
        v_object &operator=(v_object &&other)
        {
            vtypeless::operator=(std::move(other));
            return *this;
        }

        v_object &operator=(v_object const &other)
        {
            vtypeless::operator=(other);
            return *this;
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
                this->m_address, size, access_mode | AccessOptions::write
            );
            // mark the entire writable area as modified
            this->m_mem_lock.modify();
            this->m_resource_flags = db0::RESOURCE_AVAILABLE_FOR_READ | db0::RESOURCE_AVAILABLE_FOR_WRITE;
            this->m_access_mode = access_mode;
            // collect as a modified instance for commit speedup
            this->m_memspace_ptr->collectModified(this);
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
            // collect as a modified instance for commit speedup
            this->m_memspace_ptr->collectModified(this);
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
            // collect as a modified instance for commit speedup
            this->m_memspace_ptr->collectModified(this);
        }
        
        static inline unsigned char getLocality(FlagSet<AccessOptions> access_mode) {
            // NOTE: use locality = 1 for no_cache allocations, 0 otherwise (undefined)
            return access_mode[AccessOptions::no_cache] ? 1 : 0;
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