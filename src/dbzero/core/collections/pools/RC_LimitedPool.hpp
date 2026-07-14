// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "LimitedPool.hpp"
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/collections/map/v_map.hpp>
#include <dbzero/core/serialization/compose.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::pools

{
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_rc_limited_pool: public o_fixed_versioned<o_rc_limited_pool>
    {
        Address m_pool_map_address = {};

        o_rc_limited_pool(Address pool_map_address)
            : m_pool_map_address(pool_map_address)
        {            
        }
    };
DB0_PACKED_END
    
    /**
     * Limited pool with in-memory lookup index and ref-counting
     * NOTE: limited pool items combine (using o_compose) map iterator's address + actual item (T)
     * the iterator is required to implement unRef by address
    */
    template <typename T, typename CompT, typename AddrT = std::uint32_t> class RC_LimitedPool
        : public LimitedPool<db0::o_compose<Address, T>, AddrT>
        , public db0::v_object<o_rc_limited_pool>
    {
    public:
        using AddressT = AddrT;
        using ItemT = o_compose<Address, T>;
        using LP_Type = LimitedPool<db0::o_compose<Address, T>, AddrT>;

        RC_LimitedPool(const Memspace &pool_memspace, Memspace &);
        RC_LimitedPool(const Memspace &pool_memspace, mptr);
        RC_LimitedPool(RC_LimitedPool const &);

        /**
        * Adds a new object or increase ref-count of the existing element (upon request)
         * @param inc_ref - whether to increase ref-count of the existing element, note that for
         * newly created elements ref-count is always set to 1 (in such case inc_ref fill be flipped from false to true)
        */
        template <typename... Args> AddressT add(bool &inc_ref, Args&&... args);

        /**
         * Adds a new object with ref_count = 1 or increase ref-count of the existing element
         */
        template <typename... Args> AddressT addRef(Args&&... args);

        /**
         * Unreference existing element by key, drop when ref-count reaches 0
        */
        template <typename... Args> void unRef(Args&&... args);

        // Increment ref-count of an existing element by its address
        void addRefByAddr(AddressT address);

        // Unreference existing element by its address
        void unRefByAddr(AddressT address);

        /**
         * Try finding element by an arbitrary key
         * @return true if found, false otherwise
        */
        template <typename KeyT> bool find(const KeyT &, AddressT &) const;
        
        std::size_t size() const;

        void commit() const;

        void detach() const;

    private:
        // address + ref count
DB0_PACKED_BEGIN
        struct DB0_PACKED_ATTR MapItemT: public o_fixed<MapItemT>
        {
            AddressT m_address = 0;
            std::uint32_t m_ref_count = 0;

            MapItemT(AddressT address, std::uint32_t ref_count)
                : m_address(address)
                , m_ref_count(ref_count)
            {
            }
        };
DB0_PACKED_END

        using PoolMapT = db0::v_map<T, MapItemT, CompT>;
        PoolMapT m_pool_map;

        void unRefItem(typename PoolMapT::iterator &);
    };
    
    template <typename T, typename CompT, typename AddressT>
    RC_LimitedPool<T, CompT, AddressT>::RC_LimitedPool(const Memspace &pool_memspace, Memspace &memspace)
        : LP_Type(pool_memspace)
        , db0::v_object<o_rc_limited_pool>(memspace, PoolMapT(memspace).getAddress())
        , m_pool_map(memspace.myPtr((*this)->m_pool_map_address))
    {
    }
    
    template <typename T, typename CompT, typename AddressT>
    RC_LimitedPool<T, CompT, AddressT>::RC_LimitedPool(const Memspace &pool_memspace, mptr ptr)
        : LP_Type(pool_memspace)
        , db0::v_object<o_rc_limited_pool>(ptr)
        , m_pool_map(this->myPtr((*this)->m_pool_map_address))
    {
    }
    
    template <typename T, typename CompT, typename AddressT>
    RC_LimitedPool<T, CompT, AddressT>::RC_LimitedPool(RC_LimitedPool const &other)
        : LP_Type(other)
        , db0::v_object<o_rc_limited_pool>(other->myPtr(other.getAddress()))
        , m_pool_map(this->myPtr((*this)->m_pool_map_address))
    {
    }
    
    template <typename T, typename CompT, typename AddressT> template <typename KeyT>
    bool RC_LimitedPool<T, CompT, AddressT>::find(const KeyT &key, AddressT &address) const
    {
        auto it = m_pool_map.find(key);
        if (it == m_pool_map.end()) {
            return false;
        }
        address = it->second().m_address;
        return true;
    }
    
    template <typename T, typename CompT, typename AddressT> template <typename... Args>
    AddressT RC_LimitedPool<T, CompT, AddressT>::add(bool &inc_ref, Args&&... args)
    {
        // try finding existing element
        auto it = m_pool_map.find(std::forward<Args>(args)...);
        if (it != m_pool_map.end()) {
            // increase ref count
            if (inc_ref) {
                ++it.modify().second().m_ref_count;
            }
            return it->second().m_address;
        }
        
        // add to the map with ref_cout = 1 (use 0x0 address placeholder)
        it = m_pool_map.insert_equal(std::forward<Args>(args)..., MapItemT{0, 1});        
        // add new element (and the iterator's address) into the underlying limited pool
        auto new_address = LP_Type::add(it.getAddress(), std::forward<Args>(args)...);
        it.modify().second().m_address = new_address;
        // set inc_ref to true to indicate that ref_count was set to 1
        inc_ref = true;
        return new_address;
    }

    template <typename T, typename CompT, typename AddressT> template <typename... Args>
    AddressT RC_LimitedPool<T, CompT, AddressT>::addRef(Args&&... args)
    {
        // try finding existing element
        auto it = m_pool_map.find(std::forward<Args>(args)...);
        if (it != m_pool_map.end()) {
            // increase ref count            
            auto &item = it.modify().second();
            ++item.m_ref_count;
            return item.m_address;
        }
        
        // add to the map with ref_cout = 1 (use 0x0 address placeholder)
        it = m_pool_map.insert_equal(std::forward<Args>(args)..., MapItemT{0, 1});
        // add new element (and the iterator's address) into the underlying limited pool
        auto new_address = LP_Type::add(it.getAddress(), std::forward<Args>(args)...);
        it.modify().second().m_address = new_address;
        return new_address;
    }
    
    template <typename T, typename CompT, typename AddressT> template <typename... Args>
    void RC_LimitedPool<T, CompT, AddressT>::unRef(Args&&... args)
    {
        // find existing element
        auto it = m_pool_map.find(std::forward<Args>(args)...);
        unRefItem(it);
    }
    
    template <typename T, typename CompT, typename AddressT>
    std::size_t RC_LimitedPool<T, CompT, AddressT>::size() const {
        return m_pool_map.size();
    }
    
    template <typename T, typename CompT, typename AddressT>
    void RC_LimitedPool<T, CompT, AddressT>::commit() const
    {
        m_pool_map.commit();
        db0::vtypeless::commit();
    }

    template <typename T, typename CompT, typename AddressT>
    void RC_LimitedPool<T, CompT, AddressT>::detach() const
    {        
        m_pool_map.detach();
        db0::vtypeless::detach();
    }
    
    template <typename T, typename CompT, typename AddressT>
    void RC_LimitedPool<T, CompT, AddressT>::addRefByAddr(AddressT addr)
    {
        MemLock lock;
        auto it_addr = LP_Type::template fetch<const ItemT&>(addr, lock).m_first;
        auto it = m_pool_map.beginFromAddress(it_addr);
        assert(it != m_pool_map.end());
        ++it.modify().second().m_ref_count;
    }
    
    template <typename T, typename CompT, typename AddressT>
    void RC_LimitedPool<T, CompT, AddressT>::unRefByAddr(AddressT addr)
    {
        MemLock lock;
        auto it_addr = LP_Type::template fetch<const ItemT&>(addr, lock).m_first;
        auto it = m_pool_map.beginFromAddress(it_addr);
        assert(it != m_pool_map.end());
        unRefItem(it);
    }
    
    template <typename T, typename CompT, typename AddressT>
    void RC_LimitedPool<T, CompT, AddressT>::unRefItem(typename PoolMapT::iterator &it)
    {
        assert(it != m_pool_map.end());
        auto &item = it.modify().second();
        if (--item.m_ref_count == 0) {
            // erase from the underlying limited pool
            LP_Type::erase(item.m_address);
            // drop from the map
            m_pool_map.erase(it);
        }
    }

}