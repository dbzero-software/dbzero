#pragma once

#include "LimitedPool.hpp"
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/collections/map/v_map.hpp>

namespace db0::pools

{
    
    struct [[gnu::packed]] o_rc_limited_pool: public o_fixed<o_rc_limited_pool>
    {
        std::uint64_t m_pool_map_address = 0;

        o_rc_limited_pool(std::uint64_t pool_map_address)
            : m_pool_map_address(pool_map_address)
        {            
        }
    };

    /**
     * Limited pool with in-memory lookup index and ref-counting
    */
    template <typename T, typename CompT, typename AddrT = std::uint32_t> class RC_LimitedPool
        : public LimitedPool<T, AddrT>
        , public db0::v_object<o_rc_limited_pool>
    {
    public:
        using AddressT = AddrT;

        RC_LimitedPool(const Memspace &pool_memspace, Memspace &);
        RC_LimitedPool(const Memspace &pool_memspace, mptr);
        RC_LimitedPool(RC_LimitedPool const &);

        /**
         * Adds a new object or increase ref-count of the existing element
        */
        template <typename... Args> AddressT add(Args&&... args);

        /**
         * Try finding element by an arbitrary key
         * @return true if found, false otherwise
        */
        template <typename KeyT> bool find(const KeyT &, AddressT &) const;

        void commit() const;

        void detach() const;

    private:
        // address + ref count
        struct [[gnu::packed]] MapItemT: public o_fixed<MapItemT>
        {
            AddressT m_address = 0;
            std::uint32_t m_ref_count = 0;

            MapItemT(AddressT address, std::uint32_t ref_count)
                : m_address(address)
                , m_ref_count(ref_count)
            {
            }
        };

        using PoolMapT = db0::v_map<T, MapItemT, CompT>;
        PoolMapT m_pool_map;
    };
    
    template <typename T, typename CompT, typename AddressT>
    RC_LimitedPool<T, CompT, AddressT>::RC_LimitedPool(const Memspace &pool_memspace, Memspace &memspace)
        : LimitedPool<T, AddressT>(pool_memspace)
        , db0::v_object<o_rc_limited_pool>(memspace, PoolMapT(memspace).getAddress())
        , m_pool_map(memspace.myPtr((*this)->m_pool_map_address))
    {
    }
    
    template <typename T, typename CompT, typename AddressT>
    RC_LimitedPool<T, CompT, AddressT>::RC_LimitedPool(const Memspace &pool_memspace, mptr ptr)
        : LimitedPool<T, AddressT>(pool_memspace)
        , db0::v_object<o_rc_limited_pool>(ptr)
        , m_pool_map(this->myPtr((*this)->m_pool_map_address))
    {
    }
    
    template <typename T, typename CompT, typename AddressT>
    RC_LimitedPool<T, CompT, AddressT>::RC_LimitedPool(RC_LimitedPool const &other)
        : LimitedPool<T, AddressT>(other)
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
    AddressT RC_LimitedPool<T, CompT, AddressT>::add(Args&&... args)
    {
        // try finding existing element
        auto it = m_pool_map.find(std::forward<Args>(args)...);
        if (it != m_pool_map.end()) {
            // increase ref count
            auto &item = it.modify().second();
            ++item.m_ref_count;
            return item.m_address;
        }

        // add new element into the underlying limited pool
        auto new_address = LimitedPool<T, AddressT>::add(std::forward<Args>(args)...);
        // add to the map
        m_pool_map.insert_equal(std::forward<Args>(args)..., MapItemT{new_address, 1});
        return new_address;
    }
    
    template <typename T, typename CompT, typename AddressT>
    void RC_LimitedPool<T, CompT, AddressT>::commit() const
    {
        m_pool_map.commit();
        db0::v_object<o_rc_limited_pool>::commit();
    }

    template <typename T, typename CompT, typename AddressT>
    void RC_LimitedPool<T, CompT, AddressT>::detach() const
    {        
        m_pool_map.detach();
        db0::v_object<o_rc_limited_pool>::detach();
    }

}