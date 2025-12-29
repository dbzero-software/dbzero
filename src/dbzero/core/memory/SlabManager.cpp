// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SlabManager.hpp"

namespace db0

{

    SlabManager::SlabManager(std::shared_ptr<Prefix> prefix, MetaAllocator::SlabTreeT &slab_defs,
        MetaAllocator::CapacityTreeT &capacity_items, SlabRecycler *recycler, std::uint32_t slab_size, std::uint32_t page_size,
        std::function<Address(unsigned int)> address_func, std::function<std::uint32_t(Address)> slab_id_func, 
        unsigned char realm_id, bool deferred_free)
        : m_prefix(prefix)
        , m_realm_id(realm_id)
        , m_slab_defs(slab_defs)
        , m_capacity_items(capacity_items)
        , m_recycler_ptr(recycler)
        , m_slab_size(slab_size)
        , m_page_size(page_size)
        , m_slab_address_func(address_func)
        , m_slab_id_func(slab_id_func)
        , m_next_slab_id(fetchNextSlabId())
        , m_deferred_free(deferred_free)
    {
    }
    
    bool SlabManager::ActiveSlab::contains(std::uint32_t slab_id) const {
        return (((*this)[0] && *(*this)[0] == slab_id) || ((*this)[1] && *(*this)[1] == slab_id));
    }
    
    bool SlabManager::ActiveSlab::contains(std::shared_ptr<SlabItem> slab) const {
        return ((*this)[0] == slab || (*this)[1] == slab);
    }
    
    std::shared_ptr<SlabItem> SlabManager::ActiveSlab::find(std::uint32_t slab_id) const
    {
        if ((*this)[0] && *(*this)[0] == slab_id) {
            return (*this)[0];
        } else if ((*this)[1] && *(*this)[1] == slab_id) {
            return (*this)[1];
        }
        return {};
    }
    
    void SlabManager::ActiveSlab::erase(std::shared_ptr<SlabItem> slab)
    {
        if ((*this)[0] == slab) {
            (*this)[0] = {};
        } else if ((*this)[1] == slab) {
            (*this)[1] = {};
        } else {
            assert(false);
            THROWF(db0::InternalException) << "Slab not found in active slabs." << THROWF_END;
        }
    }
    
    std::shared_ptr<SlabItem> SlabManager::tryGetActiveSlab(unsigned char locality)
    {
        assert(locality < m_active_slab.size());
        return m_active_slab[locality];
    }
    
    void SlabManager::resetActiveSlab(unsigned char locality)
    {
        assert(locality < m_active_slab.size());
        m_active_slab[locality] = {};
    }
    
    std::shared_ptr<SlabItem> SlabManager::findFirst(std::size_t size, unsigned char locality)
    {
        // NOTE: before accessing capacity items we must synchronize any updates
        saveDirtySlabs();
        // visit slabs starting from the largest available capacity
        auto min_capacity = std::max(size, SlabAllocatorConfig::MIN_OP_CAPACITY(m_slab_size));
        auto it = m_capacity_items.cbegin();
        for (;;) {
            if (it.is_end() || it->m_remaining_capacity < min_capacity) {
                // no existing slab has sufficient capacity
                return {};
            }
            
            if (m_active_slab.contains(it->m_slab_id)) {
                // do not include active slab in find operation
                ++it;
                continue;
            }
            auto slab = openSlab(m_slab_address_func(it->m_slab_id));
            // make the slab active
            m_active_slab[locality] = slab;                
            return slab;
        }
    }
    
    std::shared_ptr<SlabItem> SlabManager::findNext(std::shared_ptr<SlabItem> last_result, std::size_t size,
        unsigned char locality)
    {
        saveDirtySlabs();
        auto min_capacity = std::max(size, SlabAllocatorConfig::MIN_OP_CAPACITY(m_slab_size)); 
        auto last_key = last_result->m_cap_item;
        for (;;) {
            // this is to find the next item in order
            last_key.m_slab_id += NUM_REALMS;
            auto it = m_capacity_items.upper_equal_bound(last_key);
            if (!it.first || it.first->m_remaining_capacity < min_capacity) {
                return {};
            }
            
            if (m_active_slab.contains(it.first->m_slab_id)) {
                last_key = *(it.first);
                // do not include active slab in find operation                    
                continue;
            }
            auto slab = openSlab(m_slab_address_func(it.first->m_slab_id));
            // make the slab active and for a specific locality
            m_active_slab[locality] = slab;
            return slab;
        }
    }
    
    std::pair<std::shared_ptr<SlabAllocator>, std::uint32_t> SlabManager::createNewSlab()
    {
        if (!m_next_slab_id) {
            m_next_slab_id = fetchNextSlabId();
        }

        auto slab_id = *m_next_slab_id;
        (*m_next_slab_id) += NUM_REALMS;
        auto address = m_slab_address_func(slab_id);
        // create the new slab
        auto capacity = SlabAllocator::formatSlab(m_prefix, address, m_slab_size, m_page_size);
        // NOTE: for a new slab, the initial lost capacity is 0
        auto slab = std::make_shared<SlabAllocator>(m_prefix, address, m_slab_size, m_page_size, capacity, 0);
        if (m_atomic) {
            // if atomic operation is in progress, add to the volatile slabs
            m_volatile_slabs.push_back(address);
        }
        
        return { slab, slab_id };
    }
    
    std::shared_ptr<SlabItem> SlabManager::addNewSlab(unsigned char locality)
    {
        auto [slab, slab_id] = createNewSlab();
        auto address = m_slab_address_func(slab_id);
        CapacityItem cap_item { 
            static_cast<std::uint32_t>(slab->getRemainingCapacity()), 
            static_cast<std::uint32_t>(slab->getLostCapacity()),
            slab_id
        };
        // register with slab defs
        m_slab_defs.emplace(slab_id,
            static_cast<std::uint32_t>(cap_item.m_remaining_capacity), 
            static_cast<std::uint32_t>(cap_item.m_lost_capacity)
        );
        // register with capacity items
        m_capacity_items.insert(cap_item);
        // add to cache
        auto cache_item = std::make_shared<SlabItem>(slab, cap_item);
        m_slabs.emplace(address, cache_item);
        
        // append with the recycler
        if (m_recycler_ptr) {
            m_recycler_ptr->append(cache_item);
        }
        
        // make the newly added slab active
        m_active_slab[locality] = cache_item;
        return m_active_slab[locality];
    }

    std::uint32_t SlabManager::getRemainingCapacity(std::uint32_t slab_id) const
    {
        // look up with the cache first
        auto address = m_slab_address_func(slab_id);
        auto it = m_slabs.find(address);
        if (it != m_slabs.end()) {
            auto slab = it->second.lock();
            if (slab) {
                return (*slab)->getRemainingCapacity();
            }
        }

        // look up with the slab defs if not in cache
        auto slab_def_ptr = m_slab_defs.find_equal(slab_id);
        if (!slab_def_ptr.first) {
            THROWF(db0::InternalException) << "Slab definition not found.";
        }
        return slab_def_ptr.first->m_remaining_capacity;
    }
        
    void SlabManager::close()
    {
        m_active_slab = {};
        m_reserved_slabs.clear();
        saveDirtySlabs();
        m_slabs.clear();
    }    
    
    std::shared_ptr<SlabItem> SlabManager::tryFind(std::uint32_t slab_id) const
    {
        if (slab_id < nextSlabId()) {
            if (m_active_slab.contains(slab_id)) {
                return m_active_slab.find(slab_id);
            }
            // look up with the cache first
            auto address = m_slab_address_func(slab_id);
            auto it = m_slabs.find(address);
            if (it != m_slabs.end()) {
                auto slab_item = it->second.lock();
                if (slab_item) {
                    return slab_item;
                }
                // remove expired cache entry
                m_slabs.erase(it);
            }

            return tryOpenSlab(address);
        }
        return {};
    }

    std::shared_ptr<SlabItem> SlabManager::find(std::uint32_t slab_id) const
    {
        auto slab = tryFind(slab_id);
        if (!slab) {
            THROWF(db0::BadAddressException) << "Slab " << slab_id << " not found";
        }
        return slab;
    }
    
    void SlabManager::erase(std::shared_ptr<SlabItem> slab) {
        erase(slab, true);
    }
    
    bool SlabManager::empty() const {
        return nextSlabId() == m_realm_id;
    }
    
    std::shared_ptr<SlabAllocator> SlabManager::reserveNewSlab()
    {
        auto [slab, slab_id] = createNewSlab();
        // internally register the slab with capacity = 0 (to avoid use in regular allocations)
        CapacityItem cap_item { 0, 0, slab_id };
        // register with slab defs
        m_slab_defs.emplace(
            slab_id, 
            static_cast<std::uint32_t>(cap_item.m_remaining_capacity), 
            static_cast<std::uint32_t>(cap_item.m_lost_capacity)
        );
        // register with capacity items
        m_capacity_items.insert(cap_item);
        return slab;
    }
    
    std::shared_ptr<SlabAllocator> SlabManager::openExistingSlab(const SlabDef &slab_def)
    {
        if (slab_def.m_slab_id >= nextSlabId()) {
            THROWF(db0::InputException) << "Slab " << slab_def.m_slab_id << " does not exist";
        }            
        auto address = m_slab_address_func(slab_def.m_slab_id);
        // look up with the cache first
        auto it = m_slabs.find(address);
        if (it != m_slabs.end()) {
            auto slab_item = it->second.lock();
            if (slab_item) {
                return slab_item->m_slab;
            }
        }
        // pull through cache
        return openSlab(slab_def)->m_slab;
    }
    
    std::shared_ptr<SlabAllocator> SlabManager::openReservedSlab(Address address) const {
        return openReservedSlab(address, m_slab_id_func(address));
    }
    
    std::shared_ptr<SlabAllocator> SlabManager::openReservedSlab(Address address, std::uint32_t slab_id) const
    {
        assert(m_slab_id_func(address) == slab_id);
        if (slab_id >= nextSlabId()) {
            THROWF(db0::InputException) << "Slab " << slab_id << " does not exist";
        }

        // look up with the cache first
        auto it = m_slabs.find(address);
        if (it != m_slabs.end()) {
            auto slab_item = it->second.lock();
            if (slab_item) {
                return slab_item->m_slab;
            }
        }

        // retrieve slab definition
        auto slab_def_ptr = m_slab_defs.find_equal(slab_id);
        if (!slab_def_ptr.first) {
            THROWF(db0::InternalException) << "Slab definition not found: " << slab_id;
        }
        
        // pull through cache
        auto result = openSlab(*slab_def_ptr.first)->m_slab;
        // and add for non-expiry cache
        m_reserved_slabs.push_back(result);
        return result;
    }

    Address SlabManager::getFirstAddress() const {
        return m_slab_address_func(m_realm_id) + SlabAllocator::getFirstAddress();
    }
    
    void SlabManager::commit() const
    {
        saveDirtySlabs();
        for (auto &item : m_slabs) {
            auto slab_item = item.second.lock();
            if (slab_item) {
                slab_item->commit();
            }
        }
    }
    
    void SlabManager::detach() const
    {
        // detach all cached slabs
        for (auto &item : m_slabs) {
            auto slab_item = item.second.lock();
            if (slab_item) {
                slab_item->detach();
            }
        }
        // NOTE: we retain the slab element because it's detached
        // invalidate cached variable
        m_next_slab_id = {};
    }
    
    std::uint32_t SlabManager::nextSlabId() const
    {
        if (!m_next_slab_id) {
            m_next_slab_id = fetchNextSlabId();
        }
        return *m_next_slab_id;
    }

    void SlabManager::beginAtomic()
    {            
        assert(!m_atomic);
        assert(m_volatile_slabs.empty());
        m_atomic = true;            
    }
    
    void SlabManager::endAtomic()
    {            
        assert(m_atomic);
        // merge atomic deferred free operations
        if (!m_atomic_deferred_free_ops.empty()) {
            for (auto addr : m_atomic_deferred_free_ops) {
                m_deferred_free_ops.insert(addr);
            }
            m_atomic_deferred_free_ops.clear();
        }

        m_volatile_slabs.clear();
        m_atomic = false;         
    }

    void SlabManager::cancelAtomic()
    {
        assert(m_atomic);
        // rollback atomic deferred free operations
        m_atomic_deferred_free_ops.clear();

        // revert all volatile slabs from cache
        for (auto slab_addr : m_volatile_slabs) {
            auto it = m_slabs.find(slab_addr);
            if (it != m_slabs.end()) {
                auto slab_item = it->second.lock();
                if (slab_item) {
                    slab_item->m_is_dirty = false;
                }
                m_slabs.erase(it);
            }
        }
        m_active_slab = {};
        m_volatile_slabs.clear();
        m_atomic = false;
    }
    
    void SlabManager::saveItem(SlabItem &item) const
    {
        // if the remaining capacity has hanged, reflect this with backend
        if (item.m_is_dirty) {
            auto slab_id = item.m_cap_item.m_slab_id;
            auto remaining_capacity = item->getRemainingCapacity();
            auto lost_capacity = item->getLostCapacity();
            
            auto it = m_capacity_items.find_equal(item.m_cap_item);
            assert(!it.isEnd());

            // re-register under a modified key    
            m_capacity_items.erase(it);
            m_capacity_items.emplace(
                remaining_capacity, lost_capacity, slab_id
            );
            
            // and update with the slab defs
            auto slab_def_ptr = m_slab_defs.find_equal(slab_id);      
            m_slab_defs.modify(slab_def_ptr)->m_remaining_capacity = remaining_capacity;
            m_slab_defs.modify(slab_def_ptr)->m_lost_capacity = lost_capacity;

            // update cached item
            item.m_cap_item.m_remaining_capacity = remaining_capacity;
            item.m_cap_item.m_lost_capacity = lost_capacity;
            item.m_is_dirty = false;
        }
    }

    void SlabManager::saveDirtySlabs() const
    {
        for (auto &slab_item : m_dirty_slabs) {
            saveItem(*slab_item);
        }
        m_dirty_slabs.clear();
    }
    
    std::shared_ptr<SlabItem> SlabManager::tryOpenSlab(Address address) const
    {
        auto it = m_slabs.find(address);
        if (it != m_slabs.end()) {
            auto slab_item = it->second.lock();
            if (slab_item) {
                return slab_item;
            }
            m_slabs.erase(it);
        }
        
        auto slab_id = m_slab_id_func(address);
        // retrieve slab definition
        auto slab_def_ptr = m_slab_defs.find_equal(slab_id);
        if (!slab_def_ptr.first) {
            return {};
        }
        
        return openSlab(*slab_def_ptr.first);          
    }

    std::shared_ptr<SlabItem> SlabManager::openSlab(Address address) const
    {
        auto slab = tryOpenSlab(address);
        if (!slab) {
            THROWF(db0::BadAddressException) << "Invalid address accessed";
        }
        return slab;
    }

    std::shared_ptr<SlabItem> SlabManager::openSlab(const SlabDef &def) const
    {
        auto cap_item = CapacityItem(def.m_remaining_capacity, def.m_lost_capacity, def.m_slab_id);
        auto addr = m_slab_address_func(def.m_slab_id);
        auto slab = std::make_shared<SlabAllocator>(
            m_prefix, addr, m_slab_size, m_page_size, def.m_remaining_capacity, def.m_lost_capacity
        );
        // add to cache (it's safe to reference item from the unordered_map)
        auto cache_item = std::make_shared<SlabItem>(slab, cap_item);
        m_slabs.emplace(addr, cache_item);
        
        // append with the recycler
        if (m_recycler_ptr) {
            m_recycler_ptr->append(cache_item);
        }

        return cache_item;
    }
    
    void SlabManager::erase(std::shared_ptr<SlabItem> slab, bool cleanup)
    {
        assert(slab);
        // Only the last slab can be erased
        if (slab->m_cap_item.m_slab_id != nextSlabId() - NUM_REALMS) {
            return;
        }

        auto slab_id = slab->m_cap_item.m_slab_id;
        auto addr = m_slab_address_func(slab_id);
        // clear the dirty flag since it's being erased anyway
        slab->m_is_dirty = false;
        // unregister from cache
        auto it = m_slabs.find(addr);
        if (it != m_slabs.end()) {
            m_slabs.erase(it);
        }

        // unregister from recycler
        if (m_recycler_ptr) {
            m_recycler_ptr->closeOne([&slab](const SlabItem &item) {
                return slab.get() == &item;
            });
        }
        // unregister if active
        if (m_active_slab.contains(slab)) {
            m_active_slab.erase(slab);
        }
        // unregister from slab defs
        if (!m_slab_defs.erase_equal(slab_id).first) {
            THROWF(db0::InternalException) << "Slab definition not found.";
        }
        // unregister from capacity items
        if (!m_capacity_items.erase_equal(slab->m_cap_item).first) {
            THROWF(db0::InternalException) << "Capacity item not found.";
        }
        if (!m_next_slab_id) {
            m_next_slab_id = fetchNextSlabId();
        }
        (*m_next_slab_id) -= NUM_REALMS;
        // try removing other empty slabs if such exist
        if (cleanup) {
            while (!empty()) {
                auto slab = openSlab(m_slab_address_func(nextSlabId() - NUM_REALMS));
                if (!((*slab)->empty())) {
                    break;
                }
                erase(slab, false);
            }
        }
    }

    std::uint32_t SlabManager::fetchNextSlabId() const
    {
        // determine the max slab id
        auto it = m_slab_defs.find_max();
        if (it.first) {
            return it.first->m_slab_id + NUM_REALMS;
        } else {
            // first slab being created
            return m_realm_id;
        }
    }
    
    std::optional<Address> SlabManager::tryAlloc(std::size_t size, std::uint32_t slot_num, bool aligned,
        bool unique, std::uint16_t &instance_id, unsigned char locality)
    {
        auto slab = tryGetActiveSlab(locality);
        bool is_first = true;
        bool is_new = false;
        // The number of alloc attempts from existing slabs before
        // resorting to adding a new slab
        int num_remaining_attempts = SlabAllocatorConfig::NUM_EXISTING_SLAB_ALLOC_ATTEMPTS;
        for (;;) {
            if (slab) {
                for (;;) {
                    auto addr = (*slab)->tryAlloc(size, 0, aligned);
                    if (!addr) {
                        // NOTE: since the last allocation failed, don't use this slab as "active"
                        resetActiveSlab(locality);                        
                        break;
                    }
                    
                    if (!unique || ((*slab)->tryMakeAddressUnique(*addr, instance_id))) {                        
                        // modified, add to dirty slabs
                        if (!slab->m_is_dirty) {
                            slab->m_is_dirty = true;
                            m_dirty_slabs.push_back(slab);
                        }
                        return addr;
                    }
                    
                    // unable to make the address unique, schedule for deferred free and try again
                    // NOTE: the allocation is lost
                    deferredFree(*addr);
                }
                if (size > ((*slab)->getMaxAllocSize())) {
                    THROWF(db0::InternalException) 
                        << "Requested allocation size " << size << " is larger than the slab size " << (*slab)->getMaxAllocSize();
                }
                if (is_new) {
                    THROWF(db0::InternalException) << "Slab is new but cannot allocate " << size;
                }
            }
            if (is_first) {
                slab = findFirst(size, locality);
                is_first = false;
                --num_remaining_attempts;
            } else if (num_remaining_attempts-- > 0) {
                slab = findNext(slab, size, locality);
            } else {
                slab = {};
            }
            // Create if unable to allocate from existing slabs
            // or the number of attempts has been exhausted
            if (!slab) {
                slab = addNewSlab(locality);
                is_new = true;
            }
        }
    }
    
    void SlabManager::free(Address address)
    {
        if (m_deferred_free) {
            deferredFree(address);
        } else {
            _free(address);
        }
    }
    
    void SlabManager::free(Address address, std::uint32_t slab_id)
    {
        assert(m_deferred_free_ops.find(address) == m_deferred_free_ops.end());
        if (m_deferred_free) {
            deferredFree(address);
        } else {
            _free(address, slab_id);
        }
    }

    void SlabManager::_free(Address address) {
        _free(address, m_slab_id_func(address));
    }
    
    void SlabManager::_free(Address address, std::uint32_t slab_id)
    {
        assert(m_slab_id_func(address) == slab_id); 
        auto slab = find(slab_id);
        assert(slab);
        (*slab)->free(address);
        if ((*slab)->empty()) {
            // erase or mark as erased
            erase(slab);
        } else {
            // modified, add to dirty slabs
            if (!slab->m_is_dirty) {
                slab->m_is_dirty = true;
                m_dirty_slabs.push_back(slab);
            }
        }
    }

    std::size_t SlabManager::getAllocSize(Address address) const {
        return getAllocSize(address, m_slab_id_func(address));
    }
    
    std::size_t SlabManager::getAllocSize(Address address, std::uint32_t slab_id) const
    { 
        if (m_deferred_free_ops.find(address) != m_deferred_free_ops.end()) {
            THROWF(db0::BadAddressException) << "Address " << address << " not found (pending deferred free)";
        }
        
        assert(m_slab_id_func(address) == slab_id);
        return (*find(slab_id))->getAllocSize(address);
    }
    
    bool SlabManager::isAllocated(Address address, std::size_t *size_of_result) const {
        return isAllocated(address, m_slab_id_func(address), size_of_result);
    }
    
    bool SlabManager::isAllocated(Address address, std::uint32_t slab_id, std::size_t *size_of_result) const
    {
        if (m_deferred_free_ops.find(address) != m_deferred_free_ops.end()) {
            return false;
        }

        auto slab = tryFind(slab_id);
        if (!slab) {
            return false;
        }
        return ((*slab)->isAllocated(address, size_of_result));
    }
    
    void SlabManager::forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)> f) const
    {
        auto it = m_slab_defs.cbegin();
        for (;!it.is_end();++it) {
            auto slab = const_cast<SlabManager&>(*this).openExistingSlab(*it);
            f(*slab, it->m_slab_id);
        }
    }
    
    void SlabManager::deferredFree(Address address)
    {        
        if (m_atomic) {
            m_atomic_deferred_free_ops.push_back(address);
        } else {
            m_deferred_free_ops.insert(address);
        }
    }

    void SlabManager::flush() const
    {
        assert(!m_atomic);
        assert(m_atomic_deferred_free_ops.empty());
        // perform the deferred free operations
        if (!m_deferred_free_ops.empty()) {
            for (auto addr : m_deferred_free_ops) {
                const_cast<SlabManager&>(*this)._free(addr);
            }
            m_deferred_free_ops.clear();
        }
    }

    std::size_t SlabManager::getDeferredFreeCount() const {
        return m_deferred_free_ops.size();
    }

}