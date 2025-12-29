// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Allocator.hpp"
#include "Prefix.hpp"
#include "BitSpace.hpp"
#include "Memspace.hpp"
#include "SlabAllocatorConfig.hpp"
#include "SlabItem.hpp"
#include "MetaAllocator.hpp"
#include <dbzero/core/crdt/CRDT_Allocator.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/collections/vector/LimitedVector.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
    
    /**
     * SlabManager allows efficient access to a working set of slabs
     * either for read-only or read-write operations
     * It's also capable of synchronizing metadata between slabs and the meta-indexes
     * The following requirements apply:
     * - it's only allowed to access slabs via the SlabCache (no direct access permitted)
     * - SlabCache must be part of commit/rollback flows
     * - SlabCache must be part of atomic operations
     */
    class SlabManager
    {
    public:
        static constexpr std::size_t NUM_REALMS = MetaAllocator::NUM_REALMS;
        using SlabTreeT = MetaAllocator::SlabTreeT;
        using CapacityTreeT = MetaAllocator::CapacityTreeT;
        
        SlabManager(std::shared_ptr<Prefix> prefix, SlabTreeT &slab_defs,
            CapacityTreeT &capacity_items, SlabRecycler *recycler, std::uint32_t slab_size, std::uint32_t page_size,
            std::function<Address(unsigned int)> address_func, std::function<std::uint32_t(Address)> slab_id_func, 
            unsigned char realm_id, bool deferred_free);
        
        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num, bool aligned, bool unique, 
            std::uint16_t &instance_id, unsigned char locality);
        
        void free(Address address);
        // @param slab_id must match the one calcuated from the address
        void free(Address address, std::uint32_t slab_id);
        
        std::size_t getAllocSize(Address address) const;
        std::size_t getAllocSize(Address address, std::uint32_t slab_id) const;
        
        bool isAllocated(Address address, std::size_t *size_of_result) const;
        bool isAllocated(Address address, std::uint32_t slab_id, std::size_t *size_of_result) const;
        
        unsigned int getSlabCount() const {
            return (nextSlabId() - m_realm_id) / NUM_REALMS;
        }
        
        // NOTE: reserved slabs are not updated in the CapacityItems tree
        // since they're registered with capacity = 0 (to avoid using them in regular allocations)
        std::shared_ptr<SlabAllocator> reserveNewSlab();
        
        // Open an existing reserved slab
        std::shared_ptr<SlabAllocator> openReservedSlab(Address) const;
        std::shared_ptr<SlabAllocator> openReservedSlab(Address, std::uint32_t slab_id) const;
        
        std::uint32_t getRemainingCapacity(std::uint32_t slab_id) const;
        
        std::size_t getDeferredFreeCount() const;

        Address getFirstAddress() const;

        bool empty() const;

        void commit() const;
        
        void detach() const;

        void beginAtomic();
        void endAtomic();
        void cancelAtomic();
        
        void close();
        
        void forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)> f) const;
        
        void flush() const;
        
    private:
        
        // NOTE: only localities 0 and 1 are currently supported
        struct ActiveSlab: public std::array<std::shared_ptr<SlabItem>, 2>
        {
            bool contains(std::uint32_t slab_id) const;
            bool contains(std::shared_ptr<SlabItem>) const;
            
            std::shared_ptr<SlabItem> find(std::uint32_t slab_id) const;

            void erase(std::shared_ptr<SlabItem>);
        };
        
        /**
         * Retrieves the active slab or returns nullptr if no active slab available
        */
        std::shared_ptr<SlabItem> tryGetActiveSlab(unsigned char locality);        
        void resetActiveSlab(unsigned char locality);

        /**
         * Retrieve the 1st slab to allocate a block of at least min_capacity
         * this is only a 'hint' and if the allocation is not possible, the next slab should be attempted         
        */
        std::shared_ptr<SlabItem> findFirst(std::size_t size, unsigned char locality);

        // Continue after findFirst
        std::shared_ptr<SlabItem> findNext(std::shared_ptr<SlabItem> last_result, std::size_t size, 
            unsigned char locality);
        
        /**
         * Create a new, unregistered slab instance
        */
        std::pair<std::shared_ptr<SlabAllocator>, std::uint32_t> createNewSlab();
        
        // Create a new, registered slab instance
        std::shared_ptr<SlabItem> addNewSlab(unsigned char locality);
        
        // Find existing slab by ID
        std::shared_ptr<SlabItem> tryFind(std::uint32_t slab_id) const;
        std::shared_ptr<SlabItem> find(std::uint32_t slab_id) const;
        
        /**
         * Erase if 'slab' is the last slab
        */
        void erase(std::shared_ptr<SlabItem>);
        
        std::shared_ptr<SlabAllocator> openExistingSlab(const SlabDef &);
        
        std::uint32_t nextSlabId() const;
        
        std::shared_ptr<Prefix> m_prefix;
        const unsigned char m_realm_id;
        SlabTreeT &m_slab_defs;
        CapacityTreeT &m_capacity_items;
        SlabRecycler *m_recycler_ptr = nullptr;
        const std::uint32_t m_slab_size;
        const std::uint32_t m_page_size;
        // slab cache by address
        mutable std::unordered_map<std::uint64_t, std::weak_ptr<SlabItem> > m_slabs;
        mutable std::vector<std::shared_ptr<SlabAllocator> > m_reserved_slabs;
        // active slabs for each supported locality (0 or 1)
        mutable ActiveSlab m_active_slab;
        // address by allocation ID (from the algo-allocator)
        std::function<Address(unsigned int)> m_slab_address_func;
        std::function<std::uint32_t(Address)> m_slab_id_func;
        mutable std::optional<std::uint32_t> m_next_slab_id;
        // addresses of slabs newly created during atomic operations (potentially to be reverted)
        mutable std::vector<std::uint64_t> m_volatile_slabs;
        // the atomic operation's flag
        bool m_atomic = false;
        std::vector<Address> m_atomic_deferred_free_ops;
        const bool m_deferred_free;
        mutable std::unordered_set<Address> m_deferred_free_ops;
        // the list of modified slabs (need backend refresh)
        mutable std::vector<std::shared_ptr<SlabItem> > m_dirty_slabs;
        
        // Reflect item changes with the backend (if modified)
        void saveItem(SlabItem &item) const;
        // Save all dirty slabs to the backend
        void saveDirtySlabs() const;
        
        std::shared_ptr<SlabItem> tryOpenSlab(Address address) const;
        std::shared_ptr<SlabItem> openSlab(Address address) const;
        
        // open slab by definition and add to cache
        std::shared_ptr<SlabItem> openSlab(const SlabDef &def) const;
        
        void erase(std::shared_ptr<SlabItem>, bool cleanup);
        
        std::uint32_t fetchNextSlabId() const;

        void deferredFree(Address);

        // internal "free" implementation which performs the dealloc instanly
        void _free(Address);
        void _free(Address, std::uint32_t slab_id);
    };
    
}