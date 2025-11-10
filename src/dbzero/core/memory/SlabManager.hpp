#pragma once

#include "Allocator.hpp"
#include "Prefix.hpp"
#include "BitSpace.hpp"
#include "Memspace.hpp"
#include "SlabAllocatorConfig.hpp"
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
        using CapacityItem = MetaAllocator::CapacityItem;
        using SlabDef = MetaAllocator::SlabDef;
        
        SlabManager(std::shared_ptr<Prefix> prefix, MetaAllocator::SlabTreeT &slab_defs,
            MetaAllocator::CapacityTreeT &capacity_items, SlabRecycler *recycler, std::uint32_t slab_size, std::uint32_t page_size,
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
        
        std::shared_ptr<SlabAllocator> reserveNewSlab();

        /**
         * Open an existing slab which has been previously reserved
        */
        std::shared_ptr<SlabAllocator> openReservedSlab(Address) const;
        std::shared_ptr<SlabAllocator> openReservedSlab(Address, std::uint32_t slab_id) const;
        
        std::uint32_t getRemainingCapacity(std::uint32_t slab_id) const;
        
        Address getFirstAddress() const;

        bool empty() const;

        void commit() const;
        
        void detach() const;

        void beginAtomic();
        void endAtomic();
        void cancelAtomic();
        
        void close();
        
        void forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)> f) const;
        
    private:

        struct FindResult
        {
            std::shared_ptr<SlabAllocator> m_slab;
            CapacityItem m_cap_item;

            bool operator==(std::uint32_t slab_id) const {
                return m_slab && m_cap_item.m_slab_id == slab_id;
            }

            bool operator==(const FindResult &rhs) const {
                return *this == rhs.m_cap_item.m_slab_id;
            }

            const SlabAllocator &operator*() const {
                return *m_slab;
            }
            
            inline bool operator!() const {
                return !m_slab;
            }
        };
        
        // NOTE: only localities 0 and 1 are currently supported
        struct ActiveSlab: public std::array<FindResult, 2>
        {
            bool contains(std::uint32_t slab_id) const {
                return ((*this)[0] == slab_id || (*this)[1] == slab_id);
            }

            bool contains(const FindResult &slab) const {
                return ((*this)[0] == slab || (*this)[1] == slab);
            }
            
            FindResult find(std::uint32_t slab_id) const
            {
                if ((*this)[0] == slab_id) {
                    return (*this)[0];
                } else if ((*this)[1] == slab_id) {
                    return (*this)[1];
                }
                return {};
            }

            void erase(const FindResult &slab)
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
        };

        /**
         * Retrieves the active slab or returns nullptr if no active slab available
        */
        FindResult tryGetActiveSlab(unsigned char locality);
        
        void resetActiveSlab(unsigned char locality);

        /**
         * Retrieve the 1st slab to allocate a block of at least min_capacity
         * this is only a 'hint' and if the allocation is not possible, the next slab should be attempted         
        */
        FindResult findFirst(std::size_t size, unsigned char locality);

        // Continue after findFirst
        FindResult findNext(FindResult last_result, std::size_t size, unsigned char locality);
                
        /**
         * Create a new, unregistered slab instance
        */
        std::pair<std::shared_ptr<SlabAllocator>, std::uint32_t> createNewSlab();

        // Create a new, registered slab instance        
        FindResult addNewSlab(unsigned char locality);
        
        // Find existing slab by ID
        FindResult tryFind(std::uint32_t slab_id) const;
        FindResult find(std::uint32_t slab_id) const;

        /**
         * Erase if 'slab' is the last slab
        */
        void erase(const FindResult &slab);
        
        std::shared_ptr<SlabAllocator> openExistingSlab(const SlabDef &);
                        
        std::uint32_t nextSlabId() const;

        struct CacheItem
        {
            SlabManager &m_manager;
            std::weak_ptr<SlabAllocator> m_slab;
            CapacityItem m_cap_item;
            // the slab's remaining capacity reflected with backend when the SlabAllocator gets destroyed
            std::uint32_t m_final_remaining_capacity = 0;
            std::uint32_t m_final_lost_capacity = 0;

            CacheItem(SlabManager &manager, std::weak_ptr<SlabAllocator> slab, CapacityItem cap)
                : m_manager(manager)
                , m_slab(slab)
                , m_cap_item(cap)
            {
            }

            void save()
            {
                if (auto slab = m_slab.lock()) {
                    if (slab) {
                        m_final_remaining_capacity = slab->getRemainingCapacity();
                        m_final_lost_capacity = slab->getLostCapacity();
                    }
                }
                // reflect changes with the backend
                m_manager.saveItem(*this);
            }
            
            void commit() const
            {
                // NOTE: SlabManager::commit calls back "save" to reflect & persist capacity changes
                if (auto slab = m_slab.lock()) {
                    if (slab) {
                        slab->commit();
                    }
                }
            }

            void detach() const
            {
                if (auto slab = m_slab.lock()) {
                    if (slab) {
                        slab->detach();
                    }
                }
            }

            // Check if any of the properties changed when compared to "capacity item"
            bool isModified() const {
                return m_final_remaining_capacity != m_cap_item.m_remaining_capacity ||
                    m_final_lost_capacity != m_cap_item.m_lost_capacity;
            }
        };
        
        using CacheIterator = std::unordered_map<std::uint64_t, std::shared_ptr<CacheItem> >::iterator;

        std::shared_ptr<Prefix> m_prefix;
        const unsigned char m_realm_id;
        MetaAllocator::SlabTreeT &m_slab_defs;
        MetaAllocator::CapacityTreeT &m_capacity_items;
        SlabRecycler *m_recycler_ptr = nullptr;
        const std::uint32_t m_slab_size;
        const std::uint32_t m_page_size;
        // slab cache by address
        mutable std::unordered_map<std::uint64_t, std::shared_ptr<CacheItem> > m_slabs;
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
        
        // Update item changes in the backend (if modified)
        void saveItem(CacheItem &item) const;

        CacheIterator unregisterSlab(CacheIterator it) const;
            
        FindResult tryOpenSlab(Address address) const;

        FindResult openSlab(Address address) const;

        // open slab by definition and add to cache
        FindResult openSlab(const SlabDef &def) const;

        void erase(const FindResult &slab, bool cleanup);

        std::uint32_t fetchNextSlabId() const;

        void deferredFree(Address);
        // internal "free" implementation which performs the dealloc instanly
        void _free(Address);
    };
    
}