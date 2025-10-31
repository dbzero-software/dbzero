#pragma once

#include "Prefix.hpp"
#include "SlabAllocator.hpp"
#include "AlgoAllocator.hpp"
#include "Allocator.hpp"
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

    class SlabRecycler;
    class SlabManager;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_realm: public o_fixed<o_realm>
    {
        Address m_slab_defs_ptr;
        Address m_capacity_items_ptr;
        
        o_realm() = default;
        o_realm(const std::pair<Address, Address> &);
    };
DB0_PACKED_END
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_meta_header: public o_fixed<o_meta_header>
    {
        // NOTE: when needed, this values can be changed to 4 (or 8?) or 1 (no realms)
        static constexpr std::size_t NUM_REALMS = 2;
        // page size in bytes
        std::uint32_t m_page_size;
        // slab size in bytes
        std::uint32_t m_slab_size;
        o_realm m_realms[NUM_REALMS];
        
        o_meta_header(std::uint32_t page_size, std::uint32_t slab_size);
    };
DB0_PACKED_END    
    
    class MetaAllocator: public Allocator
    {
    public:
        static constexpr std::size_t NUM_REALMS = o_meta_header::NUM_REALMS;
        static constexpr std::uint32_t REALM_MASK = NUM_REALMS - 1;

        /**
         * Opens an existing instance of a MetaAllocator over a specific prefix
         * @param deferred_free if true, free operations are deferred until commit (see Transactional Allocator)
        */
        MetaAllocator(std::shared_ptr<Prefix> prefix, SlabRecycler *recycler = nullptr, 
            bool deferred_free = true);
        
        virtual ~MetaAllocator();

        /**
         * Initialize a new MetaAllocator instance over an empty prefix
        */
        static void formatPrefix(std::shared_ptr<Prefix> prefix, std::size_t page_size, std::size_t slab_size);
        
DB0_PACKED_BEGIN
        struct DB0_PACKED_ATTR CapacityItem
        {
            // primary key
            std::uint32_t m_remaining_capacity;
            std::uint32_t m_lost_capacity;
            // secondary key
            std::uint32_t m_slab_id;
            
            CapacityItem() = default;

            CapacityItem(std::uint32_t remaining_capacity, std::uint32_t lost_capacity, std::uint32_t slab_id)
                : m_remaining_capacity(remaining_capacity)
                , m_lost_capacity(lost_capacity)
                , m_slab_id(slab_id)
            {
            }

            static std::uint64_t getKey(const CapacityItem &item) {
                return ((std::uint64_t)item.m_remaining_capacity << 32) | item.m_slab_id;
            }
            
            // Construct key from construction args
            static std::uint64_t getKey(std::uint32_t remaining_capacity, std::uint32_t, std::uint32_t slab_id) {
                return ((std::uint64_t)remaining_capacity << 32) | slab_id;
            }

            inline static std::uint32_t first(std::uint64_t key) {
                return static_cast<std::uint32_t>(key >> 32);
            }

            inline static std::uint32_t second(std::uint64_t key) {
                return static_cast<std::uint32_t>(key & 0xFFFFFFFF);
            }

            // note descending order of comparisons
            struct CompT
            {
                inline bool operator()(const CapacityItem &lhs, const CapacityItem &rhs) const {
                    if (lhs.m_remaining_capacity == rhs.m_remaining_capacity)
                        return lhs.m_slab_id < rhs.m_slab_id;
                    return rhs.m_remaining_capacity < lhs.m_remaining_capacity;
                }
                
                inline bool operator()(const CapacityItem &lhs, std::uint64_t rhs) const {
                    if (lhs.m_remaining_capacity == first(rhs))
                        return lhs.m_slab_id < second(rhs);
                    return first(rhs) < lhs.m_remaining_capacity;
                }

                inline bool operator()(std::uint64_t lhs, const CapacityItem &rhs) const {
                    if (first(lhs) == rhs.m_remaining_capacity)
                        return second(lhs) < rhs.m_slab_id;
                    return rhs.m_remaining_capacity < first(lhs);
                }
            };

            struct EqualT
            {
                inline bool operator()(const CapacityItem &lhs, const CapacityItem &rhs) const {
                    return lhs.m_remaining_capacity == rhs.m_remaining_capacity && lhs.m_slab_id == rhs.m_slab_id;
                }
                
                inline bool operator()(const CapacityItem &lhs, std::uint64_t rhs) const {
                    return lhs.m_remaining_capacity == first(rhs) && lhs.m_slab_id == second(rhs);                    
                }
                
                inline bool operator()(std::uint64_t lhs, const CapacityItem &rhs) const {
                    return first(lhs) == rhs.m_remaining_capacity && second(lhs) == rhs.m_slab_id;
                }
            };
        };
DB0_PACKED_END

DB0_PACKED_BEGIN
        struct DB0_PACKED_ATTR SlabDef
        {
            // primary key
            std::uint32_t m_slab_id;
            std::uint32_t m_remaining_capacity;
            std::uint32_t m_lost_capacity;
            
            SlabDef(std::uint32_t slab_id, std::uint32_t remaining_capacity, std::uint32_t lost_capacity)
                : m_slab_id(slab_id)
                , m_remaining_capacity(remaining_capacity)
                , m_lost_capacity(lost_capacity)
            {
            }
            
            static inline std::uint32_t getKey(const SlabDef &item) {
                return item.m_slab_id;
            }

            // Extract key from construction args
            static inline std::uint32_t getKey(std::uint32_t slab_id, std::uint32_t, std::uint32_t) {
                return slab_id;
            }
            
            struct CompT
            {
                inline bool operator()(const SlabDef &lhs, const SlabDef &rhs) const {                    
                    return lhs.m_slab_id < rhs.m_slab_id;                    
                }
                
                inline bool operator()(const SlabDef &lhs, std::uint32_t rhs) const {
                    return lhs.m_slab_id < rhs;
                }

                inline bool operator()(std::uint32_t lhs, const SlabDef &rhs) const {
                    return lhs < rhs.m_slab_id;
                }
            };

            struct EqualT
            {
                inline bool operator()(const SlabDef &lhs, const SlabDef &rhs) const {
                    return lhs.m_slab_id == rhs.m_slab_id;
                }
                
                inline bool operator()(const SlabDef &lhs, std::uint32_t rhs) const {
                    return lhs.m_slab_id == rhs;
                }

                inline bool operator()(std::uint32_t lhs, const SlabDef &rhs) const {
                    return lhs == rhs.m_slab_id;
                }
            };
        };
DB0_PACKED_END

        using CapacityTreeT = SGB_Tree<CapacityItem, CapacityItem::CompT, CapacityItem::EqualT>;
        using SlabTreeT = SGB_Tree<SlabDef, SlabDef::CompT, SlabDef::EqualT>;
        
        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;
        
        std::optional<UniqueAddress> tryAllocUnique(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;
        
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;
        std::size_t getAllocSize(Address, unsigned char realm_id) const override;

        bool isAllocated(Address, std::size_t *size_of_result = nullptr) const override;
        bool isAllocated(Address, unsigned char realm_id, std::size_t *size_of_result = nullptr) const override;
        
        void commit() const override;

        void detach() const override;

        // Flush any pending "deferred" free operations
        void flush() const override;
        
        /**
         * Calculate the number of slabs which can be annotated by a single page pair
        */
        static std::size_t getSlabCount(std::size_t page_size, std::size_t slab_size);

        static std::function<Address(unsigned int)> getAddressPool(std::size_t offset, std::size_t page_size,
            std::size_t slab_size);
        
        static std::function<unsigned int(Address)> getReverseAddressPool(std::size_t offset, std::size_t page_size,
            std::size_t slab_size);

        static std::function<std::uint32_t(Address)> getSlabIdFunction(std::size_t offset, std::size_t page_size,
            std::size_t slab_size);
                
        unsigned int getSlabCount() const;
        
        /**
         * Retrieve information about the remaining space available to the Slab
        */
        std::uint32_t getRemainingCapacity(std::uint32_t slab_id) const;

        /**
         * Retrieve a new slab reserved for private use
         * note that this slab will not be available for allocations from MetaAllocator and has to be used directly
        */
        std::shared_ptr<SlabAllocator> reserveNewSlab(unsigned char realm_id = 0);
        
        /**
         * Open existing slab for private use (reserved slab)
        */
        std::shared_ptr<SlabAllocator> openReservedSlab(Address, std::size_t size) const;

        /**
         * Close the allocator and flush all modifications with backed
        */
        void close();
        
        /**
         * Get address of the 1st allocation (irrespective of whether it was performed by the MetaAllocator or not)
        */
        Address getFirstAddress() const;
        
        SlabRecycler *getSlabRecyclerPtr() const;
        
        // Visit all underlying slabs
        void forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t slab_id)>) const;
        
        // Get the number of queued defferred free operations
        std::size_t getDeferredFreeCount() const;

        // Atomic operations need special handling of the deferred free operations
        void beginAtomic();
        void endAtomic();
        void cancelAtomic();
                
    protected:
        // Calculate slab ID for the given address
        std::uint32_t getSlabId(Address) const;
        
    private:        
        std::shared_ptr<Prefix> m_prefix;
        o_meta_header m_header;
        mutable AlgoAllocator m_algo_allocator;
        Memspace m_metaspace;
        
        struct Realm
        {
            SlabTreeT m_slab_defs;
            // slab defs ordered by capacity descending
            CapacityTreeT m_capacity_items;
            std::unique_ptr<SlabManager> m_slab_manager;

            Realm(Memspace &, std::shared_ptr<Prefix>, SlabRecycler *, o_realm, std::uint32_t slab_size,
                std::uint32_t page_size, unsigned char realm_id);
            
            // get the max address from all underlying slabs
            std::uint64_t getSlabMaxAddress() const;
            void close();
            void commit() const;
            void detach() const;
            
            void beginAtomic();
            void endAtomic();
            void cancelAtomic();

            void forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)>) const;
        };
        
        struct RealmsVector: protected std::vector<Realm>
        {
            RealmsVector(Memspace &, std::shared_ptr<Prefix>, SlabRecycler *, o_meta_header &, 
                unsigned int size);

            // evaluate the max address from all realms
            std::uint64_t getSlabMaxAddress() const;

            inline SlabManager &operator[](unsigned char realm_id) {
                return *at(realm_id).m_slab_manager;
            }

            inline const SlabManager &operator[](unsigned char realm_id) const {
                return *at(realm_id).m_slab_manager;
            }

            void forAllSlabs(std::function<void(const SlabAllocator &, std::uint32_t)>) const;

            void detach() const;
            void commit() const;

            void beginAtomic();
            void endAtomic();
            void cancelAtomic();
            
            void close();
        };
        
        RealmsVector m_realms;
        SlabRecycler *m_recycler_ptr;
        const bool m_deferred_free;
        mutable std::unordered_set<Address> m_deferred_free_ops;
        std::function<std::uint32_t(Address)> m_slab_id_function;
        // flag indicating if the atomic operation is in progress
        bool m_atomic = false;
        std::vector<Address> m_atomic_deferred_free_ops;
        
        /**
         * Reads header information from the prefix
        */
        o_meta_header getMetaHeader(std::shared_ptr<Prefix> prefix);
        
        Memspace createMetaspace() const;

        /**
         * Find the slab with at least the given capacity
         * if not found then create a new slab
        */
        std::shared_ptr<SlabAllocator> getSlabAllocator(std::size_t min_capacity);

        // internal "free" implementation which performs the dealloc instanly
        void _free(Address);
        void deferredFree(Address);
        
        // NOTE: instance ID will only be populated when unique = true
        std::optional<Address> tryAllocImpl(std::size_t size, std::uint32_t slot_num, bool aligned, bool unique, 
            std::uint16_t &instance_id, unsigned char realm_id, unsigned char locality);
    };
    
}

namespace std 

{
    
    ostream &operator<<(ostream &os, const db0::MetaAllocator::CapacityItem &item);

    ostream &operator<<(ostream &os, const db0::MetaAllocator::SlabDef &item);

}