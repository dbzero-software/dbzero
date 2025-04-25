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
    
namespace db0

{

    class SlabRecycler;
    class SlabManager;

    struct [[gnu::packed]] o_meta_header: public o_fixed<o_meta_header> 
    {
        // page size in bytes
        std::uint32_t m_page_size;
        // slab size in bytes
        std::uint32_t m_slab_size;
        Address m_slab_defs_ptr;
        Address m_capacity_items_ptr;
        
        o_meta_header(std::uint32_t page_size, std::uint32_t slab_size, Address slab_defs_ptr, Address capacity_items_ptr)
            : m_page_size(page_size)
            , m_slab_size(slab_size)
            , m_slab_defs_ptr(slab_defs_ptr)
            , m_capacity_items_ptr(capacity_items_ptr)
        {
        }
    };
    
    class MetaAllocator: public Allocator
    {
    public:
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
        
        struct [[gnu::packed]] CapacityItem
        {
            std::uint32_t m_remaining_capacity;
            std::uint32_t m_slab_id;
            
            CapacityItem() = default;

            CapacityItem(std::uint32_t remaining_capacity, std::uint32_t slab_id)
                : m_remaining_capacity(remaining_capacity)
                , m_slab_id(slab_id)
            {
            }

            // note descending order of comparisons
            struct CompT
            {
                inline bool operator()(const CapacityItem &lhs, const CapacityItem &rhs) const {
                    if (lhs.m_remaining_capacity == rhs.m_remaining_capacity)
                        return lhs.m_slab_id < rhs.m_slab_id;
                    return rhs.m_remaining_capacity < lhs.m_remaining_capacity;
                }
                
                inline bool operator()(const CapacityItem &lhs, std::uint32_t rhs) const {
                    return rhs < lhs.m_remaining_capacity;
                }

                inline bool operator()(std::uint32_t lhs, const CapacityItem &rhs) const {
                    return rhs.m_remaining_capacity < lhs;
                }
            };

            struct EqualT
            {
                inline bool operator()(const CapacityItem &lhs, const CapacityItem &rhs) const {
                    return lhs.m_remaining_capacity == rhs.m_remaining_capacity && lhs.m_slab_id == rhs.m_slab_id;
                }
                
                inline bool operator()(const CapacityItem &lhs, std::uint32_t rhs) const {
                    return lhs.m_remaining_capacity == rhs;
                }

                inline bool operator()(std::uint32_t lhs, const CapacityItem &rhs) const {
                    return lhs == rhs.m_remaining_capacity;
                }
            };
        };

        struct [[gnu::packed]] SlabDef
        {
            std::uint32_t m_slab_id;
            std::uint32_t m_remaining_capacity;
            
            SlabDef(std::uint32_t slab_id, std::uint32_t remaining_capacity)
                : m_slab_id(slab_id)
                , m_remaining_capacity(remaining_capacity)
            {
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

        using CapacityTreeT = SGB_Tree<CapacityItem, CapacityItem::CompT, CapacityItem::EqualT>;
        using SlabTreeT = SGB_Tree<SlabDef, SlabDef::CompT, SlabDef::EqualT>;
        
        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false) override;

        std::optional<UniqueAddress> tryAllocUnique(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false) override;
        
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;

        bool isAllocated(Address) const override;
            
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
        
        /**
         * Calculate slab ID for the given address
        */
        std::uint32_t getSlabId(Address) const;
        
        unsigned int getSlabCount() const;
        
        /**
         * Retrieve information about the remaining space available to the Slab
        */
        std::uint32_t getRemainingCapacity(std::uint32_t slab_id) const;

        /**
         * Retrieve a new slab reserved for private use
         * note that this slab will not be available for allocations from MetaAllocator and has to be used directly
        */
        std::shared_ptr<SlabAllocator> reserveNewSlab();

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

    private:
        std::shared_ptr<Prefix> m_prefix;
        o_meta_header m_header;
        mutable AlgoAllocator m_algo_allocator;
        Memspace m_metaspace;
        SlabTreeT m_slab_defs;
        // slab defs ordered by capacity descending
        CapacityTreeT m_capacity_items;
        std::unique_ptr<SlabManager> m_slab_manager;
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
    };
    
}

namespace std 

{
    
    ostream &operator<<(ostream &os, const db0::MetaAllocator::CapacityItem &item);

    ostream &operator<<(ostream &os, const db0::MetaAllocator::SlabDef &item);

}